/*
Copyright 2022 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package provider

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/services/compute/mgmt/2022-08-01/compute"

	"k8s.io/apimachinery/pkg/types"
	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/klog/v2"

	azcache "sigs.k8s.io/cloud-provider-azure/pkg/cache"
	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
)

func (fs *FlexScaleSet) newVmssFlexCache(ctx context.Context) (azcache.Resource, error) {
	getter := func(key string) (interface{}, error) {
		localCache := &sync.Map{}

		allResourceGroups, err := fs.GetResourceGroups()
		if err != nil {
			return nil, err
		}

		for _, resourceGroup := range allResourceGroups.UnsortedList() {
			allScaleSets, rerr := fs.VirtualMachineScaleSetsClient.List(ctx, resourceGroup)
			if rerr != nil {
				if rerr.IsNotFound() {
					klog.Warningf("Skip caching vmss for resource group %s due to error: %v", resourceGroup, rerr.Error())
					continue
				}
				klog.Errorf("VirtualMachineScaleSetsClient.List failed: %v", rerr)
				return nil, rerr.Error()
			}

			for i := range allScaleSets {
				scaleSet := allScaleSets[i]
				if scaleSet.ID == nil || *scaleSet.ID == "" {
					klog.Warning("failed to get the ID of VMSS Flex")
					continue
				}

				if scaleSet.OrchestrationMode == compute.Flexible {
					localCache.Store(*scaleSet.ID, &scaleSet)
				}
			}
		}

		return localCache, nil
	}

	if fs.Config.VmssFlexCacheTTLInSeconds == 0 {
		fs.Config.VmssFlexCacheTTLInSeconds = consts.VmssFlexCacheTTLDefaultInSeconds
	}
	return azcache.NewTimedCache(time.Duration(fs.Config.VmssFlexCacheTTLInSeconds)*time.Second, getter, fs.Cloud.Config.DisableAPICallCache)
}

func (fs *FlexScaleSet) getNodeNameByVMName(vmName string) (string, error) {
	fs.lockMap.LockEntry(consts.GetNodeVmssFlexIDLockKey)
	defer fs.lockMap.UnlockEntry(consts.GetNodeVmssFlexIDLockKey)
	cachedNodeName, isCached := fs.vmssFlexVMNameToNodeName.Load(vmName)
	if isCached {
		return fmt.Sprintf("%v", cachedNodeName), nil
	}

	getter := func(vmName string, crt azcache.AzureCacheReadType) (string, error) {
		vm, err := fs.getVmssFlexVMByVMName(vmName, crt)
		if err != nil {
			return "", err
		}

		if vm.OsProfile != nil && vm.OsProfile.ComputerName != nil {
			return strings.ToLower(*vm.OsProfile.ComputerName), nil
		}

		return "", cloudprovider.InstanceNotFound
	}

	nodeName, err := getter(vmName, azcache.CacheReadTypeDefault)
	if errors.Is(err, cloudprovider.InstanceNotFound) {
		klog.V(2).Infof("Could not find node (%s) in the existing cache. Forcely freshing the cache to check again...", vmName)
		return getter(vmName, azcache.CacheReadTypeForceRefresh)
	}
	return nodeName, err

}

func (fs *FlexScaleSet) getNodeVmssFlexID(nodeName string) (string, error) {
	fs.lockMap.LockEntry(consts.GetNodeVmssFlexIDLockKey)
	defer fs.lockMap.UnlockEntry(consts.GetNodeVmssFlexIDLockKey)
	cachedVmssFlexID, isCached := fs.vmssFlexNodeNameToVmssID.Load(nodeName)

	if isCached {
		return fmt.Sprintf("%v", cachedVmssFlexID), nil
	}

	getter := func(nodeName string, crt azcache.AzureCacheReadType) (string, error) {
		vm, err := fs.getVmssFlexVM(nodeName, crt)
		if err != nil {
			return "", err
		}

		if vm.VirtualMachineScaleSet == nil || vm.VirtualMachineScaleSet.ID == nil {
			return "", cloudprovider.InstanceNotFound
		}
		return *vm.VirtualMachineScaleSet.ID, nil
	}

	vmssFlexID, err := getter(nodeName, azcache.CacheReadTypeDefault)
	if errors.Is(err, cloudprovider.InstanceNotFound) {
		klog.V(2).Infof("Could not find node (%s) in the existing cache. Forcely freshing the cache to check again...", nodeName)
		return getter(nodeName, azcache.CacheReadTypeForceRefresh)
	}
	return vmssFlexID, err

}

func (fs *FlexScaleSet) getVmssFlexVM(nodeName string, crt azcache.AzureCacheReadType) (vm compute.VirtualMachine, err error) {
	cachedVMName, isCached := fs.vmssFlexNodeNameToVMName.Load(nodeName)
	if isCached {
		return fs.getVmssFlexVMByVMName(cachedVMName.(string), crt)
	}

	vmName, rerr := fs.VirtualMachinesClientV2.GetVMNameByComputerName(context.Background(), fs.ResourceGroup, nodeName)
	if rerr != nil {
		if rerr.IsNotFound() {
			return vm, cloudprovider.InstanceNotFound
		}
		return vm, rerr.Error()
	}
	return fs.getVmssFlexVMByVMName(vmName, crt)
}

func (fs *FlexScaleSet) getVmssFlexByVmssFlexID(vmssFlexID string, crt azcache.AzureCacheReadType) (*compute.VirtualMachineScaleSet, error) {
	cached, err := fs.vmssFlexCache.Get(consts.VmssFlexKey, crt)
	if err != nil {
		return nil, err
	}
	vmssFlexes := cached.(*sync.Map)
	if vmssFlex, ok := vmssFlexes.Load(vmssFlexID); ok {
		result := vmssFlex.(*compute.VirtualMachineScaleSet)
		return result, nil
	}

	klog.V(2).Infof("Couldn't find VMSS Flex with ID %s, refreshing the cache", vmssFlexID)
	cached, err = fs.vmssFlexCache.Get(consts.VmssFlexKey, azcache.CacheReadTypeForceRefresh)
	if err != nil {
		return nil, err
	}
	vmssFlexes = cached.(*sync.Map)
	if vmssFlex, ok := vmssFlexes.Load(vmssFlexID); ok {
		result := vmssFlex.(*compute.VirtualMachineScaleSet)
		return result, nil
	}
	return nil, cloudprovider.InstanceNotFound
}

func (fs *FlexScaleSet) getVmssFlexByNodeName(nodeName string, crt azcache.AzureCacheReadType) (*compute.VirtualMachineScaleSet, error) {
	vmssFlexID, err := fs.getNodeVmssFlexID(nodeName)
	if err != nil {
		return nil, err
	}
	vmssFlex, err := fs.getVmssFlexByVmssFlexID(vmssFlexID, crt)
	if err != nil {
		return nil, err
	}
	return vmssFlex, nil
}

func (fs *FlexScaleSet) getVmssFlexIDByName(vmssFlexName string) (string, error) {
	cached, err := fs.vmssFlexCache.Get(consts.VmssFlexKey, azcache.CacheReadTypeDefault)
	if err != nil {
		return "", err
	}
	var targetVmssFlexID string
	vmssFlexes := cached.(*sync.Map)
	vmssFlexes.Range(func(key, value interface{}) bool {
		vmssFlexID := key.(string)
		name, err := getLastSegment(vmssFlexID, "/")
		if err != nil {
			return true
		}
		if strings.EqualFold(name, vmssFlexName) {
			targetVmssFlexID = vmssFlexID
			return false
		}
		return true
	})
	if targetVmssFlexID != "" {
		return targetVmssFlexID, nil
	}
	return "", cloudprovider.InstanceNotFound
}

func (fs *FlexScaleSet) getVmssFlexByName(vmssFlexName string) (*compute.VirtualMachineScaleSet, error) {
	cached, err := fs.vmssFlexCache.Get(consts.VmssFlexKey, azcache.CacheReadTypeDefault)
	if err != nil {
		return nil, err
	}

	var targetVmssFlex *compute.VirtualMachineScaleSet
	vmssFlexes := cached.(*sync.Map)
	vmssFlexes.Range(func(key, value interface{}) bool {
		vmssFlexID := key.(string)
		vmssFlex := value.(*compute.VirtualMachineScaleSet)
		name, err := getLastSegment(vmssFlexID, "/")
		if err != nil {
			return true
		}
		if strings.EqualFold(name, vmssFlexName) {
			targetVmssFlex = vmssFlex
			return false
		}
		return true
	})
	if targetVmssFlex != nil {
		return targetVmssFlex, nil
	}
	return nil, cloudprovider.InstanceNotFound
}

func (fs *FlexScaleSet) getVmssFlexVMByVMName(vmName string, crt azcache.AzureCacheReadType) (compute.VirtualMachine, error) {
	vm, err := fs.getVirtualMachine(types.NodeName(vmName), crt)
	if err != nil {
		return compute.VirtualMachine{}, err
	}
	fs.cacheVirtualMachine(vm)
	return vm, nil
}

func (fs *FlexScaleSet) cacheVirtualMachine(vm compute.VirtualMachine) {
	if vm.OsProfile != nil && vm.OsProfile.ComputerName != nil {
		fs.vmssFlexVMNameToNodeName.Store(*vm.Name, strings.ToLower(*vm.OsProfile.ComputerName))
		fs.vmssFlexNodeNameToVMName.Store(strings.ToLower(*vm.OsProfile.ComputerName), *vm.Name)
		if vm.VirtualMachineScaleSet != nil && vm.VirtualMachineScaleSet.ID != nil {
			fs.vmssFlexNodeNameToVmssID.Store(strings.ToLower(*vm.OsProfile.ComputerName), *vm.VirtualMachineScaleSet.ID)
		}
	}
}

func (fs *FlexScaleSet) DeleteCacheForNode(nodeName string) error {
	if fs.Config.DisableAPICallCache {
		return nil
	}
	cachedVMName, isCached := fs.vmssFlexNodeNameToVMName.Load(nodeName)
	if isCached {
		vmName := cachedVMName.(string)
		fs.vmssFlexVMNameToNodeName.Delete(vmName)
	}

	fs.vmssFlexNodeNameToVmssID.Delete(nodeName)
	fs.vmssFlexNodeNameToVMName.Delete(nodeName)

	klog.V(2).Infof("DeleteCacheForNode(%s) successfully", nodeName)
	return nil
}
