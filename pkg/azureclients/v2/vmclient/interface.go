package vmclient

import (
	"context"

	"sigs.k8s.io/cloud-provider-azure/pkg/retry"
)

type Interface interface {
	GetVMNameByComputerName(ctx context.Context, resourceGroupName string, computerName string) (string, *retry.Error)
}
