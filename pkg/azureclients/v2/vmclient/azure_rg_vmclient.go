package vmclient

import (
	"context"
	"errors"
	"fmt"
	"time"

	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/klog/v2"

	azclients "sigs.k8s.io/cloud-provider-azure/pkg/azureclients"
	"sigs.k8s.io/cloud-provider-azure/pkg/azureclients/v2/resourcegraph"
	"sigs.k8s.io/cloud-provider-azure/pkg/metrics"
	"sigs.k8s.io/cloud-provider-azure/pkg/retry"
)

// Client implements VirtualMachine client Interface.
type Client struct {
	argClient      *resourcegraph.Client
	subscriptionID string
	cloudName      string

	// Rate limiting configures.
	rateLimiterReader flowcontrol.RateLimiter

	// ARM throttling configures.
	RetryAfterReader time.Time
}

// New creates a new VirtualMachine client with ratelimiting.
func New(config *azclients.ClientConfig) *Client {
	baseURI := config.ResourceManagerEndpoint
	authorizer := config.Authorizer
	argClient := resourcegraph.New(authorizer, *config, baseURI)
	rateLimiterReader, _ := azclients.NewRateLimiter(config.RateLimitConfig)

	if azclients.RateLimitEnabled(config.RateLimitConfig) {
		klog.V(2).Infof("Azure VirtualMachineV2 client (read ops) using rate limit config: QPS=%g, bucket=%d",
			config.RateLimitConfig.CloudProviderRateLimitQPS,
			config.RateLimitConfig.CloudProviderRateLimitBucket)
	}

	client := &Client{
		argClient:         argClient,
		rateLimiterReader: rateLimiterReader,
		subscriptionID:    config.SubscriptionID,
		cloudName:         config.CloudName,
	}
	return client
}

func (c *Client) GetVMNameByComputerName(ctx context.Context, resourceGroupName string, computerName string) (string, *retry.Error) {
	query := `Resources
| where type =~ 'Microsoft.Compute/virtualMachines'
| where properties['osProfile']['computerName'] =~ '%s'
| where resourceGroup =~ '%s'
| limit 1
| project name`
	mc := metrics.NewMetricContext("vm", "get_resourcegraph", resourceGroupName, c.subscriptionID, "")
	// Report errors if the client is rate limited.
	if !c.rateLimiterReader.TryAccept() {
		mc.RateLimitedCount()
		return "", retry.GetRateLimitError(false, "VMGetV2")
	}

	// Report errors if the client is throttled.
	if c.RetryAfterReader.After(time.Now()) {
		mc.ThrottledCount()
		rerr := retry.GetThrottlingError("VMGetV2", "client throttled", c.RetryAfterReader)
		return "", rerr
	}

	vmGetQuery := fmt.Sprintf(query, computerName, resourceGroupName)
	result, rerr := c.argClient.SendQuery(ctx, c.subscriptionID, vmGetQuery)
	mc.Observe(rerr)
	if rerr != nil {
		if rerr.IsThrottled() {
			// Update RetryAfterReader so that no more requests would be sent until RetryAfter expires.
			c.RetryAfterReader = rerr.RetryAfter
		}
		return "", rerr
	}

	if result.TotalRecords == nil || *result.TotalRecords == 0 {
		return "", retry.NewErrorOrNil(true, errors.New("no matching virtual machine found"))
	}

	if result.TotalRecords != nil && *result.TotalRecords > 1 {
		return "", retry.NewErrorOrNil(true, errors.New("more than one matching virtual machine found"))
	}

	data := result.Data.([]interface{})[0]
	if data == nil {
		return "", retry.NewErrorOrNil(true, errors.New("no matching virtual machine found"))
	}
	return data.(map[string]interface{})["name"].(string), nil
}
