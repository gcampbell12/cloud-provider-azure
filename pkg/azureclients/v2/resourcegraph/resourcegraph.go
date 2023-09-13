package resourcegraph

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/http/cookiejar"
	"sync"
	"time"

	arg "github.com/Azure/azure-sdk-for-go/services/resourcegraph/mgmt/2021-03-01/resourcegraph"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/tracing"
	"k8s.io/utils/pointer"

	"sigs.k8s.io/cloud-provider-azure/pkg/azureclients"
	"sigs.k8s.io/cloud-provider-azure/pkg/retry"
	"sigs.k8s.io/cloud-provider-azure/pkg/version"
)

type defaultSender struct {
	sender autorest.Sender
	init   *sync.Once
}

// each type of sender will be created on demand in sender()
var defaultSenders defaultSender

func init() {
	defaultSenders.init = &sync.Once{}
}

// Client implements ARM client Interface.
type Client struct {
	client arg.BaseClient
}

func sender() autorest.Sender {
	// note that we can't init defaultSenders in init() since it will
	// execute before calling code has had a chance to enable tracing
	defaultSenders.init.Do(func() {
		// copied from http.DefaultTransport with a TLS minimum version.
		transport := &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second, // the same as default transport
				KeepAlive: 30 * time.Second, // the same as default transport
			}).DialContext,
			ForceAttemptHTTP2:     false,            // respect custom dialer (default is true)
			MaxIdleConns:          100,              // Zero means no limit, the same as default transport
			MaxIdleConnsPerHost:   100,              // Default is 2, ref:https://cs.opensource.google/go/go/+/go1.18.4:src/net/http/transport.go;l=58
			IdleConnTimeout:       90 * time.Second, // the same as default transport
			TLSHandshakeTimeout:   10 * time.Second, // the same as default transport
			ExpectContinueTimeout: 1 * time.Second,  // the same as default transport
			TLSClientConfig: &tls.Config{
				MinVersion:    tls.VersionTLS12,     //force to use TLS 1.2
				Renegotiation: tls.RenegotiateNever, // the same as default transport https://pkg.go.dev/crypto/tls#RenegotiationSupport
			},
		}
		var roundTripper http.RoundTripper = transport
		if tracing.IsEnabled() {
			roundTripper = tracing.NewTransport(transport)
		}
		j, _ := cookiejar.New(nil)
		defaultSenders.sender = &http.Client{Jar: j, Transport: roundTripper}

		// In go-autorest SDK https://github.com/Azure/go-autorest/blob/master/autorest/sender.go#L258-L287,
		// if ARM returns http.StatusTooManyRequests, the sender doesn't increase the retry attempt count,
		// hence the Azure clients will keep retrying forever until it get a status code other than 429.
		// So we explicitly removes http.StatusTooManyRequests from autorest.StatusCodesForRetry.
		// Refer https://github.com/Azure/go-autorest/issues/398.
		// TODO(feiskyer): Use autorest.SendDecorator to customize the retry policy when new Azure SDK is available.
		statusCodesForRetry := make([]int, 0)
		for _, code := range autorest.StatusCodesForRetry {
			if code != http.StatusTooManyRequests {
				statusCodesForRetry = append(statusCodesForRetry, code)
			}
		}
		autorest.StatusCodesForRetry = statusCodesForRetry
	})
	return defaultSenders.sender
}

func New(authorizer autorest.Authorizer, clientConfig azureclients.ClientConfig, baseURI string, sendDecoraters ...autorest.SendDecorator) *Client {
	argClient := arg.NewWithBaseURI(baseURI)
	argClient.Authorizer = authorizer
	argClient.Sender = sender()

	if clientConfig.UserAgent == "" {
		argClient.UserAgent = GetUserAgent(argClient)
	} else {
		argClient.UserAgent = clientConfig.UserAgent
	}

	if clientConfig.RestClientConfig.PollingDelay == nil {
		argClient.PollingDelay = 5 * time.Second
	} else {
		argClient.PollingDelay = *clientConfig.RestClientConfig.PollingDelay
	}

	if clientConfig.RestClientConfig.RetryAttempts == nil {
		argClient.RetryAttempts = 3
	} else {
		argClient.RetryAttempts = *clientConfig.RestClientConfig.RetryAttempts
	}

	if clientConfig.RestClientConfig.RetryDuration == nil {
		argClient.RetryDuration = 1 * time.Second
	} else {
		argClient.RetryDuration = *clientConfig.RestClientConfig.RetryDuration
	}

	backoff := clientConfig.Backoff
	if backoff == nil {
		backoff = &retry.Backoff{}
	}
	if backoff.Steps == 0 {
		// 1 steps means no retry.
		backoff.Steps = 1
	}

	client := &Client{
		client: argClient,
	}
	client.client.Sender = autorest.DecorateSender(client.client,
		autorest.DoCloseIfError(),
		retry.DoExponentialBackoffRetry(backoff),
		DoDumpRequest(10),
	)

	client.client.Sender = autorest.DecorateSender(client.client.Sender, sendDecoraters...)
	return client
}

// GetUserAgent gets the autorest client with a user agent that
// includes "kubernetes" and the full kubernetes git version string
// example:
// Azure-SDK-for-Go/7.0.1 arm-network/2016-09-01; kubernetes-cloudprovider/v1.17.0;
func GetUserAgent(client arg.BaseClient) string {
	k8sVersion := version.Get().GitVersion
	return fmt.Sprintf("%s; kubernetes-cloudprovider/%s", client.UserAgent, k8sVersion)
}

func (c *Client) SendQuery(ctx context.Context, subscriptionID, query string) (arg.QueryResponse, *retry.Error) {
	subscriptions := []string{subscriptionID}
	resp, err := c.client.Resources(ctx, arg.QueryRequest{
		Subscriptions: &subscriptions,
		Query:         pointer.String(query),
	})
	if resp.Response.Response == nil && err == nil {
		return resp, retry.NewError(false, fmt.Errorf("Empty response and no HTTP code"))
	}
	return resp, retry.GetError(resp.Response.Response, err)
}
