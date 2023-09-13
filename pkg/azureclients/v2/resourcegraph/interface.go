/*
Copyright 2020 The Kubernetes Authors.

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

package resourcegraph

import (
	"context"

	arg "github.com/Azure/azure-sdk-for-go/services/resourcegraph/mgmt/2021-03-01/resourcegraph"

	"sigs.k8s.io/cloud-provider-azure/pkg/retry"
)

// Interface is the client interface for ARM.
// Don't forget to run "hack/update-mock-clients.sh" command to generate the mock client.
type Interface interface {
	SendQuery(ctx context.Context, subscriptionID, query string) (arg.QueryResponse, *retry.Error)
}
