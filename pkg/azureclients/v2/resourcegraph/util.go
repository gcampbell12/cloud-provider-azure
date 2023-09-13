package resourcegraph

import (
	"net/http"
	"net/http/httputil"

	"github.com/Azure/go-autorest/autorest"
	"k8s.io/klog/v2"
)

func DoDumpRequest(v klog.Level) autorest.SendDecorator {
	return func(s autorest.Sender) autorest.Sender {
		return autorest.SenderFunc(func(request *http.Request) (*http.Response, error) {
			if request != nil {
				requestDump, err := httputil.DumpRequest(request, true)
				if err != nil {
					klog.Errorf("Failed to dump request: %v", err)
				} else {
					klog.V(v).Infof("Dumping request: %s", string(requestDump))
				}
			}
			return s.Do(request)
		})
	}
}
