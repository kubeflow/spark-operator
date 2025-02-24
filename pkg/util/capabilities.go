/*
Copyright 2017 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package util

import (
	"strings"

	"github.com/golang/glog"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
)

type Capabilities map[string]bool

func (c Capabilities) Has(wanted string) bool {
	return c[wanted]
}

func (c Capabilities) String() string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return strings.Join(keys, ", ")
}

var (
	IngressCapabilities Capabilities
)

func InitializeIngressCapabilities(client kubernetes.Interface) (err error) {
	if IngressCapabilities != nil {
		return
	}

	IngressCapabilities, err = getPreferredAvailableAPIs(client, "Ingress")
	return
}

// getPreferredAvailableAPIs queries the cluster for the preferred resources information and returns a Capabilities
// instance containing those api groups that support the specified kind.
//
// kind should be the title case singular name of the kind. For example, "Ingress" is the kind for a resource "ingress".
func getPreferredAvailableAPIs(client kubernetes.Interface, kind string) (Capabilities, error) {
	discoveryclient := client.Discovery()
	lists, err := discoveryclient.ServerPreferredResources()
	if err != nil {
		if discovery.IsGroupDiscoveryFailedError(err) {
			glog.Infof("There is an orphaned API service. Server reports: %s", err)
		} else {
			return nil, err
		}
	}

	caps := Capabilities{}
	for _, list := range lists {
		if len(list.APIResources) == 0 {
			continue
		}
		for _, resource := range list.APIResources {
			if len(resource.Verbs) == 0 {
				continue
			}
			if resource.Kind == kind {
				caps[list.GroupVersion] = true
			}
		}
	}

	return caps, nil
}
