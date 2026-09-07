/*
Copyright The Kubeflow Authors.

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

package util_test

import (
	"errors"

	"github.com/kubeflow/spark-operator/v2/pkg/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
)

// fakeDiscovery implements just enough of discovery.DiscoveryInterface for
// these tests; ServerPreferredResources is the only method util.Capabilities
// calls.
type fakeDiscovery struct {
	discovery.DiscoveryInterface
	resources []*metav1.APIResourceList
	err       error
}

func (f *fakeDiscovery) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	return f.resources, f.err
}

type fakeClientset struct {
	kubernetes.Interface
	discovery discovery.DiscoveryInterface
}

func (f *fakeClientset) Discovery() discovery.DiscoveryInterface {
	return f.discovery
}

var _ = Describe("Capabilities", func() {
	AfterEach(func() {
		util.IngressCapabilities = nil
	})

	Describe("InitializeIngressCapabilities", func() {
		It("populates IngressCapabilities when the cluster has an Ingress kind", func() {
			client := &fakeClientset{discovery: &fakeDiscovery{resources: []*metav1.APIResourceList{
				{
					GroupVersion: "networking.k8s.io/v1",
					APIResources: []metav1.APIResource{{Kind: "Ingress", Verbs: metav1.Verbs{"get", "list"}}},
				},
			}}}

			Expect(util.InitializeIngressCapabilities(client)).To(Succeed())
			Expect(util.IngressCapabilities.Has("networking.k8s.io/v1")).To(BeTrue())
		})

		It("leaves IngressCapabilities empty when the cluster has no Ingress kind", func() {
			client := &fakeClientset{discovery: &fakeDiscovery{resources: []*metav1.APIResourceList{
				{
					GroupVersion: "apps/v1",
					APIResources: []metav1.APIResource{{Kind: "Deployment", Verbs: metav1.Verbs{"get", "list"}}},
				},
			}}}

			Expect(util.InitializeIngressCapabilities(client)).To(Succeed())
			Expect(util.IngressCapabilities.Has("apps/v1")).To(BeFalse())
		})

		It("is a no-op on the second call", func() {
			first := &fakeClientset{discovery: &fakeDiscovery{resources: []*metav1.APIResourceList{
				{
					GroupVersion: "networking.k8s.io/v1",
					APIResources: []metav1.APIResource{{Kind: "Ingress", Verbs: metav1.Verbs{"get"}}},
				},
			}}}
			Expect(util.InitializeIngressCapabilities(first)).To(Succeed())

			second := &fakeClientset{discovery: &fakeDiscovery{resources: nil}}
			Expect(util.InitializeIngressCapabilities(second)).To(Succeed())

			Expect(util.IngressCapabilities.Has("networking.k8s.io/v1")).To(BeTrue())
		})

		It("tolerates an orphaned API service instead of failing", func() {
			gv := schema.GroupVersion{Group: "orphaned.example.com", Version: "v1"}
			client := &fakeClientset{discovery: &fakeDiscovery{
				err: &discovery.ErrGroupDiscoveryFailed{Groups: map[schema.GroupVersion]error{gv: errors.New("boom")}},
			}}

			Expect(util.InitializeIngressCapabilities(client)).To(Succeed())
			Expect(util.IngressCapabilities).To(BeEmpty())
		})

		It("returns other discovery errors", func() {
			client := &fakeClientset{discovery: &fakeDiscovery{err: errors.New("discovery unavailable")}}

			Expect(util.InitializeIngressCapabilities(client)).To(MatchError("discovery unavailable"))
		})
	})
})
