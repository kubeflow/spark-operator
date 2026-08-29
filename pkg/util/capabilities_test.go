/*
Copyright 2026 The Kubeflow authors.

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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

// client-go's FakeDiscovery hardcodes ServerPreferredResources to return nil, so it cannot
// drive capability discovery. These stubs return the resource lists a test declares.
type stubDiscovery struct {
	discovery.DiscoveryInterface
	resources []*metav1.APIResourceList
}

func (d *stubDiscovery) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	return d.resources, nil
}

type stubClientset struct {
	kubernetes.Interface
	discovery discovery.DiscoveryInterface
}

func (c *stubClientset) Discovery() discovery.DiscoveryInterface {
	return c.discovery
}

func newStubClientset(resources []*metav1.APIResourceList) kubernetes.Interface {
	base := fake.NewSimpleClientset()
	return &stubClientset{
		Interface: base,
		discovery: &stubDiscovery{DiscoveryInterface: base.Discovery(), resources: resources},
	}
}

func TestInitializeHTTPRouteCapabilities(t *testing.T) {
	testCases := []struct {
		name      string
		resources []*metav1.APIResourceList
		expectHas bool
	}{
		{
			name: "cluster serving the Gateway API is detected",
			resources: []*metav1.APIResourceList{
				{
					GroupVersion: "gateway.networking.k8s.io/v1",
					APIResources: []metav1.APIResource{
						{Name: "httproutes", Kind: "HTTPRoute", Verbs: metav1.Verbs{"get", "list", "create"}},
					},
				},
			},
			expectHas: true,
		},
		{
			// The Gateway API CRDs are optional, so their absence must not be an error.
			name: "cluster without the Gateway API CRDs yields no capability",
			resources: []*metav1.APIResourceList{
				{
					GroupVersion: "networking.k8s.io/v1",
					APIResources: []metav1.APIResource{
						{Name: "ingresses", Kind: "Ingress", Verbs: metav1.Verbs{"get", "list", "create"}},
					},
				},
			},
			expectHas: false,
		},
		{
			name: "a kind advertised with no verbs is not usable",
			resources: []*metav1.APIResourceList{
				{
					GroupVersion: "gateway.networking.k8s.io/v1",
					APIResources: []metav1.APIResource{
						{Name: "httproutes", Kind: "HTTPRoute", Verbs: metav1.Verbs{}},
					},
				},
			},
			expectHas: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// The capability is cached in a package-level var, so reset it per case.
			HTTPRouteCapabilities = nil
			t.Cleanup(func() { HTTPRouteCapabilities = nil })

			require.NoError(t, InitializeHTTPRouteCapabilities(newStubClientset(tc.resources)))
			assert.Equal(t, tc.expectHas, HTTPRouteCapabilities.Has("gateway.networking.k8s.io/v1"))
		})
	}
}

func TestInitializeHTTPRouteCapabilitiesIsCached(t *testing.T) {
	HTTPRouteCapabilities = Capabilities{"gateway.networking.k8s.io/v1": true}
	t.Cleanup(func() { HTTPRouteCapabilities = nil })

	// A second call must not re-run discovery and must not clear what was already found.
	require.NoError(t, InitializeHTTPRouteCapabilities(newStubClientset(nil)))
	assert.True(t, HTTPRouteCapabilities.Has("gateway.networking.k8s.io/v1"))
}
