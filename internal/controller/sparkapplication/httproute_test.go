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

package sparkapplication

import (
	"context"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

func httpRouteTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, networkingv1.AddToScheme(scheme))
	require.NoError(t, v1beta2.AddToScheme(scheme))
	require.NoError(t, gatewayv1.Install(scheme))
	return scheme
}

func httpRouteTestApp() *v1beta2.SparkApplication {
	return &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "test-ns",
			UID:       "test-uid",
		},
	}
}

func httpRouteTestService() SparkService {
	return SparkService{
		serviceName: "test-app-ui-svc",
		servicePort: 4040,
	}
}

func TestCreateWebUIHTTPRoute(t *testing.T) {
	testCases := []struct {
		name       string
		rawURL     string
		parentRefs []gatewayv1.ParentReference

		expectError       bool
		expectHostnames   []gatewayv1.Hostname
		expectPathPrefix  string
		expectRewrite     bool
		expectParentNS    string
		expectBackendName string
		expectBackendPort int32
	}{
		{
			name:   "host with root path produces no match and no rewrite filter",
			rawURL: "http://spark.example.com/",
			parentRefs: []gatewayv1.ParentReference{
				{Name: "eg", Namespace: ptr.To(gatewayv1.Namespace("envoy-gateway"))},
			},
			expectHostnames:   []gatewayv1.Hostname{"spark.example.com"},
			expectRewrite:     false,
			expectParentNS:    "envoy-gateway",
			expectBackendName: "test-app-ui-svc",
			expectBackendPort: 4040,
		},
		{
			name:   "subpath produces a PathPrefix match and a URLRewrite filter",
			rawURL: "http://spark.example.com/test-ns/test-app",
			parentRefs: []gatewayv1.ParentReference{
				{Name: "eg", Namespace: ptr.To(gatewayv1.Namespace("envoy-gateway"))},
			},
			expectHostnames:   []gatewayv1.Hostname{"spark.example.com"},
			expectPathPrefix:  "/test-ns/test-app",
			expectRewrite:     true,
			expectParentNS:    "envoy-gateway",
			expectBackendName: "test-app-ui-svc",
			expectBackendPort: 4040,
		},
		{
			name:   "parentRef without a namespace defaults to the application namespace",
			rawURL: "http://spark.example.com/test-ns/test-app",
			parentRefs: []gatewayv1.ParentReference{
				{Name: "eg"},
			},
			expectHostnames:   []gatewayv1.Hostname{"spark.example.com"},
			expectPathPrefix:  "/test-ns/test-app",
			expectRewrite:     true,
			expectParentNS:    "test-ns",
			expectBackendName: "test-app-ui-svc",
			expectBackendPort: 4040,
		},
		{
			name:        "no parentRefs is rejected",
			rawURL:      "http://spark.example.com/",
			parentRefs:  nil,
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := httpRouteTestScheme(t)
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
			reconciler := Reconciler{client: fakeClient}

			app := httpRouteTestApp()
			routeURL, err := url.Parse(tc.rawURL)
			require.NoError(t, err)

			result, err := reconciler.createWebUIHTTPRoute(context.TODO(), app, httpRouteTestService(), routeURL, tc.parentRefs)
			if tc.expectError {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, result)

			created := &gatewayv1.HTTPRoute{}
			require.NoError(t, fakeClient.Get(context.TODO(), types.NamespacedName{
				Name:      result.ingressName,
				Namespace: app.Namespace,
			}, created))

			assert.Equal(t, tc.expectHostnames, created.Spec.Hostnames)
			require.Len(t, created.Spec.ParentRefs, 1)
			require.NotNil(t, created.Spec.ParentRefs[0].Namespace)
			assert.Equal(t, tc.expectParentNS, string(*created.Spec.ParentRefs[0].Namespace))

			require.Len(t, created.Spec.Rules, 1)
			require.Len(t, created.Spec.Rules[0].BackendRefs, 1)
			backendRef := created.Spec.Rules[0].BackendRefs[0]
			assert.Equal(t, tc.expectBackendName, string(backendRef.Name))
			require.NotNil(t, backendRef.Port)
			assert.Equal(t, tc.expectBackendPort, *backendRef.Port)

			if tc.expectRewrite {
				require.Len(t, created.Spec.Rules[0].Matches, 1)
				pathMatch := created.Spec.Rules[0].Matches[0].Path
				require.NotNil(t, pathMatch)
				require.NotNil(t, pathMatch.Type)
				assert.Equal(t, gatewayv1.PathMatchPathPrefix, *pathMatch.Type)
				require.NotNil(t, pathMatch.Value)
				assert.Equal(t, tc.expectPathPrefix, *pathMatch.Value)

				require.Len(t, created.Spec.Rules[0].Filters, 1)
				filter := created.Spec.Rules[0].Filters[0]
				assert.Equal(t, gatewayv1.HTTPRouteFilterURLRewrite, filter.Type)
				require.NotNil(t, filter.URLRewrite)
				require.NotNil(t, filter.URLRewrite.Path)
				assert.Equal(t, gatewayv1.PrefixMatchHTTPPathModifier, filter.URLRewrite.Path.Type)
				require.NotNil(t, filter.URLRewrite.Path.ReplacePrefixMatch)
				assert.Equal(t, "/", *filter.URLRewrite.Path.ReplacePrefixMatch)
			} else {
				assert.Empty(t, created.Spec.Rules[0].Matches)
				assert.Empty(t, created.Spec.Rules[0].Filters)
			}

			// The route must be garbage collected with the application.
			require.Len(t, created.OwnerReferences, 1)
			assert.Equal(t, app.Name, created.OwnerReferences[0].Name)
		})
	}
}

// The reconciler is called on every pass, so an existing route must be updated in place
// rather than reported as a conflict.
func TestCreateWebUIHTTPRouteIsIdempotent(t *testing.T) {
	scheme := httpRouteTestScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := Reconciler{client: fakeClient}

	app := httpRouteTestApp()
	parentRefs := []gatewayv1.ParentReference{{Name: "eg"}}

	firstURL, err := url.Parse("http://spark.example.com/old-path")
	require.NoError(t, err)
	first, err := reconciler.createWebUIHTTPRoute(context.TODO(), app, httpRouteTestService(), firstURL, parentRefs)
	require.NoError(t, err)

	secondURL, err := url.Parse("http://spark.example.com/new-path")
	require.NoError(t, err)
	second, err := reconciler.createWebUIHTTPRoute(context.TODO(), app, httpRouteTestService(), secondURL, parentRefs)
	require.NoError(t, err)

	assert.Equal(t, first.ingressName, second.ingressName)

	updated := &gatewayv1.HTTPRoute{}
	require.NoError(t, fakeClient.Get(context.TODO(), types.NamespacedName{
		Name:      second.ingressName,
		Namespace: app.Namespace,
	}, updated))

	require.Len(t, updated.Spec.Rules, 1)
	require.Len(t, updated.Spec.Rules[0].Matches, 1)
	require.NotNil(t, updated.Spec.Rules[0].Matches[0].Path.Value)
	assert.Equal(t, "/new-path", *updated.Spec.Rules[0].Matches[0].Path.Value)
}

func TestCreateWebUIRouteDispatch(t *testing.T) {
	// Both are mutable package globals; leaking them changes the behaviour of every later
	// test in this package depending on run order.
	originalIngressCaps := util.IngressCapabilities
	originalHTTPRouteCaps := util.HTTPRouteCapabilities
	t.Cleanup(func() {
		util.IngressCapabilities = originalIngressCaps
		util.HTTPRouteCapabilities = originalHTTPRouteCaps
	})

	app := httpRouteTestApp()
	routeURL, err := url.Parse("http://spark.example.com/test-ns/test-app")
	require.NoError(t, err)

	t.Run("defaults to Ingress when the HTTPRoute option is off", func(t *testing.T) {
		util.IngressCapabilities = util.Capabilities{"networking.k8s.io/v1": true}
		scheme := httpRouteTestScheme(t)
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		reconciler := Reconciler{client: fakeClient, options: Options{}}

		result, err := reconciler.createWebUIRoute(context.TODO(), app, httpRouteTestService(), routeURL)
		require.NoError(t, err)

		ingress := &networkingv1.Ingress{}
		require.NoError(t, fakeClient.Get(context.TODO(), types.NamespacedName{
			Name:      result.ingressName,
			Namespace: app.Namespace,
		}, ingress))

		// No HTTPRoute should have been created.
		routes := &gatewayv1.HTTPRouteList{}
		require.NoError(t, fakeClient.List(context.TODO(), routes))
		assert.Empty(t, routes.Items)
	})

	t.Run("fails clearly when enabled on a cluster without the Gateway API", func(t *testing.T) {
		util.HTTPRouteCapabilities = util.Capabilities{}
		scheme := httpRouteTestScheme(t)
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		reconciler := Reconciler{
			client: fakeClient,
			options: Options{
				EnableUIHTTPRoute:     true,
				UIHTTPRouteParentRefs: []gatewayv1.ParentReference{{Name: "eg"}},
			},
		}

		_, err := reconciler.createWebUIRoute(context.TODO(), app, httpRouteTestService(), routeURL)
		require.Error(t, err)
		assert.Contains(t, err.Error(), util.HTTPRouteCapabilityV1)
	})

	t.Run("creates an HTTPRoute when enabled and supported", func(t *testing.T) {
		util.HTTPRouteCapabilities = util.Capabilities{util.HTTPRouteCapabilityV1: true}
		scheme := httpRouteTestScheme(t)
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		reconciler := Reconciler{
			client: fakeClient,
			options: Options{
				EnableUIHTTPRoute:     true,
				UIHTTPRouteParentRefs: []gatewayv1.ParentReference{{Name: "eg"}},
			},
		}

		result, err := reconciler.createWebUIRoute(context.TODO(), app, httpRouteTestService(), routeURL)
		require.NoError(t, err)

		created := &gatewayv1.HTTPRoute{}
		require.NoError(t, fakeClient.Get(context.TODO(), types.NamespacedName{
			Name:      result.ingressName,
			Namespace: app.Namespace,
		}, created))

		// No Ingress should have been created.
		ingresses := &networkingv1.IngressList{}
		require.NoError(t, fakeClient.List(context.TODO(), ingresses))
		assert.Empty(t, ingresses.Items)
	})
}

// Switching the operator between Ingress and HTTPRoute mode must not orphan whichever
// object the previous mode created: the SparkApplication still exists at this point, so
// owner-reference garbage collection has not run.
func TestDeleteWebUIIngressCleansUpBothExposureTypes(t *testing.T) {
	originalIngressCaps := util.IngressCapabilities
	originalHTTPRouteCaps := util.HTTPRouteCapabilities
	t.Cleanup(func() {
		util.IngressCapabilities = originalIngressCaps
		util.HTTPRouteCapabilities = originalHTTPRouteCaps
	})
	util.IngressCapabilities = util.Capabilities{"networking.k8s.io/v1": true}
	util.HTTPRouteCapabilities = util.Capabilities{util.HTTPRouteCapabilityV1: true}

	routeName := "test-app-ui-ingress"
	app := httpRouteTestApp()
	app.Status.DriverInfo.WebUIIngressName = routeName

	scheme := httpRouteTestScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: routeName, Namespace: app.Namespace}},
		&gatewayv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Name: routeName, Namespace: app.Namespace}},
	).Build()

	// Configured for Ingress, but an HTTPRoute left over from a previous configuration.
	reconciler := Reconciler{client: fakeClient, options: Options{}}
	require.NoError(t, reconciler.deleteWebUIIngress(context.TODO(), app))

	routes := &gatewayv1.HTTPRouteList{}
	require.NoError(t, fakeClient.List(context.TODO(), routes))
	assert.Empty(t, routes.Items, "stale HTTPRoute should be deleted even in Ingress mode")

	ingresses := &networkingv1.IngressList{}
	require.NoError(t, fakeClient.List(context.TODO(), ingresses))
	assert.Empty(t, ingresses.Items)

	// The resubmission gate must not report the web UI as gone while a route still exists.
	fakeClient = fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&gatewayv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Name: routeName, Namespace: app.Namespace}},
	).Build()
	reconciler = Reconciler{client: fakeClient, options: Options{}}
	assert.False(t, reconciler.validateSparkResourceDeletion(context.TODO(), app),
		"a live HTTPRoute should block resubmission")
}
