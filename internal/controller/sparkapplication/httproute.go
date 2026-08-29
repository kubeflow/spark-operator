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
	"fmt"
	"net/url"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

// createWebUIHTTPRoute creates or updates a Gateway API HTTPRoute exposing the Spark web UI
// service. It mirrors createDriverIngressV1: the same URL format drives the hostname and
// path, and the route is owned by the SparkApplication so it is garbage collected with it.
//
// The nginx-specific rewrite-target annotation that createDriverIngressV1 has to set when
// serving on a subpath is expressed natively here as a URLRewrite filter, so no
// implementation-specific annotations are required.
func (r *Reconciler) createWebUIHTTPRoute(
	ctx context.Context,
	app *v1beta2.SparkApplication,
	service SparkService,
	routeURL *url.URL,
	parentRefs []gatewayv1.ParentReference,
) (*SparkIngress, error) {
	logger := log.FromContext(ctx)

	if len(parentRefs) == 0 {
		return nil, fmt.Errorf("cannot create HTTPRoute for application %s/%s: no parentRefs configured", app.Namespace, app.Name)
	}

	routeName := util.GetDefaultUIIngressName(app)

	// parentRefs default to the SparkApplication's own namespace when the operator-wide
	// configuration does not pin one, matching how a namespace-local Gateway is referenced.
	resolvedParentRefs := make([]gatewayv1.ParentReference, 0, len(parentRefs))
	for _, parentRef := range parentRefs {
		if parentRef.Namespace == nil {
			parentRef.Namespace = ptr.To(gatewayv1.Namespace(app.Namespace))
		}
		resolvedParentRefs = append(resolvedParentRefs, parentRef)
	}

	httpRoute := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:            routeName,
			Namespace:       app.Namespace,
			Labels:          util.GetResourceLabels(app),
			OwnerReferences: []metav1.OwnerReference{util.GetOwnerReference(app)},
		},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: resolvedParentRefs,
			},
			Rules: []gatewayv1.HTTPRouteRule{{
				BackendRefs: []gatewayv1.HTTPBackendRef{{
					BackendRef: gatewayv1.BackendRef{
						BackendObjectReference: gatewayv1.BackendObjectReference{
							Name: gatewayv1.ObjectName(service.serviceName),
							Port: ptr.To(gatewayv1.PortNumber(service.servicePort)),
						},
					},
				}},
			}},
		},
	}

	// Only constrain the route by hostname when the URL format actually carries one.
	// An empty Hostnames list attaches the route to every hostname the Gateway serves.
	if hostname := routeURL.Hostname(); hostname != "" {
		httpRoute.Spec.Hostnames = []gatewayv1.Hostname{gatewayv1.Hostname(hostname)}
	}

	// When serving on a subpath, match the prefix and strip it before proxying, which is
	// what the nginx rewrite-target annotation does for the Ingress path.
	if routeURL.Path != "" && routeURL.Path != "/" {
		pathPrefix := gatewayv1.PathMatchPathPrefix
		httpRoute.Spec.Rules[0].Matches = []gatewayv1.HTTPRouteMatch{{
			Path: &gatewayv1.HTTPPathMatch{
				Type:  &pathPrefix,
				Value: ptr.To(routeURL.Path),
			},
		}}
		httpRoute.Spec.Rules[0].Filters = []gatewayv1.HTTPRouteFilter{{
			Type: gatewayv1.HTTPRouteFilterURLRewrite,
			URLRewrite: &gatewayv1.HTTPURLRewriteFilter{
				Path: &gatewayv1.HTTPPathModifier{
					Type:               gatewayv1.PrefixMatchHTTPPathModifier,
					ReplacePrefixMatch: ptr.To("/"),
				},
			},
		}}
	}

	existingHTTPRoute := &gatewayv1.HTTPRoute{}
	err := r.client.Get(ctx, client.ObjectKeyFromObject(httpRoute), existingHTTPRoute)
	if err != nil {
		if !errors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to get HTTPRoute %s/%s: %v", httpRoute.Namespace, httpRoute.Name, err)
		}
		// HTTPRoute does not exist, create it.
		if err := r.client.Create(ctx, httpRoute); err != nil {
			if !errors.IsAlreadyExists(err) {
				return nil, fmt.Errorf("failed to create HTTPRoute %s/%s: %v", httpRoute.Namespace, httpRoute.Name, err)
			}
			logger.Info("HTTPRoute already exists (race), skipping create", "httpRouteName", httpRoute.Name)
		} else {
			logger.Info("Created gateway.networking.k8s.io/v1 HTTPRoute for SparkApplication", "httpRouteName", httpRoute.Name)
		}
	} else {
		// HTTPRoute already exists, update it.
		existingHTTPRoute.Spec = httpRoute.Spec
		existingHTTPRoute.Labels = httpRoute.Labels
		existingHTTPRoute.OwnerReferences = httpRoute.OwnerReferences
		if err := r.client.Update(ctx, existingHTTPRoute); err != nil {
			return nil, fmt.Errorf("failed to update HTTPRoute %s/%s: %v", httpRoute.Namespace, httpRoute.Name, err)
		}
		logger.Info("Updated gateway.networking.k8s.io/v1 HTTPRoute for SparkApplication", "httpRouteName", httpRoute.Name)
	}

	return &SparkIngress{
		ingressName: httpRoute.Name,
		ingressURL:  routeURL,
	}, nil
}

// deleteWebUIHTTPRoute removes the HTTPRoute created for the application's web UI. The
// object carries an owner reference, so this only matters for the explicit cleanup path.
func (r *Reconciler) deleteWebUIHTTPRoute(ctx context.Context, app *v1beta2.SparkApplication) error {
	logger := log.FromContext(ctx)
	routeName := app.Status.DriverInfo.WebUIIngressName
	if routeName == "" {
		return nil
	}

	logger.Info("Deleting Spark web UI HTTPRoute", "httpRoute", routeName)
	if err := r.client.Delete(
		ctx,
		&gatewayv1.HTTPRoute{
			ObjectMeta: metav1.ObjectMeta{
				Name:      routeName,
				Namespace: app.Namespace,
			},
		},
		&client.DeleteOptions{
			GracePeriodSeconds: ptr.To[int64](0),
		},
	); err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}
