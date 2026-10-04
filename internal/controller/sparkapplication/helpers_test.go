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

package sparkapplication_test

import (
	"context"
	"fmt"

	. "github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// newFailingCreateClient returns a client that fails the first N Create
// calls for objects of the same GVK as objType, then delegates to a real
// client for every subsequent Create. This simulates a transient API
// server failure for one specific resource type without affecting other
// writes the reconciler performs in the same pass.
//
// The interceptor wraps client.WithWatch (not the plain client.Client that
// envtest hands tests via k8sClient), so we build a fresh WithWatch from
// the package-level cfg here rather than reusing k8sClient directly.
func newFailingCreateClient(objType client.Object, failures int) client.Client {
	base, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
	Expect(err).NotTo(HaveOccurred())
	targetGVK, err := base.GroupVersionKindFor(objType)
	Expect(err).NotTo(HaveOccurred())
	remaining := failures
	return interceptor.NewClient(base, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if remaining > 0 && objectGVK(c, obj) == targetGVK {
				remaining--
				return fmt.Errorf("simulated transient create failure for %s", targetGVK.Kind)
			}
			return c.Create(ctx, obj, opts...)
		},
	})
}

// newFailingDeleteClient fails the first N matching Delete calls.
func newFailingDeleteClient(objType client.Object, failures int) (client.Client, *int) {
	base, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
	Expect(err).NotTo(HaveOccurred())
	targetGVK, err := base.GroupVersionKindFor(objType)
	Expect(err).NotTo(HaveOccurred())
	remaining := failures
	attempts := 0
	return interceptor.NewClient(base, interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if objectGVK(c, obj) == targetGVK {
				attempts++
				if remaining > 0 {
					remaining--
					return apierrors.NewServiceUnavailable(fmt.Sprintf("simulated transient delete failure for %s", targetGVK.Kind))
				}
			}
			return c.Delete(ctx, obj, opts...)
		},
	}), &attempts
}

// objectGVK resolves obj's GroupVersionKind via the client's scheme.
// Returns the zero value if the lookup fails or yields no kinds.
func objectGVK(c client.Client, obj client.Object) schema.GroupVersionKind {
	gvks, _, err := c.Scheme().ObjectKinds(obj)
	if err != nil || len(gvks) == 0 {
		return schema.GroupVersionKind{}
	}
	return gvks[0]
}
