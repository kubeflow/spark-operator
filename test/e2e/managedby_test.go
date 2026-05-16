/*
Copyright 2024 The Kubeflow authors.

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

package e2e_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

var _ = Describe("SparkApplication managedBy field", func() {
	ctx := context.Background()

	DescribeTable("create validation",
		func(managedBy *string, wantInvalid bool) {
			app := newManagedByApp("managed-by-test", managedBy)
			err := k8sClient.Create(ctx, app)
			if wantInvalid {
				Expect(err).To(HaveOccurred())
				Expect(apierrors.IsInvalid(err)).To(BeTrue(),
					"expected Invalid status error, got: %v", err)
			} else {
				Expect(err).To(Succeed())
				DeferCleanup(k8sClient.Delete, ctx, app)
			}
		},
		Entry("accepts nil (field omitted)", nil, false),
		Entry("accepts sparkoperator.k8s.io/spark-operator", ptr.To("sparkoperator.k8s.io/spark-operator"), false),
		Entry("accepts kueue.x-k8s.io/multikueue", ptr.To("kueue.x-k8s.io/multikueue"), false),
		Entry("rejects unknown controller", ptr.To("unknown-controller.example.com"), true),
	)

	It("is immutable once set", func() {
		app := newManagedByApp("managed-by-immutable", ptr.To("sparkoperator.k8s.io/spark-operator"))
		Expect(k8sClient.Create(ctx, app)).To(Succeed())
		DeferCleanup(k8sClient.Delete, ctx, app)

		updated := app.DeepCopy()
		updated.Spec.ManagedBy = ptr.To("kueue.x-k8s.io/multikueue")
		err := k8sClient.Update(ctx, updated)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(),
			"expected Invalid status error for immutability violation, got: %v", err)
	})
})

func newManagedByApp(name string, managedBy *string) *v1beta2.SparkApplication {
	mainFile := "local:///app.py"
	return &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Type:                v1beta2.SparkApplicationTypeScala,
			SparkVersion:        "3.5.0",
			Mode:                v1beta2.DeployModeCluster,
			MainApplicationFile: &mainFile,
			ManagedBy:           managedBy,
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Cores:  ptr.To[int32](1),
					Memory: ptr.To("1g"),
				},
			},
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Cores:  ptr.To[int32](1),
					Memory: ptr.To("1g"),
				},
				Instances: ptr.To[int32](1),
			},
		},
	}
}
