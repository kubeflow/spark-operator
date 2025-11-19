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

package scheduledsparkapplication

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

var _ = Describe("ScheduledSparkApplication Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default", // TODO(user):Modify as needed
		}
		scheduledsparkapplication := &v1beta2.ScheduledSparkApplication{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind ScheduledSparkApplication")
			err := k8sClient.Get(ctx, typeNamespacedName, scheduledsparkapplication)
			if err != nil && errors.IsNotFound(err) {
				resource := &v1beta2.ScheduledSparkApplication{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: v1beta2.ScheduledSparkApplicationSpec{
						Schedule:          "@every 1m",
						ConcurrencyPolicy: v1beta2.ConcurrencyAllow,
						Template: v1beta2.SparkApplicationSpec{
							Type: v1beta2.SparkApplicationTypeScala,
							Mode: v1beta2.DeployModeCluster,
							RestartPolicy: v1beta2.RestartPolicy{
								Type: v1beta2.RestartPolicyNever,
							},
							MainApplicationFile: ptr.To("local:///dummy.jar"),
						},
					},
					// TODO(user): Specify other spec details if needed.
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &v1beta2.ScheduledSparkApplication{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance ScheduledSparkApplication")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			reconciler := NewReconciler(k8sClient.Scheme(), k8sClient, nil, clock.RealClock{}, Options{Namespaces: []string{"default"}})
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			// TODO(user): Add more specific assertions depending on your controller's reconciliation logic.
			// Example: If you expect a certain status condition after reconciliation, verify it here.
		})
	})
})

// TestFormatTimestampLengths validates timestamp lengths returned for each precision.
func TestFormatTimestampLengths(t *testing.T) {
	// deterministic time value
	// seconds: 1700000000 (10 digits)
	// choose a time with specific nanoseconds so nanos/millis/micros lengths are stable
	now := time.Unix(1700000000, 123456789) // arbitrary fixed timestamp

	cases := map[string]int{
		"minutes": 8,  // Unix()/60 => ~8 digits depending on epoch; test expects approximate len
		"seconds": 10,
		"millis":  13,
		"micros":  16,
		"nanos":   19,
	}

	for precision, wantLen := range cases {
		s := formatTimestamp(precision, now)
		if len(s) != wantLen {
			t.Fatalf("precision=%s: got len %d, want %d (value=%s)", precision, len(s), wantLen, s)
		}
	}
}
