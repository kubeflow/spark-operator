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

		const name = "test-resource"
		ctx := context.Background()

		key := types.NamespacedName{Name: name, Namespace: "default"}
		obj := &v1beta2.ScheduledSparkApplication{}

		BeforeEach(func() {
			err := k8sClient.Get(ctx, key, obj)
			if errors.IsNotFound(err) {
				resource := &v1beta2.ScheduledSparkApplication{
					ObjectMeta: metav1.ObjectMeta{
						Name:      name,
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
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			res := &v1beta2.ScheduledSparkApplication{}
			Expect(k8sClient.Get(ctx, key, res)).To(Succeed())
			Expect(k8sClient.Delete(ctx, res)).To(Succeed())
		})

		It("should reconcile successfully", func() {
			r := NewReconciler(
				k8sClient.Scheme(),
				k8sClient,
				nil,
				clock.RealClock{},
				Options{Namespaces: []string{"default"}},
			)

			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
		})
	})
})

// Timestamp precision tests
func TestFormatTimestampLengths(t *testing.T) {
	now := time.Unix(1700000000, 123456789)

	cases := map[string]int{
		"minutes": 8, // variable → validated differently
		"seconds": 10,
		"millis":  13,
		"micros":  16,
		"nanos":   19,
	}

	for precision, expectLen := range cases {
		s := formatTimestamp(precision, now)

		if precision == "minutes" {
			if len(s) < 7 || len(s) > 9 {
				t.Fatalf("minutes: got %d digits (%s), expected between 7-9", len(s), s)
			}
			continue
		}

		if len(s) != expectLen {
			t.Fatalf("precision=%s: got len %d want %d (%s)",
				precision, len(s), expectLen, s)
		}
	}
}
