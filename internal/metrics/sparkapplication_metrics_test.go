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

package metrics

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

func newSparkApplication(name, namespace string, state v1beta2.ApplicationStateType, labels map[string]string) *v1beta2.SparkApplication {
	return &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels},
		Status:     v1beta2.SparkApplicationStatus{AppState: v1beta2.ApplicationState{State: state}},
	}
}

var _ = Describe("SparkApplicationMetrics", func() {
	Context("State count", func() {
		var m *SparkApplicationMetrics

		BeforeEach(func() {
			m = NewSparkApplicationMetrics("", []string{"namespace", "app_type"}, []float64{1}, []float64{1})
		})

		count := func(namespace, appType, state string) float64 {
			return testutil.ToFloat64(m.stateCount.With(prometheus.Labels{"namespace": namespace, "app_type": appType, "state": state}))
		}

		transition := func(app *v1beta2.SparkApplication, state v1beta2.ApplicationStateType) *v1beta2.SparkApplication {
			newApp := app.DeepCopy()
			newApp.Status.AppState.State = state
			m.HandleSparkApplicationUpdate(app, newApp)
			return newApp
		}

		It("Should track an app through a rerun, reporting the empty state as NEW", func() {
			app := newSparkApplication("spark-pi", "default", v1beta2.ApplicationStateNew, map[string]string{"app_type": "batch"})
			m.HandleSparkApplicationCreate(app)
			app = transition(app, v1beta2.ApplicationStateSubmitted)
			app = transition(app, v1beta2.ApplicationStateRunning)
			app = transition(app, v1beta2.ApplicationStatePendingRerun)

			Expect(testutil.CollectAndCompare(m.stateCount, strings.NewReader(`
# HELP spark_application_state_count Current number of SparkApplication in each state
# TYPE spark_application_state_count gauge
spark_application_state_count{app_type="batch",namespace="default",state="NEW"} 0
spark_application_state_count{app_type="batch",namespace="default",state="PENDING_RERUN"} 1
spark_application_state_count{app_type="batch",namespace="default",state="RUNNING"} 0
spark_application_state_count{app_type="batch",namespace="default",state="SUBMITTED"} 0
`))).To(Succeed())

			transition(app, v1beta2.ApplicationStateSubmitted)
			Expect(count("default", "batch", "PENDING_RERUN")).To(BeZero())
			Expect(count("default", "batch", "SUBMITTED")).To(Equal(1.0))
		})

		It("Should count apps separately per namespace and label value", func() {
			a1 := newSparkApplication("a1", "ns-a", v1beta2.ApplicationStateRunning, map[string]string{"app_type": "batch"})
			a2 := newSparkApplication("a2", "ns-a", v1beta2.ApplicationStateRunning, map[string]string{"app_type": "batch"})
			b1 := newSparkApplication("b1", "ns-b", v1beta2.ApplicationStateRunning, map[string]string{"app_type": "streaming"})
			for _, app := range []*v1beta2.SparkApplication{a1, a2, b1} {
				m.HandleSparkApplicationCreate(app)
			}
			transition(a1, v1beta2.ApplicationStatePendingRerun)

			Expect(count("ns-a", "batch", "RUNNING")).To(Equal(1.0))
			Expect(count("ns-a", "batch", "PENDING_RERUN")).To(Equal(1.0))
			Expect(count("ns-b", "streaming", "RUNNING")).To(Equal(1.0))
			Expect(count("ns-b", "streaming", "PENDING_RERUN")).To(BeZero())
		})

		It("Should move the app to another series when a configured label changes", func() {
			app := newSparkApplication("spark-pi", "default", v1beta2.ApplicationStateRunning, map[string]string{"app_type": "batch"})
			m.HandleSparkApplicationCreate(app)
			relabeled := app.DeepCopy()
			relabeled.Labels["app_type"] = "streaming"
			m.HandleSparkApplicationUpdate(app, relabeled)
			transition(relabeled, v1beta2.ApplicationStateCompleted)

			Expect(count("default", "batch", "RUNNING")).To(BeZero())
			Expect(count("default", "streaming", "RUNNING")).To(BeZero())
			Expect(count("default", "streaming", "COMPLETED")).To(Equal(1.0))
		})

		It("Should not change the counts on an update without a state or label change", func() {
			app := newSparkApplication("spark-pi", "default", v1beta2.ApplicationStatePendingRerun, nil)
			m.HandleSparkApplicationCreate(app)
			m.HandleSparkApplicationUpdate(app, app.DeepCopy())

			Expect(count("default", "Unknown", "PENDING_RERUN")).To(Equal(1.0))
		})

		DescribeTable("Should count an app created in any state and remove it from that state on deletion",
			func(state v1beta2.ApplicationStateType, label string) {
				app := newSparkApplication("spark-pi", "default", state, nil)
				m.HandleSparkApplicationCreate(app)
				Expect(count("default", "Unknown", label)).To(Equal(1.0))

				m.HandleSparkApplicationDelete(app)
				Expect(count("default", "Unknown", label)).To(BeZero())
			},
			Entry("new", v1beta2.ApplicationStateNew, "NEW"),
			Entry("submitted", v1beta2.ApplicationStateSubmitted, "SUBMITTED"),
			Entry("running", v1beta2.ApplicationStateRunning, "RUNNING"),
			Entry("pending rerun", v1beta2.ApplicationStatePendingRerun, "PENDING_RERUN"),
			Entry("invalidating", v1beta2.ApplicationStateInvalidating, "INVALIDATING"),
			Entry("suspended", v1beta2.ApplicationStateSuspended, "SUSPENDED"),
			Entry("completed", v1beta2.ApplicationStateCompleted, "COMPLETED"),
			Entry("failed", v1beta2.ApplicationStateFailed, "FAILED"),
		)

		It("Should not count apps managed by MultiKueue", func() {
			app := newSparkApplication("spark-pi", "default", v1beta2.ApplicationStateNew, nil)
			app.Spec.ManagedBy = ptr.To("kueue.x-k8s.io/multikueue")
			m.HandleSparkApplicationCreate(app)
			completed := app.DeepCopy()
			completed.Status.AppState.State = v1beta2.ApplicationStateCompleted
			m.HandleSparkApplicationDelete(completed)

			Expect(testutil.CollectAndCount(m.stateCount)).To(BeZero())
		})

		It("Should register the state count with the controller-runtime registry", func() {
			registry := metrics.Registry
			metrics.Registry = prometheus.NewRegistry()
			DeferCleanup(func() { metrics.Registry = registry })

			m.Register()
			m.HandleSparkApplicationCreate(newSparkApplication("spark-pi", "default", v1beta2.ApplicationStatePendingRerun, nil))

			Expect(testutil.GatherAndCount(metrics.Registry, "spark_application_state_count")).To(Equal(1))
		})
	})
})
