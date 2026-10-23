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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// newExecutorPod returns an executor pod in the default namespace with the given phase and labels.
func newExecutorPod(phase corev1.PodPhase, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-pi-exec-1",
			Namespace: "default",
			Labels:    labels,
		},
		Status: corev1.PodStatus{Phase: phase},
	}
}

var _ = Describe("SparkExecutorMetrics", func() {
	// Every spec builds its own metrics and never registers them, so no state is
	// shared with other specs or with the controller-runtime registry.
	var (
		m      *SparkExecutorMetrics
		labels prometheus.Labels
	)

	BeforeEach(func() {
		m = NewSparkExecutorMetrics("", []string{"namespace"})
		labels = prometheus.Labels{"namespace": "default"}
	})

	running := func() float64 { return testutil.ToFloat64(m.runningCount.With(labels)) }
	success := func() float64 { return testutil.ToFloat64(m.successCount.With(labels)) }
	failure := func() float64 { return testutil.ToFloat64(m.failureCount.With(labels)) }

	Context("HandleSparkExecutorCreate", func() {
		DescribeTable("Should only count executors that are created running",
			func(phase corev1.PodPhase, expectedRunning float64) {
				m.HandleSparkExecutorCreate(newExecutorPod(phase, nil))
				Expect(running()).To(Equal(expectedRunning))
				Expect(success()).To(BeZero())
				Expect(failure()).To(BeZero())
			},
			Entry("running", corev1.PodRunning, 1.0),
			Entry("pending", corev1.PodPending, 0.0),
			Entry("succeeded", corev1.PodSucceeded, 0.0),
			Entry("failed", corev1.PodFailed, 0.0),
		)
	})

	Context("HandleSparkExecutorUpdate", func() {
		DescribeTable("Should move the counts according to the phase transition",
			func(oldPhase, newPhase corev1.PodPhase, expectedRunning, expectedSuccess, expectedFailure float64) {
				oldPod := newExecutorPod(oldPhase, nil)
				m.HandleSparkExecutorCreate(oldPod)
				m.HandleSparkExecutorUpdate(oldPod, newExecutorPod(newPhase, nil))
				Expect(running()).To(Equal(expectedRunning))
				Expect(success()).To(Equal(expectedSuccess))
				Expect(failure()).To(Equal(expectedFailure))
			},
			Entry("pending to running", corev1.PodPending, corev1.PodRunning, 1.0, 0.0, 0.0),
			Entry("running to succeeded", corev1.PodRunning, corev1.PodSucceeded, 0.0, 1.0, 0.0),
			Entry("running to failed", corev1.PodRunning, corev1.PodFailed, 0.0, 0.0, 1.0),
			Entry("running to unknown", corev1.PodRunning, corev1.PodUnknown, 0.0, 0.0, 0.0),
			Entry("pending to succeeded without running", corev1.PodPending, corev1.PodSucceeded, 0.0, 1.0, 0.0),
			Entry("pending to failed without running", corev1.PodPending, corev1.PodFailed, 0.0, 0.0, 1.0),
			Entry("running without a phase change", corev1.PodRunning, corev1.PodRunning, 1.0, 0.0, 0.0),
		)
	})

	Context("HandleSparkExecutorDelete", func() {
		DescribeTable("Should only decrease the running count for running executors",
			func(phase corev1.PodPhase) {
				pod := newExecutorPod(phase, nil)
				m.HandleSparkExecutorCreate(pod)
				m.HandleSparkExecutorDelete(pod)
				Expect(running()).To(BeZero())
				Expect(success()).To(BeZero())
				Expect(failure()).To(BeZero())
			},
			Entry("running", corev1.PodRunning),
			Entry("pending", corev1.PodPending),
			Entry("succeeded", corev1.PodSucceeded),
			Entry("failed", corev1.PodFailed),
		)
	})

	Context("Metric labels", func() {
		BeforeEach(func() {
			// "spark-role" is not a valid Prometheus label name, so it is exposed as "spark_role".
			m = NewSparkExecutorMetrics("", []string{"namespace", "spark-role"})
		})

		It("Should take label values from the pod labels and the pod namespace", func() {
			pod := newExecutorPod(corev1.PodRunning, map[string]string{"spark-role": "executor"})
			pod.Namespace = "ns-1"
			m.HandleSparkExecutorCreate(pod)

			labels = prometheus.Labels{"namespace": "ns-1", "spark_role": "executor"}
			Expect(running()).To(Equal(1.0))
		})

		It("Should use Unknown for a configured label the pod does not have", func() {
			m.HandleSparkExecutorCreate(newExecutorPod(corev1.PodRunning, nil))

			labels = prometheus.Labels{"namespace": "default", "spark_role": "Unknown"}
			Expect(running()).To(Equal(1.0))
		})
	})

	Context("Invalid label values", func() {
		It("Should leave every count unchanged when the labels cannot be used", func() {
			// Prometheus rejects label values that are not valid UTF-8, so every
			// counter update for this pod fails and is only logged.
			pod := newExecutorPod(corev1.PodRunning, nil)
			pod.Namespace = "\xff"
			m.HandleSparkExecutorCreate(pod)

			succeeded := pod.DeepCopy()
			succeeded.Status.Phase = corev1.PodSucceeded
			m.HandleSparkExecutorUpdate(pod, succeeded)

			failed := pod.DeepCopy()
			failed.Status.Phase = corev1.PodFailed
			m.HandleSparkExecutorUpdate(pod, failed)

			Expect(testutil.CollectAndCount(m.runningCount)).To(BeZero())
			Expect(testutil.CollectAndCount(m.successCount)).To(BeZero())
			Expect(testutil.CollectAndCount(m.failureCount)).To(BeZero())
		})
	})

	Context("Metric names", func() {
		It("Should prefix the metric names and replace invalid characters", func() {
			m = NewSparkExecutorMetrics("my-prefix_", []string{"namespace"})
			m.HandleSparkExecutorCreate(newExecutorPod(corev1.PodRunning, nil))

			expected := `
# HELP my_prefix_spark_executor_running_count Total number of running Spark executors
# TYPE my_prefix_spark_executor_running_count gauge
my_prefix_spark_executor_running_count{namespace="default"} 1
`
			Expect(testutil.CollectAndCompare(m.runningCount, strings.NewReader(expected))).To(Succeed())
		})
	})

	Context("Register", func() {
		It("Should register all executor metrics with the controller-runtime registry", func() {
			m = NewSparkExecutorMetrics("register_test_", []string{"namespace"})
			// Restore the shared registry even if an assertion below fails.
			DeferCleanup(func() {
				metrics.Registry.Unregister(m.runningCount)
				metrics.Registry.Unregister(m.successCount)
				metrics.Registry.Unregister(m.failureCount)
			})

			m.Register()
			// A second call fails to register the same collectors again; Register
			// only logs those errors, so it must not panic.
			Expect(m.Register).NotTo(Panic())

			for _, c := range []prometheus.Collector{m.runningCount, m.successCount, m.failureCount} {
				Expect(metrics.Registry.Register(c)).To(BeAssignableToTypeOf(prometheus.AlreadyRegisteredError{}))
			}
		})
	})
})
