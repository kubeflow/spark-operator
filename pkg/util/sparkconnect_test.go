/*
Copyright 2026 Kubeflow

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package util_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

var _ = Describe("SparkConnect restart helpers", func() {
	DescribeTable("ShouldRestartSparkConnectServerPod",
		func(policy v1alpha1.RestartPolicyType, phase corev1.PodPhase, expected bool) {
			conn := &v1alpha1.SparkConnect{
				Spec: v1alpha1.SparkConnectSpec{
					RestartPolicy: v1alpha1.RestartPolicy{
						RestartPolicyType: policy,
					},
				},
			}
			pod := &corev1.Pod{Status: corev1.PodStatus{Phase: phase}}

			Expect(util.ShouldRestartSparkConnectServerPod(conn, pod)).To(Equal(expected))
		},
		Entry("defaults empty policy to on failure for failed pods", v1alpha1.RestartPolicyType(""), corev1.PodFailed, true),
		Entry("defaults empty policy to on failure for succeeded pods", v1alpha1.RestartPolicyType(""), corev1.PodSucceeded, false),
		Entry("always restarts failed pods", v1alpha1.RestartPolicyTypeAlways, corev1.PodFailed, true),
		Entry("always restarts succeeded pods", v1alpha1.RestartPolicyTypeAlways, corev1.PodSucceeded, true),
		Entry("always does not restart running pods", v1alpha1.RestartPolicyTypeAlways, corev1.PodRunning, false),
		Entry("on failure restarts failed pods", v1alpha1.RestartPolicyTypeOnFailure, corev1.PodFailed, true),
		Entry("on failure does not restart succeeded pods", v1alpha1.RestartPolicyTypeOnFailure, corev1.PodSucceeded, false),
		Entry("never does not restart failed pods", v1alpha1.RestartPolicyTypeNever, corev1.PodFailed, false),
		Entry("unknown policy does not restart failed pods", v1alpha1.RestartPolicyType("Sometimes"), corev1.PodFailed, false),
	)

	DescribeTable("HasSparkConnectRestartAttemptsRemaining",
		func(retries *int32, attempts int32, expected bool) {
			conn := &v1alpha1.SparkConnect{
				Spec: v1alpha1.SparkConnectSpec{
					RestartPolicy: v1alpha1.RestartPolicy{
						OnFailureRetries: retries,
					},
				},
				Status: v1alpha1.SparkConnectStatus{
					RestartAttempts: attempts,
				},
			}

			Expect(util.HasSparkConnectRestartAttemptsRemaining(conn)).To(Equal(expected))
		},
		Entry("nil retries means none remaining", nil, int32(0), false),
		Entry("zero retries means none remaining", ptrTo(int32(0)), int32(0), false),
		Entry("attempts below retry count remain", ptrTo(int32(2)), int32(1), true),
		Entry("attempts equal retry count are exhausted", ptrTo(int32(2)), int32(2), false),
		Entry("attempts above retry count are exhausted", ptrTo(int32(2)), int32(3), false),
	)

	It("returns the restart backoff start time from the restarting condition", func() {
		start := metav1.NewTime(time.Now().Add(-time.Minute))
		conn := &v1alpha1.SparkConnect{
			Status: v1alpha1.SparkConnectStatus{
				Conditions: []metav1.Condition{
					{
						Type:               string(v1alpha1.SparkConnectConditionServerPodReady),
						Status:             metav1.ConditionFalse,
						Reason:             string(v1alpha1.SparkConnectConditionReasonRestarting),
						LastTransitionTime: start,
					},
				},
			},
		}

		got, ok := util.SparkConnectRestartBackoffStartTime(conn)
		Expect(ok).To(BeTrue())
		Expect(got).To(Equal(start.Time))
	})

	DescribeTable("SparkConnectRestartBackoffStartTime ignores non-restarting conditions",
		func(condition *metav1.Condition) {
			conn := &v1alpha1.SparkConnect{}
			if condition != nil {
				conn.Status.Conditions = []metav1.Condition{*condition}
			}

			_, ok := util.SparkConnectRestartBackoffStartTime(conn)
			Expect(ok).To(BeFalse())
		},
		Entry("missing condition", nil),
		Entry("wrong status", &metav1.Condition{
			Type:               string(v1alpha1.SparkConnectConditionServerPodReady),
			Status:             metav1.ConditionTrue,
			Reason:             string(v1alpha1.SparkConnectConditionReasonRestarting),
			LastTransitionTime: metav1.Now(),
		}),
		Entry("wrong reason", &metav1.Condition{
			Type:               string(v1alpha1.SparkConnectConditionServerPodReady),
			Status:             metav1.ConditionFalse,
			Reason:             "Ready",
			LastTransitionTime: metav1.Now(),
		}),
		Entry("zero transition time", &metav1.Condition{
			Type:   string(v1alpha1.SparkConnectConditionServerPodReady),
			Status: metav1.ConditionFalse,
			Reason: string(v1alpha1.SparkConnectConditionReasonRestarting),
		}),
	)

	It("returns the default restart backoff when interval is unset", func() {
		conn := &v1alpha1.SparkConnect{}

		Expect(util.SparkConnectRestartBackoff(conn)).To(Equal(5 * time.Second))
	})

	It("returns the configured restart backoff in seconds", func() {
		interval := int64(10)
		conn := &v1alpha1.SparkConnect{
			Spec: v1alpha1.SparkConnectSpec{
				RestartPolicy: v1alpha1.RestartPolicy{
					OnFailureRetryInterval: &interval,
				},
			},
		}

		Expect(util.SparkConnectRestartBackoff(conn)).To(Equal(10 * time.Second))
	})
})

func ptrTo[T any](value T) *T {
	return &value
}
