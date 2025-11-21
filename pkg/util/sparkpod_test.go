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

package util_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

var _ = Describe("IsLaunchedBySparkOperator", func() {
	Context("Pod without labels", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
			},
		}

		It("Should return false", func() {
			Expect(util.IsLaunchedBySparkOperator(pod)).To(BeFalse())
		})
	})

	Context("Pod without launched by spark operator label", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
				Labels: map[string]string{
					common.LabelSparkAppName: "test-app",
				},
			},
		}

		It("Should return false", func() {
			Expect(util.IsLaunchedBySparkOperator(pod)).To(BeFalse())
		})
	})

	Context("Pod with launched by spark operator label", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
				Labels: map[string]string{
					common.LabelSparkAppName:            "test-app",
					common.LabelLaunchedBySparkOperator: "true",
				},
			},
		}

		It("Should return true", func() {
			Expect(util.IsLaunchedBySparkOperator(pod)).To(BeTrue())
		})
	})
})

var _ = Describe("IsDriverPod", func() {
	Context("Pod without labels", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
			},
		}

		It("Should return false", func() {
			Expect(util.IsDriverPod(pod)).To(BeFalse())
		})
	})

	Context("Pod without spark role label", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
				Labels: map[string]string{
					common.LabelSparkAppName: "test-app",
				},
			},
		}

		It("Should return false", func() {
			Expect(util.IsDriverPod(pod)).To(BeFalse())
		})
	})

	Context("Pod with spark role label not equal to driver", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
				Labels: map[string]string{
					common.LabelSparkAppName: "test-app",
					common.LabelSparkRole:    common.SparkRoleExecutor,
				},
			},
		}

		It("Should return false", func() {
			Expect(util.IsDriverPod(pod)).To(BeFalse())
		})
	})

	Context("Pod with spark role label equal to driver", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
				Labels: map[string]string{
					common.LabelSparkAppName: "test-app",
					common.LabelSparkRole:    common.SparkRoleDriver,
				},
			},
		}

		It("Should return true", func() {
			Expect(util.IsDriverPod(pod)).To(BeTrue())
		})
	})
})

var _ = Describe("IsExecutorPod", func() {
	Context("Pod without labels", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
			},
		}

		It("Should return false", func() {
			Expect(util.IsExecutorPod(pod)).To(BeFalse())
		})
	})

	Context("Pod without spark role label", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
				Labels: map[string]string{
					common.LabelSparkAppName: "test-app",
				},
			},
		}

		It("Should return false", func() {
			Expect(util.IsExecutorPod(pod)).To(BeFalse())
		})
	})

	Context("Pod with spark role label not equal to executor", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
				Labels: map[string]string{
					common.LabelSparkAppName: "test-app",
					common.LabelSparkRole:    common.SparkRoleDriver,
				},
			},
		}

		It("Should return false", func() {
			Expect(util.IsExecutorPod(pod)).To(BeFalse())
		})
	})

	Context("Pod with spark role label equal to executor", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
				Labels: map[string]string{
					common.LabelSparkAppName: "test-app",
					common.LabelSparkRole:    common.SparkRoleExecutor,
				},
			},
		}

		It("Should return true", func() {
			Expect(util.IsExecutorPod(pod)).To(BeTrue())
		})
	})
})

var _ = Describe("GetAppName", func() {
	Context("Pod without labels", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
			},
		}

		It("Should return empty application name", func() {
			Expect(util.GetAppName(pod)).To(BeEmpty())
		})
	})

	Context("Pod without app name label", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
				Labels: map[string]string{
					common.LabelScheduledSparkAppName: "true",
				},
			},
		}

		It("Should return empty application name", func() {
			Expect(util.GetAppName(pod)).To(BeEmpty())
		})
	})

	Context("Pod with app name label", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
				Labels: map[string]string{
					common.LabelSparkAppName: "test-app",
				},
			},
		}

		It("Should return the application name", func() {
			Expect(util.GetAppName(pod)).To(Equal("test-app"))
		})
	})
})

var _ = Describe("GetSparkApplicationID", func() {
	Context("Pod without labels", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
			},
		}

		It("Should return empty application ID", func() {
			Expect(util.GetSparkApplicationID(pod)).To(BeEmpty())
		})
	})

	Context("Pod without spark app selector label", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
				Labels: map[string]string{
					common.LabelSparkAppName: "test-app",
				},
			},
		}

		It("Should return empty application ID", func() {
			Expect(util.GetSparkApplicationID(pod)).To(BeEmpty())
		})
	})

	Context("Pod with spark app selector label", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
				Labels: map[string]string{
					common.LabelSparkAppName:             "test-app",
					common.LabelSparkApplicationSelector: "test-app-id",
				},
			},
		}

		It("Should return the application ID", func() {
			Expect(util.GetSparkApplicationID(pod)).To(Equal("test-app-id"))
		})
	})
})

var _ = Describe("GetSparkExecutorID", func() {
	Context("Pod without labels", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
			},
		}

		It("Should return empty executor ID", func() {
			Expect(util.GetSparkExecutorID(pod)).To(BeEmpty())
		})
	})

	Context("Pod without executor ID label", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
				Labels: map[string]string{
					common.LabelSparkAppName: "test-app",
				},
			},
		}

		It("Should return empty executor ID", func() {
			Expect(util.GetSparkExecutorID(pod)).To(BeEmpty())
		})
	})

	Context("Pod with executor ID label", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
				Labels: map[string]string{
					common.LabelSparkAppName:    "test-app",
					common.LabelSparkExecutorID: "1",
				},
			},
		}

		It("Should return the executor ID", func() {
			Expect(util.GetSparkExecutorID(pod)).To(Equal("1"))
		})
	})
})

var _ = Describe("GetConnName", func() {
	Context("Pod without labels", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
			},
		}

		It("Should return empty connection name", func() {
			Expect(util.GetConnName(pod)).To(BeEmpty())
		})
	})

	Context("Pod with labels but without connect name", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
				Labels: map[string]string{
					common.LabelSparkAppName: "test-app",
				},
			},
		}

		It("Should return empty connection name", func() {
			Expect(util.GetConnName(pod)).To(BeEmpty())
		})
	})

	Context("Pod with spark connect name label", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
				Labels: map[string]string{
					common.LabelSparkAppName:     "test-app",
					common.LabelSparkConnectName: "conn-1",
				},
			},
		}

		It("Should return the connection name", func() {
			Expect(util.GetConnName(pod)).To(Equal("conn-1"))
		})
	})
})

var _ = Describe("IsPodReady", func() {
	It("Returns true when PodReady condition is true", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "ready-pod",
				Namespace: "test-namespace",
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{
						Type:   corev1.PodReady,
						Status: corev1.ConditionTrue,
					},
				},
			},
		}

		Expect(util.IsPodReady(pod)).To(BeTrue())
	})

	It("Returns false when PodReady condition is false", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "not-ready-pod",
				Namespace: "test-namespace",
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{
						Type:   corev1.PodReady,
						Status: corev1.ConditionFalse,
					},
				},
			},
		}

		Expect(util.IsPodReady(pod)).To(BeFalse())
	})

	It("Returns false when PodReady condition is missing", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "no-condition-pod",
				Namespace: "test-namespace",
			},
		}

		Expect(util.IsPodReady(pod)).To(BeFalse())
	})
})

var _ = Describe("ShouldProcessPodUpdate", func() {
	It("Returns true when pod phase changes", func() {
		oldPod := newTestPod(corev1.PodPending, nil, nil, nil)
		newPod := newTestPod(corev1.PodRunning, nil, nil, nil)

		Expect(util.ShouldProcessPodUpdate(oldPod, newPod)).To(BeTrue())
	})

	It("Returns false for non-driver pods when phase is unchanged", func() {
		labels := map[string]string{
			common.LabelSparkRole: common.SparkRoleExecutor,
		}
		oldPod := newTestPod(corev1.PodPending, labels, nil, nil)
		newPod := newTestPod(corev1.PodPending, labels, nil, nil)

		Expect(util.ShouldProcessPodUpdate(oldPod, newPod)).To(BeFalse())
	})

	It("Returns true when driver failure reason changes", func() {
		labels := map[string]string{
			common.LabelSparkRole: common.SparkRoleDriver,
		}
		oldStatuses := []corev1.ContainerStatus{
			newWaitingContainerStatus(""),
		}
		newStatuses := []corev1.ContainerStatus{
			newWaitingContainerStatus(common.ReasonImagePullBackOff),
		}
		oldPod := newTestPod(corev1.PodPending, labels, oldStatuses, nil)
		newPod := newTestPod(corev1.PodPending, labels, newStatuses, nil)

		Expect(util.ShouldProcessPodUpdate(oldPod, newPod)).To(BeTrue())
	})

	It("Returns true when driver pod becomes unschedulable", func() {
		labels := map[string]string{
			common.LabelSparkRole: common.SparkRoleDriver,
		}
		oldPod := newTestPod(corev1.PodPending, labels, nil, nil)
		newPod := newTestPod(corev1.PodPending, labels, nil, []corev1.PodCondition{
			{
				Type:   corev1.PodScheduled,
				Status: corev1.ConditionFalse,
				Reason: corev1.PodReasonUnschedulable,
			},
		})

		Expect(util.ShouldProcessPodUpdate(oldPod, newPod)).To(BeTrue())
	})

	It("Returns false when driver pod status does not change", func() {
		labels := map[string]string{
			common.LabelSparkRole: common.SparkRoleDriver,
		}
		oldPod := newTestPod(corev1.PodPending, labels, nil, nil)
		newPod := newTestPod(corev1.PodPending, labels, nil, nil)

		Expect(util.ShouldProcessPodUpdate(oldPod, newPod)).To(BeFalse())
	})
})

var _ = Describe("DriverFailureReasonChanged", func() {
	It("Returns true when failure reason changes to image pull backoff", func() {
		oldPod := newTestPod(corev1.PodPending, nil, []corev1.ContainerStatus{
			newWaitingContainerStatus(common.ReasonErrImagePull),
		}, nil)
		newPod := newTestPod(corev1.PodPending, nil, []corev1.ContainerStatus{
			newWaitingContainerStatus(common.ReasonImagePullBackOff),
		}, nil)

		Expect(util.DriverFailureReasonChanged(oldPod, newPod)).To(BeTrue())
	})

	It("Returns false when new failure reason is empty", func() {
		oldPod := newTestPod(corev1.PodPending, nil, []corev1.ContainerStatus{
			newWaitingContainerStatus(common.ReasonImagePullBackOff),
		}, nil)
		newPod := newTestPod(corev1.PodPending, nil, []corev1.ContainerStatus{
			newWaitingContainerStatus(""),
		}, nil)

		Expect(util.DriverFailureReasonChanged(oldPod, newPod)).To(BeFalse())
	})

	It("Returns false when failure reason is unchanged", func() {
		oldPod := newTestPod(corev1.PodPending, nil, []corev1.ContainerStatus{
			newWaitingContainerStatus(common.ReasonImagePullBackOff),
		}, nil)
		newPod := newTestPod(corev1.PodPending, nil, []corev1.ContainerStatus{
			newWaitingContainerStatus(common.ReasonImagePullBackOff),
		}, nil)

		Expect(util.DriverFailureReasonChanged(oldPod, newPod)).To(BeFalse())
	})
})

var _ = Describe("GetPodFailureReason", func() {
	It("Returns ImagePullBackOff when container is waiting with that reason", func() {
		pod := newTestPod(corev1.PodPending, nil, []corev1.ContainerStatus{
			newWaitingContainerStatus(common.ReasonImagePullBackOff),
		}, nil)

		Expect(util.GetPodFailureReason(pod)).To(Equal(common.ReasonImagePullBackOff))
	})

	It("Returns ErrImagePull when container is waiting with that reason", func() {
		pod := newTestPod(corev1.PodPending, nil, []corev1.ContainerStatus{
			newWaitingContainerStatus(common.ReasonErrImagePull),
		}, nil)

		Expect(util.GetPodFailureReason(pod)).To(Equal(common.ReasonErrImagePull))
	})

	It("Returns empty string when waiting reason is not a failure", func() {
		pod := newTestPod(corev1.PodPending, nil, []corev1.ContainerStatus{
			newWaitingContainerStatus("CrashLoopBackOff"),
		}, nil)

		Expect(util.GetPodFailureReason(pod)).To(BeEmpty())
	})
})

var _ = Describe("IsPodUnschedulable", func() {
	It("Returns true when PodScheduled condition indicates unschedulable", func() {
		pod := newTestPod(corev1.PodPending, nil, nil, []corev1.PodCondition{
			{
				Type:   corev1.PodScheduled,
				Status: corev1.ConditionFalse,
				Reason: corev1.PodReasonUnschedulable,
			},
		})

		Expect(util.IsPodUnschedulable(pod)).To(BeTrue())
	})

	It("Returns false when PodScheduled condition reason differs", func() {
		pod := newTestPod(corev1.PodPending, nil, nil, []corev1.PodCondition{
			{
				Type:   corev1.PodScheduled,
				Status: corev1.ConditionFalse,
				Reason: "SomeOtherReason",
			},
		})

		Expect(util.IsPodUnschedulable(pod)).To(BeFalse())
	})
})

var _ = Describe("BecameUnschedulable", func() {
	It("Returns true when new pod becomes unschedulable", func() {
		oldPod := newTestPod(corev1.PodPending, nil, nil, nil)
		newPod := newTestPod(corev1.PodPending, nil, nil, []corev1.PodCondition{
			{
				Type:   corev1.PodScheduled,
				Status: corev1.ConditionFalse,
				Reason: corev1.PodReasonUnschedulable,
			},
		})

		Expect(util.BecameUnschedulable(oldPod, newPod)).To(BeTrue())
	})

	It("Returns false when both pods are unschedulable", func() {
		cond := []corev1.PodCondition{
			{
				Type:   corev1.PodScheduled,
				Status: corev1.ConditionFalse,
				Reason: corev1.PodReasonUnschedulable,
			},
		}
		oldPod := newTestPod(corev1.PodPending, nil, nil, cond)
		newPod := newTestPod(corev1.PodPending, nil, nil, cond)

		Expect(util.BecameUnschedulable(oldPod, newPod)).To(BeFalse())
	})
})

func newTestPod(phase corev1.PodPhase, labels map[string]string, statuses []corev1.ContainerStatus, conditions []corev1.PodCondition) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "test-namespace",
			Labels:    labels,
		},
		Status: corev1.PodStatus{
			Phase:             phase,
			ContainerStatuses: statuses,
			Conditions:        conditions,
		},
	}
}

func newWaitingContainerStatus(reason string) corev1.ContainerStatus {
	var waiting *corev1.ContainerStateWaiting
	if reason != "" {
		waiting = &corev1.ContainerStateWaiting{Reason: reason}
	} else {
		waiting = &corev1.ContainerStateWaiting{}
	}

	return corev1.ContainerStatus{
		State: corev1.ContainerState{
			Waiting: waiting,
		},
	}
}
