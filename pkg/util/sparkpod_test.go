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

	"github.com/kubeflow/spark-operator/pkg/common"
	"github.com/kubeflow/spark-operator/pkg/util"
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
