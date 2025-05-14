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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/common"
	"github.com/kubeflow/spark-operator/pkg/util"
)

var _ = Describe("GetDriverPodName", func() {
	Context("SparkApplication without driver pod name field and driver pod name conf", func() {
		app := &v1beta2.SparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
			},
		}

		It("Should return the default driver pod name", func() {
			Expect(util.GetDriverPodName(app)).To(Equal("test-app-driver"))
		})
	})

	Context("SparkApplication with only driver pod name field", func() {
		driverPodName := "test-app-driver-pod"
		app := &v1beta2.SparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
			},
			Spec: v1beta2.SparkApplicationSpec{
				Driver: v1beta2.DriverSpec{
					PodName: &driverPodName,
				},
			},
		}

		It("Should return the driver pod name from driver spec", func() {
			Expect(util.GetDriverPodName(app)).To(Equal(driverPodName))
		})
	})

	Context("SparkApplication with only driver pod name conf", func() {
		driverPodName := "test-app-driver-pod"
		app := &v1beta2.SparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
			},
			Spec: v1beta2.SparkApplicationSpec{
				SparkConf: map[string]string{
					common.SparkKubernetesDriverPodName: driverPodName,
				},
			},
		}

		It("Should return the driver name from spark conf", func() {
			Expect(util.GetDriverPodName(app)).To(Equal(driverPodName))
		})
	})

	Context("SparkApplication with both driver pod name field and driver pod name conf", func() {
		driverPodName1 := "test-app-driver-1"
		driverPodName2 := "test-app-driver-2"
		app := &v1beta2.SparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
			},
			Spec: v1beta2.SparkApplicationSpec{
				SparkConf: map[string]string{
					common.SparkKubernetesDriverPodName: driverPodName1,
				},
				Driver: v1beta2.DriverSpec{
					PodName: &driverPodName2,
				},
			},
		}

		It("Should return the driver pod name from driver spec", func() {
			Expect(util.GetDriverPodName(app)).To(Equal(driverPodName2))
		})
	})
})

var _ = Describe("GetApplicationState", func() {
	Context("SparkApplication with completed state", func() {
		app := &v1beta2.SparkApplication{
			Status: v1beta2.SparkApplicationStatus{
				AppState: v1beta2.ApplicationState{
					State: v1beta2.ApplicationStateCompleted,
				},
			},
		}

		It("Should return completed state", func() {
			Expect(util.GetApplicationState(app)).To(Equal(v1beta2.ApplicationStateCompleted))
		})
	})
})

var _ = Describe("IsExpired", func() {
	Context("SparkApplication without TTL", func() {
		app := &v1beta2.SparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
			},
		}

		It("Should return false", func() {
			Expect(util.IsExpired(app)).To(BeFalse())
		})
	})

	Context("SparkApplication not terminated with TTL", func() {
		app := &v1beta2.SparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
			},
			Spec: v1beta2.SparkApplicationSpec{
				TimeToLiveSeconds: util.Int64Ptr(3600),
			},
		}

		It("Should return false", func() {
			Expect(util.IsExpired(app)).To(BeFalse())
		})
	})

	Context("SparkApplication terminated with TTL not expired", func() {
		now := time.Now()
		app := &v1beta2.SparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
			},
			Spec: v1beta2.SparkApplicationSpec{
				TimeToLiveSeconds: util.Int64Ptr(3600),
			},
			Status: v1beta2.SparkApplicationStatus{
				TerminationTime: metav1.NewTime(now.Add(-30 * time.Minute)),
			},
		}

		It("Should return false", func() {
			Expect(util.IsExpired(app)).To(BeFalse())
		})
	})

	Context("SparkApplication terminated with TTL expired", func() {
		now := time.Now()
		app := &v1beta2.SparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
			},
			Spec: v1beta2.SparkApplicationSpec{
				TimeToLiveSeconds: util.Int64Ptr(3600),
			},
			Status: v1beta2.SparkApplicationStatus{
				TerminationTime: metav1.NewTime(now.Add(-2 * time.Hour)),
			},
		}

		It("Should return true", func() {
			Expect(util.IsExpired(app)).To(BeTrue())
		})
	})
})

var _ = Describe("IsDriverRunning", func() {
	Context("SparkApplication with completed state", func() {
		app := &v1beta2.SparkApplication{
			Status: v1beta2.SparkApplicationStatus{
				AppState: v1beta2.ApplicationState{
					State: v1beta2.ApplicationStateCompleted,
				},
			},
		}

		It("Should return false", func() {
			Expect(util.IsDriverRunning(app)).To(BeFalse())
		})
	})

	Context("SparkApplication with running state", func() {
		app := &v1beta2.SparkApplication{
			Status: v1beta2.SparkApplicationStatus{
				AppState: v1beta2.ApplicationState{
					State: v1beta2.ApplicationStateRunning,
				},
			},
		}

		It("Should return true", func() {
			Expect(util.IsDriverRunning(app)).To(BeTrue())
		})
	})
})

var _ = Describe("GetLocalVolumes", func() {
	Context("SparkApplication with local volumes", func() {
		volume1 := corev1.Volume{
			Name: "local-volume",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/tmp",
				},
			},
		}

		volume2 := corev1.Volume{
			Name: "spark-local-dir-1",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/mnt/spark-local-dir-1",
				},
			},
		}

		volume3 := corev1.Volume{
			Name: "spark-local-dir-2",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/mnt/spark-local-dir-2",
				},
			},
		}

		app := &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Volumes: []corev1.Volume{
					volume1,
					volume2,
					volume3,
				},
			},
		}

		It("Should return volumes with the correct prefix", func() {
			volumes := util.GetLocalVolumes(app)
			expected := map[string]corev1.Volume{
				volume2.Name: volume2,
				volume3.Name: volume3,
			}
			Expect(volumes).To(Equal(expected))
		})
	})
})

var _ = Describe("GetDefaultUIServiceName", func() {
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "test-namespace",
		},
	}

	It("Should return the default UI service name", func() {
		Expect(util.GetDefaultUIServiceName(app)).To(Equal("test-app-ui-svc"))
	})

	appWithLongName := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app-with-a-long-name-that-would-be-over-63-characters",
			Namespace: "test-namespace",
		},
	}

	It("Should truncate the app name so the service name is below 63 characters", func() {
		serviceName := util.GetDefaultUIServiceName(appWithLongName)
		Expect(len(serviceName)).To(BeNumerically("<=", 63))
		Expect(serviceName).To(HavePrefix(appWithLongName.Name[:47]))
		Expect(serviceName).To(HaveSuffix("-ui-svc"))
	})
})

var _ = Describe("GetDefaultUIIngressName", func() {
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "test-namespace",
		},
	}

	It("Should return the default UI ingress name", func() {
		Expect(util.GetDefaultUIIngressName(app)).To(Equal("test-app-ui-ingress"))
	})

	appWithLongName := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app-with-a-long-name-that-would-be-over-63-characters",
			Namespace: "test-namespace",
		},
	}

	It("Should truncate the app name so the ingress name is below 63 characters", func() {
		serviceName := util.GetDefaultUIIngressName(appWithLongName)
		Expect(len(serviceName)).To(BeNumerically("<=", 63))
		Expect(serviceName).To(HavePrefix(appWithLongName.Name[:42]))
		Expect(serviceName).To(HaveSuffix("-ui-ingress"))
	})
})

var _ = Describe("IsDriverTerminated", func() {
	It("Should check whether driver is terminated", func() {
		Expect(util.IsDriverTerminated(v1beta2.DriverStatePending)).To(BeFalse())
		Expect(util.IsDriverTerminated(v1beta2.DriverStateRunning)).To(BeFalse())
		Expect(util.IsDriverTerminated(v1beta2.DriverStateCompleted)).To(BeTrue())
		Expect(util.IsDriverTerminated(v1beta2.DriverStateFailed)).To(BeTrue())
		Expect(util.IsDriverTerminated(v1beta2.DriverStateUnknown)).To(BeFalse())
	})
})

var _ = Describe("IsExecutorTerminated", func() {
	It("Should check whether executor is terminated", func() {
		Expect(util.IsExecutorTerminated(v1beta2.ExecutorStatePending)).To(BeFalse())
		Expect(util.IsExecutorTerminated(v1beta2.ExecutorStateRunning)).To(BeFalse())
		Expect(util.IsExecutorTerminated(v1beta2.ExecutorStateCompleted)).To(BeTrue())
		Expect(util.IsExecutorTerminated(v1beta2.ExecutorStateFailed)).To(BeTrue())
		Expect(util.IsExecutorTerminated(v1beta2.ExecutorStateUnknown)).To(BeFalse())
	})
})

var _ = Describe("DriverStateToApplicationState", func() {
	It("Should convert driver state to application state correctly", func() {
		Expect(util.DriverStateToApplicationState(v1beta2.DriverStatePending)).To(Equal(v1beta2.ApplicationStateSubmitted))
		Expect(util.DriverStateToApplicationState(v1beta2.DriverStateRunning)).To(Equal(v1beta2.ApplicationStateRunning))
		Expect(util.DriverStateToApplicationState(v1beta2.DriverStateCompleted)).To(Equal(v1beta2.ApplicationStateSucceeding))
		Expect(util.DriverStateToApplicationState(v1beta2.DriverStateFailed)).To(Equal(v1beta2.ApplicationStateFailing))
		Expect(util.DriverStateToApplicationState(v1beta2.DriverStateUnknown)).To(Equal(v1beta2.ApplicationStateUnknown))
	})
})

var _ = Describe("Check if IsDynamicAllocationEnabled", func() {
	Context("when app.Spec.DynamicAllocation is True", func() {
		app := &v1beta2.SparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
			},
			Spec: v1beta2.SparkApplicationSpec{
				DynamicAllocation: &v1beta2.DynamicAllocation{
					Enabled: true,
				},
			},
		}
		It("Should return true", func() {
			Expect(util.IsDynamicAllocationEnabled(app)).To(BeTrue())
		})
	})
	Context("when app.Spec.DynamicAllocation is nil but True in app.Spec.SparkConf", func() {
		app := &v1beta2.SparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
			},
			Spec: v1beta2.SparkApplicationSpec{
				SparkConf: map[string]string{
					"spark.dynamicAllocation.enabled": "true",
				},
			},
		}
		It("Should return true", func() {
			Expect(util.IsDynamicAllocationEnabled(app)).To(BeTrue())
		})
	})
	Context("when app.Spec.DynamicAllocation is nil and not set in app.Spec.SparkConf", func() {
		app := &v1beta2.SparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
			},
		}
		It("Should return false", func() {
			Expect(util.IsDynamicAllocationEnabled(app)).To(BeFalse())
		})
	})
	Context("when app.Spec.DynamicAllocation is True but false in app.Spec.SparkConf", func() {
		app := &v1beta2.SparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "test-namespace",
			},
			Spec: v1beta2.SparkApplicationSpec{
				DynamicAllocation: &v1beta2.DynamicAllocation{
					Enabled: true,
				},
				SparkConf: map[string]string{
					"spark.dynamicAllocation.enabled": "false",
				},
			},
		}
		It("Should return true because app.Spec.DynamicAllocation configs will take precedence over "+
			"app.Spec.SparkConf configs", func() {
			Expect(util.IsDynamicAllocationEnabled(app)).To(BeTrue())
		})
	})
})
