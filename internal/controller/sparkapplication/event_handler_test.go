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

package sparkapplication_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/internal/controller/sparkapplication"
	"github.com/kubeflow/spark-operator/pkg/common"
)

func stringPtr(s string) *string {
	return &s
}

// mockQueue implements workqueue.RateLimitingInterface for testing
type mockQueue struct {
	items []interface{}
}

func newMockQueue() *mockQueue {
	return &mockQueue{items: make([]interface{}, 0)}
}

func (m *mockQueue) Add(item interface{}) {
	m.items = append(m.items, item)
}

func (m *mockQueue) Len() int {
	return len(m.items)
}

func (m *mockQueue) Get() (item interface{}, shutdown bool) {
	if len(m.items) == 0 {
		return nil, false
	}
	item = m.items[0]
	m.items = m.items[1:]
	return item, false
}

func (m *mockQueue) Done(item interface{}) {}

func (m *mockQueue) ShutDown() {}

func (m *mockQueue) ShutDownWithDrain() {}

func (m *mockQueue) ShuttingDown() bool {
	return false
}

func (m *mockQueue) AddAfter(item interface{}, duration time.Duration) {
	m.Add(item)
}

func (m *mockQueue) AddRateLimited(item interface{}) {
	m.Add(item)
}

func (m *mockQueue) Forget(item interface{}) {}

func (m *mockQueue) NumRequeues(item interface{}) int {
	return 0
}

var _ = Describe("SparkPodEventHandler", func() {
	const (
		appName      = "test-event-handler-app"
		appNamespace = "default"
		driverName   = "test-event-handler-app-driver"
		submissionID1 = "submission-id-1"
		submissionID2 = "submission-id-2"
	)

	var (
		ctx     context.Context
		handler *sparkapplication.SparkPodEventHandler
		queue   workqueue.RateLimitingInterface
		app     *v1beta2.SparkApplication
		pod     *corev1.Pod
	)

	BeforeEach(func() {
		ctx = context.Background()
		handler = sparkapplication.NewSparkPodEventHandler(k8sClient, nil)
		// Use a mock queue for testing
		queue = newMockQueue()

		// Create a test SparkApplication
		cores := int32(1)
		app = &v1beta2.SparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				Name:      appName,
				Namespace: appNamespace,
			},
			Spec: v1beta2.SparkApplicationSpec{
				Type:                v1beta2.SparkApplicationTypeScala,
				Mode:                v1beta2.DeployModeCluster,
				MainApplicationFile: stringPtr("local:///test.jar"),
				Driver: v1beta2.DriverSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Cores: &cores,
					},
				},
			},
			Status: v1beta2.SparkApplicationStatus{
				AppState: v1beta2.ApplicationState{
					State: v1beta2.ApplicationStateNew,
				},
				SubmissionID: submissionID1,
			},
		}
		Expect(k8sClient.Create(ctx, app)).To(Succeed())

		// Create a test driver pod
		pod = &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      driverName,
				Namespace: appNamespace,
				Labels: map[string]string{
					common.LabelSparkAppName: appName,
					common.LabelSubmissionID: submissionID1,
					common.LabelSparkRole:    common.SparkRoleDriver,
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  "spark-driver",
						Image: "spark:latest",
					},
				},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodPending,
			},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
	})

	AfterEach(func() {
		// Clean up
		Expect(k8sClient.Delete(ctx, pod)).To(Succeed())
		Expect(k8sClient.Delete(ctx, app)).To(Succeed())
	})

	Context("When SubmissionID matches", func() {
		It("should enqueue the event for SUBMITTED state", func() {
			// Update app to SUBMITTED state with matching SubmissionID
			Eventually(func() error {
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: appName, Namespace: appNamespace}, app); err != nil {
					return err
				}
				app.Status.AppState.State = v1beta2.ApplicationStateSubmitted
				app.Status.SubmissionID = submissionID1
				return k8sClient.Status().Update(ctx, app)
			}).Should(Succeed())

			// Update pod to trigger event
			Eventually(func() error {
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: driverName, Namespace: appNamespace}, pod); err != nil {
					return err
				}
				pod.Status.Phase = corev1.PodRunning
				return k8sClient.Status().Update(ctx, pod)
			}).Should(Succeed())

			// Simulate pod update event
			handler.Update(ctx, event.UpdateEvent{
				ObjectOld: &corev1.Pod{
					ObjectMeta: pod.ObjectMeta,
					Status:     corev1.PodStatus{Phase: corev1.PodPending},
				},
				ObjectNew: pod,
			}, queue)

			// Verify event was enqueued
			Expect(queue.Len()).To(Equal(1))
			item, shutdown := queue.Get()
			Expect(shutdown).To(BeFalse())
			req := item.(ctrl.Request)
			Expect(req.Name).To(Equal(appName))
			Expect(req.Namespace).To(Equal(appNamespace))
		})
	})

	Context("When SubmissionID does not match", func() {
		It("should drop the event for SUBMITTED state", func() {
			// Update app to SUBMITTED state with different SubmissionID
			Eventually(func() error {
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: appName, Namespace: appNamespace}, app); err != nil {
					return err
				}
				app.Status.AppState.State = v1beta2.ApplicationStateSubmitted
				app.Status.SubmissionID = submissionID2 // Different from pod's submissionID1
				return k8sClient.Status().Update(ctx, app)
			}).Should(Succeed())

			// Update pod to trigger event
			Eventually(func() error {
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: driverName, Namespace: appNamespace}, pod); err != nil {
					return err
				}
				pod.Status.Phase = corev1.PodRunning
				return k8sClient.Status().Update(ctx, pod)
			}).Should(Succeed())

			// Simulate pod update event
			handler.Update(ctx, event.UpdateEvent{
				ObjectOld: &corev1.Pod{
					ObjectMeta: pod.ObjectMeta,
					Status:     corev1.PodStatus{Phase: corev1.PodPending},
				},
				ObjectNew: pod,
			}, queue)

			// Verify event was NOT enqueued (dropped)
			Expect(queue.Len()).To(Equal(0))
		})

		It("should drop the event for RUNNING state", func() {
			// Update app to RUNNING state with different SubmissionID
			Eventually(func() error {
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: appName, Namespace: appNamespace}, app); err != nil {
					return err
				}
				app.Status.AppState.State = v1beta2.ApplicationStateRunning
				app.Status.SubmissionID = submissionID2
				return k8sClient.Status().Update(ctx, app)
			}).Should(Succeed())

			// Update pod to trigger event
			Eventually(func() error {
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: driverName, Namespace: appNamespace}, pod); err != nil {
					return err
				}
				pod.Status.Phase = corev1.PodRunning
				return k8sClient.Status().Update(ctx, pod)
			}).Should(Succeed())

			// Simulate pod update event
			handler.Update(ctx, event.UpdateEvent{
				ObjectOld: &corev1.Pod{
					ObjectMeta: pod.ObjectMeta,
					Status:     corev1.PodStatus{Phase: corev1.PodPending},
				},
				ObjectNew: pod,
			}, queue)

			// Verify event was NOT enqueued
			Expect(queue.Len()).To(Equal(0))
		})
	})

	Context("When SubmissionID does not match but app is in PENDING_RERUN state", func() {
		It("should enqueue the event to allow pod adoption", func() {
			// Update app to PENDING_RERUN state with different/empty SubmissionID
			Eventually(func() error {
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: appName, Namespace: appNamespace}, app); err != nil {
					return err
				}
				app.Status.AppState.State = v1beta2.ApplicationStatePendingRerun
				app.Status.SubmissionID = "" // Empty or different SubmissionID
				return k8sClient.Status().Update(ctx, app)
			}).Should(Succeed())

			// Update pod to trigger event
			Eventually(func() error {
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: driverName, Namespace: appNamespace}, pod); err != nil {
					return err
				}
				pod.Status.Phase = corev1.PodRunning
				return k8sClient.Status().Update(ctx, pod)
			}).Should(Succeed())

			// Simulate pod update event
			handler.Update(ctx, event.UpdateEvent{
				ObjectOld: &corev1.Pod{
					ObjectMeta: pod.ObjectMeta,
					Status:     corev1.PodStatus{Phase: corev1.PodPending},
				},
				ObjectNew: pod,
			}, queue)

			// Verify event WAS enqueued (allowed for adoption)
			Expect(queue.Len()).To(Equal(1))
			item, shutdown := queue.Get()
			Expect(shutdown).To(BeFalse())
			req := item.(ctrl.Request)
			Expect(req.Name).To(Equal(appName))
			Expect(req.Namespace).To(Equal(appNamespace))
		})

		It("should enqueue the event even with completely different SubmissionID", func() {
			// Update app to PENDING_RERUN state with explicitly different SubmissionID
			Eventually(func() error {
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: appName, Namespace: appNamespace}, app); err != nil {
					return err
				}
				app.Status.AppState.State = v1beta2.ApplicationStatePendingRerun
				app.Status.SubmissionID = submissionID2 // Different from pod's submissionID1
				return k8sClient.Status().Update(ctx, app)
			}).Should(Succeed())

			// Update pod to trigger event
			Eventually(func() error {
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: driverName, Namespace: appNamespace}, pod); err != nil {
					return err
				}
				pod.Status.Phase = corev1.PodRunning
				return k8sClient.Status().Update(ctx, pod)
			}).Should(Succeed())

			// Simulate pod update event
			handler.Update(ctx, event.UpdateEvent{
				ObjectOld: &corev1.Pod{
					ObjectMeta: pod.ObjectMeta,
					Status:     corev1.PodStatus{Phase: corev1.PodPending},
				},
				ObjectNew: pod,
			}, queue)

			// Verify event WAS enqueued (allowed for adoption)
			Expect(queue.Len()).To(Equal(1))
			item, shutdown := queue.Get()
			Expect(shutdown).To(BeFalse())
			req := item.(ctrl.Request)
			Expect(req.Name).To(Equal(appName))
			Expect(req.Namespace).To(Equal(appNamespace))
		})
	})

	Context("When pod has no SubmissionID label", func() {
		It("should enqueue the event regardless of app state", func() {
			// Create pod without SubmissionID label
			podNoSubmissionID := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "driver-no-submission-id",
					Namespace: appNamespace,
					Labels: map[string]string{
						common.LabelSparkAppName: appName,
						common.LabelSparkRole:    common.SparkRoleDriver,
						// No SubmissionID label
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "spark-driver",
							Image: "spark:latest",
						},
					},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodPending,
				},
			}
			Expect(k8sClient.Create(ctx, podNoSubmissionID)).To(Succeed())
			defer k8sClient.Delete(ctx, podNoSubmissionID)

			// Update pod to trigger event
			Eventually(func() error {
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: podNoSubmissionID.Name, Namespace: appNamespace}, podNoSubmissionID); err != nil {
					return err
				}
				podNoSubmissionID.Status.Phase = corev1.PodRunning
				return k8sClient.Status().Update(ctx, podNoSubmissionID)
			}).Should(Succeed())

			// Simulate pod update event
			handler.Update(ctx, event.UpdateEvent{
				ObjectOld: &corev1.Pod{
					ObjectMeta: podNoSubmissionID.ObjectMeta,
					Status:     corev1.PodStatus{Phase: corev1.PodPending},
				},
				ObjectNew: podNoSubmissionID,
			}, queue)

			// Verify event was enqueued (no SubmissionID check)
			Expect(queue.Len()).To(Equal(1))
		})
	})
})
