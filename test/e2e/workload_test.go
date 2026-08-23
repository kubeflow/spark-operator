/*
Copyright 2026 The Kubeflow authors.

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

package e2e_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	schedulingv1alpha2 "k8s.io/api/scheduling/v1alpha2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientretry "k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

const (
	workloadE2EDriverNodeLabel   = "workload-e2e-driver"
	workloadE2EExecutorNodeLabel = "workload-e2e-executor"
)

var _ = Describe("Kubernetes native workload scheduler", Label("workload"), func() {
	ctx := context.Background()
	path := filepath.Join("..", "..", "examples", "spark-pi-workload.yaml")
	app := &v1beta2.SparkApplication{}
	driverNodeName := ""

	BeforeEach(func() {
		if !workloadE2EEnabled() {
			Skip("requires a cluster serving scheduling.k8s.io/v1alpha2 with GenericWorkload and GangScheduling enabled")
		}
		Expect(deployMethod).To(Equal("helm"), "the workload E2E job enables the backend through Helm values")

		app = loadSparkApplication(path, "spark-pi-workload-e2e")
		app.Spec.Arguments = []string{"100000000"}
		app.Spec.Driver.NodeSelector = map[string]string{workloadE2EDriverNodeLabel: "true"}
		app.Spec.Executor.NodeSelector = map[string]string{workloadE2EExecutorNodeLabel: "true"}
		app.Spec.Executor.Affinity = &corev1.Affinity{
			PodAntiAffinity: &corev1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
					{
						LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
							common.LabelSparkAppName: app.Name,
							common.LabelSparkRole:    common.SparkRoleExecutor,
						}},
						TopologyKey: corev1.LabelHostname,
					},
				},
			},
		}

		driverNodes := &corev1.NodeList{}
		Expect(k8sClient.List(ctx, driverNodes, client.MatchingLabels{
			workloadE2EDriverNodeLabel: "true",
		})).To(Succeed())
		Expect(driverNodes.Items).To(HaveLen(1))
		driverNodeName = driverNodes.Items[0].Name

		By("Creating the workload-scheduled SparkApplication")
		Expect(k8sClient.Create(ctx, app)).To(Succeed())
	})

	AfterEach(func() {
		if app.Name != "" {
			current := &v1beta2.SparkApplication{}
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			if err := k8sClient.Get(ctx, key, current); err == nil {
				Expect(k8sClient.Delete(ctx, current)).To(Succeed())
			} else {
				Expect(apierrors.IsNotFound(err)).To(BeTrue())
			}
		}

		if driverNodeName != "" {
			Expect(clientretry.RetryOnConflict(clientretry.DefaultRetry, func() error {
				node := &corev1.Node{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: driverNodeName}, node); err != nil {
					return err
				}
				delete(node.Labels, workloadE2EExecutorNodeLabel)
				return k8sClient.Update(ctx, node)
			})).To(Succeed())
		}
	})

	It("creates owned scheduling resources and binds the executor gang atomically", func() {
		appKey := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
		currentApp := &v1beta2.SparkApplication{}
		Eventually(func(g Gomega) string {
			g.Expect(k8sClient.Get(ctx, appKey, currentApp)).To(Succeed())
			return currentApp.Status.SubmissionID
		}).WithTimeout(WaitTimeout).WithPolling(PollInterval).ShouldNot(BeEmpty())

		workload := &schedulingv1alpha2.Workload{}
		workloadKey := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(ctx, workloadKey, workload)).To(Succeed())
		}).WithTimeout(WaitTimeout).WithPolling(PollInterval).Should(Succeed())
		Expect(metav1.IsControlledBy(workload, currentApp)).To(BeTrue())
		Expect(workload.Spec.ControllerRef).NotTo(BeNil())
		Expect(workload.Spec.ControllerRef.Name).To(Equal(app.Name))
		Expect(workload.Spec.PodGroupTemplates).To(HaveLen(1))
		Expect(workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang).NotTo(BeNil())
		Expect(workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount).To(Equal(int32(2)))

		podGroupName := fmt.Sprintf("%s-%s", app.Name, currentApp.Status.SubmissionID)
		podGroup := &schedulingv1alpha2.PodGroup{}
		podGroupKey := types.NamespacedName{Namespace: app.Namespace, Name: podGroupName}
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(ctx, podGroupKey, podGroup)).To(Succeed())
		}).WithTimeout(WaitTimeout).WithPolling(PollInterval).Should(Succeed())
		Expect(metav1.IsControlledBy(podGroup, currentApp)).To(BeTrue())
		Expect(podGroup.Spec.PodGroupTemplateRef).NotTo(BeNil())
		Expect(podGroup.Spec.PodGroupTemplateRef.Workload).NotTo(BeNil())
		Expect(podGroup.Spec.PodGroupTemplateRef.Workload.WorkloadName).To(Equal(workload.Name))
		Expect(podGroup.Spec.SchedulingPolicy.Gang).NotTo(BeNil())
		Expect(podGroup.Spec.SchedulingPolicy.Gang.MinCount).To(Equal(int32(2)))

		driverPod := &corev1.Pod{}
		driverPodKey := types.NamespacedName{Namespace: app.Namespace, Name: util.GetDriverPodName(currentApp)}
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(ctx, driverPodKey, driverPod)).To(Succeed())
			g.Expect(driverPod.Spec.NodeName).NotTo(BeEmpty())
		}).WithTimeout(WaitTimeout).WithPolling(PollInterval).Should(Succeed())
		Expect(driverPod.Spec.SchedulingGroup).To(BeNil(), "the driver must stay outside the executor gang")

		executorPods := &corev1.PodList{}
		executorLabels := client.MatchingLabels{
			common.LabelSparkAppName: app.Name,
			common.LabelSparkRole:    common.SparkRoleExecutor,
		}
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.List(ctx, executorPods, client.InNamespace(app.Namespace), executorLabels)).To(Succeed())
			g.Expect(executorPods.Items).To(HaveLen(2))
		}).WithTimeout(WaitTimeout).WithPolling(PollInterval).Should(Succeed())
		Eventually(func(g Gomega) {
			currentPodGroup := &schedulingv1alpha2.PodGroup{}
			g.Expect(k8sClient.Get(ctx, podGroupKey, currentPodGroup)).To(Succeed())
			condition := apimeta.FindStatusCondition(currentPodGroup.Status.Conditions, schedulingv1alpha2.PodGroupScheduled)
			g.Expect(condition).NotTo(BeNil())
			g.Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			g.Expect(condition.Reason).To(Equal(schedulingv1alpha2.PodGroupReasonUnschedulable))
		}).WithTimeout(WaitTimeout).WithPolling(PollInterval).Should(Succeed())

		By("Confirming no executor binds while only one anti-affinity placement is available")
		Consistently(func(g Gomega) {
			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(ctx, pods, client.InNamespace(app.Namespace), executorLabels)).To(Succeed())
			g.Expect(pods.Items).To(HaveLen(2))
			for i := range pods.Items {
				g.Expect(pods.Items[i].Spec.NodeName).To(BeEmpty())
				g.Expect(pods.Items[i].Spec.SchedulingGroup).NotTo(BeNil())
				g.Expect(pods.Items[i].Spec.SchedulingGroup.PodGroupName).NotTo(BeNil())
				g.Expect(*pods.Items[i].Spec.SchedulingGroup.PodGroupName).To(Equal(podGroupName))
			}
		}).WithTimeout(10 * time.Second).WithPolling(PollInterval).Should(Succeed())

		By("Making a second executor placement available")
		Expect(clientretry.RetryOnConflict(clientretry.DefaultRetry, func() error {
			node := &corev1.Node{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: driverNodeName}, node); err != nil {
				return err
			}
			if node.Labels == nil {
				node.Labels = map[string]string{}
			}
			node.Labels[workloadE2EExecutorNodeLabel] = "true"
			return k8sClient.Update(ctx, node)
		})).To(Succeed())

		By("Confirming the complete executor gang binds")
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.List(ctx, executorPods, client.InNamespace(app.Namespace), executorLabels)).To(Succeed())
			g.Expect(executorPods.Items).To(HaveLen(2))
			nodes := map[string]struct{}{}
			for i := range executorPods.Items {
				g.Expect(executorPods.Items[i].Spec.NodeName).NotTo(BeEmpty())
				nodes[executorPods.Items[i].Spec.NodeName] = struct{}{}
			}
			g.Expect(nodes).To(HaveLen(2), "required pod anti-affinity should place the executors on distinct nodes")
		}).WithTimeout(WaitTimeout).WithPolling(PollInterval).Should(Succeed())
		Eventually(func(g Gomega) {
			currentPodGroup := &schedulingv1alpha2.PodGroup{}
			g.Expect(k8sClient.Get(ctx, podGroupKey, currentPodGroup)).To(Succeed())
			condition := apimeta.FindStatusCondition(currentPodGroup.Status.Conditions, schedulingv1alpha2.PodGroupScheduled)
			g.Expect(condition).NotTo(BeNil())
			g.Expect(condition.Status).To(Equal(metav1.ConditionTrue))
		}).WithTimeout(WaitTimeout).WithPolling(PollInterval).Should(Succeed())

		placements := make([]string, 0, len(executorPods.Items))
		for i := range executorPods.Items {
			Expect(executorPods.Items[i].Spec.SchedulingGroup).NotTo(BeNil())
			Expect(executorPods.Items[i].Spec.SchedulingGroup.PodGroupName).NotTo(BeNil())
			Expect(*executorPods.Items[i].Spec.SchedulingGroup.PodGroupName).To(Equal(podGroupName))
			placements = append(placements, fmt.Sprintf("%s=%s", executorPods.Items[i].Name, executorPods.Items[i].Spec.NodeName))
		}
		sort.Strings(placements)
		workloadOwner := metav1.GetControllerOf(workload)
		podGroupOwner := metav1.GetControllerOf(podGroup)
		Expect(workloadOwner).NotTo(BeNil())
		Expect(podGroupOwner).NotTo(BeNil())
		GinkgoWriter.Printf(
			"Verified Workload %s (owner=%s), PodGroup %s (owner=%s, minCount=%d), executorPodGroup=%s, placements=%v\n",
			workload.Name,
			workloadOwner.Name,
			podGroup.Name,
			podGroupOwner.Name,
			podGroup.Spec.SchedulingPolicy.Gang.MinCount,
			podGroupName,
			placements,
		)
	})
})
