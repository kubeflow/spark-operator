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

package e2e_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/common"
	"github.com/kubeflow/spark-operator/pkg/util"
)

var _ = Describe("Example SparkApplication", func() {
	Context("spark-pi", func() {
		ctx := context.Background()
		path := filepath.Join("..", "..", "examples", "spark-pi.yaml")
		app := &v1beta2.SparkApplication{}

		BeforeEach(func() {
			By("Parsing SparkApplication from file")
			file, err := os.Open(path)
			Expect(err).NotTo(HaveOccurred())
			Expect(file).NotTo(BeNil())

			decoder := yaml.NewYAMLOrJSONDecoder(file, 100)
			Expect(decoder).NotTo(BeNil())
			Expect(decoder.Decode(app)).NotTo(HaveOccurred())

			By("Creating SparkApplication")
			Expect(k8sClient.Create(ctx, app)).To(Succeed())
		})

		AfterEach(func() {
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

			By("Deleting SparkApplication")
			Expect(k8sClient.Delete(ctx, app)).To(Succeed())
		})

		It("should complete successfully", func() {
			By("Waiting for SparkApplication to complete")
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			Expect(waitForSparkApplicationCompleted(ctx, key)).NotTo(HaveOccurred())

			By("Checking out driver logs")
			driverPodName := util.GetDriverPodName(app)
			bytes, err := clientset.CoreV1().Pods(app.Namespace).GetLogs(driverPodName, &corev1.PodLogOptions{}).Do(ctx).Raw()
			Expect(err).NotTo(HaveOccurred())
			Expect(bytes).NotTo(BeEmpty())
			Expect(strings.Contains(string(bytes), "Pi is roughly 3")).To(BeTrue())
		})
	})

	Context("spark-pi-configmap", func() {
		ctx := context.Background()
		path := filepath.Join("..", "..", "examples", "spark-pi-configmap.yaml")
		app := &v1beta2.SparkApplication{}

		BeforeEach(func() {
			By("Parsing SparkApplication from file")
			file, err := os.Open(path)
			Expect(err).NotTo(HaveOccurred())
			Expect(file).NotTo(BeNil())

			decoder := yaml.NewYAMLOrJSONDecoder(file, 100)
			Expect(decoder).NotTo(BeNil())
			Expect(decoder.Decode(app)).NotTo(HaveOccurred())

			By("Creating ConfigMap")
			for _, volume := range app.Spec.Volumes {
				if volume.ConfigMap != nil {
					configMap := &corev1.ConfigMap{
						ObjectMeta: metav1.ObjectMeta{
							Name:      volume.ConfigMap.Name,
							Namespace: app.Namespace,
						},
					}
					Expect(k8sClient.Create(ctx, configMap)).To(Succeed())
				}
			}

			By("Creating SparkApplication")
			Expect(k8sClient.Create(ctx, app)).To(Succeed())
		})

		AfterEach(func() {
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

			volumes := app.Spec.Volumes
			By("Deleting SparkApplication")
			Expect(k8sClient.Delete(ctx, app)).To(Succeed())

			By("Deleting ConfigMap")
			for _, volume := range volumes {
				if volume.ConfigMap != nil {
					configMap := &corev1.ConfigMap{
						ObjectMeta: metav1.ObjectMeta{
							Name:      volume.ConfigMap.Name,
							Namespace: app.Namespace,
						},
					}
					Expect(k8sClient.Delete(ctx, configMap)).To(Succeed())
				}
			}
		})

		It("Should complete successfully with configmap mounted", func() {
			By("Waiting for SparkApplication to complete")
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			Expect(waitForSparkApplicationCompleted(ctx, key)).NotTo(HaveOccurred())

			By("Checking out whether volumes are mounted to driver pod")
			driverPodName := util.GetDriverPodName(app)
			driverPodKey := types.NamespacedName{Namespace: app.Namespace, Name: driverPodName}
			driverPod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, driverPodKey, driverPod)).NotTo(HaveOccurred())
			hasVolumes := false
			hasVolumeMounts := false
			for _, volume := range app.Spec.Volumes {
				for _, podVolume := range driverPod.Spec.Volumes {
					if volume.Name == podVolume.Name {
						hasVolumes = true
						break
					}
				}
			}
			for _, volumeMount := range app.Spec.Driver.VolumeMounts {
				for _, container := range driverPod.Spec.Containers {
					if container.Name != common.SparkDriverContainerName {
						continue
					}
					for _, podVolumeMount := range container.VolumeMounts {
						if equality.Semantic.DeepEqual(volumeMount, podVolumeMount) {
							hasVolumeMounts = true
							break
						}
					}
				}
			}
			Expect(hasVolumes).To(BeTrue())
			Expect(hasVolumeMounts).To(BeTrue())

			By("Checking out driver logs")
			bytes, err := clientset.CoreV1().Pods(app.Namespace).GetLogs(driverPodName, &corev1.PodLogOptions{}).Do(ctx).Raw()
			Expect(err).NotTo(HaveOccurred())
			Expect(bytes).NotTo(BeEmpty())
			Expect(strings.Contains(string(bytes), "Pi is roughly 3")).To(BeTrue())
		})
	})

	Context("spark-pi-custom-resource", func() {
		ctx := context.Background()
		path := filepath.Join("..", "..", "examples", "spark-pi-custom-resource.yaml")
		app := &v1beta2.SparkApplication{}

		BeforeEach(func() {
			By("Parsing SparkApplication from file")
			file, err := os.Open(path)
			Expect(err).NotTo(HaveOccurred())
			Expect(file).NotTo(BeNil())

			decoder := yaml.NewYAMLOrJSONDecoder(file, 100)
			Expect(decoder).NotTo(BeNil())
			Expect(decoder.Decode(app)).NotTo(HaveOccurred())

			By("Creating SparkApplication")
			Expect(k8sClient.Create(ctx, app)).To(Succeed())
		})

		AfterEach(func() {
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

			By("Deleting SparkApplication")
			Expect(k8sClient.Delete(ctx, app)).To(Succeed())
		})

		It("Should complete successfully", func() {
			By("Waiting for SparkApplication to complete")
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			Expect(waitForSparkApplicationCompleted(ctx, key)).NotTo(HaveOccurred())

			By("Checking out whether resource requests and limits of driver pod are set")
			driverPodName := util.GetDriverPodName(app)
			driverPodKey := types.NamespacedName{Namespace: app.Namespace, Name: driverPodName}
			driverPod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, driverPodKey, driverPod)).NotTo(HaveOccurred())
			for _, container := range driverPod.Spec.Containers {
				if container.Name != common.SparkDriverContainerName {
					continue
				}
				if app.Spec.Driver.CoreRequest != nil {
					Expect(container.Resources.Requests.Cpu().Equal(resource.MustParse(*app.Spec.Driver.CoreRequest))).To(BeTrue())
				}
				if app.Spec.Driver.CoreLimit != nil {
					Expect(container.Resources.Limits.Cpu().Equal(resource.MustParse(*app.Spec.Driver.CoreLimit))).To(BeTrue())
				}
				Expect(container.Resources.Requests.Memory).NotTo(BeNil())
				Expect(container.Resources.Limits.Memory).NotTo(BeNil())
			}

			By("Checking out driver logs")
			bytes, err := clientset.CoreV1().Pods(app.Namespace).GetLogs(driverPodName, &corev1.PodLogOptions{}).Do(ctx).Raw()
			Expect(err).NotTo(HaveOccurred())
			Expect(bytes).NotTo(BeEmpty())
			Expect(strings.Contains(string(bytes), "Pi is roughly 3")).To(BeTrue())
		})
	})

	Context("fail-submission", func() {
		ctx := context.Background()
		path := filepath.Join("bad_examples", "fail-submission.yaml")
		app := &v1beta2.SparkApplication{}

		BeforeEach(func() {
			By("Parsing SparkApplication from file")
			file, err := os.Open(path)
			Expect(err).NotTo(HaveOccurred())
			Expect(file).NotTo(BeNil())

			decoder := yaml.NewYAMLOrJSONDecoder(file, 100)
			Expect(decoder).NotTo(BeNil())
			Expect(decoder.Decode(app)).NotTo(HaveOccurred())

			By("Creating SparkApplication")
			Expect(k8sClient.Create(ctx, app)).To(Succeed())
		})

		AfterEach(func() {
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

			By("Deleting SparkApplication")
			Expect(k8sClient.Delete(ctx, app)).To(Succeed())
		})

		It("Fails submission and retries until retries are exhausted", func() {
			By("Waiting for SparkApplication to terminate")
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			apps, polling_err := collectSparkApplicationsUntilTermination(ctx, key)
			Expect(polling_err).To(HaveOccurred())

			By("Should eventually fail")
			finalApp := apps[len(apps)-1]
			Expect(finalApp.Status.AppState.State).To(Equal(v1beta2.ApplicationStateFailed))
			Expect(finalApp.Status.AppState.ErrorMessage).To(ContainSubstring("failed to run spark-submit"))
			Expect(finalApp.Status.SubmissionAttempts).To(Equal(*app.Spec.RestartPolicy.OnSubmissionFailureRetries + 1))

			By("Only valid statuses appear in other apps")
			validStatuses := []v1beta2.ApplicationStateType{
				v1beta2.ApplicationStateNew,
				v1beta2.ApplicationStateFailedSubmission,
			}
			for _, app := range apps[:len(apps)-1] {
				Expect(validStatuses).To(ContainElement(app.Status.AppState.State))
			}

			By("Checking driver does not exist")
			driverPodName := util.GetDriverPodName(app)
			driverPodKey := types.NamespacedName{Namespace: app.Namespace, Name: driverPodName}
			err := k8sClient.Get(ctx, driverPodKey, &corev1.Pod{})
			Expect(errors.IsNotFound(err)).To(BeTrue())
		})
	})

	Context("application-fails", func() {
		ctx := context.Background()
		path := filepath.Join("bad_examples", "fail-application.yaml")
		app := &v1beta2.SparkApplication{}

		BeforeEach(func() {
			By("Parsing SparkApplication from file")
			file, err := os.Open(path)
			Expect(err).NotTo(HaveOccurred())
			Expect(file).NotTo(BeNil())

			decoder := yaml.NewYAMLOrJSONDecoder(file, 100)
			Expect(decoder).NotTo(BeNil())
			Expect(decoder.Decode(app)).NotTo(HaveOccurred())

			By("Creating SparkApplication")
			Expect(k8sClient.Create(ctx, app)).To(Succeed())
		})

		AfterEach(func() {
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

			By("Deleting SparkApplication")
			Expect(k8sClient.Delete(ctx, app)).To(Succeed())
		})

		It("Application fails and retries until retries are exhausted", func() {
			By("Waiting for SparkApplication to terminate")
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			apps, polling_err := collectSparkApplicationsUntilTermination(ctx, key)
			Expect(polling_err).To(HaveOccurred())

			By("Should eventually fail")
			final_app := apps[len(apps)-1]
			Expect(final_app.Status.AppState.State).To(Equal(v1beta2.ApplicationStateFailed))
			Expect(final_app.Status.AppState.ErrorMessage).To(ContainSubstring("driver container failed"))
			Expect(final_app.Status.ExecutionAttempts).To(Equal(*app.Spec.RestartPolicy.OnFailureRetries + 1))

			By("Only valid statuses appear in other apps")
			validStatuses := []v1beta2.ApplicationStateType{
				v1beta2.ApplicationStateNew,
				v1beta2.ApplicationStateSubmitted,
				v1beta2.ApplicationStateRunning,
				v1beta2.ApplicationStateFailing,
				v1beta2.ApplicationStatePendingRerun,
			}
			for _, app := range apps[:len(apps)-1] {
				Expect(validStatuses).To(ContainElement(app.Status.AppState.State))
			}

			By("Checking out driver logs")
			driverPodName := util.GetDriverPodName(app)
			bytes, err := clientset.CoreV1().Pods(app.Namespace).GetLogs(driverPodName, &corev1.PodLogOptions{}).Do(ctx).Raw()
			Expect(err).NotTo(HaveOccurred())
			Expect(bytes).NotTo(BeEmpty())
			Expect(strings.Contains(string(bytes), "NoSuchFileException")).To(BeTrue())
		})
	})

	Context("spark-pi-python", func() {
		ctx := context.Background()
		path := filepath.Join("..", "..", "examples", "spark-pi-python.yaml")
		app := &v1beta2.SparkApplication{}

		BeforeEach(func() {
			By("Parsing SparkApplication from file")
			file, err := os.Open(path)
			Expect(err).NotTo(HaveOccurred())
			Expect(file).NotTo(BeNil())

			decoder := yaml.NewYAMLOrJSONDecoder(file, 100)
			Expect(decoder).NotTo(BeNil())
			Expect(decoder.Decode(app)).NotTo(HaveOccurred())

			By("Creating SparkApplication")
			Expect(k8sClient.Create(ctx, app)).To(Succeed())
		})

		AfterEach(func() {
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

			By("Deleting SparkApplication")
			Expect(k8sClient.Delete(ctx, app)).To(Succeed())
		})

		It("Should complete successfully", func() {
			By("Waiting for SparkApplication to complete")
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			Expect(waitForSparkApplicationCompleted(ctx, key)).NotTo(HaveOccurred())

			By("Checking out driver logs")
			driverPodName := util.GetDriverPodName(app)
			bytes, err := clientset.CoreV1().Pods(app.Namespace).GetLogs(driverPodName, &corev1.PodLogOptions{}).Do(ctx).Raw()
			Expect(err).NotTo(HaveOccurred())
			Expect(bytes).NotTo(BeEmpty())
			Expect(strings.Contains(string(bytes), "Pi is roughly 3")).To(BeTrue())
		})
	})
})
