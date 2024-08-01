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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/util/yaml"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/util"
)

const (
	PollInterval = 1 * time.Second
	WaitTimeout  = 300 * time.Second
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
			cancelCtx, cancelFunc := context.WithTimeout(ctx, WaitTimeout)
			defer cancelFunc()
			Expect(wait.PollUntilContextCancel(cancelCtx, PollInterval, true, func(ctx context.Context) (done bool, err error) {
				err = k8sClient.Get(ctx, key, app)
				if app.Status.AppState.State == v1beta2.ApplicationStateCompleted {
					return true, nil
				}
				return false, err
			})).NotTo(HaveOccurred())

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

		It("Should complete successfully", func() {
			By("Waiting for SparkApplication to complete")
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			cancelCtx, cancelFunc := context.WithTimeout(ctx, WaitTimeout)
			defer cancelFunc()
			Expect(wait.PollUntilContextCancel(cancelCtx, PollInterval, true, func(ctx context.Context) (done bool, err error) {
				err = k8sClient.Get(ctx, key, app)
				if app.Status.AppState.State == v1beta2.ApplicationStateCompleted {
					return true, nil
				}
				return false, err
			})).NotTo(HaveOccurred())

			By("Checking out driver logs")
			driverPodName := util.GetDriverPodName(app)
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
			cancelCtx, cancelFunc := context.WithTimeout(ctx, WaitTimeout)
			defer cancelFunc()
			Expect(wait.PollUntilContextCancel(cancelCtx, PollInterval, true, func(ctx context.Context) (done bool, err error) {
				err = k8sClient.Get(ctx, key, app)
				if app.Status.AppState.State == v1beta2.ApplicationStateCompleted {
					return true, nil
				}
				return false, err
			})).NotTo(HaveOccurred())

			By("Checking out driver logs")
			driverPodName := util.GetDriverPodName(app)
			bytes, err := clientset.CoreV1().Pods(app.Namespace).GetLogs(driverPodName, &corev1.PodLogOptions{}).Do(ctx).Raw()
			Expect(err).NotTo(HaveOccurred())
			Expect(bytes).NotTo(BeEmpty())
			Expect(strings.Contains(string(bytes), "Pi is roughly 3")).To(BeTrue())
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
			cancelCtx, cancelFunc := context.WithTimeout(ctx, WaitTimeout)
			defer cancelFunc()
			Expect(wait.PollUntilContextCancel(cancelCtx, PollInterval, true, func(ctx context.Context) (done bool, err error) {
				err = k8sClient.Get(ctx, key, app)
				if app.Status.AppState.State == v1beta2.ApplicationStateCompleted {
					return true, nil
				}
				return false, err
			})).NotTo(HaveOccurred())

			By("Checking out driver logs")
			driverPodName := util.GetDriverPodName(app)
			bytes, err := clientset.CoreV1().Pods(app.Namespace).GetLogs(driverPodName, &corev1.PodLogOptions{}).Do(ctx).Raw()
			Expect(err).NotTo(HaveOccurred())
			Expect(bytes).NotTo(BeEmpty())
			Expect(strings.Contains(string(bytes), "Pi is roughly 3")).To(BeTrue())
		})
	})
})
