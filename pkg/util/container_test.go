/*
Copyright The Kubeflow Authors.

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
	"github.com/kubeflow/spark-operator/v2/pkg/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

var _ = Describe("GetContainerByNameOrFirst", func() {
	It("returns nil when input container list is empty", func() {
		Expect(util.GetContainerByNameOrFirst(nil, "spark")).To(BeNil())
	})

	It("returns the named container when it exists", func() {
		containers := []corev1.Container{
			{Name: "sidecar", Image: "busybox"},
			{Name: "spark", Image: "apache/spark"},
		}

		container := util.GetContainerByNameOrFirst(containers, "spark")

		Expect(container).NotTo(BeNil())
		Expect(container.Name).To(Equal("spark"))
		Expect(container.Image).To(Equal("apache/spark"))
	})

	It("returns the first container when the named container is absent", func() {
		containers := []corev1.Container{
			{Name: "first", Image: "first-image"},
			{Name: "second", Image: "second-image"},
		}

		container := util.GetContainerByNameOrFirst(containers, "spark")

		Expect(container).NotTo(BeNil())
		Expect(container.Name).To(Equal("first"))
		Expect(container.Image).To(Equal("first-image"))
	})

	It("returns a pointer to the original slice element", func() {
		containers := []corev1.Container{
			{Name: "first"},
			{Name: "spark"},
		}

		container := util.GetContainerByNameOrFirst(containers, "spark")
		Expect(container).NotTo(BeNil())

		container.Image = "updated-image"

		Expect(containers[1].Image).To(Equal("updated-image"))
	})
})

var _ = Describe("SetGPUResources", func() {
	gpu := corev1.ResourceName("nvidia.com/gpu")

	It("initializes nil requests and limits", func() {
		container := &corev1.Container{Name: "spark"}

		util.SetGPUResources(container, "nvidia.com/gpu", 2)

		Expect(container.Resources.Requests.Name(gpu, resource.DecimalSI).Value()).To(Equal(int64(2)))
		Expect(container.Resources.Limits.Name(gpu, resource.DecimalSI).Value()).To(Equal(int64(2)))
	})

	It("preserves other resources and overrides an existing GPU value", func() {
		container := &corev1.Container{
			Name: "spark",
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), gpu: resource.MustParse("9")},
				Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi"), gpu: resource.MustParse("9")},
			},
		}

		util.SetGPUResources(container, "nvidia.com/gpu", 1)

		Expect(container.Resources.Requests).To(HaveKeyWithValue(corev1.ResourceCPU, resource.MustParse("1")))
		Expect(container.Resources.Limits).To(HaveKeyWithValue(corev1.ResourceMemory, resource.MustParse("1Gi")))
		Expect(container.Resources.Requests.Name(gpu, resource.DecimalSI).Value()).To(Equal(int64(1)))
		Expect(container.Resources.Limits.Name(gpu, resource.DecimalSI).Value()).To(Equal(int64(1)))
	})
})
