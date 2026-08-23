/*
Copyright 2025 The Kubeflow authors.

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

package util

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
)

var _ = Describe("GetContainerByNameOrFirst", func() {
	It("returns nil when input container list is empty", func() {
		Expect(GetContainerByNameOrFirst(nil, "spark")).To(BeNil())
	})

	It("returns the named container when it exists", func() {
		containers := []corev1.Container{
			{Name: "sidecar", Image: "busybox"},
			{Name: "spark", Image: "apache/spark"},
		}

		container := GetContainerByNameOrFirst(containers, "spark")

		Expect(container).NotTo(BeNil())
		Expect(container.Name).To(Equal("spark"))
		Expect(container.Image).To(Equal("apache/spark"))
	})

	It("returns the first container when the named container is absent", func() {
		containers := []corev1.Container{
			{Name: "first", Image: "first-image"},
			{Name: "second", Image: "second-image"},
		}

		container := GetContainerByNameOrFirst(containers, "spark")

		Expect(container).NotTo(BeNil())
		Expect(container.Name).To(Equal("first"))
		Expect(container.Image).To(Equal("first-image"))
	})

	It("returns a pointer to the original slice element", func() {
		containers := []corev1.Container{
			{Name: "first"},
			{Name: "spark"},
		}

		container := GetContainerByNameOrFirst(containers, "spark")
		Expect(container).NotTo(BeNil())

		container.Image = "updated-image"

		Expect(containers[1].Image).To(Equal("updated-image"))
	})
})
