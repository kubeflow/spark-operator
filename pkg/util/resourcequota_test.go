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

var _ = Describe("SumResourceList", func() {
	It("returns an empty list for no input", func() {
		Expect(util.SumResourceList(nil)).To(BeEmpty())
	})

	It("sums a single list", func() {
		list := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}

		total := util.SumResourceList([]corev1.ResourceList{list})
		cpu := total[corev1.ResourceCPU]

		Expect(cpu.Value()).To(Equal(int64(1)))
	})

	It("sums overlapping resource names across multiple lists", func() {
		lists := []corev1.ResourceList{
			{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")},
			{corev1.ResourceCPU: resource.MustParse("2")},
		}

		total := util.SumResourceList(lists)
		cpu, mem := total[corev1.ResourceCPU], total[corev1.ResourceMemory]
		wantMem := resource.MustParse("1Gi")

		Expect(cpu.Value()).To(Equal(int64(3)))
		Expect(mem.Value()).To(Equal(wantMem.Value()))
	})

	It("does not mutate the input lists", func() {
		list := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}
		lists := []corev1.ResourceList{list, {corev1.ResourceCPU: resource.MustParse("2")}}

		util.SumResourceList(lists)
		cpu := list[corev1.ResourceCPU]

		Expect(cpu.Value()).To(Equal(int64(1)))
	})
})
