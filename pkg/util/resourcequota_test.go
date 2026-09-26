/*
Copyright 2026 The Kubeflow authors.

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
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

var _ = Describe("SumResourceList", func() {
	DescribeTable("Should sum the resource lists",
		func(lists []corev1.ResourceList, expected corev1.ResourceList) {
			total := util.SumResourceList(lists)
			Expect(total).NotTo(BeNil())
			Expect(total).To(HaveLen(len(expected)))
			for name, want := range expected {
				got, ok := total[name]
				Expect(ok).To(BeTrue(), "missing resource %s", name)
				Expect(got.Cmp(want)).To(BeZero(), "resource %s: got %s, want %s", name, got.String(), want.String())
			}
		},
		Entry("nil input",
			[]corev1.ResourceList(nil),
			corev1.ResourceList{},
		),
		Entry("empty input",
			[]corev1.ResourceList{},
			corev1.ResourceList{},
		),
		Entry("nil and empty lists",
			[]corev1.ResourceList{nil, {}},
			corev1.ResourceList{},
		),
		Entry("single list",
			[]corev1.ResourceList{
				{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("1Gi"),
				},
			},
			corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
		),
		Entry("same resource across lists",
			[]corev1.ResourceList{
				{corev1.ResourceCPU: resource.MustParse("1")},
				{corev1.ResourceCPU: resource.MustParse("500m")},
			},
			corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("1500m"),
			},
		),
		Entry("same resource with mixed units",
			[]corev1.ResourceList{
				{corev1.ResourceMemory: resource.MustParse("1Gi")},
				{corev1.ResourceMemory: resource.MustParse("512Mi")},
			},
			corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("1536Mi"),
			},
		),
		Entry("different resources across lists",
			[]corev1.ResourceList{
				{corev1.ResourceCPU: resource.MustParse("2")},
				{corev1.ResourceMemory: resource.MustParse("4Gi")},
			},
			corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("2"),
				corev1.ResourceMemory: resource.MustParse("4Gi"),
			},
		),
		Entry("overlapping and distinct resources across many lists",
			[]corev1.ResourceList{
				{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("1Gi"),
				},
				{
					corev1.ResourceCPU:    resource.MustParse("2"),
					corev1.ResourceMemory: resource.MustParse("2Gi"),
				},
				{
					corev1.ResourceCPU:              resource.MustParse("250m"),
					corev1.ResourceEphemeralStorage: resource.MustParse("10Gi"),
				},
			},
			corev1.ResourceList{
				corev1.ResourceCPU:              resource.MustParse("3250m"),
				corev1.ResourceMemory:           resource.MustParse("3Gi"),
				corev1.ResourceEphemeralStorage: resource.MustParse("10Gi"),
			},
		),
	)

	It("Should not mutate the input lists", func() {
		first := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}
		second := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}

		total := util.SumResourceList([]corev1.ResourceList{first, second})
		Expect(total.Cpu().Cmp(resource.MustParse("3"))).To(BeZero())

		// Modifying the result must not leak back into the inputs either.
		cpu := total[corev1.ResourceCPU]
		cpu.Add(resource.MustParse("10"))
		total[corev1.ResourceCPU] = cpu

		Expect(first.Cpu().Cmp(resource.MustParse("1"))).To(BeZero())
		Expect(second.Cpu().Cmp(resource.MustParse("2"))).To(BeZero())
	})
})
