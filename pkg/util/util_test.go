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
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeflow/spark-operator/pkg/common"
	"github.com/kubeflow/spark-operator/pkg/util"
)

var _ = Describe("GetMasterURL", func() {
	BeforeEach(func() {
		os.Setenv(common.EnvKubernetesServiceHost, "127.0.0.1")
		os.Setenv(common.EnvKubernetesServicePort, "443")
	})

	AfterEach(func() {
		os.Unsetenv(common.EnvKubernetesServiceHost)
		os.Unsetenv(common.EnvKubernetesServicePort)
	})

	Context("IPv4 address", func() {
		It("Should return correct master URL without error", func() {
			masterURL, err := util.GetMasterURL()
			Expect(masterURL).To(Equal("k8s://https://127.0.0.1:443"))
			Expect(err).NotTo(HaveOccurred())
		})
	})
})

var _ = Describe("GetMasterURL", func() {
	BeforeEach(func() {
		os.Setenv(common.EnvKubernetesServiceHost, "::1")
		os.Setenv(common.EnvKubernetesServicePort, "443")
	})

	AfterEach(func() {
		os.Unsetenv(common.EnvKubernetesServiceHost)
		os.Unsetenv(common.EnvKubernetesServicePort)
	})

	Context("IPv6 address", func() {
		It("Should return correct master URL without error", func() {
			masterURL, err := util.GetMasterURL()
			Expect(masterURL).To(Equal("k8s://https://[::1]:443"))
			Expect(err).NotTo(HaveOccurred())
		})
	})
})

var _ = Describe("ContainsString", func() {
	slice := []string{"a", "b", "c"}

	Context("When the string is in the slice", func() {
		It("Should return true", func() {
			Expect(util.ContainsString(slice, "b")).To(BeTrue())
		})
	})

	Context("When the string is not in the slice", func() {
		It("Should return false", func() {
			Expect(util.ContainsString(slice, "d")).To(BeFalse())
		})
	})
})

var _ = Describe("RemoveString", func() {
	Context("When the string is in the slice", func() {
		slice := []string{"a", "b", "c"}
		expected := []string{"a", "c"}

		It("Should remove the string", func() {
			Expect(util.RemoveString(slice, "b")).To(Equal(expected))
		})
	})

	Context("When the string is not in the slice", func() {
		slice := []string{"a", "b", "c"}
		expected := []string{"a", "b", "c"}

		It("Should do nothing", func() {
			Expect(util.RemoveString(slice, "d")).To(Equal(expected))
		})
	})
})

var _ = Describe("BoolPtr", func() {
	It("Should return a pointer to the given bool value", func() {
		b := true
		Expect(util.BoolPtr(b)).To(Equal(&b))
	})
})

var _ = Describe("Int32Ptr", func() {
	It("Should return a pointer to the given int32 value", func() {
		i := int32(42)
		Expect(util.Int32Ptr(i)).To(Equal(&i))
	})
})

var _ = Describe("Int64Ptr", func() {
	It("Should return a pointer to the given int64 value", func() {
		i := int64(42)
		Expect(util.Int64Ptr(i)).To(Equal(&i))
	})
})

var _ = Describe("StringPtr", func() {
	It("Should return a pointer to the given string value", func() {
		s := "hello"
		Expect(util.StringPtr(s)).To(Equal(&s))
	})
})

var _ = Describe("CompareSemanticVersions", func() {
	It("Should return 0 if the two versions are equal", func() {
		Expect(util.CompareSemanticVersion("1.2.3", "1.2.3"))
		Expect(util.CompareSemanticVersion("1.2.3", "v1.2.3")).To(Equal(0))
	})

	It("Should return -1 if the first version is less than the second version", func() {
		Expect(util.CompareSemanticVersion("2.3.4", "2.4.5")).To(Equal(-1))
		Expect(util.CompareSemanticVersion("2.4.5", "2.4.8")).To(Equal(-1))
		Expect(util.CompareSemanticVersion("2.4.8", "3.5.2")).To(Equal(-1))
	})

	It("Should return +1 if the first version is greater than the second version", func() {
		Expect(util.CompareSemanticVersion("2.4.5", "2.3.4")).To(Equal(1))
		Expect(util.CompareSemanticVersion("2.4.8", "2.4.5")).To(Equal(1))
		Expect(util.CompareSemanticVersion("3.5.2", "2.4.8")).To(Equal(1))
	})
})

var _ = Describe("WriteObjectToFile", func() {
	It("Should write the object to the file", func() {
		podTemplate := &corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-pod",
				Labels: map[string]string{
					"key1": "value1",
					"key2": "value2",
				},
				Annotations: map[string]string{
					"key3": "value3",
					"key4": "value4",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  "test-container",
						Image: "test-image",
					},
				},
			},
		}

		expected := `metadata:
  annotations:
    key3: value3
    key4: value4
  creationTimestamp: null
  labels:
    key1: value1
    key2: value2
  name: test-pod
spec:
  containers:
  - image: test-image
    name: test-container
    resources: {}
`
		file := "pod-template.yaml"
		Expect(util.WriteObjectToFile(podTemplate, file)).To(Succeed())

		data, err := os.ReadFile(file)
		Expect(err).NotTo(HaveOccurred())
		actual := string(data)

		Expect(actual).To(Equal(expected))
		Expect(os.Remove(file)).NotTo(HaveOccurred())
	})
})

var _ = Describe("ConvertJavaMemoryStringToK8sMemoryString", func() {
	It("Should return a memory converted in expected K8s unit", func() {

		unitMap := map[string]string{
			"k":  "Ki",
			"kb": "Ki",
			"m":  "Mi",
			"mb": "Mi",
			"g":  "Gi",
			"gb": "Gi",
			"t":  "Ti",
			"tb": "Ti",
			"p":  "Pi",
			"pb": "Pi",
			"Ki": "Ki",
			"Mi": "Mi",
			"Gi": "Gi",
			"Ti": "Ti",
			"Pi": "Pi",
		}

		for unit, response := range unitMap {
			Expect(util.ConvertJavaMemoryStringToK8sMemoryString(unit)).To(Equal(response))
		}

	})
})
