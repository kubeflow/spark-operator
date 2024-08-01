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
