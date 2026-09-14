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
	"time"

	"github.com/kubeflow/spark-operator/v2/pkg/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("NewRateLimiter", func() {
	It("caps the exponential backoff at maxDelay and resets after Forget", func() {
		limiter := util.NewRateLimiter[string](1000, 1000, 100*time.Millisecond)

		first := limiter.When("app")
		Expect(first).To(BeNumerically("<", 100*time.Millisecond))
		Expect(first).To(BeNumerically(">", 0))

		for i := 0; i < 9; i++ {
			limiter.When("app")
		}

		Expect(limiter.When("app")).To(Equal(100 * time.Millisecond))
		Expect(limiter.NumRequeues("app")).To(Equal(11))

		limiter.Forget("app")

		Expect(limiter.NumRequeues("app")).To(Equal(0))
		Expect(limiter.When("app")).To(Equal(first))
	})
})
