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
	"errors"
	"os"
	"syscall"

	"github.com/kubeflow/spark-operator/v2/pkg/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("InterruptHandler", func() {
	It("runs notify functions exactly once on Close even when called repeatedly", func() {
		count := 0
		handler := util.NewInterruptHandler(nil, func() { count++ }, func() { count++ })

		handler.Close()
		handler.Close()

		Expect(count).To(Equal(2))
	})

	It("runs notify functions then the final handler exactly once on Signal", func() {
		var notified []string
		var signaled os.Signal
		handler := util.NewInterruptHandler(
			func(s os.Signal) { signaled = s },
			func() { notified = append(notified, "a") },
			func() { notified = append(notified, "b") },
		)

		handler.Signal(syscall.SIGTERM)
		handler.Signal(syscall.SIGINT)

		Expect(notified).To(Equal([]string{"a", "b"}))
		Expect(signaled).To(Equal(syscall.SIGTERM))
	})

	It("returns the error from the critical section and still runs notify", func() {
		count := 0
		handler := util.NewInterruptHandler(nil, func() { count++ })

		err := handler.Run(func() error { return errors.New("boom") })

		Expect(err).To(MatchError("boom"))
		Expect(count).To(Equal(1))
	})
})
