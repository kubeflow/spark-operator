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

package scheduler

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

type fakeScheduler struct {
	name string
}

func (f *fakeScheduler) Name() string                                      { return f.name }
func (f *fakeScheduler) ShouldSchedule(app *v1beta2.SparkApplication) bool { return true }
func (f *fakeScheduler) Schedule(app *v1beta2.SparkApplication) error      { return nil }
func (f *fakeScheduler) Cleanup(app *v1beta2.SparkApplication) error       { return nil }

func newFakeFactory(name string) Factory {
	return func(config Config) (Interface, error) {
		return &fakeScheduler{name: name}, nil
	}
}

func newTestRegistry() *Registry {
	return &Registry{factories: make(map[string]Factory)}
}

var _ = Describe("Registry", func() {
	It("registers and returns a scheduler", func() {
		r := newTestRegistry()

		Expect(r.Register("foo", newFakeFactory("foo"))).To(Succeed())

		sched, err := r.GetScheduler("foo", nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(sched.Name()).To(Equal("foo"))
	})

	It("errors on a duplicate name and keeps the original factory", func() {
		r := newTestRegistry()

		Expect(r.Register("foo", newFakeFactory("foo"))).To(Succeed())
		Expect(r.Register("foo", newFakeFactory("foo-again"))).To(HaveOccurred())

		sched, err := r.GetScheduler("foo", nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(sched.Name()).To(Equal("foo"))
	})

	It("errors when the scheduler is not found", func() {
		r := newTestRegistry()

		_, err := r.GetScheduler("missing", nil)
		Expect(err).To(HaveOccurred())
	})

	It("lists the registered scheduler names", func() {
		r := newTestRegistry()

		Expect(r.GetRegisteredSchedulerNames()).To(BeEmpty())

		Expect(r.Register("foo", newFakeFactory("foo"))).To(Succeed())
		Expect(r.Register("bar", newFakeFactory("bar"))).To(Succeed())

		Expect(r.GetRegisteredSchedulerNames()).To(ConsistOf("foo", "bar"))
	})

	It("returns the same registry instance from GetRegistry", func() {
		first := GetRegistry()
		Expect(first).NotTo(BeNil())

		second := GetRegistry()
		Expect(second).To(BeIdenticalTo(first))
	})
})
