/*
Copyright The Kubeflow Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduler

import (
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

// fakeScheduler is a minimal Interface implementation used to exercise the registry.
type fakeScheduler struct {
	name string
}

func (f *fakeScheduler) Name() string {
	return f.name
}

func (f *fakeScheduler) ShouldSchedule(_ *v1beta2.SparkApplication) bool {
	return true
}

func (f *fakeScheduler) Schedule(_ *v1beta2.SparkApplication) error {
	return nil
}

func (f *fakeScheduler) Cleanup(_ *v1beta2.SparkApplication) error {
	return nil
}

func newFakeFactory(name string) Factory {
	return func(_ Config) (Interface, error) {
		return &fakeScheduler{name: name}, nil
	}
}

var _ = Describe("GetRegistry", func() {
	// GetRegistry operates on the package level singleton, so it is saved and
	// restored around each spec to keep the specs independent of each other.
	var original *Registry

	BeforeEach(func() {
		original = registry
		registry = nil
	})

	AfterEach(func() {
		registry = original
	})

	It("Should lazily create a registry with an initialized factory map", func() {
		r := GetRegistry()
		Expect(r).NotTo(BeNil())
		Expect(r.factories).NotTo(BeNil())
		Expect(r.factories).To(BeEmpty())
	})

	It("Should return the same instance on subsequent calls", func() {
		Expect(GetRegistry()).To(BeIdenticalTo(GetRegistry()))
	})
})

var _ = Describe("Registry.Register", func() {
	// Every spec below uses its own registry rather than the package level
	// singleton, so registrations never leak between specs.
	var r *Registry

	BeforeEach(func() {
		r = &Registry{factories: make(map[string]Factory)}
	})

	Context("Registering distinct scheduler names", func() {
		It("Should register every scheduler", func() {
			Expect(r.Register("volcano", newFakeFactory("volcano"))).To(Succeed())
			Expect(r.Register("yunikorn", newFakeFactory("yunikorn"))).To(Succeed())
			Expect(r.factories).To(HaveLen(2))
		})
	})

	Context("Registering a name that is already registered", func() {
		BeforeEach(func() {
			Expect(r.Register("volcano", newFakeFactory("volcano"))).To(Succeed())
		})

		It("Should return an error and keep the original registration", func() {
			err := r.Register("volcano", newFakeFactory("volcano"))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("already registered"))
			Expect(r.factories).To(HaveLen(1))
		})
	})
})

var _ = Describe("Registry.GetScheduler", func() {
	var r *Registry

	BeforeEach(func() {
		r = &Registry{factories: make(map[string]Factory)}
	})

	Context("Looking up a registered scheduler", func() {
		BeforeEach(func() {
			Expect(r.Register("volcano", newFakeFactory("volcano"))).To(Succeed())
		})

		It("Should return the scheduler built by the factory", func() {
			scheduler, err := r.GetScheduler("volcano", nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(scheduler).NotTo(BeNil())
			Expect(scheduler.Name()).To(Equal("volcano"))
		})
	})

	Context("Looking up a scheduler that is not registered", func() {
		BeforeEach(func() {
			Expect(r.Register("volcano", newFakeFactory("volcano"))).To(Succeed())
		})

		It("Should return an error", func() {
			scheduler, err := r.GetScheduler("yunikorn", nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("not found"))
			Expect(scheduler).To(BeNil())
		})
	})

	Context("Looking up a scheduler whose factory fails", func() {
		factoryErr := errors.New("failed to create scheduler")

		BeforeEach(func() {
			Expect(r.Register("broken", func(_ Config) (Interface, error) {
				return nil, factoryErr
			})).To(Succeed())
		})

		It("Should propagate the error returned by the factory", func() {
			scheduler, err := r.GetScheduler("broken", nil)
			Expect(err).To(MatchError(factoryErr))
			Expect(scheduler).To(BeNil())
		})
	})

	Context("Passing a configuration to the factory", func() {
		type testConfig struct {
			value string
		}

		var (
			want *testConfig
			got  Config
		)

		BeforeEach(func() {
			want = &testConfig{value: "expected"}
			got = nil

			Expect(r.Register("config-capture", func(config Config) (Interface, error) {
				got = config
				return &fakeScheduler{name: "config-capture"}, nil
			})).To(Succeed())
		})

		It("Should hand the configuration to the factory unchanged", func() {
			_, err := r.GetScheduler("config-capture", want)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(want))
		})
	})
})

var _ = Describe("Registry.GetRegisteredSchedulerNames", func() {
	var r *Registry

	BeforeEach(func() {
		r = &Registry{factories: make(map[string]Factory)}
	})

	Context("Registry without any registered scheduler", func() {
		It("Should return no names", func() {
			Expect(r.GetRegisteredSchedulerNames()).To(BeEmpty())
		})
	})

	Context("Registry with registered schedulers", func() {
		names := []string{"volcano", "yunikorn", "kube-scheduler"}

		BeforeEach(func() {
			for _, name := range names {
				Expect(r.Register(name, newFakeFactory(name))).To(Succeed())
			}
		})

		It("Should return every registered name", func() {
			Expect(r.GetRegisteredSchedulerNames()).To(ConsistOf(names))
		})
	})
})
