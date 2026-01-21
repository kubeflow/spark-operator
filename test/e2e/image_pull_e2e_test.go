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

package e2e_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kubeflow/spark-operator/v2/pkg/common"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/util/yaml"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

// This E2E test verifies that a SparkApplication transitions out of Submitted and into
// Failing/Failed when the driver image cannot be pulled (ErrImagePull/ImagePullBackOff).
var _ = Describe("image-pull failure", func() {
	ctx := context.Background()
	path := filepath.Join("bad_examples", "image-pull-failure.yaml")
	app := &v1beta2.SparkApplication{}

	BeforeEach(func() {
		By("Parsing SparkApplication from file")
		file, err := os.Open(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(file).NotTo(BeNil())

		decoder := yaml.NewYAMLOrJSONDecoder(file, 100)
		Expect(decoder).NotTo(BeNil())
		Expect(decoder.Decode(app)).NotTo(HaveOccurred())

		app.Spec.RestartPolicy.Type = v1beta2.RestartPolicyNever

		By("Creating SparkApplication")
		Expect(k8sClient.Create(ctx, app)).To(Succeed())
	})

	AfterEach(func() {
		key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
		Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

		By("Deleting SparkApplication")
		Expect(k8sClient.Delete(ctx, app)).To(Succeed())
	})

	It("should move to Failing then Failed with an image pull error message", func() {
		key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}

		By("Waiting for application to enter Failing due to image pull error")
		cancelCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()

		sawFailing := false
		err := wait.PollUntilContextCancel(cancelCtx, PollInterval, true, func(ctx context.Context) (bool, error) {
			got := &v1beta2.SparkApplication{}
			if e := k8sClient.Get(ctx, key, got); e != nil {
				return false, e
			}
			if got.Status.AppState.State == v1beta2.ApplicationStateFailed {
				sawFailing = true
				msg := got.Status.AppState.ErrorMessage
				Expect(msg).NotTo(BeEmpty())
				Expect(strings.Contains(msg, common.ReasonErrImagePull) || strings.Contains(msg, common.ReasonImagePullBackOff)).To(BeTrue())
				Expect(got.Status.TerminationTime.Time.IsZero()).To(BeFalse())
				return true, nil
			}
			return false, nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(sawFailing).To(BeTrue())

		By("Waiting for application to reach Failed")
		Expect(waitForSparkAppFailed(ctx, key)).NotTo(HaveOccurred())
	})
})

func waitForSparkAppFailed(ctx context.Context, key types.NamespacedName) error {
	cancelCtx, cancel := context.WithTimeout(ctx, WaitTimeout)
	defer cancel()

	app := &v1beta2.SparkApplication{}
	return wait.PollUntilContextCancel(cancelCtx, PollInterval, true, func(ctx context.Context) (bool, error) {
		if err := k8sClient.Get(ctx, key, app); err != nil {
			return false, err
		}
		switch app.Status.AppState.State {
		case v1beta2.ApplicationStateFailed:
			return true, nil
		case v1beta2.ApplicationStateCompleted, v1beta2.ApplicationStateFailedSubmission:
			return false, &unexpectedTerminalState{state: string(app.Status.AppState.State), message: app.Status.AppState.ErrorMessage}
		}
		return false, nil
	})
}

type unexpectedTerminalState struct {
	state   string
	message string
}

func (e *unexpectedTerminalState) Error() string {
	return "unexpected terminal state: " + e.state + ": " + e.message
}
