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

package webhook

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

func TestScheduledSparkApplicationDefaulterDefault(t *testing.T) {
	defaulter := NewScheduledSparkApplicationDefaulter()

	t.Run("returns nil for unrelated object types", func(t *testing.T) {
		err := defaulter.Default(context.Background(), &v1beta2.SparkApplication{})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})

	t.Run("returns nil for ScheduledSparkApplication", func(t *testing.T) {
		app := &v1beta2.ScheduledSparkApplication{}
		err := defaulter.Default(context.Background(), app)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})
}

func TestScheduledSparkApplicationDefaulterWebhookManifest(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to get current file path")
	}
	repoRoot := filepath.Join(filepath.Dir(currentFile), "..", "..")
	manifestPath := filepath.Join(repoRoot, "config", "webhook", "manifests.yaml")

	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("failed to read manifests.yaml: %v", err)
	}
	manifest := string(data)

	t.Run("mutating webhook has correct path", func(t *testing.T) {
		expected := "path: /mutate-sparkoperator-k8s-io-v1beta2-scheduledsparkapplication"
		if !strings.Contains(manifest, expected) {
			t.Errorf("manifests.yaml missing expected mutating webhook path.\nExpected to contain: %s", expected)
		}
	})

	t.Run("mutating webhook is in MutatingWebhookConfiguration", func(t *testing.T) {
		mutatingIdx := strings.Index(manifest, "kind: MutatingWebhookConfiguration")
		validatingIdx := strings.Index(manifest, "kind: ValidatingWebhookConfiguration")
		pathIdx := strings.Index(manifest, "/mutate-sparkoperator-k8s-io-v1beta2-scheduledsparkapplication")

		if mutatingIdx == -1 {
			t.Fatal("MutatingWebhookConfiguration not found in manifests.yaml")
		}
		if pathIdx == -1 {
			t.Fatal("ScheduledSparkApplication mutating webhook path not found")
		}
		if validatingIdx != -1 && pathIdx > validatingIdx {
			t.Error("ScheduledSparkApplication mutating webhook path appears in ValidatingWebhookConfiguration instead of MutatingWebhookConfiguration")
		}
	})

	t.Run("mutating webhook targets scheduledsparkapplications resource", func(t *testing.T) {
		pathIdx := strings.Index(manifest, "/mutate-sparkoperator-k8s-io-v1beta2-scheduledsparkapplication")
		if pathIdx == -1 {
			t.Fatal("mutating webhook path not found")
		}
		webhookBlock := manifest[pathIdx:]
		nextWebhookIdx := strings.Index(webhookBlock[1:], "- admissionReviewVersions")
		if nextWebhookIdx != -1 {
			webhookBlock = webhookBlock[:nextWebhookIdx+1]
		}
		if !strings.Contains(webhookBlock, "scheduledsparkapplications") {
			t.Error("mutating webhook block does not target 'scheduledsparkapplications' resource")
		}
	})

	t.Run("mutating webhook does not point to validate path", func(t *testing.T) {
		mutatingIdx := strings.Index(manifest, "kind: MutatingWebhookConfiguration")
		validatingIdx := strings.Index(manifest, "kind: ValidatingWebhookConfiguration")
		if mutatingIdx == -1 || validatingIdx == -1 {
			t.Skip("could not find both webhook configuration sections")
		}
		mutatingSection := manifest[mutatingIdx:validatingIdx]

		if strings.Contains(mutatingSection, "path: /validate-sparkoperator-k8s-io-v1beta2-sparkapplication") {
			t.Error("MutatingWebhookConfiguration contains a /validate- path for ScheduledSparkApplication — this was the original bug")
		}
	})

	t.Run("validating webhook has correct path", func(t *testing.T) {
		expected := "path: /validate-sparkoperator-k8s-io-v1beta2-scheduledsparkapplication"
		if !strings.Contains(manifest, expected) {
			t.Errorf("manifests.yaml missing expected validating webhook path.\nExpected to contain: %s", expected)
		}
	})
}
