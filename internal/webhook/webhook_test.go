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

package webhook

import (
	"strings"
	"testing"

	"github.com/go-logr/logr/funcr"
	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func TestLogConstructorWithNilRequest(t *testing.T) {
	logger := funcr.New(func(prefix, args string) {}, funcr.Options{})

	got := LogConstructor(logger, nil)

	if got != logger {
		t.Errorf("expected the logger to be returned unchanged for a nil request")
	}
}

func TestLogConstructorWithRequest(t *testing.T) {
	var captured string
	logger := funcr.New(func(prefix, args string) { captured = args }, funcr.Options{})
	req := &admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Kind:      metav1.GroupVersionKind{Kind: "SparkApplication"},
		Namespace: "ns",
		Name:      "app",
		UID:       types.UID("abc-123"),
	}}

	LogConstructor(logger, req).Info("test")

	want := `"SparkApplication"={"name"="app" "namespace"="ns"} "requestID"="abc-123"`
	if !strings.Contains(captured, want) {
		t.Errorf("expected %q to contain %q", captured, want)
	}
}
