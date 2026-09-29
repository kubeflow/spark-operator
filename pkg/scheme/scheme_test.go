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

package scheme

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

func TestControllerSchemeRecognizesCRDTypes(t *testing.T) {
	for _, obj := range []runtime.Object{
		&v1beta2.SparkApplication{},
		&v1beta2.ScheduledSparkApplication{},
		&v1alpha1.SparkConnect{},
		&corev1.Pod{},
		&schedulingv1alpha1.PodGroup{},
	} {
		_, _, err := ControllerScheme.ObjectKinds(obj)
		assert.NoError(t, err, "%T should be registered on ControllerScheme", obj)
	}
}

func TestWebhookSchemeRecognizesCRDTypes(t *testing.T) {
	for _, obj := range []runtime.Object{
		&v1beta2.SparkApplication{},
		&v1beta2.ScheduledSparkApplication{},
		&v1alpha1.SparkConnect{},
		&corev1.Pod{},
	} {
		_, _, err := WebhookScheme.ObjectKinds(obj)
		assert.NoError(t, err, "%T should be registered on WebhookScheme", obj)
	}
}
