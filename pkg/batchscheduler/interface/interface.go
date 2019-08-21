/*
Copyright 2019 Google LLC

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

package schedulerinterface

import (
	corev1 "k8s.io/api/core/v1"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta1"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/util"
)

type BatchScheduler interface {
	Name() string

	ShouldSchedule(app *v1beta1.SparkApplication) bool
	PatchApplicationPod(pod *corev1.Pod, app *v1beta1.SparkApplication) []util.PatchOperation
	BeforeSubmitSparkApplication(app *v1beta1.SparkApplication) error
}
