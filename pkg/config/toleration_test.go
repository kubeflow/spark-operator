/*
Copyright 2017 Google LLC

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

package config

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	apiv1 "k8s.io/api/core/v1"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/util"
)

func TestFindTolerations(t *testing.T) {
	toleration1 := &apiv1.Toleration{
		Key:      "Key1",
		Operator: "Equal",
		Value:    "Value",
		Effect:   "NoEffect",
	}
	toleration2 := &apiv1.Toleration{
		Key:      "Key2",
		Operator: "Exists",
		Effect:   "NoSchedule",
	}
	annotations := make(map[string]string)
	toleration1Str, err := util.MarshalToleration(toleration1)
	if err != nil {
		t.Fatal(err)
	}
	annotations[fmt.Sprintf("%s%s", TolerationsAnnotationPrefix, "toleration1")] = toleration1Str
	toleration2Str, err := util.MarshalToleration(toleration2)
	if err != nil {
		t.Fatal(err)
	}
	annotations[fmt.Sprintf("%s%s", TolerationsAnnotationPrefix, "toleration2")] = toleration2Str
	tolerations, err := FindTolerations(annotations)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 2, len(tolerations))
	if tolerations[0].Key == "Key1" {
		assert.Equal(t, "Value", tolerations[0].Value)
		assert.Equal(t, "Equal", string(tolerations[0].Operator))
		assert.Equal(t, "NoEffect", string(tolerations[0].Effect))
		assert.Equal(t, "Key2", tolerations[1].Key)
		assert.Equal(t, "Exists", string(tolerations[1].Operator))
		assert.Equal(t, "NoSchedule", string(tolerations[1].Effect))
	} else {
		assert.Equal(t, "Key1", tolerations[1].Key)
		assert.Equal(t, "Value", tolerations[1].Value)
		assert.Equal(t, "Equal", string(tolerations[1].Operator))
		assert.Equal(t, "NoEffect", string(tolerations[1].Effect))
		assert.Equal(t, "Key2", tolerations[0].Key)
		assert.Equal(t, "Exists", string(tolerations[0].Operator))
		assert.Equal(t, "NoSchedule", string(tolerations[0].Effect))
	}
}

func TestGetTolerationAnnotations(t *testing.T) {
	toleration1 := apiv1.Toleration{
		Key:      "Key1",
		Operator: "Equal",
		Value:    "Value",
		Effect:   "NoEffect",
	}
	toleration2 := apiv1.Toleration{
		Key:      "Key2",
		Operator: "Exists",
		Effect:   "NoSchedule",
	}

	annotations, err := getTolerationAnnotations([]apiv1.Toleration{toleration1, toleration2})
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 2, len(annotations))
	value, ok := annotations[fmt.Sprintf("%s%s", TolerationsAnnotationPrefix, "toleration1")]
	assert.True(t, ok)
	toleration, err := util.UnmarshalToleration(value)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, toleration1.Key, toleration.Key)
	assert.Equal(t, toleration1.Value, toleration.Value)
	assert.Equal(t, string(toleration1.Operator), string(toleration.Operator))
	assert.Equal(t, string(toleration1.Effect), string(toleration.Effect))

	value, ok = annotations[fmt.Sprintf("%s%s", TolerationsAnnotationPrefix, "toleration2")]
	assert.True(t, ok)
	toleration, err = util.UnmarshalToleration(value)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, toleration2.Key, toleration.Key)
	assert.Equal(t, string(toleration2.Operator), string(toleration.Operator))
	assert.Equal(t, string(toleration2.Effect), string(toleration.Effect))
}

func TestGetDriverTolerationConfOptions(t *testing.T) {
	app := &v1alpha1.SparkApplication{
		Spec: v1alpha1.SparkApplicationSpec{
			Driver: v1alpha1.DriverSpec{
				SparkPodSpec: v1alpha1.SparkPodSpec{
					Tolerations: []apiv1.Toleration{
						{
							Key:      "Key1",
							Operator: "Equal",
							Value:    "Value",
							Effect:   "NoEffect",
						},
						{
							Key:      "Key2",
							Operator: "Exists",
							Effect:   "NoSchedule",
						},
					},
				},
			},
		},
	}

	options, err := GetDriverTolerationConfOptions(app)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 2, len(options))
	sort.Strings(options)
	assert.True(t, strings.HasPrefix(options[0], fmt.Sprintf("%s%s%s=", SparkDriverAnnotationKeyPrefix,
		TolerationsAnnotationPrefix, "toleration1")))
	assert.True(t, strings.HasPrefix(options[1], fmt.Sprintf("%s%s%s=", SparkDriverAnnotationKeyPrefix,
		TolerationsAnnotationPrefix, "toleration2")))
}

func TestGetExecutorTolerationConfOptions(t *testing.T) {
	app := &v1alpha1.SparkApplication{
		Spec: v1alpha1.SparkApplicationSpec{
			Executor: v1alpha1.ExecutorSpec{
				SparkPodSpec: v1alpha1.SparkPodSpec{
					Tolerations: []apiv1.Toleration{
						{
							Key:      "Key1",
							Operator: "Equal",
							Value:    "Value",
							Effect:   "NoEffect",
						},
						{
							Key:      "Key2",
							Operator: "Exists",
							Effect:   "NoSchedule",
						},
					},
				},
			},
		},
	}

	options, err := GetExecutorTolerationConfOptions(app)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 2, len(options))
	sort.Strings(options)
	assert.True(t, strings.HasPrefix(options[0], fmt.Sprintf("%s%s%s=", SparkExecutorAnnotationKeyPrefix,
		TolerationsAnnotationPrefix, "toleration1")))
	assert.True(t, strings.HasPrefix(options[1], fmt.Sprintf("%s%s%s=", SparkExecutorAnnotationKeyPrefix,
		TolerationsAnnotationPrefix, "toleration2")))
}
