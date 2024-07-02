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

package util

import (
	"fmt"
	"hash"
	"hash/fnv"
	"os"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/kubeflow/spark-operator/pkg/common"
)

var (
	logger = log.Log.WithName("")
)

func GetMasterURL() (string, error) {
	kubernetesServiceHost := os.Getenv(common.EnvKubernetesServiceHost)
	if kubernetesServiceHost == "" {
		return "", fmt.Errorf("environment variable %s is not found", common.EnvKubernetesServiceHost)
	}
	kubernetesServicePort := os.Getenv(common.EnvKubernetesServicePort)
	if kubernetesServicePort == "" {
		return "", fmt.Errorf("environment variable %s is not found", common.EnvKubernetesServicePort)
	}
	// check if the host is IPv6 address
	if strings.Contains(kubernetesServiceHost, ":") && !strings.HasPrefix(kubernetesServiceHost, "[") {
		return fmt.Sprintf("k8s://https://[%s]:%s", kubernetesServiceHost, kubernetesServicePort), nil
	}
	return fmt.Sprintf("k8s://https://%s:%s", kubernetesServiceHost, kubernetesServicePort), nil
}

// GetDriverAnnotationOption returns a spark-submit option for a driver annotation of the given key and value.
func GetDriverAnnotationOption(key string, value string) string {
	return fmt.Sprintf("%s%s=%s", common.SparkKubernetesDriverAnnotationPrefix, key, value)
}

// GetExecutorAnnotationOption returns a spark-submit option for an executor annotation of the given key and value.
func GetExecutorAnnotationOption(key string, value string) string {
	return fmt.Sprintf("%s%s=%s", common.SparkKubernetesExecutorAnnotationPrefix, key, value)
}

// NewHash32 returns a 32-bit hash computed from the given byte slice.
func NewHash32() hash.Hash32 {
	return fnv.New32()
}

// Helper functions to check and remove a string from a slice of strings.
// ContainsString checks if a given string is present in a slice
func ContainsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

// RemoveString removes a given string from a slice, if present
func RemoveString(slice []string, s string) (result []string) {
	for _, item := range slice {
		if item != s {
			result = append(result, item)
		}
	}
	return result
}

func BoolPtr(b bool) *bool {
	return &b
}

func Int32Ptr(n int32) *int32 {
	return &n
}

func Int64Ptr(n int64) *int64 {
	return &n
}

func StringPtr(s string) *string {
	return &s
}

// SumResourceList sums the resource list.
func SumResourceList(list []corev1.ResourceList) corev1.ResourceList {
	totalResource := corev1.ResourceList{}
	for _, l := range list {
		for name, quantity := range l {
			if value, ok := totalResource[name]; !ok {
				totalResource[name] = quantity.DeepCopy()
			} else {
				value.Add(quantity)
				totalResource[name] = value
			}
		}
	}
	return totalResource
}
