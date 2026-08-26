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

package util

import corev1 "k8s.io/api/core/v1"

// GetContainerByNameOrFirst returns the container matching name.
// If no container matches, it returns the first container.
// It returns nil when containers is empty.
func GetContainerByNameOrFirst(
	containers []corev1.Container,
	name string,
) *corev1.Container {
	if len(containers) == 0 {
		return nil
	}
	for i := range containers {
		if containers[i].Name == name {
			return &containers[i]
		}
	}
	return &containers[0]
}
