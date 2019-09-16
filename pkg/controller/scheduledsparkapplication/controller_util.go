/*
Copyright 2018 Google LLC

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

package scheduledsparkapplication

import (
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta1"
)

type sparkApps v1beta1.SparkApplicationList

func (s sparkApps) Len() int {
	return len(s.Items)
}

func (s sparkApps) Swap(i, j int) {
	list := s.Items
	list[i], list[j] = list[j], list[i]
	s.Items = list
}

func (s sparkApps) Less(i, j int) bool {
	// Sort by decreasing order of application names and correspondingly creation time.
	list := s.Items
	return list[i].Name > list[j].Name
}
