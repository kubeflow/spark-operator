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

package cmd

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/duration"
)

func getSinceTime(timestamp metav1.Time) string {
	if timestamp.IsZero() {
		return "N.A."
	}

	return duration.ShortHumanDuration(time.Since(timestamp.Time))
}

func formatNotAvailable(info string) string {
	if info == "" {
		return "N.A."
	}
	return info
}
