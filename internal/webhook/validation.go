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

package webhook

import (
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// newInvalidErrors turns the messages an apimachinery validation helper reports about value
// into one field error each, so every reason a value is rejected reaches the client.
func newInvalidErrors(path *field.Path, value string, msgs []string) field.ErrorList {
	var errs field.ErrorList
	for _, msg := range msgs {
		errs = append(errs, field.Invalid(path, value, msg))
	}
	return errs
}
