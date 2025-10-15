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
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/stretchr/testify/assert"
)

func TestValidateSparkApplicationLabelsMutationSpec(t *testing.T) {
	path := field.NewPath("field")

	for name, tc := range map[string]struct {
		mutation        *v1beta2.SparkApplicationLabelsMutationSpec
		wantErr         bool
		expectedErrList field.ErrorList
	}{
		"nil mutation": {
			mutation: nil,
			wantErr:  false,
		},
		"empty mutation": {
			mutation: &v1beta2.SparkApplicationLabelsMutationSpec{},
			wantErr:  false,
		},
		"all conditions are valid": {
			mutation: &v1beta2.SparkApplicationLabelsMutationSpec{
				LabelKeyMatches: []v1beta2.MutatingLabelKeyMatchCondition{
					{Fixed: ptr.To("fixed")}, {Regex: ptr.To("^[a-zA-Z0-9_.-]+$")},
				},
			},
			wantErr: false,
		},
		"exists invalid condition": {
			mutation: &v1beta2.SparkApplicationLabelsMutationSpec{
				LabelKeyMatches: []v1beta2.MutatingLabelKeyMatchCondition{
					{Fixed: ptr.To("fixed")}, {},
				},
			},
			wantErr: true,
			expectedErrList: field.ErrorList{
				field.Required(
					path.Child("labelKeyMatches").Index(1),
					"must specify either fixed or regex",
				),
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			gotErr := validateSparkApplicationLabelsMutationSpec(path, tc.mutation)
			if tc.wantErr {
				assert.Equal(t, tc.expectedErrList, gotErr)
			} else {
				assert.Empty(t, gotErr)
			}
		})
	}
}

func TestValidateMutatingLabelKeyCondition(t *testing.T) {
	path := field.NewPath("field")

	for name, tc := range map[string]struct {
		condition       v1beta2.MutatingLabelKeyMatchCondition
		wantErr         bool
		expectedErrList field.ErrorList
	}{
		"both fixed and regex are nil": {
			condition:       v1beta2.MutatingLabelKeyMatchCondition{},
			wantErr:         true,
			expectedErrList: field.ErrorList{field.Required(path, "must specify either fixed or regex")},
		},
		"both fixed and regex are set": {
			condition: v1beta2.MutatingLabelKeyMatchCondition{
				Fixed: ptr.To("fixed"),
				Regex: ptr.To("regex"),
			},
			wantErr:         true,
			expectedErrList: field.ErrorList{field.Invalid(path, "", "only one of fixed or regex can be specified")},
		},
		"fixed is set": {
			condition: v1beta2.MutatingLabelKeyMatchCondition{
				Fixed: ptr.To("fixed"),
			},
			wantErr: false,
		},
		"regex is set to valid regex": {
			condition: v1beta2.MutatingLabelKeyMatchCondition{
				Regex: ptr.To("^[a-zA-Z0-9_.-]+$"),
			},
			wantErr: false,
		},
		"regex is set to invalid regex": {
			condition: v1beta2.MutatingLabelKeyMatchCondition{
				Regex: ptr.To("[invalid-regex"),
			},
			wantErr: true,
			expectedErrList: field.ErrorList{field.Invalid(
				path.Child("regex"), "[invalid-regex", "invalid regex: error parsing regexp: missing closing ]: `[invalid-regex`",
			)},
		},
	} {
		t.Run(name, func(t *testing.T) {
			gotErr := validateMutatingLabelKeyCondition(path, tc.condition)
			if tc.wantErr {
				assert.Equal(t, tc.expectedErrList, gotErr)
			} else {
				assert.Empty(t, gotErr)
			}
		})
	}
}
