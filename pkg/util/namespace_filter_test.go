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

package util

import (
	"reflect"
	"regexp"
	"testing"

	apiv1 "k8s.io/api/core/v1"
)

func TestGetNamespaceFilter(t *testing.T) {

	tests := []struct {
		name                  string
		namespaceFilterConfig NamespaceConfig
		want                  *regexp.Regexp
	}{
		{
			name: "Should replace comma by pipe",
			namespaceFilterConfig: NamespaceConfig{
				Namespace:       apiv1.NamespaceAll,
				NamespaceFilter: ".*-dev,.*-testing",
			},
			want: regexp.MustCompile(".*-dev\\b|.*-testing\\b"),
		},
		{
			name: "Should add zero-length word boundry sequence",
			namespaceFilterConfig: NamespaceConfig{
				Namespace:       apiv1.NamespaceAll,
				NamespaceFilter: "dev-*",
			},
			want: regexp.MustCompile("dev-*\\b"),
		},
		{
			name: "Filter with multiple commas",
			namespaceFilterConfig: NamespaceConfig{
				Namespace:       apiv1.NamespaceAll,
				NamespaceFilter: "^*-dev,,,^*-testing,",
			},
			want: regexp.MustCompile("^*-dev\\b|^*-testing\\b"),
		},
		{
			name: "Should use a wildcard as a regex",
			namespaceFilterConfig: NamespaceConfig{
				Namespace:       apiv1.NamespaceAll,
				NamespaceFilter: "",
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetNamespaceFilter(&tt.namespaceFilterConfig); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getNamespaceFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsNamespaceMatch(t *testing.T) {

	tests := []struct {
		name      string
		regex     *regexp.Regexp
		namespace string
		want      bool
	}{
		{
			name:      "namespace matches filter",
			regex:     regexp.MustCompile("^*.-dev\\b"),
			namespace: "test1-dev",
			want:      true,
		},
		{
			name:      "namespace does not matche filter",
			regex:     regexp.MustCompile("^*.-prod\\b"),
			namespace: "test1-pro",
			want:      false,
		},
		{
			name:      "namespace should match one of the regex",
			regex:     regexp.MustCompile("^*-dev\\b|^*-testing\\b|^*-staging\\b"),
			namespace: "test1-testing",
			want:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsNamespaceMatchesFilter(*tt.regex, tt.namespace); got != tt.want {
				t.Errorf("isNamespaceMatch() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsNamespaceFilterEnabled(t *testing.T) {

	tests := []struct {
		name                string
		namespaceFilterSpec NamespaceConfig
		want                bool
	}{
		{
			name: "namespace is sepcified should not apply filter",
			namespaceFilterSpec: NamespaceConfig{
				Namespace:       "test-prod",
				NamespaceFilter: "\\*.-dev",
			},
			want: false,
		},
		{
			name: "namespace is not sepcified and namespaceFilter is not empty: should apply the filter",
			namespaceFilterSpec: NamespaceConfig{
				Namespace:       apiv1.NamespaceAll,
				NamespaceFilter: "\\*.-dev",
			},
			want: true,
		},
		{
			name: "namespaceFilter is empty should not apply filter",
			namespaceFilterSpec: NamespaceConfig{
				Namespace:       apiv1.NamespaceAll,
				NamespaceFilter: "",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isNamespaceFilterEnabled(&tt.namespaceFilterSpec); got != tt.want {
				t.Errorf("isNamsepaceShouldBeFiltered() = %v, want %v", got, tt.want)
			}
		})
	}
}
