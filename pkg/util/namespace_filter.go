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
	"github.com/golang/glog"
	apiv1 "k8s.io/api/core/v1"
	"regexp"
	"strings"
)

type NamespaceConfig struct {
	Namespace       string
	NamespaceFilter string
}

func isNamespaceFilterEnabled(namespaceFilterConfig *NamespaceConfig) bool {
	if namespaceFilterConfig == nil {
		glog.Info("No filter on namespaces is applied")
		return false
	}
	if len(namespaceFilterConfig.Namespace) > 0 {
		glog.Info("A namespace is specified. namespace-filter will be ignored even if it is specified")
		return false
	}
	if len(namespaceFilterConfig.NamespaceFilter) == 0 && namespaceFilterConfig.Namespace == apiv1.NamespaceAll {
		glog.Info("Namespace filter is empty. No filter on namespaces is applied")
		return false
	}
	if len(namespaceFilterConfig.NamespaceFilter) > 0 && namespaceFilterConfig.Namespace == apiv1.NamespaceAll {
		glog.Info("Namespace filter will be applied")
		return true
	}
	return false
}

func GetNamespaceFilter(namespaceFilterConfig *NamespaceConfig) *regexp.Regexp {
	if isNamespaceFilterEnabled(namespaceFilterConfig) == false {
		return nil
	}

	var returnFilter = namespaceFilterConfig.NamespaceFilter
	lastChar := returnFilter[len(returnFilter)-1]
	for ok := true; ok; ok = string(lastChar) == "," {
		returnFilter = strings.TrimSuffix(returnFilter, ",")
		lastChar = returnFilter[len(returnFilter)-1]
	}
	re := regexp.MustCompile(",+")
	returnFilter = re.ReplaceAllString(returnFilter, "\\b|")
	returnFilter = returnFilter + "\\b"

	return regexp.MustCompile(returnFilter)
}

func IsNamespaceMatchesFilter(regex regexp.Regexp, namespace string) bool {
	return regex.MatchString(namespace)
}
