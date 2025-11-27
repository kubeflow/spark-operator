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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/mod/semver"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/yaml"

	"github.com/go-logr/logr"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

var unitMap = map[string]string{
	"k":  "Ki",
	"kb": "Ki",
	"m":  "Mi",
	"mb": "Mi",
	"g":  "Gi",
	"gb": "Gi",
	"t":  "Ti",
	"tb": "Ti",
	"p":  "Pi",
	"pb": "Pi",
}

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

// CompareSemanticVersion compares two semantic versions.
func CompareSemanticVersion(v1, v2 string) int {
	// Add 'v' prefix if needed
	addPrefix := func(s string) string {
		if !strings.HasPrefix(s, "v") {
			return "v" + s
		}
		return s
	}
	return semver.Compare(addPrefix(v1), addPrefix(v2))
}

// WriteObjectToFile marshals the given object into a YAML document and writes it to the given file.
func WriteObjectToFile(obj interface{}, filePath string) (err error) {
	err = os.MkdirAll(filepath.Dir(filePath), 0755)
	if err != nil {
		return err
	}

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		fileErr := file.Close()
		if fileErr != nil {
			err = errors.Join(err, fileErr)
		}
	}(file)

	data, err := yaml.Marshal(obj)
	if err != nil {
		return err
	}

	_, err = file.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func ConvertJavaMemoryStringToK8sMemoryString(memory string) string {

	for unit, k8sUnit := range unitMap {
		if strings.HasSuffix(strings.ToLower(memory), unit) {
			return strings.TrimSuffix(memory, unit) + k8sUnit
		}
	}

	// return original memory value if no conversion is needed
	return memory
}

func SetIfNotExists[K comparable, V any](m map[K]V, key K, value V) {
	if m == nil {
		return
	}

	if _, ok := m[key]; !ok {
		m[key] = value
	}
}

// NewLogConstructor returns a log constructor for the given kind.
func NewLogConstructor(logger logr.Logger, kind string) func(*reconcile.Request) logr.Logger {
	// Use the lowercase of kind as controller name, as it will show up in the metrics,
	// and thus should be a prometheus compatible name(underscores and alphanumeric characters only).
	name := strings.ToLower(kind)

	// Use a custom log constructor.
	logConstructor := func(req *reconcile.Request) logr.Logger {
		logger := logger.WithValues("controller", name)
		if req != nil {
			logger = logger.WithValues(kind, klog.KRef(req.Namespace, req.Name))
		}
		return logger
	}

	return logConstructor
}
