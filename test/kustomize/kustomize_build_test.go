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

package kustomize_test

import (
	"bytes"
	"encoding/json"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

// buildKustomize runs "kubectl kustomize" on config/default and returns the
// parsed Kubernetes resources. The test is skipped when kubectl is absent.
func buildKustomize(t *testing.T) []unstructured.Unstructured {
	t.Helper()

	if _, err := exec.LookPath("kubectl"); err != nil {
		t.Skip("kubectl not found in PATH, skipping kustomize build test")
	}

	repoRoot := filepath.Join("..", "..")
	kustomizeDir := filepath.Join(repoRoot, "config", "default")

	cmd := exec.Command("kubectl", "kustomize", kustomizeDir)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "kustomize build failed:\n%s", string(output))

	var resources []unstructured.Unstructured
	decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(output), 4096)
	for {
		obj := unstructured.Unstructured{}
		if err := decoder.Decode(&obj); err != nil {
			if err == io.EOF {
				break
			}
			t.Logf("skipping unparseable document: %v", err)
			continue
		}
		if obj.GetKind() != "" {
			resources = append(resources, obj)
		}
	}

	require.NotEmpty(t, resources, "kustomize build produced no resources")
	return resources
}

// --- helpers ---

func countKind(resources []unstructured.Unstructured, kind string) int {
	n := 0
	for i := range resources {
		if resources[i].GetKind() == kind {
			n++
		}
	}
	return n
}

func findResource(resources []unstructured.Unstructured, kind, name string) *unstructured.Unstructured {
	for i := range resources {
		if resources[i].GetKind() == kind && resources[i].GetName() == name {
			return &resources[i]
		}
	}
	return nil
}

func findResources(resources []unstructured.Unstructured, kind string) []unstructured.Unstructured {
	var out []unstructured.Unstructured
	for i := range resources {
		if resources[i].GetKind() == kind {
			out = append(out, resources[i])
		}
	}
	return out
}

func convertTo[T any](t *testing.T, obj *unstructured.Unstructured) *T {
	t.Helper()
	data, err := json.Marshal(obj.Object)
	require.NoError(t, err)
	result := new(T)
	require.NoError(t, json.Unmarshal(data, result))
	return result
}

func rulesHaveResource(rules []rbacv1.PolicyRule, resource string) bool {
	for _, rule := range rules {
		for _, r := range rule.Resources {
			if r == resource {
				return true
			}
		}
	}
	return false
}

func rulesHaveResourceName(rules []rbacv1.PolicyRule, name string) bool {
	for _, rule := range rules {
		for _, rn := range rule.ResourceNames {
			if rn == name {
				return true
			}
		}
	}
	return false
}

// --- tests ---

func TestKustomizeBuild(t *testing.T) {
	resources := buildKustomize(t)

	t.Run("ResourceInventory", func(t *testing.T) {
		expected := map[string]int{
			"Namespace":                      1,
			"CustomResourceDefinition":       3,
			"ServiceAccount":                 2,
			"ClusterRole":                    6,
			"ClusterRoleBinding":             2,
			"Role":                           2,
			"RoleBinding":                    2,
			"Deployment":                     2,
			"Service":                        1,
			"MutatingWebhookConfiguration":   1,
			"ValidatingWebhookConfiguration": 1,
		}
		for kind, want := range expected {
			t.Run(kind, func(t *testing.T) {
				assert.Equal(t, want, countKind(resources, kind), "unexpected %s count", kind)
			})
		}
	})

	t.Run("NamespaceConsistency", func(t *testing.T) {
		nsCount := 0
		for i := range resources {
			if resources[i].GetNamespace() == "spark-operator" {
				nsCount++
			}
		}
		assert.GreaterOrEqual(t, nsCount, 5,
			"expected at least 5 namespaced resources in spark-operator namespace, got %d", nsCount)

		ns := findResource(resources, "Namespace", "spark-operator")
		require.NotNil(t, ns, "Namespace 'spark-operator' not found")
	})

	t.Run("ImageReplacement", func(t *testing.T) {
		var imageCount int
		for _, d := range findResources(resources, "Deployment") {
			dep := convertTo[appsv1.Deployment](t, &d)
			for _, c := range dep.Spec.Template.Spec.Containers {
				if strings.HasPrefix(c.Image, "ghcr.io/kubeflow/spark-operator/controller:") {
					imageCount++
				}
			}
		}
		assert.Equal(t, 2, imageCount, "expected 2 upstream image references across deployments")
	})

	t.Run("ControllerRBAC", func(t *testing.T) {
		crObj := findResource(resources, "ClusterRole", "spark-operator-controller")
		require.NotNil(t, crObj, "ClusterRole 'spark-operator-controller' not found")

		// Leader election lease is scoped in the controller Role, not ClusterRole.
		roleObj := findResource(resources, "Role", "spark-operator-controller")
		require.NotNil(t, roleObj, "Role 'controller' (leader-election) not found")
		role := convertTo[rbacv1.Role](t, roleObj)
		assert.True(t, rulesHaveResourceName(role.Rules, "spark-operator-controller-lock"),
			"leader election lease should be scoped to 'spark-operator-controller-lock'")

		crbObj := findResource(resources, "ClusterRoleBinding", "spark-operator-controller")
		require.NotNil(t, crbObj, "ClusterRoleBinding 'spark-operator-controller' not found")
		crb := convertTo[rbacv1.ClusterRoleBinding](t, crbObj)

		hasSA := false
		for _, s := range crb.Subjects {
			if s.Kind == "ServiceAccount" {
				hasSA = true
				break
			}
		}
		assert.True(t, hasSA, "controller ClusterRoleBinding should reference a ServiceAccount")
	})

	t.Run("WebhookClusterRole", func(t *testing.T) {
		obj := findResource(resources, "ClusterRole", "spark-operator-webhook")
		require.NotNil(t, obj, "ClusterRole 'spark-operator-webhook' not found")
		cr := convertTo[rbacv1.ClusterRole](t, obj)

		for _, res := range []string{"pods", "sparkapplications", "scheduledsparkapplications", "mutatingwebhookconfigurations"} {
			assert.True(t, rulesHaveResource(cr.Rules, res),
				"webhook ClusterRole should have '%s'", res)
		}
		for _, res := range []string{"events", "resourcequotas"} {
			assert.False(t, rulesHaveResource(cr.Rules, res),
				"webhook ClusterRole should NOT have '%s' (not code-required)", res)
		}

		assert.True(t, rulesHaveResourceName(cr.Rules, "mutating-webhook-configuration"),
			"webhook ClusterRole should scope resourceNames for webhook configs")
	})

	t.Run("WebhookRole", func(t *testing.T) {
		obj := findResource(resources, "Role", "spark-operator-webhook")
		require.NotNil(t, obj, "Role 'webhook' not found")
		role := convertTo[rbacv1.Role](t, obj)

		assert.True(t, rulesHaveResource(role.Rules, "secrets"),
			"webhook Role should have 'secrets'")
		assert.False(t, rulesHaveResource(role.Rules, "events"),
			"webhook Role should NOT have 'events' (events are in ClusterRole for Helm parity)")
		assert.True(t, rulesHaveResourceName(role.Rules, "spark-operator-webhook-certs"),
			"webhook Role should scope secret to 'spark-operator-webhook-certs'")
		assert.True(t, rulesHaveResourceName(role.Rules, "spark-operator-webhook-lock"),
			"webhook Role should scope lease to 'spark-operator-webhook-lock'")
	})

	t.Run("WebhookConfiguration", func(t *testing.T) {
		mwObj := findResource(resources, "MutatingWebhookConfiguration", "mutating-webhook-configuration")
		require.NotNil(t, mwObj, "MutatingWebhookConfiguration not found")
		mw := convertTo[admissionregistrationv1.MutatingWebhookConfiguration](t, mwObj)

		hasObjectSelector := false
		for _, wh := range mw.Webhooks {
			if strings.Contains(wh.Name, "mutate-pod") && wh.ObjectSelector != nil {
				hasObjectSelector = true
				break
			}
		}
		assert.True(t, hasObjectSelector,
			"pod mutation webhook should have objectSelector to prevent chicken-and-egg deadlock")

		svcRefCount := 0
		for _, wh := range mw.Webhooks {
			if wh.ClientConfig.Service != nil && wh.ClientConfig.Service.Name == "spark-operator-webhook-svc" {
				svcRefCount++
			}
		}

		vwObj := findResource(resources, "ValidatingWebhookConfiguration", "validating-webhook-configuration")
		require.NotNil(t, vwObj, "ValidatingWebhookConfiguration not found")
		vw := convertTo[admissionregistrationv1.ValidatingWebhookConfiguration](t, vwObj)

		for _, wh := range vw.Webhooks {
			if wh.ClientConfig.Service != nil && wh.ClientConfig.Service.Name == "spark-operator-webhook-svc" {
				svcRefCount++
			}
		}
		assert.GreaterOrEqual(t, svcRefCount, 4,
			"webhook configs should reference 'spark-operator-webhook-svc' (got %d)", svcRefCount)
	})

	t.Run("DeploymentConfiguration", func(t *testing.T) {
		deployments := findResources(resources, "Deployment")
		require.Len(t, deployments, 2, "expected 2 deployments")

		var (
			saNames      []string
			ports        []int32
			readOnlyRoot bool
			runAsNonRoot bool
			hasHealthz   bool
			hasReadyz    bool
		)

		for _, d := range deployments {
			dep := convertTo[appsv1.Deployment](t, &d)
			saNames = append(saNames, dep.Spec.Template.Spec.ServiceAccountName)

			podSC := dep.Spec.Template.Spec.SecurityContext
			if podSC != nil && podSC.RunAsNonRoot != nil && *podSC.RunAsNonRoot {
				runAsNonRoot = true
			}

			for _, c := range dep.Spec.Template.Spec.Containers {
				for _, p := range c.Ports {
					ports = append(ports, p.ContainerPort)
				}
				if c.SecurityContext != nil {
					if c.SecurityContext.ReadOnlyRootFilesystem != nil && *c.SecurityContext.ReadOnlyRootFilesystem {
						readOnlyRoot = true
					}
					if c.SecurityContext.RunAsNonRoot != nil && *c.SecurityContext.RunAsNonRoot {
						runAsNonRoot = true
					}
				}
				if c.LivenessProbe != nil && c.LivenessProbe.HTTPGet != nil && c.LivenessProbe.HTTPGet.Path == "/healthz" {
					hasHealthz = true
				}
				if c.ReadinessProbe != nil && c.ReadinessProbe.HTTPGet != nil && c.ReadinessProbe.HTTPGet.Path == "/readyz" {
					hasReadyz = true
				}
			}
		}

		assert.Contains(t, saNames, "spark-operator-controller", "expected serviceAccountName 'spark-operator-controller'")
		assert.Contains(t, saNames, "spark-operator-webhook", "expected serviceAccountName 'spark-operator-webhook'")
		assert.Contains(t, ports, int32(9443), "expected containerPort 9443 (webhook)")
		assert.Contains(t, ports, int32(8080), "expected containerPort 8080 (metrics)")
		assert.True(t, readOnlyRoot, "expected readOnlyRootFilesystem: true")
		assert.True(t, runAsNonRoot, "expected runAsNonRoot: true")
		assert.True(t, hasHealthz, "expected /healthz liveness probe")
		assert.True(t, hasReadyz, "expected /readyz readiness probe")
	})
}
