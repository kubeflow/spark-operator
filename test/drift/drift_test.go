/*
Copyright 2026 The Kubeflow authors.

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

// Package drift_test detects semantic drift between the Helm chart (source of
// truth) and the Kustomize manifests. It renders both sides, parses them into
// typed Kubernetes objects, and compares the fields that matter: RBAC rules,
// webhook configurations, and deployment specs.
package drift_test

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

// ---------------------------------------------------------------------------
// Name mapping between Helm and Kustomize
// ---------------------------------------------------------------------------

// Helm uses "spark-operator-controller" / "spark-operator-webhook" for all
// resource names (derived from fullname helper). Kustomize uses short names
// for namespace-scoped resources and the same names for cluster-scoped ones.

const (
	helmReleaseName = "spark-operator"
	helmNamespace   = "spark-operator"

	// ClusterRole names (same on both sides)
	controllerClusterRole = "spark-operator-controller"
	webhookClusterRole    = "spark-operator-webhook"

	// Role names differ
	helmControllerRole = "spark-operator-controller"
	kustControllerRole = "spark-operator-controller"
	helmWebhookRole    = "spark-operator-webhook"
	kustWebhookRole    = "spark-operator-webhook"

	// Deployment names (same on both sides after kustomize namePrefix)
	helmControllerDeploy = "spark-operator-controller"
	kustControllerDeploy = "spark-operator-controller"
	helmWebhookDeploy    = "spark-operator-webhook"
	kustWebhookDeploy    = "spark-operator-webhook"

	// Webhook configuration names differ
	helmMutatingWH = "spark-operator-webhook"
	kustMutatingWH = "mutating-webhook-configuration"
	helmValidWH    = "spark-operator-webhook"
	kustValidWH    = "validating-webhook-configuration"

	// Container names differ (Kustomize uses short names)
	helmControllerContainer = "spark-operator-controller"
	kustControllerContainer = "controller"
	helmWebhookContainer    = "spark-operator-webhook"
	kustWebhookContainer    = "webhook"
)

// Resource kinds that legitimately exist on only one side.
var ignoredKinds = map[string]bool{
	"Job":                      true, // Helm CRD hook
	"PodMonitor":               true, // Helm prometheus
	"Certificate":              true, // Helm cert-manager
	"Issuer":                   true, // Helm cert-manager
	"ConfigMap":                true, // Kustomize params
	"Namespace":                true, // Kustomize creates NS
	"CustomResourceDefinition": true, // Covered by detect-crds-drift
	"PodDisruptionBudget":      true, // Neither side produces PDB by default (Helm: gated behind podDisruptionBudget.enable + replicas>1; Kustomize: config/pdb/ not included in config/default/kustomization.yaml)
}

// ---------------------------------------------------------------------------
// Rendering helpers
// ---------------------------------------------------------------------------

func repoRoot(t *testing.T) string {
	t.Helper()
	// test/drift/ -> repo root is ../..
	root, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	return root
}

func helmBinary() string {
	if h := os.Getenv("HELM"); h != "" {
		return h
	}
	return "helm"
}

// resolvebin resolves a binary path that may be relative to the repo root.
func resolveBin(t *testing.T, bin string) string {
	t.Helper()
	if filepath.IsAbs(bin) {
		return bin
	}
	// Relative paths are relative to the repo root, not the test working dir.
	return filepath.Join(repoRoot(t), bin)
}

func renderHelm(t *testing.T) []unstructured.Unstructured {
	t.Helper()

	bin := helmBinary()
	if !filepath.IsAbs(bin) {
		if _, err := exec.LookPath(bin); err != nil {
			// Try resolving relative to repo root
			resolved := resolveBin(t, bin)
			if _, err2 := os.Stat(resolved); err2 != nil {
				t.Skipf("%s not found in PATH or repo root, skipping drift test", bin)
			}
			bin = resolved
		}
	}

	root := repoRoot(t)
	chartPath := filepath.Join(root, "charts", "spark-operator-chart")
	valuesFile := filepath.Join(root, "test", "drift", "drift-check-values.yaml")

	cmd := exec.Command(bin, "template", helmReleaseName, chartPath,
		"--namespace", helmNamespace,
		"-f", valuesFile,
	)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "helm template failed:\n%s", string(output))

	return parseYAML(t, output)
}

func renderKustomize(t *testing.T) []unstructured.Unstructured {
	t.Helper()

	root := repoRoot(t)
	kustomizeDir := filepath.Join(root, "config", "default")

	// Try standalone kustomize binary (from KUSTOMIZE env or PATH) first,
	// then fall back to kubectl kustomize.
	var cmd *exec.Cmd
	if k := os.Getenv("KUSTOMIZE"); k != "" {
		bin := resolveBin(t, k)
		cmd = exec.Command(bin, "build", kustomizeDir)
	} else if kBin, err := exec.LookPath("kustomize"); err == nil {
		cmd = exec.Command(kBin, "build", kustomizeDir)
	} else if _, err := exec.LookPath("kubectl"); err == nil {
		cmd = exec.Command("kubectl", "kustomize", kustomizeDir)
	} else {
		t.Skip("neither kustomize nor kubectl found, skipping drift test")
	}

	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "kustomize build failed:\n%s", string(output))

	return parseYAML(t, output)
}

func parseYAML(t *testing.T, data []byte) []unstructured.Unstructured {
	t.Helper()
	var resources []unstructured.Unstructured
	decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	for {
		obj := unstructured.Unstructured{}
		if err := decoder.Decode(&obj); err != nil {
			if err == io.EOF {
				break
			}
			t.Logf("skipping malformed YAML document: %v", err)
			continue
		}
		if obj.GetKind() != "" {
			resources = append(resources, obj)
		}
	}
	require.NotEmpty(t, resources)
	return resources
}

// ---------------------------------------------------------------------------
// Resource lookup helpers
// ---------------------------------------------------------------------------

func findResource(resources []unstructured.Unstructured, kind, name string) *unstructured.Unstructured {
	for i := range resources {
		if resources[i].GetKind() == kind && resources[i].GetName() == name {
			return &resources[i]
		}
	}
	return nil
}

func convertTo[T any](t *testing.T, obj *unstructured.Unstructured) *T {
	t.Helper()
	data, err := json.Marshal(obj.Object)
	require.NoError(t, err)
	result := new(T)
	require.NoError(t, json.Unmarshal(data, result))
	return result
}

// ---------------------------------------------------------------------------
// RBAC comparison helpers
// ---------------------------------------------------------------------------

// Webhook configuration names that differ between Helm and Kustomize. In RBAC
// ResourceNames they reference the same logical resource but with different names.
var webhookConfigNames = map[string]bool{
	"spark-operator-webhook":           true,
	"mutating-webhook-configuration":   true,
	"validating-webhook-configuration": true,
}

// normalizeRules merges rules that share the same verbs+apiGroups (regardless of
// how they are split across YAML entries) and sorts everything deterministically.
// This handles the case where Helm splits rules per sub-resource while Kustomize
// merges them. ResourceNames referencing webhook configuration names are replaced
// with a canonical placeholder since those inherently differ by naming convention.
func normalizeRules(rules []rbacv1.PolicyRule) []rbacv1.PolicyRule {
	type ruleKey struct {
		verbs     string
		apiGroups string
	}

	merged := make(map[ruleKey]*rbacv1.PolicyRule)
	for _, r := range rules {
		verbs := sortedCopy(r.Verbs)
		apiGroups := sortedCopy(r.APIGroups)
		key := ruleKey{
			verbs:     strings.Join(verbs, ","),
			apiGroups: strings.Join(apiGroups, ","),
		}

		// Normalize webhook config resource names
		resourceNames := make([]string, 0, len(r.ResourceNames))
		for _, rn := range r.ResourceNames {
			if webhookConfigNames[rn] {
				resourceNames = append(resourceNames, "<webhook-config>")
			} else {
				resourceNames = append(resourceNames, rn)
			}
		}

		if existing, ok := merged[key]; ok {
			existing.Resources = append(existing.Resources, r.Resources...)
			existing.ResourceNames = append(existing.ResourceNames, resourceNames...)
			existing.NonResourceURLs = append(existing.NonResourceURLs, r.NonResourceURLs...)
		} else {
			merged[key] = &rbacv1.PolicyRule{
				Verbs:           verbs,
				APIGroups:       apiGroups,
				Resources:       append([]string{}, r.Resources...),
				ResourceNames:   append([]string{}, resourceNames...),
				NonResourceURLs: append([]string{}, r.NonResourceURLs...),
			}
		}
	}

	var out []rbacv1.PolicyRule
	for _, r := range merged {
		r.Resources = dedupSorted(sortedCopy(r.Resources))
		r.ResourceNames = dedupSorted(sortedCopy(r.ResourceNames))
		r.NonResourceURLs = sortedCopy(r.NonResourceURLs)
		if len(r.ResourceNames) == 0 {
			r.ResourceNames = nil
		}
		if len(r.NonResourceURLs) == 0 {
			r.NonResourceURLs = nil
		}
		out = append(out, *r)
	}

	sort.Slice(out, func(i, j int) bool {
		ai := strings.Join(out[i].APIGroups, ",") + "/" + strings.Join(out[i].Resources, ",")
		aj := strings.Join(out[j].APIGroups, ",") + "/" + strings.Join(out[j].Resources, ",")
		if ai != aj {
			return ai < aj
		}
		// Tiebreaker: verbs, then resourceNames
		vi := strings.Join(out[i].Verbs, ",")
		vj := strings.Join(out[j].Verbs, ",")
		if vi != vj {
			return vi < vj
		}
		return strings.Join(out[i].ResourceNames, ",") < strings.Join(out[j].ResourceNames, ",")
	})
	return out
}

func dedupSorted(s []string) []string {
	if len(s) <= 1 {
		return s
	}
	out := []string{s[0]}
	for i := 1; i < len(s); i++ {
		if s[i] != s[i-1] {
			out = append(out, s[i])
		}
	}
	return out
}

func sortedCopy(s []string) []string {
	if s == nil {
		return nil
	}
	c := make([]string, len(s))
	copy(c, s)
	sort.Strings(c)
	return c
}

// ---------------------------------------------------------------------------
// Webhook comparison helpers
// ---------------------------------------------------------------------------

type webhookEntry struct {
	Path              string
	FailPolicy        string
	SideEffects       string
	APIGroups         []string
	Resources         []string
	Operations        []string
	ObjectSelector    *metav1.LabelSelector
	NamespaceSelector *metav1.LabelSelector
}

func extractMutatingWebhooks(t *testing.T, obj *unstructured.Unstructured) []webhookEntry {
	t.Helper()
	wh := convertTo[admissionregistrationv1.MutatingWebhookConfiguration](t, obj)
	var entries []webhookEntry
	for _, w := range wh.Webhooks {
		e := webhookEntry{
			FailPolicy:        string(*w.FailurePolicy),
			SideEffects:       string(*w.SideEffects),
			ObjectSelector:    w.ObjectSelector,
			NamespaceSelector: w.NamespaceSelector,
		}
		if w.ClientConfig.Service != nil {
			e.Path = derefStr(w.ClientConfig.Service.Path)
		}
		for _, rule := range w.Rules {
			e.APIGroups = append(e.APIGroups, rule.APIGroups...)
			e.Resources = append(e.Resources, rule.Resources...)
			e.Operations = append(e.Operations, opsToStrings(rule.Operations)...)
		}
		e.APIGroups = dedupSorted(sortedCopy(e.APIGroups))
		e.Resources = dedupSorted(sortedCopy(e.Resources))
		e.Operations = dedupSorted(sortedCopy(e.Operations))
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	return entries
}

func extractValidatingWebhooks(t *testing.T, obj *unstructured.Unstructured) []webhookEntry {
	t.Helper()
	wh := convertTo[admissionregistrationv1.ValidatingWebhookConfiguration](t, obj)
	var entries []webhookEntry
	for _, w := range wh.Webhooks {
		e := webhookEntry{
			FailPolicy:        string(*w.FailurePolicy),
			SideEffects:       string(*w.SideEffects),
			ObjectSelector:    w.ObjectSelector,
			NamespaceSelector: w.NamespaceSelector,
		}
		if w.ClientConfig.Service != nil {
			e.Path = derefStr(w.ClientConfig.Service.Path)
		}
		for _, rule := range w.Rules {
			e.APIGroups = append(e.APIGroups, rule.APIGroups...)
			e.Resources = append(e.Resources, rule.Resources...)
			e.Operations = append(e.Operations, opsToStrings(rule.Operations)...)
		}
		e.APIGroups = dedupSorted(sortedCopy(e.APIGroups))
		e.Resources = dedupSorted(sortedCopy(e.Resources))
		e.Operations = dedupSorted(sortedCopy(e.Operations))
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	return entries
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func opsToStrings(ops []admissionregistrationv1.OperationType) []string {
	out := make([]string, len(ops))
	for i, o := range ops {
		out[i] = string(o)
	}
	sort.Strings(out)
	return out
}

// ---------------------------------------------------------------------------
// Deployment comparison helpers
// ---------------------------------------------------------------------------

type containerSpec struct {
	Args            []string
	Ports           []portSpec
	LivenessPath    string
	LivenessPort    int
	ReadinessPath   string
	ReadinessPort   int
	SecurityContext map[string]interface{}
}

type portSpec struct {
	Name          string
	ContainerPort int32
}

// argsToIgnoreValue contains arg prefixes whose values are expected to differ
// due to naming conventions between Helm and Kustomize. We compare only that
// the arg key exists on both sides.
var argsToIgnoreValue = []string{
	"--mutating-webhook-name=",
	"--validating-webhook-name=",
}

func normalizeArg(arg string) string {
	for _, prefix := range argsToIgnoreValue {
		if strings.HasPrefix(arg, prefix) {
			return prefix + "<ignored>"
		}
	}
	return arg
}

func extractContainer(t *testing.T, obj *unstructured.Unstructured, containerName string) containerSpec {
	t.Helper()
	dep := convertTo[appsv1.Deployment](t, obj)

	for _, c := range dep.Spec.Template.Spec.Containers {
		if c.Name != containerName {
			continue
		}

		spec := containerSpec{}

		for _, arg := range c.Args {
			normalized := arg
			normalized = strings.ReplaceAll(normalized, "$(CONTROLLER_NAMESPACE)", helmNamespace)
			normalized = strings.ReplaceAll(normalized, "$(WEBHOOK_NAMESPACE)", helmNamespace)
			// Normalize --namespaces="" to --namespaces=
			normalized = strings.TrimSuffix(normalized, `""`)
			normalized = normalizeArg(normalized)
			spec.Args = append(spec.Args, normalized)
		}
		sort.Strings(spec.Args)

		for _, p := range c.Ports {
			spec.Ports = append(spec.Ports, portSpec{Name: p.Name, ContainerPort: p.ContainerPort})
		}
		sort.Slice(spec.Ports, func(i, j int) bool {
			return spec.Ports[i].ContainerPort < spec.Ports[j].ContainerPort
		})

		if c.LivenessProbe != nil && c.LivenessProbe.HTTPGet != nil {
			spec.LivenessPath = c.LivenessProbe.HTTPGet.Path
			spec.LivenessPort = c.LivenessProbe.HTTPGet.Port.IntValue()
		}
		if c.ReadinessProbe != nil && c.ReadinessProbe.HTTPGet != nil {
			spec.ReadinessPath = c.ReadinessProbe.HTTPGet.Path
			spec.ReadinessPort = c.ReadinessProbe.HTTPGet.Port.IntValue()
		}

		if c.SecurityContext != nil {
			scData, _ := json.Marshal(c.SecurityContext)
			_ = json.Unmarshal(scData, &spec.SecurityContext)
		}

		return spec
	}

	t.Fatalf("container %q not found in deployment %s", containerName, obj.GetName())
	return containerSpec{}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestDrift(t *testing.T) {
	helmResources := renderHelm(t)
	kustResources := renderKustomize(t)

	t.Run("ResourceInventory", func(t *testing.T) {
		helmKinds := make(map[string]int)
		for _, r := range helmResources {
			if !ignoredKinds[r.GetKind()] {
				helmKinds[r.GetKind()]++
			}
		}
		kustKinds := make(map[string]int)
		for _, r := range kustResources {
			if !ignoredKinds[r.GetKind()] {
				kustKinds[r.GetKind()]++
			}
		}

		for kind := range helmKinds {
			assert.Contains(t, kustKinds, kind,
				"resource kind %q exists in Helm but not in Kustomize", kind)
		}
		for kind := range kustKinds {
			assert.Contains(t, helmKinds, kind,
				"resource kind %q exists in Kustomize but not in Helm", kind)
		}
	})

	t.Run("RBAC", func(t *testing.T) {
		t.Run("ControllerClusterRole", func(t *testing.T) {
			helmObj := findResource(helmResources, "ClusterRole", controllerClusterRole)
			kustObj := findResource(kustResources, "ClusterRole", controllerClusterRole)
			require.NotNil(t, helmObj, "Helm ClusterRole %q not found", controllerClusterRole)
			require.NotNil(t, kustObj, "Kustomize ClusterRole %q not found", controllerClusterRole)

			helmCR := convertTo[rbacv1.ClusterRole](t, helmObj)
			kustCR := convertTo[rbacv1.ClusterRole](t, kustObj)

			assert.Equal(t, normalizeRules(helmCR.Rules), normalizeRules(kustCR.Rules),
				"ClusterRole %q rules differ between Helm and Kustomize", controllerClusterRole)
		})

		t.Run("WebhookClusterRole", func(t *testing.T) {
			helmObj := findResource(helmResources, "ClusterRole", webhookClusterRole)
			kustObj := findResource(kustResources, "ClusterRole", webhookClusterRole)
			require.NotNil(t, helmObj, "Helm ClusterRole %q not found", webhookClusterRole)
			require.NotNil(t, kustObj, "Kustomize ClusterRole %q not found", webhookClusterRole)

			helmCR := convertTo[rbacv1.ClusterRole](t, helmObj)
			kustCR := convertTo[rbacv1.ClusterRole](t, kustObj)

			assert.Equal(t, normalizeRules(helmCR.Rules), normalizeRules(kustCR.Rules),
				"ClusterRole %q rules differ between Helm and Kustomize", webhookClusterRole)
		})

		t.Run("ControllerRole", func(t *testing.T) {
			helmObj := findResource(helmResources, "Role", helmControllerRole)
			kustObj := findResource(kustResources, "Role", kustControllerRole)
			require.NotNil(t, helmObj, "Helm Role %q not found", helmControllerRole)
			require.NotNil(t, kustObj, "Kustomize Role %q not found", kustControllerRole)

			helmRole := convertTo[rbacv1.Role](t, helmObj)
			kustRole := convertTo[rbacv1.Role](t, kustObj)

			assert.Equal(t, normalizeRules(helmRole.Rules), normalizeRules(kustRole.Rules),
				"Role rules differ (Helm: %q, Kustomize: %q)", helmControllerRole, kustControllerRole)
		})

		t.Run("WebhookRole", func(t *testing.T) {
			helmObj := findResource(helmResources, "Role", helmWebhookRole)
			kustObj := findResource(kustResources, "Role", kustWebhookRole)
			require.NotNil(t, helmObj, "Helm Role %q not found", helmWebhookRole)
			require.NotNil(t, kustObj, "Kustomize Role %q not found", kustWebhookRole)

			helmRole := convertTo[rbacv1.Role](t, helmObj)
			kustRole := convertTo[rbacv1.Role](t, kustObj)

			assert.Equal(t, normalizeRules(helmRole.Rules), normalizeRules(kustRole.Rules),
				"Role rules differ (Helm: %q, Kustomize: %q)", helmWebhookRole, kustWebhookRole)
		})
	})

	t.Run("WebhookConfiguration", func(t *testing.T) {
		t.Run("MutatingWebhookPaths", func(t *testing.T) {
			helmObj := findResource(helmResources, "MutatingWebhookConfiguration", helmMutatingWH)
			kustObj := findResource(kustResources, "MutatingWebhookConfiguration", kustMutatingWH)
			require.NotNil(t, helmObj, "Helm MutatingWebhookConfiguration not found")
			require.NotNil(t, kustObj, "Kustomize MutatingWebhookConfiguration not found")

			helmEntries := extractMutatingWebhooks(t, helmObj)
			kustEntries := extractMutatingWebhooks(t, kustObj)

			helmPaths := make([]string, len(helmEntries))
			for i, e := range helmEntries {
				helmPaths[i] = e.Path
			}
			kustPaths := make([]string, len(kustEntries))
			for i, e := range kustEntries {
				kustPaths[i] = e.Path
			}
			assert.Equal(t, helmPaths, kustPaths,
				"MutatingWebhookConfiguration paths differ")
		})

		t.Run("MutatingWebhookRules", func(t *testing.T) {
			helmObj := findResource(helmResources, "MutatingWebhookConfiguration", helmMutatingWH)
			kustObj := findResource(kustResources, "MutatingWebhookConfiguration", kustMutatingWH)
			require.NotNil(t, helmObj)
			require.NotNil(t, kustObj)

			helmEntries := extractMutatingWebhooks(t, helmObj)
			kustEntries := extractMutatingWebhooks(t, kustObj)

			// Compare per-path entries by matching on path
			kustByPath := make(map[string]webhookEntry)
			for _, e := range kustEntries {
				kustByPath[e.Path] = e
			}
			for _, he := range helmEntries {
				ke, ok := kustByPath[he.Path]
				if !ok {
					t.Errorf("MutatingWebhook path %q exists in Helm but not Kustomize", he.Path)
					continue
				}
				assert.Equal(t, he.FailPolicy, ke.FailPolicy,
					"MutatingWebhook %s: failurePolicy differs", he.Path)
				assert.Equal(t, sortedCopy(he.Resources), sortedCopy(ke.Resources),
					"MutatingWebhook %s: resources differ", he.Path)
				assert.Equal(t, sortedCopy(he.Operations), sortedCopy(ke.Operations),
					"MutatingWebhook %s: operations differ", he.Path)
			}
		})

		t.Run("MutatingWebhookSelectors", func(t *testing.T) {
			helmObj := findResource(helmResources, "MutatingWebhookConfiguration", helmMutatingWH)
			kustObj := findResource(kustResources, "MutatingWebhookConfiguration", kustMutatingWH)
			require.NotNil(t, helmObj)
			require.NotNil(t, kustObj)

			helmEntries := extractMutatingWebhooks(t, helmObj)
			kustEntries := extractMutatingWebhooks(t, kustObj)

			kustByPath := make(map[string]webhookEntry)
			for _, e := range kustEntries {
				kustByPath[e.Path] = e
			}
			for _, he := range helmEntries {
				ke, ok := kustByPath[he.Path]
				if !ok {
					continue
				}
				assert.Equal(t, he.ObjectSelector, ke.ObjectSelector,
					"MutatingWebhook %s: objectSelector differs", he.Path)
				// Helm in cluster-wide mode (jobNamespaces: [""]) omits namespaceSelector,
				// while Kustomize hardcodes it via strategic-merge patches (controller-gen
				// cannot emit it). Only compare when Helm actually renders one.
				if he.NamespaceSelector != nil {
					assert.Equal(t, he.NamespaceSelector, ke.NamespaceSelector,
						"MutatingWebhook %s: namespaceSelector differs", he.Path)
				}
			}
		})

		t.Run("MutatingWebhookSideEffects", func(t *testing.T) {
			helmObj := findResource(helmResources, "MutatingWebhookConfiguration", helmMutatingWH)
			kustObj := findResource(kustResources, "MutatingWebhookConfiguration", kustMutatingWH)
			require.NotNil(t, helmObj)
			require.NotNil(t, kustObj)

			helmEntries := extractMutatingWebhooks(t, helmObj)
			kustEntries := extractMutatingWebhooks(t, kustObj)

			kustByPath := make(map[string]webhookEntry)
			for _, e := range kustEntries {
				kustByPath[e.Path] = e
			}
			for _, he := range helmEntries {
				ke, ok := kustByPath[he.Path]
				if !ok {
					continue
				}
				assert.Equal(t, he.SideEffects, ke.SideEffects,
					"MutatingWebhook %s: sideEffects differs", he.Path)
			}
		})

		t.Run("ValidatingWebhookPaths", func(t *testing.T) {
			helmObj := findResource(helmResources, "ValidatingWebhookConfiguration", helmValidWH)
			kustObj := findResource(kustResources, "ValidatingWebhookConfiguration", kustValidWH)
			require.NotNil(t, helmObj, "Helm ValidatingWebhookConfiguration not found")
			require.NotNil(t, kustObj, "Kustomize ValidatingWebhookConfiguration not found")

			helmEntries := extractValidatingWebhooks(t, helmObj)
			kustEntries := extractValidatingWebhooks(t, kustObj)

			helmPaths := make([]string, len(helmEntries))
			for i, e := range helmEntries {
				helmPaths[i] = e.Path
			}
			kustPaths := make([]string, len(kustEntries))
			for i, e := range kustEntries {
				kustPaths[i] = e.Path
			}
			assert.Equal(t, helmPaths, kustPaths,
				"ValidatingWebhookConfiguration paths differ")
		})

		t.Run("ValidatingWebhookRules", func(t *testing.T) {
			helmObj := findResource(helmResources, "ValidatingWebhookConfiguration", helmValidWH)
			kustObj := findResource(kustResources, "ValidatingWebhookConfiguration", kustValidWH)
			require.NotNil(t, helmObj)
			require.NotNil(t, kustObj)

			helmEntries := extractValidatingWebhooks(t, helmObj)
			kustEntries := extractValidatingWebhooks(t, kustObj)

			kustByPath := make(map[string]webhookEntry)
			for _, e := range kustEntries {
				kustByPath[e.Path] = e
			}
			for _, he := range helmEntries {
				ke, ok := kustByPath[he.Path]
				if !ok {
					t.Errorf("ValidatingWebhook path %q exists in Helm but not Kustomize", he.Path)
					continue
				}
				assert.Equal(t, he.FailPolicy, ke.FailPolicy,
					"ValidatingWebhook %s: failurePolicy differs", he.Path)
				assert.Equal(t, sortedCopy(he.Resources), sortedCopy(ke.Resources),
					"ValidatingWebhook %s: resources differ", he.Path)
				assert.Equal(t, sortedCopy(he.Operations), sortedCopy(ke.Operations),
					"ValidatingWebhook %s: operations differ", he.Path)
			}
		})

		t.Run("ValidatingWebhookSelectors", func(t *testing.T) {
			helmObj := findResource(helmResources, "ValidatingWebhookConfiguration", helmValidWH)
			kustObj := findResource(kustResources, "ValidatingWebhookConfiguration", kustValidWH)
			require.NotNil(t, helmObj)
			require.NotNil(t, kustObj)

			helmEntries := extractValidatingWebhooks(t, helmObj)
			kustEntries := extractValidatingWebhooks(t, kustObj)

			kustByPath := make(map[string]webhookEntry)
			for _, e := range kustEntries {
				kustByPath[e.Path] = e
			}
			for _, he := range helmEntries {
				ke, ok := kustByPath[he.Path]
				if !ok {
					continue
				}
				assert.Equal(t, he.ObjectSelector, ke.ObjectSelector,
					"ValidatingWebhook %s: objectSelector differs", he.Path)
				// See MutatingWebhookSelectors comment: skip namespaceSelector when
				// Helm renders nil (cluster-wide mode).
				if he.NamespaceSelector != nil {
					assert.Equal(t, he.NamespaceSelector, ke.NamespaceSelector,
						"ValidatingWebhook %s: namespaceSelector differs", he.Path)
				}
			}
		})

		t.Run("ValidatingWebhookSideEffects", func(t *testing.T) {
			helmObj := findResource(helmResources, "ValidatingWebhookConfiguration", helmValidWH)
			kustObj := findResource(kustResources, "ValidatingWebhookConfiguration", kustValidWH)
			require.NotNil(t, helmObj)
			require.NotNil(t, kustObj)

			helmEntries := extractValidatingWebhooks(t, helmObj)
			kustEntries := extractValidatingWebhooks(t, kustObj)

			kustByPath := make(map[string]webhookEntry)
			for _, e := range kustEntries {
				kustByPath[e.Path] = e
			}
			for _, he := range helmEntries {
				ke, ok := kustByPath[he.Path]
				if !ok {
					continue
				}
				assert.Equal(t, he.SideEffects, ke.SideEffects,
					"ValidatingWebhook %s: sideEffects differs", he.Path)
			}
		})
	})

	t.Run("DeploymentSpecs", func(t *testing.T) {
		t.Run("ControllerArgs", func(t *testing.T) {
			helmObj := findResource(helmResources, "Deployment", helmControllerDeploy)
			kustObj := findResource(kustResources, "Deployment", kustControllerDeploy)
			require.NotNil(t, helmObj, "Helm controller Deployment not found")
			require.NotNil(t, kustObj, "Kustomize controller Deployment not found")

			helmSpec := extractContainer(t, helmObj, helmControllerContainer)
			kustSpec := extractContainer(t, kustObj, kustControllerContainer)

			assert.Equal(t, helmSpec.Args, kustSpec.Args,
				"Controller Deployment: container args differ")
		})

		t.Run("ControllerPorts", func(t *testing.T) {
			helmObj := findResource(helmResources, "Deployment", helmControllerDeploy)
			kustObj := findResource(kustResources, "Deployment", kustControllerDeploy)
			require.NotNil(t, helmObj)
			require.NotNil(t, kustObj)

			helmSpec := extractContainer(t, helmObj, helmControllerContainer)
			kustSpec := extractContainer(t, kustObj, kustControllerContainer)

			assert.Equal(t, helmSpec.Ports, kustSpec.Ports,
				"Controller Deployment: ports differ")
		})

		t.Run("ControllerProbes", func(t *testing.T) {
			helmObj := findResource(helmResources, "Deployment", helmControllerDeploy)
			kustObj := findResource(kustResources, "Deployment", kustControllerDeploy)
			require.NotNil(t, helmObj)
			require.NotNil(t, kustObj)

			helmSpec := extractContainer(t, helmObj, helmControllerContainer)
			kustSpec := extractContainer(t, kustObj, kustControllerContainer)

			assert.Equal(t, helmSpec.LivenessPath, kustSpec.LivenessPath, "Controller: livenessProbe path differs")
			assert.Equal(t, helmSpec.LivenessPort, kustSpec.LivenessPort, "Controller: livenessProbe port differs")
			assert.Equal(t, helmSpec.ReadinessPath, kustSpec.ReadinessPath, "Controller: readinessProbe path differs")
			assert.Equal(t, helmSpec.ReadinessPort, kustSpec.ReadinessPort, "Controller: readinessProbe port differs")
		})

		t.Run("ControllerSecurityContext", func(t *testing.T) {
			helmObj := findResource(helmResources, "Deployment", helmControllerDeploy)
			kustObj := findResource(kustResources, "Deployment", kustControllerDeploy)
			require.NotNil(t, helmObj)
			require.NotNil(t, kustObj)

			helmSpec := extractContainer(t, helmObj, helmControllerContainer)
			kustSpec := extractContainer(t, kustObj, kustControllerContainer)

			assert.Equal(t, helmSpec.SecurityContext, kustSpec.SecurityContext,
				"Controller Deployment: securityContext differs")
		})

		t.Run("WebhookArgs", func(t *testing.T) {
			helmObj := findResource(helmResources, "Deployment", helmWebhookDeploy)
			kustObj := findResource(kustResources, "Deployment", kustWebhookDeploy)
			require.NotNil(t, helmObj, "Helm webhook Deployment not found")
			require.NotNil(t, kustObj, "Kustomize webhook Deployment not found")

			helmSpec := extractContainer(t, helmObj, helmWebhookContainer)
			kustSpec := extractContainer(t, kustObj, kustWebhookContainer)

			assert.Equal(t, helmSpec.Args, kustSpec.Args,
				"Webhook Deployment: container args differ")
		})

		t.Run("WebhookPorts", func(t *testing.T) {
			helmObj := findResource(helmResources, "Deployment", helmWebhookDeploy)
			kustObj := findResource(kustResources, "Deployment", kustWebhookDeploy)
			require.NotNil(t, helmObj)
			require.NotNil(t, kustObj)

			helmSpec := extractContainer(t, helmObj, helmWebhookContainer)
			kustSpec := extractContainer(t, kustObj, kustWebhookContainer)

			assert.Equal(t, helmSpec.Ports, kustSpec.Ports,
				"Webhook Deployment: ports differ")
		})

		t.Run("WebhookProbes", func(t *testing.T) {
			helmObj := findResource(helmResources, "Deployment", helmWebhookDeploy)
			kustObj := findResource(kustResources, "Deployment", kustWebhookDeploy)
			require.NotNil(t, helmObj)
			require.NotNil(t, kustObj)

			helmSpec := extractContainer(t, helmObj, helmWebhookContainer)
			kustSpec := extractContainer(t, kustObj, kustWebhookContainer)

			assert.Equal(t, helmSpec.LivenessPath, kustSpec.LivenessPath, "Webhook: livenessProbe path differs")
			assert.Equal(t, helmSpec.LivenessPort, kustSpec.LivenessPort, "Webhook: livenessProbe port differs")
			assert.Equal(t, helmSpec.ReadinessPath, kustSpec.ReadinessPath, "Webhook: readinessProbe path differs")
			assert.Equal(t, helmSpec.ReadinessPort, kustSpec.ReadinessPort, "Webhook: readinessProbe port differs")
		})

		t.Run("WebhookSecurityContext", func(t *testing.T) {
			helmObj := findResource(helmResources, "Deployment", helmWebhookDeploy)
			kustObj := findResource(kustResources, "Deployment", kustWebhookDeploy)
			require.NotNil(t, helmObj)
			require.NotNil(t, kustObj)

			helmSpec := extractContainer(t, helmObj, helmWebhookContainer)
			kustSpec := extractContainer(t, kustObj, kustWebhookContainer)

			assert.Equal(t, helmSpec.SecurityContext, kustSpec.SecurityContext,
				"Webhook Deployment: securityContext differs")
		})
	})
}
