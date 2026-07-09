/*
Copyright 2025 The Kubeflow authors.

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
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

func TestSparkApplicationValidatorValidateCreate_NodeSelectorConflict(t *testing.T) {
	validator := newTestValidator(t, false)

	app := newSparkApplication()
	app.Spec.NodeSelector = map[string]string{"role": "shared"}
	app.Spec.Driver.NodeSelector = map[string]string{"role": "driver"}

	if _, err := validator.ValidateCreate(context.Background(), app); err == nil || !strings.Contains(err.Error(), "node selector cannot be defined") {
		t.Fatalf("expected node selector validation error, got %v", err)
	}
}

func TestSparkApplicationValidatorValidateCreate_Success(t *testing.T) {
	validator := newTestValidator(t, false)

	if _, err := validator.ValidateCreate(context.Background(), newSparkApplication()); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
}

func TestSparkApplicationValidatorValidateCreate_DriverIngressDuplicatePort(t *testing.T) {
	validator := newTestValidator(t, false)

	app := newSparkApplication()
	app.Spec.DriverIngressOptions = []v1beta2.DriverIngressConfiguration{
		{
			ServicePort:      ptr.To[int32](4040),
			IngressURLFormat: "http://spark-a",
		},
		{
			ServicePort:      ptr.To[int32](4040),
			IngressURLFormat: "http://spark-b",
		},
	}

	if _, err := validator.ValidateCreate(context.Background(), app); err == nil || !strings.Contains(err.Error(), "duplicate ServicePort") {
		t.Fatalf("expected duplicate service port error, got %v", err)
	}
}

func TestSparkApplicationValidatorValidateCreate_PodTemplateRequiresSpark3(t *testing.T) {
	validator := newTestValidator(t, false)

	app := newSparkApplication()
	app.Spec.SparkVersion = "2.4.0"
	app.Spec.Driver.Template = &corev1.PodTemplateSpec{}

	if _, err := validator.ValidateCreate(context.Background(), app); err == nil || !strings.Contains(err.Error(), "requires Spark version 3.0.0 or higher") {
		t.Fatalf("expected spark version validation error, got %v", err)
	}
}

func TestSparkApplicationValidatorValidateCreate_ResourceQuotaSatisfied(t *testing.T) {
	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ample",
			Namespace: "default",
		},
		Spec: corev1.ResourceQuotaSpec{
			Hard: corev1.ResourceList{
				corev1.ResourceCPU:         resource.MustParse("20"),
				corev1.ResourceRequestsCPU: resource.MustParse("20"),
				corev1.ResourceLimitsCPU:   resource.MustParse("20"),
			},
		},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceCPU:         resource.MustParse("20"),
				corev1.ResourceRequestsCPU: resource.MustParse("20"),
				corev1.ResourceLimitsCPU:   resource.MustParse("20"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceCPU:         resource.MustParse("0"),
				corev1.ResourceRequestsCPU: resource.MustParse("0"),
				corev1.ResourceLimitsCPU:   resource.MustParse("0"),
			},
		},
	}

	validator := newTestValidator(t, true, quota)

	if _, err := validator.ValidateCreate(context.Background(), newSparkApplication()); err != nil {
		t.Fatalf("expected quota satisfied, got %v", err)
	}
}

func TestSparkApplicationValidatorValidateUpdate_SameSpecSkipsValidation(t *testing.T) {
	validator := newTestValidator(t, true)

	base := newSparkApplication()
	base.Spec.NodeSelector = map[string]string{"role": "shared"}
	base.Spec.Driver.NodeSelector = map[string]string{"role": "driver"}

	oldApp := base.DeepCopy()
	newApp := base.DeepCopy()

	if _, err := validator.ValidateUpdate(context.Background(), oldApp, newApp); err != nil {
		t.Fatalf("expected no error when spec unchanged, got %v", err)
	}
}

func TestSparkApplicationValidatorValidateUpdate_SpecChangedTriggersValidation(t *testing.T) {
	validator := newTestValidator(t, false)

	oldApp := newSparkApplication()
	newApp := oldApp.DeepCopy()
	newApp.Spec.NodeSelector = map[string]string{"role": "shared"}
	newApp.Spec.Driver.NodeSelector = map[string]string{"role": "driver"}

	if _, err := validator.ValidateUpdate(context.Background(), oldApp, newApp); err == nil || !strings.Contains(err.Error(), "node selector cannot be defined") {
		t.Fatalf("expected node selector validation error, got %v", err)
	}
}

func TestSparkApplicationValidatorValidateUpdate_SuccessWithSpecChange(t *testing.T) {
	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ample",
			Namespace: "default",
		},
		Spec: corev1.ResourceQuotaSpec{
			Hard: corev1.ResourceList{
				corev1.ResourceCPU:         resource.MustParse("20"),
				corev1.ResourceRequestsCPU: resource.MustParse("20"),
				corev1.ResourceLimitsCPU:   resource.MustParse("20"),
			},
		},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceCPU:         resource.MustParse("20"),
				corev1.ResourceRequestsCPU: resource.MustParse("20"),
				corev1.ResourceLimitsCPU:   resource.MustParse("20"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceCPU:         resource.MustParse("1"),
				corev1.ResourceRequestsCPU: resource.MustParse("1"),
				corev1.ResourceLimitsCPU:   resource.MustParse("1"),
			},
		},
	}

	validator := newTestValidator(t, true, quota)

	oldApp := newSparkApplication()
	newApp := oldApp.DeepCopy()
	newApp.Spec.Arguments = []string{"--foo"}

	if _, err := validator.ValidateUpdate(context.Background(), oldApp, newApp); err != nil {
		t.Fatalf("expected successful update validation, got %v", err)
	}
}

func TestSparkApplicationValidatorValidateCreate_ResourceQuotaExceeded(t *testing.T) {
	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "strict",
			Namespace: "default",
		},
		Spec: corev1.ResourceQuotaSpec{
			Hard: corev1.ResourceList{
				corev1.ResourceLimitsCPU: resource.MustParse("1"),
			},
		},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceLimitsCPU: resource.MustParse("1"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceLimitsCPU: resource.MustParse("0"),
			},
		},
	}

	validator := newTestValidator(t, true, quota)

	if _, err := validator.ValidateCreate(context.Background(), newSparkApplication()); err == nil || !strings.Contains(err.Error(), "failed to validate resource quota") {
		t.Fatalf("expected resource quota validation error, got %v", err)
	}
}

func TestSparkApplicationValidatorValidateDelete_Success(t *testing.T) {
	validator := newTestValidator(t, false)

	if _, err := validator.ValidateDelete(context.Background(), newSparkApplication()); err != nil {
		t.Fatalf("expected successful delete validation, got %v", err)
	}
}

func newTestValidator(t *testing.T, enforceQuota bool, objs ...client.Object) *SparkApplicationValidator {
	t.Helper()
	// URL-scheme validation is opt-in and off here; the dedicated URL-scheme tests enable it
	// explicitly via newTestValidatorWithSchemes.
	return newTestValidatorWithSchemes(t, enforceQuota, false, nil, objs...)
}

func newTestValidatorWithSchemes(t *testing.T, enforceQuota bool, enableURLSchemeValidation bool, allowedSchemes []string, objs ...client.Object) *SparkApplicationValidator {
	t.Helper()

	scheme := newTestScheme(t)

	builder := fake.NewClientBuilder().WithScheme(scheme)
	if len(objs) > 0 {
		builder = builder.WithObjects(objs...)
	}

	return NewSparkApplicationValidator(builder.Build(), enforceQuota, enableURLSchemeValidation, allowedSchemes)
}

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add corev1 to scheme: %v", err)
	}
	if err := v1beta2.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add v1beta2 to scheme: %v", err)
	}
	return scheme
}

func TestSparkApplicationValidatorValidateName(t *testing.T) {
	validator := newTestValidator(t, false)

	tests := []struct {
		name      string
		appName   string
		wantError bool
	}{
		// Valid names
		{"valid simple name", "test-app", false},
		{"valid name with numbers", "test-app-123", false},
		{"valid single letter", "a", false},
		{"valid name ending with number", "my-app-1", false},
		{"valid name with multiple hyphens", "my-test-app-123", false},
		{"valid 63 char name", strings.Repeat("a", 63), false},
		{"valid name with hyphens in middle", "a-b-c-d-e", false},

		// Invalid names
		{"name starting with number", "123test-app", true},
		{"name with uppercase", "Test-App", true},
		{"name with uppercase at start", "TestApp", true},
		{"name with uppercase in middle", "test-App", true},
		{"name starting with hyphen", "-test-app", true},
		{"name ending with hyphen", "test-app-", true},
		{"name with consecutive hyphens", "test--app", false}, // Kubernetes validation allows consecutive hyphens
		{"empty name", "", true},
		{"name too long", strings.Repeat("a", 64), true},
		{"name with special characters", "test@app", true},
		{"name with underscore", "test_app", true},
		{"name with spaces", "test app", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := newSparkApplication()
			app.Name = tt.appName

			_, err := validator.ValidateCreate(context.Background(), app)
			hasError := err != nil

			if hasError != tt.wantError {
				t.Errorf("validateName(%q) = error %v, wantError %v, got error: %v", tt.appName, hasError, tt.wantError, err)
			}

			if hasError && err.Error() == "" {
				t.Errorf("validateName(%q) should return a non-empty error message, got: %v", tt.appName, err)
			}
		})
	}
}

// TestDepsURLFieldsExempt guards the depsURLFieldsExempt carve-out. validateDepsURLSchemes
// URL-checks every []string field of v1beta2.Dependencies by reflection except the exempt tags,
// so a newly-added fetch field is covered automatically. This test only has to ensure the
// exempt set does not drift: every exempt tag must name a real []string field on the struct
// (a stale entry would silently skip a future field that reused the name).
func TestDepsURLFieldsExempt(t *testing.T) {
	depsType := reflect.TypeFor[v1beta2.Dependencies]()
	stringListTags := make(map[string]struct{}, depsType.NumField())
	for i := range depsType.NumField() {
		f := depsType.Field(i)
		if f.Type != reflect.TypeFor[[]string]() {
			continue
		}
		tag := strings.Split(f.Tag.Get("json"), ",")[0]
		if tag == "" || tag == "-" {
			continue
		}
		stringListTags[tag] = struct{}{}
	}

	for _, tag := range depsURLFieldsExempt {
		if _, ok := stringListTags[tag]; !ok {
			t.Errorf("depsURLFieldsExempt tag %q does not name a []string field on v1beta2.Dependencies; "+
				"remove it or fix the tag", tag)
		}
	}
}

func TestSparkApplicationValidatorSparkConfURLSchemes(t *testing.T) {
	tests := []struct {
		name           string
		allowedSchemes []string
		sparkConf      map[string]string
		wantError      bool
		errContains    string
	}{
		// file:// and schemeless are always allowed regardless of the list.
		{
			name:           "file:// always allowed with empty list",
			allowedSchemes: nil,
			sparkConf:      map[string]string{"spark.jars": "file:///opt/spark/jars/app.jar"},
			wantError:      false,
		},
		{
			name:           "schemeless path always allowed with empty list",
			allowedSchemes: nil,
			sparkConf:      map[string]string{"spark.jars": "/opt/spark/jars/app.jar"},
			wantError:      false,
		},
		{
			// Runtime-only sparkConf keys are not forwarded to spark-submit by the operator, so
			// they are never scheme-checked even with a scheme that would otherwise be rejected.
			// This is the key-based allow-set at work (sparkConfURLKeys): submit-time keys only.
			name:           "runtime-only key not checked",
			allowedSchemes: nil,
			sparkConf:      map[string]string{"spark.eventLog.dir": "s3a://logs/bucket"},
			wantError:      false,
		},
		{
			name:           "local:// always allowed with empty list",
			allowedSchemes: nil,
			sparkConf:      map[string]string{"spark.jars": "local:///opt/spark/jars/app.jar"},
			wantError:      false,
		},
		// Any scheme not in the always-allowed set or the extra list is rejected; the
		// error names the offending key. http represents all such schemes (https, ftp, gs, ...).
		{
			name:           "disallowed scheme rejected with empty extra list",
			allowedSchemes: nil,
			sparkConf:      map[string]string{"spark.jars": "http://example.com/app.jar"},
			wantError:      true,
			errContains:    `spec.sparkConf["spark.jars"]`,
		},
		// A scheme is allowed once added to the extra list.
		{
			name:           "scheme allowed when added to extra list",
			allowedSchemes: []string{"gs", "s3a"},
			sparkConf:      map[string]string{"spark.jars": "gs://my-bucket/app.jar"},
			wantError:      false,
		},
		// Comma-separated values are each checked.
		{
			name:           "comma-separated: one disallowed entry rejects",
			allowedSchemes: []string{"gs"},
			sparkConf:      map[string]string{"spark.jars": "gs://ok.jar,https://evil.com/bad.jar"},
			wantError:      true,
			errContains:    "https",
		},
		{
			name:           "comma-separated: all in extra list passes",
			allowedSchemes: []string{"gs", "s3a"},
			sparkConf:      map[string]string{"spark.jars": "gs://bucket/a.jar,s3a://bucket/b.jar"},
			wantError:      false,
		},
		{
			// Allow-list entries are normalized to lower case in the constructor, so an
			// uppercase entry still matches a lower-case URL scheme.
			name:           "allow-list entry is case-insensitive",
			allowedSchemes: []string{"GS"},
			sparkConf:      map[string]string{"spark.jars": "gs://my-bucket/app.jar"},
			wantError:      false,
		},
		{
			name:           "empty sparkConf always passes",
			allowedSchemes: nil,
			sparkConf:      map[string]string{},
			wantError:      false,
		},
		{
			// Empty and whitespace-only values (incl. empty comma elements) are skipped.
			name:           "empty and blank values pass",
			allowedSchemes: nil,
			sparkConf:      map[string]string{"spark.jars": "  ", "spark.files": "file:///a.jar,,file:///b.jar"},
			wantError:      false,
		},
		// Defense-in-depth: scheme-relative "//host/path" has an empty scheme but a host, so it
		// is not a local path and must be rejected.
		{
			name:           "scheme-relative //host rejected",
			allowedSchemes: nil,
			sparkConf:      map[string]string{"spark.jars": "//attacker.example.com/evil.jar"},
			wantError:      true,
			errContains:    "scheme-relative",
		},
		// Defense-in-depth: a value with embedded control characters is not a valid URL and
		// must be rejected (fail closed) rather than waved through.
		{
			name:           "value with control character rejected",
			allowedSchemes: nil,
			sparkConf:      map[string]string{"spark.jars": "ht\ttp://attacker.example.com/evil.jar"},
			wantError:      true,
			errContains:    "not a valid URL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := newTestValidatorWithSchemes(t, false, true, tt.allowedSchemes)
			app := newSparkApplication()
			app.Spec.SparkConf = tt.sparkConf

			_, err := validator.ValidateCreate(context.Background(), app)
			if tt.wantError {
				if err == nil {
					t.Fatalf("expected error but got none")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Fatalf("expected error containing %q, got: %v", tt.errContains, err)
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got: %v", err)
				}
			}
		})
	}
}

// TestSparkApplicationValidatorURLSchemesAggregateAllErrors proves that a spec with several
// scheme violations across mainApplicationFile, deps.*, and sparkConf surfaces every violation
// in one admission response (so a user does not have to fix them one round-trip at a time), and
// that the message is deterministic: sparkConf keys are sorted rather than emitted in Go's
// randomized map order, so the same spec always produces byte-identical error text.
func TestSparkApplicationValidatorURLSchemesAggregateAllErrors(t *testing.T) {
	validator := newTestValidatorWithSchemes(t, false, true, nil)

	app := newSparkApplication()
	app.Spec.MainApplicationFile = ptr.To("http://evil.example.com/main.py")
	app.Spec.Deps.Repositories = []string{"http://repo.example.com/maven"}
	app.Spec.SparkConf = map[string]string{
		"spark.jars":  "https://a.example.com/a.jar",
		"spark.files": "ftp://b.example.com/b.txt",
	}

	_, err := validator.ValidateCreate(context.Background(), app)
	if err == nil {
		t.Fatalf("expected error but got none")
	}

	// Every offending field must appear in the aggregated message.
	wantSubstrings := []string{
		"spec.mainApplicationFile",
		"spec.deps.repositories",
		`spec.sparkConf["spark.files"]`,
		`spec.sparkConf["spark.jars"]`,
	}
	for _, want := range wantSubstrings {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("expected aggregated error to mention %q, got: %v", want, err)
		}
	}

	// The message must be stable across repeated validations of the same spec; map iteration
	// order would otherwise vary the sparkConf portion. Run several times and require identical
	// output, and require the sorted "spark.files" before "spark.jars" ordering explicitly.
	first := err.Error()
	for range 10 {
		_, e := validator.ValidateCreate(context.Background(), app)
		if e == nil || e.Error() != first {
			t.Fatalf("expected deterministic error text across runs;\n first: %v\n later: %v", first, e)
		}
	}
	if fi, ji := strings.Index(first, `spec.sparkConf["spark.files"]`), strings.Index(first, `spec.sparkConf["spark.jars"]`); fi > ji {
		t.Errorf("expected sparkConf keys in sorted order (spark.files before spark.jars), got: %v", first)
	}
}

// TestSparkApplicationValidatorURLSchemesOptIn proves the check is strictly opt-in: a spec that
// is rejected when validation is enabled is accepted unchanged when it is disabled (the default),
// so operators already submitting remote deps are not broken on upgrade.
func TestSparkApplicationValidatorURLSchemesOptIn(t *testing.T) {
	app := newSparkApplication()
	app.Spec.MainApplicationFile = ptr.To("http://evil.example.com/main.py")
	app.Spec.Deps.Repositories = []string{"http://repo.example.com/maven"}
	app.Spec.SparkConf = map[string]string{"spark.jars": "https://a.example.com/a.jar"}

	// Disabled (default): the http/https values must be accepted.
	disabled := newTestValidatorWithSchemes(t, false, false, nil)
	if _, err := disabled.ValidateCreate(context.Background(), app); err != nil {
		t.Fatalf("expected no error when URL-scheme validation is disabled, got: %v", err)
	}

	// Enabled: the very same spec must now be rejected, proving the gate is what flips behaviour.
	enabled := newTestValidatorWithSchemes(t, false, true, nil)
	if _, err := enabled.ValidateCreate(context.Background(), app); err == nil {
		t.Fatalf("expected error when URL-scheme validation is enabled, got none")
	}
}

// TestSparkApplicationValidatorURLSchemesOtherFields proves the check is applied to the
// fetch-capable fields beyond sparkConf (mainApplicationFile and the reflected deps.* lists),
// that the Maven-coordinate deps fields are carved out, and that sparkConf keys the operator does
// not dereference at submit time (Maven/Ivy keys) are simply not in the allow-set and therefore
// not checked. Scheme rules themselves are covered by the sparkConf table above; these cases only
// pin field wiring.
func TestSparkApplicationValidatorURLSchemesOtherFields(t *testing.T) {
	tests := []struct {
		name        string
		mutate      func(app *v1beta2.SparkApplication)
		wantError   bool
		errContains string
	}{
		{
			name: "mainApplicationFile http rejected",
			mutate: func(app *v1beta2.SparkApplication) {
				app.Spec.MainApplicationFile = ptr.To("http://evil.example.com/main.py")
			},
			wantError:   true,
			errContains: "spec.mainApplicationFile",
		},
		{
			name: "deps.repositories http rejected",
			mutate: func(app *v1beta2.SparkApplication) {
				app.Spec.Deps.Repositories = []string{"http://repo.example.com/maven"}
			},
			wantError:   true,
			errContains: "spec.deps.repositories",
		},
		{
			// Maven coordinates (groupId:artifactId:version) are not URLs and must not be
			// rejected even though url.Parse reads "com.example" as a scheme-like prefix.
			name:      "deps.packages maven coordinates not rejected",
			mutate:    func(app *v1beta2.SparkApplication) { app.Spec.Deps.Packages = []string{"com.example:my-lib:1.2.3"} },
			wantError: false,
		},
		{
			// spark.jars.packages holds Maven coordinates, not URLs; net/url parses
			// "org.postgresql:postgresql:42.7.3" as scheme "org.postgresql" but the key
			// is not in the submit-time allow-set (sparkConfURLKeys) so it is never checked.
			name: "sparkConf spark.jars.packages maven coordinates not rejected",
			mutate: func(app *v1beta2.SparkApplication) {
				if app.Spec.SparkConf == nil {
					app.Spec.SparkConf = make(map[string]string)
				}
				app.Spec.SparkConf["spark.jars.packages"] = "org.postgresql:postgresql:42.7.3"
			},
			wantError: false,
		},
		{
			// spark.jars.excludePackages is also Maven coordinates and not in the allow-set.
			name: "sparkConf spark.jars.excludePackages maven coordinates not rejected",
			mutate: func(app *v1beta2.SparkApplication) {
				if app.Spec.SparkConf == nil {
					app.Spec.SparkConf = make(map[string]string)
				}
				app.Spec.SparkConf["spark.jars.excludePackages"] = "org.slf4j:slf4j-api"
			},
			wantError: false,
		},
		{
			// spark.jars.ivySettings is consumed by Spark's Ivy subsystem in the driver, not
			// dereferenced by the operator at submit time, so it is not in sparkConfURLKeys and is
			// never checked - not even a remote scheme is rejected here.
			name: "sparkConf spark.jars.ivySettings not checked",
			mutate: func(app *v1beta2.SparkApplication) {
				if app.Spec.SparkConf == nil {
					app.Spec.SparkConf = make(map[string]string)
				}
				app.Spec.SparkConf["spark.jars.ivySettings"] = "/etc/ivy-settings.xml"
			},
			wantError: false,
		},
		{
			// spark.jars is still URL-checked; it holds JAR URLs, not Maven coordinates.
			name: "sparkConf spark.jars http rejected",
			mutate: func(app *v1beta2.SparkApplication) {
				if app.Spec.SparkConf == nil {
					app.Spec.SparkConf = make(map[string]string)
				}
				app.Spec.SparkConf["spark.jars"] = "http://evil.example.com/app.jar"
			},
			wantError:   true,
			errContains: `spec.sparkConf["spark.jars"]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := newTestValidatorWithSchemes(t, false, true, nil)
			app := newSparkApplication()
			tt.mutate(app)

			_, err := validator.ValidateCreate(context.Background(), app)
			if tt.wantError {
				if err == nil {
					t.Fatalf("expected error but got none")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Fatalf("expected error containing %q, got: %v", tt.errContains, err)
				}
			} else if err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}
		})
	}
}

func newSparkApplication() *v1beta2.SparkApplication {
	mainFile := "local:///app.py"
	return &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "default",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Type:                v1beta2.SparkApplicationTypeScala,
			SparkVersion:        "3.5.0",
			Mode:                v1beta2.DeployModeCluster,
			MainApplicationFile: &mainFile,
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Cores:  ptr.To[int32](1),
					Memory: ptr.To("1g"),
				},
			},
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Cores:  ptr.To[int32](1),
					Memory: ptr.To("1g"),
				},
				Instances: ptr.To[int32](1),
			},
		},
	}
}

var sparkConfSecurityVectors = []struct {
	name      string
	sparkConf map[string]string
}{
	{
		name:      "driver service account override",
		sparkConf: map[string]string{common.SparkKubernetesAuthenticateDriverServiceAccountName: "cluster-admin"},
	},
	{
		name:      "executor service account override",
		sparkConf: map[string]string{common.SparkKubernetesAuthenticateExecutorServiceAccountName: "cluster-admin"},
	},
	{
		name:      "submission OAuth token file path",
		sparkConf: map[string]string{common.SparkKubernetesAuthenticateOAuthTokenFile: "/var/run/secrets/attacker/token"},
	},
	{
		name:      "submission OAuth token injection",
		sparkConf: map[string]string{common.SparkKubernetesAuthenticateOAuthToken: "stolen-token"},
	},
	{
		name:      "driver OAuth token file path",
		sparkConf: map[string]string{common.SparkKubernetesAuthenticateDriverOAuthTokenFile: "/var/run/secrets/attacker/token"},
	},
	{
		name:      "driver OAuth token injection",
		sparkConf: map[string]string{common.SparkKubernetesAuthenticateDriverOAuthToken: "stolen-token"},
	},
	{
		name:      "executor OAuth token file path",
		sparkConf: map[string]string{common.SparkKubernetesAuthenticateExecutorOAuthTokenFile: "/var/run/secrets/attacker/token"},
	},
	{
		name:      "executor OAuth token injection",
		sparkConf: map[string]string{common.SparkKubernetesAuthenticateExecutorOAuthToken: "stolen-token"},
	},
	{
		name:      "namespace override",
		sparkConf: map[string]string{common.SparkKubernetesNamespace: "kube-system"},
	},
	{
		name:      "spark.master redirect",
		sparkConf: map[string]string{common.SparkMaster: "k8s://https://attacker-cluster:443"},
	},
	{
		name:      "spark.kubernetes.driver.master redirect",
		sparkConf: map[string]string{common.SparkKubernetesDriverMaster: "k8s://https://attacker-cluster:443"},
	},
	{
		name:      "container image override",
		sparkConf: map[string]string{common.SparkKubernetesContainerImage: "attacker/malicious-image:latest"},
	},
	{
		name:      "driver container image override",
		sparkConf: map[string]string{common.SparkKubernetesDriverContainerImage: "attacker/malicious-image:latest"},
	},
	{
		name:      "executor container image override",
		sparkConf: map[string]string{common.SparkKubernetesExecutorContainerImage: "attacker/malicious-image:latest"},
	},
}

func TestSparkApplicationValidatorSparkConf_SecurityVectorsRejected(t *testing.T) {
	validator := newTestValidator(t, false)

	for _, tt := range sparkConfSecurityVectors {
		t.Run(tt.name, func(t *testing.T) {
			app := newSparkApplication()
			app.Spec.SparkConf = tt.sparkConf

			if _, err := validator.ValidateCreate(context.Background(), app); err == nil {
				t.Fatalf("expected sparkConf to be rejected, but it was allowed")
			}
		})
	}
}

func TestSparkApplicationValidatorSparkConf_UpdateRejected(t *testing.T) {
	validator := newTestValidator(t, false)

	oldApp := newSparkApplication()
	newApp := newSparkApplication()
	newApp.Spec.SparkConf = map[string]string{common.SparkMaster: "k8s://https://attacker-cluster:443"}

	if _, err := validator.ValidateUpdate(context.Background(), oldApp, newApp); err == nil {
		t.Fatalf("expected sparkConf to be rejected on update, but it was allowed")
	}
}

func TestSparkApplicationValidatorSparkConf_TypedError(t *testing.T) {
	validator := newTestValidator(t, false)

	app := newSparkApplication()
	app.Spec.SparkConf = map[string]string{common.SparkMaster: "k8s://https://attacker-cluster:443"}

	_, err := validator.ValidateCreate(context.Background(), app)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}

	var denied *SparkConfKeyDeniedError
	if !errors.As(err, &denied) {
		t.Fatalf("expected SparkConfKeyDeniedError, got %T", err)
	}
	if denied.Key != common.SparkMaster {
		t.Fatalf("expected key %q, got %q", common.SparkMaster, denied.Key)
	}
}

func TestSparkApplicationValidatorSparkConf_BenignKeysPass(t *testing.T) {
	validator := newTestValidator(t, false)

	tests := []struct {
		name      string
		sparkConf map[string]string
	}{
		{name: "executor memory tuning", sparkConf: map[string]string{"spark.executor.memory": "4g"}},
		{name: "shuffle partitions tuning", sparkConf: map[string]string{"spark.sql.shuffle.partitions": "200"}},
		{name: "namespace matching app namespace", sparkConf: map[string]string{common.SparkKubernetesNamespace: "default"}},
		{name: "arbitrary user-defined key", sparkConf: map[string]string{"spark.myapp.customSetting": "42"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := newSparkApplication()
			app.Spec.SparkConf = tt.sparkConf

			if _, err := validator.ValidateCreate(context.Background(), app); err != nil {
				t.Fatalf("expected benign sparkConf to be allowed, got error: %v", err)
			}
		})
	}
}
