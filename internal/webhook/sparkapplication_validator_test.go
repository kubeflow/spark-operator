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
	"fmt"
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

	scheme := newTestScheme(t)

	builder := fake.NewClientBuilder().WithScheme(scheme)
	if len(objs) > 0 {
		builder = builder.WithObjects(objs...)
	}

	return NewSparkApplicationValidator(builder.Build(), enforceQuota)
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

func TestSparkApplicationValidatorValidateCreate_ConfigMapNames(t *testing.T) {
	validator := newTestValidator(t, false)

	tests := []struct {
		name       string
		mutate     func(app *v1beta2.SparkApplication)
		wantErrors []string
	}{
		{
			name:   "valid driver ConfigMap name",
			mutate: func(app *v1beta2.SparkApplication) { app.Spec.Driver.ConfigMaps = configMapRefs("spark.conf") },
		},
		{
			// Volume names are DNS labels, but ConfigMap names are DNS subdomains,
			// so the longest legal name must keep passing.
			name: "driver ConfigMap name at the length limit",
			mutate: func(app *v1beta2.SparkApplication) {
				app.Spec.Driver.ConfigMaps = configMapRefs(strings.Repeat("a", 253))
			},
		},
		{
			name:       "driver ConfigMap name with uppercase and underscore",
			mutate:     func(app *v1beta2.SparkApplication) { app.Spec.Driver.ConfigMaps = configMapRefs("MY_CONFIG") },
			wantErrors: []string{`spec.driver.configMaps[0].name has invalid ConfigMap name "MY_CONFIG"`},
		},
		{
			name: "invalid name after a valid one",
			mutate: func(app *v1beta2.SparkApplication) {
				app.Spec.Driver.ConfigMaps = configMapRefs("spark-conf", "MY_CONFIG")
			},
			wantErrors: []string{`spec.driver.configMaps[1].name has invalid ConfigMap name "MY_CONFIG"`},
		},
		{
			name:       "empty executor ConfigMap name",
			mutate:     func(app *v1beta2.SparkApplication) { app.Spec.Executor.ConfigMaps = configMapRefs("") },
			wantErrors: []string{`spec.executor.configMaps[0].name has invalid ConfigMap name ""`},
		},
		{
			name:       "Spark ConfigMap name with a space",
			mutate:     func(app *v1beta2.SparkApplication) { app.Spec.SparkConfigMap = ptr.To("spark conf") },
			wantErrors: []string{`spec.sparkConfigMap has invalid ConfigMap name "spark conf"`},
		},
		{
			name:       "Hadoop ConfigMap name that is too long",
			mutate:     func(app *v1beta2.SparkApplication) { app.Spec.HadoopConfigMap = ptr.To(strings.Repeat("a", 254)) },
			wantErrors: []string{fmt.Sprintf("spec.hadoopConfigMap has invalid ConfigMap name %q", strings.Repeat("a", 254))},
		},
		{
			name: "same ConfigMap mounted twice at different paths",
			mutate: func(app *v1beta2.SparkApplication) {
				app.Spec.Driver.ConfigMaps = configMapRefs("spark-conf", "spark-conf")
			},
		},
		{
			name: "duplicate driver ConfigMap mount paths",
			mutate: func(app *v1beta2.SparkApplication) {
				app.Spec.Driver.ConfigMaps = []v1beta2.NamePath{
					{Name: "spark-conf", Path: "/etc/spark/conf"},
					{Name: "other-conf", Path: "/etc/spark/conf"},
				}
			},
			wantErrors: []string{`spec.driver.configMaps[1].path has duplicate mount path "/etc/spark/conf"`},
		},
		{
			name: "same ConfigMap in both driver and executor",
			mutate: func(app *v1beta2.SparkApplication) {
				app.Spec.Driver.ConfigMaps = configMapRefs("spark-conf")
				app.Spec.Executor.ConfigMaps = configMapRefs("spark-conf")
			},
		},
		{
			name: "empty driver ConfigMap mount path",
			mutate: func(app *v1beta2.SparkApplication) {
				app.Spec.Driver.ConfigMaps = []v1beta2.NamePath{{Name: "spark-conf", Path: ""}}
			},
			wantErrors: []string{`spec.driver.configMaps[0].path must not be empty`},
		},
		{
			name: "driver ConfigMap mount path collides with sparkConfigMap",
			mutate: func(app *v1beta2.SparkApplication) {
				app.Spec.SparkConfigMap = ptr.To("spark-conf")
				app.Spec.Driver.ConfigMaps = []v1beta2.NamePath{{Name: "other-conf", Path: "/etc/spark/conf"}}
			},
			wantErrors: []string{`spec.driver.configMaps[0].path has mount path "/etc/spark/conf" reserved for sparkConfigMap`},
		},
		{
			name: "executor ConfigMap mount path collides with hadoopConfigMap",
			mutate: func(app *v1beta2.SparkApplication) {
				app.Spec.HadoopConfigMap = ptr.To("hadoop-conf")
				app.Spec.Executor.ConfigMaps = []v1beta2.NamePath{{Name: "other-conf", Path: "/etc/hadoop/conf"}}
			},
			wantErrors: []string{`spec.executor.configMaps[0].path has mount path "/etc/hadoop/conf" reserved for hadoopConfigMap`},
		},
		{
			name: "driver ConfigMap mount path collides with Prometheus ConfigMap",
			mutate: func(app *v1beta2.SparkApplication) {
				app.Spec.Monitoring = &v1beta2.MonitoringSpec{
					ExposeDriverMetrics: true,
					Prometheus:          &v1beta2.PrometheusSpec{JmxExporterJar: "/prometheus/jmx_prometheus_javaagent.jar"},
				}
				app.Spec.Driver.ConfigMaps = []v1beta2.NamePath{{Name: "other-conf", Path: "/etc/metrics/conf"}}
			},
			wantErrors: []string{`spec.driver.configMaps[0].path has mount path "/etc/metrics/conf" reserved for monitoring.prometheus`},
		},
		{
			name: "executor ConfigMap mount path matching Prometheus path is fine when executor metrics are not exposed",
			mutate: func(app *v1beta2.SparkApplication) {
				app.Spec.Monitoring = &v1beta2.MonitoringSpec{
					ExposeDriverMetrics: true,
					Prometheus:          &v1beta2.PrometheusSpec{JmxExporterJar: "/prometheus/jmx_prometheus_javaagent.jar"},
				}
				app.Spec.Executor.ConfigMaps = []v1beta2.NamePath{{Name: "other-conf", Path: "/etc/metrics/conf"}}
			},
		},
		{
			name: "every invalid name is reported",
			mutate: func(app *v1beta2.SparkApplication) {
				app.Spec.SparkConfigMap = ptr.To("BAD_SPARK")
				app.Spec.Driver.ConfigMaps = configMapRefs("BAD_DRIVER")
				app.Spec.Executor.ConfigMaps = configMapRefs("BAD_EXECUTOR")
			},
			wantErrors: []string{
				`spec.sparkConfigMap has invalid ConfigMap name "BAD_SPARK"`,
				`spec.driver.configMaps[0].name has invalid ConfigMap name "BAD_DRIVER"`,
				`spec.executor.configMaps[0].name has invalid ConfigMap name "BAD_EXECUTOR"`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := newSparkApplication()
			tt.mutate(app)

			_, err := validator.ValidateCreate(context.Background(), app)
			if hasError := err != nil; hasError != (len(tt.wantErrors) > 0) {
				t.Fatalf("ValidateCreate() error = %v, wantErrors %v", err, tt.wantErrors)
			}
			for _, want := range tt.wantErrors {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("expected error to report %s, got %v", want, err)
				}
			}
		})
	}
}

func TestSparkApplicationValidatorValidateUpdate_ConfigMapNames(t *testing.T) {
	validator := newTestValidator(t, false)

	t.Run("metadata-only update to an invalid application is allowed", func(t *testing.T) {
		oldApp := newSparkApplication()
		oldApp.Spec.Driver.ConfigMaps = configMapRefs("MY_CONFIG")
		newApp := oldApp.DeepCopy()
		newApp.Labels = map[string]string{"team": "data"}

		if _, err := validator.ValidateUpdate(context.Background(), oldApp, newApp); err != nil {
			t.Fatalf("expected an unchanged spec to skip validation, got %v", err)
		}
	})

	t.Run("spec update to an invalid application is rejected", func(t *testing.T) {
		oldApp := newSparkApplication()
		oldApp.Spec.Driver.ConfigMaps = configMapRefs("MY_CONFIG")
		newApp := oldApp.DeepCopy()
		newApp.Spec.Arguments = []string{"--foo"}

		_, err := validator.ValidateUpdate(context.Background(), oldApp, newApp)
		if err == nil || !strings.Contains(err.Error(), `spec.driver.configMaps[0].name has invalid ConfigMap name "MY_CONFIG"`) {
			t.Fatalf("expected an invalid ConfigMap name error, got %v", err)
		}
	})

	t.Run("spec update to a valid application is allowed", func(t *testing.T) {
		oldApp := newSparkApplication()
		oldApp.Spec.Driver.ConfigMaps = configMapRefs("spark-conf")
		newApp := oldApp.DeepCopy()
		newApp.Spec.Arguments = []string{"--foo"}

		if _, err := validator.ValidateUpdate(context.Background(), oldApp, newApp); err != nil {
			t.Fatalf("expected a valid spec update to be allowed, got %v", err)
		}
	})
}

func configMapRefs(names ...string) []v1beta2.NamePath {
	refs := make([]v1beta2.NamePath, 0, len(names))
	for i, name := range names {
		refs = append(refs, v1beta2.NamePath{Name: name, Path: fmt.Sprintf("/etc/spark/conf%d", i)})
	}
	return refs
}
