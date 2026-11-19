/*
Copyright The Kubeflow Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package admission_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	openapierrors "k8s.io/kube-openapi/pkg/validation/errors"
	"k8s.io/kube-openapi/pkg/validation/strfmt"
	"k8s.io/kube-openapi/pkg/validation/validate"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/yaml"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	operatorwebhook "github.com/kubeflow/spark-operator/v2/internal/webhook"
)

func TestSparkApplicationRequiredFields(t *testing.T) {
	crd := sparkApplicationCRD(t)
	var schema *apiextensionsv1.JSONSchemaProps
	for _, version := range crd.Spec.Versions {
		if version.Name == v1beta2.SchemeGroupVersion.Version {
			require.NotNil(t, version.Schema)
			schema = version.Schema.OpenAPIV3Schema
		}
	}
	require.NotNil(t, schema)
	var internalSchema apiextensions.JSONSchemaProps
	require.NoError(t, apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(schema, &internalSchema, nil))
	structural, err := structuralschema.NewStructural(&internalSchema)
	require.NoError(t, err)
	validator := validate.NewSchemaValidator(structural.ToKubeOpenAPI(), nil, "", strfmt.Default)
	require.Empty(t, validator.Validate(sparkApplication("valid").Object).Errors)

	// The API server's name validation alone cannot detect a missing CRD metadata requirement.
	for _, missing := range []string{"metadata", "spec"} {
		t.Run(missing, func(t *testing.T) {
			app := sparkApplication("missing-" + missing)
			delete(app.Object, missing)
			errs := validator.Validate(app.Object).Errors
			require.Len(t, errs, 1)
			var validationErr *openapierrors.Validation
			require.ErrorAs(t, errs[0], &validationErr)
			assert.Equal(t, int32(openapierrors.RequiredFailCode), validationErr.Code())
			assert.Equal(t, "."+missing, validationErr.Name)
		})
	}
}

func TestSparkApplicationOptionalFieldsAfterAdmission(t *testing.T) {
	ctx, k8sClient := startAdmissionTest(t)
	app := sparkApplication("optional-fields")
	tls := []any{map[string]any{
		"hosts":      []any{"optional-fields.example.com"},
		"secretName": "spark-ui-tls",
	}}
	require.NoError(t, unstructured.SetNestedMap(app.Object, map[string]any{
		"ingressTLS": tls,
	}, "spec", "sparkUIOptions"))
	require.NoError(t, unstructured.SetNestedSlice(app.Object, []any{map[string]any{
		"servicePort":      int64(8080),
		"servicePortName":  "driver-extra",
		"ingressURLFormat": "{{$appName}}.example.com",
	}}, "spec", "driverIngressOptions"))

	// Omitting mode forces the defaulter to mutate and serialize the decoded object.
	require.NoError(t, k8sClient.Create(ctx, app))
	stored := &unstructured.Unstructured{}
	stored.SetGroupVersionKind(v1beta2.SchemeGroupVersion.WithKind("SparkApplication"))
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(app), stored))
	mode, found, err := unstructured.NestedString(stored.Object, "spec", "mode")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, string(v1beta2.DeployModeCluster), mode)

	ui, found, err := unstructured.NestedMap(stored.Object, "spec", "sparkUIOptions")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, tls, ui["ingressTLS"])
	for _, name := range []string{"servicePort", "servicePortName", "serviceType"} {
		assert.NotContains(t, ui, name)
	}
	ingresses, found, err := unstructured.NestedSlice(stored.Object, "spec", "driverIngressOptions")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, ingresses, 1)
	ingress, ok := ingresses[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, int64(8080), ingress["servicePort"])
	assert.Equal(t, "driver-extra", ingress["servicePortName"])
	assert.Equal(t, "{{$appName}}.example.com", ingress["ingressURLFormat"])
	assert.NotContains(t, ingress, "serviceType")
}

func startAdmissionTest(t *testing.T) (context.Context, client.Client) {
	t.Helper()
	scheme := k8sruntime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, v1beta2.AddToScheme(scheme))
	testEnv := &envtest.Environment{
		CRDs:                  []*apiextensionsv1.CustomResourceDefinition{sparkApplicationCRD(t)},
		WebhookInstallOptions: sparkApplicationWebhooks(t, scheme),
		BinaryAssetsDirectory: filepath.Join("..", "..", "..", "bin", "k8s",
			fmt.Sprintf("1.35.0-%s-%s", runtime.GOOS, runtime.GOARCH)),
	}
	t.Cleanup(func() { assert.NoError(t, testEnv.Stop()) })
	cfg, err := testEnv.Start()
	require.NoError(t, err)

	options := &testEnv.WebhookInstallOptions
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		WebhookServer: webhook.NewServer(webhook.Options{
			Host:    options.LocalServingHost,
			Port:    options.LocalServingPort,
			CertDir: options.LocalServingCertDir,
		}),
	})
	require.NoError(t, err)
	require.NoError(t, ctrl.NewWebhookManagedBy(mgr, &v1beta2.SparkApplication{}).
		WithDefaulter(operatorwebhook.NewSparkApplicationDefaulter()).
		WithValidator(operatorwebhook.NewSparkApplicationValidator(mgr.GetClient(), false)).
		Complete())

	ctx, cancel := context.WithCancel(context.Background())
	managerDone := make(chan error, 1)
	go func() { managerDone <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-managerDone:
			assert.NoError(t, err)
		case <-time.After(30 * time.Second):
			t.Error("admission manager did not stop")
		}
	})
	k8sClient, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	require.NoError(t, k8sClient.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "admission-test"},
	}))

	// Configuration creation does not guarantee both admission plugins have observed it.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		probe := sparkApplication("ready")
		if !assert.NoError(c, k8sClient.Create(ctx, probe, client.DryRunAll)) {
			return
		}
		mode, found, err := unstructured.NestedString(probe.Object, "spec", "mode")
		assert.NoError(c, err)
		assert.True(c, found)
		assert.Equal(c, string(v1beta2.DeployModeCluster), mode)
		assert.ErrorContains(c, k8sClient.Create(ctx, sparkApplication("invalid.name"), client.DryRunAll),
			"invalid SparkApplication name")
	}, 30*time.Second, 100*time.Millisecond, "SparkApplication admission webhooks did not become ready")
	return ctx, k8sClient
}

func sparkApplicationCRD(t *testing.T) *apiextensionsv1.CustomResourceDefinition {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "config", "crd", "bases", "sparkoperator.k8s.io_sparkapplications.yaml"))
	require.NoError(t, err)
	crd := &apiextensionsv1.CustomResourceDefinition{}
	require.NoError(t, yaml.Unmarshal(data, crd))
	return crd
}

func sparkApplicationWebhooks(t *testing.T, scheme *k8sruntime.Scheme) envtest.WebhookInstallOptions {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "config", "webhook", "manifests.yaml"))
	require.NoError(t, err)
	reader := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	decoder := serializer.NewCodecFactory(scheme).UniversalDeserializer()
	options := envtest.WebhookInstallOptions{}
	for {
		var raw k8sruntime.RawExtension
		if err := reader.Decode(&raw); errors.Is(err, io.EOF) {
			break
		} else {
			require.NoError(t, err)
		}
		if len(raw.Raw) == 0 {
			continue
		}
		object, _, err := decoder.Decode(raw.Raw, nil, nil)
		require.NoError(t, err)
		switch configuration := object.(type) {
		case *admissionregistrationv1.MutatingWebhookConfiguration:
			configuration.Webhooks = slices.DeleteFunc(configuration.Webhooks, func(hook admissionregistrationv1.MutatingWebhook) bool {
				return hook.Name != "mutate-sparkapplication.sparkoperator.k8s.io"
			})
			options.MutatingWebhooks = append(options.MutatingWebhooks, configuration)
		case *admissionregistrationv1.ValidatingWebhookConfiguration:
			configuration.Webhooks = slices.DeleteFunc(configuration.Webhooks, func(hook admissionregistrationv1.ValidatingWebhook) bool {
				return hook.Name != "validate-sparkapplication.sparkoperator.k8s.io"
			})
			options.ValidatingWebhooks = append(options.ValidatingWebhooks, configuration)
		}
	}
	require.Len(t, options.MutatingWebhooks, 1)
	require.Len(t, options.MutatingWebhooks[0].Webhooks, 1)
	require.Len(t, options.ValidatingWebhooks, 1)
	require.Len(t, options.ValidatingWebhooks[0].Webhooks, 1)
	return options
}

func sparkApplication(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": v1beta2.SchemeGroupVersion.String(),
		"kind":       "SparkApplication",
		"metadata": map[string]any{
			"name":      name,
			"namespace": "admission-test",
		},
		"spec": map[string]any{
			"type":                "Python",
			"sparkVersion":        "3.5.0",
			"mainApplicationFile": "local:///opt/spark/examples/src/main/python/pi.py",
			"driver":              map[string]any{},
			"executor":            map[string]any{},
		},
	}}
}
