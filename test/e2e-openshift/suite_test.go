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

// Package e2e_openshift_test contains OpenShift-specific e2e tests.
// These tests are SEPARATE from the main e2e tests and install the
// Spark operator from a remote Helm repository.
package e2e_openshift_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/cli"
	"helm.sh/helm/v3/pkg/getter"
	"helm.sh/helm/v3/pkg/repo"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

// ============================================================================
// OPENSHIFT E2E TEST SUITE
// ============================================================================
//
// This is a STANDALONE test suite that:
// 1. Installs Spark operator from https://opendatahub-io.github.io/spark-operator
// 2. Verifies fsGroup is NOT 185 (OpenShift security requirement)
// 3. Runs the docling-spark-app workload
//
// Run with: go test ./test/e2e-openshift/ -v -ginkgo.v -timeout 30m
//
// ============================================================================

const (
	// Release configuration
	ReleaseName      = "spark-operator-openshift"
	ReleaseNamespace = "spark-operator-openshift"

	// Namespace for SparkApplications (the operator will watch this namespace)
	DoclingNamespace = "docling-spark"

	// Webhook names (must match what the chart creates: {{ fullname }}-webhook)
	MutatingWebhookName   = "spark-operator-openshift-webhook"
	ValidatingWebhookName = "spark-operator-openshift-webhook"

	// Custom Helm repository
	HelmRepoName = "opendatahub-spark-operator"
	HelmRepoURL  = "https://opendatahub-io.github.io/spark-operator"
	ChartName    = "spark-operator"

	// Timeouts
	PollInterval = 1 * time.Second
	WaitTimeout  = 5 * time.Minute
)

// Global test variables
var (
	cfg       *rest.Config
	testEnv   *envtest.Environment
	k8sClient client.Client
	clientset *kubernetes.Clientset
)

// TestOpenShiftSparkOperator is the Ginkgo test entry point
func TestOpenShiftSparkOperator(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "OpenShift Spark Operator Suite")
}

// ============================================================================
// BeforeSuite: Set up the test environment
// ============================================================================
var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	By("Bootstrapping OpenShift test environment")

	// Initialize test environment - connect to existing Kind cluster
	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		BinaryAssetsDirectory: filepath.Join("..", "..", "bin", "k8s",
			fmt.Sprintf("1.32.0-%s-%s", runtime.GOOS, runtime.GOARCH)),
		UseExistingCluster: ptr.To(true),
	}

	var err error
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	// Register schemes
	Expect(v1alpha1.AddToScheme(scheme.Scheme)).NotTo(HaveOccurred())
	Expect(v1beta2.AddToScheme(scheme.Scheme)).NotTo(HaveOccurred())

	// Create clients
	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	clientset, err = kubernetes.NewForConfig(cfg)
	Expect(err).NotTo(HaveOccurred())
	Expect(clientset).NotTo(BeNil())

	// Create namespaces
	By("Creating release namespace: " + ReleaseNamespace)
	releaseNs := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ReleaseNamespace}}
	err = k8sClient.Create(context.TODO(), releaseNs)
	if err != nil {
		logf.Log.Info("Namespace may already exist", "namespace", ReleaseNamespace, "error", err)
	}

	By("Creating docling namespace: " + DoclingNamespace)
	doclingNs := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: DoclingNamespace}}
	err = k8sClient.Create(context.TODO(), doclingNs)
	if err != nil {
		logf.Log.Info("Namespace may already exist", "namespace", DoclingNamespace, "error", err)
	}

	// Apply RBAC from examples/openshift/k8s/base/rbac.yaml
	// This creates: ServiceAccount, Role, and RoleBinding for spark-driver
	By("Applying RBAC from examples/openshift/k8s/base/rbac.yaml")
	rbacPath := filepath.Join("..", "..", "examples", "openshift", "k8s", "base", "rbac.yaml")
	applyYAMLFile(rbacPath)

	// Add Helm repository
	By("Adding Helm repository: " + HelmRepoURL)
	addHelmRepo()

	// Install chart from remote repository
	By("Installing Spark operator from remote Helm repository")
	installChartFromRepo()

	// Wait for webhooks
	By("Waiting for webhooks to be ready")
	mutatingWebhookKey := types.NamespacedName{Name: MutatingWebhookName}
	validatingWebhookKey := types.NamespacedName{Name: ValidatingWebhookName}
	Expect(waitForMutatingWebhookReady(context.Background(), mutatingWebhookKey)).NotTo(HaveOccurred())
	Expect(waitForValidatingWebhookReady(context.Background(), validatingWebhookKey)).NotTo(HaveOccurred())

	// Give webhooks time to initialize
	time.Sleep(10 * time.Second)

	By("OpenShift test environment setup complete")
})

// ============================================================================
// AfterSuite: Clean up the test environment
// ============================================================================
var _ = AfterSuite(func() {
	By("Tearing down OpenShift test environment")

	// Uninstall Helm release
	By("Uninstalling Spark operator Helm release")
	uninstallChart()

	// Delete namespaces
	By("Deleting docling namespace")
	doclingNs := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: DoclingNamespace}}
	_ = k8sClient.Delete(context.TODO(), doclingNs)

	By("Deleting release namespace")
	releaseNs := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ReleaseNamespace}}
	_ = k8sClient.Delete(context.TODO(), releaseNs)

	// Stop test environment
	By("Stopping test environment")
	if testEnv != nil {
		err := testEnv.Stop()
		Expect(err).ToNot(HaveOccurred())
	}
})

// ============================================================================
// Helm Helper Functions
// ============================================================================

// addHelmRepo adds the custom Helm repository
func addHelmRepo() {
	repoEntry := &repo.Entry{
		Name: HelmRepoName,
		URL:  HelmRepoURL,
	}

	settings := cli.New()
	repoFile := settings.RepositoryConfig

	r, err := repo.NewChartRepository(repoEntry, getter.All(settings))
	Expect(err).NotTo(HaveOccurred())

	// Download repository index
	_, err = r.DownloadIndexFile()
	Expect(err).NotTo(HaveOccurred(), "Failed to download Helm repo index from %s", HelmRepoURL)

	// Load or create repo file
	var repoFileObj *repo.File
	if _, err := os.Stat(repoFile); os.IsNotExist(err) {
		repoFileObj = repo.NewFile()
	} else {
		repoFileObj, err = repo.LoadFile(repoFile)
		Expect(err).NotTo(HaveOccurred())
	}

	repoFileObj.Update(repoEntry)
	Expect(repoFileObj.WriteFile(repoFile, 0644)).NotTo(HaveOccurred())

	logf.Log.Info("Successfully added Helm repository", "name", HelmRepoName, "url", HelmRepoURL)
}

// installChartFromRepo installs the Spark operator from the remote repository
func installChartFromRepo() {
	settings := cli.New()
	settings.SetNamespace(ReleaseNamespace)

	actionConfig := &action.Configuration{}
	Expect(actionConfig.Init(settings.RESTClientGetter(), settings.Namespace(), os.Getenv("HELM_DRIVER"), func(format string, v ...interface{}) {
		logf.Log.Info(fmt.Sprintf(format, v...))
	})).NotTo(HaveOccurred())

	installClient := action.NewInstall(actionConfig)
	installClient.ReleaseName = ReleaseName
	installClient.Namespace = ReleaseNamespace
	installClient.Wait = true
	installClient.Timeout = WaitTimeout
	installClient.CreateNamespace = true

	// Chart reference: repoName/chartName
	chartRef := fmt.Sprintf("%s/%s", HelmRepoName, ChartName)

	chartPath, err := installClient.LocateChart(chartRef, settings)
	Expect(err).NotTo(HaveOccurred(), "Failed to locate chart: %s", chartRef)

	chart, err := loader.Load(chartPath)
	Expect(err).NotTo(HaveOccurred(), "Failed to load chart from: %s", chartPath)

	// Configure operator to watch the docling-spark namespace
	vals := map[string]interface{}{
		"spark": map[string]interface{}{
			"jobNamespaces": []string{DoclingNamespace},
		},
	}

	release, err := installClient.Run(chart, vals)
	Expect(err).NotTo(HaveOccurred(), "Failed to install Helm chart")
	Expect(release).NotTo(BeNil())

	logf.Log.Info("Successfully installed Spark operator",
		"release", ReleaseName,
		"namespace", ReleaseNamespace,
		"chart", chartRef,
		"jobNamespaces", DoclingNamespace)
}

// uninstallChart removes the Helm release
func uninstallChart() {
	settings := cli.New()
	settings.SetNamespace(ReleaseNamespace)

	actionConfig := &action.Configuration{}
	err := actionConfig.Init(settings.RESTClientGetter(), settings.Namespace(), os.Getenv("HELM_DRIVER"), func(format string, v ...interface{}) {
		logf.Log.Info(fmt.Sprintf(format, v...))
	})
	if err != nil {
		logf.Log.Error(err, "Failed to initialize action config for uninstall")
		return
	}

	uninstallAction := action.NewUninstall(actionConfig)
	uninstallAction.Wait = true
	uninstallAction.Timeout = WaitTimeout

	_, err = uninstallAction.Run(ReleaseName)
	if err != nil {
		logf.Log.Error(err, "Failed to uninstall Helm release")
	}
}

// ============================================================================
// Webhook Helper Functions
// ============================================================================

func waitForMutatingWebhookReady(ctx context.Context, key types.NamespacedName) error {
	cancelCtx, cancelFunc := context.WithTimeout(ctx, WaitTimeout)
	defer cancelFunc()

	mutatingWebhook := admissionregistrationv1.MutatingWebhookConfiguration{}
	return wait.PollUntilContextCancel(cancelCtx, PollInterval, true, func(ctx context.Context) (bool, error) {
		if err := k8sClient.Get(ctx, key, &mutatingWebhook); err != nil {
			return false, nil // Keep polling
		}

		for _, wh := range mutatingWebhook.Webhooks {
			if wh.ClientConfig.CABundle == nil {
				return false, nil
			}

			svcRef := wh.ClientConfig.Service
			if svcRef == nil {
				return false, fmt.Errorf("webhook service is nil")
			}

			endpoints := corev1.Endpoints{}
			endpointsKey := types.NamespacedName{Namespace: svcRef.Namespace, Name: svcRef.Name}
			if err := k8sClient.Get(ctx, endpointsKey, &endpoints); err != nil {
				return false, nil // Keep polling
			}
			if len(endpoints.Subsets) == 0 {
				return false, nil
			}
		}
		return true, nil
	})
}

func waitForValidatingWebhookReady(ctx context.Context, key types.NamespacedName) error {
	cancelCtx, cancelFunc := context.WithTimeout(ctx, WaitTimeout)
	defer cancelFunc()

	validatingWebhook := admissionregistrationv1.ValidatingWebhookConfiguration{}
	return wait.PollUntilContextCancel(cancelCtx, PollInterval, true, func(ctx context.Context) (bool, error) {
		if err := k8sClient.Get(ctx, key, &validatingWebhook); err != nil {
			return false, nil // Keep polling
		}

		for _, wh := range validatingWebhook.Webhooks {
			if wh.ClientConfig.CABundle == nil {
				return false, nil
			}

			svcRef := wh.ClientConfig.Service
			if svcRef == nil {
				return false, fmt.Errorf("webhook service is nil")
			}

			endpoints := corev1.Endpoints{}
			endpointsKey := types.NamespacedName{Namespace: svcRef.Namespace, Name: svcRef.Name}
			if err := k8sClient.Get(ctx, endpointsKey, &endpoints); err != nil {
				return false, nil // Keep polling
			}
			if len(endpoints.Subsets) == 0 {
				return false, nil
			}
		}
		return true, nil
	})
}

// ============================================================================
// SparkApplication Helper Functions
// ============================================================================

func waitForSparkApplicationCompleted(ctx context.Context, key types.NamespacedName) error {
	cancelCtx, cancelFunc := context.WithTimeout(ctx, WaitTimeout)
	defer cancelFunc()

	app := &v1beta2.SparkApplication{}
	return wait.PollUntilContextCancel(cancelCtx, PollInterval, true, func(ctx context.Context) (bool, error) {
		if err := k8sClient.Get(ctx, key, app); err != nil {
			return false, err
		}
		switch app.Status.AppState.State {
		case v1beta2.ApplicationStateFailedSubmission, v1beta2.ApplicationStateFailed:
			return false, errors.New(app.Status.AppState.ErrorMessage)
		case v1beta2.ApplicationStateCompleted:
			return true, nil
		}
		return false, nil
	})
}

// ============================================================================
// YAML Helper Functions
// ============================================================================

// applyYAMLFile reads a YAML file and applies all resources it contains.
// Supports multi-document YAML files (separated by ---).
func applyYAMLFile(path string) {
	logf.Log.Info("Applying YAML file", "path", path)

	data, err := os.ReadFile(path)
	Expect(err).NotTo(HaveOccurred(), "Failed to read YAML file: %s", path)

	// Split by YAML document separator and process each document
	reader := k8syaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(data)))

	for {
		doc, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			Expect(err).NotTo(HaveOccurred(), "Failed to read YAML document")
		}

		// Skip empty documents
		if len(bytes.TrimSpace(doc)) == 0 {
			continue
		}

		// Decode into unstructured object
		obj := &unstructured.Unstructured{}
		decoder := k8syaml.NewYAMLOrJSONDecoder(bytes.NewReader(doc), 4096)
		err = decoder.Decode(obj)
		if err != nil {
			if err == io.EOF {
				continue
			}
			Expect(err).NotTo(HaveOccurred(), "Failed to decode YAML document")
		}

		// Skip empty or invalid objects
		if obj.GetKind() == "" {
			continue
		}

		// Create the resource
		err = k8sClient.Create(context.TODO(), obj)
		if err != nil {
			logf.Log.Info("Resource may already exist",
				"kind", obj.GetKind(),
				"name", obj.GetName(),
				"namespace", obj.GetNamespace(),
				"error", err)
		} else {
			logf.Log.Info("Created resource",
				"kind", obj.GetKind(),
				"name", obj.GetName(),
				"namespace", obj.GetNamespace())
		}
	}
}
