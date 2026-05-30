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

package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/cli"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
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
	// +kubebuilder:scaffold:imports
)

// These tests use Ginkgo (BDD-style Go testing framework). Refer to
// http://onsi.github.io/ginkgo/ to learn more about Ginkgo.

const (
	ReleaseName      = "spark-operator"
	ReleaseNamespace = "spark-operator"

	PollInterval = 1 * time.Second
	WaitTimeout  = 5 * time.Minute
)

var (
	cfg       *rest.Config
	testEnv   *envtest.Environment
	k8sClient client.Client
	clientset *kubernetes.Clientset

	// deployMethod is read from DEPLOY_METHOD env var; defaults to "helm".
	deployMethod          string
	mutatingWebhookName   string
	validatingWebhookName string
)

func TestSparkOperator(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecs(t, "Spark Operator Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	deployMethod = strings.ToLower(os.Getenv("DEPLOY_METHOD"))
	if deployMethod == "" {
		deployMethod = "helm"
	}
	GinkgoWriter.Printf("Deploy method: %s\n", deployMethod)

	switch deployMethod {
	case "helm":
		mutatingWebhookName = "spark-operator-webhook"
		validatingWebhookName = "spark-operator-webhook"
	case "kustomize":
		mutatingWebhookName = "mutating-webhook-configuration"
		validatingWebhookName = "validating-webhook-configuration"
	default:
		Fail(fmt.Sprintf("unsupported DEPLOY_METHOD: %s (must be 'helm' or 'kustomize')", deployMethod))
	}

	var err error

	By("Bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,

		// The BinaryAssetsDirectory is only required if you want to run the tests directly
		// without call the makefile target test. If not informed it will look for the
		// default path defined in controller-runtime which is /usr/local/kubebuilder/.
		// Note that you must have the required binaries setup under the bin directory to perform
		// the tests directly. When we run make test it will be setup and used automatically.
		BinaryAssetsDirectory: filepath.Join("..", "..", "bin", "k8s",
			fmt.Sprintf("1.33.0-%s-%s", runtime.GOOS, runtime.GOARCH)),
		UseExistingCluster: ptr.To(true),
	}

	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	Expect(v1alpha1.AddToScheme(scheme.Scheme)).NotTo(HaveOccurred())
	Expect(v1beta2.AddToScheme(scheme.Scheme)).NotTo(HaveOccurred())
	// +kubebuilder:scaffold:scheme

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	clientset, err = kubernetes.NewForConfig(cfg)
	Expect(err).NotTo(HaveOccurred())
	Expect(clientset).NotTo(BeNil())

	switch deployMethod {
	case "helm":
		installViaHelm()
	case "kustomize":
		installViaKustomize()
	}

	By("Waiting for the webhooks to be ready")
	mutatingWebhookKey := types.NamespacedName{Name: mutatingWebhookName}
	validatingWebhookKey := types.NamespacedName{Name: validatingWebhookName}
	Expect(waitForMutatingWebhookReady(context.Background(), mutatingWebhookKey)).NotTo(HaveOccurred())
	Expect(waitForValidatingWebhookReady(context.Background(), validatingWebhookKey)).NotTo(HaveOccurred())
	// TODO: Remove this when there is a better way to ensure the webhooks are ready before running the e2e tests.
	time.Sleep(10 * time.Second)
})

var _ = AfterSuite(func() {
	switch deployMethod {
	case "helm":
		uninstallViaHelm()
	case "kustomize":
		uninstallViaKustomize()
	}

	By("Tearing down the test environment")
	err := testEnv.Stop()
	Expect(err).ToNot(HaveOccurred())
})

func installViaHelm() {
	By("Creating release namespace")
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ReleaseNamespace}}
	Expect(k8sClient.Create(context.TODO(), namespace)).NotTo(HaveOccurred())

	By("Installing the Spark operator helm chart")
	envSettings := cli.New()
	envSettings.SetNamespace(ReleaseNamespace)
	actionConfig := &action.Configuration{}
	Expect(actionConfig.Init(envSettings.RESTClientGetter(), envSettings.Namespace(), os.Getenv("HELM_DRIVER"), func(format string, v ...interface{}) {
		logf.Log.Info(fmt.Sprintf(format, v...))
	})).NotTo(HaveOccurred())
	installAction := action.NewInstall(actionConfig)
	Expect(installAction).NotTo(BeNil())
	installAction.ReleaseName = ReleaseName
	installAction.Namespace = envSettings.Namespace()
	installAction.Wait = true
	installAction.Timeout = WaitTimeout
	chartPath := filepath.Join("..", "..", "charts", "spark-operator-chart")
	chart, err := loader.Load(chartPath)
	Expect(err).NotTo(HaveOccurred())
	Expect(chart).NotTo(BeNil())
	values, err := chartutil.ReadValuesFile(filepath.Join(chartPath, "ci", "ci-values.yaml"))
	Expect(err).NotTo(HaveOccurred())
	Expect(values).NotTo(BeNil())
	release, err := installAction.Run(chart, values)
	Expect(err).NotTo(HaveOccurred())
	Expect(release).NotTo(BeNil())
}

func uninstallViaHelm() {
	By("Uninstalling the Spark operator helm chart")
	envSettings := cli.New()
	envSettings.SetNamespace(ReleaseNamespace)
	actionConfig := &action.Configuration{}
	Expect(actionConfig.Init(envSettings.RESTClientGetter(), envSettings.Namespace(), os.Getenv("HELM_DRIVER"), func(format string, v ...interface{}) {
		logf.Log.Info(fmt.Sprintf(format, v...))
	})).NotTo(HaveOccurred())
	uninstallAction := action.NewUninstall(actionConfig)
	Expect(uninstallAction).NotTo(BeNil())
	uninstallAction.Wait = true
	uninstallAction.Timeout = WaitTimeout
	resp, err := uninstallAction.Run(ReleaseName)
	Expect(err).To(BeNil())
	Expect(resp).NotTo(BeNil())

	By("Deleting release namespace")
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ReleaseNamespace}}
	Expect(k8sClient.Delete(context.TODO(), namespace)).NotTo(HaveOccurred())
}

func installViaKustomize() {
	repoRoot := filepath.Join("..", "..")
	kustomizeDir := filepath.Join(repoRoot, "config", "default")
	kustomizationPath := filepath.Join(kustomizeDir, "kustomization.yaml")

	imageTag := os.Getenv("IMAGE_TAG")
	if imageTag != "" {
		By(fmt.Sprintf("Installing the Spark operator via Kustomize (image tag override: %s)", imageTag))

		origKustomization, err := os.ReadFile(kustomizationPath)
		Expect(err).NotTo(HaveOccurred())
		defer func() {
			_ = os.WriteFile(kustomizationPath, origKustomization, 0644)
		}()

		lines := strings.Split(string(origKustomization), "\n")
		for i, line := range lines {
			if strings.Contains(line, "newTag:") {
				lines[i] = "    newTag: " + imageTag
			}
		}
		Expect(os.WriteFile(kustomizationPath, []byte(strings.Join(lines, "\n")), 0644)).NotTo(HaveOccurred())
	} else {
		By("Installing the Spark operator via Kustomize (using image from kustomization.yaml)")
	}

	applyCmd := exec.Command("kubectl", "apply", "-k", kustomizeDir, "--server-side", "--force-conflicts")
	applyCmd.Stdout = GinkgoWriter
	applyCmd.Stderr = GinkgoWriter
	Expect(applyCmd.Run()).NotTo(HaveOccurred(), "Failed to apply Kustomize manifests")

	By("Waiting for controller and webhook deployments to be ready")
	for _, deploy := range []string{"spark-operator-controller", "spark-operator-webhook"} {
		waitCmd := exec.Command("kubectl", "rollout", "status",
			fmt.Sprintf("deployment/%s", deploy),
			"-n", ReleaseNamespace,
			"--timeout=300s")
		waitCmd.Stdout = GinkgoWriter
		waitCmd.Stderr = GinkgoWriter
		Expect(waitCmd.Run()).NotTo(HaveOccurred(), fmt.Sprintf("%s deployment not ready", deploy))
	}

	// Apply Spark job RBAC (SA, Role, RoleBinding) to the namespace where e2e tests run Spark apps.
	By("Applying Spark job RBAC to default namespace")
	sparkRBACDir := filepath.Join(repoRoot, "config", "spark-rbac")
	rbacCmd := exec.Command("kubectl", "apply", "-k", sparkRBACDir, "-n", "default", "--server-side", "--force-conflicts")
	rbacCmd.Stdout = GinkgoWriter
	rbacCmd.Stderr = GinkgoWriter
	Expect(rbacCmd.Run()).NotTo(HaveOccurred(), "Failed to apply Spark job RBAC")
}

func uninstallViaKustomize() {
	repoRoot := filepath.Join("..", "..")

	By("Cleaning up Spark job RBAC from default namespace")
	sparkRBACDir := filepath.Join(repoRoot, "config", "spark-rbac")
	rbacDelCmd := exec.Command("kubectl", "delete", "-k", sparkRBACDir, "-n", "default", "--ignore-not-found", "--timeout=60s")
	rbacDelCmd.Stdout = GinkgoWriter
	rbacDelCmd.Stderr = GinkgoWriter
	_ = rbacDelCmd.Run()

	kustomizeDir := filepath.Join(repoRoot, "config", "default")
	By("Uninstalling the Spark operator via Kustomize")
	deleteCmd := exec.Command("kubectl", "delete", "-k", kustomizeDir, "--ignore-not-found", "--timeout=120s")
	deleteCmd.Stdout = GinkgoWriter
	deleteCmd.Stderr = GinkgoWriter
	_ = deleteCmd.Run()
}

func waitForMutatingWebhookReady(ctx context.Context, key types.NamespacedName) error {
	cancelCtx, cancelFunc := context.WithTimeout(ctx, WaitTimeout)
	defer cancelFunc()

	mutatingWebhook := admissionregistrationv1.MutatingWebhookConfiguration{}
	err := wait.PollUntilContextCancel(cancelCtx, PollInterval, true, func(ctx context.Context) (bool, error) {
		if err := k8sClient.Get(ctx, key, &mutatingWebhook); err != nil {
			return false, err
		}

		for _, wh := range mutatingWebhook.Webhooks {
			// Checkout webhook CA certificate
			if wh.ClientConfig.CABundle == nil {
				return false, nil
			}

			// Checkout webhook service endpoints
			svcRef := wh.ClientConfig.Service
			if svcRef == nil {
				return false, fmt.Errorf("webhook service is nil")
			}
			endpoints := corev1.Endpoints{}
			endpointsKey := types.NamespacedName{Namespace: svcRef.Namespace, Name: svcRef.Name}
			if err := k8sClient.Get(ctx, endpointsKey, &endpoints); err != nil {
				return false, err
			}
			if len(endpoints.Subsets) == 0 {
				return false, nil
			}
		}

		return true, nil
	})
	return err
}

func waitForValidatingWebhookReady(ctx context.Context, key types.NamespacedName) error {
	cancelCtx, cancelFunc := context.WithTimeout(ctx, WaitTimeout)
	defer cancelFunc()

	validatingWebhook := admissionregistrationv1.ValidatingWebhookConfiguration{}
	err := wait.PollUntilContextCancel(cancelCtx, PollInterval, true, func(ctx context.Context) (bool, error) {
		if err := k8sClient.Get(ctx, key, &validatingWebhook); err != nil {
			return false, err
		}

		for _, wh := range validatingWebhook.Webhooks {
			// Checkout webhook CA certificate
			if wh.ClientConfig.CABundle == nil {
				return false, nil
			}

			// Checkout webhook service endpoints
			svcRef := wh.ClientConfig.Service
			if svcRef == nil {
				return false, fmt.Errorf("webhook service is nil")
			}
			endpoints := corev1.Endpoints{}
			endpointsKey := types.NamespacedName{Namespace: svcRef.Namespace, Name: svcRef.Name}
			if err := k8sClient.Get(ctx, endpointsKey, &endpoints); err != nil {
				return false, err
			}
			if len(endpoints.Subsets) == 0 {
				return false, nil
			}
		}

		return true, nil
	})
	return err
}

func waitForSparkApplicationCompleted(ctx context.Context, key types.NamespacedName) error {
	cancelCtx, cancelFunc := context.WithTimeout(ctx, WaitTimeout)
	defer cancelFunc()

	app := &v1beta2.SparkApplication{}
	err := wait.PollUntilContextCancel(cancelCtx, PollInterval, true, func(ctx context.Context) (bool, error) {
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
	return err
}

func collectSparkApplicationsUntilTermination(ctx context.Context, key types.NamespacedName) ([]v1beta2.SparkApplication, error) {
	cancelCtx, cancelFunc := context.WithTimeout(ctx, WaitTimeout)
	defer cancelFunc()

	apps := []v1beta2.SparkApplication{}

	err := wait.PollUntilContextCancel(cancelCtx, PollInterval, true, func(ctx context.Context) (bool, error) {
		app := v1beta2.SparkApplication{}
		if err := k8sClient.Get(ctx, key, &app); err != nil {
			return false, err
		}
		apps = append(apps, app)
		switch app.Status.AppState.State {
		case v1beta2.ApplicationStateFailed:
			return true, errors.New(app.Status.AppState.ErrorMessage)
		case v1beta2.ApplicationStateCompleted:
			return true, nil
		}
		return false, nil
	})
	return apps, err
}
