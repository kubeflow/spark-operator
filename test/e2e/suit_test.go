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
	"path/filepath"
	"runtime"
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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/util"
	// +kubebuilder:scaffold:imports
)

// These tests use Ginkgo (BDD-style Go testing framework). Refer to
// http://onsi.github.io/ginkgo/ to learn more about Ginkgo.

const (
	ReleaseName      = "spark-operator"
	ReleaseNamespace = "spark-operator"

	MutatingWebhookName   = "spark-operator-webhook"
	ValidatingWebhookName = "spark-operator-webhook"

	PollInterval = 1 * time.Second
	WaitTimeout  = 5 * time.Minute
)

var (
	cfg       *rest.Config
	testEnv   *envtest.Environment
	k8sClient client.Client
	clientset *kubernetes.Clientset
)

func TestSparkOperator(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecs(t, "Spark Operator Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))
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
			fmt.Sprintf("1.32.0-%s-%s", runtime.GOOS, runtime.GOARCH)),
		UseExistingCluster: util.BoolPtr(true),
	}

	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	err = v1beta2.AddToScheme(scheme.Scheme)
	Expect(err).NotTo(HaveOccurred())

	// +kubebuilder:scaffold:scheme

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	clientset, err = kubernetes.NewForConfig(cfg)
	Expect(err).NotTo(HaveOccurred())
	Expect(clientset).NotTo(BeNil())

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

	By("Waiting for the webhooks to be ready")
	mutatingWebhookKey := types.NamespacedName{Name: MutatingWebhookName}
	validatingWebhookKey := types.NamespacedName{Name: ValidatingWebhookName}
	Expect(waitForMutatingWebhookReady(context.Background(), mutatingWebhookKey)).NotTo(HaveOccurred())
	Expect(waitForValidatingWebhookReady(context.Background(), validatingWebhookKey)).NotTo(HaveOccurred())
	// TODO: Remove this when there is a better way to ensure the webhooks are ready before running the e2e tests.
	time.Sleep(10 * time.Second)
})

var _ = AfterSuite(func() {
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

	By("Tearing down the test environment")
	err = testEnv.Stop()
	Expect(err).ToNot(HaveOccurred())
})

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
