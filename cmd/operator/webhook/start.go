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

package webhook

import (
	"context"
	"crypto/tls"
	"flag"
	"os"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	logzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	ctrlwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"

	sparkoperator "github.com/kubeflow/spark-operator"
	"github.com/kubeflow/spark-operator/api/v1beta1"
	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/internal/controller/mutatingwebhookconfiguration"
	"github.com/kubeflow/spark-operator/internal/controller/validatingwebhookconfiguration"
	"github.com/kubeflow/spark-operator/internal/webhook"
	"github.com/kubeflow/spark-operator/pkg/certificate"
	"github.com/kubeflow/spark-operator/pkg/common"
	"github.com/kubeflow/spark-operator/pkg/util"
	// +kubebuilder:scaffold:imports
)

var (
	scheme = runtime.NewScheme()
	logger = ctrl.Log.WithName("")
)

var (
	namespaces          []string
	labelSelectorFilter string

	// Controller
	controllerThreads int
	cacheSyncTimeout  time.Duration

	// Webhook
	enableResourceQuotaEnforcement bool
	webhookCertDir                 string
	webhookCertName                string
	webhookKeyName                 string
	mutatingWebhookName            string
	validatingWebhookName          string
	webhookPort                    int
	webhookSecretName              string
	webhookSecretNamespace         string
	webhookServiceName             string
	webhookServiceNamespace        string

	// Leader election
	enableLeaderElection        bool
	leaderElectionLockName      string
	leaderElectionLockNamespace string
	leaderElectionLeaseDuration time.Duration
	leaderElectionRenewDeadline time.Duration
	leaderElectionRetryPeriod   time.Duration

	// Metrics
	enableMetrics      bool
	metricsBindAddress string
	metricsEndpoint    string
	metricsPrefix      string
	metricsLabels      []string

	healthProbeBindAddress string
	secureMetrics          bool
	enableHTTP2            bool
	development            bool
	zapOptions             = logzap.Options{}
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(v1beta1.AddToScheme(scheme))
	utilruntime.Must(v1beta2.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

func NewStartCommand() *cobra.Command {
	var command = &cobra.Command{
		Use:   "start",
		Short: "Start controller and webhook",
		PreRun: func(_ *cobra.Command, args []string) {
			development = viper.GetBool("development")
		},
		Run: func(cmd *cobra.Command, args []string) {
			sparkoperator.PrintVersion(false)
			start()
		},
	}

	command.Flags().IntVar(&controllerThreads, "controller-threads", 10, "Number of worker threads used by the SparkApplication controller.")
	command.Flags().StringSliceVar(&namespaces, "namespaces", []string{}, "The Kubernetes namespace to manage. Will manage custom resource objects of the managed CRD types for the whole cluster if unset or contains empty string.")
	command.Flags().StringVar(&labelSelectorFilter, "label-selector-filter", "", "A comma-separated list of key=value, or key labels to filter resources during watch and list based on the specified labels.")
	command.Flags().DurationVar(&cacheSyncTimeout, "cache-sync-timeout", 30*time.Second, "Informer cache sync timeout.")

	command.Flags().StringVar(&webhookCertDir, "webhook-cert-dir", "/etc/k8s-webhook-server/serving-certs", "The directory that contains the webhook server key and certificate. "+
		"When running as nonRoot, you must create and own this directory before running this command.")
	command.Flags().StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The file name of webhook server certificate.")
	command.Flags().StringVar(&webhookKeyName, "webhook-key-name", "tls.key", "The file name of webhook server key.")
	command.Flags().StringVar(&mutatingWebhookName, "mutating-webhook-name", "spark-operator-webhook", "The name of the mutating webhook.")
	command.Flags().StringVar(&validatingWebhookName, "validating-webhook-name", "spark-operator-webhook", "The name of the validating webhook.")
	command.Flags().IntVar(&webhookPort, "webhook-port", 9443, "Service port of the webhook server.")
	command.Flags().StringVar(&webhookSecretName, "webhook-secret-name", "spark-operator-webhook-certs", "The name of the secret that contains the webhook server's TLS certificate and key.")
	command.Flags().StringVar(&webhookSecretNamespace, "webhook-secret-namespace", "spark-operator", "The namespace of the secret that contains the webhook server's TLS certificate and key.")
	command.Flags().StringVar(&webhookServiceName, "webhook-svc-name", "spark-webhook", "The name of the Service for the webhook server.")
	command.Flags().StringVar(&webhookServiceNamespace, "webhook-svc-namespace", "spark-webhook", "The name of the Service for the webhook server.")
	command.Flags().BoolVar(&enableResourceQuotaEnforcement, "enable-resource-quota-enforcement", false, "Whether to enable ResourceQuota enforcement for SparkApplication resources. Requires the webhook to be enabled.")

	command.Flags().BoolVar(&enableLeaderElection, "leader-election", false, "Enable leader election for controller manager. "+
		"Enabling this will ensure there is only one active controller manager.")
	command.Flags().StringVar(&leaderElectionLockName, "leader-election-lock-name", "spark-operator-lock", "Name of the ConfigMap for leader election.")
	command.Flags().StringVar(&leaderElectionLockNamespace, "leader-election-lock-namespace", "spark-operator", "Namespace in which to create the ConfigMap for leader election.")
	command.Flags().DurationVar(&leaderElectionLeaseDuration, "leader-election-lease-duration", 15*time.Second, "Leader election lease duration.")
	command.Flags().DurationVar(&leaderElectionRenewDeadline, "leader-election-renew-deadline", 14*time.Second, "Leader election renew deadline.")
	command.Flags().DurationVar(&leaderElectionRetryPeriod, "leader-election-retry-period", 4*time.Second, "Leader election retry period.")

	command.Flags().BoolVar(&enableMetrics, "enable-metrics", false, "Enable metrics.")
	command.Flags().StringVar(&metricsBindAddress, "metrics-bind-address", "0", "The address the metric endpoint binds to. "+
		"Use the port :8080. If not set, it will be 0 in order to disable the metrics server")
	command.Flags().StringVar(&metricsEndpoint, "metrics-endpoint", "/metrics", "Metrics endpoint.")
	command.Flags().StringVar(&metricsPrefix, "metrics-prefix", "", "Prefix for the metrics.")
	command.Flags().StringSliceVar(&metricsLabels, "metrics-labels", []string{}, "Labels to be added to the metrics.")

	command.Flags().StringVar(&healthProbeBindAddress, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	command.Flags().BoolVar(&secureMetrics, "secure-metrics", false, "If set the metrics endpoint is served securely")
	command.Flags().BoolVar(&enableHTTP2, "enable-http2", false, "If set, HTTP/2 will be enabled for the metrics and webhook servers")

	flagSet := flag.NewFlagSet("controller", flag.ExitOnError)
	ctrl.RegisterFlags(flagSet)
	zapOptions.BindFlags(flagSet)
	command.Flags().AddGoFlagSet(flagSet)

	return command
}

func start() {
	setupLog()

	// Create the client rest config. Use kubeConfig if given, otherwise assume in-cluster.
	cfg, err := ctrl.GetConfig()
	if err != nil {
		logger.Error(err, "failed to get kube config")
		os.Exit(1)
	}

	// Create the manager.
	tlsOptions := newTLSOptions()
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme,
		Cache:  newCacheOptions(),
		Metrics: metricsserver.Options{
			BindAddress:   metricsBindAddress,
			SecureServing: secureMetrics,
			TLSOpts:       tlsOptions,
		},
		WebhookServer: ctrlwebhook.NewServer(ctrlwebhook.Options{
			Port:     webhookPort,
			CertDir:  webhookCertDir,
			CertName: webhookCertName,
			KeyName:  webhookKeyName,
			TLSOpts:  tlsOptions,
		}),
		HealthProbeBindAddress:  healthProbeBindAddress,
		LeaderElection:          enableLeaderElection,
		LeaderElectionID:        leaderElectionLockName,
		LeaderElectionNamespace: leaderElectionLockNamespace,
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		logger.Error(err, "Failed to create manager")
		os.Exit(1)
	}

	client, err := client.New(cfg, client.Options{Scheme: mgr.GetScheme()})
	if err != nil {
		logger.Error(err, "Failed to create client")
		os.Exit(1)
	}

	certProvider := certificate.NewProvider(
		client,
		webhookServiceName,
		webhookServiceNamespace,
	)

	if err := wait.ExponentialBackoff(
		wait.Backoff{
			Steps:    5,
			Duration: 1 * time.Second,
			Factor:   2.0,
			Jitter:   0.1,
		},
		func() (bool, error) {
			logger.Info("Syncing webhook secret", "name", webhookSecretName, "namespace", webhookSecretNamespace)
			if err := certProvider.SyncSecret(context.TODO(), webhookSecretName, webhookSecretNamespace); err != nil {
				if errors.IsAlreadyExists(err) || errors.IsConflict(err) {
					return false, nil
				}
				return false, err
			}
			return true, nil
		},
	); err != nil {
		logger.Error(err, "Failed to sync webhook secret")
		os.Exit(1)
	}

	logger.Info("Writing certificates", "path", webhookCertDir, "certificate name", webhookCertName, "key name", webhookKeyName)
	if err := certProvider.WriteFile(webhookCertDir, webhookCertName, webhookKeyName); err != nil {
		logger.Error(err, "Failed to save certificate")
		os.Exit(1)
	}

	if err := mutatingwebhookconfiguration.NewReconciler(
		mgr.GetClient(),
		certProvider,
		mutatingWebhookName,
	).SetupWithManager(mgr, controller.Options{}); err != nil {
		logger.Error(err, "Failed to create controller", "controller", "MutatingWebhookConfiguration")
		os.Exit(1)
	}

	if err := validatingwebhookconfiguration.NewReconciler(
		mgr.GetClient(),
		certProvider,
		validatingWebhookName,
	).SetupWithManager(mgr, controller.Options{}); err != nil {
		logger.Error(err, "Failed to create controller", "controller", "ValidatingWebhookConfiguration")
		os.Exit(1)
	}

	if err := ctrl.NewWebhookManagedBy(mgr).
		For(&v1beta2.SparkApplication{}).
		WithDefaulter(webhook.NewSparkApplicationDefaulter()).
		WithValidator(webhook.NewSparkApplicationValidator(mgr.GetClient(), enableResourceQuotaEnforcement)).
		Complete(); err != nil {
		logger.Error(err, "Failed to create mutating webhook for Spark application")
		os.Exit(1)
	}

	if err := ctrl.NewWebhookManagedBy(mgr).
		For(&v1beta2.ScheduledSparkApplication{}).
		WithDefaulter(webhook.NewScheduledSparkApplicationDefaulter()).
		WithValidator(webhook.NewScheduledSparkApplicationValidator()).
		Complete(); err != nil {
		logger.Error(err, "Failed to create mutating webhook for Scheduled Spark application")
		os.Exit(1)
	}

	if err := ctrl.NewWebhookManagedBy(mgr).
		For(&corev1.Pod{}).
		WithDefaulter(webhook.NewSparkPodDefaulter(mgr.GetClient(), namespaces)).
		Complete(); err != nil {
		logger.Error(err, "Failed to create mutating webhook for Spark pod")
		os.Exit(1)
	}

	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", mgr.GetWebhookServer().StartedChecker()); err != nil {
		logger.Error(err, "Failed to set up health check")
		os.Exit(1)
	}

	if err := mgr.AddReadyzCheck("readyz", mgr.GetWebhookServer().StartedChecker()); err != nil {
		logger.Error(err, "Failed to set up ready check")
		os.Exit(1)
	}

	logger.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		logger.Error(err, "Failed to start manager")
		os.Exit(1)
	}
}

// setupLog Configures the logging system
func setupLog() {
	ctrl.SetLogger(logzap.New(
		logzap.UseFlagOptions(&zapOptions),
		func(o *logzap.Options) {
			o.Development = development
		}, func(o *logzap.Options) {
			o.ZapOpts = append(o.ZapOpts, zap.AddCaller())
		}, func(o *logzap.Options) {
			var config zapcore.EncoderConfig
			if !development {
				config = zap.NewProductionEncoderConfig()
			} else {
				config = zap.NewDevelopmentEncoderConfig()
				config.EncodeLevel = zapcore.CapitalColorLevelEncoder
			}
			config.EncodeTime = zapcore.ISO8601TimeEncoder
			config.EncodeCaller = zapcore.ShortCallerEncoder
			if !development {
				o.Encoder = zapcore.NewJSONEncoder(config)
			} else {
				o.Encoder = zapcore.NewConsoleEncoder(config)
			}
		}),
	)
}

func newTLSOptions() []func(c *tls.Config) {
	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		logger.Info("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}

	tlsOpts := []func(*tls.Config){}
	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}
	return tlsOpts
}

// newCacheOptions creates and returns a cache.Options instance configured with default namespaces and object caching settings.
func newCacheOptions() cache.Options {
	defaultNamespaces := make(map[string]cache.Config)
	if !util.ContainsString(namespaces, cache.AllNamespaces) {
		for _, ns := range namespaces {
			defaultNamespaces[ns] = cache.Config{}
		}
	}

	byObject := map[client.Object]cache.ByObject{
		&corev1.Pod{}: {
			Label: labels.SelectorFromSet(labels.Set{
				common.LabelLaunchedBySparkOperator: "true",
			}),
		},
		&v1beta2.SparkApplication{}:          {},
		&v1beta2.ScheduledSparkApplication{}: {},
		&admissionregistrationv1.MutatingWebhookConfiguration{}: {
			Field: fields.SelectorFromSet(fields.Set{
				"metadata.name": mutatingWebhookName,
			}),
		},
		&admissionregistrationv1.ValidatingWebhookConfiguration{}: {
			Field: fields.SelectorFromSet(fields.Set{
				"metadata.name": validatingWebhookName,
			}),
		},
	}

	if enableResourceQuotaEnforcement {
		byObject[&corev1.ResourceQuota{}] = cache.ByObject{}
	}

	options := cache.Options{
		Scheme:            scheme,
		DefaultNamespaces: defaultNamespaces,
		ByObject:          byObject,
	}

	return options
}
