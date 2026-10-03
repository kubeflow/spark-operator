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
	"slices"
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
	"k8s.io/apimachinery/pkg/util/wait"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	ctrlwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"

	operatortls "github.com/kubeflow/spark-operator/v2/pkg/tls"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/internal/controller/mutatingwebhookconfiguration"
	"github.com/kubeflow/spark-operator/v2/internal/controller/validatingwebhookconfiguration"
	"github.com/kubeflow/spark-operator/v2/internal/webhook"
	"github.com/kubeflow/spark-operator/v2/pkg/certificate"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	operatorscheme "github.com/kubeflow/spark-operator/v2/pkg/scheme"
	"github.com/kubeflow/spark-operator/v2/pkg/version"
	// +kubebuilder:scaffold:imports
)

var (
	logger = ctrl.Log.WithName("")
)

const filesystemCertificateRetryInterval = time.Second

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
	webhookCertProvider            string
	webhookCertWaitTimeout         time.Duration
	webhookCABundleFile            string
	webhookCABundleSync            string
	webhookCABundleSyncInterval    time.Duration

	// Cert Manager
	enableCertManager bool

	// Leader election
	enableLeaderElection        bool
	leaderElectionLockName      string
	leaderElectionLockNamespace string
	leaderElectionLeaseDuration time.Duration
	leaderElectionRenewDeadline time.Duration
	leaderElectionRetryPeriod   time.Duration

	// Kubernetes API server QPS and Burst for the controller manager's client.
	kubeAPIQPS   float32
	kubeAPIBurst int

	// Metrics
	enableMetrics      bool
	metricsBindAddress string
	metricsEndpoint    string
	metricsPrefix      string
	metricsLabels      []string

	healthProbeBindAddress string
	secureMetrics          bool
	tlsMinVersion          string
	tlsCipherSuites        []string
	development            bool
	zapOptions             = logzap.Options{}
)

func NewStartCommand() *cobra.Command {
	var command = &cobra.Command{
		Use:   "start",
		Short: "Start controller and webhook",
		PreRun: func(_ *cobra.Command, args []string) {
			development = viper.GetBool("development")
		},
		Run: func(cmd *cobra.Command, args []string) {
			version.PrintVersion(false)
			start(cmd.Flags().Changed("webhook-cert-provider"))
		},
	}

	// Controller
	command.Flags().IntVar(&controllerThreads, "controller-threads", 10, "Number of worker threads used by the SparkApplication controller.")
	command.Flags().StringSliceVar(&namespaces, "namespaces", []string{}, "The Kubernetes namespace to manage. Will manage custom resource objects of the managed CRD types for the whole cluster if unset or contains empty string.")
	command.Flags().StringVar(&labelSelectorFilter, "label-selector-filter", "", "A comma-separated list of key=value, or key labels to filter resources during watch and list based on the specified labels.")
	command.Flags().DurationVar(&cacheSyncTimeout, "cache-sync-timeout", 30*time.Second, "Informer cache sync timeout.")

	command.Flags().Float32Var(&kubeAPIQPS, "kube-api-qps", 20, "Maximum QPS to the API server from the controller client.")
	command.Flags().IntVar(&kubeAPIBurst, "kube-api-burst", 30, "Maximum burst for throttle from the controller client.")

	// Webhook
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
	command.Flags().StringVar(&webhookCertProvider, "webhook-cert-provider", string(certificateProviderSelfSigned), "The provider for the webhook server certificate. Valid values are self-signed, cert-manager, and filesystem.")
	command.Flags().DurationVar(&webhookCertWaitTimeout, "webhook-cert-wait-timeout", 2*time.Minute, "Maximum time to wait for an initial filesystem certificate and key pair.")
	command.Flags().StringVar(&webhookCABundleFile, "webhook-ca-bundle-file", "", "Path to the CA bundle file the operator publishes to the webhook configurations' caBundle when it owns trust (filesystem provider). Defaults to ca.crt under --webhook-cert-dir.")
	command.Flags().StringVar(&webhookCABundleSync, "webhook-ca-bundle-sync", string(caBundleSyncModeAuto), "Who reconciles the webhook caBundle. Valid values are auto, enabled, and disabled. auto lets the certificate provider decide.")
	command.Flags().DurationVar(&webhookCABundleSyncInterval, "webhook-ca-bundle-sync-interval", 10*time.Second, "Interval at which the operator re-reads the CA bundle file when it owns trust (filesystem provider).")
	command.Flags().BoolVar(&enableResourceQuotaEnforcement, "enable-resource-quota-enforcement", false, "Whether to enable ResourceQuota enforcement for SparkApplication resources. Requires the webhook to be enabled.")

	// Cert Manager
	command.Flags().BoolVar(&enableCertManager, "enable-cert-manager", false, "Enable cert-manager to manage the webhook server's TLS certificate.")

	// Leader election
	command.Flags().BoolVar(&enableLeaderElection, "leader-election", false, "Enable leader election for controller manager. "+
		"Enabling this will ensure there is only one active controller manager.")
	command.Flags().StringVar(&leaderElectionLockName, "leader-election-lock-name", "spark-operator-lock", "Name of the ConfigMap for leader election.")
	command.Flags().StringVar(&leaderElectionLockNamespace, "leader-election-lock-namespace", "spark-operator", "Namespace in which to create the ConfigMap for leader election.")
	command.Flags().DurationVar(&leaderElectionLeaseDuration, "leader-election-lease-duration", 15*time.Second, "Leader election lease duration.")
	command.Flags().DurationVar(&leaderElectionRenewDeadline, "leader-election-renew-deadline", 10*time.Second, "Leader election renew deadline.")
	command.Flags().DurationVar(&leaderElectionRetryPeriod, "leader-election-retry-period", 2*time.Second, "Leader election retry period.")

	// Prometheus metrics
	command.Flags().BoolVar(&enableMetrics, "enable-metrics", false, "Enable metrics.")
	command.Flags().StringVar(&metricsBindAddress, "metrics-bind-address", "0", "The address the metric endpoint binds to. "+
		"Use the port :8080. If not set, it will be 0 in order to disable the metrics server")
	command.Flags().StringVar(&metricsEndpoint, "metrics-endpoint", "/metrics", "Metrics endpoint.")
	command.Flags().StringVar(&metricsPrefix, "metrics-prefix", "", "Prefix for the metrics.")
	command.Flags().StringSliceVar(&metricsLabels, "metrics-labels", []string{}, "Labels to be added to the metrics.")

	command.Flags().StringVar(&healthProbeBindAddress, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	command.Flags().BoolVar(&secureMetrics, "secure-metrics", false, "If set the metrics endpoint is served securely")
	command.Flags().StringVar(&tlsMinVersion, "webhook-tls-min-version", "VersionTLS12",
		"Minimum TLS version for the webhook and metrics servers. "+
			"Possible values: VersionTLS12, VersionTLS13")
	command.Flags().StringSliceVar(&tlsCipherSuites, "webhook-tls-cipher-suites", []string{},
		"Comma-separated list of cipher suites for the webhook and metrics servers. "+
			"If omitted, the default Go cipher suites are used. "+
			"Applies to TLS 1.2 only; TLS 1.3 cipher suites are not configurable in Go. Possible values listed at https://pkg.go.dev/crypto/tls#CipherSuites")

	flagSet := flag.NewFlagSet("controller", flag.ExitOnError)
	ctrl.RegisterFlags(flagSet)
	zapOptions.BindFlags(flagSet)
	command.Flags().AddGoFlagSet(flagSet)

	return command
}

func start(providerExplicit bool) {
	setupLog()

	certOptions, err := resolveCertificateOptions(certificateOptionsInput{
		provider:             webhookCertProvider,
		providerExplicit:     providerExplicit,
		enableCertManager:    enableCertManager,
		certDir:              webhookCertDir,
		certName:             webhookCertName,
		keyName:              webhookKeyName,
		waitTimeout:          webhookCertWaitTimeout,
		retryInterval:        filesystemCertificateRetryInterval,
		caBundleFile:         webhookCABundleFile,
		caBundleSync:         webhookCABundleSync,
		caBundleSyncInterval: webhookCABundleSyncInterval,
	})
	if err != nil {
		logger.Error(err, "Failed to resolve certificate provider")
		os.Exit(1)
	}

	ctx := ctrl.SetupSignalHandler()

	// Create the client rest config. Use kubeConfig if given, otherwise assume in-cluster.
	cfg, err := ctrl.GetConfig()
	if err != nil {
		logger.Error(err, "failed to get kube config")
		os.Exit(1)
	}

	cfg.QPS = kubeAPIQPS
	cfg.Burst = kubeAPIBurst

	// Create the manager.
	tlsOptions, err := operatortls.SetupTLS(tlsMinVersion, tlsCipherSuites)
	if err != nil {
		logger.Error(err, "Failed to set up TLS")
		os.Exit(1)
	}
	webhookTLSOptions := tlsOptions
	var dynamicServing *certificate.DynamicTLSConfig
	if certOptions.provider == certificateProviderFilesystem {
		baseTLSConfig := &tls.Config{}
		for _, option := range tlsOptions {
			option(baseTLSConfig)
		}
		dynamicServing, err = certificate.NewDynamicTLSConfig(
			ctx,
			certOptions.certPath,
			certOptions.keyPath,
			certOptions.waitTimeout,
			certOptions.retryInterval,
			baseTLSConfig,
		)
		if err != nil {
			logger.Error(err, "Failed to initialize filesystem certificate provider")
			os.Exit(1)
		}
		webhookTLSOptions = dynamicWebhookTLSOptions(tlsOptions, dynamicServing)
	}
	webhookServer := newWebhookServer(ctrlwebhook.Options{
		Port:     webhookPort,
		CertDir:  webhookCertDir,
		CertName: webhookCertName,
		KeyName:  webhookKeyName,
		TLSOpts:  webhookTLSOptions,
	}, dynamicServing)
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: operatorscheme.WebhookScheme,
		Cache:  newCacheOptions(),
		Client: client.Options{},
		Metrics: metricsserver.Options{
			BindAddress:   metricsBindAddress,
			SecureServing: secureMetrics,
			TLSOpts:       tlsOptions,
		},
		WebhookServer:           webhookServer,
		HealthProbeBindAddress:  healthProbeBindAddress,
		LeaderElection:          enableLeaderElection,
		LeaderElectionID:        leaderElectionLockName,
		LeaderElectionNamespace: leaderElectionLockNamespace,
		LeaseDuration:           &leaderElectionLeaseDuration,
		RenewDeadline:           &leaderElectionRenewDeadline,
		RetryPeriod:             &leaderElectionRetryPeriod,
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

	var certProvider *certificate.Provider
	if certOptions.provider != certificateProviderFilesystem {
		directClient, err := client.New(cfg, client.Options{Scheme: mgr.GetScheme()})
		if err != nil {
			logger.Error(err, "Failed to create client")
			os.Exit(1)
		}
		certProvider = certificate.NewProvider(
			directClient,
			webhookServiceName,
			webhookServiceNamespace,
			certOptions.provider == certificateProviderCertManager,
		)
	}

	// caBundleSource feeds the reconcilers the trust bundle to publish. When the
	// operator owns the caBundle in filesystem mode, it is a FilesystemCABundleSource
	// running as a non-leader-elected manager runnable that reads the mounted CA
	// file and signals each reconciler on change; otherwise it is the Provider.
	var caBundleSource certificate.CABundleSource = certProvider
	var mutatingCAEvents, validatingCAEvents <-chan event.GenericEvent
	if certOptions.provider == certificateProviderFilesystem && certOptions.caBundleOwner == caBundleOwnerOperator {
		filesystemCABundleSource, err := certificate.NewFilesystemCABundleSource(
			ctx,
			certOptions.caBundleFile,
			certOptions.caBundleSyncInterval,
			certOptions.waitTimeout,
			certOptions.retryInterval,
			dynamicServing,
		)
		if err != nil {
			logger.Error(err, "Failed to initialize filesystem CA bundle source")
			os.Exit(1)
		}
		if err := mgr.Add(filesystemCABundleSource); err != nil {
			logger.Error(err, "Failed to register filesystem CA bundle source")
			os.Exit(1)
		}
		caBundleSource = filesystemCABundleSource
		mutatingCAEvents = filesystemCABundleSource.RegisterSink()
		validatingCAEvents = filesystemCABundleSource.RegisterSink()
	}

	if err := runCertificateStartup(ctx, certOptions, certificateStartupActions{
		syncSecret: func(ctx context.Context, _ certificateProvider) error {
			return wait.ExponentialBackoffWithContext(
				ctx,
				wait.Backoff{
					Steps:    5,
					Duration: 1 * time.Second,
					Factor:   2.0,
					Jitter:   0.1,
				},
				func(ctx context.Context) (bool, error) {
					if err := certProvider.SyncSecret(ctx, webhookSecretName, webhookSecretNamespace); err != nil {
						if errors.IsAlreadyExists(err) || errors.IsConflict(err) {
							return false, nil
						}
						return false, err
					}
					return true, nil
				},
			)
		},
		writeFiles: func() error {
			logger.Info("Writing certificates", "path", webhookCertDir, "certificate name", webhookCertName, "key name", webhookKeyName)
			return certProvider.WriteFile(webhookCertDir, webhookCertName, webhookKeyName)
		},
		setupCAReconcilers: func() error {
			mutatingReconciler := mutatingwebhookconfiguration.NewReconciler(
				mgr.GetClient(),
				caBundleSource,
				mutatingWebhookName,
			)
			if mutatingCAEvents != nil {
				mutatingReconciler = mutatingReconciler.WithCABundleEventChannel(mutatingCAEvents)
			}
			if err := mutatingReconciler.SetupWithManager(mgr, controller.Options{}); err != nil {
				return err
			}
			validatingReconciler := validatingwebhookconfiguration.NewReconciler(
				mgr.GetClient(),
				caBundleSource,
				validatingWebhookName,
			)
			if validatingCAEvents != nil {
				validatingReconciler = validatingReconciler.WithCABundleEventChannel(validatingCAEvents)
			}
			return validatingReconciler.SetupWithManager(mgr, controller.Options{})
		},
	}); err != nil {
		logger.Error(err, "Failed to initialize certificate provider", "provider", certOptions.provider)
		os.Exit(1)
	}

	if err := ctrl.NewWebhookManagedBy(mgr, &v1alpha1.SparkConnect{}).
		WithDefaulter(webhook.NewSparkConnectDefaulter()).
		WithValidator(webhook.NewSparkConnectValidator()).
		WithLogConstructor(webhook.LogConstructor).
		Complete(); err != nil {
		logger.Error(err, "Failed to create mutating webhook for SparkConnect")
		os.Exit(1)
	}

	if err := ctrl.NewWebhookManagedBy(mgr, &v1beta2.SparkApplication{}).
		WithDefaulter(webhook.NewSparkApplicationDefaulter()).
		WithValidator(webhook.NewSparkApplicationValidator(mgr.GetClient(), enableResourceQuotaEnforcement)).
		WithLogConstructor(webhook.LogConstructor).
		Complete(); err != nil {
		logger.Error(err, "Failed to create mutating webhook for Spark application")
		os.Exit(1)
	}

	if err := ctrl.NewWebhookManagedBy(mgr, &v1beta2.ScheduledSparkApplication{}).
		WithDefaulter(webhook.NewScheduledSparkApplicationDefaulter()).
		WithValidator(webhook.NewScheduledSparkApplicationValidator()).
		WithLogConstructor(webhook.LogConstructor).
		Complete(); err != nil {
		logger.Error(err, "Failed to create mutating webhook for Scheduled Spark application")
		os.Exit(1)
	}

	if err := ctrl.NewWebhookManagedBy(mgr, &corev1.Pod{}).
		WithDefaulter(webhook.NewSparkPodDefaulter(mgr.GetClient(), namespaces)).
		WithLogConstructor(webhook.LogConstructor).
		Complete(); err != nil {
		logger.Error(err, "Failed to create mutating webhook for Spark pod")
		os.Exit(1)
	}

	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", mgr.GetWebhookServer().StartedChecker()); err != nil {
		logger.Error(err, "Failed to set up health check")
		os.Exit(1)
	}

	readyzChecker := mgr.GetWebhookServer().StartedChecker()
	if certOptions.provider == certificateProviderFilesystem && certOptions.caBundleOwner == caBundleOwnerOperator {
		// The operator publishes the caBundle here, so readiness must reflect that
		// both fail-closed admission objects actually carry it before this replica
		// is declared ready. Other modes never read admission objects.
		readyzChecker = allCheckers(
			readyzChecker,
			caBundleReadinessChecker(mgr.GetClient(), caBundleSource, mutatingWebhookName, validatingWebhookName),
		)
	}
	if err := mgr.AddReadyzCheck("readyz", readyzChecker); err != nil {
		logger.Error(err, "Failed to set up ready check")
		os.Exit(1)
	}

	logger.Info("Starting manager")
	if err := mgr.Start(ctx); err != nil {
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
			o.ZapOpts = append(o.ZapOpts, zap.AddCaller())
			o.EncoderConfigOptions = append(o.EncoderConfigOptions, func(config *zapcore.EncoderConfig) {
				config.EncodeLevel = zapcore.CapitalLevelEncoder
				config.EncodeTime = zapcore.ISO8601TimeEncoder
				config.EncodeCaller = zapcore.ShortCallerEncoder
			})
		}),
	)
}

// newCacheOptions creates and returns a cache.Options instance configured with default namespaces and object caching settings.
func newCacheOptions() cache.Options {
	defaultNamespaces := make(map[string]cache.Config)
	if !slices.Contains(namespaces, cache.AllNamespaces) {
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
		&corev1.ResourceQuota{}:              {},
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

	options := cache.Options{
		Scheme:            operatorscheme.WebhookScheme,
		DefaultNamespaces: defaultNamespaces,
		DefaultTransform:  cache.TransformStripManagedFields(),
		ByObject:          byObject,
	}

	return options
}
