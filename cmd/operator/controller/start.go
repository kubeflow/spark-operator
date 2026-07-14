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

package controller

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"slices"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	// Import features package to register feature gates.
	"github.com/kubeflow/spark-operator/v2/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/rest"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	logzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	ctrlwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"

	operatortls "github.com/kubeflow/spark-operator/v2/pkg/tls"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/internal/controller/scheduledsparkapplication"
	"github.com/kubeflow/spark-operator/v2/internal/controller/sparkapplication"
	"github.com/kubeflow/spark-operator/v2/internal/controller/sparkconnect"
	"github.com/kubeflow/spark-operator/v2/internal/metrics"
	"github.com/kubeflow/spark-operator/v2/internal/scheduler"
	"github.com/kubeflow/spark-operator/v2/internal/scheduler/kubescheduler"
	"github.com/kubeflow/spark-operator/v2/internal/scheduler/volcano"
	"github.com/kubeflow/spark-operator/v2/internal/scheduler/yunikorn"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	operatorscheme "github.com/kubeflow/spark-operator/v2/pkg/scheme"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
	"github.com/kubeflow/spark-operator/v2/pkg/version"
	// +kubebuilder:scaffold:imports
)

var (
	logger = ctrl.Log.WithName("")
)

var (
	namespaces        []string
	namespaceSelector string

	// Controller
	controllerThreads        int
	cacheSyncTimeout         time.Duration
	maxTrackedExecutorPerApp int

	// Driver PDB feature gate. When enabled, the controller creates a
	// PodDisruptionBudget for each SparkApplication that sets
	// spec.driverPodDisruptionBudget=true. Defaults to false so the upgrade
	// path is no-op for existing clusters.
	enableDriverPDB bool

	//WorkQueue
	workqueueRateLimiterBucketQPS  int
	workqueueRateLimiterBucketSize int
	workqueueRateLimiterMaxDelay   time.Duration

	// Batch scheduler
	enableBatchScheduler  bool
	kubeSchedulerNames    []string
	defaultBatchScheduler string

	// Spark web UI service and ingress
	enableUIService    bool
	ingressClassName   string
	ingressURLFormat   string
	ingressTLS         []networkingv1.IngressTLS
	ingressAnnotations map[string]string

	// Leader election
	enableLeaderElection        bool
	leaderElectionLockName      string
	leaderElectionLockNamespace string
	leaderElectionLeaseDuration time.Duration
	leaderElectionRenewDeadline time.Duration
	leaderElectionRetryPeriod   time.Duration

	driverPodCreationGracePeriod time.Duration

	// Kubernetes API server QPS and Burst for the controller manager's client.
	kubeAPIQPS   float32
	kubeAPIBurst int

	// Metrics
	enableMetrics                  bool
	metricsBindAddress             string
	metricsEndpoint                string
	metricsPrefix                  string
	metricsLabels                  []string
	metricsJobSubmitLatencyBuckets []float64
	metricsJobStartLatencyBuckets  []float64

	healthProbeBindAddress                      string
	pprofBindAddress                            string
	secureMetrics                               bool
	tlsMinVersion                               string
	tlsCipherSuites                             []string
	scheduledSparkApplicationTimestampPrecision string
	development                                 bool
	zapOptions                                  = logzap.Options{}

	// REST submitter (behind RestSubmitter feature gate)
	submitterServiceURL     string
	submitterStartupTimeout time.Duration
	submitterRequestTimeout time.Duration
	submitterMaxRetries     int
	submitterInitialBackoff time.Duration
	submitterTLSEnabled     bool
	submitterTLSCertFile    string
	submitterTLSKeyFile     string
	submitterTLSCAFile      string
)

func NewStartCommand() *cobra.Command {
	var ingressTLSstring string
	var ingressAnnotationsString string
	var command = &cobra.Command{
		Use:   "start",
		Short: "Start controller and webhook",
		PreRunE: func(_ *cobra.Command, args []string) error {
			development = viper.GetBool("development")

			if ingressTLSstring != "" {
				if err := json.Unmarshal([]byte(ingressTLSstring), &ingressTLS); err != nil {
					return fmt.Errorf("failed parsing ingress-tls JSON string from CLI: %v", err)
				}
			}
			if ingressAnnotationsString != "" {
				if err := json.Unmarshal([]byte(ingressAnnotationsString), &ingressAnnotations); err != nil {
					return fmt.Errorf("failed parsing ingress-annotations JSON string from CLI: %v", err)
				}
			}

			validPrecisions := []string{"nanos", "micros", "millis", "seconds", "minutes"}
			if !slices.Contains(validPrecisions, scheduledSparkApplicationTimestampPrecision) {
				return fmt.Errorf("invalid value %q for --scheduled-spark-application-timestamp-precision, valid values: %v", scheduledSparkApplicationTimestampPrecision, validPrecisions)
			}

			if features.Enabled(features.RestSubmitter) {
				if submitterServiceURL == "" {
					return fmt.Errorf("--submitter-service-url is required when RestSubmitter feature gate is enabled")
				}
				if submitterMaxRetries < 1 {
					return fmt.Errorf("invalid value %d for --submitter-max-retries, must be at least 1", submitterMaxRetries)
				}
				if submitterTLSEnabled {
					if submitterTLSCertFile == "" || submitterTLSKeyFile == "" || submitterTLSCAFile == "" {
						return fmt.Errorf("--submitter-tls-enabled requires --submitter-tls-cert-file, --submitter-tls-key-file, and --submitter-tls-ca-file")
					}
				}
			}

			return nil
		},
		Run: func(_ *cobra.Command, args []string) {
			version.PrintVersion(false)
			start()
		},
	}

	command.Flags().IntVar(&controllerThreads, "controller-threads", 10, "Number of worker threads used by the SparkApplication controller.")
	command.Flags().StringSliceVar(&namespaces, "namespaces", []string{}, "The Kubernetes namespace to manage. Will manage custom resource objects of the managed CRD types for the whole cluster if unset or contains empty string.")
	command.Flags().StringVar(&namespaceSelector, "namespace-selector", "", "Label selector for namespaces to watch (e.g., 'spark-operator=enabled,env in (prod,staging)'). Namespaces matching this selector will be watched in addition to those specified via --namespaces. Requires ClusterRole permission to list and watch namespaces.")
	command.Flags().DurationVar(&cacheSyncTimeout, "cache-sync-timeout", 30*time.Second, "Informer cache sync timeout.")
	command.Flags().IntVar(&maxTrackedExecutorPerApp, "max-tracked-executor-per-app", 1000, "The maximum number of tracked executors per SparkApplication.")
	command.Flags().BoolVar(&enableDriverPDB, "enable-driver-pdb", false,
		"Enable creation of a PodDisruptionBudget for Spark driver pods. "+
			"Each SparkApplication must additionally opt in via "+
			"spec.driverPodDisruptionBudget=true.")

	command.Flags().IntVar(&workqueueRateLimiterBucketQPS, "workqueue-ratelimiter-bucket-qps", 10, "QPS of the bucket rate of the workqueue.")
	command.Flags().IntVar(&workqueueRateLimiterBucketSize, "workqueue-ratelimiter-bucket-size", 100, "The token bucket size of the workqueue.")
	command.Flags().DurationVar(&workqueueRateLimiterMaxDelay, "workqueue-ratelimiter-max-delay", rate.InfDuration, "The maximum delay of the workqueue.")

	command.Flags().BoolVar(&enableBatchScheduler, "enable-batch-scheduler", false, "Enable batch schedulers.")
	command.Flags().StringSliceVar(&kubeSchedulerNames, "kube-scheduler-names", []string{}, "The kube-scheduler names for scheduling Spark applications.")
	command.Flags().StringVar(&defaultBatchScheduler, "default-batch-scheduler", "", "Default batch scheduler.")

	command.Flags().BoolVar(&enableUIService, "enable-ui-service", true, "Enable Spark Web UI service.")
	command.Flags().StringVar(&ingressClassName, "ingress-class-name", "", "Set ingressClassName for ingress resources created.")
	command.Flags().StringVar(&ingressURLFormat, "ingress-url-format", "", "Ingress URL format.")
	command.Flags().StringVar(&ingressTLSstring, "ingress-tls", "", "JSON format string for the default TLS config on the Spark UI ingresses. e.g. '[{\"hosts\":[\"*.example.com\"],\"secretName\":\"example-secret\"}]'. `ingressTLS` in the SparkApplication spec will override this value.")
	command.Flags().StringVar(&ingressAnnotationsString, "ingress-annotations", "", "JSON format string for the default ingress annotations for the Spark UI ingresses. e.g. '[{\"cert-manager.io/cluster-issuer\": \"letsencrypt\"}]'. `ingressAnnotations` in the SparkApplication spec will override this value.")

	command.Flags().BoolVar(&enableLeaderElection, "leader-election", false, "Enable leader election for controller manager. "+
		"Enabling this will ensure there is only one active controller manager.")
	command.Flags().StringVar(&leaderElectionLockName, "leader-election-lock-name", "spark-operator-lock", "Name of the ConfigMap for leader election.")
	command.Flags().StringVar(&leaderElectionLockNamespace, "leader-election-lock-namespace", "spark-operator", "Namespace in which to create the ConfigMap for leader election.")
	command.Flags().DurationVar(&leaderElectionLeaseDuration, "leader-election-lease-duration", 15*time.Second, "Leader election lease duration.")
	command.Flags().DurationVar(&leaderElectionRenewDeadline, "leader-election-renew-deadline", 10*time.Second, "Leader election renew deadline.")
	command.Flags().DurationVar(&leaderElectionRetryPeriod, "leader-election-retry-period", 2*time.Second, "Leader election retry period.")

	command.Flags().DurationVar(&driverPodCreationGracePeriod, "driver-pod-creation-grace-period", 10*time.Second, "Grace period after a successful spark-submit when driver pod not found errors will be retried, and maximum time to wait for an in-flight spark-submit to exit after SIGTERM during controller shutdown. Manager GracefulShutdownTimeout is this value plus one second. Useful if driver pod creation can take some time. Should be set so this value plus one second is lower than the pod's terminationGracePeriodSeconds.")

	command.Flags().Float32Var(&kubeAPIQPS, "kube-api-qps", 20, "Maximum QPS to the API server from the controller client.")
	command.Flags().IntVar(&kubeAPIBurst, "kube-api-burst", 30, "Maximum burst for throttle from the controller client.")

	command.Flags().BoolVar(&enableMetrics, "enable-metrics", false, "Enable metrics.")
	command.Flags().StringVar(&metricsBindAddress, "metrics-bind-address", "0", "The address the metric endpoint binds to. "+
		"Use the port :8080. If not set, it will be 0 in order to disable the metrics server")
	command.Flags().StringVar(&metricsEndpoint, "metrics-endpoint", "/metrics", "Metrics endpoint.")
	command.Flags().StringVar(&metricsPrefix, "metrics-prefix", "", "Prefix for the metrics.")
	command.Flags().StringSliceVar(&metricsLabels, "metrics-labels", []string{}, "Labels to be added to the metrics.")
	command.Flags().Float64SliceVar(&metricsJobSubmitLatencyBuckets, "metrics-job-submit-latency-buckets", []float64{0.5, 1, 2, 4, 8, 16, 32, 64, 128, 256}, "Buckets for the job submit latency histogram.")
	command.Flags().Float64SliceVar(&metricsJobStartLatencyBuckets, "metrics-job-start-latency-buckets", []float64{30, 60, 90, 120, 150, 180, 210, 240, 270, 300}, "Buckets for the job start latency histogram.")

	command.Flags().StringVar(&healthProbeBindAddress, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	command.Flags().BoolVar(&secureMetrics, "secure-metrics", false, "If set the metrics endpoint is served securely")
	command.Flags().StringVar(&tlsMinVersion, "metric-tls-min-version", "VersionTLS12",
		"Minimum TLS version for the metrics server. "+
			"Possible values: VersionTLS12, VersionTLS13")
	command.Flags().StringSliceVar(&tlsCipherSuites, "metric-tls-cipher-suites", []string{},
		"Comma-separated list of cipher suites for the metrics server. "+
			"If omitted, the default Go cipher suites are used. "+
			"Applies to TLS 1.2 only; TLS 1.3 cipher suites are not configurable in Go. Possible values listed at https://pkg.go.dev/crypto/tls#CipherSuites")

	command.Flags().StringVar(&pprofBindAddress, "pprof-bind-address", "0", "The address the pprof endpoint binds to. "+
		"If not set, it will be 0 in order to disable the pprof server")

	command.Flags().StringVar(&scheduledSparkApplicationTimestampPrecision, "scheduled-spark-application-timestamp-precision", "nanos", "Timestamp precision for ScheduledSparkApplication run names. Valid values: nanos, micros, millis, seconds, minutes.")

	// REST submitter flags (used when RestSubmitter feature gate is enabled)
	command.Flags().StringVar(&submitterServiceURL, "submitter-service-url", "", "Full submit endpoint URL of the submitter service (required when RestSubmitter feature gate is enabled).")
	command.Flags().DurationVar(&submitterStartupTimeout, "submitter-startup-timeout", 5*time.Minute, "How long the controller waits for the submitter service to become reachable at startup.")
	command.Flags().DurationVar(&submitterRequestTimeout, "submitter-request-timeout", 2*time.Minute, "HTTP request timeout per spark submission attempt.")
	command.Flags().IntVar(&submitterMaxRetries, "submitter-max-retries", 3, "Max retry attempts for transient spark submission failures.")
	command.Flags().DurationVar(&submitterInitialBackoff, "submitter-initial-backoff", 2*time.Second, "Initial backoff duration before the first retry.")
	command.Flags().BoolVar(&submitterTLSEnabled, "submitter-tls-enabled", false, "Enable mTLS for communication with the submitter service.")
	command.Flags().StringVar(&submitterTLSCertFile, "submitter-tls-cert-file", "", "Path to the client certificate file for mTLS with the submitter service.")
	command.Flags().StringVar(&submitterTLSKeyFile, "submitter-tls-key-file", "", "Path to the client private key file for mTLS with the submitter service.")
	command.Flags().StringVar(&submitterTLSCAFile, "submitter-tls-ca-file", "", "Path to the CA certificate file for verifying the submitter service.")

	flagSet := flag.NewFlagSet("controller", flag.ExitOnError)
	ctrl.RegisterFlags(flagSet)
	zapOptions.BindFlags(flagSet)
	command.Flags().AddGoFlagSet(flagSet)

	utilfeature.DefaultMutableFeatureGate.AddFlag(command.Flags())

	return command
}

func start() {
	setupLog()

	// Bounds how long the manager waits for in-flight reconciles after SIGTERM. Each reconcile
	// blocked in runSparkSubmit may take up to driverPodCreationGracePeriod to return (SIGTERM +
	// WaitDelay). Add one second so the manager does not exit while the subprocess is still
	// within that wait window. Must stay within terminationGracePeriodSeconds on the pod.
	managerGracefulShutdownTimeout := driverPodCreationGracePeriod + time.Second

	// Create the client rest config. Use kubeConfig if given, otherwise assume in-cluster.
	cfg, err := ctrl.GetConfig()
	cfg.WarningHandler = rest.NoWarnings{}
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
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: operatorscheme.ControllerScheme,
		Cache:  newCacheOptions(),
		Metrics: metricsserver.Options{
			BindAddress:   metricsBindAddress,
			SecureServing: secureMetrics,
			TLSOpts:       tlsOptions,
		},
		WebhookServer: ctrlwebhook.NewServer(ctrlwebhook.Options{
			TLSOpts: tlsOptions,
		}),
		HealthProbeBindAddress:  healthProbeBindAddress,
		PprofBindAddress:        pprofBindAddress,
		LeaderElection:          enableLeaderElection,
		LeaderElectionID:        leaderElectionLockName,
		LeaderElectionNamespace: leaderElectionLockNamespace,
		LeaseDuration:           &leaderElectionLeaseDuration,
		RenewDeadline:           &leaderElectionRenewDeadline,
		RetryPeriod:             &leaderElectionRetryPeriod,
		GracefulShutdownTimeout: &managerGracefulShutdownTimeout,
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
		logger.Error(err, "failed to create manager")
		os.Exit(1)
	}

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		logger.Error(err, "failed to create clientset")
		os.Exit(1)
	}

	if err = util.InitializeIngressCapabilities(clientset); err != nil {
		logger.Error(err, "failed to retrieve cluster ingress capabilities")
		os.Exit(1)
	}

	var registry *scheduler.Registry
	if enableBatchScheduler {
		registry = scheduler.GetRegistry()
		_ = registry.Register(common.VolcanoSchedulerName, volcano.Factory)
		_ = registry.Register(yunikorn.SchedulerName, yunikorn.Factory)

		// Register kube-schedulers.
		for _, name := range kubeSchedulerNames {
			_ = registry.Register(name, kubescheduler.Factory)
		}

		schedulerNames := registry.GetRegisteredSchedulerNames()
		if defaultBatchScheduler != "" && !slices.Contains(schedulerNames, defaultBatchScheduler) {
			logger.Error(nil, "Failed to find default batch scheduler in registered schedulers")
			os.Exit(1)
		}
	}

	ctx := ctrl.SetupSignalHandler()

	sparkSubmitter, err := newSparkSubmitter(ctx)
	if err != nil {
		logger.Error(err, "Failed to create spark submitter")
		os.Exit(1)
	}

	// Setup controller for SparkApplication.
	if err = sparkapplication.NewReconciler(
		mgr,
		mgr.GetScheme(),
		mgr.GetClient(),
		mgr.GetEventRecorder("spark-application-controller"),
		registry,
		sparkSubmitter,
		newSparkApplicationReconcilerOptions(),
	).SetupWithManager(mgr, newControllerOptions()); err != nil {
		logger.Error(err, "Failed to create controller", "controller", "SparkApplication")
		os.Exit(1)
	}

	// Setup controller for ScheduledSparkApplication.
	if err = scheduledsparkapplication.NewReconciler(
		mgr.GetScheme(),
		mgr.GetClient(),
		mgr.GetEventRecorder("scheduled-spark-application-controller"),
		clock.RealClock{},
		newScheduledSparkApplicationReconcilerOptions(),
	).SetupWithManager(mgr, newControllerOptions()); err != nil {
		logger.Error(err, "Failed to create controller", "controller", "ScheduledSparkApplication")
		os.Exit(1)
	}

	// Setup controller for SparkConnect.
	if err = sparkconnect.NewReconciler(
		mgr,
		mgr.GetScheme(),
		mgr.GetClient(),
		mgr.GetEventRecorder("SparkConnect"),
		newSparkConnectReconcilerOptions(),
	).SetupWithManager(mgr, newControllerOptions()); err != nil {
		logger.Error(err, "Failed to create controller", "controller", "SparkConnect")
		os.Exit(1)
	}

	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		logger.Error(err, "Failed to set up health check")
		os.Exit(1)
	}

	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
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
	var defaultNamespaces map[string]cache.Config

	// When using namespace selector, we need to cache ALL namespaces for resources
	// because we'll filter dynamically in the event filter based on namespace labels.
	// When using explicit namespace list only, we can cache just those namespaces.
	if namespaceSelector != "" {
		defaultNamespaces = nil
	} else if !slices.Contains(namespaces, cache.AllNamespaces) {
		defaultNamespaces = make(map[string]cache.Config)
		for _, ns := range namespaces {
			defaultNamespaces[ns] = cache.Config{}
		}
	}

	options := cache.Options{
		Scheme:            operatorscheme.ControllerScheme,
		DefaultNamespaces: defaultNamespaces,
		ByObject: map[client.Object]cache.ByObject{
			&corev1.Namespace{}: {},
			&corev1.Pod{}: {
				Label: labels.SelectorFromSet(labels.Set{
					common.LabelLaunchedBySparkOperator: "true",
				}),
			},
			&corev1.ConfigMap{}: {
				Label: labels.SelectorFromSet(labels.Set{
					common.LabelCreatedBySparkOperator: "true",
				}),
			},
			&corev1.PersistentVolumeClaim{}:      {},
			&corev1.Service{}:                    {},
			&v1beta2.SparkApplication{}:          {},
			&v1beta2.ScheduledSparkApplication{}: {},
			&v1alpha1.SparkConnect{}:             {},
			&policyv1.PodDisruptionBudget{}: {
				Label: labels.SelectorFromSet(labels.Set{
					common.LabelLaunchedBySparkOperator: "true",
				}),
			},
		},
	}

	return options
}

// newControllerOptions creates and returns a controller.Options instance configured with the given options.
func newControllerOptions() controller.Options {
	options := controller.Options{
		MaxConcurrentReconciles: controllerThreads,
		CacheSyncTimeout:        cacheSyncTimeout,
		RateLimiter:             util.NewRateLimiter[ctrl.Request](workqueueRateLimiterBucketQPS, workqueueRateLimiterBucketSize, workqueueRateLimiterMaxDelay),
	}
	return options
}

func newSparkApplicationReconcilerOptions() sparkapplication.Options {
	var sparkApplicationMetrics *metrics.SparkApplicationMetrics
	var sparkExecutorMetrics *metrics.SparkExecutorMetrics
	if enableMetrics {
		sparkApplicationMetrics = metrics.NewSparkApplicationMetrics(metricsPrefix, metricsLabels, metricsJobSubmitLatencyBuckets, metricsJobStartLatencyBuckets)
		sparkApplicationMetrics.Register()
		sparkExecutorMetrics = metrics.NewSparkExecutorMetrics(metricsPrefix, metricsLabels)
		sparkExecutorMetrics.Register()
	}
	options := sparkapplication.Options{
		Namespaces:                   namespaces,
		NamespaceSelector:            namespaceSelector,
		EnableUIService:              enableUIService,
		IngressClassName:             ingressClassName,
		IngressURLFormat:             ingressURLFormat,
		IngressTLS:                   ingressTLS,
		IngressAnnotations:           ingressAnnotations,
		DefaultBatchScheduler:        defaultBatchScheduler,
		DriverPodCreationGracePeriod: driverPodCreationGracePeriod,
		SparkApplicationMetrics:      sparkApplicationMetrics,
		SparkExecutorMetrics:         sparkExecutorMetrics,
		MaxTrackedExecutorPerApp:     maxTrackedExecutorPerApp,
		EnableDriverPDB:              enableDriverPDB,
	}
	if enableBatchScheduler {
		options.KubeSchedulerNames = kubeSchedulerNames
	}
	return options
}

func newScheduledSparkApplicationReconcilerOptions() scheduledsparkapplication.Options {
	options := scheduledsparkapplication.Options{
		Namespaces:         namespaces,
		NamespaceSelector:  namespaceSelector,
		TimestampPrecision: scheduledSparkApplicationTimestampPrecision,
	}
	return options
}

func newSparkConnectReconcilerOptions() sparkconnect.Options {
	options := sparkconnect.Options{
		Namespaces:        namespaces,
		NamespaceSelector: namespaceSelector,
	}
	return options
}

func newSparkSubmitter(ctx context.Context) (sparkapplication.SparkApplicationSubmitter, error) {
	if !features.Enabled(features.RestSubmitter) {
		return &sparkapplication.SparkSubmitter{
			ShutdownGracePeriod: driverPodCreationGracePeriod,
		}, nil
	}

	var tlsCfg *sparkapplication.TLSConfig
	if submitterTLSEnabled {
		tlsCfg = &sparkapplication.TLSConfig{
			Enabled:    true,
			CertFile:   submitterTLSCertFile,
			KeyFile:    submitterTLSKeyFile,
			CACertFile: submitterTLSCAFile,
		}
	}

	submitter, err := sparkapplication.NewRestSparkSubmitter(sparkapplication.RestSparkSubmitterConfig{
		URL:             submitterServiceURL,
		RetryMaxRetries: submitterMaxRetries,
		RequestTimeout:  submitterRequestTimeout,
		InitialBackoff:  submitterInitialBackoff,
		TLS:             tlsCfg,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize RestSubmitter: %w", err)
	}

	startupCtx, cancel := context.WithTimeout(ctx, submitterStartupTimeout)
	defer cancel()
	if err := submitter.WaitForConnection(startupCtx); err != nil {
		return nil, fmt.Errorf("RestSubmitter service not reachable at startup: %w", err)
	}

	logger.Info("Using REST submitter service", "url", submitterServiceURL)
	return submitter, nil
}
