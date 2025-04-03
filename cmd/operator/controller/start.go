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
	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	logzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	ctrlwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	sparkoperator "github.com/kubeflow/spark-operator"
	"github.com/kubeflow/spark-operator/api/v1beta1"
	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/internal/controller/scheduledsparkapplication"
	"github.com/kubeflow/spark-operator/internal/controller/sparkapplication"
	"github.com/kubeflow/spark-operator/internal/metrics"
	"github.com/kubeflow/spark-operator/internal/scheduler"
	"github.com/kubeflow/spark-operator/internal/scheduler/kubescheduler"
	"github.com/kubeflow/spark-operator/internal/scheduler/volcano"
	"github.com/kubeflow/spark-operator/internal/scheduler/yunikorn"
	"github.com/kubeflow/spark-operator/pkg/common"
	"github.com/kubeflow/spark-operator/pkg/util"
	// +kubebuilder:scaffold:imports
)

var (
	scheme = runtime.NewScheme()
	logger = ctrl.Log.WithName("")
)

var (
	namespaces []string

	// Controller
	controllerThreads        int
	cacheSyncTimeout         time.Duration
	maxTrackedExecutorPerApp int

	//WorkQueue
	workqueueRateLimiterBucketQPS  int
	workqueueRateLimiterBucketSize int
	workqueueRateLimiterMaxDelay   time.Duration

	// Batch scheduler
	enableBatchScheduler  bool
	kubeSchedulerNames    []string
	defaultBatchScheduler string

	// Spark web UI service and ingress
	enableUIService  bool
	ingressClassName string
	ingressURLFormat string

	// Leader election
	enableLeaderElection        bool
	leaderElectionLockName      string
	leaderElectionLockNamespace string
	leaderElectionLeaseDuration time.Duration
	leaderElectionRenewDeadline time.Duration
	leaderElectionRetryPeriod   time.Duration

	driverPodCreationGracePeriod time.Duration

	// Metrics
	enableMetrics                 bool
	metricsBindAddress            string
	metricsEndpoint               string
	metricsPrefix                 string
	metricsLabels                 []string
	metricsJobStartLatencyBuckets []float64

	healthProbeBindAddress string
	pprofBindAddress       string
	secureMetrics          bool
	enableHTTP2            bool
	development            bool
	zapOptions             = logzap.Options{}
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(schedulingv1alpha1.AddToScheme(scheme))

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
		Run: func(_ *cobra.Command, args []string) {
			sparkoperator.PrintVersion(false)
			start()
		},
	}

	command.Flags().IntVar(&controllerThreads, "controller-threads", 10, "Number of worker threads used by the SparkApplication controller.")
	command.Flags().StringSliceVar(&namespaces, "namespaces", []string{}, "The Kubernetes namespace to manage. Will manage custom resource objects of the managed CRD types for the whole cluster if unset or contains empty string.")
	command.Flags().DurationVar(&cacheSyncTimeout, "cache-sync-timeout", 30*time.Second, "Informer cache sync timeout.")
	command.Flags().IntVar(&maxTrackedExecutorPerApp, "max-tracked-executor-per-app", 1000, "The maximum number of tracked executors per SparkApplication.")

	command.Flags().IntVar(&workqueueRateLimiterBucketQPS, "workqueue-ratelimiter-bucket-qps", 10, "QPS of the bucket rate of the workqueue.")
	command.Flags().IntVar(&workqueueRateLimiterBucketSize, "workqueue-ratelimiter-bucket-size", 100, "The token bucket size of the workqueue.")
	command.Flags().DurationVar(&workqueueRateLimiterMaxDelay, "workqueue-ratelimiter-max-delay", rate.InfDuration, "The maximum delay of the workqueue.")

	command.Flags().BoolVar(&enableBatchScheduler, "enable-batch-scheduler", false, "Enable batch schedulers.")
	command.Flags().StringSliceVar(&kubeSchedulerNames, "kube-scheduler-names", []string{}, "The kube-scheduler names for scheduling Spark applications.")
	command.Flags().StringVar(&defaultBatchScheduler, "default-batch-scheduler", "", "Default batch scheduler.")

	command.Flags().BoolVar(&enableUIService, "enable-ui-service", true, "Enable Spark Web UI service.")
	command.Flags().StringVar(&ingressClassName, "ingress-class-name", "", "Set ingressClassName for ingress resources created.")
	command.Flags().StringVar(&ingressURLFormat, "ingress-url-format", "", "Ingress URL format.")

	command.Flags().BoolVar(&enableLeaderElection, "leader-election", false, "Enable leader election for controller manager. "+
		"Enabling this will ensure there is only one active controller manager.")
	command.Flags().StringVar(&leaderElectionLockName, "leader-election-lock-name", "spark-operator-lock", "Name of the ConfigMap for leader election.")
	command.Flags().StringVar(&leaderElectionLockNamespace, "leader-election-lock-namespace", "spark-operator", "Namespace in which to create the ConfigMap for leader election.")
	command.Flags().DurationVar(&leaderElectionLeaseDuration, "leader-election-lease-duration", 15*time.Second, "Leader election lease duration.")
	command.Flags().DurationVar(&leaderElectionRenewDeadline, "leader-election-renew-deadline", 14*time.Second, "Leader election renew deadline.")
	command.Flags().DurationVar(&leaderElectionRetryPeriod, "leader-election-retry-period", 4*time.Second, "Leader election retry period.")

	command.Flags().DurationVar(&driverPodCreationGracePeriod, "driver-pod-creation-grace-period", 10*time.Second, "Grace period after a successful spark-submit when driver pod not found errors will be retried. Useful if the driver pod can take some time to be created.")

	command.Flags().BoolVar(&enableMetrics, "enable-metrics", false, "Enable metrics.")
	command.Flags().StringVar(&metricsBindAddress, "metrics-bind-address", "0", "The address the metric endpoint binds to. "+
		"Use the port :8080. If not set, it will be 0 in order to disable the metrics server")
	command.Flags().StringVar(&metricsEndpoint, "metrics-endpoint", "/metrics", "Metrics endpoint.")
	command.Flags().StringVar(&metricsPrefix, "metrics-prefix", "", "Prefix for the metrics.")
	command.Flags().StringSliceVar(&metricsLabels, "metrics-labels", []string{}, "Labels to be added to the metrics.")
	command.Flags().Float64SliceVar(&metricsJobStartLatencyBuckets, "metrics-job-start-latency-buckets", []float64{30, 60, 90, 120, 150, 180, 210, 240, 270, 300}, "Buckets for the job start latency histogram.")

	command.Flags().StringVar(&healthProbeBindAddress, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	command.Flags().BoolVar(&secureMetrics, "secure-metrics", false, "If set the metrics endpoint is served securely")
	command.Flags().BoolVar(&enableHTTP2, "enable-http2", false, "If set, HTTP/2 will be enabled for the metrics and webhook servers")

	command.Flags().StringVar(&pprofBindAddress, "pprof-bind-address", "0", "The address the pprof endpoint binds to. "+
		"If not set, it will be 0 in order to disable the pprof server")

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
			TLSOpts: tlsOptions,
		}),
		HealthProbeBindAddress:  healthProbeBindAddress,
		PprofBindAddress:        pprofBindAddress,
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

	// Setup controller for SparkApplication.
	if err = sparkapplication.NewReconciler(
		mgr,
		mgr.GetScheme(),
		mgr.GetClient(),
		mgr.GetEventRecorderFor("spark-application-controller"),
		registry,
		newSparkApplicationReconcilerOptions(),
	).SetupWithManager(mgr, newControllerOptions()); err != nil {
		logger.Error(err, "Failed to create controller", "controller", "SparkApplication")
		os.Exit(1)
	}

	// Setup controller for ScheduledSparkApplication.
	if err = scheduledsparkapplication.NewReconciler(
		mgr.GetScheme(),
		mgr.GetClient(),
		mgr.GetEventRecorderFor("scheduled-spark-application-controller"),
		clock.RealClock{},
		newScheduledSparkApplicationReconcilerOptions(),
	).SetupWithManager(mgr, newControllerOptions()); err != nil {
		logger.Error(err, "Failed to create controller", "controller", "ScheduledSparkApplication")
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

	options := cache.Options{
		Scheme:            scheme,
		DefaultNamespaces: defaultNamespaces,
		ByObject: map[client.Object]cache.ByObject{
			&corev1.Pod{}: {
				Label: labels.SelectorFromSet(labels.Set{
					common.LabelLaunchedBySparkOperator: "true",
				}),
			},
			&corev1.ConfigMap{}:             {},
			&corev1.PersistentVolumeClaim{}: {},
			&corev1.Service{}:               {},
			&v1beta2.SparkApplication{}:     {},
		},
	}

	return options
}

// newControllerOptions creates and returns a controller.Options instance configured with the given options.
func newControllerOptions() controller.Options {
	options := controller.Options{
		MaxConcurrentReconciles: controllerThreads,
		CacheSyncTimeout:        cacheSyncTimeout,
		RateLimiter:             util.NewRateLimiter(workqueueRateLimiterBucketQPS, workqueueRateLimiterBucketSize, workqueueRateLimiterMaxDelay),
	}
	return options
}

func newSparkApplicationReconcilerOptions() sparkapplication.Options {
	var sparkApplicationMetrics *metrics.SparkApplicationMetrics
	var sparkExecutorMetrics *metrics.SparkExecutorMetrics
	if enableMetrics {
		sparkApplicationMetrics = metrics.NewSparkApplicationMetrics(metricsPrefix, metricsLabels, metricsJobStartLatencyBuckets)
		sparkApplicationMetrics.Register()
		sparkExecutorMetrics = metrics.NewSparkExecutorMetrics(metricsPrefix, metricsLabels)
		sparkExecutorMetrics.Register()
	}
	options := sparkapplication.Options{
		Namespaces:                   namespaces,
		EnableUIService:              enableUIService,
		IngressClassName:             ingressClassName,
		IngressURLFormat:             ingressURLFormat,
		DefaultBatchScheduler:        defaultBatchScheduler,
		DriverPodCreationGracePeriod: driverPodCreationGracePeriod,
		SparkApplicationMetrics:      sparkApplicationMetrics,
		SparkExecutorMetrics:         sparkExecutorMetrics,
		MaxTrackedExecutorPerApp:     maxTrackedExecutorPerApp,
	}
	if enableBatchScheduler {
		options.KubeSchedulerNames = kubeSchedulerNames
	}
	return options
}

func newScheduledSparkApplicationReconcilerOptions() scheduledsparkapplication.Options {
	options := scheduledsparkapplication.Options{
		Namespaces: namespaces,
	}
	return options
}
