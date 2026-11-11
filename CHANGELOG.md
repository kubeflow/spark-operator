# Changelog

## [v2.6.0](https://github.com/kubeflow/spark-operator/tree/v2.6.0) (2026-11-10)

### Breaking Changes

- feat!: Add webhook namespace-selector for multi-tenant deployments ([#3217](https://github.com/kubeflow/spark-operator/pull/3217) by [@RobuRishabh](https://github.com/RobuRishabh))
  - `spark.jobNamespaceSelector` no longer accepts a string; it must be a label selector object. The webhook now uses the same namespace matcher as the controller.

### Deprecations

- feat(api): deprecate client deploy mode for SparkApplication ([#3270](https://github.com/kubeflow/spark-operator/pull/3270) by [@vikas-saxena02](https://github.com/vikas-saxena02))
  - `spec.mode: client` still works but is deprecated; the webhook returns a warning and the controller logs one on submission.

### Features

- feat: upgrade Spark runtime to 4.2.1 ([#3245](https://github.com/kubeflow/spark-operator/pull/3245) by [@thisis-Shitanshu](https://github.com/thisis-Shitanshu))
- feat(sparkconnect): add a `Dependencies` field to `SparkConnectSpec` ([#3120](https://github.com/kubeflow/spark-operator/pull/3120) by [@Mr-Neutr0n](https://github.com/Mr-Neutr0n))
- feat: Expose Spark Operator control-plane version via public ConfigMap ([#3225](https://github.com/kubeflow/spark-operator/pull/3225) by [@tariq-hasan](https://github.com/tariq-hasan))
- feat: support physical CPU request and limit in SparkConnect CRD ([#3096](https://github.com/kubeflow/spark-operator/pull/3096) by [@DUKartik](https://github.com/DUKartik))
- feat(controller): allow Spark 4.2 NetworkPolicy creation ([#3240](https://github.com/kubeflow/spark-operator/pull/3240) by [@thisis-Shitanshu](https://github.com/thisis-Shitanshu))
- feat: add support for managedBy field on SparkApplication ([#3221](https://github.com/kubeflow/spark-operator/pull/3221) by [@amirialy](https://github.com/amirialy))
- feat(sparkconnect): support GPU resources in SparkConnect CRD ([#3169](https://github.com/kubeflow/spark-operator/pull/3169) by [@Rajadi16](https://github.com/Rajadi16))
- Make webhook leader election timings configurable ([#3065](https://github.com/kubeflow/spark-operator/pull/3065) by [@AxcelBlaze003](https://github.com/AxcelBlaze003))
- feat(sparkconnect): add service account fallback for Spark Connect server pod ([#3109](https://github.com/kubeflow/spark-operator/pull/3109) by [@vikas-saxena02](https://github.com/vikas-saxena02))
- feat(controller): add default service account fallback for SparkApplication driver ([#3092](https://github.com/kubeflow/spark-operator/pull/3092) by [@vikas-saxena02](https://github.com/vikas-saxena02))
- feat: garbage-collect terminated SparkApplications via operator default TTL ([#3042](https://github.com/kubeflow/spark-operator/pull/3042) by [@dineshkumar181094](https://github.com/dineshkumar181094))
- feat: add per-application driver PodDisruptionBudget (#2949) ([#2954](https://github.com/kubeflow/spark-operator/pull/2954) by [@vjanelle](https://github.com/vjanelle))
- Add deploymentAnnotations to controller and webhook deployments ([#3001](https://github.com/kubeflow/spark-operator/pull/3001) by [@cHiv0rz](https://github.com/cHiv0rz))
- feat(helm): add RestSubmitter feature gate with controller integration ([#2999](https://github.com/kubeflow/spark-operator/pull/2999) by [@venkomirisetti](https://github.com/venkomirisetti))
- feat: Add Spark Connect server probes ([#2995](https://github.com/kubeflow/spark-operator/pull/2995) by [@Shekharrajak](https://github.com/Shekharrajak))
- feat(chart): make Helm hook Job resources configurable ([#3015](https://github.com/kubeflow/spark-operator/pull/3015) by [@maxpschonder](https://github.com/maxpschonder))
- REST-based spark submitter service as pluggable submission strategy ([#2976](https://github.com/kubeflow/spark-operator/pull/2976) by [@venkomirisetti](https://github.com/venkomirisetti))
- feat: add TLS version and cipher suite configuration flags ([#2986](https://github.com/kubeflow/spark-operator/pull/2986) by [@ugiordan](https://github.com/ugiordan))
- feat(controller): log when SparkApplication is marked Failing after driver-pod grace period ([#2947](https://github.com/kubeflow/spark-operator/pull/2947) by [@dineshkumar181094](https://github.com/dineshkumar181094))
- Add labels and annotations support for upgrade hook ([#2940](https://github.com/kubeflow/spark-operator/pull/2940) by [@MonkeyCanCode](https://github.com/MonkeyCanCode))
- feat(controller): emit Kubernetes event when driver pod missing after grace period ([#2971](https://github.com/kubeflow/spark-operator/pull/2971) by [@RobuRishabh](https://github.com/RobuRishabh))

### Bug Fixes

- fix(controller): resolve {{SPARK_VERSION}} in images with the application's Spark version ([#3292](https://github.com/kubeflow/spark-operator/pull/3292) by [@vara-bonthu](https://github.com/vara-bonthu))
- fix(kustomize): remove stale OPERATOR_VERSION from controller deployment ([#3291](https://github.com/kubeflow/spark-operator/pull/3291) by [@tariq-hasan](https://github.com/tariq-hasan))
- fix(sparkconnect): use server.cores as server pod CPU request ([#3252](https://github.com/kubeflow/spark-operator/pull/3252) by [@vikas-saxena02](https://github.com/vikas-saxena02))
- fix(scheduledsparkapplication): don't panic on negative history limits ([#3229](https://github.com/kubeflow/spark-operator/pull/3229) by [@rautaditya2606](https://github.com/rautaditya2606))
- fix(controller): mark missing executors completed when the driver succeeds ([#3244](https://github.com/kubeflow/spark-operator/pull/3244) by [@rishabhsai](https://github.com/rishabhsai))
- fix(controller): authorize events.k8s.io for SparkApplication events ([#3227](https://github.com/kubeflow/spark-operator/pull/3227) by [@RobuRishabh](https://github.com/RobuRishabh))
- fix(scheduledsparkapplication): recover from FailedValidation state when spec is corrected ([#3175](https://github.com/kubeflow/spark-operator/pull/3175) by [@rautaditya2606](https://github.com/rautaditya2606))
- fix: name the actual flag in TLS setup errors ([#3204](https://github.com/kubeflow/spark-operator/pull/3204) by [@pratik-naik003](https://github.com/pratik-naik003))
- fix: default the retry interval when the webhook has not set it ([#3209](https://github.com/kubeflow/spark-operator/pull/3209) by [@pratik-naik003](https://github.com/pratik-naik003))
- fix(sparkconnect): update GPU specs for per-argument container args ([#3212](https://github.com/kubeflow/spark-operator/pull/3212) by [@vikas-saxena02](https://github.com/vikas-saxena02))
- fix(controller): always clean up pod template files ([#3178](https://github.com/kubeflow/spark-operator/pull/3178) by [@Aakcht](https://github.com/Aakcht))
- fix(sparkconnect): pass server options as individual container arguments ([#3188](https://github.com/kubeflow/spark-operator/pull/3188) by [@vikas-saxena02](https://github.com/vikas-saxena02))
- fix: stop adding SparkApplication owner reference to executor pods ([#3152](https://github.com/kubeflow/spark-operator/pull/3152) by [@KR-Ravindra](https://github.com/KR-Ravindra))
- fix(webhook): validate ConfigMap mount-path uniqueness ([#3140](https://github.com/kubeflow/spark-operator/pull/3140) by [@pujitha24](https://github.com/pujitha24))
- fix(webhook): validate ConfigMap references in SparkApplication specs ([#3117](https://github.com/kubeflow/spark-operator/pull/3117) by [@rjgoyln](https://github.com/rjgoyln))
- fix(webhook): generate valid volume name for Prometheus ConfigMap ([#3111](https://github.com/kubeflow/spark-operator/pull/3111) by [@rjgoyln](https://github.com/rjgoyln))
- fix(sparkconnect): resolve images from selected template containers ([#3103](https://github.com/kubeflow/spark-operator/pull/3103) by [@immanuwell](https://github.com/immanuwell))
- fix(webhook): generate valid volume names for ConfigMaps ([#3099](https://github.com/kubeflow/spark-operator/pull/3099) by [@immanuwell](https://github.com/immanuwell))
- fix: preserve shell-sensitive Spark Connect configuration values ([#3051](https://github.com/kubeflow/spark-operator/pull/3051) by [@jayanth86](https://github.com/jayanth86))
- fix: harden controller-runtime cache against OOMKill vulnerabilities ([#3032](https://github.com/kubeflow/spark-operator/pull/3032) by [@RobuRishabh](https://github.com/RobuRishabh))
- fix: always set NextProtos regardless of enableHTTP2 flag ([#2990](https://github.com/kubeflow/spark-operator/pull/2990) by [@ugiordan](https://github.com/ugiordan))
- fix(kustomize): Add webhook subPath and expand Helm/Kustomize drift checks ([#2982](https://github.com/kubeflow/spark-operator/pull/2982) by [@RobuRishabh](https://github.com/RobuRishabh))
- Use driverIngressOptions annotations and TLS instead of sparkUIOptions ([#2973](https://github.com/kubeflow/spark-operator/pull/2973) by [@shruthis4](https://github.com/shruthis4))
- fix: handle uppercase units in ConvertJavaMemoryStringToK8sMemoryString ([#2937](https://github.com/kubeflow/spark-operator/pull/2937) by [@tariq-hasan](https://github.com/tariq-hasan))

### Refactor

- chore: remove unused pkg/util/interrupt.go ([#3162](https://github.com/kubeflow/spark-operator/pull/3162) by [@pujitha24](https://github.com/pujitha24))
- refactor: centralize Spark container selection ([#3115](https://github.com/kubeflow/spark-operator/pull/3115) by [@rohith1X8](https://github.com/rohith1X8))
- fix: remove unused Provider.TLSConfig() method ([#3062](https://github.com/kubeflow/spark-operator/pull/3062) by [@ugiordan](https://github.com/ugiordan))
- Scope codegen paths to workload-operator packages ([#3021](https://github.com/kubeflow/spark-operator/pull/3021) by [@RobuRishabh](https://github.com/RobuRishabh))

### Tests

- test(scheduler): add unit tests for the batch scheduler registry ([#3232](https://github.com/kubeflow/spark-operator/pull/3232) by [@vikas-saxena02](https://github.com/vikas-saxena02))
- test(e2e): run SparkConnect query on the Connect server, not a local … ([#3239](https://github.com/kubeflow/spark-operator/pull/3239) by [@vikas-saxena02](https://github.com/vikas-saxena02))
- test: add unit tests for LogConstructor ([#3181](https://github.com/kubeflow/spark-operator/pull/3181) by [@magic-peach](https://github.com/magic-peach))
- test: add unit tests for SumResourceList ([#3200](https://github.com/kubeflow/spark-operator/pull/3200) by [@pratik-naik003](https://github.com/pratik-naik003))
- test(util): add unit tests for InitializeIngressCapabilities ([#3156](https://github.com/kubeflow/spark-operator/pull/3156) by [@magic-peach](https://github.com/magic-peach))
- test(util): add unit tests for CreateValidMetricNameLabel ([#3159](https://github.com/kubeflow/spark-operator/pull/3159) by [@magic-peach](https://github.com/magic-peach))
- (Chore): Move e2e tests into own submodule for dependency isolation ([#3071](https://github.com/kubeflow/spark-operator/pull/3071) by [@vjanelle](https://github.com/vjanelle))
- test(e2e): add Prometheus metrics and Spark UI e2e tests ([#3047](https://github.com/kubeflow/spark-operator/pull/3047) by [@sahana-sreeram](https://github.com/sahana-sreeram))
- test(e2e): add SparkConnect query e2e test ([#3031](https://github.com/kubeflow/spark-operator/pull/3031) by [@sahana-sreeram](https://github.com/sahana-sreeram))
- test(e2e): add ScheduledSparkApplication workload e2e test ([#3029](https://github.com/kubeflow/spark-operator/pull/3029) by [@sahana-sreeram](https://github.com/sahana-sreeram))
- Scope make e2e-test to a dedicated Kind cluster ([#3004](https://github.com/kubeflow/spark-operator/pull/3004) by [@nabuskey](https://github.com/nabuskey))

### Documentation

- docs: show how to set runtimeClassName with pod templates ([#3248](https://github.com/kubeflow/spark-operator/pull/3248) by [@highlyavailable](https://github.com/highlyavailable))
- Add comment policy to AGENTS.md ([#3247](https://github.com/kubeflow/spark-operator/pull/3247) by [@nabuskey](https://github.com/nabuskey))
- docs: add 2026 roadmap ([#3179](https://github.com/kubeflow/spark-operator/pull/3179) by [@tariq-hasan](https://github.com/tariq-hasan))
- docs: add AGENTS.md and CLAUDE.md for AI coding agents ([#3192](https://github.com/kubeflow/spark-operator/pull/3192) by [@Abhayanthk](https://github.com/Abhayanthk))
- docs: add REST submitter integration guide ([#3216](https://github.com/kubeflow/spark-operator/pull/3216) by [@venkomirisetti](https://github.com/venkomirisetti))
- docs: clarify inherited dependency security boundary ([#3214](https://github.com/kubeflow/spark-operator/pull/3214) by [@thisis-Shitanshu](https://github.com/thisis-Shitanshu))
- docs: add SparkConnect to the website API reference ([#3138](https://github.com/kubeflow/spark-operator/pull/3138) by [@sxivansx](https://github.com/sxivansx))
- docs: add SparkConnect API reference to api-docs.md ([#3126](https://github.com/kubeflow/spark-operator/pull/3126) by [@Fumer057](https://github.com/Fumer057))
- docs: replace Enterprise Gateway notebooks guide with Kubeflow SDK ([#3141](https://github.com/kubeflow/spark-operator/pull/3141) by [@vikas-saxena02](https://github.com/vikas-saxena02))
- docs: add Spark Connect user guide ([#3091](https://github.com/kubeflow/spark-operator/pull/3091) by [@reckless-sherixx](https://github.com/reckless-sherixx))
- docs: document webhook failure when a service mesh sidecar is injected ([#3100](https://github.com/kubeflow/spark-operator/pull/3100) by [@vikas-saxena02](https://github.com/vikas-saxena02))
- Chore(Remove go card from readme) - #3058 ([#3060](https://github.com/kubeflow/spark-operator/pull/3060) by [@Tankdoggo](https://github.com/Tankdoggo))
- adding my company RAICS.AI to adopters list ([#3056](https://github.com/kubeflow/spark-operator/pull/3056) by [@vikas-saxena02](https://github.com/vikas-saxena02))
- docs: point documentation links to spark.kubeflow.org ([#3020](https://github.com/kubeflow/spark-operator/pull/3020) by [@vara-bonthu](https://github.com/vara-bonthu))
- docs: Add dedicated Spark Operator documentation website ([#2988](https://github.com/kubeflow/spark-operator/pull/2988) by [@vara-bonthu](https://github.com/vara-bonthu))

### CI and Release

- feat(ci): automate the release process ([#3226](https://github.com/kubeflow/spark-operator/pull/3226) by [@tariq-hasan](https://github.com/tariq-hasan))
- chore: fix CI failures on master (boilerplate headers, ubuntu-26.04 runner) ([#3271](https://github.com/kubeflow/spark-operator/pull/3271) by [@vikas-saxena02](https://github.com/vikas-saxena02))
- feat(ci): add required boilerplate (license header) check ([#3213](https://github.com/kubeflow/spark-operator/pull/3213) by [@reckless-sherixx](https://github.com/reckless-sherixx))
- chore(ci): migrate vulnerability scanning to OSV-Scanner ([#3110](https://github.com/kubeflow/spark-operator/pull/3110) by [@vikas-saxena02](https://github.com/vikas-saxena02))
- feat(ci): publish the operator binary extracted from the released image ([#3098](https://github.com/kubeflow/spark-operator/pull/3098) by [@vikas-saxena02](https://github.com/vikas-saxena02))
- feat(ci): Publish Spark Operator binaries with GitHub releases ([#3078](https://github.com/kubeflow/spark-operator/pull/3078) by [@vikas-saxena02](https://github.com/vikas-saxena02))
- ci: add gh-workflow-approve workflow for /ok-to-test on fork PRs ([#3066](https://github.com/kubeflow/spark-operator/pull/3066) by [@RobuRishabh](https://github.com/RobuRishabh))
- Update setup-helm github action versions ([#3030](https://github.com/kubeflow/spark-operator/pull/3030) by [@nabuskey](https://github.com/nabuskey))
- ci: add shfmt and shellcheck checks for shell scripts ([#2993](https://github.com/kubeflow/spark-operator/pull/2993) by [@tariq-hasan](https://github.com/tariq-hasan))
- Add dependabot config for website ([#3007](https://github.com/kubeflow/spark-operator/pull/3007) by [@nabuskey](https://github.com/nabuskey))
- tighten GH workflows permissions ([#2981](https://github.com/kubeflow/spark-operator/pull/2981) by [@nabuskey](https://github.com/nabuskey))

### Dependencies

- chore(deps): bump certifi from 2026.5.20 to 2026.10.19 in /docs/website ([#3255](https://github.com/kubeflow/spark-operator/pull/3255) by [@dependabot[bot]](https://github.com/apps/dependabot))
- chore(deps): bump tornado from 6.5.8 to 6.5.9 in /docs/website ([#3219](https://github.com/kubeflow/spark-operator/pull/3219) by [@dependabot[bot]](https://github.com/apps/dependabot))
- chore(deps): bump urllib3 from 2.7.0 to 2.8.0 in /docs/website ([#3220](https://github.com/kubeflow/spark-operator/pull/3220) by [@dependabot[bot]](https://github.com/apps/dependabot))
- chore(deps): bump tornado from 6.5.7 to 6.5.8 in /docs/website ([#3135](https://github.com/kubeflow/spark-operator/pull/3135) by [@dependabot[bot]](https://github.com/apps/dependabot))
- chore(deps): bump google.golang.org/grpc from 1.82.1 to 1.83.1 in /test/e2e ([#3136](https://github.com/kubeflow/spark-operator/pull/3136) by [@dependabot[bot]](https://github.com/apps/dependabot))
- chore(deps): bump github.com/onsi/ginkgo/v2 from 2.27.2 to 2.32.1 in /test/e2e ([#3095](https://github.com/kubeflow/spark-operator/pull/3095) by [@dependabot[bot]](https://github.com/apps/dependabot))
- chore(deps): bump actions/checkout from 6.0.2 to 7.0.1 ([#3086](https://github.com/kubeflow/spark-operator/pull/3086) by [@dependabot[bot]](https://github.com/apps/dependabot))
- chore(deps): bump github.com/prometheus/common from 0.67.5 to 0.70.1 in /test/e2e ([#3083](https://github.com/kubeflow/spark-operator/pull/3083) by [@dependabot[bot]](https://github.com/apps/dependabot))
- chore(deps): bump github.com/onsi/gomega from 1.38.2 to 1.42.1 in /test/e2e ([#3085](https://github.com/kubeflow/spark-operator/pull/3085) by [@dependabot[bot]](https://github.com/apps/dependabot))
- chore(deps): bump oras.land/oras-go/v2 from 2.6.1 to 2.6.2 in /test/e2e ([#3088](https://github.com/kubeflow/spark-operator/pull/3088) by [@dependabot[bot]](https://github.com/apps/dependabot))
- chore(deps): bump github.com/containerd/containerd from 1.7.30 to 1.7.33 in /test/e2e ([#3087](https://github.com/kubeflow/spark-operator/pull/3087) by [@dependabot[bot]](https://github.com/apps/dependabot))
- chore(deps): bump google.golang.org/grpc from 1.79.3 to 1.82.1 ([#3054](https://github.com/kubeflow/spark-operator/pull/3054) by [@dependabot[bot]](https://github.com/apps/dependabot))
- chore(deps): bump golang.org/x/crypto from 0.46.0 to 0.52.0 ([#3039](https://github.com/kubeflow/spark-operator/pull/3039) by [@dependabot[bot]](https://github.com/apps/dependabot))
- chore(deps): bump docker/login-action from 3.7.0 to 4.2.0 ([#3008](https://github.com/kubeflow/spark-operator/pull/3008) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump Go to 1.25.11 ([#2998](https://github.com/kubeflow/spark-operator/pull/2998) by [@nabuskey](https://github.com/nabuskey))
- Bump k8s to v1.35.4 and controller-runtime to v0.23.3 ([#2946](https://github.com/kubeflow/spark-operator/pull/2946) by [@everpeace](https://github.com/everpeace))

### Misc

- Adding Vikas and Vincent to reviewers ([#3246](https://github.com/kubeflow/spark-operator/pull/3246) by [@vikas-saxena02](https://github.com/vikas-saxena02))
- chore: Adding Tariq Hasan as new reviewer to the repo ([#3064](https://github.com/kubeflow/spark-operator/pull/3064) by [@vara-bonthu](https://github.com/vara-bonthu))
- (chore) use `command` in entrypoint.sh ([#2975](https://github.com/kubeflow/spark-operator/pull/2975) by [@alimaredia](https://github.com/alimaredia))
- Replace `tini` in entrypoint.sh with `catatonit` ([#2968](https://github.com/kubeflow/spark-operator/pull/2968) by [@alimaredia](https://github.com/alimaredia))
- promote roburishabh to reviewers ([#2969](https://github.com/kubeflow/spark-operator/pull/2969) by [@RobuRishabh](https://github.com/RobuRishabh))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/v2.5.2...v2.6.0)

## [v2.5.2](https://github.com/kubeflow/spark-operator/tree/v2.5.2) (2026-07-30)

### Features

- feat(webhook): add sparkConf key validation ([#3043](https://github.com/kubeflow/spark-operator/pull/3043) by [@sahana-sreeram](https://github.com/sahana-sreeram))

### Bug Fixes

- fix(chart): grant webhook leader election event permissions ([#3027](https://github.com/kubeflow/spark-operator/pull/3027) by [@immanuwell](https://github.com/immanuwell))

### Dependencies

- bump base image to spark 4.0.4 ([#3061](https://github.com/kubeflow/spark-operator/pull/3061) by [@nabuskey](https://github.com/nabuskey))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/v2.5.1...v2.5.2)

## [v2.5.1](https://github.com/kubeflow/spark-operator/tree/v2.5.1) (2026-06-14)

### Features

- Add spark_application_submit_latency_seconds metric ([#2942](https://github.com/kubeflow/spark-operator/pull/2942) by [@venkomirisetti](https://github.com/venkomirisetti))
- feat(kustomize): Add core deployment infrastructure and E2E test suite ([#2919](https://github.com/kubeflow/spark-operator/pull/2919) by [@RobuRishabh](https://github.com/RobuRishabh))
- feat(prometheus): Add Prometheus PodMonitor Kustomize resource ([#2922](https://github.com/kubeflow/spark-operator/pull/2922) by [@Vedant-Deshpande](https://github.com/Vedant-Deshpande))
- feat(pdb): Add PodDisruptionBudget Kustomize resource for controller-manager ([#2918](https://github.com/kubeflow/spark-operator/pull/2918) by [@Vedant-Deshpande](https://github.com/Vedant-Deshpande))
- feat(controller): add --kube-api-qps and --kube-api-burst flags ([#2865](https://github.com/kubeflow/spark-operator/pull/2865) by [@houyuting](https://github.com/houyuting))

### Bug Fixes

- fix(controller): avoid invalidating apps on TTL updates ([#2956](https://github.com/kubeflow/spark-operator/pull/2956) by [@dineshkumar181094](https://github.com/dineshkumar181094))
- Fix(SparkConnect): Add BlockManager port to match Service definition ([#2948](https://github.com/kubeflow/spark-operator/pull/2948) by [@Vedant-Deshpande](https://github.com/Vedant-Deshpande))
- fix(kustomize): Add missing --metrics-job-submit-latency-buckets arg to controller deployment ([#2950](https://github.com/kubeflow/spark-operator/pull/2950) by [@RobuRishabh](https://github.com/RobuRishabh))
- fix(metrics): use sanitized labels for executor/application metric vecs ([#2931](https://github.com/kubeflow/spark-operator/pull/2931) by [@dineshkumar181094](https://github.com/dineshkumar181094))
- fix(kustomize,helm): Align Helm and Kustomize RBAC, args, and sideEffects via code tracing ([#2936](https://github.com/kubeflow/spark-operator/pull/2936) by [@RobuRishabh](https://github.com/RobuRishabh))
- fix(kustomize): Add namespaceSelector to webhook configurations via kustomize patches ([#2941](https://github.com/kubeflow/spark-operator/pull/2941) by [@RobuRishabh](https://github.com/RobuRishabh))
- fix(webhook): Fix path for mutating ScheduledSparkApplication, correct sideEffects, remove update verb in manifests ([#2915](https://github.com/kubeflow/spark-operator/pull/2915) by [@Vedant-Deshpande](https://github.com/Vedant-Deshpande))
- fix: EmptyDir medium field is not forwarded to Spark configuration ([#2925](https://github.com/kubeflow/spark-operator/pull/2925) by [@MathildeLeroi](https://github.com/MathildeLeroi))
- fix(rbac): Fix Helm RBAC bugs, add trailing newline, and add unit test ([#2917](https://github.com/kubeflow/spark-operator/pull/2917) by [@RobuRishabh](https://github.com/RobuRishabh))
- fix(rbac): Tighten controller RBAC markers to enforce least privilege ([#2914](https://github.com/kubeflow/spark-operator/pull/2914) by [@RobuRishabh](https://github.com/RobuRishabh))
- fix(rbac): Replace hardcoded namespace: default with namespace: system ([#2898](https://github.com/kubeflow/spark-operator/pull/2898) by [@RobuRishabh](https://github.com/RobuRishabh))
- fix(crd): Add missing SparkConnect CRD resource and patches to kustomization ([#2890](https://github.com/kubeflow/spark-operator/pull/2890) by [@RobuRishabh](https://github.com/RobuRishabh))
- fix: avoid duplicate web UI service/ingress creation during reconciliation ([#2868](https://github.com/kubeflow/spark-operator/pull/2868) by [@YassinNouh21](https://github.com/YassinNouh21))

### Documentation

- docs(readme): Add installation instructions via kustomize manifests ([#2951](https://github.com/kubeflow/spark-operator/pull/2951) by [@alimaredia](https://github.com/alimaredia))

### Dependencies

- chore(deps): bump google.golang.org/grpc from 1.69.4 to 1.79.3 ([#2884](https://github.com/kubeflow/spark-operator/pull/2884) by [@dependabot[bot]](https://github.com/apps/dependabot))
- chore(deps): bump aquasecurity/trivy-action from 0.33.1 to 0.34.0 in /.github/workflows ([#2847](https://github.com/kubeflow/spark-operator/pull/2847) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/checkout from 5.0.0 to 6.0.2 ([#2821](https://github.com/kubeflow/spark-operator/pull/2821) by [@dependabot[bot]](https://github.com/apps/dependabot))
- chore(deps): bump actions/upload-artifact from 6.0.0 to 7.0.0 ([#2861](https://github.com/kubeflow/spark-operator/pull/2861) by [@dependabot[bot]](https://github.com/apps/dependabot))

### Misc

- feat(ci): Implement drift detection as Go test in test/drift/ ([#2933](https://github.com/kubeflow/spark-operator/pull/2933) by [@RobuRishabh](https://github.com/RobuRishabh))
- Promote nabuskey to approver ([#2944](https://github.com/kubeflow/spark-operator/pull/2944) by [@nabuskey](https://github.com/nabuskey))
- Chore/remove standalone kustomize ([#2929](https://github.com/kubeflow/spark-operator/pull/2929) by [@RobuRishabh](https://github.com/RobuRishabh))
- chore(ci): Remove Trivy Action ([#2893](https://github.com/kubeflow/spark-operator/pull/2893) by [@nabuskey](https://github.com/nabuskey))
- chore(ci): Remove trivy - Spark-Operator ([#2894](https://github.com/kubeflow/spark-operator/pull/2894) by [@sameerdattav](https://github.com/sameerdattav))
- pin github actions to hashes ([#2892](https://github.com/kubeflow/spark-operator/pull/2892) by [@nabuskey](https://github.com/nabuskey))
- Update integration test k8s matrix ([#2874](https://github.com/kubeflow/spark-operator/pull/2874) by [@nabuskey](https://github.com/nabuskey))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/v2.5.0...v2.5.1)

## [v2.5.0](https://github.com/kubeflow/spark-operator/tree/v2.5.0) (2026-03-19)

### Features

- feat(webhook): Add webhook validation for SparkConnect CRD ([#2862](https://github.com/kubeflow/spark-operator/pull/2862) by [@tariq-hasan](https://github.com/tariq-hasan))
- feat: add Python API generation for Spark Operator CRDs ([#2828](https://github.com/kubeflow/spark-operator/pull/2828) by [@tariq-hasan](https://github.com/tariq-hasan))
- feat: add configurable timestamp precision for ScheduledSparkApplication names ([#2827](https://github.com/kubeflow/spark-operator/pull/2827) by [@puwun](https://github.com/puwun))
- Set appProtocol on SparkConnect server service ports ([#2823](https://github.com/kubeflow/spark-operator/pull/2823) by [@wkd-woo](https://github.com/wkd-woo))
- feat: Expose metrics-labels flag via Spark Operator Helm chart ([#2817](https://github.com/kubeflow/spark-operator/pull/2817) by [@IgorSteps](https://github.com/IgorSteps))
- feat: watch namespaces based on labels ([#2808](https://github.com/kubeflow/spark-operator/pull/2808) by [@tariq-hasan](https://github.com/tariq-hasan))
- Update to spark 4.0.1 ([#2804](https://github.com/kubeflow/spark-operator/pull/2804) by [@nabuskey](https://github.com/nabuskey))
- feature: Add alpha feature gate LoadSparkDefaults ([#2798](https://github.com/kubeflow/spark-operator/pull/2798) by [@ChenYi015](https://github.com/ChenYi015))
- Server-side apply CRDs by forcing conflicts ([#2800](https://github.com/kubeflow/spark-operator/pull/2800) by [@ChenYi015](https://github.com/ChenYi015))
- feat: skip reconcile for webhook-patched executor fields ([#2786](https://github.com/kubeflow/spark-operator/pull/2786) by [@Kevinz857](https://github.com/Kevinz857))
- feat: introduce feature gate mechanism ([#2794](https://github.com/kubeflow/spark-operator/pull/2794) by [@Kevinz857](https://github.com/Kevinz857))
- Helm: Add toleration, affinity, and nodeselector for upgrade hook ([#2780](https://github.com/kubeflow/spark-operator/pull/2780) by [@MonkeyCanCode](https://github.com/MonkeyCanCode))
- feat(chart): add hostUsers (user namespace) option ([#2721](https://github.com/kubeflow/spark-operator/pull/2721) by [@t3mi](https://github.com/t3mi))

### Bug Fixes

- fix: Add label selector to ConfigMap cache to prevent OOM via informer flooding ([#2878](https://github.com/kubeflow/spark-operator/pull/2878) by [@RobuRishabh](https://github.com/RobuRishabh))
- Fix duplicate webhook patch and add missing ScheduledSparkApplication Patches ([#2820](https://github.com/kubeflow/spark-operator/pull/2820) by [@RobuRishabh](https://github.com/RobuRishabh))
- fix: correct schedule parse error logging in `ScheduledSparkApplication` controller ([#2841](https://github.com/kubeflow/spark-operator/pull/2841) by [@puwun](https://github.com/puwun))
- fix: handle nil Executor.Instances in GetExecutorRequestResource ([#2834](https://github.com/kubeflow/spark-operator/pull/2834) by [@puwun](https://github.com/puwun))
- Fix SparkConnect nil executor template panic ([#2814](https://github.com/kubeflow/spark-operator/pull/2814) by [@sameerdattav](https://github.com/sameerdattav))
- Fix : Propagate batch scheduler initialization errors to trigger retries ([#2783](https://github.com/kubeflow/spark-operator/pull/2783) by [@houyuting](https://github.com/houyuting))
- Reset SparkApplication status when transitioning from SUCCEEDING/FAILING to PENDING_RERUN ([#2773](https://github.com/kubeflow/spark-operator/pull/2773) by [@ChenYi015](https://github.com/ChenYi015))

### Refactor

- refactor: replace glog with controller-runtime structured logging ([#2836](https://github.com/kubeflow/spark-operator/pull/2836) by [@puwun](https://github.com/puwun))
- fix: correct filename typo in metrics package ([#2830](https://github.com/kubeflow/spark-operator/pull/2830) by [@puwun](https://github.com/puwun))
- refactor: reuse version.Info from k8s.io/component-base/version ([#2797](https://github.com/kubeflow/spark-operator/pull/2797) by [@ChenYi015](https://github.com/ChenYi015))
- Refactor webhook logging ([#2799](https://github.com/kubeflow/spark-operator/pull/2799) by [@ChenYi015](https://github.com/ChenYi015))
- refactor: replace util.ContainsString with slices.Contains ([#2796](https://github.com/kubeflow/spark-operator/pull/2796) by [@ChenYi015](https://github.com/ChenYi015))
- Use custom log constructors for controllers ([#2756](https://github.com/kubeflow/spark-operator/pull/2756) by [@ChenYi015](https://github.com/ChenYi015))

### Dependencies

- Bump golang.org/x/crypto from 0.42.0 to 0.45.0 ([#2743](https://github.com/kubeflow/spark-operator/pull/2743) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github/codeql-action from 4.31.2 to 4.31.8 ([#2778](https://github.com/kubeflow/spark-operator/pull/2778) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/upload-artifact from 4 to 6 ([#2777](https://github.com/kubeflow/spark-operator/pull/2777) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/download-artifact from 5 to 7 ([#2776](https://github.com/kubeflow/spark-operator/pull/2776) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump golang from 1.24.1 to 1.24.10 ([#2732](https://github.com/kubeflow/spark-operator/pull/2732) by [@ChenYi015](https://github.com/ChenYi015))

### Misc

- chore(api): bump Python API version to 2.4.0 ([#2853](https://github.com/kubeflow/spark-operator/pull/2853) by [@tariq-hasan](https://github.com/tariq-hasan))
- docs(readme): update Slack channel name in community section ([#2829](https://github.com/kubeflow/spark-operator/pull/2829) by [@ChenYi015](https://github.com/ChenYi015))
- feat: add welcome-new-contributors workflow ([#2824](https://github.com/kubeflow/spark-operator/pull/2824) by [@ChenYi015](https://github.com/ChenYi015))
- Update ENVTEST_K8S_VERSION for e2e and unit tests ([#2751](https://github.com/kubeflow/spark-operator/pull/2751) by [@ChenYi015](https://github.com/ChenYi015))
- Add GitHub action workflow for publishing latest spark-operator and kubectl images ([#2710](https://github.com/kubeflow/spark-operator/pull/2710) by [@ChenYi015](https://github.com/ChenYi015))
- Add changelog for v2.4.0 ([#2731](https://github.com/kubeflow/spark-operator/pull/2731) by [@ChenYi015](https://github.com/ChenYi015))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/v2.4.0...v2.5.0)

## [v2.4.0](https://github.com/kubeflow/spark-operator/tree/v2.4.0) (2025-11-14)

### Features

- fix(chart): add revisionHistoryLimit option ([#2625](https://github.com/kubeflow/spark-operator/pull/2625) by [@t3mi](https://github.com/t3mi))
- Suspend/Resume feature on SparkApplication ([#2387](https://github.com/kubeflow/spark-operator/pull/2387) by [@everpeace](https://github.com/everpeace))
- Set ControllerReference on driver pod and non-Controller OwnerReference on executor pod at submission time ([#2670](https://github.com/kubeflow/spark-operator/pull/2670) by [@everpeace](https://github.com/everpeace))
- added SparkApp name validator to accept valid DNS-1035 format ([#2711](https://github.com/kubeflow/spark-operator/pull/2711) by [@aryankumar04](https://github.com/aryankumar04))
- No propagate Kueue labels to driver and executor pods ([#2714](https://github.com/kubeflow/spark-operator/pull/2714) by [@everpeace](https://github.com/everpeace))
- feat: add support for customizing connect service ([#2709](https://github.com/kubeflow/spark-operator/pull/2709) by [@ChenYi015](https://github.com/ChenYi015))

### Bug Fixes

- Grant get/patch permissions for SparkConnect CRD to hook ClusterRole ([#2605](https://github.com/kubeflow/spark-operator/pull/2605) by [@ChenYi015](https://github.com/ChenYi015))
- Grant create events permissions to Controller ([#2616](https://github.com/kubeflow/spark-operator/pull/2616) by [@Rockawear](https://github.com/Rockawear))
- fix(connect): propagate executor memory setting to spark options ([#2656](https://github.com/kubeflow/spark-operator/pull/2656) by [@mrjoe7](https://github.com/mrjoe7))
- [fix] add miss cli params. Add leader election params in chart ([#2657](https://github.com/kubeflow/spark-operator/pull/2657) by [@aagumin](https://github.com/aagumin))
- fix: webhook encoder configuration ([#2664](https://github.com/kubeflow/spark-operator/pull/2664) by [@pvbouwel](https://github.com/pvbouwel))
- bugfix: hook.upgradeCrd use cases ([#2663](https://github.com/kubeflow/spark-operator/pull/2663) by [@pvbouwel](https://github.com/pvbouwel))
- Correct entrypoint.sh for Openshift ([#2645](https://github.com/kubeflow/spark-operator/pull/2645) by [@Reamer](https://github.com/Reamer))
- Fix driver host configuration to handle IPv6 addresses ([#2703](https://github.com/kubeflow/spark-operator/pull/2703) by [@tiagotxm](https://github.com/tiagotxm))
- Add permissions for sparkconnects/finalizers ([#2727](https://github.com/kubeflow/spark-operator/pull/2727) by [@ChenYi015](https://github.com/ChenYi015))
- Remove redundant name and namespace context in logs ([#2723](https://github.com/kubeflow/spark-operator/pull/2723) by [@ChenYi015](https://github.com/ChenYi015))
- Logging info when resources associated with SparkApplication still exist ([#2725](https://github.com/kubeflow/spark-operator/pull/2725) by [@ChenYi015](https://github.com/ChenYi015))

### Unit Tests

- Add kube-scheduler podgroup unit tests ([#2689](https://github.com/kubeflow/spark-operator/pull/2689) by [@shadowinlife](https://github.com/shadowinlife))
- Generate HTML coverage report after running unit tests ([#2691](https://github.com/kubeflow/spark-operator/pull/2691) by [@shadowinlife](https://github.com/shadowinlife))
- Avoid 409 (Conflict) error at e2e tests with retry. ([#2695](https://github.com/kubeflow/spark-operator/pull/2695) by [@shadowinlife](https://github.com/shadowinlife))
- Add unit test for sparkapplication_validator ([#2692](https://github.com/kubeflow/spark-operator/pull/2692) by [@shadowinlife](https://github.com/shadowinlife))
- Add unit test for scheduledsparkapplication_validator ([#2694](https://github.com/kubeflow/spark-operator/pull/2694) by [@shadowinlife](https://github.com/shadowinlife))
- Refactor the unit tests for web_ui.go ([#2696](https://github.com/kubeflow/spark-operator/pull/2696) by [@shadowinlife](https://github.com/shadowinlife))

### Refactor

- unify SparkApplication defaulting logic ([#2671](https://github.com/kubeflow/spark-operator/pull/2671) by [@zhzhuang-zju](https://github.com/zhzhuang-zju))
- Replace strconv.Atoi with strconv.ParseInt ([#2699](https://github.com/kubeflow/spark-operator/pull/2699) by [@ChenYi015](https://github.com/ChenYi015))
- refactor: use ptr.To to replace util.XxxPtr ([#2693](https://github.com/kubeflow/spark-operator/pull/2693) by [@ChenYi015](https://github.com/ChenYi015))

### Dependencies

- Bump actions/checkout from 4 to 5 ([#2623](https://github.com/kubeflow/spark-operator/pull/2623) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/download-artifact from 4 to 5 ([#2624](https://github.com/kubeflow/spark-operator/pull/2624) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump helm.sh/helm/v3 from 3.18.4 to 3.18.5 ([#2627](https://github.com/kubeflow/spark-operator/pull/2627) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/go-viper/mapstructure/v2 from 2.3.0 to 2.4.0 ([#2635](https://github.com/kubeflow/spark-operator/pull/2635) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/setup-go from 5 to 6 ([#2651](https://github.com/kubeflow/spark-operator/pull/2651) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/stale from 9 to 10 ([#2649](https://github.com/kubeflow/spark-operator/pull/2649) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump aquasecurity/trivy-action from 0.32.0 to 0.33.1 ([#2650](https://github.com/kubeflow/spark-operator/pull/2650) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/golang/glog from 1.2.4 to 1.2.5 ([#2620](https://github.com/kubeflow/spark-operator/pull/2620) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/onsi/ginkgo/v2 from 2.23.3 to 2.26.0 ([#2660](https://github.com/kubeflow/spark-operator/pull/2660) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump sigs.k8s.io/scheduler-plugins from 0.31.8 to 0.32.7 ([#2675](https://github.com/kubeflow/spark-operator/pull/2675) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github/codeql-action from 3 to 4 ([#2672](https://github.com/kubeflow/spark-operator/pull/2672) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/stretchr/testify from 1.10.0 to 1.11.1 ([#2673](https://github.com/kubeflow/spark-operator/pull/2673) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump golang.org/x/time from 0.9.0 to 0.14.0 ([#2686](https://github.com/kubeflow/spark-operator/pull/2686) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/prometheus/client_golang from 1.22.0 to 1.23.2 ([#2682](https://github.com/kubeflow/spark-operator/pull/2682) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/spf13/viper from 1.20.1 to 1.21.0 ([#2683](https://github.com/kubeflow/spark-operator/pull/2683) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github/codeql-action from 3.29.2 to 4.31.0 ([#2684](https://github.com/kubeflow/spark-operator/pull/2684) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/containerd/containerd from 1.7.27 to 1.7.29 ([#2712](https://github.com/kubeflow/spark-operator/pull/2712) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump sigs.k8s.io/yaml from 1.5.0 to 1.6.0 ([#2704](https://github.com/kubeflow/spark-operator/pull/2704) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/spf13/cobra from 1.9.1 to 1.10.1 ([#2705](https://github.com/kubeflow/spark-operator/pull/2705) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github/codeql-action from 4.31.0 to 4.31.2 ([#2707](https://github.com/kubeflow/spark-operator/pull/2707) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/upload-artifact from 4 to 5 ([#2681](https://github.com/kubeflow/spark-operator/pull/2681) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/download-artifact from 5 to 6 ([#2680](https://github.com/kubeflow/spark-operator/pull/2680) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/checkout from 4.2.2 to 5.0.0 ([#2685](https://github.com/kubeflow/spark-operator/pull/2685) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump ossf/scorecard-action from 2.4.2 to 2.4.3 ([#2687](https://github.com/kubeflow/spark-operator/pull/2687) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/onsi/ginkgo/v2 from 2.26.0 to 2.27.2 ([#2719](https://github.com/kubeflow/spark-operator/pull/2719) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump golang.org/x/mod from 0.27.0 to 0.29.0 ([#2720](https://github.com/kubeflow/spark-operator/pull/2720) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump helm/chart-testing-action from 2.7.0 to 2.8.0 ([#2722](https://github.com/kubeflow/spark-operator/pull/2722) by [@dependabot[bot]](https://github.com/apps/dependabot))

### Misc

- Add changelog for v2.3.0 ([#2614](https://github.com/kubeflow/spark-operator/pull/2614) by [@ChenYi015](https://github.com/ChenYi015))
- Update README ([#2634](https://github.com/kubeflow/spark-operator/pull/2634) by [@ChenYi015](https://github.com/ChenYi015))
- Add Spark Operator Roadmap ([#2622](https://github.com/kubeflow/spark-operator/pull/2622) by [@ChenYi015](https://github.com/ChenYi015))
- CI: Add Scorecard workflow ([#2654](https://github.com/kubeflow/spark-operator/pull/2654) by [@AdamKorcz](https://github.com/AdamKorcz))
- Update GitHub action workflow permissions ([#2678](https://github.com/kubeflow/spark-operator/pull/2678) by [@ChenYi015](https://github.com/ChenYi015))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/v2.3.0...v2.4.0)

## [v2.3.0](https://github.com/kubeflow/spark-operator/tree/v2.3.0) (2025-07-22)

### Features

- Add support for Spark Connect ([#2569](https://github.com/kubeflow/spark-operator/pull/2569) by [@ChenYi015](https://github.com/ChenYi015))
- upgrade to Spark 4.0.0 ([#2564](https://github.com/kubeflow/spark-operator/pull/2564) by [@nabuskey](https://github.com/nabuskey))
- Make logging encoder configurable ([#2580](https://github.com/kubeflow/spark-operator/pull/2580) by [@ChenYi015](https://github.com/ChenYi015))
- Include pod.Status.Message in recordExecutorEvent ([#2589](https://github.com/kubeflow/spark-operator/pull/2589) by [@matschaffer-roblox](https://github.com/matschaffer-roblox))
- Add print columns for Spark Connect ([#2592](https://github.com/kubeflow/spark-operator/pull/2592) by [@nabuskey](https://github.com/nabuskey)
- chore: update prometheus pattern and labels for structured streaming driver ([#2581](https://github.com/kubeflow/spark-operator/pull/2581) by [@yahwang](https://github.com/yahwang))
- Add Helm hook to upgrade CRDs ([#2371](https://github.com/kubeflow/spark-operator/pull/2371) by [@ChenYi015](https://github.com/ChenYi015))

### Bug Fixes

- Splat recordExecutorEvent args for cleaner event messages ([#2582](https://github.com/kubeflow/spark-operator/pull/2582) by [@matschaffer-roblox](https://github.com/matschaffer-roblox))
- fix: should add executor env when driver env is empty ([#2586](https://github.com/kubeflow/spark-operator/pull/2586) by [@ChenYi015](https://github.com/ChenYi015))
- Add web UI configurations when enabling UI service and ingress ([#2599](https://github.com/kubeflow/spark-operator/pull/2599) by [@ChenYi015](https://github.com/ChenYi015))
- Grant get/patch permissions for SparkConnect CRD to hook ClusterRole ([#2605](https://github.com/kubeflow/spark-operator/pull/2605) by [@ChenYi015](https://github.com/ChenYi015))

### Dependencies

- Bump aquasecurity/trivy-action from 0.31.0 to 0.32.0 ([#2585](https://github.com/kubeflow/spark-operator/pull/2585) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/go-viper/mapstructure/v2 from 2.2.1 to 2.3.0 ([#2572](https://github.com/kubeflow/spark-operator/pull/2572) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump sigs.k8s.io/yaml from 1.4.0 to 1.5.0 ([#2577](https://github.com/kubeflow/spark-operator/pull/2577) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump helm.sh/helm/v3 from 3.17.3 to 3.18.4 ([#2587](https://github.com/kubeflow/spark-operator/pull/2587) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump golang.org/x/mod from 0.25.0 to 0.26.0 ([#2608](https://github.com/kubeflow/spark-operator/pull/2608) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/onsi/gomega from 1.36.1 to 1.37.0 ([#2607](https://github.com/kubeflow/spark-operator/pull/2607) by [@dependabot[bot]](https://github.com/apps/dependabot))

### Misc

- Add SparkConnect e2e test ([#2578](https://github.com/kubeflow/spark-operator/pull/2578) by [@ChenYi015](https://github.com/ChenYi015))
- feat(docs): Guide to report security vulnerabilities ([#2593](https://github.com/kubeflow/spark-operator/pull/2593) by [@andreyvelich](https://github.com/andreyvelich))
- Read Helm version from go.mod file ([#2598](https://github.com/kubeflow/spark-operator/pull/2598) by [@ChenYi015](https://github.com/ChenYi015))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/v2.2.1...v2.3.0)

## [v2.2.1](https://github.com/kubeflow/spark-operator/tree/v2.2.1) (2025-06-27)

### Features

- Customize ingress URL with Spark application ID ([#2554](https://github.com/kubeflow/spark-operator/pull/2554) by [@ChenYi015](https://github.com/ChenYi015))
- Make default ingress tls and annotations congurable in the helm config ([#2513](https://github.com/kubeflow/spark-operator/pull/2513) by [@Tom-Newton](https://github.com/Tom-Newton))
- Use code-generator for clientset, informers, listers ([#2563](https://github.com/kubeflow/spark-operator/pull/2563) by [@jbhalodia-slack](https://github.com/jbhalodia-slack))

### Misc

- add driver ingress unit tests ([#2552](https://github.com/kubeflow/spark-operator/pull/2552) by [@nabuskey](https://github.com/nabuskey))
- Get logger from context ([#2551](https://github.com/kubeflow/spark-operator/pull/2551) by [@ChenYi015](https://github.com/ChenYi015))
- Update golangci lint ([#2560](https://github.com/kubeflow/spark-operator/pull/2560) by [@joshuacuellar1](https://github.com/joshuacuellar1))

### Dependencies

- Bump aquasecurity/trivy-action from 0.30.0 to 0.31.0 ([#2557](https://github.com/kubeflow/spark-operator/pull/2557) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/prometheus/client_golang from 1.21.1 to 1.22.0 ([#2548](https://github.com/kubeflow/spark-operator/pull/2548) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump sigs.k8s.io/scheduler-plugins from 0.30.6 to 0.31.8 ([#2549](https://github.com/kubeflow/spark-operator/pull/2549) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump golang.org/x/mod from 0.24.0 to 0.25.0 ([#2566](https://github.com/kubeflow/spark-operator/pull/2566) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/go-logr/logr from 1.4.2 to 1.4.3 ([#2567](https://github.com/kubeflow/spark-operator/pull/2567) by [@dependabot[bot]](https://github.com/apps/dependabot))

## [v2.2.0](https://github.com/kubeflow/spark-operator/tree/v2.2.0) (2025-05-29)

### Features

- Upgrade to Spark 3.5.5 ([#2490](https://github.com/kubeflow/spark-operator/pull/2490) by [@jacobsalway](https://github.com/jacobsalway))
- Add timeZone to ScheduledSparkApplication ([#2471](https://github.com/kubeflow/spark-operator/pull/2471) by [@jacobsalway](https://github.com/jacobsalway))
- Enable the override of MemoryLimit through webhook ([#2478](https://github.com/kubeflow/spark-operator/pull/2478) by [@danielrsfreitas](https://github.com/danielrsfreitas))
- Add ShuffleTrackingEnabled to DynamicAllocation struct to allow disabling shuffle tracking ([#2511](https://github.com/kubeflow/spark-operator/pull/2511) by [@jbhalodia-slack](https://github.com/jbhalodia-slack))
- Define SparkApplicationSubmitter interface to allow customizing submitting mechanism ([#2500](https://github.com/kubeflow/spark-operator/pull/2500) by [@ChenYi015](https://github.com/ChenYi015))
- Add support for using cert manager to generate webhook certificates ([#2373](https://github.com/kubeflow/spark-operator/pull/2373) by [@ChenYi015](https://github.com/ChenYi015))

### Bug Fixes

- fix: add webhook cert validity checking ([#2489](https://github.com/kubeflow/spark-operator/pull/2489) by [@teejaded](https://github.com/teejaded))
- fix and add back unit tests ([#2532](https://github.com/kubeflow/spark-operator/pull/2532) by [@nabuskey](https://github.com/nabuskey))
- fix volcano tests ([#2533](https://github.com/kubeflow/spark-operator/pull/2533) by [@nabuskey](https://github.com/nabuskey))
- Add v2 to module path ([#2515](https://github.com/kubeflow/spark-operator/pull/2515) by [@ChenYi015](https://github.com/ChenYi015))
- #2525 spark metrics in depends on prometheus ([#2529](https://github.com/kubeflow/spark-operator/pull/2529) by [@blcksrx](https://github.com/blcksrx))

### Misc

- Add APRA AMCOS to adopters ([#2485](https://github.com/kubeflow/spark-operator/pull/2485) by [@shuch3ng](https://github.com/shuch3ng))
- Bump github.com/stretchr/testify from 1.9.0 to 1.10.0 ([#2488](https://github.com/kubeflow/spark-operator/pull/2488) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/prometheus/client_golang from 1.20.5 to 1.21.1 ([#2487](https://github.com/kubeflow/spark-operator/pull/2487) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump sigs.k8s.io/controller-runtime from 0.20.1 to 0.20.4 ([#2486](https://github.com/kubeflow/spark-operator/pull/2486) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Deprecating sparkctl ([#2484](https://github.com/kubeflow/spark-operator/pull/2484) by [@vikas-saxena02](https://github.com/vikas-saxena02))
- Changing image repo from docker.io to  ghcr.io ([#2483](https://github.com/kubeflow/spark-operator/pull/2483) by [@vikas-saxena02](https://github.com/vikas-saxena02))
- Upgrade Golang to 1.24.1 and golangci-lint to 1.64.8 ([#2494](https://github.com/kubeflow/spark-operator/pull/2494) by [@jacobsalway](https://github.com/jacobsalway))
- Bump helm.sh/helm/v3 from 3.16.2 to 3.17.3 ([#2503](https://github.com/kubeflow/spark-operator/pull/2503) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Add changelog for v2.1.1 ([#2504](https://github.com/kubeflow/spark-operator/pull/2504) by [@ChenYi015](https://github.com/ChenYi015))
- Remove sparkctl ([#2466](https://github.com/kubeflow/spark-operator/pull/2466) by [@ChenYi015](https://github.com/ChenYi015))
- Bump github.com/spf13/viper from 1.19.0 to 1.20.1 ([#2496](https://github.com/kubeflow/spark-operator/pull/2496) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump golang.org/x/net from 0.37.0 to 0.38.0 ([#2505](https://github.com/kubeflow/spark-operator/pull/2505) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Remove clientset, informer and listers generated by code-generator ([#2506](https://github.com/kubeflow/spark-operator/pull/2506) by [@ChenYi015](https://github.com/ChenYi015))
- Remove v1beta1 API ([#2516](https://github.com/kubeflow/spark-operator/pull/2516) by [@ChenYi015](https://github.com/ChenYi015))
- add unit tests for driver and executor configs ([#2521](https://github.com/kubeflow/spark-operator/pull/2521) by [@nabuskey](https://github.com/nabuskey))
- Adding securityContext to spark examples ([#2530](https://github.com/kubeflow/spark-operator/pull/2530) by [@tarekabouzeid](https://github.com/tarekabouzeid))
- Bump github.com/spf13/cobra from 1.8.1 to 1.9.1 ([#2497](https://github.com/kubeflow/spark-operator/pull/2497) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump golang.org/x/mod from 0.23.0 to 0.24.0 ([#2495](https://github.com/kubeflow/spark-operator/pull/2495) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Adding Manabu to the reviewers ([#2522](https://github.com/kubeflow/spark-operator/pull/2522) by [@vara-bonthu](https://github.com/vara-bonthu))
- Bump manusa/actions-setup-minikube from 2.13.1 to 2.14.0 ([#2523](https://github.com/kubeflow/spark-operator/pull/2523) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump k8s.io dependencies to v0.32.5 ([#2540](https://github.com/kubeflow/spark-operator/pull/2540) by [@ChenYi015](https://github.com/ChenYi015))
- Pass the correct LDFLAGS when building the operator image ([#2541](https://github.com/kubeflow/spark-operator/pull/2541) by [@ChenYi015](https://github.com/ChenYi015))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/v2.1.1...v2.2.0)

## [v2.1.1](https://github.com/kubeflow/spark-operator/tree/v2.1.1) (2025-03-21)

### Features

- Adding seccompProfile RuntimeDefault ([#2397](https://github.com/kubeflow/spark-operator/pull/2397) by [@tarekabouzeid](https://github.com/tarekabouzeid))
- Add option for disabling leader election ([#2423](https://github.com/kubeflow/spark-operator/pull/2423) by [@ChenYi015](https://github.com/ChenYi015))
- Controller should only be granted event permissions in spark job namespaces ([#2426](https://github.com/kubeflow/spark-operator/pull/2426) by [@ChenYi015](https://github.com/ChenYi015))
- Make image optional ([#2439](https://github.com/kubeflow/spark-operator/pull/2439) by [@jbhalodia-slack](https://github.com/jbhalodia-slack))
- Support non-standard Spark container names ([#2441](https://github.com/kubeflow/spark-operator/pull/2441) by [@jbhalodia-slack](https://github.com/jbhalodia-slack))
- add support for metrics-job-start-latency-buckets flag in helm ([#2450](https://github.com/kubeflow/spark-operator/pull/2450) by [@nabuskey](https://github.com/nabuskey))

### Bug Fixes

- fix: webhook fail to add lifecycle to Spark3 executor pods ([#2458](https://github.com/kubeflow/spark-operator/pull/2458) by [@pvbouwel](https://github.com/pvbouwel))
- change env in executorSecretOption ([#2467](https://github.com/kubeflow/spark-operator/pull/2467) by [@TQJADE](https://github.com/TQJADE))

### Misc

- Move sparkctl to cmd directory ([#2347](https://github.com/kubeflow/spark-operator/pull/2347) by [@ChenYi015](https://github.com/ChenYi015))
- Bump golang.org/x/net from 0.30.0 to 0.32.0 ([#2350](https://github.com/kubeflow/spark-operator/pull/2350) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump golang.org/x/crypto from 0.30.0 to 0.31.0 ([#2365](https://github.com/kubeflow/spark-operator/pull/2365) by [@dependabot[bot]](https://github.com/apps/dependabot))
- add an example of using prometheus servlet ([#2403](https://github.com/kubeflow/spark-operator/pull/2403) by [@nabuskey](https://github.com/nabuskey))
- Remove dependency on `k8s.io/kubernetes` ([#2398](https://github.com/kubeflow/spark-operator/pull/2398) by [@jacobsalway](https://github.com/jacobsalway))
- fix make deploy and install ([#2412](https://github.com/kubeflow/spark-operator/pull/2412) by [@nabuskey](https://github.com/nabuskey))
- Add helm unittest step to integration test workflow ([#2424](https://github.com/kubeflow/spark-operator/pull/2424) by [@ChenYi015](https://github.com/ChenYi015))
- ensure passed context is used ([#2432](https://github.com/kubeflow/spark-operator/pull/2432) by [@nabuskey](https://github.com/nabuskey))
- Bump manusa/actions-setup-minikube from 2.13.0 to 2.13.1 ([#2390](https://github.com/kubeflow/spark-operator/pull/2390) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump helm/chart-testing-action from 2.6.1 to 2.7.0 ([#2391](https://github.com/kubeflow/spark-operator/pull/2391) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump golang.org/x/mod from 0.21.0 to 0.23.0 ([#2427](https://github.com/kubeflow/spark-operator/pull/2427) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/golang/glog from 1.2.2 to 1.2.4 ([#2411](https://github.com/kubeflow/spark-operator/pull/2411) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump golang.org/x/net from 0.32.0 to 0.35.0 ([#2428](https://github.com/kubeflow/spark-operator/pull/2428) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Support Kubernetes 1.32 ([#2416](https://github.com/kubeflow/spark-operator/pull/2416) by [@jacobsalway](https://github.com/jacobsalway))
- use cmd context in sparkctl ([#2447](https://github.com/kubeflow/spark-operator/pull/2447) by [@nabuskey](https://github.com/nabuskey))
- Bump golang.org/x/net from 0.35.0 to 0.36.0 ([#2470](https://github.com/kubeflow/spark-operator/pull/2470) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump aquasecurity/trivy-action from 0.29.0 to 0.30.0 ([#2475](https://github.com/kubeflow/spark-operator/pull/2475) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump golang.org/x/net from 0.35.0 to 0.37.0 ([#2472](https://github.com/kubeflow/spark-operator/pull/2472) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/containerd/containerd from 1.7.19 to 1.7.27 ([#2476](https://github.com/kubeflow/spark-operator/pull/2476) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump k8s.io/apimachinery from 0.32.0 to 0.32.3 ([#2474](https://github.com/kubeflow/spark-operator/pull/2474) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/aws/aws-sdk-go-v2/service/s3 from 1.66.0 to 1.78.2 ([#2473](https://github.com/kubeflow/spark-operator/pull/2473) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/aws/aws-sdk-go-v2/config from 1.28.0 to 1.29.9 ([#2463](https://github.com/kubeflow/spark-operator/pull/2463) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump sigs.k8s.io/scheduler-plugins from 0.29.8 to 0.30.6 ([#2444](https://github.com/kubeflow/spark-operator/pull/2444) by [@dependabot[bot]](https://github.com/apps/dependabot))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/v2.1.0...v2.1.1)

## [v2.1.0](https://github.com/kubeflow/spark-operator/tree/v2.1.0) (2024-12-06)

### New Features

- Upgrade to Spark 3.5.3 ([#2202](https://github.com/kubeflow/spark-operator/pull/2202) by [@jacobsalway](https://github.com/jacobsalway))
- feat: support archives param for spark-submit ([#2256](https://github.com/kubeflow/spark-operator/pull/2256) by [@kaka-zb](https://github.com/kaka-zb))
- Allow --ingress-class-name to be specified in chart ([#2278](https://github.com/kubeflow/spark-operator/pull/2278) by [@jacobsalway](https://github.com/jacobsalway))
- Update default container security context ([#2265](https://github.com/kubeflow/spark-operator/pull/2265) by [@ChenYi015](https://github.com/ChenYi015))
- Support pod template for Spark 3.x applications ([#2141](https://github.com/kubeflow/spark-operator/pull/2141) by [@ChenYi015](https://github.com/ChenYi015))
- Allow setting automountServiceAccountToken ([#2298](https://github.com/kubeflow/spark-operator/pull/2298) by [@Aranch](https://github.com/Aransh))
- Allow the Controller and Webhook Containers to run with the securityContext: readOnlyRootfilesystem: true ([#2282](https://github.com/kubeflow/spark-operator/pull/2282) by [@npgretz](https://github.com/npgretz))
- Use NSS_WRAPPER_PASSWD instead of /etc/passwd as in spark-operator image entrypoint.sh ([#2312](https://github.com/kubeflow/spark-operator/pull/2312) by [@Aakcht](https://github.com/Aakcht))

### Bug Fixes

- Minor fixes to e2e test `make` targets ([#2242](https://github.com/kubeflow/spark-operator/pull/2242) by [@Tom-Newton](https://github.com/Tom-Newton))
- Added off heap memory to calculation for YuniKorn gang scheduling ([#2209](https://github.com/kubeflow/spark-operator/pull/2209) by [@guangyu-yang-rokt](https://github.com/guangyu-yang-rokt))
- Add permissions to controller serviceaccount to list and watch ingresses ([#2246](https://github.com/kubeflow/spark-operator/pull/2246) by [@tcassaert](https://github.com/tcassaert))
- Make sure enable-ui-service flag is set to false when controller.uiService.enable is set to false ([#2261](https://github.com/kubeflow/spark-operator/pull/2261) by [@Roberdvs](https://github.com/Roberdvs))
- `omitempty` corrections ([#2255](https://github.com/kubeflow/spark-operator/pull/2255) by [@Tom-Newton](https://github.com/Tom-Newton))
- Fix retries ([#2241](https://github.com/kubeflow/spark-operator/pull/2241) by [@Tom-Newton](https://github.com/Tom-Newton))
- Fix: executor container security context does not work ([#2306](https://github.com/kubeflow/spark-operator/pull/2306) by [@ChenYi015](https://github.com/ChenYi015))
- Fix: should not add emptyDir sizeLimit conf if it is nil ([#2305](https://github.com/kubeflow/spark-operator/pull/2305) by [@ChenYi015](https://github.com/ChenYi015))
- Fix: should not add emptyDir sizeLimit conf on executor pods if it is nil ([#2316](https://github.com/kubeflow/spark-operator/pull/2316) by [@Cian911](https://github.com/Cian911))
- Truncate UI service name if over 63 characters ([#2311](https://github.com/kubeflow/spark-operator/pull/2311) by [@jacobsalway](https://github.com/jacobsalway))
- The webhook-key-name command-line param isn't taking effect ([#2344](https://github.com/kubeflow/spark-operator/pull/2344) by [@c-h-afzal](https://github.com/c-h-afzal))
- Robustness to driver pod taking time to create ([#2315](https://github.com/kubeflow/spark-operator/pull/2315) by [@Tom-Newton](https://github.com/Tom-Newton))

### Misc

- remove redundant test.sh file ([#2243](https://github.com/kubeflow/spark-operator/pull/2243) by [@ChenYi015](https://github.com/ChenYi015))
- Bump github.com/aws/aws-sdk-go-v2/config from 1.27.42 to 1.27.43 ([#2252](https://github.com/kubeflow/spark-operator/pull/2252) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump manusa/actions-setup-minikube from 2.12.0 to 2.13.0 ([#2247](https://github.com/kubeflow/spark-operator/pull/2247) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump golang.org/x/net from 0.29.0 to 0.30.0 ([#2251](https://github.com/kubeflow/spark-operator/pull/2251) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump aquasecurity/trivy-action from 0.24.0 to 0.27.0 ([#2248](https://github.com/kubeflow/spark-operator/pull/2248) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump gocloud.dev from 0.39.0 to 0.40.0 ([#2250](https://github.com/kubeflow/spark-operator/pull/2250) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Add Quick Start guide to README ([#2259](https://github.com/kubeflow/spark-operator/pull/2259) by [@jacobsalway](https://github.com/jacobsalway))
- Bump github.com/aws/aws-sdk-go-v2/service/s3 from 1.63.3 to 1.65.3 ([#2249](https://github.com/kubeflow/spark-operator/pull/2249) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Add release badge to README ([#2263](https://github.com/kubeflow/spark-operator/pull/2263) by [@jacobsalway](https://github.com/jacobsalway))
- Bump helm.sh/helm/v3 from 3.16.1 to 3.16.2 ([#2275](https://github.com/kubeflow/spark-operator/pull/2275) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/prometheus/client_golang from 1.20.4 to 1.20.5 ([#2274](https://github.com/kubeflow/spark-operator/pull/2274) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump cloud.google.com/go/storage from 1.44.0 to 1.45.0 ([#2273](https://github.com/kubeflow/spark-operator/pull/2273) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Run e2e tests with Kubernetes version matrix ([#2266](https://github.com/kubeflow/spark-operator/pull/2266) by [@jacobsalway](https://github.com/jacobsalway))
- Bump aquasecurity/trivy-action from 0.27.0 to 0.28.0 ([#2270](https://github.com/kubeflow/spark-operator/pull/2270) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/aws/aws-sdk-go-v2/service/s3 from 1.65.3 to 1.66.0 ([#2271](https://github.com/kubeflow/spark-operator/pull/2271) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/aws/aws-sdk-go-v2/config from 1.27.43 to 1.28.0 ([#2272](https://github.com/kubeflow/spark-operator/pull/2272) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Add workflow for releasing sparkctl binary ([#2264](https://github.com/kubeflow/spark-operator/pull/2264) by [@ChenYi015](https://github.com/ChenYi015))
- Bump `volcano.sh/apis` to 1.10.0 ([#2320](https://github.com/kubeflow/spark-operator/pull/2320) by [@jacobsalway](https://github.com/jacobsalway))
- Bump aquasecurity/trivy-action from 0.28.0 to 0.29.0 ([#2332](https://github.com/kubeflow/spark-operator/pull/2332) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/onsi/ginkgo/v2 from 2.20.2 to 2.22.0 ([#2335](https://github.com/kubeflow/spark-operator/pull/2335) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Move sparkctl to cmd directory ([#2347](https://github.com/kubeflow/spark-operator/pull/2347) by [@ChenYi015](https://github.com/ChenYi015))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/a8b5d6...v2.1.0 )

## [v2.0.2](https://github.com/kubeflow/spark-operator/tree/v2.0.2) (2024-10-10)

### Bug Fixes

- Fix ingress capability discovery ([#2201](https://github.com/kubeflow/spark-operator/pull/2201) by [@jacobsalway](https://github.com/jacobsalway))
- fix: imagePullPolicy was ignored ([#2222](https://github.com/kubeflow/spark-operator/pull/2222) by [@missedone](https://github.com/missedone))
- fix: spark-submission failed due to lack of permission by user `spark` ([#2223](https://github.com/kubeflow/spark-operator/pull/2223) by [@missedone](https://github.com/missedone))
- Remove `cap_net_bind_service` from image ([#2216](https://github.com/kubeflow/spark-operator/pull/2216) by [@jacobsalway](https://github.com/jacobsalway))
- fix: webhook panics due to logging ([#2232](https://github.com/kubeflow/spark-operator/pull/2232) by [@ChenYi015](https://github.com/ChenYi015))

### Misc

- Bump github.com/aws/aws-sdk-go-v2 from 1.30.5 to 1.31.0 ([#2207](https://github.com/kubeflow/spark-operator/pull/2207) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump golang.org/x/net from 0.28.0 to 0.29.0 ([#2205](https://github.com/kubeflow/spark-operator/pull/2205) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/docker/docker from 27.0.3+incompatible to 27.1.1+incompatible ([#2125](https://github.com/kubeflow/spark-operator/pull/2125) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/aws/aws-sdk-go-v2/service/s3 from 1.58.3 to 1.63.3 ([#2206](https://github.com/kubeflow/spark-operator/pull/2206) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Update integration test workflow and add golangci lint check ([#2197](https://github.com/kubeflow/spark-operator/pull/2197) by [@ChenYi015](https://github.com/ChenYi015))
- Bump github.com/aws/aws-sdk-go-v2 from 1.31.0 to 1.32.0 ([#2229](https://github.com/kubeflow/spark-operator/pull/2229) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump cloud.google.com/go/storage from 1.43.0 to 1.44.0 ([#2228](https://github.com/kubeflow/spark-operator/pull/2228) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump manusa/actions-setup-minikube from 2.11.0 to 2.12.0 ([#2226](https://github.com/kubeflow/spark-operator/pull/2226) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump golang.org/x/time from 0.6.0 to 0.7.0 ([#2227](https://github.com/kubeflow/spark-operator/pull/2227) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/aws/aws-sdk-go-v2/config from 1.27.33 to 1.27.42 ([#2231](https://github.com/kubeflow/spark-operator/pull/2231) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/prometheus/client_golang from 1.19.1 to 1.20.4 ([#2204](https://github.com/kubeflow/spark-operator/pull/2204) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Add check for generating manifests and code ([#2234](https://github.com/kubeflow/spark-operator/pull/2234) by [@ChenYi015](https://github.com/ChenYi015))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/v2.0.1...v2.0.2)

## [v2.0.1](https://github.com/kubeflow/spark-operator/tree/v2.0.1) (2024-09-26)

### New Features

- FEATURE: build operator image as non-root ([#2171](https://github.com/kubeflow/spark-operator/pull/2171) by [@ImpSy](https://github.com/ImpSy))

### Bug Fixes

- Update controller RBAC for ConfigMap and PersistentVolumeClaim ([#2187](https://github.com/kubeflow/spark-operator/pull/2187) by [@ChenYi015](https://github.com/ChenYi015))

### Misc

- Bump github.com/onsi/ginkgo/v2 from 2.19.0 to 2.20.2 ([#2188](https://github.com/kubeflow/spark-operator/pull/2188) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump github.com/onsi/gomega from 1.33.1 to 1.34.2 ([#2189](https://github.com/kubeflow/spark-operator/pull/2189) by [@dependabot[bot]](https://github.com/apps/dependabot))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/v2.0.0...v2.0.1)

## [v2.0.0](https://github.com/kubeflow/spark-operator/tree/v2.0.0) (2024-09-23)

### Breaking Changes

- Use controller-runtime to reconsturct spark operator ([#2072](https://github.com/kubeflow/spark-operator/pull/2072) by [@ChenYi015](https://github.com/ChenYi015))
- feat: support driver and executor pod use different priority ([#2146](https://github.com/kubeflow/spark-operator/pull/2146) by [@Kevinz857](https://github.com/Kevinz857))

### New Features

- Support gang scheduling with Yunikorn ([#2107](https://github.com/kubeflow/spark-operator/pull/2107)) by [@jacobsalway](https://github.com/jacobsalway)
- Reintroduce option webhook.enable ([#2142](https://github.com/kubeflow/spark-operator/pull/2142) by [@ChenYi015](https://github.com/ChenYi015))
- Add default batch scheduler argument ([#2143](https://github.com/kubeflow/spark-operator/pull/2143) by [@jacobsalway](https://github.com/jacobsalway))
- Support extended kube-scheduler as batch scheduler ([#2136](https://github.com/kubeflow/spark-operator/pull/2136) by [@ChenYi015](https://github.com/ChenYi015))
- Set schedulerName to Yunikorn ([#2153](https://github.com/kubeflow/spark-operator/pull/2153) by [@jacobsalway](https://github.com/jacobsalway))
- Feature: Add pprof endpoint ([#2164](https://github.com/kubeflow/spark-operator/pull/2164) by [@ImpSy](https://github.com/ImpSy))

### Bug Fixes

- fix: Add default values for namespaces to match usage descriptions  ([#2128](https://github.com/kubeflow/spark-operator/pull/2128) by [@snappyyouth](https://github.com/snappyyouth))
- Fix: Spark role binding did not render properly when setting spark service account name ([#2135](https://github.com/kubeflow/spark-operator/pull/2135) by [@ChenYi015](https://github.com/ChenYi015))
- fix: unable to set controller/webhook replicas to zero ([#2147](https://github.com/kubeflow/spark-operator/pull/2147) by [@ChenYi015](https://github.com/ChenYi015))
- Adding support for setting spark job namespaces to all namespaces ([#2123](https://github.com/kubeflow/spark-operator/pull/2123) by [@ChenYi015](https://github.com/ChenYi015))
- Fix: e2e test failes due to webhook not ready ([#2149](https://github.com/kubeflow/spark-operator/pull/2149) by [@ChenYi015](https://github.com/ChenYi015))
- fix: webhook not working when settings spark job namespaces to empty ([#2163](https://github.com/kubeflow/spark-operator/pull/2163) by [@ChenYi015](https://github.com/ChenYi015))
- fix: The logger had an odd number of arguments, making it panic ([#2166](https://github.com/kubeflow/spark-operator/pull/2166) by [@tcassaert](https://github.com/tcassaert))
- fix the make kind-delete-custer to avoid accidental kubeconfig deletion ([#2172](https://github.com/kubeflow/spark-operator/pull/2172) by [@ImpSy](https://github.com/ImpSy))
- Add specific error in log line when failed to create web UI service ([#2170](https://github.com/kubeflow/spark-operator/pull/2170) by [@tcassaert](https://github.com/tcassaert))
- Account for spark.executor.pyspark.memory in Yunikorn gang scheduling ([#2178](https://github.com/kubeflow/spark-operator/pull/2178) by [@jacobsalway](https://github.com/jacobsalway))
- Fix: spark application does not respect time to live seconds ([#2165](https://github.com/kubeflow/spark-operator/pull/2165) by [@ChenYi015](https://github.com/ChenYi015))

### Misc

- Update workflow and docs for releasing Spark operator ([#2089](https://github.com/kubeflow/spark-operator/pull/2089) by [@ChenYi015](https://github.com/ChenYi015))
- Fix broken integration test CI ([#2109](https://github.com/kubeflow/spark-operator/pull/2109) by [@ChenYi015](https://github.com/ChenYi015))
- Fix CI: environment variable BRANCH is missed ([#2111](https://github.com/kubeflow/spark-operator/pull/2111) by [@ChenYi015](https://github.com/ChenYi015))
- Update Makefile for building sparkctl ([#2119](https://github.com/kubeflow/spark-operator/pull/2119) by [@ChenYi015](https://github.com/ChenYi015))
- Update release workflow and docs ([#2121](https://github.com/kubeflow/spark-operator/pull/2121) by [@ChenYi015](https://github.com/ChenYi015))
- Run e2e tests on Kind ([#2148](https://github.com/kubeflow/spark-operator/pull/2148) by [@jacobsalway](https://github.com/jacobsalway))
- Upgrade to Go 1.23.1 ([#2155](https://github.com/kubeflow/spark-operator/pull/2155) by [@jacobsalway](https://github.com/jacobsalway))
- Upgrade to Spark 3.5.2 ([#2154](https://github.com/kubeflow/spark-operator/pull/2154) by [@jacobsalway](https://github.com/jacobsalway))
- Bump sigs.k8s.io/scheduler-plugins from 0.29.7 to 0.29.8 ([#2159](https://github.com/kubeflow/spark-operator/pull/2159) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump gocloud.dev from 0.37.0 to 0.39.0 ([#2160](https://github.com/kubeflow/spark-operator/pull/2160) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Update e2e tests ([#2161](https://github.com/kubeflow/spark-operator/pull/2161) by [@ChenYi015](https://github.com/ChenYi015))
- Upgrade to Spark 3.5.2(#2012) ([#2157](https://github.com/kubeflow/spark-operator/pull/2157) by [@ha2hi](https://github.com/ha2hi))
- Bump github.com/aws/aws-sdk-go-v2/config from 1.27.27 to 1.27.33 ([#2174](https://github.com/kubeflow/spark-operator/pull/2174) by [@dependabot[bot]](https://github.com/apps/dependabot))
- Bump helm.sh/helm/v3 from 3.15.3 to 3.16.1 ([#2173](https://github.com/kubeflow/spark-operator/pull/2173) by [@dependabot[bot]](https://github.com/apps/dependabot))
- implement workflow to scan latest released docker image ([#2177](https://github.com/kubeflow/spark-operator/pull/2177) by [@ImpSy](https://github.com/ImpSy))

## What's Changed

- Cherry pick #2081 #2046 #2091 #2072 by @ChenYi015 in <https://github.com/kubeflow/spark-operator/pull/2108>
- Cherry pick #2089 #2109 #2111 by @ChenYi015 in <https://github.com/kubeflow/spark-operator/pull/2110>
- Release v2.0.0-rc.0 by @ChenYi015 in <https://github.com/kubeflow/spark-operator/pull/2115>
- Cherry pick commits for releasing v2.0.0 by @ChenYi015 in <https://github.com/kubeflow/spark-operator/pull/2156>
- Release v2.0.0 by @ChenYi015 in <https://github.com/kubeflow/spark-operator/pull/2182>

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/v1beta2-1.6.2-3.5.0...v2.0.0)

## [v2.0.0-rc.0](https://github.com/kubeflow/spark-operator/tree/v2.0.0-rc.0) (2024-08-09)

### Breaking Changes

- Use controller-runtime to reconsturct spark operator ([#2072](https://github.com/kubeflow/spark-operator/pull/2072) by [@ChenYi015](https://github.com/ChenYi015))

### Misc

- Fix CI: environment variable BRANCH is missed ([#2111](https://github.com/kubeflow/spark-operator/pull/2111) by [@ChenYi015](https://github.com/ChenYi015))
- Fix broken integration test CI ([#2109](https://github.com/kubeflow/spark-operator/pull/2109) by [@ChenYi015](https://github.com/ChenYi015))

- Update workflow and docs for releasing Spark operator ([#2089](https://github.com/kubeflow/spark-operator/pull/2089) by [@ChenYi015](https://github.com/ChenYi015))

### What's Changed

- Release v2.0.0-rc.0 ([#2115](https://github.com/kubeflow/spark-operator/pull/2115) by [@ChenYi015](https://github.com/ChenYi015))
- Cherry pick #2089 #2109 #2111 ([#2110](https://github.com/kubeflow/spark-operator/pull/2110) by [@ChenYi015](https://github.com/ChenYi015))
- Cherry pick #2081 #2046 #2091 #2072 ([#2108](https://github.com/kubeflow/spark-operator/pull/2108) by [@ChenYi015](https://github.com/ChenYi015))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.4.3...v2.0.0-rc.0)

## [spark-operator-chart-1.4.6](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.4.6) (2024-07-26)

- Add topologySpreadConstraints ([#2091](https://github.com/kubeflow/spark-operator/pull/2091) by [@jbhalodia-slack](https://github.com/jbhalodia-slack))
- Add Alibaba Cloud to adopters ([#2097](https://github.com/kubeflow/spark-operator/pull/2097) by [@ChenYi015](https://github.com/ChenYi015))
- Update Stale bot settings ([#2095](https://github.com/kubeflow/spark-operator/pull/2095) by [@andreyvelich](https://github.com/andreyvelich))
- Add @ChenYi015 to approvers ([#2096](https://github.com/kubeflow/spark-operator/pull/2096) by [@ChenYi015](https://github.com/ChenYi015))
- Add CHANGELOG.md file and use python script to generate it automatically ([#2087](https://github.com/kubeflow/spark-operator/pull/2087) by [@ChenYi015](https://github.com/ChenYi015))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.4.5...spark-operator-chart-1.4.6)

## [spark-operator-chart-1.4.5](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.4.5) (2024-07-22)

- Update the process to build api-docs, generate CRD manifests and code ([#2046](https://github.com/kubeflow/spark-operator/pull/2046) by [@ChenYi015](https://github.com/ChenYi015))
- Add workflow for closing stale issues and PRs ([#2073](https://github.com/kubeflow/spark-operator/pull/2073) by [@ChenYi015](https://github.com/ChenYi015))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.4.4...spark-operator-chart-1.4.5)

## [spark-operator-chart-1.4.4](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.4.4) (2024-07-22)

- Update helm docs ([#2081](https://github.com/kubeflow/spark-operator/pull/2081) by [@csp33](https://github.com/csp33))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.4.3...spark-operator-chart-1.4.4)

## [spark-operator-chart-1.4.3](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.4.3) (2024-07-03)

- Add PodDisruptionBudget to chart ([#2078](https://github.com/kubeflow/spark-operator/pull/2078) by [@csp33](https://github.com/csp33))
- Update README and documentation ([#2047](https://github.com/kubeflow/spark-operator/pull/2047) by [@ChenYi015](https://github.com/ChenYi015))
- Add code of conduct and update contributor guide ([#2074](https://github.com/kubeflow/spark-operator/pull/2074) by [@ChenYi015](https://github.com/ChenYi015))
- Remove .gitlab-ci.yml ([#2069](https://github.com/kubeflow/spark-operator/pull/2069) by [@jacobsalway](https://github.com/jacobsalway))
- Modified README.MD as per changes discussed on <https://github.com/kubeflow/spark-operator/pull/2062> ([#2066](https://github.com/kubeflow/spark-operator/pull/2066) by [@vikas-saxena02](https://github.com/vikas-saxena02))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.4.2...spark-operator-chart-1.4.3)

## [spark-operator-chart-1.4.2](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.4.2) (2024-06-17)

- Support objectSelector on mutating webhook ([#2058](https://github.com/kubeflow/spark-operator/pull/2058) by [@Cian911](https://github.com/Cian911))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.4.1...spark-operator-chart-1.4.2)

## [spark-operator-chart-1.4.1](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.4.1) (2024-06-15)

- Adding an option to set the priority class for spark-operator pod ([#2043](https://github.com/kubeflow/spark-operator/pull/2043) by [@pkgajulapalli](https://github.com/pkgajulapalli))
- Update minikube version in CI ([#2059](https://github.com/kubeflow/spark-operator/pull/2059) by [@Cian911](https://github.com/Cian911))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.4.0...spark-operator-chart-1.4.1)

## [spark-operator-chart-1.4.0](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.4.0) (2024-06-05)

- Certifictes are generated by operator rather than gencerts.sh ([#2016](https://github.com/kubeflow/spark-operator/pull/2016) by [@ChenYi015](https://github.com/ChenYi015))
- Add ChenYi015 as spark-operator reviewer ([#2045](https://github.com/kubeflow/spark-operator/pull/2045) by [@ChenYi015](https://github.com/ChenYi015))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.3.2...spark-operator-chart-1.4.0)

## [spark-operator-chart-1.3.2](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.3.2) (2024-06-05)

- Bump appVersion to v1beta2-1.5.0-3.5.0 ([#2044](https://github.com/kubeflow/spark-operator/pull/2044) by [@ChenYi015](https://github.com/ChenYi015))
- Add restartPolicy field to SparkApplication Driver/Executor initContainers CRDs ([#2022](https://github.com/kubeflow/spark-operator/pull/2022) by [@mschroering](https://github.com/mschroering))
- :memo: Add Inter&Co to who-is-using.md ([#2040](https://github.com/kubeflow/spark-operator/pull/2040) by [@ignitz](https://github.com/ignitz))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.3.1...spark-operator-chart-1.3.2)

## [spark-operator-chart-1.3.1](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.3.1) (2024-05-31)

- Chart: add POD_NAME env for leader election ([#2039](https://github.com/kubeflow/spark-operator/pull/2039) by [@Aakcht](https://github.com/Aakcht))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.3.0...spark-operator-chart-1.3.1)

## [spark-operator-chart-1.3.0](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.3.0) (2024-05-20)

- Support exposing extra TCP ports in Spark Driver via K8s Ingress ([#1998](https://github.com/kubeflow/spark-operator/pull/1998) by [@hiboyang](https://github.com/hiboyang))
- Fixes a bug with dynamic allocation forcing the executor count to be 1 even when minExecutors is set to 0 ([#1979](https://github.com/kubeflow/spark-operator/pull/1979) by [@peter-mcclonski](https://github.com/peter-mcclonski))
- Remove outdated PySpark experimental warning in example ([#2014](https://github.com/kubeflow/spark-operator/pull/2014) by [@andrejpk](https://github.com/andrejpk))
- Update Spark Job Namespace docs ([#2000](https://github.com/kubeflow/spark-operator/pull/2000) by [@matthewrossi](https://github.com/matthewrossi))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.2.15...spark-operator-chart-1.3.0)

## [spark-operator-chart-1.2.15](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.2.15) (2024-05-07)

- Fix examples ([#2010](https://github.com/kubeflow/spark-operator/pull/2010) by [@peter-mcclonski](https://github.com/peter-mcclonski))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.2.14...spark-operator-chart-1.2.15)

## [spark-operator-chart-1.2.14](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.2.14) (2024-04-26)

- feat: add support for service labels on driver-svc ([#1985](https://github.com/kubeflow/spark-operator/pull/1985) by [@Cian911](https://github.com/Cian911))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.2.13...spark-operator-chart-1.2.14)

## [spark-operator-chart-1.2.13](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.2.13) (2024-04-24)

- fix(chart): remove operator namespace default for job namespaces value ([#1989](https://github.com/kubeflow/spark-operator/pull/1989) by [@t3mi](https://github.com/t3mi))
- Fix Docker Hub Credentials in CI ([#2003](https://github.com/kubeflow/spark-operator/pull/2003) by [@andreyvelich](https://github.com/andreyvelich))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.2.12...spark-operator-chart-1.2.13)

## [spark-operator-chart-1.2.12](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.2.12) (2024-04-19)

- Add emptyDir sizeLimit support for local dirs ([#1993](https://github.com/kubeflow/spark-operator/pull/1993) by [@jacobsalway](https://github.com/jacobsalway))
- fix: Removed `publish-image` dependency on publishing the helm chart ([#1995](https://github.com/kubeflow/spark-operator/pull/1995) by [@vara-bonthu](https://github.com/vara-bonthu))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.2.11...spark-operator-chart-1.2.12)

## [spark-operator-chart-1.2.11](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.2.11) (2024-04-19)

- fix: Update Github workflow to publish Helm charts on chart changes, irrespective of image change ([#1992](https://github.com/kubeflow/spark-operator/pull/1992) by [@vara-bonthu](https://github.com/vara-bonthu))
- chore: Add Timo to user list ([#1615](https://github.com/kubeflow/spark-operator/pull/1615) by [@vanducng](https://github.com/vanducng))
- Update spark operator permissions for CRD ([#1973](https://github.com/kubeflow/spark-operator/pull/1973) by [@ChenYi015](https://github.com/ChenYi015))
- fix spark-rbac ([#1986](https://github.com/kubeflow/spark-operator/pull/1986) by [@Aransh](https://github.com/Aransh))
- Use Kubeflow Docker Hub for Spark Operator Image ([#1974](https://github.com/kubeflow/spark-operator/pull/1974) by [@andreyvelich](https://github.com/andreyvelich))
- fix: fixed serviceaccount annotations ([#1972](https://github.com/kubeflow/spark-operator/pull/1972) by [@AndrewChubatiuk](https://github.com/AndrewChubatiuk))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.2.7...spark-operator-chart-1.2.11)

## [spark-operator-chart-1.2.7](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.2.7) (2024-04-16)

- fix: upgraded k8s deps ([#1983](https://github.com/kubeflow/spark-operator/pull/1983) by [@AndrewChubatiuk](https://github.com/AndrewChubatiuk))
- chore: remove k8s.io/kubernetes replaces and adapt to v1.29.3 apis ([#1968](https://github.com/kubeflow/spark-operator/pull/1968) by [@ajayk](https://github.com/ajayk))
- Add some helm chart unit tests and fix spark service account render failure when extra annotations are specified ([#1967](https://github.com/kubeflow/spark-operator/pull/1967) by [@ChenYi015](https://github.com/ChenYi015))
- feat: Doc updates, Issue and PR templates are added ([#1970](https://github.com/kubeflow/spark-operator/pull/1970) by [@vara-bonthu](https://github.com/vara-bonthu))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.2.5...spark-operator-chart-1.2.7)

## [spark-operator-chart-1.2.5](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.2.5) (2024-04-14)

- fixed docker image tag and updated chart docs ([#1969](https://github.com/kubeflow/spark-operator/pull/1969) by [@AndrewChubatiuk](https://github.com/AndrewChubatiuk))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.2.4...spark-operator-chart-1.2.5)

## [spark-operator-chart-1.2.4](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.2.4) (2024-04-13)

- publish chart independently, incremented both chart and image versions to trigger build of both ([#1964](https://github.com/kubeflow/spark-operator/pull/1964) by [@AndrewChubatiuk](https://github.com/AndrewChubatiuk))
- Update helm chart README ([#1958](https://github.com/kubeflow/spark-operator/pull/1958) by [@ChenYi015](https://github.com/ChenYi015))
- fix: add containerPort declaration for webhook in helm chart ([#1961](https://github.com/kubeflow/spark-operator/pull/1961) by [@zevisert](https://github.com/zevisert))
- added id for a build job to fix digests artifact creation ([#1963](https://github.com/kubeflow/spark-operator/pull/1963) by [@AndrewChubatiuk](https://github.com/AndrewChubatiuk))
- support multiple namespaces ([#1955](https://github.com/kubeflow/spark-operator/pull/1955) by [@AndrewChubatiuk](https://github.com/AndrewChubatiuk))
- chore: replace GoogleCloudPlatform/spark-on-k8s-operator with kubeflow/spark-operator ([#1937](https://github.com/kubeflow/spark-operator/pull/1937) by [@zevisert](https://github.com/zevisert))
- Chart: add patch permissions for spark operator SA to support spark 3.5.0 ([#1884](https://github.com/kubeflow/spark-operator/pull/1884) by [@Aakcht](https://github.com/Aakcht))
- Cleanup after golang upgrade ([#1956](https://github.com/kubeflow/spark-operator/pull/1956) by [@AndrewChubatiuk](https://github.com/AndrewChubatiuk))
- feat: add support for custom service labels ([#1952](https://github.com/kubeflow/spark-operator/pull/1952) by [@Cian911](https://github.com/Cian911))
- upgraded golang and dependencies ([#1954](https://github.com/kubeflow/spark-operator/pull/1954) by [@AndrewChubatiuk](https://github.com/AndrewChubatiuk))
- README for installing operator using kustomize with custom namespace and service name ([#1778](https://github.com/kubeflow/spark-operator/pull/1778) by [@shahsiddharth08](https://github.com/shahsiddharth08))
- BUGFIX: Added cancel method to fix context leak ([#1917](https://github.com/kubeflow/spark-operator/pull/1917) by [@fazledyn-or](https://github.com/fazledyn-or))
- remove unmatched quotes from user-guide.md ([#1584](https://github.com/kubeflow/spark-operator/pull/1584) by [@taeyeopkim1](https://github.com/taeyeopkim1))
- Add PVC permission to Operator role ([#1889](https://github.com/kubeflow/spark-operator/pull/1889) by [@wyangsun](https://github.com/wyangsun))
- Allow to set webhook job resource limits (#1429,#1300) ([#1946](https://github.com/kubeflow/spark-operator/pull/1946) by [@karbyshevds](https://github.com/karbyshevds))
- Create OWNERS ([#1927](https://github.com/kubeflow/spark-operator/pull/1927) by [@zijianjoy](https://github.com/zijianjoy))
- fix: fix issue #1723 about spark-operator not working with volcano on OCP ([#1724](https://github.com/kubeflow/spark-operator/pull/1724) by [@disaster37](https://github.com/disaster37))
- Add Rokt to who-is-using.md ([#1867](https://github.com/kubeflow/spark-operator/pull/1867) by [@jacobsalway](https://github.com/jacobsalway))
- Handle invalid API resources in discovery ([#1758](https://github.com/kubeflow/spark-operator/pull/1758) by [@wiltonsr](https://github.com/wiltonsr))
- Fix docs for Volcano integration ([#1719](https://github.com/kubeflow/spark-operator/pull/1719) by [@VVKot](https://github.com/VVKot))
- Added qualytics to who is using ([#1736](https://github.com/kubeflow/spark-operator/pull/1736) by [@josecsotomorales](https://github.com/josecsotomorales))
- Allowing optional annotation on rbac ([#1770](https://github.com/kubeflow/spark-operator/pull/1770) by [@cxfcxf](https://github.com/cxfcxf))
- Support `seccompProfile` in Spark application CRD and fix pre-commit jobs ([#1768](https://github.com/kubeflow/spark-operator/pull/1768) by [@ordukhanian](https://github.com/ordukhanian))
- Updating webhook docs to also mention eks ([#1763](https://github.com/kubeflow/spark-operator/pull/1763) by [@JunaidChaudry](https://github.com/JunaidChaudry))
- Link to helm docs fixed ([#1783](https://github.com/kubeflow/spark-operator/pull/1783) by [@haron](https://github.com/haron))
- Improve getMasterURL() to add [] to IPv6 if needed ([#1825](https://github.com/kubeflow/spark-operator/pull/1825) by [@LittleWat](https://github.com/LittleWat))
- Add envFrom to operator deployment ([#1785](https://github.com/kubeflow/spark-operator/pull/1785) by [@matschaffer-roblox](https://github.com/matschaffer-roblox))
- Expand ingress docs a bit ([#1806](https://github.com/kubeflow/spark-operator/pull/1806) by [@matschaffer-roblox](https://github.com/matschaffer-roblox))
- Optional sidecars for operator pod ([#1754](https://github.com/kubeflow/spark-operator/pull/1754) by [@qq157755587](https://github.com/qq157755587))
- Add Roblox to who-is ([#1784](https://github.com/kubeflow/spark-operator/pull/1784) by [@matschaffer-roblox](https://github.com/matschaffer-roblox))
- Molex started using spark K8 operator. ([#1714](https://github.com/kubeflow/spark-operator/pull/1714) by [@AshishPushpSingh](https://github.com/AshishPushpSingh))
- Extra helm chart labels ([#1669](https://github.com/kubeflow/spark-operator/pull/1669) by [@kvanzuijlen](https://github.com/kvanzuijlen))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.1.27...spark-operator-chart-1.2.4)

## [spark-operator-chart-1.1.27](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.1.27) (2023-03-17)

- Added permissions for leader election #1635 ([#1647](https://github.com/kubeflow/spark-operator/pull/1647) by [@ordukhanian](https://github.com/ordukhanian))
- Fix #1393 : fix tolerations block in wrong segment for webhook jobs ([#1633](https://github.com/kubeflow/spark-operator/pull/1633) by [@zhiminglim](https://github.com/zhiminglim))
- add dependabot ([#1629](https://github.com/kubeflow/spark-operator/pull/1629) by [@monotek](https://github.com/monotek))
- Add support for `ephemeral.volumeClaimTemplate` in helm chart CRDs ([#1661](https://github.com/kubeflow/spark-operator/pull/1661) by [@ArshiAAkhavan](https://github.com/ArshiAAkhavan))
- Add Kognita to "Who is using" ([#1637](https://github.com/kubeflow/spark-operator/pull/1637) by [@claudino-kognita](https://github.com/claudino-kognita))
- add lifecycle to executor ([#1674](https://github.com/kubeflow/spark-operator/pull/1674) by [@tiechengsu](https://github.com/tiechengsu))
- Fix signal handling for non-leader processes ([#1680](https://github.com/kubeflow/spark-operator/pull/1680) by [@antonipp](https://github.com/antonipp))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.1.26...spark-operator-chart-1.1.27)

## [spark-operator-chart-1.1.26](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.1.26) (2022-10-25)

- update go to 1.19 + k8s.io libs to v0.25.3 ([#1630](https://github.com/kubeflow/spark-operator/pull/1630) by [@ImpSy](https://github.com/ImpSy))
- Update README - secrets and sidecars need mutating webhooks ([#1550](https://github.com/kubeflow/spark-operator/pull/1550) by [@djdillon](https://github.com/djdillon))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.1.25...spark-operator-chart-1.1.26)

## [spark-operator-chart-1.1.25](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.1.25) (2022-06-08)

- Webhook init and cleanup should respect nodeSelector ([#1545](https://github.com/kubeflow/spark-operator/pull/1545) by [@erikcw](https://github.com/erikcw))
- rename unit tests to integration tests in Makefile#integration-test ([#1539](https://github.com/kubeflow/spark-operator/pull/1539) by [@dcoliversun](https://github.com/dcoliversun))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.1.24...spark-operator-chart-1.1.25)

## [spark-operator-chart-1.1.24](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.1.24) (2022-06-01)

- Fix: use V1 api for CRDs for volcano integration ([#1540](https://github.com/kubeflow/spark-operator/pull/1540) by [@Aakcht](https://github.com/Aakcht))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.1.23...spark-operator-chart-1.1.24)

## [spark-operator-chart-1.1.23](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.1.23) (2022-05-18)

- fix: add pre-upgrade hook to rbac resources ([#1511](https://github.com/kubeflow/spark-operator/pull/1511) by [@cwyl02](https://github.com/cwyl02))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.1.22...spark-operator-chart-1.1.23)

## [spark-operator-chart-1.1.22](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.1.22) (2022-05-16)

- Fixes issue #1467 (issue when deleting SparkApplication without metrics server) ([#1530](https://github.com/kubeflow/spark-operator/pull/1530) by [@aneagoe](https://github.com/aneagoe))
- Implement --logs and --delete flags on 'sparkctl create' and a timeout on 'sparkctl log' to wait a pod startup ([#1506](https://github.com/kubeflow/spark-operator/pull/1506) by [@alaurentinoofficial](https://github.com/alaurentinoofficial))
- Fix Spark UI URL in app status ([#1518](https://github.com/kubeflow/spark-operator/pull/1518) by [@gtopper](https://github.com/gtopper))
- remove quotes from yaml file ([#1524](https://github.com/kubeflow/spark-operator/pull/1524) by [@zencircle](https://github.com/zencircle))
- Added missing manifest yaml, point the manifest to the right direction ([#1504](https://github.com/kubeflow/spark-operator/pull/1504) by [@RonZhang724](https://github.com/RonZhang724))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.1.21...spark-operator-chart-1.1.22)

## [spark-operator-chart-1.1.21](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.1.21) (2022-05-12)

- Ensure that driver is deleted prior to sparkapplication resubmission ([#1521](https://github.com/kubeflow/spark-operator/pull/1521) by [@khorshuheng](https://github.com/khorshuheng))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.1.20...spark-operator-chart-1.1.21)

## [spark-operator-chart-1.1.20](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.1.20) (2022-04-11)

- Add ingress-class-name controller flag ([#1482](https://github.com/kubeflow/spark-operator/pull/1482) by [@voyvodov](https://github.com/voyvodov))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.1.19...spark-operator-chart-1.1.20)

## [spark-operator-chart-1.1.19](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.1.19) (2022-02-14)

- Add Operator volumes and volumeMounts in chart ([#1475](https://github.com/kubeflow/spark-operator/pull/1475) by [@ocworld](https://github.com/ocworld))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.1.18...spark-operator-chart-1.1.19)

## [spark-operator-chart-1.1.18](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.1.18) (2022-02-13)

- Updated default registry to ghcr.io ([#1454](https://github.com/kubeflow/spark-operator/pull/1454) by [@aneagoe](https://github.com/aneagoe))
- Github actions workflow fix for Helm chart deployment ([#1456](https://github.com/kubeflow/spark-operator/pull/1456) by [@vara-bonthu](https://github.com/vara-bonthu))
- Kubernetes v1.22 extensions/v1beta1 API removal ([#1427](https://github.com/kubeflow/spark-operator/pull/1427) by [@aneagoe](https://github.com/aneagoe))
- Fixes an issue with github action in job build-spark-operator ([#1452](https://github.com/kubeflow/spark-operator/pull/1452) by [@aneagoe](https://github.com/aneagoe))
- use github container registry instead of gcr.io for releases ([#1422](https://github.com/kubeflow/spark-operator/pull/1422) by [@TomHellier](https://github.com/TomHellier))
- Fixes an error that was preventing the pods from being mutated ([#1421](https://github.com/kubeflow/spark-operator/pull/1421) by [@ssullivan](https://github.com/ssullivan))
- Make github actions more feature complete ([#1418](https://github.com/kubeflow/spark-operator/pull/1418) by [@TomHellier](https://github.com/TomHellier))
- Resolves an error when deploying the webhook where the k8s api indica… ([#1413](https://github.com/kubeflow/spark-operator/pull/1413) by [@ssullivan](https://github.com/ssullivan))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.1.15...spark-operator-chart-1.1.18)

## [spark-operator-chart-1.1.15](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.1.15) (2021-12-02)

- Add docker build to github action ([#1415](https://github.com/kubeflow/spark-operator/pull/1415) by [@TomHellier](https://github.com/TomHellier))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.1.14...spark-operator-chart-1.1.15)

## [spark-operator-chart-1.1.14](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.1.14) (2021-11-30)

- Updating API version of admissionregistration.k8s.io ([#1401](https://github.com/kubeflow/spark-operator/pull/1401) by [@sairamankumar2](https://github.com/sairamankumar2))
- Add C2FO to who is using ([#1391](https://github.com/kubeflow/spark-operator/pull/1391) by [@vanhoale](https://github.com/vanhoale))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.1.13...spark-operator-chart-1.1.14)

## [spark-operator-chart-1.1.13](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.1.13) (2021-11-18)

- delete-service-accounts-and-roles-before-creation ([#1384](https://github.com/kubeflow/spark-operator/pull/1384) by [@TiansuYu](https://github.com/TiansuYu))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.1.12...spark-operator-chart-1.1.13)

## [spark-operator-chart-1.1.12](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.1.12) (2021-11-14)

- webhook timeout variable ([#1387](https://github.com/kubeflow/spark-operator/pull/1387) by [@sairamankumar2](https://github.com/sairamankumar2))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.1.11...spark-operator-chart-1.1.12)

## [spark-operator-chart-1.1.11](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.1.11) (2021-11-12)

- [FIX] add service account access to persistentvolumeclaims ([#1390](https://github.com/kubeflow/spark-operator/pull/1390) by [@mschroering](https://github.com/mschroering))
- Add DeepCure to who is using ([#1389](https://github.com/kubeflow/spark-operator/pull/1389) by [@mschroering](https://github.com/mschroering))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.1.10...spark-operator-chart-1.1.11)

## [spark-operator-chart-1.1.10](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.1.10) (2021-11-09)

- Add custom toleration support for webhook jobs ([#1383](https://github.com/kubeflow/spark-operator/pull/1383) by [@korjek](https://github.com/korjek))
- fix container name in addsecuritycontext patch ([#1377](https://github.com/kubeflow/spark-operator/pull/1377) by [@lybavsky](https://github.com/lybavsky))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.1.9...spark-operator-chart-1.1.10)

## [spark-operator-chart-1.1.9](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.1.9) (2021-11-01)

- `Role` and `RoleBinding` not installed for `webhook-init` in Helm `pre-hook` ([#1379](https://github.com/kubeflow/spark-operator/pull/1379) by [@zzvara](https://github.com/zzvara))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.1.8...spark-operator-chart-1.1.9)

## [spark-operator-chart-1.1.8](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.1.8) (2021-10-26)

- Regenerate deleted cert after upgrade ([#1373](https://github.com/kubeflow/spark-operator/pull/1373) by [@simplylizz](https://github.com/simplylizz))
- Make manifests usable by Kustomize ([#1367](https://github.com/kubeflow/spark-operator/pull/1367) by [@karpoftea](https://github.com/karpoftea))
- #1329 update the operator to allow subpaths to be used with the spark ui ingress. ([#1330](https://github.com/kubeflow/spark-operator/pull/1330) by [@TomHellier](https://github.com/TomHellier))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.1.7...spark-operator-chart-1.1.8)

## [spark-operator-chart-1.1.7](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.1.7) (2021-10-21)

- serviceAccount annotations ([#1350](https://github.com/kubeflow/spark-operator/pull/1350) by [@moskitone](https://github.com/moskitone))
- Update Dockerfile ([#1369](https://github.com/kubeflow/spark-operator/pull/1369) by [@Sadagopan88](https://github.com/Sadagopan88))
- [FIX] tolerations are not directly present in Driver(/Executor)Spec ([#1365](https://github.com/kubeflow/spark-operator/pull/1365) by [@s-pedamallu](https://github.com/s-pedamallu))
- fix running metrics for application deletion ([#1358](https://github.com/kubeflow/spark-operator/pull/1358) by [@Aakcht](https://github.com/Aakcht))
- Update who-is-using.md ([#1338](https://github.com/kubeflow/spark-operator/pull/1338) by [@Juandavi1](https://github.com/Juandavi1))
- Update who-is-using.md ([#1082](https://github.com/kubeflow/spark-operator/pull/1082) by [@Juandavi1](https://github.com/Juandavi1))
- Add support for executor service account ([#1322](https://github.com/kubeflow/spark-operator/pull/1322) by [@bbenzikry](https://github.com/bbenzikry))
- fix NPE introduce on #1280 ([#1325](https://github.com/kubeflow/spark-operator/pull/1325) by [@ImpSy](https://github.com/ImpSy))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.1.6...spark-operator-chart-1.1.7)

## [spark-operator-chart-1.1.6](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.1.6) (2021-08-04)

- Add hook deletion policy for spark-operator service account ([#1313](https://github.com/kubeflow/spark-operator/pull/1313) by [@pdrastil](https://github.com/pdrastil))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.1.5...spark-operator-chart-1.1.6)

## [spark-operator-chart-1.1.5](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.1.5) (2021-07-28)

- Add user defined pod labels ([#1288](https://github.com/kubeflow/spark-operator/pull/1288) by [@pdrastil](https://github.com/pdrastil))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.1.4...spark-operator-chart-1.1.5)

## [spark-operator-chart-1.1.4](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.1.4) (2021-07-25)

- Migrate CRDs from v1beta1 to v1. Add additionalPrinterColumns ([#1298](https://github.com/kubeflow/spark-operator/pull/1298) by [@drazul](https://github.com/drazul))
- Explain "signal: kill" errors during submission ([#1292](https://github.com/kubeflow/spark-operator/pull/1292) by [@zzvara](https://github.com/zzvara))
- fix the invalid repo address ([#1291](https://github.com/kubeflow/spark-operator/pull/1291) by [@william-wang](https://github.com/william-wang))
- add failure context to recordExecutorEvent ([#1280](https://github.com/kubeflow/spark-operator/pull/1280) by [@ImpSy](https://github.com/ImpSy))
- Update pythonVersion to fix example ([#1284](https://github.com/kubeflow/spark-operator/pull/1284) by [@stratus](https://github.com/stratus))
- add crds drift check between chart/ and manifest/ ([#1272](https://github.com/kubeflow/spark-operator/pull/1272) by [@ImpSy](https://github.com/ImpSy))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.1.3...spark-operator-chart-1.1.4)

## [spark-operator-chart-1.1.3](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.1.3) (2021-05-25)

- Allow user to specify service annotation on Spark UI service ([#1264](https://github.com/kubeflow/spark-operator/pull/1264) by [@khorshuheng](https://github.com/khorshuheng))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.1.2...spark-operator-chart-1.1.3)

## [spark-operator-chart-1.1.2](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.1.2) (2021-05-25)

- implement shareProcessNamespace in SparkPodSpec ([#1262](https://github.com/kubeflow/spark-operator/pull/1262) by [@ImpSy](https://github.com/ImpSy))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.1.1...spark-operator-chart-1.1.2)

## [spark-operator-chart-1.1.1](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.1.1) (2021-05-19)

- Enable UI service flag for disabling UI service ([#1261](https://github.com/kubeflow/spark-operator/pull/1261) by [@sairamankumar2](https://github.com/sairamankumar2))
- Add DiDi to who-is-using.md ([#1255](https://github.com/kubeflow/spark-operator/pull/1255) by [@Run-Lin](https://github.com/Run-Lin))
- doc: update who is using page ([#1251](https://github.com/kubeflow/spark-operator/pull/1251) by [@luizm](https://github.com/luizm))
- Add Tongdun under who-is-using  ([#1249](https://github.com/kubeflow/spark-operator/pull/1249) by [@lomoJG](https://github.com/lomoJG))
- [#1239] Custom service port name for spark application UI ([#1240](https://github.com/kubeflow/spark-operator/pull/1240) by [@marcozov](https://github.com/marcozov))
- fix: do not remove preemptionPolicy in patcher when not present ([#1246](https://github.com/kubeflow/spark-operator/pull/1246) by [@HHK1](https://github.com/HHK1))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.1.0...spark-operator-chart-1.1.1)

## [spark-operator-chart-1.1.0](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.1.0) (2021-04-28)

- Updating Spark version from 3.0 to 3.1.1 ([#1153](https://github.com/kubeflow/spark-operator/pull/1153) by [@chethanuk](https://github.com/chethanuk))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.0.10...spark-operator-chart-1.1.0)

## [spark-operator-chart-1.0.10](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.0.10) (2021-04-28)

- Add support for blue/green deployments ([#1230](https://github.com/kubeflow/spark-operator/pull/1230) by [@flupke](https://github.com/flupke))
- Update who-is-using.md: Fossil is using Spark Operator for Production ([#1244](https://github.com/kubeflow/spark-operator/pull/1244) by [@duyet](https://github.com/duyet))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.0.9...spark-operator-chart-1.0.10)

## [spark-operator-chart-1.0.9](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.0.9) (2021-04-23)

- Link to Kubernetes Slack ([#1234](https://github.com/kubeflow/spark-operator/pull/1234) by [@jsoref](https://github.com/jsoref))
- fix: remove preemptionPolicy when priority class name is used ([#1236](https://github.com/kubeflow/spark-operator/pull/1236) by [@HHK1](https://github.com/HHK1))
- Spelling ([#1231](https://github.com/kubeflow/spark-operator/pull/1231) by [@jsoref](https://github.com/jsoref))
- Add support to expose custom ports ([#1205](https://github.com/kubeflow/spark-operator/pull/1205) by [@luizm](https://github.com/luizm))
- Fix the error of hostAliases when there are more than 2 hostnames ([#1209](https://github.com/kubeflow/spark-operator/pull/1209) by [@cdmikechen](https://github.com/cdmikechen))
- remove multiple prefixes for 'p' ([#1210](https://github.com/kubeflow/spark-operator/pull/1210) by [@chaudhryfaisal](https://github.com/chaudhryfaisal))
- added --s3-force-path-style to force path style URLs for S3 objects ([#1206](https://github.com/kubeflow/spark-operator/pull/1206) by [@chaudhryfaisal](https://github.com/chaudhryfaisal))
- Allow custom bucket path ([#1207](https://github.com/kubeflow/spark-operator/pull/1207) by [@bribroder](https://github.com/bribroder))
- fix: Remove priority from the spec when using priority class ([#1203](https://github.com/kubeflow/spark-operator/pull/1203) by [@HHK1](https://github.com/HHK1))
- Fix go get issue with "unknown revision v0.0.0" ([#1198](https://github.com/kubeflow/spark-operator/pull/1198) by [@hongshaoyang](https://github.com/hongshaoyang))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.0.8...spark-operator-chart-1.0.9)

## [spark-operator-chart-1.0.8](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.0.8) (2021-03-07)

- Helm: Put service account into pre-install hook. ([#1155](https://github.com/kubeflow/spark-operator/pull/1155) by [@tandrup](https://github.com/tandrup))
- correct hook annotation for webhook job ([#1193](https://github.com/kubeflow/spark-operator/pull/1193) by [@chaudhryfaisal](https://github.com/chaudhryfaisal))
- Update who-is-using.md ([#1174](https://github.com/kubeflow/spark-operator/pull/1174) by [@tarek-izemrane](https://github.com/tarek-izemrane))
- add Carrefour as adopter and contributor ([#1156](https://github.com/kubeflow/spark-operator/pull/1156) by [@AliGouta](https://github.com/AliGouta))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.0.7...spark-operator-chart-1.0.8)

## [spark-operator-chart-1.0.7](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.0.7) (2021-02-05)

- fix issue #1131 ([#1142](https://github.com/kubeflow/spark-operator/pull/1142) by [@kz33](https://github.com/kz33))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.0.6...spark-operator-chart-1.0.7)

## [spark-operator-chart-1.0.6](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.0.6) (2021-02-04)

- Add Fossil to who-is-using.md ([#1152](https://github.com/kubeflow/spark-operator/pull/1152) by [@duyet](https://github.com/duyet))
- #1143 Helm issues while deploying using argocd ([#1145](https://github.com/kubeflow/spark-operator/pull/1145) by [@TomHellier](https://github.com/TomHellier))
- Include Gojek in who-is-using.md ([#1146](https://github.com/kubeflow/spark-operator/pull/1146) by [@pradithya](https://github.com/pradithya))
- add hostAliases for SparkPodSpec ([#1133](https://github.com/kubeflow/spark-operator/pull/1133) by [@ImpSy](https://github.com/ImpSy))
- Adding MavenCode ([#1128](https://github.com/kubeflow/spark-operator/pull/1128) by [@charlesa101](https://github.com/charlesa101))
- Add MongoDB to who-is-using.md ([#1123](https://github.com/kubeflow/spark-operator/pull/1123) by [@chickenPopcorn](https://github.com/chickenPopcorn))
- update go version to 1.15 and k8s deps to v0.19.6 ([#1119](https://github.com/kubeflow/spark-operator/pull/1119) by [@stpabhi](https://github.com/stpabhi))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.0.5...spark-operator-chart-1.0.6)

## [spark-operator-chart-1.0.5](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.0.5) (2020-12-15)

- Add prometheus containr port name ([#1099](https://github.com/kubeflow/spark-operator/pull/1099) by [@nicholas-fwang](https://github.com/nicholas-fwang))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.0.4...spark-operator-chart-1.0.5)

## [spark-operator-chart-1.0.4](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.0.4) (2020-12-12)

- Upgrade the Chart version to 1.0.4 ([#1113](https://github.com/kubeflow/spark-operator/pull/1113) by [@ordukhanian](https://github.com/ordukhanian))
- Support Prometheus PodMonitor Deployment (#1106) ([#1112](https://github.com/kubeflow/spark-operator/pull/1112) by [@ordukhanian](https://github.com/ordukhanian))
- update executor status if pod is lost while app is still running ([#1111](https://github.com/kubeflow/spark-operator/pull/1111) by [@ImpSy](https://github.com/ImpSy))
- Add scheduler func for clearing batch scheduling on completed ([#1079](https://github.com/kubeflow/spark-operator/pull/1079) by [@nicholas-fwang](https://github.com/nicholas-fwang))
- Add configuration for SparkUI service type ([#1100](https://github.com/kubeflow/spark-operator/pull/1100) by [@jutley](https://github.com/jutley))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.0.3...spark-operator-chart-1.0.4)

## [spark-operator-chart-1.0.3](https://github.com/kubeflow/spark-operator/tree/spark-operator-chart-1.0.3) (2020-12-07)

- Update docs with new helm instructions ([#1105](https://github.com/kubeflow/spark-operator/pull/1105) by [@hagaibarel](https://github.com/hagaibarel))

[Full Changelog](https://github.com/kubeflow/spark-operator/compare/spark-operator-chart-1.0.2...spark-operator-chart-1.0.3)
