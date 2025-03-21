# Changelog

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
