# Spark Operator ROADMAP

## 2026

- [ ] Improve support for Spark Connect ([#2927](https://github.com/kubeflow/spark-operator/issues/2927), [#2957](https://github.com/kubeflow/spark-operator/issues/2957), [#2958](https://github.com/kubeflow/spark-operator/issues/2958), [#2959](https://github.com/kubeflow/spark-operator/issues/2959))
- [ ] SparkCluster CRD ([#2744](https://github.com/kubeflow/spark-operator/issues/2744))
- [ ] Improve controller performance
- [ ] REST API for submitting jobs ([#2972](https://github.com/kubeflow/spark-operator/issues/2972))
- [ ] Watching namespaces based on labels ([#2479](https://github.com/kubeflow/spark-operator/issues/2479), [#2913](https://github.com/kubeflow/spark-operator/issues/2913))
- [ ] Fenced, progress-preserving driver restarts ([#3040](https://github.com/kubeflow/spark-operator/issues/3040))
- [ ] Notebooks integration
- [ ] Security issues ([#3143](https://github.com/kubeflow/spark-operator/issues/3143), [#2963](https://github.com/kubeflow/spark-operator/issues/2963), [#3006](https://github.com/kubeflow/spark-operator/issues/3006))
- [ ] Filesystem certificates ([#3165](https://github.com/kubeflow/spark-operator/issues/3165))
- [ ] Gateway API support for Spark UI ([#3119](https://github.com/kubeflow/spark-operator/issues/3119))
- [ ] Workload-aware Scheduling ([#2962](https://github.com/kubeflow/spark-operator/issues/2962))
- [ ] Support for Dynamic Resource Allocation ([#2858](https://github.com/kubeflow/spark-operator/issues/2858))
- [ ] Support for MultiKueue ([#2879](https://github.com/kubeflow/spark-operator/issues/2879))
- [ ] Doc improvement ([#2987](https://github.com/kubeflow/spark-operator/issues/2987))

## 2025

- [x] A new CR to support Spark Connect ([#2569](https://github.com/kubeflow/spark-operator/pull/2569))
- [x] Cert manager support ([#1178](https://github.com/kubeflow/spark-operator/issues/1178))
- [ ] Improve controller performance
- [ ] Notebooks integration
- [x] [Kubeflow SDK](https://github.com/kubeflow/sdk) integration
- [ ] REST API for submitting jobs
- [ ] Deprecation of webhook by moving all functionality into the pod template ([#2502](https://github.com/kubeflow/spark-operator/issues/2502))
- [ ] Watching namespaces based on labels
- [x] A web UI for visibility into currently running applications
- [ ] Doc improvement

## 2024

- [x] Pod template support ([#2101](https://github.com/kubeflow/spark-operator/issues/2101))
- [x] Spark Connect Support ([#1801](https://github.com/kubeflow/spark-operator/issues/1801))
- [x] Support Yunikorn as a batch scheduler ([#2107](https://github.com/kubeflow/spark-operator/pull/2107))
- [x] Improve test coverage to improve the confidence in releases, particularly with e2e tests
