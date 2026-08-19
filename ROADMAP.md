# Spark Operator Roadmap

This document summarizes active project themes and points contributors to the
places where roadmap work is planned and tracked. It is intentionally not tied
to a calendar year so it does not become stale between releases.

## Planning and Tracking

Roadmap items are discussed and refined through GitHub issues, pull requests,
and the Kubeflow Spark Operator community channels. The most up-to-date view of
planned and in-progress work is the set of open issues and pull requests:

- [Open issues](https://github.com/kubeflow/spark-operator/issues)
- [Open pull requests](https://github.com/kubeflow/spark-operator/pulls)
- [Contributing guide](CONTRIBUTING.md)

If you want to work on an item, please comment on the relevant issue before
opening a pull request so maintainers can confirm the approach.

## Current Focus Areas

- Improve controller performance and memory usage.
- Improve webhook and controller reliability.
- Improve namespace watching and namespace selector behavior.
- Continue Spark Connect support and readiness improvements.
- Explore integration points with Kubeflow components and SDKs.
- Improve APIs for submitting and managing Spark applications.
- Improve observability for running Spark applications.
- Improve documentation, examples, and contributor onboarding.

## Completed Highlights

- Spark Connect custom resource support
  ([#2569](https://github.com/kubeflow/spark-operator/pull/2569)).
- Cert-manager support
  ([#1178](https://github.com/kubeflow/spark-operator/issues/1178)).
- Pod template support
  ([#2101](https://github.com/kubeflow/spark-operator/issues/2101)).
- Support for YuniKorn as a batch scheduler
  ([#2107](https://github.com/kubeflow/spark-operator/pull/2107)).
- Expanded test coverage, including end-to-end tests.
