[![Build Status](https://travis-ci.org/GoogleCloudPlatform/spark-on-k8s-operator.svg?branch=master)](https://travis-ci.org/GoogleCloudPlatform/spark-on-k8s-operator.svg?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/GoogleCloudPlatform/spark-on-k8s-operator)](https://goreportcard.com/report/github.com/GoogleCloudPlatform/spark-on-k8s-operator)

**This is not an officially supported Google product.**

## Community

* [Slack](https://kubernetes.slack.com/messages/CALBDHMTL) channel.

## Project Status

**Project status:** *alpha* 

Spark Operator is still under active development. Backward compatibility of the APIs is not guaranteed for alpha releases.

Customization of Spark pods, e.g., mounting arbitrary volumes and setting pod affinity, is currently experimental and implemented using a Kubernetes
[Mutating Admission Webhook](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/), which became beta in Kubernetes 1.9. 
The mutating admission webhook is disabled by default but can be enabled if there are needs for pod customization. Check out the [Quick Start Guide](docs/quick-start-guide.md#using-the-mutating-admission-webhook) 
on how to enable the webhook.

## Prerequisites

* Version >= 1.8 of Kubernetes.
* Version >= 1.9 of Kubernetes if **using the mutating admission webhook for Spark pod customization**.

Spark Operator relies on [garbage collection](https://kubernetes.io/docs/concepts/workloads/controllers/garbage-collection/) support for custom resources that is available in Kubernetes 1.8+ 
and **optionally** the [Mutating Admission Webhook](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/) which is available in Kubernetes 1.9+.

**Due to this [bug](https://github.com/kubernetes/kubernetes/issues/56018) in Kubernetes 1.9 and earlier, CRD objects with
escaped quotes (e.g., `spark.ui.port\"`) in map keys can cause serialization problems in the API server. So please pay
extra attention to make sure no offending escaping is in your `SparkAppliction` CRD objects, particularly if you use 
Kubernetes prior to 1.10.**

## Get Started

Get started quickly with the Spark Operator using the [Quick Start Guide](docs/quick-start-guide.md). 

If you are running the Spark Operator on Google Kubernetes Engine and want to use Google Cloud Storage (GCS) and/or BigQuery for reading/writing data, also refer to the [GCP guide](docs/gcp.md).

For more information, check the [Design](docs/design.md), [API Specification](docs/api.md) and detailed [User Guide](docs/user-guide.md).

## Overview

Spark Operator aims to make specifying and running [Spark](https://github.com/apache/spark) 
applications as easy and idiomatic as running other workloads on Kubernetes. It uses 
[Kubernetes custom resources](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/) 
for specifying, running, and surfacing status of Spark applications. For a complete reference of the custom resource definitions, 
please refer to the [API Definition](docs/api.md). For details on its design, please refer to the [design doc](docs/design.md). 
It requires Spark 2.3 and above that supports Kubernetes as a native scheduler backend.

Spark Operator currently supports the following list of features:

* Supports Spark 2.3 and up.
* Enables declarative application specification and management of applications through custom resources. 
* Automatically runs `spark-submit` on behalf of users for each `SparkApplication` eligible for submission.
* Provides native [cron](https://en.wikipedia.org/wiki/Cron) support for running scheduled applications.
* Supports customization of Spark pods beyond what Spark natively is able to do through the mutating admission webhook, e.g., mounting ConfigMaps and volumes, and setting pod affinity/anti-affinity.
* Supports automatic application re-submission for updated `SparkAppliation` objects with updated specification.
* Supports automatic application restart with a configurable restart policy.
* Supports automatic retries of failed submissions with optional linear back-off.
* Supports mounting local Hadoop configuration as a Kubernetes ConfigMap automatically via `sparkctl`.
* Supports automatically staging local application dependencies to Google Cloud Storage (GCS) via `sparkctl`.

## Motivations

This approach is completely different than the one that has the submission client creates a CRD object. Having externally 
created and managed CRD objects offer the following benefits:
* Things like creating namespaces and setting up RBAC roles and resource quotas represent a separate concern and are better 
done before applications get submitted.
* With the external CRD controller submitting applications on behalf of users, they don't need to deal with the submission 
process and the `spark-submit` command. Instead, the focus is shifted from thinking about commands to thinking about declarative 
YAML files describing Spark applications that can be easily version controlled. 
* Externally created CRD objects make it easier to integrate Spark application submission and monitoring with users' existing 
pipelines and tooling on Kubernetes.
* Internally created CRD objects are good for capturing and communicating application/executor status to the users, but not 
for driver/executor pod configuration/customization as very likely it needs external input. Such external input most likely 
need additional command-line options to get passed in.

Additionally, keeping the CRD implementation outside the Spark repository gives us a lot of flexibility in terms of 
functionality to add to the CRD controller. We also have full control over code review and release process.
