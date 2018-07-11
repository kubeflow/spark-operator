[![Build Status](https://travis-ci.org/GoogleCloudPlatform/spark-on-k8s-operator.svg?branch=master)](https://travis-ci.org/GoogleCloudPlatform/spark-on-k8s-operator.svg?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/GoogleCloudPlatform/spark-on-k8s-operator)](https://goreportcard.com/report/github.com/GoogleCloudPlatform/spark-on-k8s-operator)

**This is not an officially supported Google product.**

## Community

* [Slack](https://kubernetes.slack.com/messages/CALBDHMTL) channel

## Project Status

**Project status:** *alpha* 

Spark Operator is still under active development and has not been extensively tested in production environment yet. Backward compatibility of the APIs is not guaranteed for alpha releases.

Customization of Spark pods, e.g., mounting ConfigMaps and PersistentVolumes is currently experimental and implemented using a Kubernetes
[Initializer](https://kubernetes.io/docs/admin/extensible-admission-controllers/#initializers), which is a Kubernetes **alpha** feature and 
requires a Kubernetes cluster with alpha features enabled. The Initializer can be disabled if there's no need for pod customization or
 if running on an alpha cluster is not desirable. Check out the [Quick Start Guide](docs/quick-start-guide.md#configuration) on how to disable
 the Initializer.

## Prerequisites

* Version >= 1.8 of Kubernetes.

Spark Operator relies on [garbage collection](https://kubernetes.io/docs/concepts/workloads/controllers/garbage-collection/) support for 
custom resources and **optionally** the [Initializers](https://kubernetes.io/docs/admin/extensible-admission-controllers/#initializers) which are in Kubernetes 1.8+.

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
applications as easy and idiomatic as running other workloads on Kubernetes. It uses a 
[CustomResourceDefinition (CRD)](https://kubernetes.io/docs/tasks/access-kubernetes-api/extend-api-custom-resource-definitions/) of 
`SparkApplication` objects for specifying, running, and surfacing status of Spark applications. For a complete reference 
of the API definition of the `SparkApplication` CRD, please refer to [API Definition](docs/api.md). For details on its design, 
please refer to the [design doc](docs/design.md). It requires Spark 2.3 and above that supports Kubernetes as a native scheduler 
backend. Below are some example things that the Spark Operator is able to automate (some are to be implemented):
* Submitting applications on behalf of users so they don't need to deal with the submission process and the `spark-submit` command.
* Mounting user-specified ConfigMaps and volumes into the driver and executor Pods.
* Mounting ConfigMaps carrying Spark or Hadoop configuration files that are to be put into a directory referred to by the 
environment variable `SPARK_CONF_DIR` or `HADOOP_CONF_DIR` into the driver and executor Pods. Example use cases include 
shipping a `log4j.properties` file for configuring logging and a `core-site.xml` file for configuring Hadoop and/or HDFS 
access.
* Creating a `NodePort` service for the Spark UI running on the driver so the UI can be accessed from outside the Kubernetes 
cluster, without needing to use API server proxy or port forwarding.

To make such automation possible, Spark Operator uses the `SparkApplication` CRD and a corresponding CRD controller as well as an 
[initializer](https://kubernetes.io/docs/admin/extensible-admission-controllers/#initializers). The CRD controller setups the 
environment for an application and submits the application to run on behalf of the user, whereas the initializer handles customization 
of the Spark Pods. It also supports running Spark applications on standard [cron](https://en.wikipedia.org/wiki/Cron) schedules using 
the `ScheduledSparkApplication` CRD and the corresponding CRD controller.

## Features

Spark Operator currently supports the following list of features:

* This branch specifically supports [Spark 2.2 fork with Kubernetes support](https://github.com/apache-spark-on-k8s/spark) to add support for PySpark. This branch will be deprecated once PySpark support is added in Spark 2.4.
* Enables declarative application specification and management of applications by manipulating `SparkApplication` resources. 
* Automatically runs `spark-submit` on behalf of users for each `SparkApplication` eligible for submission.
* Provides native [cron](https://en.wikipedia.org/wiki/Cron) support for running scheduled applications.
* Supports automatic application re-submission for updated `SparkAppliation` objects with updated specification.
* Supports automatic application restart with a configurable restart policy.
* Supports automatic retries of failed submissions with optional linear back-off.
* Supports mounting user-specified ConfigMaps and volumes (currently through the Initializer).
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
