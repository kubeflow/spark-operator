[![Build Status](https://travis-ci.org/GoogleCloudPlatform/spark-on-k8s-operator.svg?branch=master)](https://travis-ci.org/GoogleCloudPlatform/spark-on-k8s-operator.svg?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/GoogleCloudPlatform/spark-on-k8s-operator)](https://goreportcard.com/report/github.com/GoogleCloudPlatform/spark-on-k8s-operator)

**This is not an official Google product.**

## Table of Contents
1. [Project Status](#project-status)
2. [Prerequisites](#prerequisites)
3. [Overview](#overview)
4. [Features](#features)
5. [Installation](#installation)
   1. [Build](#build)
   2. [Deployment](#deployment)
   3. [Configuration](#configuration)
   4. [Running the Example](#running-the-example)
6. [Using the Initializer](#using-the-initializer)

## Project Status

**Project status:** *alpha* 

Spark Operator is still under active development and has not been extensively tested yet. Use at your own risk. Backward-compatibility is not supported for alpha releases.

## Prerequisites

* Version >= 1.8 of Kubernetes.

Spark Operator relies on [Initializers](https://kubernetes.io/docs/admin/extensible-admission-controllers/#initializers) 
and [garbage collection](https://kubernetes.io/docs/concepts/workloads/controllers/garbage-collection/) support for 
custom resources which are in Kubernetes 1.8+.

## Overview

Spark Operator aims to make specifying and running [Spark](https://github.com/apache/spark) 
applications as easy and idiomatic as running other workloads on Kubernetes. It uses a 
[CustomResourceDefinition (CRD)](https://kubernetes.io/docs/tasks/access-kubernetes-api/extend-api-custom-resource-definitions/) of 
`SparkApplication` objects for specifying, running, and surfacing status of Spark applications. For a complete reference 
of the API definition of the `SparkApplication` CRD, please refer to [API Definition](docs/api.md). For details on its design, 
please refer to the [design doc](docs/design.md). It requires Spark 2.3 and above that supports Kubernetes as a native scheduler 
backend. Below are some example things that the Spark Operator is able to automate:
* Submitting applications on behalf of users so they don't need to deal with the submission process and the `spark-submit` command.
* Mounting user-specified ConfigMaps into the driver and executor Pods.
* Mounting ConfigMaps carrying Spark or Hadoop configuration files that are to be put into a directory referred to by the 
environment variable `SPARK_CONF_DIR` or `HADOOP_CONF_DIR` into the driver and executor Pods. Example use cases include 
shipping a `log4j.properties` file for configuring logging and a `core-site.xml` file for configuring Hadoop and/or HDFS 
access.
* Creating a `NodePort` service for the Spark UI running on the driver so the UI can be accessed from outside the Kubernetes 
cluster, without needing to use API server proxy or port forwarding.

To make such automation possible, Spark Operator uses the `SparkApplication` CRD and a corresponding CRD controller as well as an 
[initializer](https://kubernetes.io/docs/admin/extensible-admission-controllers/#initializers). The CRD controller setups the 
environment for an application and submits the application to run on behalf of the user, whereas the initializer handles customization 
of the Spark Pods.

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

## Features

Spark Operator currently supports the following list of features:

* Supports Spark 2.3 and up.
* Supports automatic application submission for newly added `SparkApplication` objects.
* Supports automatic application re-submission for updated `SparkAppliation` objects with updated specification.
* Supports automatic application restart with a configurable restart policy.
* Supports automatic retries of failed submissions with optional linear back-off.
* Supports mounting local Hadoop configuration as a Kubernetes ConfigMap automatically via `sparkctl`.
* Supports automatically staging local application dependencies to Google Cloud Storage (GCS) via `sparkctl`.

The following list of features is planned:

* Supports automatically staging local application dependencies to HTTP servers and S3.
* Supports exporting Kubernetes events to Stackdriver.
* Supports automatic scale up and down when the number of executor instances changes.
* Supports automatically copying application logs to a central place, e.g., a GCS bucket, for bookkeeping, post-run checking, and analysis.
* Supports automatically creating namespaces and setting up RBAC roles and quotas, and running users' applications in separate 
namespaces for better resource isolation and quota management. 

## Installation

### Build

To get Spark Operator, run the following commands:

```bash
$ make -p $GOPATH/src/k8s.io
$ cd $GOPATH/src/k8s.io
$ git clone git@github.com:GoogleCloudPlatform/spark-on-k8s-operator.git
```

Spark Operator uses [dep](https://golang.github.io/dep/) for dependency management. Please install `dep` following the 
instruction on the website if you don't have it available locally. To install the dependencies, run the following command:

```bash
$ dep ensure
```  
To update the dependencies, run the following command:

```bash
$ dep ensure -update
```

Before building the Spark operator the first time, run the following commands to get the required Kubernetes code generators:

```bash
$ go get -u k8s.io/code-generator/cmd/deepcopy-gen
$ go get -u k8s.io/code-generator/cmd/defaulter-gen
```

To build the Spark operator, run the following command:

```bash
$ make build
```

To additionally build a Docker image of the Spark operator, run the following command:

```bash
$ make image-tag=<image tag> image
```

To further push the Docker image to Docker hub, run the following command:
console

```bash
$ make image-tag=<image tag> push
```

### Deployment

To deploy the Spark operator, run the following command:

```bash
$ kubectl create -f manifest/
```

This will create a namespace `sparkoperator`, setup RBAC for the Spark Operator to run in the namespace, and create a Deployment named 
`sparkoperator` in the namespace.

Due to a [known issue](https://cloud.google.com/kubernetes-engine/docs/how-to/role-based-access-control#defining_permissions_in_a_role) 
in GKE, you will need to first grant yourself cluster-admin privileges before you can create custom roles and role bindings on a 
GKE cluster versioned 1.6 and up.

```bash
$ kubectl create clusterrolebinding <user>-cluster-admin-binding --clusterrole=cluster-admin --user=<user>@<domain>
```

### Configuration

Spark Operator is typically deployed and run using `manifest/spark-operator.yaml` through a Kubernetes `Deployment`. 
However, users can still run it outside a Kubernetes cluster and make it talk to the Kubernetes API server of a cluster 
by specifying path to `kubeconfig`, which can be done using the `--kubeconfig` flag. 

Spark Operator uses multiple workers in the `SparkApplication` controller, the initializer, and the submission runner. The number of worker 
threads to use in the three places are controlled using command-line flags `--controller-threads`, `--initializer-threads` and `--submission-threads`, 
respectively. The default values for the flags are 10, 10, and 3, respectively.

The initializer is an optional component and can be enabled or disabled using the `--enable-initializer` flag, which defaults to `true`. 

### Running the Example

To run the Spark Pi example, run the following command:

```bash
$ kubectl apply -f examples/spark-pi.yaml
```

This will create a `SparkApplication` object named `spark-pi`. Check the object by running the following command:

```bash
$ kubectl get sparkapplications spark-pi -o=yaml
```

This will show something similar to the following:

```yaml
apiVersion: sparkoperator.k8s.io/v1alpha1
kind: SparkApplication
metadata:
  ...
spec:
  deps: {}
  driver:
    coreLimit: 200m
    cores: 0.1
    labels:
      version: 2.3.0
    memory: 512m
    serviceAccount: spark
  executor:
    cores: 1
    instances: 1
    labels:
      version: 2.3.0
    memory: 512m
  image: gcr.io/ynli-k8s/spark:v2.3.0
  mainApplicationFile: local:///opt/spark/examples/jars/spark-examples_2.11-2.3.0.jar
  mainClass: org.apache.spark.examples.SparkPi
  mode: cluster
  restartPolicy: Never
  type: Scala
status:
  appId: spark-pi-2402118027
  applicationState:
    state: COMPLETED
  completionTime: 2018-02-20T23:33:55Z
  driverInfo:
    podName: spark-pi-83ba921c85ff3f1cb04bef324f9154c9-driver
    webUIAddress: 35.192.234.248:31064
    webUIPort: 31064
    webUIServiceName: spark-pi-2402118027-ui-svc
  executorState:
    spark-pi-83ba921c85ff3f1cb04bef324f9154c9-exec-1: COMPLETED
```

## Using the Initializer

The initializer works independently with or without the CRD controller. The initializer looks for certain custom 
annotations on Spark driver and executor Pods to perform its tasks. To use the initializer without leveraging the CRD, 
simply add the needed annotations to the driver and/or executor Pods using the following Spark configuration properties 
when submitting your Spark applications through the `spark-submit` script.

```
--conf spark.kubernetes.driver.annotations.[AnnotationName]=value
--conf spark.kubernetes.executor.annotations.[AnnotationName]=value
```  

Currently the following annotations are supported:

|Annotation|Value|
| ------------- | ------------- |
|`sparkoperator.k8s.io/sparkConfigMap`|Name of the Kubernetes ConfigMap storing Spark configuration files (to which `SPARK_CONF_DIR` applies)|
|`sparkoperator.k8s.io/hadoopConfigMap`|Name of the Kubernetes ConfigMap storing Hadoop configuration files (to which `HADOOP_CONF_DIR` applies)|
|`sparkoperator.k8s.io/configMap.[ConfigMapName]`|Mount path of the ConfigMap named `ConfigMapName`|
|`sparkoperator.k8s.io/GCPServiceAccount.[SeviceAccountSecretName]`|Mount path of the secret storing GCP service account credentials (typically a JSON key file) named `SeviceAccountSecretName`|

When using the `SparkApplication` CRD to describe a Spark appliction, the annotations are automatically added by the CRD controller. 
`manifest/spark-application-example-gcp-service-account.yaml` shows an example `SparkApplication` with a user-specified GCP service 
account credential secret named `gcp-service-account` that is to be mounted into both the driver and executor containers. 
The `SparkApplication` controller automatically adds the annotation `sparkoperator.k8s.io/GCPServiceAccount.gcp-service-account=/mnt/secrets` 
to the driver and executor Pods. The initializer sees the annotation and automatically adds a secret volume into the pods.   
