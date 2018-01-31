# Spark Operator Design

Table of Contents
=================
- [Spark Operation Design](#spark-operation-design)
- [Table of Contents](#table-of-contents)
    - [Introduction](#introduction)
    - [Why Spark Operator?](#why-spark-operator)
    - [Components](#components)
        - [Controller, Submission Runner, and Spark Pod Monitor](#controller-submission-runner-and-spark-pod-monitor)
        - [Spark Pod Initializer](#spark-pod-initializer)
        - [Command-line Tool: Sparkctl](#command-line-tool-sparkctl)

## Introduction

The Spark Operator follows the recent trend of leveraging the [operator](https://coreos.com/blog/introducing-operators.html) pattern for managing the life cycle of Spark applications on a Kubernetes cluster. The Spark Operator allows Spark applications to be specified in a declarative manner (e.g., in a YAML file) and run without the need to deal with the spark submission process. It also enables status of Spark applications to be tracked and presented idiomatically like other types of workloads on Kubernetes. This document discusses the design and architecture of the Spark Operator. For documentation of the [CustomResourceDefinition](https://kubernetes.io/docs/concepts/api-extension/custom-resources/) for specification of Spark applications, please refer to [API Definition](api.md)    

## Why Spark Operator?

## Components

The Spark Operator consists of:
* a `SparkApplication` controller that watches events of creation, updates, and deletion of 
`SparkApplication` objects and acts on the watch events,
* a *submission runner* that runs `spark-submit` for submissions received from the controller,
* a *Spark pod monitor* that watches for Spark pods and sends pod status updates to the controller,
* a Spark pod [initializer](https://kubernetes.io/docs/admin/extensible-admission-controllers/#initializers)that performs initialization tasks on Spark driver and executor pods based on the annotations on the pods added by the controller,
* and also a command-line tool named `sparkctl` for working with the operator. 

The following diagram shows how different components interact and work together.

![Architecture Diagram](architecture-diagram.png)

Specifically, a user uses the `sparkctl` (or `kubectl`) to create a `SparkApplication` object. The `SparkApplication` controller receives the object through a watcher from the API server, creates a submission carrying the `spark-submit` arguments, and sends the submission to the *submission runner*. The submission runner submits the application to run and creates the driver pod of the application. Upon starting, the driver pod creates the executor pods. While the application is running, the *Spark pod monitor* watches the pods of the application and sends status updates of the pods back to the controller, which then updates the status of the application accordingly. 

### Controller, Submission Runner, and Spark Pod Monitor

The `SparkApplication` controller, or CRD controller in short, watches events of creation, updates, and deletion of `SparkApplication` objects in any namespaces in a Kubernetes cluster, and acts on the watch events. When it receives a new `SparkApplication` object, it prepares a submission and sends the submission to the submission runner, which actually submits the application to run in the Kubernetes cluster. The submission includes the list of arguments for the `spark-submit` command. The submission runner has a configurable number of workers for submitting applications to run in the cluster. 

The controller is also responsible for updating the status of a `SparkApplication` object with the help of the Spark pod monitor, which watches Spark pods and update the `SparkApplicationStatus` field of corresponding `SparkApplication` objects based on the status of the pods. The Spark pod monitor watches events of creation, updates, and deletion of Spark pods, creates status update messages based on the status of the pods, and sends the messages to the controller to process. The controller maintains the collection of `SparkApplication` objects of running applications and update the status of them based on the messages received.

As part of preparing a submission for a newly created `SparkApplication` object, the controller parses the object and adds configuration options for adding certain annotations to the driver and executor pods of the application. The annotations are later used by the Spark pod initializer to configure the pods before they start to run. For example,if a Spark application needs a certain Kubernetes ConfigMap to be mounted into the driver and executor pods, the controller adds an annotation that specifies the name of the ConfigMap to mount. Later the Spark pod initializer sees the annotation on the pods and mount the ConfigMap to the pods.

### Spark Pod Initializer

NOTE: it is planned to migrate from using an Initializer to instead using an [external admission webhook](https://kubernetes.io/docs/admin/extensible-admission-controllers/#external-admission-webhooks).

The Spark pod initializer is responsible for configuring pods of Spark applications based on certain annotations on the pods added by the CRD controller. All Spark pod customization needs except for those natively support by Spark on Kubernetes are handled by the initializer. The following annotations for pod customization are supported:

|Annotation|Value|Note|
| ------------- | ------------- | ------------- |
|`sparkoperator.k8s.io/sparkConfigMap`|Name of the Kubernetes ConfigMap storing Spark configuration files (to which `SPARK_CONF_DIR` applies)|Environment variable `SPARK_CONF_DIR` is set to point to the mount path.|
|`sparkoperator.k8s.io/hadoopConfigMap`|Name of the Kubernetes ConfigMap storing Hadoop configuration files (to which `HADOOP_CONF_DIR` applies)|Environment variable `HADOOP_CONF_DIR` is set to point to the mount path.|
|`sparkoperator.k8s.io/configMap.[ConfigMapName]`|Mount path of the ConfigMap named `ConfigMapName`||
|`sparkoperator.k8s.io/GCPServiceAccount.[SeviceAccountSecretName]`|Mount path of the secret storing GCP service account credentials (typically a JSON key file) named `SeviceAccountSecretName`|Environment variable `GOOGLE_APPLICATION_CREDENTIALS` is set to point to the mounted JSON key file.|

### Command-line Tool: Sparkctl 

[sparkctl](../sparkctl/README.md) is a command-line tool for working with the operator. It supports creating a `SparkApplication`object from a YAML file, listing existing `SparkApplication` objects, checking status of a `SparkApplication`, forwarding from a local port to the remote port on which the Spark driver runs, and deleting a `SparkApplication` object. For more details on `sparkctl`, please refer to [README](../sparkctl/README.md). 