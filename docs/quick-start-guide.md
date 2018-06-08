# Quick Start Guide

For a more detailed guide on how to use, compose, and work with `SparkAppliction`s, please refer to the
[User Guide](user-guide.md). If you are running the Spark Operator on Google Kubernetes Engine and want to use Google Cloud Storage (GCS) and/or BigQuery for reading/writing data, also refer to the [GCP guide](gcp.md).

## Table of Contents
1. [Installation](#installation)
2. [Configuration](#configuration)
3. [Upgrade](#upgrade)
4. [Running the Examples](#running-the-examples)
5. [Using the Initializer](#using-the-initializer)
6. [Build](#build)

## Installation

To install the Spark Operator on a Kubernetes cluster, run the following command:

```bash
$ kubectl apply -f manifest/
```

This will create a namespace `sparkoperator`, setup RBAC for the Spark Operator to run in the namespace, and create a
Deployment named `sparkoperator` in the namespace.

The [initializer](design.md#spark-pod-initializer) is disabled by default using the Spark Operator manifest at
`manifest/spark-operator.yaml`. It can be enabled by removing the flag `-enable-initializer=false` or setting it to
`true`, and running `kubectl apply -f manifest/spark-operator.yaml`.

Due to a [known issue](https://cloud.google.com/kubernetes-engine/docs/how-to/role-based-access-control#defining_permissions_in_a_role)
in GKE, you will need to first grant yourself cluster-admin privileges before you can create custom roles and role
bindings on a GKE cluster versioned 1.6 and up.

```bash
$ kubectl create clusterrolebinding <user>-cluster-admin-binding --clusterrole=cluster-admin --user=<user>@<domain>
```

Now you should see the Spark Operator running in the cluster by checking the status of the Deployment.

```bash
$ kubectl describe deployment sparkoperator -n sparkoperator

```

## Configuration

Spark Operator is typically deployed and run using `manifest/spark-operator.yaml` through a Kubernetes `Deployment`.
However, users can still run it outside a Kubernetes cluster and make it talk to the Kubernetes API server of a cluster
by specifying path to `kubeconfig`, which can be done using the `-kubeconfig` flag.

Spark Operator uses multiple workers in the `SparkApplication` controller, the initializer, and the submission runner.
The number of worker threads to use in the three places are controlled using command-line flags `-controller-threads`,
`-initializer-threads` and `-submission-threads`, respectively. The default values for the flags are 10, 10, and 3,
respectively.

Spark Operator enables cache resynchronization so periodically the informers used by the operator will re-list existing
objects it manages and re-trigger resource events. The resynchronization interval in seconds can be configured using the
flag `-resync-interval`, with a default value of 30 seconds.

Spark on Kubernetes needs DNS resolution for the FQDN of the driver pod used by executors to connect to the driver. By
default, Spark Operator checks the presence of `kube-dns` in the cluster and fails fast if it cannot find it. The check 
can be disabled if desirable by setting the flag `-check-dns=false`.

By default, Spark Operator will install the
[CustomResourceDefinitions](https://kubernetes.io/docs/tasks/access-kubernetes-api/extend-api-custom-resource-definitions/)
for the custom resources it managers. This can be disabled by setting the flag `-install-crds=false.`.

The initializer is an **optional** component and can be enabled or disabled using the `-enable-initializer` flag, which
defaults to `true`. Since the initializer is an alpha feature, it won't function in Kubernetes clusters without alpha
features enabled. In this case, it can be disabled by adding the argument `-enable-initializer=false` to
[spark-operator.yaml](../manifest/spark-operator.yaml).

By default, Spark Operator will manage custom resource objects of the managed CRD types for the whole cluster.
It can be configured to manage only the custom resource objects in a specific namespace with the flag `-namespace=<namespace>`

## Upgrade

To upgrade the Spark Operator, e.g., to use a newer version container image with a new tag, run the following command
with the updated YAML file for the Deployment of the Spark Operator:

```bash
$ kubectl apply -f manifest/spark-operator.yaml

```

## Running the Examples

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
  submissionTime: 2018-02-20T23:32:27Z
```

To check events for the `SparkApplication` object, run the following command:

```bash
$ kubectl describe sparkapplication spark-pi

```

This will show the events similarly to the following:

```
Events:
  Type    Reason                      Age   From            Message
  ----    ------                      ----  ----            -------
  Normal  SparkApplicationAdded       5m    spark-operator  SparkApplication spark-pi was added, enqueued it for submission
  Normal  SparkApplicationTerminated  4m    spark-operator  SparkApplication spark-pi terminated with state: COMPLETED
```

The Spark Operator submits the Spark Pi example to run once it receives an event indicating the `SparkApplication`
object was added.

## Using the Initializer

The Spark Operator comes with an optional [initializer](design.md#spark-pod-initializer) for customizing Spark driver
and executor pods based on the specification in `SparkApplication` objects, e.g., mounting user-specified ConfigMaps.
The initializer works independently with or without the [CRD controller](design.md#the-crd-controller). It works by
looking for certain custom annotations on Spark driver and executor Pods to perform its tasks. The annotations are
added by the CRD controller automatically based on application specifications in the `SparkApplication`objects.
Alternatively, to use the initializer without the controller, the needed annotations can be added manually to the driver
and executor Pods using the following Spark configuration properties when submitting your Spark applications using the
`spark-submit` script.

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

## Build

In case you want to build the Spark Operator from the source code, e.g., to test a fix or a feature you write, you can do so following the instructions below.

The easiest way to build without worrying about dependencies is to just build the Dockerfile.

```bash
$ docker build -t <image-tag> .
```

If you'd like to build/test the spark-operator locally, follow the instructions below:

```bash
$ mkdir -p $GOPATH/src/k8s.io
$ cd $GOPATH/src/k8s.io
$ git clone git@github.com:GoogleCloudPlatform/spark-on-k8s-operator.git
```

The Spark Operator uses [dep](https://golang.github.io/dep/) for dependency management. Please install `dep` following
the instruction on the website if you don't have it available locally. To install the dependencies, run the following
command:

```bash
$ dep ensure
```

To update the dependencies, run the following command. (You can skip this unless you know there's a dependency that needs updating):

```bash
$ dep ensure -update
```

Before building the Spark Operator the first time, run the following commands to get the required Kubernetes code
generators:

```bash
$ go get -u k8s.io/code-generator/cmd/client-gen
$ go get -u k8s.io/code-generator/cmd/deepcopy-gen
$ go get -u k8s.io/code-generator/cmd/defaulter-gen
```

To update the auto-generated code, run the following command. (This step is only required if the CRD types have been changed):

```bash
$ go generate
```

You can verify the current auto-generated code is up to date with:

```bash
$ hack/verify-codegen.sh
```

To build the Spark Operator, run the following command:

```bash
$ go build -o spark-operator
```

To run unit tests, run the following command:

```bash
$ go test ./...
```
