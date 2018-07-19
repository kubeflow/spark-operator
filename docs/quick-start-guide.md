# Quick Start Guide

For a more detailed guide on how to use, compose, and work with `SparkApplication`s, please refer to the
[User Guide](user-guide.md). If you are running the Spark Operator on Google Kubernetes Engine and want to use Google Cloud Storage (GCS) and/or BigQuery for reading/writing data, also refer to the [GCP guide](gcp.md).

## Table of Contents
1. [Installation](#installation)
2. [Configuration](#configuration)
3. [Upgrade](#upgrade)
4. [Running the Examples](#running-the-examples)
5. [Using the Mutating Admission Webhook](#using-the-mutating-admission-webhook)
6. [Build](#build)

## Installation

Before installing the Spark Operator, run the following command to setup the environment for the operator:

```bash
$ kubectl apply -f manifest/spark-operator-rbac.yaml
$ kubectl apply -f manifest/spark-rbac.yaml
```

This will create a namespace `sparkoperator`, setup RBAC for the Spark Operator to run in the namespace. It will also
setup RBAC for driver pods of your Spark applications to be able to manipulate executor pods. 

The Spark Operator optionally uses a [Mutating Admission Webhook](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/)
for Spark pod customization. To install the Spark Operator **without** the mutating admission webhook on a Kubernetes cluster, run the following command:

```bash
$ kubectl apply -f manifest/spark-operator.yaml
```

This will create a Deployment named `sparkoperator` in namespace `sparkoperator`.

Alternatively, follow [Using the Mutating Admission Webhook](#using-the-mutating-admission-webhook) for instructions on
how to install the operator with the mutating admission webhook.

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

Spark Operator uses multiple workers in the `SparkApplication` controller and and the submission runner.
The number of worker threads to use in the three places are controlled using command-line flags `-controller-threads` 
and `-submission-threads`, respectively. The default values for the flags are 10 and 3, respectively.

Spark Operator enables cache resynchronization so periodically the informers used by the operator will re-list existing
objects it manages and re-trigger resource events. The resynchronization interval in seconds can be configured using the
flag `-resync-interval`, with a default value of 30 seconds.

By default, Spark Operator will install the
[CustomResourceDefinitions](https://kubernetes.io/docs/tasks/access-kubernetes-api/extend-api-custom-resource-definitions/)
for the custom resources it managers. This can be disabled by setting the flag `-install-crds=false.`.

The mutating admission webhook is an **optional** component and can be enabled or disabled using the `-enable-webhook` flag, 
which defaults to `false`.

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

## Using the Mutating Admission Webhook

The Spark Operator comes with an optional mutating admission webhook for customizing Spark driver and executor pods based 
on the specification in `SparkApplication` objects, e.g., mounting user-specified ConfigMaps and volumes, and setting
pod affinity/anti-affinity.

The webhook requires a X509 certificate for TLS for pod admission requests and responses between the Kubernetes API 
server and the webhook server running inside the operator. For that, the certificate and key files must be accessible
by the webhook server and a Kubernetes secret can be used to store the files.

The Spark Operator ships with a tool at `hack/gencerts.sh` for generating the CA and server certificate and putting the 
certificate and key files into a secret. Running `hack/gencerts.sh` will generate a CA certificate and a certificate
for the webhook server signed by the CA, and create a secret named `spark-webhook-certs` in namespace `sparkoperator`. 
This secret will be mounted into the Spark Operator pod.  

With the secret storing the certificate and key files available, run the following command to install the Spark Operator
with the mutating admission webhook:

```bash
$ kubectl apply -f manifest/spark-operator-with-webhook.yaml
```

This will create a Deployment named `sparkoperator` and a Service named `spark-webhook` for the webhook in namespace 
`sparkoperator`.

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
$ GOOS=linux go build -o spark-operator
```

To run unit tests, run the following command:

```bash
$ go test ./...
```
