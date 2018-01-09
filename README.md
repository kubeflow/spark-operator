[![Go Report Card](https://goreportcard.com/badge/github.com/GoogleCloudPlatform/spark-on-k8s-operator)](https://goreportcard.com/report/github.com/GoogleCloudPlatform/spark-on-k8s-operator)

**This is not an official Google product.**

# Spark Operator

Spark Operator is an experimental project aiming to make specifying and running [Spark](https://github.com/apache/spark) 
applications as easy and idiomatic as running other workloads on Kubernetes. It requires Spark 2.3 and above that supports 
Kubernetes as a native scheduler backend. Below are some example things that the Spark Operator is able to automate``:
* Submitting applications on behalf of users so they don't need to deal with the submission process and the `spark-submit` command.
* Mounting user-specified secrets into the driver and executor Pods.
* Mounting user-specified ConfigMaps into the driver and executor Pods.
* Mounting ConfigMaps carrying Spark or Hadoop configuration files that are to be put into a directory referred to by the 
environment variable `SPARK_CONF_DIR` or `HADOOP_CONF_DIR` into the driver and executor Pods. Example use cases include 
shipping a `log4j.properties` file for configuring logging and a `core-site.xml` file for configuring Hadoop and/or HDFS 
access.
* Creating a `NodePort` service for the Spark UI running on the driver so the UI can be accessed from outside the Kubernetes 
cluster, without needing to use API server proxy or port forwarding.
* Copying application logs to a central place, e.g., a GCS bucket, for bookkeeping, post-run checking, and analysis.
* Automatically creating namespaces and setting up RBAC roles and quotas, and running users' applications in separate 
namespaces for better resource isolation and quota management. 

To make such automation possible, Spark Operator uses the Kubernetes [CustomResourceDefinition (CRD)](https://kubernetes.io/docs/tasks/access-kubernetes-api/extend-api-custom-resource-definitions/) and a corresponding CRD controller as well as an [initializer](https://kubernetes.io/docs/admin/extensible-admission-controllers/#initializers). The CRD controller setups the environment for an application and submits the application to run on behalf of the user, whereas the initializer handles customization of the Spark Pods.

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

## Current Status

Currently both the `SparkApplication` CRD controller and the pod initializer work.  

## Build and Installation

Spark Operator uses [Glide](http://glide.sh/) for dependency management. Please install Glide following the instruction on 
the website if you don't have it available locally. To install the dependencies, run the following command:

```bash
glide install --strip-vendor
```  

Before building the Spark operator the first time, run the following commands to get the required Kubernetes code generators:

```bash
go get -u k8s.io/code-generator/cmd/deepcopy-gen
go get -u k8s.io/code-generator/cmd/defaulter-gen
```

To build the Spark operator, run the following command:

```bash
make build
```

To additionally build a Docker image of the Spark operator, run the following command:

```bash
make image-tag=<image tag> image
```

To further push the Docker image to Docker hub, run the following command:
console

```bash
make image-tag=<image tag> push
```

## Deploying Spark Operator

To deploy the Spark operator, run the following command:

```bash
kubectl create -f manifest/spark-operator.yaml 
```

## Configuring Spark Operator

Spark Operator is typically deployed and run using `manifest/spark-operator.yaml` through a Kubernetes `Deployment`. 
However, users can still run it outside a Kubernetes cluster and make it talk to the Kubernetes API server of a cluster 
by specifying path to `kubeconfig`, which can be done using the `--kubeconfig` flag. 

Spark Operator uses multiple workers in the initializer controller and the submission runner. The number of worker 
threads to use in the two places are controlled using command-line flags `--initializer-threads` and `--submission-threads`, 
respectively. The default values for both flags are 10 and 3, respectively.

## Running the Example Spark Application

To run the example Spark application, run the following command:

```bash
kubectl create -f manifest/spark-application-example.yaml
```

This will create a `SparkApplication` object named `spark-app-example`. Check the object by running the following command:

```bash
kubectl get sparkapplications spark-app-example -o=yaml
```

This will show something similar to the following:

```yaml
apiVersion: spark-operator.k8s.io/v1alpha1
kind: SparkApplication
metadata:
  ...
spec:
  deps: {}
  driver:
    cores: "0.1"
    image: liyinan926/spark-driver:v2.3.0
  executor:
    image: liyinan926/spark-executor:v2.3.0
    instances: 1
    memory: 512m
  mainApplicationFile: local:///opt/spark/examples/jars/spark-examples_2.11-2.3.0-SNAPSHOT.jar
  mainClass: org.apache.spark.examples.SparkPi
  mode: cluster
  submissionByUser: false
  type: Scala
status:
  appId: spark-app-example-666247082
  applicationState:
    errorMessage: ""
    state: COMPLETED
  completionTime: 2018-01-09T23:19:59Z
  driverInfo:
    podName: spark-app-example-a751930f68d23676aa1f0c2cee31f083-driver
    webUIAddress: 35.225.33.10:30569
    webUIPort: 30569
    webUIServiceName: spark-app-example-ui-666247082
  executorState:
    spark-app-example-a751930f68d23676aa1f0c2cee31f083-exec-1: COMPLETED
  submissionTime: 2018-01-09T23:19:43Z
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
|`spark-operator.k8s.io/sparkConfigMap`|Name of the Kubernetes ConfigMap storing Spark configuration files (to which `SPARK_CONF_DIR` applies)|
|`spark-operator.k8s.io/hadoopConfigMap`|Name of the Kubernetes ConfigMap storing Hadoop configuration files (to which `HADOOP_CONF_DIR` applies)|
|`spark-operator.k8s.io/configMap.[ConfigMapName]`|Mount path of the ConfigMap named `ConfigMapName`|
|`spark-operator.k8s.io/GCPServiceAccount.[SeviceAccountSecretName]`|Mount path of the secret storing GCP service account credentials (typically a JSON key file) named `SeviceAccountSecretName`|

When using the `SparkApplication` CRD to describe a Spark appliction, the annotations are automatically added by the CRD controller. `manifest/spark-application-example-gcp-service-account.yaml` shows an example `SparkApplication` with a user-specified GCP service account credential secret named `gcp-service-account` that is to be mounted into both the driver and executor containers. The `SparkApplication` controller automatically adds the annotation `spark-operator.k8s.io/GCPServiceAccount.gcp-service-account=/mnt/secrets` to the driver and executor Pods. The initializer sees the annotation and automatically adds a secret volume into the pods.   
