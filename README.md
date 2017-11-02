# Spark Operator

Spark Operator is an experimental project aiming to make it easier to run [Spark-on-Kubernetes](https://github.com/apache-spark-on-k8s/spark) applications on a Kubernetes cluster by automating certain tasks such as the following:
* Mounting secrets necessary for a Spark application to access some services into the driver and executor Pods.
* Mounting ConfigMaps carrying Spark or Hadoop configuration files that are to be put into a directory referred to by the environment variable `SPARK_CONF_DIR` or `HADOOP_CONF_DIR` into the driver and executor Pods. Example use cases include shipping a `log4j.properties` file for configuring logging and a `core-site.xml` file for configuring Hadoop and/or HDFS access.
* Creating a `NodePort` service for the Spark UI running on the driver so the UI can be accessed from outside the Kubernetes cluster.
* Adding a sidecar container for streaming application logs written to files to stdout/stderr so the logs can be retrieved using `kubectl logs` and pushed into Stackdriver.

To make such automation possible, Spark Operator uses the Kubernetes [CustomResourceDefinition](https://kubernetes.io/docs/tasks/access-kubernetes-api/extend-api-custom-resource-definitions/) and the corresponding custom controller as well as an [initializer](https://kubernetes.io/docs/admin/extensible-admission-controllers/#initializers). The custom controller is for specification and management of Spark applications, whereas the initializer is for customizing the Spark Pods.

## Current Status

Currently the SparkApplication CRD controller and the pod initializer work.  

## Build and Installation

To build the Spark operator, run the following command:

```
make build
```

To additionally build a Docker image of the Spark operator, run the following command:

```
make image-tag=<image tag> image
```

To further push the Docker image to Docker hub, run the following command:

```
make image-tag=<image tag> push
```

## Deploying Spark Operator

To deploy the Spark operator, run the following command:

```
kubectl create -f manifest/spark-operator.yaml 
```

## Running the Example Spark Application

To run the example Spark application, run the following command:

```
kubectl create -f manifest/spark-application-example.yaml
```

This will create a `SparkApplication` object named `spark-app-example`. Check the object by running the following command:

```
kubectl get sparkapplications spark-app-example -o=yaml
```

This will show something similar to the following:

```
apiVersion: spark-operator.k8s.io/v1alpha1
kind: SparkApplication
metadata:
  ...
spec:
  deps: {}
  driver:
    image: kubespark/spark-driver:v2.2.0-kubernetes-0.5.0
  executor:
    image: kubespark/spark-executor:v2.2.0-kubernetes-0.5.0
    instances: 1
  mainApplicationFile: local:///opt/spark/examples/jars/spark-examples_2.11-2.2.0-k8s-0.5.0.jar
  mainClass: org.apache.spark.examples.SparkPi
  mode: cluster
  sparkConf:
    spark.driver.cores: "0.1"
    spark.executor.cores: "0.1"
    spark.executor.memory: 512m
  submissionByUser: false
  type: Scala
status:
  appId: spark-app-example-2877880513
  applicationState:
    errorMessage: ""
    state: COMPLETED
  driverInfo:
    podName: spark-app-example-1509647496976-driver
    webUIPort: 4040
    webUIServiceName: spark-app-example-ui-2877880513
  executorState:
    spark-app-example-1509647496976-exec-1: COMPLETED
```

## Using the Initializer

The initializer looks for certain custom annotations on Spark driver and executor Pods to perform its tasks. To use the initializer, simply add the needed annotations to the driver and/or executor Pods using the following Spark configuration properties when submitting your Spark applications through the `spark-submit` script.

```
--conf spark.kubernetes.driver.annotations.[AnnotationName]=value
--conf spark.kubernetes.executor.annotations.[AnnotationName]=value
```  

### Mounting GCP Service Account Secrets

