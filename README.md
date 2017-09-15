# Spark Operator

Spark Operator is an experimental project aiming to make it easier to run [Spark-on-Kubernetes](https://github.com/apache-spark-on-k8s/spark) applications on a Kubernetes cluster by automating certain tasks such as the following:
* Mounting secrets necessary for a Spark application to access some services into the driver and executor Pods.
* Mounting ConfigMaps carrying Spark or Hadoop configuration files that are to be put into a directory referred to by the environment variable `SPARK_CONF_DIR` or `HADOOP_CONF_DIR` into the driver and executor Pods. Example use cases include shipping a `log4j.properties` file for configuring logging and a `core-site.xml` file for configuring Hadoop and/or HDFS access.
* Creating a `NodePort` service for the Spark UI running on the driver so the UI can be accessed from outside the Kubernetes cluster.
* Adding a sidecar container for streaming application logs written to files to stdout/stderr so the logs can be retrieved using `kubectl logs` and pushed into Stackdriver.

To make such automation possible, Spark Operator uses the Kubernetes [CustomResourceDefinition](https://kubernetes.io/docs/tasks/access-kubernetes-api/extend-api-custom-resource-definitions/) and the corresponding custom controller as well as an [initializer](https://kubernetes.io/docs/admin/extensible-admission-controllers/#initializers). The custom controller is for specification and management of Spark applications, whereas the initializer is for customizing the Spark Pods.

## Current Status

Currently the initializer is the only component that is tested and actually works.  

## Build and Installation

To build Spark operator, run the following command:

```
make build
```

## Using the Initializer

The initializer looks for certain custom annotations on Spark driver and executor Pods to perform its tasks. To use the initializer, simply add the needed annotations to the driver and/or executor Pods using the following Spark configuration properties when submitting your Spark applications through the `spark-submit` script.

```
--conf spark.kubernetes.driver.annotations.[AnnotationName]=value
--conf spark.kubernetes.executor.annotations.[AnnotationName]=value
```  

### Mounting GCP Service Account Secrets

