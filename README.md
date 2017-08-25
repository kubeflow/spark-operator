# Spark Operator

Spark Operator is an experimental project aiming to make it easier to run [Spark-on-Kubernetes](https://github.com/apache-spark-on-k8s/spark) applications on a Kubernetes cluster by automating certain tasks such as the following:
* Mounting secrets necessary for a Spark application to access some services into the driver and executor Pods.
* Mounting ConfigMaps carrying Spark or Hadoop configuration files that are to be put into a directory referred to by the environment variable `SPARK_CONF_DIR` or `HADOOP_CONF_DIR` into the driver and executor Pods. Example use cases include shipping a `log4j.properties` file for configuring logging and a `core-site.xml` file for configuring Hadoop and/or HDFS access.
* Creating a `NodePort` service for the Spark UI running on the driver so the UI can be accessed from outside the Kubernetes cluster.
* Adding a sidecar container for streaming application logs written to files to stdout/stderr so the logs can be retrieved using `kubectl logs` and pushed into Stackdriver.

To make such automation possible, Spark Operator uses the Kubernetes [CustomResourceDefinition](https://kubernetes.io/docs/tasks/access-kubernetes-api/extend-api-custom-resource-definitions/) and the corresponding custom controller as well as an [initializer](https://kubernetes.io/docs/admin/extensible-admission-controllers/#initializers) or a [PodPreset](https://kubernetes.io/docs/tasks/inject-data-application/podpreset/) together. The custom controller is for specification and management of Spark applications, whereas the initializer or PodPreset is for customizing the Spark Pods.
