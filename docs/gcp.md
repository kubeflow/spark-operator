# Integration with Google Cloud Storage and BigQuery

This document describes how to use Google Cloud services, e.g., Google Cloud Storage (GCS) and BigQuery as data sources 
or sinks in `SparkApplication`s. For a detailed tutorial on building Spark applications that access GCS and BigQuery, 
please refer to [Using Spark on Kubernetes Engine to Process Data in BigQuery](https://cloud.google.com/solutions/spark-on-kubernetes-engine).

A Spark application requires the [GCS](https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage) and 
[BigQuery](https://cloud.google.com/dataproc/docs/concepts/connectors/bigquery) connectors to access GCS and BigQuery 
using the Hadoop `FileSystem` API. One way to make the connectors available to the driver and executors is to use a 
custom Spark image with the connectors built-in, as this example [Dockerfile](https://github.com/GoogleCloudPlatform/spark-on-k8s-gcp-examples/blob/master/dockerfiles/spark-gcs/Dockerfile) shows.
An image built from this Dockerfile is located at `gcr.io/ynli-k8s/spark:v2.3.0-gcs`. 

The connectors require certain Hadoop properties to be set properly to function. Setting Hadoop properties can be done 
both through a custom Hadoop configuration file, namely, `core-site.xml` in a custom image, or via the `spec.hadoopConf` 
section in a `SparkApplication`. The example Dockerfile mentioned above shows the use of a custom `core-site.xml` and a 
custom `spark-env.sh` that points the environment variable `HADOOP_CONF_DIR` to the directory in the container where 
`core-site.xml` is located. The example `core-site.xml` and `spark-env.sh` can be found 
[here](https://github.com/GoogleCloudPlatform/spark-on-k8s-gcp-examples/tree/master/conf).

The GCS and BigQuery connectors need to authenticate with the GCS and BigQuery services before they can use the services.
The connectors support using a [GCP service account JSON key file](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) 
for authentication. The service account must have the necessary IAM roles for access GCS and/or BigQuery granted. The 
[tutorial](https://cloud.google.com/solutions/spark-on-kubernetes-engine) has detailed information on how to create an 
service account, grant it the right roles, furnish a key, and download a JSON key file. To tell the connectors to use 
a service JSON key file for authentication, the following Hadoop configuration properties
must be set:

```
google.cloud.auth.service.account.enable=true
google.cloud.auth.service.account.json.keyfile=<path to the service account JSON key file in the container>
``` 

The most common way of getting the service account JSON key file into the driver and executor containers is mount the key
file in through a Kubernetes secret volume. Detailed information on how to create a secret can be found in the 
[tutorial](https://cloud.google.com/solutions/spark-on-kubernetes-engine).

Below is an example `SparkApplication` using the custom image at `gcr.io/ynli-k8s/spark:v2.3.0-gcs` with the GCS/BigQuery 
connectors and the custom Hadoop configuration files above built-in. Note that some of the necessary Hadoop configuration 
properties are set using `spec.hadoopConf`. Those Hadoop configuration properties are additional to the ones set in the 
built-in `core-site.xml`. They are set here instead of in `core-site.xml` because of their application-specific nature. 
The ones set in `core-site.xml` apply to all applications using the image. Also note how the Kubernetes secret named 
`gcs-bg` that stores the service account JSON key file gets mounted into both the driver and executors. The environment 
variable `GCS_PROJECT_ID` must be set when using the image at `gcr.io/ynli-k8s/spark:v2.3.0-gcs`.

```yaml
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: foo-gcs-bg
spec:
  type: Java
  mode: cluster
  image: gcr.io/ynli-k8s/spark:v2.3.0-gcs
  imagePullPolicy: Always
  hadoopConf:
    "fs.gs.project.id": "foo"
    "fs.gs.system.bucket": "foo-bucket"
    "google.cloud.auth.service.account.enable": "true"
    "google.cloud.auth.service.account.json.keyfile": "/mnt/secrets/key.json"
  driver:
    cores: 1
    secrets:
    - name: "gcs-bq"
      path: "/mnt/secrets"
      secretType: GCPServiceAccount
    envVars:
      GCS_PROJECT_ID: foo
    serviceAccount: spark
  executor:
    instances: 2
    cores: 1
    memory: "512m"
    secrets:
    - name: "gcs-bq"
      path: "/mnt/secrets"
      secretType: GCPServiceAccount
    envVars:
      GCS_PROJECT_ID: foo
```
