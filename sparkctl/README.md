# sparkctl

`sparkctl` is a command-line tool of the Spark Operator for creating, listing, checking status of, getting logs of, and deleting `SparkApplication`s. It can also do port forwarding from a local port to the Spark web UI port for accessing the Spark web UI on the driver. Each function is implemented as a sub-command of `sparkctl`.

To build `sparkctl`, make sure you followed build steps [here](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/master/docs/developer-guide.md#build-the-operator) and have all the dependencies, then run the following command from within `sparkctl/`:

```bash
$ go build -o sparkctl
```

## Flags

The following global flags are available for all the sub commands:
* `--namespace`: the Kubernetes namespace of the `SparkApplication`(s). Defaults to `default`.
* `--kubeconfig`: the path to the file storing configuration for accessing the Kubernetes API server. Defaults to 
`$HOME/.kube/config`

## Available Commands

### Create

`create` is a sub command of `sparkctl` for creating a `SparkApplication` object. There are two ways to create a `SparkApplication` object. One is parsing and creating a `SparkApplication` object in namespace specified by `--namespace` the from a given YAML file. In this way, `create` parses the YAML file, and sends the parsed `SparkApplication` object parsed to the Kubernetes API server. Usage of this way looks like the following:

Usage:
```bash
$ sparkctl create <path to YAML file>
```
The other way is creating a `SparkApplication` object from a named `ScheduledSparkApplication` to manually force a run of the `ScheduledSparkApplication`. Usage of this way looks like the following:

Usage:
```bash
$ sparkctl create <name of the SparkApplication> --from <name of the ScheduledSparkApplication>
```

The `create` command also supports shipping local Hadoop configuration files into the driver and executor pods. Specifically, it detects local Hadoop configuration files located at the path specified by the 
environment variable `HADOOP_CONF_DIR`, create a Kubernetes `ConfigMap` from the files, and adds the `ConfigMap` to the `SparkApplication` object so it gets mounted into the driver and executor pods by the operator. The environment variable `HADOOP_CONF_DIR` is also set in the driver and executor containers.    

#### Staging local dependencies

The `create` command also supports staging local application dependencies, though currently only uploading to a Google Cloud Storage (GCS) bucket is supported. The way it works is as follows. It checks if there is any local dependencies in `spec.mainApplicationFile`, `spec.deps.jars`, `spec.deps.files`, etc. in the parsed `SparkApplication` object. If so, it tries to upload the local dependencies to the remote location specified by `--upload-to`. The command fails if local dependencies are used but `--upload-to` is not specified. By default, a local file that already exists remotely, i.e., there exists a file with the same name and upload path remotely, will be ignored. If the remote file should be overridden instead, the `--override` flag should be specified.

##### Uploading to GCS

For uploading to GCS, the value should be in the form of `gs://<bucket>`. The bucket must exist and uploading fails if otherwise. The local dependencies will be uploaded to the path 
`spark-app-dependencies/<SparkApplication namespace>/<SparkApplication name>` in the given bucket. It replaces the file path of each local dependency with the URI of the remote copy in the parsed `SparkApplication` object if uploading is successful. 

Note that uploading to GCS requires a GCP service account with the necessary IAM permission to use the GCP project specified by service account JSON key file (`serviceusage.services.use`) and the permission to create GCS objects (`storage.object.create`). 
The service account JSON key file must be locally available and be pointed to by the environment variable 
`GOOGLE_APPLICATION_CREDENTIALS`. For more information on IAM authentication, please check 
[Getting Started with Authentication](https://cloud.google.com/docs/authentication/getting-started).

Usage:
```bash
$ export GOOGLE_APPLICATION_CREDENTIALS="[PATH]/[FILE_NAME].json"
$ sparkctl create <path to YAML file> --upload-to gs://<bucket>
```

By default, the uploaded dependencies are not made publicly accessible and are referenced using URIs in the form of  `gs://bucket/path/to/file`. Such dependencies are referenced through URIs of the form `gs://bucket/path/to/file`. To download the dependencies from GCS, a custom-built Spark init-container with the [GCS connector](https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage) installed and necessary Hadoop configuration properties specified is needed. An example Docker file of such an init-container can be found [here](https://gist.github.com/liyinan926/f9e81f7b54d94c05171a663345eb58bf). 

If you want to make uploaded dependencies publicly available so they can be downloaded by the built-in init-container, simply add `--public` to the `create` command, as the following example shows:

```bash
$ sparkctl create <path to YAML file> --upload-to gs://<bucket> --public
```

Publicly available files are referenced through URIs of the form `https://storage.googleapis.com/bucket/path/to/file`.

##### Uploading to S3

For uploading to S3, the value should be in the form of `s3://<bucket>`. The bucket must exist and uploading fails if otherwise. The local dependencies will be uploaded to the path
`spark-app-dependencies/<SparkApplication namespace>/<SparkApplication name>` in the given bucket. It replaces the file path of each local dependency with the URI of the remote copy in the parsed `SparkApplication` object if uploading is successful.

Note that uploading to S3 with [AWS SDK](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html) requires credentials to be specified. For GCP, the S3 Interoperability credentials can be retrieved as described [here](https://cloud.google.com/storage/docs/migrating#keys).
SDK uses the default credential provider chain to find AWS credentials. 
The SDK uses the first provider in the chain that returns credentials without an error.
The default provider chain looks for credentials in the following order:

- Environment variables
    ```
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    ```
- Shared credentials file (.aws/credentials)

For more information about AWS SDK authentication, please check [Specifying Credentials](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html#specifying-credentials).

Usage:
```bash
$ export AWS_ACCESS_KEY_ID=[KEY]
$ export AWS_SECRET_ACCESS_KEY=[SECRET]
$ sparkctl create <path to YAML file> --upload-to s3://<bucket>
```

By default, the uploaded dependencies are not made publicly accessible and are referenced using URIs in the form of `s3a://bucket/path/to/file`. To download the dependencies from S3, a custom-built Spark Docker image with the required jars for `S3A Connector` (`hadoop-aws-2.7.6.jar`, `aws-java-sdk-1.7.6.jar` for Spark build with Hadoop2.7 profile, or `hadoop-aws-3.1.0.jar`, `aws-java-sdk-bundle-1.11.271.jar` for Hadoop3.1) need to be available in the classpath, and `spark-default.conf` with the AWS keys and the S3A FileSystemClass needs to be set (you can also use `spec.hadoopConf` in the SparkApplication YAML):

```
spark.hadoop.fs.s3a.endpoint https://storage.googleapis.com
spark.hadoop.fs.s3a.access.key [KEY]
spark.hadoop.fs.s3a.secret.key [SECRET]
spark.hadoop.fs.s3a.impl org.apache.hadoop.fs.s3a.S3AFileSystem
```

NOTE: In Spark 2.3 init-containers are used for downloading remote application dependencies. In future versions, init-containers are removed. 
It is recommended to use Apache Spark 2.4 for staging local dependencies with `s3`,  which currently requires building a custom Docker image from the Spark master branch. Additionally, since Spark 2.4.0 
there are two available build profiles, Hadoop2.7 and Hadoop3.1. For use of Spark with `S3A Connector`, Hadoop3.1 profile is recommended as this allows to use newer version of `aws-java-sdk-bundle`.

If you want to use custom S3 endpoint or region, add `--upload-to-endpoint` and `--upload-to-region`: 

```bash
$ sparkctl create <path to YAML file> --upload-to-endpoint https://<endpoint-url> --upload-to-region <endpoint-region> --upload-to s3://<bucket>
```

If you want to force path style URLs for S3 objects add `--s3-force-path-style`: 

```bash
$ sparkctl create <path to YAML file> --s3-force-path-style
```

If you want to make uploaded dependencies publicly available, add `--public` to the `create` command, as the following example shows:

```bash
$ sparkctl create <path to YAML file> --upload-to s3://<bucket> --public
```

Publicly available files are referenced through URIs in the default form `https://<endpoint-url>/bucket/path/to/file`.

### List

`list` is a sub command of `sparkctl` for listing `SparkApplication` objects in the namespace specified by 
`--namespace`.

Usage:
```bash
$ sparkctl list
```

### Status

`status` is a sub command of `sparkctl` for checking and printing the status of a `SparkApplication` in the namespace specified by `--namespace`.

Usage:
```bash
$ sparkctl status <SparkApplication name>
```

### Event

`event` is a sub command of `sparkctl` for listing `SparkApplication` events in the namespace 
specified by `--namespace`. 

The `event` command also supports streaming the events with the `--follow` or `-f` flag. 
The command will display events since last creation of the `SparkApplication` for the specific `name`, and continues to stream events even if `ResourceVersion` changes.

Usage:
```bash
$ sparkctl event <SparkApplication name> [-f]
```

### Log

`log` is a sub command of `sparkctl` for fetching the logs of a pod of `SparkApplication` with the given name in the namespace specified by `--namespace`. The command by default fetches the logs of the driver pod. To make it fetch logs of an executor pod instead, use the flag `--executor` or `-e` to specify the ID of the executor whose logs should be fetched.

The `log` command also supports streaming the driver or executor logs with the `--follow` or `-f` flag. It works in the same way as `kubectl logs -f`, i.e., it streams logs until no more logs are available.

Usage:
```bash
$ sparkctl log <SparkApplication name> [-e <executor ID, e.g., 1>] [-f]
```

### Delete

`delete` is a sub command of `sparkctl` for deleting a `SparkApplication` with the given name in the namespace specified by `--namespace`.

Usage:
```bash
$ sparkctl delete <SparkApplication name>
```

### Forward

`forward` is a sub command of `sparkctl` for doing port forwarding from a local port to the Spark web UI port on the driver. It allows the Spark web UI served in the driver pod to be accessed locally. By default, it forwards from local port `4040` to remote port `4040`, which is the default Spark web UI port. Users can specify different local port and remote port using the flags `--local-port` and `--remote-port`, respectively. 

Usage:
```bash
$ sparkctl forward <SparkApplication name> [--local-port <local port>] [--remote-port <remote port>]
```

Once port forwarding starts, users can open `127.0.0.1:<local port>` or `localhost:<local port>` in a browser to access the Spark web UI. Forwarding continues until it is interrupted or the driver pod terminates.
