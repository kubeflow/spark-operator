# Writing a SparkApplication

As with all other Kubernetes API objects, a `SparkApplication` needs the `apiVersion`, `kind`, and `metadata` fields. For general information about working with manifests, see [object management using kubectl](https://kubernetes.io/docs/concepts/overview/object-management-kubectl/overview/).

A `SparkApplication` also needs a [`.spec` section](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#spec-and-status). This section contains fields for specifying various aspects of an application including its type (`Scala`, `Java`, `Python`, or `R`), deployment mode (`cluster` or `client`), main application resource URI (e.g., the URI of the application jar), main class, arguments, etc. Node selectors are also supported via the optional field `.spec.nodeSelector`.

It also has fields for specifying the unified container image (to use for both the driver and executors) and the image pull policy, namely, `.spec.image` and `.spec.imagePullPolicy` respectively. If a custom init-container (in both the driver and executor pods) image needs to be used, the optional field `.spec.initContainerImage` can be used to specify it. If set, `.spec.initContainerImage` overrides `.spec.image` for the init-container image. Otherwise, the image specified by `.spec.image` will be used for the init-container. It is invalid if both `.spec.image` and `.spec.initContainerImage` are not set.

Below is an example showing part of a `SparkApplication` specification:

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: spark-pi
  namespace: default
spec:
  type: Scala
  mode: cluster
  image: spark:3.5.1
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: local:///opt/spark/examples/jars/spark-examples_2.12-3.5.1.jar
```

## Specifying Deployment Mode

A `SparkApplication` should set `.spec.deployMode` to `cluster`, as `client` is not currently implemented. The driver pod will then run `spark-submit` in `client` mode internally to run the driver program. Additional details of how `SparkApplication`s are run can be found in the [design documentation](../overview/index.md#architecture).

## Specifying Application Dependencies

Often Spark applications need additional files additionally to the main application resource to run. Such application dependencies can include for example jars and data files the application needs at runtime. When using the `spark-submit` script to submit a Spark application, such dependencies are specified using the `--jars` and `--files` options. To support specification of application dependencies, a `SparkApplication` uses an optional field `.spec.deps` that in turn supports specifying jars and files, respectively. More specifically, the optional fields `.spec.deps.jars` and`.spec.deps.files` correspond to the `--jars` and `--files` options of the `spark-submit` script, respectively.

Additionally, `.spec.deps` also has fields for specifying the locations in the driver and executor containers where jars and files should be downloaded to, namely, `.spec.deps.jarsDownloadDir` and `.spec.deps.filesDownloadDir`. The optional fields `.spec.deps.downloadTimeout` and `.spec.deps.maxSimultaneousDownloads` are used to control the timeout and maximum parallelism of downloading dependencies that are hosted remotely, e.g., on an HTTP server, or in external storage such as HDFS, Google Cloud Storage, or AWS S3.

The following is an example specification with both container-local (i.e., within the container) and remote dependencies:

```yaml
spec:
  deps:
    jars:
      - local:///opt/spark-jars/gcs-connector.jar
    files:
      - gs://spark-data/data-file-1.txt
      - gs://spark-data/data-file-2.txt
```

It's also possible to specify additional jars to obtain from a remote repository by adding maven coordinates to `.spec.deps.packages`. Conflicting transitive dependencies can be addressed by adding to the exclusion list with `.spec.deps.excludePackages`. Additional repositories can be added to the `.spec.deps.repositories` list. These directly translate to the `spark-submit` parameters `--packages`, `--exclude-packages`, and `--repositories`.

NOTE:

- Each package in the `packages` list must be of the form "groupId:artifactId:version"
- Each package in the `excludePackages` list must be of the form "groupId:artifactId"

The following example shows how to use these parameters.

```yaml
spec:
  deps:
    repositories:
      - https://repository.example.com/prod
    packages:
      - com.example:some-package:1.0.0
    excludePackages:
      - com.example:other-package
```

## Specifying Spark Configuration

There are two ways to add Spark configuration: setting individual Spark configuration properties using the optional field `.spec.sparkConf` or mounting a special Kubernetes ConfigMap storing Spark configuration files (e.g. `spark-defaults.conf`, `spark-env.sh`, `log4j.properties`) using the optional field `.spec.sparkConfigMap`. If `.spec.sparkConfigMap` is used, additionally to mounting the ConfigMap into the driver and executors, the operator additionally sets the environment variable `SPARK_CONF_DIR` to point to the mount path of the ConfigMap.

```yaml
spec:
  sparkConf:
    spark.ui.port: "4045"
    spark.eventLog.enabled: "true"
    spark.eventLog.dir: "hdfs://hdfs-namenode-1:8020/spark/spark-events"
```

## Specifying Hadoop Configuration

There are two ways to add Hadoop configuration: setting individual Hadoop configuration properties using the optional field `.spec.hadoopConf` or mounting a special Kubernetes ConfigMap storing Hadoop configuration files (e.g.  `core-site.xml`) using the optional field `.spec.hadoopConfigMap`. The operator automatically adds the prefix `spark.hadoop.` to the names of individual Hadoop configuration properties in `.spec.hadoopConf`. If  `.spec.hadoopConfigMap` is used, additionally to mounting the ConfigMap into the driver and executors, the operator additionally sets the environment variable `HADOOP_CONF_DIR` to point to the mount path of the ConfigMap.

The following is an example showing the use of individual Hadoop configuration properties:

```yaml
spec:
  hadoopConf:
    "fs.gs.project.id": spark
    "fs.gs.system.bucket": spark
    "google.cloud.auth.service.account.enable": true
    "google.cloud.auth.service.account.json.keyfile": /mnt/secrets/key.json
```

## Writing Driver Specification

The `.spec` section of a `SparkApplication` has a `.spec.driver` field for configuring the driver. It allows users to set the memory and CPU resources to request for the driver pod, and the container image the driver should use. It also has fields for optionally specifying labels, annotations, and environment variables for the driver pod. By default, the driver pod name of an application is automatically generated by the Spark submission client. If instead you want to use a particular name for the driver pod, the optional field `.spec.driver.podName` can be used. The driver pod by default uses the `default` service account in the namespace it is running in to talk to the Kubernetes API server. The `default` service account, however, may or may not have sufficient permissions to create executor pods and the headless service used by the executors to connect to the driver. If it does not and a custom service account that has the right permissions should be used instead, the optional field `.spec.driver.serviceAccount` can be used to specify the name of the custom service account. When a custom container image is needed for the driver, the field `.spec.driver.image` can be used to specify it. This overrides the image specified in `.spec.image` if it is also set. It is invalid if both `.spec.image` and `.spec.driver.image` are not set.

For applications that need to mount Kubernetes [Secrets](https://kubernetes.io/docs/concepts/configuration/secret/) or [ConfigMaps](https://kubernetes.io/docs/tasks/configure-pod-container/configure-pod-configmap/) into the driver pod, fields `.spec.driver.secrets` and `.spec.driver.configMaps` can be used. For more details, please refer to
[Mounting Secrets](#mounting-secrets) and [Mounting ConfigMaps](#mounting-configmaps).

The following is an example driver specification:

```yaml
spec:
  driver:
    cores: 1
    coreLimit: 200m
    memory: 512m
    labels:
      version: 3.1.1
    serviceAccount: spark
```

## Writing Executor Specification

The `.spec` section of a `SparkApplication` has a `.spec.executor` field for configuring the executors. It allows users to set the memory and CPU resources to request for the executor pods, and the container image the executors should use. It also has fields for optionally specifying labels, annotations, and environment variables for the executor pods. By default, a single executor is requested for an application. If more than one executor are needed, the optional field `.spec.executor.instances` can be used to specify the number of executors to request. When a custom container image is needed for the executors, the field `.spec.executor.image` can be used to specify it. This overrides the image specified in `.spec.image` if it is also set. It is invalid if both `.spec.image` and `.spec.executor.image` are not set.

For applications that need to mount Kubernetes [Secrets](https://kubernetes.io/docs/concepts/configuration/secret/) or [ConfigMaps](https://kubernetes.io/docs/tasks/configure-pod-container/configure-pod-configmap/) into the executor pods, fields `.spec.executor.secrets` and `.spec.executor.configMaps` can be used. For more details, please refer to
[Mounting Secrets](#mounting-secrets) and [Mounting ConfigMaps](#mounting-configmaps).

An example executor specification is shown below:

```yaml
spec:
  executor:
    cores: 1
    instances: 1
    memory: 512m
    labels:
      version: 3.1.1
    serviceAccount: spark
```

## Specifying Extra Java Options

A `SparkApplication` can specify extra Java options for the driver or executors, using the optional field `.spec.driver.javaOptions` for the driver and `.spec.executor.javaOptions` for executors. Below is an example:

```yaml
spec:
  executor:
    javaOptions: "-XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap"
```

Values specified using those two fields get converted to Spark configuration properties `spark.driver.extraJavaOptions` and `spark.executor.extraJavaOptions`, respectively. **Prefer using the above two fields over configuration properties `spark.driver.extraJavaOptions` and `spark.executor.extraJavaOptions`** as the fields work well with other fields that might modify what gets set for `spark.driver.extraJavaOptions` or `spark.executor.extraJavaOptions`.

## Specifying Environment Variables

There are two fields for specifying environment variables for the driver and/or executor containers, namely `.spec.driver.env` (or `.spec.executor.env` for the executor container) and `.spec.driver.envFrom` (or `.spec.executor.envFrom` for the executor container). Specifically, `.spec.driver.env` (and `.spec.executor.env`) takes a list of [EnvVar](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#envvar-v1-core), each of which specifies an environment variable or the source of an environment variable, e.g., a name-value pair, a ConfigMap key, a Secret key, etc. Alternatively, `.spec.driver.envFrom` (and `.spec.executor.envFrom`) takes a list of [EnvFromSource](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#envfromsource-v1-core) and allows [using all key-value pairs in a ConfigMap or Secret as environment variables](https://kubernetes.io/docs/tasks/configure-pod-container/configure-pod-configmap/#configure-all-key-value-pairs-in-a-configmap-as-container-environment-variables). The `SparkApplication` snippet below shows the use of both fields:

```yaml
spec:
  driver:
    env:
      - name: ENV1
        value: VAL1
      - name: ENV2
        value: VAL2
      - name: ENV3
        valueFrom:
          configMapKeyRef:
            name: some-config-map
            key: env3-key
      - name: AUTH_KEY
        valueFrom:
          secretKeyRef:
            name: some-secret
            key: auth-key
    envFrom:
      - configMapRef:
          name: env-config-map
      - secretRef:
          name: env-secret
  executor:
    env:
      - name: ENV1
        value: VAL1
      - name: ENV2
        value: VAL2
      - name: ENV3
        valueFrom:
          configMapKeyRef:
            name: some-config-map
            key: env3-key
      - name: AUTH_KEY
        valueFrom:
          secretKeyRef:
            name: some-secret
            key: auth-key
    envFrom:
      - configMapRef:
          name: my-env-config-map
      - secretRef:
          name: my-env-secret
```

**Note: legacy field `envVars` that can also be used for specifying environment variables is deprecated and will be removed in a future API version.**

## Requesting GPU Resources

A `SparkApplication` can specify GPU resources for the driver or executor pod, using the optional field `.spec.driver.gpu` or `.spec.executor.gpu`. Below is an example:

```yaml
spec:
  driver:
    cores: 0.1
    coreLimit: "200m"
    memory: "512m"
    gpu:
      name: "amd.com/gpu"   # GPU resource name
      quantity: 1           # number of GPUs to request
    labels:
      version: 3.1.1
    serviceAccount: spark
  executor:
    cores: 1
    instances: 1
    memory: "512m"
    serviceAccount: spark
    gpu:
      name: "nvidia.com/gpu"
      quantity: 1
```

Note that the mutating admission webhook is needed to use this feature. Please refer to the [Getting Started](../getting-started.md) on how to enable the mutating admission webhook.

## Host Network

A `SparkApplication` can specify `hostNetwork` for the driver or executor pod, using the optional field `.spec.driver.hostNetwork` or `.spec.executor.hostNetwork`. When `hostNetwork` is `true`, the operator sets pods' `spec.hostNetwork` to `true` and sets pods' `spec.dnsPolicy` to `ClusterFirstWithHostNet`. Below is an example:

```yaml
spec:
  driver:
    cores: 0.1
    coreLimit: "200m"
    memory: "512m"
    hostNetwork: true
    labels:
      version: 3.1.1
    serviceAccount: spark
  executor:
    cores: 1
    instances: 1
    memory: "512m"
```

Note that the mutating admission webhook is needed to use this feature. Please refer to the [Getting Started](../getting-started.md) on how to enable the mutating admission webhook.

## Mounting Secrets

As mentioned above, both the driver specification and executor specification have an optional field `secrets` for configuring the list of Kubernetes Secrets to be mounted into the driver and executors, respectively. The field is a map with the names of the Secrets as keys and values specifying the mount path and type of each Secret. For instance, the following example shows a driver specification with a Secret named `gcp-svc-account` of type `GCPServiceAccount` to be mounted to `/mnt/secrets` in the driver pod.

```yaml
spec:
  driver:
    secrets:
      - name: gcp-svc-account
        path: /mnt/secrets
        secretType: GCPServiceAccount
```

The type of a Secret as specified by the `secretType` field is a hint to the operator on what extra configuration it needs to take care of for the specific type of Secrets. For example, if a Secret is of type **`GCPServiceAccount`**, the operator additionally sets the environment variable **`GOOGLE_APPLICATION_CREDENTIALS`** to point to the JSON key file stored in the secret. Please refer to
[Getting Started with Authentication](https://cloud.google.com/docs/authentication/getting-started) for more information on how to authenticate with GCP services using a service account JSON key file. Note that the operator assumes that the key of the service account JSON key file in the Secret data map is **`key.json`** so it is able to set the environment variable automatically. Similarly, if the type of a Secret is **`HadoopDelegationToken`**, the operator additionally sets the environment variable **`HADOOP_TOKEN_FILE_LOCATION`** to point to the file storing the Hadoop delegation token. In this case, the operator assumes that the key of the delegation token file in the Secret data map is **`hadoop.token`**.
The `secretType` field should have the value `Generic` if no extra configuration is required.

Note that the mutating admission webhook is needed to use this feature. Please refer to the [Getting Started](../getting-started.md) on how to enable the mutating admission webhook.

## Mounting ConfigMaps

Both the driver specification and executor specifications have an optional field for configuring
the list of Kubernetes ConfigMaps to be mounted into the driver and executors, respectively. The field is a map with keys being the names of the ConfigMaps and values specifying the mount path of each ConfigMap. For instance, the following example shows a driver specification with a ConfigMap named `configmap1` to be mounted to `/mnt/config-maps` in the driver pod.

```yaml
spec:
  driver:
    configMaps:
      - name: configmap1
        path: /mnt/config-maps
```

Note that the mutating admission webhook is needed to use this feature. Please refer to the [Getting Started](../getting-started.md) on how to enable the mutating admission webhook.

## Mounting a ConfigMap storing Spark Configuration Files

A `SparkApplication` can specify a Kubernetes ConfigMap storing Spark configuration files such as `spark-env.sh` or `spark-defaults.conf` using the optional field `.spec.sparkConfigMap` whose value is the name of the ConfigMap. The ConfigMap is assumed to be in the same namespace as that of the `SparkApplication`. The operator mounts the ConfigMap onto path `/etc/spark/conf` in both the driver and executors. Additionally, it also sets the environment variable `SPARK_CONF_DIR` to point to `/etc/spark/conf` in the driver and executors.

Note that the mutating admission webhook is needed to use this feature. Please refer to the
[Getting Started](../getting-started.md) on how to enable the mutating admission webhook.

## Mounting a ConfigMap storing Hadoop Configuration Files

A `SparkApplication` can specify a Kubernetes ConfigMap storing Hadoop configuration files such as `core-site.xml` using the optional field `.spec.hadoopConfigMap` whose value is the name of the ConfigMap. The ConfigMap is assumed to be in the same namespace as that of the `SparkApplication`. The operator mounts the ConfigMap onto path  `/etc/hadoop/conf` in both the driver and executors. Additionally, it also sets the environment variable `HADOOP_CONF_DIR` to point to `/etc/hadoop/conf` in the driver and executors.

Note that the mutating admission webhook is needed to use this feature. Please refer to the [Getting Started](../getting-started.md) on how to enable the mutating admission webhook.

## Mounting Volumes

The operator also supports mounting user-specified Kubernetes volumes into the driver and executors. A
`SparkApplication` has an optional field `.spec.volumes` for specifying the list of [volumes](https://kubernetes.io/docs/reference/kubernetes-api/config-and-storage-resources/volume/) the driver and the executors need collectively. Then both the driver and executor specifications have an optional field `volumeMounts`  that specifies the [volume mounts](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#volumes-1) for the volumes needed by the driver and executors, respectively. The following is an example showing a `SparkApplication` with both driver and executor volume mounts.

```yaml
spec:
  volumes:
    - name: spark-data
      persistentVolumeClaim:
        claimName: my-pvc
    - name: spark-work
      emptyDir:
        sizeLimit: 5Gi
  driver:
    volumeMounts:
      - name: spark-work
        mountPath: /mnt/spark/work
  executor:
    volumeMounts:
      - name: spark-data
        mountPath: /mnt/spark/data
      - name: spark-work
        mountPath: /mnt/spark/work

```

Note that the mutating admission webhook is needed to use this feature. Please refer to the [Getting Started](../getting-started.md) on how to enable the mutating admission webhook.

## Using Secrets As Environment Variables

**Note: `envSecretKeyRefs` is deprecated and will be removed in a future API version.**

A `SparkApplication` can use [secrets as environment variables](https://kubernetes.io/docs/concepts/configuration/secret/#using-secrets-as-environment-variables), through the optional field `.spec.driver.envSecretKeyRefs` for the driver pod and the optional field
`.spec.executor.envSecretKeyRefs` for the executor pods. A `envSecretKeyRefs` is a map from environment variable names to pairs consisting of a secret name and a secret key. Below is an example:

```yaml
spec:
  driver:
    envSecretKeyRefs:
      SECRET_USERNAME:
        name: mysecret
        key: username
      SECRET_PASSWORD:
        name: mysecret
        key: password
```

## Using Image Pull Secrets

**Note that this feature requires an image based on the latest Spark master branch.**

For images that need image-pull secrets to be pulled, a `SparkApplication` has an optional field `.spec.imagePullSecrets` for specifying a list of image-pull secrets. Below is an example:

```yaml
spec:
  imagePullSecrets:
    - secret1
    - secret2
```

## Using Pod Affinity

A `SparkApplication` can specify an `Affinity` for the driver or executor pod, using the optional field `.spec.driver.affinity` or `.spec.executor.affinity`. Below is an example:

```yaml
spec:
  driver:
    affinity:
      podAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          ...
  executor:
    affinity:
      podAntiAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          ...
```

Note that the mutating admission webhook is needed to use this feature. Please refer to the [Getting Started](../getting-started.md) on how to enable the mutating admission webhook.

## Using Tolerations

A `SparkApplication` can specify an `Tolerations` for the driver or executor pod, using the optional field `.spec.driver.tolerations` or `.spec.executor.tolerations`. Below is an example:

```yaml
spec:
  driver:
    tolerations:
    - key: Key
      operator: Exists
      effect: NoSchedule

  executor:
    tolerations:
    - key: Key
      operator: Equal
      value: Value
      effect: NoSchedule
```

Note that the mutating admission webhook is needed to use this feature. Please refer to the
[Getting Started](../getting-started.md) on how to enable the mutating admission webhook.

## Using Security Context

A `SparkApplication` can specify a `SecurityContext` for the driver or executor containers, using the optional field `.spec.driver.securityContext` or `.spec.executor.securityContext`.
`SparkApplication` can also specify a `PodSecurityContext` for the driver or executor pod, using the optional field `.spec.driver.podSecurityContext` or `.spec.executor.podSecurityContext`. Below is an example:

```yaml
spec:
  driver:
    podSecurityContext:
      runAsUser: 1000
    securityContext:
      allowPrivilegeEscalation: false
      runAsUser: 2000
  executor:
    podSecurityContext:
      runAsUser: 1000
    securityContext:
      allowPrivilegeEscalation: false
      runAsUser: 2000
```

Note that the mutating admission webhook is needed to use this feature. Please refer to the
[Getting Started](../getting-started.md) on how to enable the mutating admission webhook.

## Using Sidecar Containers

A `SparkApplication` can specify one or more optional sidecar containers for the driver or executor pod, using the optional field `.spec.driver.sidecars` or `.spec.executor.sidecars`. The specification of each sidecar container follows the [Container](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#container-v1-core) API definition. Below is an example:

```yaml
spec:
  driver:
    sidecars:
    - name: "sidecar1"
      image: "sidecar1:latest"
      ...
  executor:
    sidecars:
    - name: "sidecar1"
      image: "sidecar1:latest"
      ...
```

Note that the mutating admission webhook is needed to use this feature. Please refer to the
[Getting Started](../getting-started.md) on how to enable the mutating admission webhook.

## Using Init-Containers

A `SparkApplication` can optionally specify one or more [init-containers](https://kubernetes.io/docs/concepts/workloads/pods/init-containers/) for the driver or executor pod, using the optional field `.spec.driver.initContainers` or `.spec.executor.initContainers`, respectively. The specification of each init-container follows the [Container](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#container-v1-core) API definition. Below is an example:

```yaml
spec:
  driver:
    initContainers:
    - name: "init-container1"
      image: "init-container1:latest"
      ...
  executor:
    initContainers:
    - name: "init-container1"
      image: "init-container1:latest"
      ...
```

Note that the mutating admission webhook is needed to use this feature. Please refer to the
[Getting Started](../getting-started.md) on how to enable the mutating admission webhook.

## Using DNS Settings

A `SparkApplication` can define DNS settings for the driver and/or executor pod, by adding the standard [DNS](https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/#pod-s-dns-config) kubernetes settings. Fields to add such configuration are `.spec.driver.dnsConfig` and `.spec.executor.dnsConfig`. Example:

```yaml
spec:
  driver:
    dnsConfig:
      nameservers:
        - 1.2.3.4
      searches:
        - ns1.svc.cluster.local
        - my.dns.search.suffix
      options:
        - name: ndots
          value: "2"
        - name: edns0
```

Note that the mutating admission webhook is needed to use this feature. Please refer to the
[Getting Started](../getting-started.md) on how to enable the mutating admission webhook.

## Using Volume For Scratch Space

By default, Spark uses temporary scratch space to spill data to disk during shuffles and other operations.
The scratch directory defaults to `/tmp` of the container.
If that storage isn't enough or you want to use a specific path, you can use one or more volumes.
The volume names should start with `spark-local-dir-`.

```yaml
spec:
  volumes:
    - name: "spark-local-dir-1"
      hostPath:
        path: "/tmp/spark-local-dir"
  executor:
    volumeMounts:
      - name: "spark-local-dir-1"
        mountPath: "/tmp/spark-local-dir"
    ...
```

Then you will get `SPARK_LOCAL_DIRS` set to `/tmp/spark-local-dir` in the pod like below.

```yaml
Environment:
  SPARK_USER:                 root
  SPARK_DRIVER_BIND_ADDRESS:  (v1:status.podIP)
  SPARK_LOCAL_DIRS:           /tmp/spark-local-dir
  SPARK_CONF_DIR:             /opt/spark/conf
```

> Note: Multiple volumes can be used together

```yaml
spec:
  volumes:
    - name: "spark-local-dir-1"
      hostPath:
        path: "/mnt/dir1"
    - name: "spark-local-dir-2"
      hostPath:
        path: "/mnt/dir2"
  executor:
    volumeMounts:
      - name: "spark-local-dir-1"
        mountPath: "/tmp/dir1"
      - name: "spark-local-dir-2"
        mountPath: "/tmp/dir2"
    ...
```

> Note: Besides `hostPath`, `persistentVolumeClaim` can be used as well.

```yaml
spec:
  volumes:
    - name: "spark-local-dir-1"
      persistentVolumeClaim:
        claimName: network-file-storage
  executor:
    volumeMounts:
      - name: "spark-local-dir-1"
        mountPath: "/tmp/dir1"
```

## Using Termination Grace Period

A Spark Application can optionally specify a termination grace Period seconds to the driver and executor pods. More [info](https://kubernetes.io/docs/concepts/workloads/pods/pod/#termination-of-pods)

```yaml
spec:
  driver:
    terminationGracePeriodSeconds: 60
```

## Using Container LifeCycle Hooks

A Spark Application can optionally specify a [Container Lifecycle Hooks](https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks/#container-hooks) for a driver. It is useful in cases where you need a PreStop or PostStart hooks to driver and executor.

```yaml
spec:
  driver:
    lifecycle:
      preStop:
        exec:
          command:
          - /bin/bash
          - -c
          - touch /var/run/killspark && sleep 65
```

In cases like Spark Streaming or Spark Structured Streaming applications, you can test if a file exists to start a graceful shutdown and stop all streaming queries manually.

## Python Support

Python support can be enabled by setting `.spec.mainApplicationFile` with path to your python application. Optionally, the `.spec.pythonVersion` field can be used to set the major Python version of the docker image used to run the driver and executor containers. Below is an example showing part of a `SparkApplication` specification:

```yaml
spec:
  type: Python
  pythonVersion: 2
  mainApplicationFile: local:///opt/spark/examples/src/main/python/pyfiles.py
```

Some PySpark applications need additional Python packages to run. Such dependencies are specified using the optional field `.spec.deps.pyFiles`, which translates to the `--py-files` option of the spark-submit command.

```yaml
spec:
  deps:
    pyFiles:
       - local:///opt/spark/examples/src/main/python/py_container_checks.py
       - gs://spark-data/python-dep.zip
```

In order to use the dependencies that are hosted remotely, the following PySpark code can be used in Spark 2.4.

```python
python_dep_file_path = SparkFiles.get("python-dep.zip")
spark.sparkContext.addPyFile(dep_file_path)
```

Note that Python binding for PySpark is available in Apache Spark 2.4.

## Monitoring

The operator supports using the Spark metric system to expose metrics to a variety of sinks. Particularly, it is able to automatically configure the metric system to expose metrics to [Prometheus](https://prometheus.io/). Specifically, the field `.spec.monitoring` specifies how application monitoring is handled and particularly how metrics are to be reported. The metric system is configured through the configuration file `metrics.properties`, which gets its content from the field `.spec.monitoring.metricsProperties`. The content of [metrics.properties](https://github.com/kubeflow/spark-operator/blob/master/spark-docker/conf/metrics.properties) will be used by default if `.spec.monitoring.metricsProperties` is not specified. `.spec.monitoring.metricsPropertiesFile` overwrite the value `spark.metrics.conf` in spark.properties, and will not use content from `.spec.monitoring.metricsProperties`. You can choose to enable or disable reporting driver and executor metrics using the fields `.spec.monitoring.exposeDriverMetrics` and `.spec.monitoring.exposeExecutorMetrics`, respectively.

Further, the field `.spec.monitoring.prometheus` specifies how metrics are exposed to Prometheus using the [Prometheus JMX exporter](https://github.com/prometheus/jmx_exporter). When `.spec.monitoring.prometheus` is specified, the operator automatically configures the JMX exporter to run as a Java agent. The only required field of `.spec.monitoring.prometheus` is `jmxExporterJar`, which specified the path to the Prometheus JMX exporter Java agent jar in the container. If you use the image `gcr.io/spark-operator/spark:v3.1.1-gcs-prometheus`, the jar is located at `/prometheus/jmx_prometheus_javaagent-0.11.0.jar`. The field `.spec.monitoring.prometheus.port` specifies the port the JMX exporter Java agent binds to and defaults to `8090` if not specified. The field `.spec.monitoring.prometheus.configuration` specifies the content of the configuration to be used with the JMX exporter. The content of [prometheus.yaml](https://github.com/kubeflow/spark-operator/blob/master/spark-docker/conf/prometheus.yaml) will be used by default if `.spec.monitoring.prometheus.configuration` is not specified.

Below is an example that shows how to configure the metric system to expose metrics to Prometheus using the Prometheus JMX exporter. Note that the JMX exporter Java agent jar is listed as a dependency and will be downloaded to where `.spec.dep.jarsDownloadDir` points to in Spark 2.3.x, which is `/var/spark-data/spark-jars` by default. Things are different in Spark 2.4 as dependencies will be downloaded to the local working directory instead in Spark 2.4. A complete example can be found in [examples/spark-pi-prometheus.yaml](https://github.com/kubeflow/spark-operator/blob/master/examples/spark-pi-prometheus.yaml).

```yaml
spec:
  deps:
    jars:
    - http://central.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.11.0/jmx_prometheus_javaagent-0.11.0.jar
  monitoring:
    exposeDriverMetrics: true
    prometheus:
      jmxExporterJar: "/var/spark-data/spark-jars/jmx_prometheus_javaagent-0.11.0.jar"
```

The operator automatically adds the annotations such as `prometheus.io/scrape=true` on the driver and/or executor pods (depending on the values of  `.spec.monitoring.exposeDriverMetrics` and `.spec.monitoring.exposeExecutorMetrics`) so the metrics exposed on the pods can be scraped by the Prometheus server in the same cluster.

## Dynamic Allocation

The operator supports a limited form of [Spark Dynamic Resource Allocation](http://spark.apache.org/docs/latest/job-scheduling.html#dynamic-resource-allocation) through the shuffle tracking enhancement introduced in Spark 3.0.0 *without needing an external shuffle service* (not available in the Kubernetes mode). See this [issue](https://issues.apache.org/jira/browse/SPARK-27963) for details on the enhancement. To enable this limited form of dynamic allocation, follow the example below:

```yaml
spec:
  dynamicAllocation:
    enabled: true
    initialExecutors: 2
    minExecutors: 2
    maxExecutors: 10
```

Note that if dynamic allocation is enabled, the number of executors to request initially is set to the bigger of `.spec.dynamicAllocation.initialExecutors` and `.spec.executor.instances` if both are set.
