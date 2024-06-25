# Using SparkApplications

The operator runs Spark applications specified in Kubernetes objects of the `SparkApplication` custom resource type. The most common way of using a `SparkApplication` is store the `SparkApplication` specification in a YAML file and use the `kubectl` command or alternatively the `sparkctl` command to work with the `SparkApplication`. The operator automatically submits the application as configured in a `SparkApplication` to run on the Kubernetes cluster and uses the `SparkApplication` to collect and surface the status of the driver and executors to the user.

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
