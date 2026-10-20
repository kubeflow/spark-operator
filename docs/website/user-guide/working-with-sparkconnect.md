# Working with SparkConnect

## Creating a SparkConnect

A `SparkConnect` can be created from a YAML file using `kubectl apply -f <file>`. The operator creates the server pod, the supporting `Service`, and a `ConfigMap` containing the executor pod template. The executor pods are created by Spark based on the configuration in the server pod's `spark-submit` arguments.

For a complete, end-to-end walkthrough (including prerequisites, the alpha-API caveat, client connection, and configuration reference), see [Using Spark Connect](spark-connect.md).

Apply the [`SparkConnect` example](https://github.com/kubeflow/spark-operator/blob/master/examples/sparkconnect/spark-connect.yaml):

```shell
kubectl apply -f examples/sparkconnect/spark-connect.yaml
```

## Deleting a SparkConnect

A `SparkConnect` can be deleted with `kubectl delete sparkconnect <name>`. Deleting the `SparkConnect` causes the operator to garbage-collect the server pod, the `Service`, and the `ConfigMap`. Executor pods are owned by Spark and are removed when the server shuts down.

## Updating a SparkConnect

A `SparkConnect` can be updated using `kubectl apply -f <updated file>`. The admission webhook re-validates the spec on every update.

The operator builds the server pod only on first creation, so changes to `spec.server.coreRequest`, `spec.server.coreLimit`, `spec.server.cores`, `spec.server.memory`, `spec.server.template`, or `spec.image` only take effect after the server pod is recreated (delete the existing pod, or delete and re-apply the `SparkConnect`). Other fields such as `spec.executor.*`, `spec.sparkConf`, `spec.dynamicAllocation`, and `spec.server.service` are applied to subsequent pod creations and Spark submissions.

## Checking a SparkConnect

A `SparkConnect` can be checked using the `kubectl describe sparkconnect <name>` command. The output shows the specification and status of the `SparkConnect` as well as the events associated with it. The events communicate the overall process and errors of the `SparkConnect`.

