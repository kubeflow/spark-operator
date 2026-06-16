# Running Multiple Instances of the Spark Operator

If you need to run multiple instances of the Spark operator within the same k8s cluster, then you need to ensure that the running instances should not watch the same spark job namespace.
For example, you can deploy two Spark operator instances in the `spark-operator` namespace, one with release name `spark-operator-1` which watches the `spark-1` namespace:

```bash
# Create the spark-1 namespace if it does not exist
kubectl create ns spark-1

# Install the Spark operator with release name spark-operator-1
helm install spark-operator-1 spark-operator/spark-operator \
    --namespace spark-operator \
    --create-namespace \
    --set 'spark.jobNamespaces={spark-1}'
```

And then deploy another one with release name `spark-operator-2` which watches the `spark-2` namespace:

```bash
# Create the spark-2 namespace if it does not exist
kubectl create ns spark-2

# Install the Spark operator with release name spark-operator-2
helm install spark-operator-2 spark-operator/spark-operator \
    --namespace spark-operator \
    --create-namespace \
    --set 'spark.jobNamespaces={spark-2}'
```
