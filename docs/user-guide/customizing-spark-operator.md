# Customizing Spark Operator

To customize the operator, you can follow the steps below:

1. Compile Spark distribution with Kubernetes support as per [Spark documentation](https://spark.apache.org/docs/latest/building-spark.html#building-with-kubernetes-support).
2. Create docker images to be used for Spark with [docker-image tool](https://spark.apache.org/docs/latest/running-on-kubernetes.html#docker-images).
3. Create a new operator image based on the above image. You need to modify the `FROM` tag in the [Dockerfile](https://github.com/kubeflow/spark-operator/blob/master/Dockerfile) with your Spark image.
4. Build and push your operator image built above.
5. Deploy the new image by modifying the [/manifest/spark-operator-install/spark-operator.yaml](https://github.com/kubeflow/spark-operator/blob/master/manifest/spark-operator-install/spark-operator.yaml) file and specifying your operator image.
