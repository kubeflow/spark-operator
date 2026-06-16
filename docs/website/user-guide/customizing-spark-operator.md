# Customizing Spark Operator

To customize the operator, you can follow the steps below:

1. Compile Spark distribution with Kubernetes support as per [Spark documentation](https://spark.apache.org/docs/latest/building-spark.html#building-with-kubernetes-support).
2. Create docker images to be used for Spark with [docker-image tool](https://spark.apache.org/docs/latest/running-on-kubernetes.html#docker-images).
3. Create a new operator image based on the above image. You need to modify the `FROM` tag in the [Dockerfile](https://github.com/kubeflow/spark-operator/blob/master/Dockerfile) with your Spark image.

4. Build and push multi-arch operator image to your own image registry by running the following command ([docker buildx](https://github.com/docker/buildx) is needed):

    ```bash
    make docker-build IMAGE_REGISTRY=docker.io IMAGE_REPOSITORY=kubeflow/spark-operator IMAGE_TAG=latest PLATFORMS=linux/amd64,linux/arm64
    ```

5. Deploy the Spark operator Helm chart by specifying your own operator image:

    ```bash
    helm repo add --force-update spark-operator https://kubeflow.github.io/spark-operator

    helm install spark-operator spark-operator/spark-operator \
        --namespace spark-operator \
        --create-namespace \
        --set image.registry=docker.io \
        --set image.repository=kubeflow/spark-operator \
        --set image.tag=latest
    ```
