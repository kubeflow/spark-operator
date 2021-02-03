# Developer Guide

## Build the Operator

In case you want to build the operator from the source code, e.g., to test a fix or a feature you write, you can do so following the instructions below.

The easiest way to build the operator without worrying about its dependencies is to just build an image using the [Dockerfile](../Dockerfile).

```bash
$ docker build -t <image-tag> .
```

The operator image is built upon a base Spark image that defaults to `gcr.io/spark-operator/spark:v3.1.1`. If you want to use your own Spark image (e.g., an image with a different version of Spark or some custom dependencies), specify the argument `SPARK_IMAGE` as the following example shows: 

```bash
$ docker build --build-arg SPARK_IMAGE=<your Spark image> -t <image-tag> .
```

If you want to use the operator on OpenShift clusters, first make sure you have Docker version 18.09.3 or above, then build your operator image using the [OpenShift-specific Dockerfile](../Dockerfile.rh).

```bash
$ export DOCKER_BUILDKIT=1
$ docker build -t <image-tag> -f Dockerfile.rh .
```

If you'd like to build/test the spark-operator locally, follow the instructions below:

```bash
$ mkdir -p $GOPATH/src/github.com/GoogleCloudPlatform
$ cd $GOPATH/src/github.com/GoogleCloudPlatform
$ git clone git@github.com:GoogleCloudPlatform/spark-on-k8s-operator.git
$ cd spark-on-k8s-operator
```

To update the auto-generated code, run the following command. (This step is only required if the CRD types have been changed):

```bash
$ hack/update-codegen.sh
```

To update the auto-generated CRD definitions, run the following command. After doing so, you must update the list of required fields under each `ports` field to add the `protocol` field to the list. Skipping this step will make the CRDs incompatible with Kubernetes v1.18+.

```bash
$ GO111MODULE=off go get -u sigs.k8s.io/controller-tools/cmd/controller-gen
$ controller-gen crd:trivialVersions=true,maxDescLen=0,crdVersions=v1beta1 paths="./pkg/apis/sparkoperator.k8s.io/v1beta2" output:crd:artifacts:config=./manifest/crds/
```

You can verify the current auto-generated code is up to date with:

```bash
$ hack/verify-codegen.sh
```

To build the operator, run the following command:

```bash
$ GOOS=linux go build -o spark-operator
```

To run unit tests, run the following command:

```bash
$ go test ./...
```

## Build the API Specification Doc

When you update the API, or specifically the `SparkApplication` and `ScheduledSparkApplication` specifications, the API specification doc needs to be updated. To update the API specification doc, run the following command:

```bash
make build-api-docs
```

Running the above command will update the file `docs/api-docs.md`.
