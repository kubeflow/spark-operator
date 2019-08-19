# Developer Guide

## Build the Operator

In case you want to build the operator from the source code, e.g., to test a fix or a feature you write, you can do so following the instructions below.

The easiest way to build the operator without worrying about its dependencies is to just build an image using the [Dockerfile](../Dockerfile).

```bash
$ docker build -t <image-tag> .
```

The operator image is built upon a base Spark image that defaults to `gcr.io/spark-operator/spark:v2.4.0`. If you want to use your own Spark image (e.g., an image with a different version of Spark or some custom dependencies), specify the argument `SPARK_IMAGE` as the following example shows: 

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

The operator uses [dep](https://golang.github.io/dep/) for dependency management. Please install `dep` following
the instruction on the website if you don't have it available locally. To install the dependencies, run the following command:

```bash
$ dep ensure
```

To update the dependencies, run the following command. (You can skip this unless you know there's a dependency that needs updating):

```bash
$ dep ensure -update
```

Before building the operator the first time, run the following commands to get the required Kubernetes code generators:

```bash
$ go get -u k8s.io/code-generator/cmd/client-gen
$ go get -u k8s.io/code-generator/cmd/deepcopy-gen
$ go get -u k8s.io/code-generator/cmd/defaulter-gen
```

To update the auto-generated code, run the following command. (This step is only required if the CRD types have been changed):

```bash
$ go generate
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
