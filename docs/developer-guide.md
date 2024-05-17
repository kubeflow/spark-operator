# Developer Guide

## Configure Git Pre-Commit Hooks

Git hooks are useful for identifying simple issues before submission to code review. We run hooks on every commit to automatically generate helm chart `README.md` file from `README.md.gotmpl` file. Before you can run git hooks, you need to have the pre-commit package manager installed as follows:

```shell
# Using pip
pip install pre-commit

# Using conda
conda install -c conda-forge pre-commit

# Using Homebrew
brew install pre-commit
```

To set up the pre-commit hooks, run the following command:

```shell
pre-commit install

pre-commit install-hooks
```

## Build the Operator

In case you want to build the operator from the source code, e.g., to test a fix or a feature you write, you can do so following the instructions below.

The easiest way to build the operator without worrying about its dependencies is to just build an image using the [Dockerfile](../Dockerfile).

```bash
docker build -t <image-tag> .
```

The operator image is built upon a base Spark image that defaults to `spark:3.5.0`. If you want to use your own Spark image (e.g., an image with a different version of Spark or some custom dependencies), specify the argument `SPARK_IMAGE` as the following example shows:

```bash
docker build --build-arg SPARK_IMAGE=<your Spark image> -t <image-tag> .
```

If you want to use the operator on OpenShift clusters, first make sure you have Docker version 18.09.3 or above, then build your operator image using the [OpenShift-specific Dockerfile](../Dockerfile.rh).

```bash
export DOCKER_BUILDKIT=1
docker build -t <image-tag> -f Dockerfile.rh .
```

If you'd like to build/test the spark-operator locally, follow the instructions below:

```bash
mkdir -p $GOPATH/src/github.com/kubeflow
cd $GOPATH/src/github.com/kubeflow
git clone git@github.com:kubeflow/spark-operator.git
cd spark-operator
```

To update the auto-generated code, run the following command. (This step is only required if the CRD types have been changed):

```bash
hack/update-codegen.sh
```

To update the auto-generated CRD definitions, run the following command. After doing so, you must update the list of required fields under each `ports` field to add the `protocol` field to the list. Skipping this step will make the CRDs incompatible with Kubernetes v1.18+.

```bash
GO111MODULE=off go get -u sigs.k8s.io/controller-tools/cmd/controller-gen
controller-gen crd:trivialVersions=true,maxDescLen=0,crdVersions=v1beta1 paths="./pkg/apis/sparkoperator.k8s.io/v1beta2" output:crd:artifacts:config=./manifest/crds/
```

You can verify the current auto-generated code is up to date with:

```bash
hack/verify-codegen.sh
```

To build the operator, run the following command:

```bash
GOOS=linux go build -o spark-operator
```

To run unit tests, run the following command:

```bash
go test ./...
```

## Build the API Specification Doc

When you update the API, or specifically the `SparkApplication` and `ScheduledSparkApplication` specifications, the API specification doc needs to be updated. To update the API specification doc, run the following command:

```bash
make build-api-docs
```

Running the above command will update the file `docs/api-docs.md`.

## Develop with the Helm Chart

### Run helm chart lint

```shell
$ make helm-lint
Linting charts...

------------------------------------------------------------------------------------------------------------------------
 Charts to be processed:
------------------------------------------------------------------------------------------------------------------------
 spark-operator => (version: "1.2.4", path: "charts/spark-operator-chart")
------------------------------------------------------------------------------------------------------------------------

Linting chart "spark-operator => (version: \"1.2.4\", path: \"charts/spark-operator-chart\")"
Checking chart "spark-operator => (version: \"1.2.4\", path: \"charts/spark-operator-chart\")" for a version bump...
Old chart version: 1.2.1
New chart version: 1.2.4
Chart version ok.
Validating /Users/user/go/src/github.com/kubeflow/spark-operator/charts/spark-operator-chart/Chart.yaml...
Validation success! 👍
Validating maintainers...

Linting chart with values file "charts/spark-operator-chart/ci/ci-values.yaml"...

==> Linting charts/spark-operator-chart
[INFO] Chart.yaml: icon is recommended

1 chart(s) linted, 0 chart(s) failed

------------------------------------------------------------------------------------------------------------------------
 ✔︎ spark-operator => (version: "1.2.4", path: "charts/spark-operator-chart")
------------------------------------------------------------------------------------------------------------------------
All charts linted successfully
```

### Run helm chart unit tests

First, you need to install helm chart unit test plugin as follows:

```shell
helm plugin install https://github.com/helm-unittest/helm-unittest.git
```

For more information about how to write helm chart unit tests, please refer to [helm-unittest](https://github.com/helm-unittest/helm-unittest).

Then, run `make helm-unittest` to run the helm chart unit tests:

```shell
$ make helm-unittest

### Chart [ spark-operator ] charts/spark-operator-chart

 PASS  Test spark operator deployment   charts/spark-operator-chart/tests/deployment_test.yaml
 PASS  Test spark operator rbac charts/spark-operator-chart/tests/rbac_test.yaml
 PASS  Test spark operator service account      charts/spark-operator-chart/tests/serviceaccount_test.yaml
 PASS  Test spark rbac  charts/spark-operator-chart/tests/spark-rbac_test.yaml
 PASS  Test spark service account       charts/spark-operator-chart/tests/spark-serviceaccount_test.yaml
 PASS  Test spark operator webhook service      charts/spark-operator-chart/tests/webhook-service_test.yaml

Charts:      1 passed, 1 total
Test Suites: 6 passed, 6 total
Tests:       46 passed, 46 total
Snapshot:    0 passed, 0 total
Time:        107.861083ms
```

### Build the Helm Docs

The Helm chart `README.md` file is generated by [helm-docs](https://github.com/norwoodj/helm-docs) tool. If you want to update the Helm docs, remember to modify `README.md.gotmpl` rather than `README.md`, then run `make helm-docs` to generate the `README.md` file:

```shell
$ make helm-docs
INFO[2024-04-14T07:29:26Z] Found Chart directories [charts/spark-operator-chart] 
INFO[2024-04-14T07:29:26Z] Generating README Documentation for chart charts/spark-operator-chart 
```

Note that if git pre-commit hooks are set up, `helm-docs` will automatically run before committing any changes. If there are any changes to the `README.md` file, the commit process will be aborted.
