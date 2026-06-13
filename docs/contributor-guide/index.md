# Contributor Guide

## Clone the Repository

Clone the Spark operator repository and change to the directory:

```bash
git clone git@github.com:kubeflow/spark-operator.git

cd spark-operator
```

## (Optional) Configure Git Pre-Commit Hooks

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

## Use Makefile

We use Makefile to automate common tasks. For example, to build the operator, run the `build-operator` target as follows, and `spark-operator` binary will be build and placed in the `bin` directory:

```bash
make build-operator
```

Dependencies will be automatically downloaded locally to `bin` directory as needed. For example, if you run `make manifests` target, then `controller-gen` tool will be automatically downloaded using `go install` command and then it will be renamed like `controller-gen-vX.Y.Z` and placed in the `bin` directory.

To see the full list of available targets, run the following command:

```bash
$ make help

Usage:
  make <target>

General
  help                            Display this help.
  version                         Print version information.

Development
  manifests                       Generate CustomResourceDefinition, RBAC and WebhookConfiguration manifests.
  generate                        Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
  update-crd                      Update CRD files in the Helm chart.
  go-clean                        Clean up caches and output.
  go-fmt                          Run go fmt against code.
  go-vet                          Run go vet against code.
  go-lint                         Run golangci-lint linter.
  go-lint-fix                     Run golangci-lint linter and perform fixes.
  unit-test                       Run unit tests.
  e2e-test                        Run the e2e tests against a Kind k8s instance that is spun up.

Build
  build-operator                  Build Spark operator.
  clean                           Clean binaries.
  build-api-docs                  Build api documentation.
  docker-build                    Build docker image with the operator.
  docker-push                     Push docker image with the operator.
  docker-buildx                   Build and push docker image for the operator for cross-platform support

Helm
  detect-crds-drift               Detect CRD drift.
  helm-unittest                   Run Helm chart unittests.
  helm-lint                       Run Helm chart lint test.
  helm-docs                       Generates markdown documentation for helm charts from requirements and values files.

Deployment
  kind-create-cluster             Create a kind cluster for integration tests.
  kind-load-image                 Load the image into the kind cluster.
  kind-delete-cluster             Delete the created kind cluster.
  install-crd                     Install CRDs into the K8s cluster specified in ~/.kube/config.
  uninstall-crd                   Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
  deploy                          Deploy controller to the K8s cluster specified in ~/.kube/config.
  undeploy                        Uninstall spark-operator

Dependencies
  kustomize                       Download kustomize locally if necessary.
  controller-gen                  Download controller-gen locally if necessary.
  kind                            Download kind locally if necessary.
  envtest                         Download setup-envtest locally if necessary.
  golangci-lint                   Download golangci-lint locally if necessary.
  gen-crd-api-reference-docs      Download gen-crd-api-reference-docs locally if necessary.
  helm                            Download helm locally if necessary.
  helm-unittest-plugin            Download helm unittest plugin locally if necessary.
  helm-docs-plugin                Download helm-docs plugin locally if necessary.
```

## Develop with Spark Operator

### Build the Binary

To build the operator, run the following command:

```shell
make build-operator
```

### Build the Docker Image

In case you want to build the operator from the source code, e.g., to test a fix or a feature you write, you can do so following the instructions below.

The easiest way to build the operator without worrying about its dependencies is to just build an image using the [Dockerfile](https://github.com/kubeflow/spark-operator/Dockerfile).

```shell
make docker-build IMAGE_TAG=<image-tag>
```

The operator image is built upon a base Spark image that defaults to `spark:3.5.2`. If you want to use your own Spark image (e.g., an image with a different version of Spark or some custom dependencies), specify the argument `SPARK_IMAGE` as the following example shows:

```shell
docker build --build-arg SPARK_IMAGE=<your Spark image> -t <image-tag> .
```

### Update the API definition

If you have updated the API definition, then you also need to update the auto-generated code. To update the auto-generated code which contains DeepCopy, DeepCopyInto, and DeepCopyObject method implementations, run the following command:

```shell
make generate
```

To update the auto-generated CustomResourceDefinition (CRD), RBAC and WebhookConfiguration manifests, run the following command:

```shell
make manifests
```

After updating the CRD files, run the following command to copy the CRD files to the helm chart directory:

```shell
make update-crd
```

Besides, the API specification documentation `docs/api-docs.md` also needs to be updated. To update the doc, run the following command:

```shell
make build-api-docs
```

### Run Unit Tests

To run unit tests, run the following command:

```shell
make unit-test
```

### Run E2E Tests

To run e2e tests, run the following command:

```shell
# Create a kind cluster
make kind-create-cluster

# Build docker image
make docker-build IMAGE_TAG=local

# Load docker image to kind cluster
make kind-load-image IMAGE_TAG=local

# Run e2e tests
make e2e-test

# Delete the kind cluster
make kind-delete-cluster
```

## Develop with the Helm Chart

### Run Helm Chart Lint Tests

To run Helm chart lint tests, run the following command:

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

### Run Helm chart unit tests

For detailed information about how to write Helm chart unit tests, please refer to [helm-unittest](https://github.com/helm-unittest/helm-unittest). To run the Helm chart unit tests, run the following command:

```shell
$ make helm-unittest

### Chart [ spark-operator ] charts/spark-operator-chart

 PASS  Test controller deployment       charts/spark-operator-chart/tests/controller/deployment_test.yaml
 PASS  Test controller pod disruption budget    charts/spark-operator-chart/tests/controller/poddisruptionbudget_test.yaml
 PASS  Test controller rbac     charts/spark-operator-chart/tests/controller/rbac_test.yaml
 PASS  Test controller deployment       charts/spark-operator-chart/tests/controller/service_test.yaml
 PASS  Test controller service account  charts/spark-operator-chart/tests/controller/serviceaccount_test.yaml
 PASS  Test prometheus pod monitor      charts/spark-operator-chart/tests/prometheus/podmonitor_test.yaml
 PASS  Test Spark RBAC  charts/spark-operator-chart/tests/spark/rbac_test.yaml
 PASS  Test spark service account       charts/spark-operator-chart/tests/spark/serviceaccount_test.yaml
 PASS  Test webhook deployment  charts/spark-operator-chart/tests/webhook/deployment_test.yaml
 PASS  Test mutating webhook configuration      charts/spark-operator-chart/tests/webhook/mutatingwebhookconfiguration_test.yaml
 PASS  Test webhook pod disruption budget       charts/spark-operator-chart/tests/webhook/poddisruptionbudget_test.yaml
 PASS  Test webhook rbac        charts/spark-operator-chart/tests/webhook/rbac_test.yaml
 PASS  Test webhook service     charts/spark-operator-chart/tests/webhook/service_test.yaml
 PASS  Test validating webhook configuration    charts/spark-operator-chart/tests/webhook/validatingwebhookconfiguration_test.yaml

Charts:      1 passed, 1 total
Test Suites: 14 passed, 14 total
Tests:       137 passed, 137 total
Snapshot:    0 passed, 0 total
Time:        477.748ms
```

### Build the Helm Docs

The Helm chart `README.md` file is generated by [helm-docs](https://github.com/norwoodj/helm-docs) tool. If you want to update the Helm docs, remember to modify `README.md.gotmpl` rather than `README.md`, then run `make helm-docs` to generate the `README.md` file:

```shell
$ make helm-docs
INFO[2024-04-14T07:29:26Z] Found Chart directories [charts/spark-operator-chart]
INFO[2024-04-14T07:29:26Z] Generating README Documentation for chart charts/spark-operator-chart
```

Note that if git pre-commit hooks are set up, `helm-docs` will automatically run before committing any changes. If there are any changes to the `README.md` file, the commit process will be aborted.

## Sign off your commits

After you have made changes to the code, please sign off your commits with `-s` or `--signoff` flag so that the DCO check CI will pass:

```bash
git commit -s -m "Your commit message"
```
