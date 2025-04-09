.SILENT:

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

# Version information.
VERSION ?= $(shell cat VERSION | sed "s/^v//")
BUILD_DATE := $(shell date -u +"%Y-%m-%dT%H:%M:%S%:z")
GIT_COMMIT := $(shell git rev-parse HEAD)
GIT_TAG := $(shell if [ -z "`git status --porcelain`" ]; then git describe --exact-match --tags HEAD 2>/dev/null; fi)
GIT_TREE_STATE := $(shell if [ -z "`git status --porcelain`" ]; then echo "clean" ; else echo "dirty"; fi)
GIT_SHA := $(shell git rev-parse --short HEAD || echo "HEAD")
GIT_VERSION := ${VERSION}+${GIT_SHA}

REPO := github.com/kubeflow/spark-operator
SPARK_OPERATOR_GOPATH := /go/src/github.com/kubeflow/spark-operator
SPARK_OPERATOR_CHART_PATH := charts/spark-operator-chart
DEP_VERSION := `grep DEP_VERSION= Dockerfile | awk -F\" '{print $$2}'`
BUILDER := `grep "FROM golang:" Dockerfile | awk '{print $$2}'`
UNAME := `uname | tr '[:upper:]' '[:lower:]'`

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Image URL to use all building/pushing image targets
IMAGE_REGISTRY ?= ghcr.io
IMAGE_REPOSITORY ?= kubeflow/spark-operator/controller
IMAGE_TAG ?= $(VERSION)
IMAGE ?= $(IMAGE_REGISTRY)/$(IMAGE_REPOSITORY):$(IMAGE_TAG)

# Kind cluster
KIND_CLUSTER_NAME ?= spark-operator
KIND_CONFIG_FILE ?= charts/spark-operator-chart/ci/kind-config.yaml
KIND_KUBE_CONFIG ?= $(HOME)/.kube/config

## Location to install binaries
LOCALBIN ?= $(shell pwd)/bin

## Versions
KUSTOMIZE_VERSION ?= v5.4.1
CONTROLLER_TOOLS_VERSION ?= v0.17.1
KIND_VERSION ?= v0.23.0
KIND_K8S_VERSION ?= v1.32.0
ENVTEST_VERSION ?= release-0.20
# ENVTEST_K8S_VERSION refers to the version of kubebuilder assets to be downloaded by envtest binary.
ENVTEST_K8S_VERSION ?= 1.32.0
GOLANGCI_LINT_VERSION ?= v1.64.8
GEN_CRD_API_REFERENCE_DOCS_VERSION ?= v0.3.0
HELM_VERSION ?= v3.15.3
HELM_UNITTEST_VERSION ?= 0.5.1
HELM_DOCS_VERSION ?= v1.14.2

## Binaries
SPARK_OPERATOR ?= $(LOCALBIN)/spark-operator
KUBECTL ?= kubectl
KUSTOMIZE ?= $(LOCALBIN)/kustomize-$(KUSTOMIZE_VERSION)
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen-$(CONTROLLER_TOOLS_VERSION)
KIND ?= $(LOCALBIN)/kind-$(KIND_VERSION)
ENVTEST ?= $(LOCALBIN)/setup-envtest-$(ENVTEST_VERSION)
GOLANGCI_LINT ?= $(LOCALBIN)/golangci-lint-$(GOLANGCI_LINT_VERSION)
GEN_CRD_API_REFERENCE_DOCS ?= $(LOCALBIN)/gen-crd-api-reference-docs-$(GEN_CRD_API_REFERENCE_DOCS_VERSION)
HELM ?= $(LOCALBIN)/helm-$(HELM_VERSION)
HELM_DOCS ?= $(LOCALBIN)/helm-docs-$(HELM_DOCS_VERSION)

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-30s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: version
version: ## Print version information.
	@echo "Version: ${VERSION}"
	@echo "Build Date: ${BUILD_DATE}"
	@echo "Git Commit: ${GIT_COMMIT}"
	@echo "Git Tag: ${GIT_TAG}"
	@echo "Git Tree State: ${GIT_TREE_STATE}"
	@echo "Git SHA: ${GIT_SHA}"
	@echo "Git Version: ${GIT_VERSION}"

.PHONY: print-%
print-%: ; @echo $*=$($*)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate CustomResourceDefinition, RBAC and WebhookConfiguration manifests.
	$(CONTROLLER_GEN) crd:generateEmbeddedObjectMeta=true rbac:roleName=spark-operator-controller webhook paths="./..." output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: update-crd
update-crd: manifests ## Update CRD files in the Helm chart.
	cp config/crd/bases/* charts/spark-operator-chart/crds/

.PHONY: go-clean
go-clean: ## Clean up caches and output.
	@echo "cleaning up caches and output"
	go clean -cache -testcache -r -x 2>&1 >/dev/null
	-rm -rf _output

.PHONY: go-fmt
go-fmt: ## Run go fmt against code.
	@echo "Running go fmt..."
	if [ -n "$(shell go fmt ./...)" ]; then \
		echo "Go code is not formatted, need to run \"make go-fmt\" and commit the changes."; \
		false; \
	else \
	    echo "Go code is formatted."; \
	fi

.PHONY: go-vet
go-vet: ## Run go vet against code.
	@echo "Running go vet..."
	go vet ./...

.PHONY: go-lint
go-lint: golangci-lint ## Run golangci-lint linter.
	@echo "Running golangci-lint run..."
	$(GOLANGCI_LINT) run

.PHONY: go-lint-fix
go-lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes.
	@echo "Running golangci-lint run --fix..."
	$(GOLANGCI_LINT) run --fix

.PHONY: unit-test
unit-test: envtest ## Run unit tests.
	@echo "Running unit tests..."
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)"
	go test $(shell go list ./... | grep -v /e2e) -coverprofile cover.out

.PHONY: e2e-test
e2e-test: envtest ## Run the e2e tests against a Kind k8s instance that is spun up.
	@echo "Running e2e tests..."
	go test ./test/e2e/ -v -ginkgo.v -timeout 30m

##@ Build

override LDFLAGS += \
  -X ${REPO}.version=${GIT_VERSION} \
  -X ${REPO}.buildDate=${BUILD_DATE} \
  -X ${REPO}.gitCommit=${GIT_COMMIT} \
  -X ${REPO}.gitTreeState=${GIT_TREE_STATE} \
  -extldflags "-static"

.PHONY: build-operator
build-operator: ## Build Spark operator.
	echo "Building spark-operator binary..."
	go build -o $(SPARK_OPERATOR) -ldflags '${LDFLAGS}' cmd/operator/main.go

.PHONY: clean
clean: ## Clean binaries.
	rm -f $(SPARK_OPERATOR)

.PHONY: build-api-docs
build-api-docs: gen-crd-api-reference-docs ## Build api documentation.
	$(GEN_CRD_API_REFERENCE_DOCS) \
	-config hack/api-docs/config.json \
	-api-dir github.com/kubeflow/spark-operator/api/v1beta2 \
	-template-dir hack/api-docs/template \
	-out-file docs/api-docs.md

# If you wish to build the operator image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
.PHONY: docker-build
docker-build: ## Build docker image with the operator.
	$(CONTAINER_TOOL) build -t ${IMAGE} .

.PHONY: docker-push
docker-push: ## Push docker image with the operator.
	$(CONTAINER_TOOL) push ${IMAGE}

# PLATFORMS defines the target platforms for the operator image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/amd64,linux/arm64
.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for the operator for cross-platform support
	- $(CONTAINER_TOOL) buildx create --name spark-operator-builder
	$(CONTAINER_TOOL) buildx use spark-operator-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) --tag ${IMAGE} -f Dockerfile .
	- $(CONTAINER_TOOL) buildx rm spark-operator-builder

##@ Helm

.PHONY: detect-crds-drift
detect-crds-drift: manifests ## Detect CRD drift.
	diff -q $(SPARK_OPERATOR_CHART_PATH)/crds config/crd/bases

.PHONY: helm-unittest
helm-unittest: helm-unittest-plugin ## Run Helm chart unittests.
	$(HELM) unittest $(SPARK_OPERATOR_CHART_PATH) --strict --file "tests/**/*_test.yaml"

.PHONY: helm-lint
helm-lint: ## Run Helm chart lint test.
	docker run --rm --workdir /workspace --volume "$$(pwd):/workspace" quay.io/helmpack/chart-testing:latest ct lint --target-branch master --validate-maintainers=false

.PHONY: helm-docs
helm-docs: helm-docs-plugin ## Generates markdown documentation for helm charts from requirements and values files.
	$(HELM_DOCS) --sort-values-order=file

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: kind-create-cluster
kind-create-cluster: kind ## Create a kind cluster for integration tests.
	if ! $(KIND) get clusters 2>/dev/null | grep -q "^$(KIND_CLUSTER_NAME)$$"; then \
		$(KIND) create cluster \
			--name $(KIND_CLUSTER_NAME) \
			--config $(KIND_CONFIG_FILE) \
			--image kindest/node:$(KIND_K8S_VERSION) \
			--kubeconfig $(KIND_KUBE_CONFIG) \
			--wait=1m; \
	fi

.PHONY: kind-load-image
kind-load-image: kind-create-cluster docker-build ## Load the image into the kind cluster.
	$(KIND) load docker-image --name $(KIND_CLUSTER_NAME) $(IMAGE)

.PHONY: kind-delete-cluster
kind-delete-cluster: kind ## Delete the created kind cluster.
	$(KIND) delete cluster --name $(KIND_CLUSTER_NAME) --kubeconfig $(KIND_KUBE_CONFIG)

.PHONY: install
install-crd: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) create -f - 2>/dev/null || $(KUSTOMIZE) build config/crd | $(KUBECTL) replace -f -

.PHONY: uninstall
uninstall-crd: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

.PHONY: deploy
deploy: IMAGE_TAG=local
deploy: helm manifests update-crd kind-load-image ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	$(HELM) upgrade --install -f charts/spark-operator-chart/ci/ci-values.yaml spark-operator ./charts/spark-operator-chart/

.PHONY: undeploy
undeploy: helm ## Uninstall spark-operator
	$(HELM) uninstall spark-operator

##@ Dependencies

$(LOCALBIN):
	mkdir -p $(LOCALBIN)

.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: kind
kind: $(KIND) ## Download kind locally if necessary.
$(KIND): $(LOCALBIN)
	$(call go-install-tool,$(KIND),sigs.k8s.io/kind,$(KIND_VERSION))

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/cmd/golangci-lint,${GOLANGCI_LINT_VERSION})

.PHONY: gen-crd-api-reference-docs
gen-crd-api-reference-docs: $(GEN_CRD_API_REFERENCE_DOCS) ## Download gen-crd-api-reference-docs locally if necessary.
$(GEN_CRD_API_REFERENCE_DOCS): $(LOCALBIN)
	$(call go-install-tool,$(GEN_CRD_API_REFERENCE_DOCS),github.com/ahmetb/gen-crd-api-reference-docs,$(GEN_CRD_API_REFERENCE_DOCS_VERSION))

.PHONY: helm
helm: $(HELM) ## Download helm locally if necessary.
$(HELM): $(LOCALBIN)
	$(call go-install-tool,$(HELM),helm.sh/helm/v3/cmd/helm,$(HELM_VERSION))

.PHONY: helm-unittest-plugin
helm-unittest-plugin: helm ## Download helm unittest plugin locally if necessary.
	if [ -z "$(shell $(HELM) plugin list | grep unittest)" ]; then \
		echo "Installing helm unittest plugin"; \
		$(HELM) plugin install https://github.com/helm-unittest/helm-unittest.git --version $(HELM_UNITTEST_VERSION); \
	fi

.PHONY: helm-docs-plugin
helm-docs-plugin: $(HELM_DOCS) ## Download helm-docs plugin locally if necessary.
$(HELM_DOCS): $(LOCALBIN)
	$(call go-install-tool,$(HELM_DOCS),github.com/norwoodj/helm-docs/cmd/helm-docs,$(HELM_DOCS_VERSION))

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary (ideally with version)
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f $(1) ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
GOBIN=$(LOCALBIN) go install $${package} ;\
mv "$$(echo "$(1)" | sed "s/-$(3)$$//")" $(1) ;\
}
endef
