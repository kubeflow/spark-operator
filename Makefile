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

SPARK_OPERATOR_GOPATH=/go/src/github.com/kubeflow/spark-operator
DEP_VERSION:=`grep DEP_VERSION= Dockerfile | awk -F\" '{print $$2}'`
BUILDER=`grep "FROM golang:" Dockerfile | awk '{print $$2}'`
UNAME:=`uname | tr '[:upper:]' '[:lower:]'`
REPO=github.com/kubeflow/spark-operator
SPARK_OPERATOR_CHART_PATH=charts/spark-operator-chart

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

OPERATOR_VERSION ?= $$(grep appVersion $(SPARK_OPERATOR_CHART_PATH)/Chart.yaml | awk '{print $$2}')
# Image URL to use all building/pushing image targets
IMAGE_REPOSITORY ?= docker.io/kubeflow/spark-operator
IMAGE_TAG ?= $(OPERATOR_VERSION)
OPERATOR_IMAGE ?= $(IMAGE_REPOSITORY):$(IMAGE_TAG)

# Kind cluster
KIND_CLUSTER_NAME ?= spark-operator
KIND_CONFIG_FILE ?= charts/spark-operator-chart/ci/kind-config.yaml
KIND_KUBE_CONFIG ?= $(HOME)/.kube/config

# ENVTEST_K8S_VERSION refers to the version of kubebuilder assets to be downloaded by envtest binary.
ENVTEST_K8S_VERSION = 1.29.3

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

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate CustomResourceDefinition, RBAC and WebhookConfiguration manifests.
	$(CONTROLLER_GEN) crd rbac:roleName=spark-operator-controller webhook paths="./..." output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: update-crd
update-crd: manifests
	cp config/crd/bases/* charts/spark-operator-chart/crds/

.PHONY: clean
clean: ## Clean up caches and output
	@echo "cleaning up caches and output"
	go clean -cache -testcache -r -x 2>&1 >/dev/null
	-rm -rf _output

.PHONY: go-fmt
go-fmt: ## Run go fmt against code
	go fmt ./...

.PHONY: go-vet
go-vet: ## Run go vet against code.
	go vet ./...

.PHONY: unit-test
unit-test: manifests generate go-fmt go-vet envtest ## Run unit tests.
	@echo "Running unit tests..."
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)"
	go test $$(go list ./... | grep -v /e2e) -coverprofile cover.out

# Utilize Kind or modify the e2e tests to load the image locally, enabling compatibility with other vendors.
.PHONY: e2e-test
e2e-test: envtest kind-create-cluster ## Run the e2e tests against a Kind k8s instance that is spun up.
	@echo "Running e2e tests..."
	go test ./test/e2e/ -v -ginkgo.v -timeout 30m

.PHONY: integration-test
integration-test: clean ## Run integration tests
	@echo "Running integration tests..."
	go test -v ./test/e2e/ --kubeconfig "$(HOME)/.kube/config" --operator-image=gcr.io/spark-operator/spark-operator:local

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	$(GOLANGCI_LINT) run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	$(GOLANGCI_LINT) run --fix

.PHONY: fmt-check
fmt-check: clean ## Run go fmt against code
	@echo "Running fmt check"; cd "$(dirname $0)"; \
	if [ -n "$(go fmt ./...)" ]; \
	then \
		echo "Go code is not formatted, please run 'go fmt ./...'." >&2; \
		exit 1; \
	else \
		echo "Go code is formatted"; \
	fi

.PHONY: detect-crds-drift
detect-crds-drift:
	diff -q charts/spark-operator-chart/crds config/crd/bases

.PHONY: static-analysis
static-analysis:
	@echo "running go vet"
	# echo "Building using $(BUILDER)"
	# go vet ./...
	go vet $(REPO)...

.PHONY: helm-unittest
helm-unittest: ## Run Helm chart unittests
	helm unittest charts/spark-operator-chart --strict --file "tests/**/*_test.yaml"

.PHONY: helm-lint
helm-lint: ## Run Helm chart lint
	docker run --rm --workdir /workspace --volume "$$(pwd):/workspace" quay.io/helmpack/chart-testing:latest ct lint

.PHONY: helm-docs
helm-docs: ## Generates markdown documentation for helm charts from requirements and values files
	docker run --rm --volume "$$(pwd):/helm-docs" -u "$(id -u)" jnorwood/helm-docs:latest

##@ Build

.PHONY: build-operator
build-operator: ## Build Spark operator
	go build -o bin/spark-operator cmd/main.go

.PHONY: build-sparkctl
build-sparkctl: ## Build sparkctl binary
	[ ! -f "sparkctl/sparkctl-darwin-amd64" ] || [ ! -f "sparkctl/sparkctl-linux-amd64" ] && \
	echo building using $(BUILDER) && \
	docker run -w $(SPARK_OPERATOR_GOPATH) \
	-v $$(pwd):$(SPARK_OPERATOR_GOPATH) $(BUILDER) sh -c \
	"apk add --no-cache bash git && \
	cd sparkctl && \
	bash build.sh" || true

.PHONY: install-sparkctl
install-sparkctl: | sparkctl/sparkctl-darwin-amd64 sparkctl/sparkctl-linux-amd64 ## Install sparkctl binary
	@if [ "$(UNAME)" = "linux" ]; then \
		echo "installing linux binary to /usr/local/bin/sparkctl"; \
		sudo cp sparkctl/sparkctl-linux-amd64 /usr/local/bin/sparkctl; \
		sudo chmod +x /usr/local/bin/sparkctl; \
	elif [ "$(UNAME)" = "darwin" ]; then \
		echo "installing macOS binary to /usr/local/bin/sparkctl"; \
		cp sparkctl/sparkctl-darwin-amd64 /usr/local/bin/sparkctl; \
		chmod +x /usr/local/bin/sparkctl; \
	else \
		echo "$(UNAME) not supported"; \
	fi

.PHONY: clean-sparkctl
clean-sparkctl: ## Clean sparkctl binary
	rm -f sparkctl/sparkctl-darwin-amd64 sparkctl/sparkctl-linux-amd64

.PHONY: build-api-docs
build-api-docs: gen-crd-api-reference-docs
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
	$(CONTAINER_TOOL) build -t $(OPERATOR_IMAGE) .

.PHONY: docker-push
docker-push: ## Push docker image with the operator.
	$(CONTAINER_TOOL) push ${IMAGE_REPOSITORY}:${IMAGE_TAG}

# PLATFORMS defines the target platforms for the operator image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/arm64,linux/amd64
.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for the operator for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- $(CONTAINER_TOOL) buildx create --name spark-operator-builder
	$(CONTAINER_TOOL) buildx use spark-operator-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) --tag ${IMAGE_REPOSITORY}:${IMAGE_TAG} -f Dockerfile.cross .
	- $(CONTAINER_TOOL) buildx rm spark-operator-builder
	rm Dockerfile.cross

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: kind-create-cluster
kind-create-cluster: kind ## Create a kind cluster for integration tests.
	if ! $(KIND) get clusters 2>/dev/null | grep -q "^$(KIND_CLUSTER_NAME)$$"; then \
		kind create cluster --name $(KIND_CLUSTER_NAME) --config $(KIND_CONFIG_FILE) --kubeconfig $(KIND_KUBE_CONFIG); \
	fi

.PHONY: kind-load-image
kind-load-image: kind-create-cluster docker-build ## Load the image into the kind cluster.
	kind load docker-image --name $(KIND_CLUSTER_NAME) $(OPERATOR_IMAGE)

.PHONY: kind-delete-custer
kind-delete-custer: kind ## Delete the created kind cluster.
	$(KIND) delete cluster --name $(KIND_CLUSTER_NAME) && \
	rm -f $(KIND_KUBE_CONFIG)

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) apply -f -

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMG}
	$(KUSTOMIZE) build config/default | $(KUBECTL) apply -f -

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/default | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Binaries
KUBECTL ?= kubectl
KUSTOMIZE ?= $(LOCALBIN)/kustomize-$(KUSTOMIZE_VERSION)
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen-$(CONTROLLER_TOOLS_VERSION)
KIND ?= $(LOCALBIN)/kind-$(KIND_VERSION)
ENVTEST ?= $(LOCALBIN)/setup-envtest-$(ENVTEST_VERSION)
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint-$(GOLANGCI_LINT_VERSION)
GEN_CRD_API_REFERENCE_DOCS ?= $(LOCALBIN)/gen-crd-api-reference-docs-$(GEN_CRD_API_REFERENCE_DOCS_VERSION)

## Tool Versions
KUSTOMIZE_VERSION ?= v5.4.1
CONTROLLER_TOOLS_VERSION ?= v0.15.0
KIND_VERSION ?= v0.23.0
ENVTEST_VERSION ?= release-0.18
GOLANGCI_LINT_VERSION ?= v1.57.2
GEN_CRD_API_REFERENCE_DOCS_VERSION ?= v0.3.0

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
