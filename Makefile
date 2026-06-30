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
GIT_VERSION := v${VERSION}

MODULE_PATH := $(shell awk '/^module/{print $$2; exit}' go.mod)
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

# Deployment method for e2e tests (helm or kustomize)
DEPLOY_METHOD ?= helm

# Kind cluster
KIND_CLUSTER_NAME ?= spark-operator
KIND_CONFIG_FILE ?= charts/spark-operator-chart/ci/kind-config.yaml
KIND_KUBE_CONFIG ?= $(CURDIR)/.kube/config

## Location to install binaries
LOCALBIN ?= $(shell pwd)/bin

## Versions
CONTROLLER_TOOLS_VERSION ?= v0.20.1
KIND_VERSION ?= v0.31.0
KIND_K8S_VERSION ?= v1.35.0
ENVTEST_VERSION ?= release-0.20
# ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= $(shell go list -m -f "{{ .Version }}" k8s.io/api | awk -F'[v.]' '{printf "1.%d", $$3}')
GOLANGCI_LINT_VERSION ?= v2.1.6
GEN_CRD_API_REFERENCE_DOCS_VERSION ?= v0.3.0
HELM_VERSION ?= $(shell grep -e '^	helm.sh/helm/v3 v' go.mod | cut -d ' ' -f 2)
HELM_UNITTEST_VERSION ?= 0.8.2
HELM_DOCS_VERSION ?= v1.14.2
CODE_GENERATOR_VERSION ?= v0.35.4
SHFMT_VERSION ?= v3.13.1
SHELLCHECK_VERSION ?= v0.11.0

## Binaries
SPARK_OPERATOR ?= $(LOCALBIN)/spark-operator
KUBECTL ?= kubectl
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen-$(CONTROLLER_TOOLS_VERSION)
KIND ?= $(LOCALBIN)/kind-$(KIND_VERSION)
ENVTEST ?= $(LOCALBIN)/setup-envtest-$(ENVTEST_VERSION)
GOLANGCI_LINT ?= $(LOCALBIN)/golangci-lint-$(GOLANGCI_LINT_VERSION)
GEN_CRD_API_REFERENCE_DOCS ?= $(LOCALBIN)/gen-crd-api-reference-docs-$(GEN_CRD_API_REFERENCE_DOCS_VERSION)
HELM ?= $(LOCALBIN)/helm-$(HELM_VERSION)
HELM_DOCS ?= $(LOCALBIN)/helm-docs-$(HELM_DOCS_VERSION)
SHFMT ?= $(LOCALBIN)/shfmt-$(SHFMT_VERSION)
SHELLCHECK ?= $(LOCALBIN)/shellcheck-$(SHELLCHECK_VERSION)

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
	@echo "Git Version: ${GIT_VERSION}"
	@echo "Git Commit: ${GIT_COMMIT}"
	@echo "Git Tree State: ${GIT_TREE_STATE}"
	@echo "Build Date: ${BUILD_DATE}"

.PHONY: print-%
print-%: ; @echo $*=$($*)

##@ Development

CONTROLLER_GEN_PATHS := ./api/...;./internal/...

.PHONY: manifests
manifests: controller-gen ## Generate CustomResourceDefinition, RBAC and WebhookConfiguration manifests.
	$(CONTROLLER_GEN) crd:generateEmbeddedObjectMeta=true rbac:roleName=spark-operator-controller webhook paths="$(CONTROLLER_GEN_PATHS)" output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate: controller-gen manifests ## Generate Go code and Python APIs.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="$(CONTROLLER_GEN_PATHS)"
	$(MAKE) python-api

.PHONY: update-crd
update-crd: manifests ## Update CRD files in the Helm chart.
	cp config/crd/bases/* charts/spark-operator-chart/crds/

.PHONY: verify-codegen
verify-codegen: $(LOCALBIN) ## Install code-generator commands and verify changes
	$(call go-install-tool,$(LOCALBIN)/register-gen-$(CODE_GENERATOR_VERSION),k8s.io/code-generator/cmd/register-gen,$(CODE_GENERATOR_VERSION))
	$(call go-install-tool,$(LOCALBIN)/client-gen-$(CODE_GENERATOR_VERSION),k8s.io/code-generator/cmd/client-gen,$(CODE_GENERATOR_VERSION))
	$(call go-install-tool,$(LOCALBIN)/lister-gen-$(CODE_GENERATOR_VERSION),k8s.io/code-generator/cmd/lister-gen,$(CODE_GENERATOR_VERSION))
	$(call go-install-tool,$(LOCALBIN)/informer-gen-$(CODE_GENERATOR_VERSION),k8s.io/code-generator/cmd/informer-gen,$(CODE_GENERATOR_VERSION))
	./hack/verify-codegen.sh

.PHONY: python-api
python-api: manifests ## Generate Python APIs from CRDs.
	hack/openapi/gen-openapi.sh
	CONTAINER_TOOL=$(CONTAINER_TOOL) hack/python-api/gen-api.sh

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

# Shell scripts to format and lint (all tracked *.sh files).
SHELL_SCRIPTS = $(shell git ls-files '*.sh')

# shfmt options: 2-space indent, indent switch cases, space after redirect operators.
SHFMT_OPTIONS ?= --indent 2 --case-indent --space-redirects

# Extra shellcheck options, e.g. set SHELLCHECK_OPTIONS=--severity=warning to only fail on warnings.
SHELLCHECK_OPTIONS ?=

.PHONY: shell-fmt
shell-fmt: shfmt ## Format shell scripts with shfmt.
	@echo "Running shfmt..."
	$(SHFMT) --write --list $(SHFMT_OPTIONS) $(SHELL_SCRIPTS)

.PHONY: shell-lint
shell-lint: shellcheck ## Lint shell scripts with shellcheck.
	@echo "Running shellcheck..."
	$(SHELLCHECK) $(SHELLCHECK_OPTIONS) $(SHELL_SCRIPTS)

.PHONY: unit-test
unit-test: setup-envtest ## Run unit tests.
	@echo "Running unit tests..."
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)"
	go test $(shell go list ./... | grep -v -e /e2e -e /drift) -coverprofile cover.out
	@echo "Generating HTML coverage report..."
	go tool cover -html=cover.out -o cover.html
	@echo "Coverage report available at cover.html"

.PHONY: e2e-test
e2e-test: IMAGE_TAG=local
e2e-test: envtest kind-create-cluster kind-load-image ## Run the e2e tests against a Kind k8s instance that is spun up.
	@echo "Running e2e tests (deploy_method=$(DEPLOY_METHOD))..."
	DEPLOY_METHOD=$(DEPLOY_METHOD) IMAGE_TAG=$(IMAGE_TAG) KUBECONFIG=$(KIND_KUBE_CONFIG) go test ./test/e2e/ -v -ginkgo.v -timeout 30m

##@ Kustomize

.PHONY: kustomize-set-image
kustomize-set-image: ## Update config/default/kustomization.yaml image tag from VERSION file.
	@TAG=$$(cat VERSION) && \
	sed -i.bak "s|    newTag: .*|    newTag: $$TAG|" config/default/kustomization.yaml && \
	rm -f config/default/kustomization.yaml.bak && \
	echo "Updated kustomize image tag to $$TAG"

.PHONY: kustomize-lint
kustomize-lint: ## Validate Kustomize build output (no cluster needed).
	@echo "Running Kustomize build validation..."
	go test ./test/kustomize/ -v -count=1

##@ Build

override LDFLAGS += \
  -X k8s.io/component-base/version.gitVersion=${GIT_VERSION} \
  -X k8s.io/component-base/version.gitCommit=${GIT_COMMIT} \
  -X k8s.io/component-base/version.gitTreeState=${GIT_TREE_STATE} \
  -X k8s.io/component-base/version.buildDate=${BUILD_DATE} \
  -extldflags "-static"

.PHONY: build-operator
build-operator: ## Build Spark operator.
	echo "Building spark-operator binary..."
	CGO_ENABLED=0 go build -o $(SPARK_OPERATOR) -ldflags '${LDFLAGS}' cmd/operator/main.go

.PHONY: clean
clean: ## Clean binaries.
	rm -f $(SPARK_OPERATOR)

.PHONY: build-api-docs
build-api-docs: gen-crd-api-reference-docs ## Build api documentation.
	$(GEN_CRD_API_REFERENCE_DOCS) \
	-config hack/api-docs/config.json \
	-api-dir github.com/kubeflow/spark-operator/v2/api/v1beta2 \
	-template-dir hack/api-docs/template \
	-out-file docs/api-docs.md

##@ Documentation

.PHONY: docs
docs: ## Build the documentation website (HTML) with Sphinx.
	cd docs/website && $(MAKE) html

.PHONY: docs-test
docs-test: ## Build the documentation website strictly (warnings treated as errors).
	cd docs/website && $(MAKE) test

.PHONY: docs-serve
docs-serve: ## Build and serve the documentation website locally with live reload.
	cd docs/website && $(MAKE) serve

.PHONY: docs-linkcheck
docs-linkcheck: ## Check all links in the documentation website.
	cd docs/website && $(MAKE) linkcheck

.PHONY: docs-clean
docs-clean: ## Remove documentation website build artifacts.
	cd docs/website && $(MAKE) clean

##@ Build

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

.PHONY: drift-check
drift-check: helm ## Detect drift between Helm chart and Kustomize manifests.
	HELM=$(HELM) go test ./test/drift/ -v -count=1

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
install: install-crd ## Install CRDs into the K8s cluster specified in .kube/config.
install-crd: manifests ## Install CRDs into the K8s cluster specified in .kube/config.
	$(KUBECTL) kustomize config/crd | KUBECONFIG=$(KIND_KUBE_CONFIG) $(KUBECTL) create -f - 2>/dev/null || $(KUBECTL) kustomize config/crd | KUBECONFIG=$(KIND_KUBE_CONFIG) $(KUBECTL) replace -f -

.PHONY: uninstall
uninstall: uninstall-crd ## Uninstall CRDs from the K8s cluster specified in .kube/config.
uninstall-crd: manifests ## Uninstall CRDs from the K8s cluster specified in .kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUBECTL) kustomize config/crd | KUBECONFIG=$(KIND_KUBE_CONFIG) $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

.PHONY: deploy
deploy: IMAGE_TAG=local
deploy: helm manifests update-crd kind-load-image ## Deploy controller to the K8s cluster specified in .kube/config.
	KUBECONFIG=$(KIND_KUBE_CONFIG) $(HELM) upgrade --install -f charts/spark-operator-chart/ci/ci-values.yaml spark-operator ./charts/spark-operator-chart/

.PHONY: undeploy
undeploy: helm ## Uninstall spark-operator
	KUBECONFIG=$(KIND_KUBE_CONFIG) $(HELM) uninstall spark-operator

##@ Dependencies

$(LOCALBIN):
	mkdir -p $(LOCALBIN)

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: kind
kind: $(KIND) ## Download kind locally if necessary.
$(KIND): $(LOCALBIN)
	$(call go-install-tool,$(KIND),sigs.k8s.io/kind,$(KIND_VERSION))

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@"$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,${GOLANGCI_LINT_VERSION})

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

.PHONY: shfmt
shfmt: $(SHFMT) ## Download shfmt locally if necessary.
$(SHFMT): $(LOCALBIN)
	$(call go-install-tool,$(SHFMT),mvdan.cc/sh/v3/cmd/shfmt,$(SHFMT_VERSION))

.PHONY: shellcheck
shellcheck: $(SHELLCHECK) ## Download shellcheck locally if necessary.
$(SHELLCHECK): $(LOCALBIN)
	$(call download-shellcheck,$(SHELLCHECK),$(SHELLCHECK_VERSION))

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

# download-shellcheck will download a pinned shellcheck release binary if it doesn't exist.
# shellcheck publishes .tar.gz assets from v0.11.0 onward, which avoids an
# xz/liblzma dependency. Pinning an older SHELLCHECK_VERSION would need .tar.xz.
# $1 - target path with name of binary (ideally with version)
# $2 - shellcheck version (e.g. v0.11.0)
define download-shellcheck
@[ -f "$(1)" ] || { \
set -e; \
os=$$(uname -s | tr '[:upper:]' '[:lower:]'); \
arch=$$(uname -m); \
case "$$arch" in \
  x86_64|amd64) arch=x86_64 ;; \
  arm64|aarch64) arch=aarch64 ;; \
  *) echo "Unsupported architecture: $$arch" >&2; exit 1 ;; \
esac; \
archive="shellcheck-$(2).$${os}.$${arch}.tar.gz"; \
url="https://github.com/koalaman/shellcheck/releases/download/$(2)/$${archive}"; \
echo "Downloading $${url}"; \
tmp=$$(mktemp -d); \
curl -fsSL "$${url}" | tar -xz -C "$${tmp}"; \
mv "$${tmp}/shellcheck-$(2)/shellcheck" "$(1)"; \
chmod +x "$(1)"; \
rm -rf "$${tmp}"; \
}
endef
