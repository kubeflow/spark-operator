
.SILENT:
.PHONY: clean-sparkctl

SPARK_OPERATOR_GOPATH=/go/src/github.com/GoogleCloudPlatform/spark-on-k8s-operator
DEP_VERSION:=`grep DEP_VERSION= Dockerfile | awk -F\" '{print $$2}'`
BUILDER=`grep "FROM golang:" Dockerfile | awk '{print $$2}'`
UNAME:=`uname | tr '[:upper:]' '[:lower:]'`
REPO=github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg

all: clean-sparkctl build-sparkctl install-sparkctl

build-sparkctl:
	[ ! -f "sparkctl/sparkctl-darwin-amd64" ] || [ ! -f "sparkctl/sparkctl-linux-amd64" ] && \
	echo building using $(BUILDER) && \
	docker run -w $(SPARK_OPERATOR_GOPATH) \
	-v $$(pwd):$(SPARK_OPERATOR_GOPATH) $(BUILDER) sh -c \
	"apk add --no-cache bash git && \
	cd sparkctl && \
	./build.sh" || true

clean-sparkctl:
	rm -f sparkctl/sparkctl-darwin-amd64 sparkctl/sparkctl-linux-amd64

install-sparkctl: | sparkctl/sparkctl-darwin-amd64 sparkctl/sparkctl-linux-amd64
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

build-api-docs:
	docker build -t temp-api-ref-docs hack/api-docs
	docker run -v $$(pwd):/repo/ temp-api-ref-docs \
		sh -c "cd /repo/ && /go/gen-crd-api-reference-docs/gen-crd-api-reference-docs \
			-config /repo/hack/api-docs/api-docs-config.json \
			-api-dir github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2 \
			-template-dir /repo/hack/api-docs/api-docs-template \
			-out-file /repo/docs/api-docs.md"

helm-docs:
	docker run --rm --volume "$(pwd):/helm-docs" -u "$(id -u)" jnorwood/helm-docs:latest

fmt-check: clean
	@echo "running fmt check"; cd "$(dirname $0)"; \
	if [ -n "$(go fmt ./...)" ]; \
	then \
		echo "Go code is not formatted, please run 'go fmt ./...'." >&2; \
		exit 1; \
	else \
		echo "Go code is formatted"; \
	fi

detect-crds-drift:
	diff -q charts/spark-operator-chart/crds manifest/crds --exclude=kustomization.yaml

clean:
	@echo "cleaning up caches and output"
	go clean -cache -testcache -r -x ./... 2>&1 >/dev/null
	-rm -rf _output

unit-test: clean
	@echo "running unit tests"
	go test -v ./... -covermode=atomic

integration-test: clean
	@echo "running integration tests"
	go test -v ./test/e2e/ --kubeconfig "$(HOME)/.kube/config" --operator-image=gcr.io/spark-operator/spark-operator:local

static-analysis:
	@echo "running go vet"
	# echo "Building using $(BUILDER)"
	# go vet ./...
	go vet $(REPO)...
