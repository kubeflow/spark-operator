
.SILENT:
.PHONY: clean-sparkctl

SPARK_OPERATOR_GOPATH=/go/src/github.com/GoogleCloudPlatform/spark-on-k8s-operator
DEP_VERSION:=`grep DEP_VERSION= Dockerfile | awk -F\" '{print $$2}'`
BUILDER=`grep "FROM golang:" Dockerfile | awk '{print $$2}'`
UNAME:=`uname | tr '[:upper:]' '[:lower:]'`
ARCH?=amd64

all: clean-sparkctl build-sparkctl install-sparkctl

build-sparkctl:
	[ ! -f "sparkctl/sparkctl-darwin-${ARCH}" ] || [ ! -f "sparkctl/sparkctl-linux-${ARCH}" ] && \
	echo building using $(BUILDER) && \
	docker run -it -w $(SPARK_OPERATOR_GOPATH) \
	-v $$(pwd):$(SPARK_OPERATOR_GOPATH) $(BUILDER) sh -c \
	"apk add --no-cache bash git && \
	cd sparkctl && \
	./build.sh" || true

clean-sparkctl:
	rm -f sparkctl/sparkctl-darwin-${ARCH} sparkctl/sparkctl-linux-${ARCH}

install-sparkctl: | sparkctl/sparkctl-darwin-${ARCH} sparkctl/sparkctl-linux-${ARCH}
	@if [ "$(UNAME)" = "linux" ]; then \
		echo "installing linux binary to /usr/local/bin/sparkctl"; \
		sudo cp sparkctl/sparkctl-linux-${ARCH} /usr/local/bin/sparkctl; \
		sudo chmod +x /usr/local/bin/sparkctl; \
	elif [ "$(UNAME)" = "darwin" ]; then \
		echo "installing macos binary to /usr/local/bin/sparkctl"; \
		cp sparkctl/sparkctl-darwin-${ARCH} /usr/local/bin/sparkctl; \
		chmod +x /usr/local/bin/sparkctl; \
	else \
		echo "$(UNAME) not supported"; \
	fi

build-api-docs:
	hack/api-ref-docs \
	-config hack/api-docs-config.json \
	-api-dir github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2 \
	-template-dir hack/api-docs-template \
	-out-file docs/api-docs.md
