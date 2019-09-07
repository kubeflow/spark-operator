
.SILENT:
.PHONY: build-sparkctl clean-build

SPARK_OPERATOR_GOPATH=/go/src/github.com/GoogleCloudPlatform/spark-on-k8s-operator
DEP_VERSION=0.5.3

build-sparkctl:
	docker run -it -w $(SPARK_OPERATOR_GOPATH) \
	-v $$(pwd):$(SPARK_OPERATOR_GOPATH) golang:1.12.5-alpine sh -c \
	"apk add --no-cache bash git && \
	wget https://github.com/golang/dep/releases/download/v$(DEP_VERSION)/dep-linux-amd64 -O /usr/bin/dep && \
	chmod +x /usr/bin/dep && \
	cd sparkctl && \
	dep ensure -v -vendor-only && \
	./build.sh"

clean-sparkctl:
	rm -f sparkctl/sparkctl-darwin-amd64 sparkctl/sparkctl-linux-amd64
