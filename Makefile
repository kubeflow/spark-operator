
.SILENT:
.PHONY: build-sparkctl clean-build

SPARK_OPERATOR_GOPATH=/go/src/github.com/GoogleCloudPlatform/spark-on-k8s-operator
DEP_VERSION:=`grep DEP_VERSION= Dockerfile | awk -F\" '{print $$2}'`
BUILDER=`grep "FROM golang:" Dockerfile | awk '{print $$2}'`

build-sparkctl:
	echo building using $(BUILDER) and dep $(DEP_VERSION)
	docker run -it -w $(SPARK_OPERATOR_GOPATH) \
	-v $$(pwd):$(SPARK_OPERATOR_GOPATH) $(BUILDER) sh -c \
	"apk add --no-cache bash git && \
	wget https://github.com/golang/dep/releases/download/v"$(DEP_VERSION)"/dep-linux-amd64 -O /usr/bin/dep && \
	chmod +x /usr/bin/dep && \
	cd sparkctl && \
	dep ensure -v -vendor-only && \
	./build.sh"

clean-sparkctl:
	rm -f sparkctl/sparkctl-darwin-amd64 sparkctl/sparkctl-linux-amd64
