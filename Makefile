all: build

build:
	go build

build-image:
	docker build -t gcr.io/ynli-kubernetes/spark-operator .

push:
	gcloud docker -- push gcr.io/enisoc-kubernetes/metacontroller