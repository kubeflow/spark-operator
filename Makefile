all: build

build:
	GOOS=linux go build

build-image:
	docker build -t gcr.io/ynli-k8s/spark-operator .

push:
	gcloud docker -- push gcr.io/ynli-k8s/spark-operator
