all: build

build:
	GOOS=linux go build

image: build
	docker build -t liyinan926/spark-operator:latest -f ./Dockerfile .

push: image
	docker push liyinan926/spark-operator:latest
