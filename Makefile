all: build

generate_files:
	deepcopy-gen -i ./pkg/apis/v1alpha1 -O zz_generated.deepcopy --bounding-dirs=github.com/liyinan926/spark-operator

build: generate_files
	GOOS=linux go build

image: build
	docker build -t liyinan926/spark-operator:latest -f ./Dockerfile .

push: image
	docker push liyinan926/spark-operator:latest
