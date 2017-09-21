all: build

generate_files:
	deepcopy-gen -i ./pkg/apis/v1alpha1 -O zz_generated.deepcopy --bounding-dirs=github.com/liyinan926/spark-operator

build: generate_files
	GOOS=linux go build
	chmod +x spark-operator

image: build
	docker build -t $(image-tag) -f ./Dockerfile .

push: image
	docker push $(image-tag)
