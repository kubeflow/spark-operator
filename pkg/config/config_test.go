package config

import (
	"testing"

	"k8s.io/client-go/kubernetes/fake"
)

func TestCreateSparkConfigMap(t *testing.T) {
	_ = fake.NewSimpleClientset()
}
