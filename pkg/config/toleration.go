package config

import (
	"fmt"
	"strings"

	apiv1 "k8s.io/api/core/v1"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/util"
)

func getTolerationAnnotations(tolerations []apiv1.Toleration) (map[string]string, error) {
	annotations := make(map[string]string)
	i := 0
	for _, v := range tolerations {
		i++
		tolerationStr, err := util.MarshalToleration(&v)
		if err != nil {
			return nil, err
		}
		key := fmt.Sprintf("%s%s%d", TolerationsAnnotationPrefix, "toleration", i)
		annotations[key] = tolerationStr
	}

	return annotations, nil
}

// GetDriverTolerationConfOptions returns a list of spark-submit options for driver annotations for tolerations to be
// applied to the driver.
func GetDriverTolerationConfOptions(app *v1alpha1.SparkApplication) ([]string, error) {
	var options []string

	annotations, err := getTolerationAnnotations(app.Spec.Driver.Tolerations)
	if err != nil {
		return nil, err
	}
	for key, value := range annotations {
		options = append(options, GetDriverAnnotationOption(key, value))
	}

	return options, nil
}

// GetExecutorTolerationConfOptions returns a list of spark-submit options for executor annotations for tolerations to be
// applied to the executor.
func GetExecutorTolerationConfOptions(app *v1alpha1.SparkApplication) ([]string, error) {
	var options []string

	annotations, err := getTolerationAnnotations(app.Spec.Executor.Tolerations)
	if err != nil {
		return nil, err
	}
	for key, value := range annotations {
		options = append(options, GetExecutorAnnotationOption(key, value))
	}

	return options, nil
}

// FindTolerations finds and parses Tolerations in the given annotations.
func FindTolerations(annotations map[string]string) ([]*apiv1.Toleration, error) {
	var tolerations []*apiv1.Toleration
	for key, value := range annotations {
		if strings.HasPrefix(key, TolerationsAnnotationPrefix) {
			toleration, err := util.UnmarshalToleration(value)
			if err != nil {
				return nil, err
			}
			tolerations = append(tolerations, toleration)
		}
	}

	return tolerations, nil
}
