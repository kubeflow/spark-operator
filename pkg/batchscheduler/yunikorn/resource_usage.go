package yunikorn

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/kubeflow/spark-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
)

const (
	defaultCpuMillicores        = 1000
	defaultMemoryBytes          = 1 << 30 // 1Gi
	defaultMemoryOverheadFactor = 0.1
	minMemoryOverheadBytes      = 384 * (1 << 20) // 384Mi
	nonJvmDefaultMemoryOverhead = 0.4
)

var (
	javaStringSuffixes = map[string]int64{
		"b":  1,
		"kb": 1 << 10,
		"k":  1 << 10,
		"mb": 1 << 20,
		"m":  1 << 20,
		"gb": 1 << 30,
		"g":  1 << 30,
		"tb": 1 << 40,
		"t":  1 << 40,
		"pb": 1 << 50,
		"p":  1 << 50,
	}

	javaStringPattern         = regexp.MustCompile(`([0-9]+)([a-z]+)?`)
	javaFractionStringPattern = regexp.MustCompile(`([0-9]+\.[0-9]+)([a-z]+)?`)
)

func parseJavaMemoryString(str string) (int64, error) {
	lower := strings.ToLower(str)
	if matches := javaStringPattern.FindStringSubmatch(lower); matches != nil {
		value, err := strconv.ParseInt(matches[1], 10, 64)
		if err != nil {
			return 0, err
		}
		suffix := matches[2]
		if multiplier, present := javaStringSuffixes[suffix]; present {
			return multiplier * value, nil
		}
	} else if matches = javaFractionStringPattern.FindStringSubmatch(lower); matches != nil {
		value, err := strconv.ParseFloat(matches[1], 64)
		if err != nil {
			return 0, err
		}
		suffix := matches[2]
		if multiplier, present := javaStringSuffixes[suffix]; present {
			return int64(float64(multiplier) * value), nil
		}
	}
	return 0, fmt.Errorf("could not parse string '%s' as a Java-style memory value. Examples: 100kb, 1.5mb, 1g", str)
}

func sparkPodCoreRequest(coreRequest *string, cores *int32) (resource.Quantity, error) {
	value := fmt.Sprintf("%dm", defaultCpuMillicores)
	if coreRequest != nil {
		value = *coreRequest
	} else if cores != nil {
		value = fmt.Sprintf("%dm", *cores*1000)
	}

	quantity, err := resource.ParseQuantity(value)
	if err != nil {
		return resource.Quantity{}, fmt.Errorf("error parsing core request value %s: %v", value, err)
	}

	return quantity, nil
}

func sparkPodMemoryRequest(podSpec v1beta2.SparkPodSpec, appMemoryOverheadFactor *string, appType v1beta2.SparkApplicationType) (resource.Quantity, error) {
	memoryBytes := int64(defaultMemoryBytes)
	if podSpec.Memory != nil {
		memory, err := parseJavaMemoryString(*podSpec.Memory)
		if err != nil {
			return resource.Quantity{}, err
		}
		memoryBytes = memory
	}

	var memoryOverheadFactor float64
	if appType == v1beta2.JavaApplicationType || appType == v1beta2.ScalaApplicationType {
		memoryOverheadFactor = defaultMemoryOverheadFactor
	} else {
		memoryOverheadFactor = nonJvmDefaultMemoryOverhead
	}

	if appMemoryOverheadFactor != nil {
		value, err := strconv.ParseFloat(*appMemoryOverheadFactor, 64)
		if err != nil {
			return resource.Quantity{}, err
		}
		memoryOverheadFactor = value
	}

	var memoryOverheadBytes int64
	if podSpec.MemoryOverhead != nil {
		value, err := parseJavaMemoryString(*podSpec.MemoryOverhead)
		if err != nil {
			return resource.Quantity{}, err
		}
		memoryOverheadBytes = value
	} else {
		memoryOverheadBytes = int64(max(float64(memoryBytes)*memoryOverheadFactor, minMemoryOverheadBytes))
	}

	return *resource.NewQuantity(memoryBytes+memoryOverheadBytes, resource.DecimalSI), nil
}

func resourceToString(cpu, memory resource.Quantity) map[string]string {
	return map[string]string{
		"cpu":    cpu.String(),
		"memory": memory.String(),
	}
}

func driverPodResourceUsage(app *v1beta2.SparkApplication) (map[string]string, error) {
	cpuRequest, err := sparkPodCoreRequest(app.Spec.Driver.CoreRequest, app.Spec.Driver.Cores)
	if err != nil {
		return nil, err
	}

	memoryRequest, err := sparkPodMemoryRequest(app.Spec.Driver.SparkPodSpec, app.Spec.Driver.MemoryOverhead, app.Spec.Type)
	if err != nil {
		return nil, err
	}

	return resourceToString(cpuRequest, memoryRequest), nil
}

func executorPodResourceUsage(app *v1beta2.SparkApplication) (map[string]string, error) {
	cpuRequest, err := sparkPodCoreRequest(app.Spec.Executor.CoreRequest, app.Spec.Executor.Cores)
	if err != nil {
		return nil, err
	}

	memoryRequest, err := sparkPodMemoryRequest(app.Spec.Executor.SparkPodSpec, app.Spec.Executor.MemoryOverhead, app.Spec.Type)
	if err != nil {
		return nil, err
	}

	return resourceToString(cpuRequest, memoryRequest), nil
}
