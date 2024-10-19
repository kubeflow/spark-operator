package volcano

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/common"
)

const (
	defaultCpuMillicores  = 1000
	defaultMemoryBytes    = 1 << 30 // 1Gi
	defaultMemoryOverhead = 0.1

	minMemoryOverhead           = 384 * (1 << 20) // 384Mi
	nonJvmDefaultMemoryOverhead = 0.4

	SparkDriverMemoryOverheadFactor   = "spark.driver.memoryOverheadFactor"
	SparkExecutorMemoryOverheadFactor = "spark.executor.memoryOverheadFactor"

	SparkDriverMemoryOverhead   = "spark.driver.memoryOverhead"
	SparkExecutorMemoryOverhead = "spark.executor.memoryOverhead"
)

func coresRequiredForSparkPod(spec v1beta2.SparkPodSpec, instances int64) (int64, error) {
	var cpu int64
	if spec.Cores != nil {
		cpu = int64(*spec.Cores) * 1000
	} else {
		cpu = defaultCpuMillicores
	}
	return cpu * instances, nil
}

var javaStringSuffixes = map[string]int64{
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

var javaStringPattern = regexp.MustCompile(`^([0-9]+)([a-z]+)$`)
var javaFractionStringPattern = regexp.MustCompile(`^([0-9]+\.[0-9]+)([a-z]+)$`)

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
			//return int64(math.Ceil(float64(multiplier)*float64(value)/(1<<20)) * (1 << 20)), nil
		}
	} else if matches = javaFractionStringPattern.FindStringSubmatch(lower); matches != nil {
		value, err := strconv.ParseFloat(matches[1], 64)
		if err != nil {
			return 0, err
		}
		suffix := matches[2]
		if multiplier, present := javaStringSuffixes[suffix]; present {
			return int64(float64(multiplier) * value), nil
			//return int64(math.Ceil(float64(multiplier)*value/(1<<20)) * (1 << 20)), nil
		}
	}
	return 0, fmt.Errorf("could not parse string '%s' as a Java-style memory value. Examples: 100kb, 1.5mb, 1g", str)
}

func memoryRequiredForSparkPod(spec v1beta2.SparkPodSpec, memoryOverheadFactor string, appType v1beta2.SparkApplicationType, replicas int64) (int64, error) {
	var memoryBytes int64
	if spec.Memory != nil {
		memory, err := parseJavaMemoryString(*spec.Memory)
		if err != nil {
			return 0, err
		}
		memoryBytes = memory
	} else {
		memoryBytes = defaultMemoryBytes
	}
	var memoryOverheadBytes int64
	if spec.MemoryOverhead != nil {
		overhead, err := parseJavaMemoryString(*spec.MemoryOverhead)
		if err != nil {
			return 0, err
		}
		memoryOverheadBytes = overhead
	} else {
		var overheadFactor float64
		if memoryOverheadFactor != "" {
			overheadFactorScope, err := strconv.ParseFloat(memoryOverheadFactor, 64)
			if err != nil {
				return 0, err
			}
			overheadFactor = overheadFactorScope
		} else {
			if appType == v1beta2.SparkApplicationTypeJava {
				overheadFactor = defaultMemoryOverhead
			} else {
				overheadFactor = nonJvmDefaultMemoryOverhead
			}
		}
		memoryOverheadBytes = int64(math.Max(overheadFactor*float64(memoryBytes), minMemoryOverhead))
	}

	return (memoryBytes + memoryOverheadBytes) * replicas, nil
}

func getDriverMemoryOverheadFactor(sparkApp *v1beta2.SparkApplication) string {
	var memoryOverheadFactor string

	if _, ok := sparkApp.Spec.SparkConf[common.SparkKubernetesMemoryOverheadFactor]; ok {
		memoryOverheadFactor = sparkApp.Spec.SparkConf[common.SparkKubernetesMemoryOverheadFactor]
	}

	if _, ok := sparkApp.Spec.SparkConf[SparkDriverMemoryOverheadFactor]; ok {
		memoryOverheadFactor = sparkApp.Spec.SparkConf[SparkDriverMemoryOverheadFactor]
	}

	if sparkApp.Spec.MemoryOverheadFactor != nil {
		memoryOverheadFactor = *sparkApp.Spec.MemoryOverheadFactor
	}

	return memoryOverheadFactor
}

func getExecutorMemoryOverheadFactor(sparkApp *v1beta2.SparkApplication) string {
	var memoryOverheadFactor string

	if _, ok := sparkApp.Spec.SparkConf[common.SparkKubernetesMemoryOverheadFactor]; ok {
		memoryOverheadFactor = sparkApp.Spec.SparkConf[common.SparkKubernetesMemoryOverheadFactor]
	}

	if _, ok := sparkApp.Spec.SparkConf[SparkExecutorMemoryOverheadFactor]; ok {
		memoryOverheadFactor = sparkApp.Spec.SparkConf[SparkExecutorMemoryOverheadFactor]
	}

	if sparkApp.Spec.MemoryOverheadFactor != nil {
		memoryOverheadFactor = *sparkApp.Spec.MemoryOverheadFactor
	}

	return memoryOverheadFactor
}

func fixSparkApplication(sparkApp *v1beta2.SparkApplication) {
	if value, ok := sparkApp.Spec.SparkConf[SparkExecutorMemoryOverhead]; ok {
		if sparkApp.Spec.Executor.SparkPodSpec.MemoryOverhead == nil {
			sparkApp.Spec.Executor.SparkPodSpec.MemoryOverhead = &value
		}
	}

	if value, ok := sparkApp.Spec.SparkConf[SparkDriverMemoryOverhead]; ok {
		if sparkApp.Spec.Driver.SparkPodSpec.MemoryOverhead == nil {
			sparkApp.Spec.Driver.SparkPodSpec.MemoryOverhead = &value
		}
	}
}

func getExecutorMaxNum(app *v1beta2.SparkApplication) int64 {
	var maxNum int64 = 1
	if app.Spec.Executor.Instances != nil {
		maxNum = int64(*app.Spec.Executor.Instances)
	}

	if app.Spec.SparkConf == nil {
		return maxNum
	}

	_, ok := app.Spec.SparkConf[common.SparkDynamicAllocationEnabled]
	if !ok {
		return maxNum
	}

	dynamicInitialExecutors := int64(0)
	if _, ok := app.Spec.SparkConf[common.SparkDynamicAllocationInitialExecutors]; ok {
		num, _ := strconv.Atoi(app.Spec.SparkConf[common.SparkDynamicAllocationInitialExecutors])
		dynamicInitialExecutors = int64(num)
	}
	if dynamicInitialExecutors > maxNum {
		maxNum = dynamicInitialExecutors
	}

	dynamicMinExecutors := int64(0)
	if _, ok := app.Spec.SparkConf[common.SparkDynamicAllocationMinExecutors]; ok {
		num, _ := strconv.Atoi(app.Spec.SparkConf[common.SparkDynamicAllocationMinExecutors])
		dynamicMinExecutors = int64(num)
	}
	if dynamicMinExecutors > maxNum {
		maxNum = dynamicMinExecutors
	}

	dynamicMaxExecutors := int64(0)
	if _, ok := app.Spec.SparkConf[common.SparkDynamicAllocationMaxExecutors]; ok {
		num, _ := strconv.Atoi(app.Spec.SparkConf[common.SparkDynamicAllocationMaxExecutors])
		dynamicMaxExecutors = int64(num)
	}
	if dynamicMaxExecutors > maxNum {
		maxNum = dynamicMaxExecutors
	}

	return maxNum
}

func sparkApplicationResourceUsage(sparkApp *v1beta2.SparkApplication) (corev1.ResourceList, error) {
	sparkAppResource := corev1.ResourceList{}

	driverMemoryOverheadFactor := getDriverMemoryOverheadFactor(sparkApp)
	executorMemoryOverheadFactor := getExecutorMemoryOverheadFactor(sparkApp)

	executorInstances := getExecutorMaxNum(sparkApp)

	fixSparkApplication(sparkApp)

	driverMemory, err := memoryRequiredForSparkPod(sparkApp.Spec.Driver.SparkPodSpec,
		driverMemoryOverheadFactor, sparkApp.Spec.Type, 1)
	if err != nil {
		return sparkAppResource, err
	}
	executorMemory, err := memoryRequiredForSparkPod(sparkApp.Spec.Executor.SparkPodSpec,
		executorMemoryOverheadFactor, sparkApp.Spec.Type, executorInstances)
	if err != nil {
		return sparkAppResource, err
	}

	driverCores, err := coresRequiredForSparkPod(sparkApp.Spec.Driver.SparkPodSpec, 1)
	if err != nil {
		return sparkAppResource, err
	}
	executorCores, err := coresRequiredForSparkPod(sparkApp.Spec.Executor.SparkPodSpec, executorInstances)
	if err != nil {
		return sparkAppResource, err
	}

	sparkAppResource[corev1.ResourceCPU] = *resource.NewMilliQuantity(driverCores+executorCores, resource.DecimalSI)
	sparkAppResource[corev1.ResourceMemory] = *resource.NewQuantity(driverMemory+executorMemory, resource.BinarySI)

	return sparkAppResource, nil
}
