package resourceusage

import (
	"fmt"
	so "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"math"
	"regexp"
	"strconv"
	"strings"
)

// ...are you serious, Go?
func max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

const (
	// https://spark.apache.org/docs/latest/configuration.html
	defaultCpuMillicores  = 1000
	defaultMemoryBytes    = 1 << 30 // 1Gi
	defaultMemoryOverhead = 0.1

	// https://github.com/apache/spark/blob/c4bbfd177b4e7cb46f47b39df9fd71d2d9a12c6d/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/Constants.scala#L85
	minMemoryOverhead           = 384 * (1 << 20) // 384Mi
	nonJvmDefaultMemoryOverhead = 0.4
)

func namespaceOrDefault(meta metav1.ObjectMeta) string {
	namespace := meta.Namespace
	if namespace == "" {
		namespace = "default"
	}
	return namespace
}

func launchedBySparkOperator(meta metav1.ObjectMeta) bool {
	val, present := meta.Labels[config.LaunchedBySparkOperatorLabel]
	return present && val == "true"
}

func resourcesRequiredToSchedule(resourceRequirements corev1.ResourceRequirements) (cpu int64, memoryBytes int64) {
	if coresRequest, present := resourceRequirements.Requests[corev1.ResourceCPU]; present {
		cpu = coresRequest.MilliValue()
	} else if coresLimit, present := resourceRequirements.Limits[corev1.ResourceCPU]; present {
		cpu = coresLimit.MilliValue()
	}
	if memoryRequest, present := resourceRequirements.Requests[corev1.ResourceMemory]; present {
		memoryBytes = memoryRequest.Value()
	} else if memoryLimit, present := resourceRequirements.Limits[corev1.ResourceMemory]; present {
		memoryBytes = memoryLimit.Value()
	}
	return cpu, memoryBytes
}

func coresRequiredForSparkPod(spec so.SparkPodSpec, instances int64) (int64, error) {
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

var javaStringPattern = regexp.MustCompile(`([0-9]+)([a-z]+)?`)
var javaFractionStringPattern = regexp.MustCompile(`([0-9]+\.[0-9]+)([a-z]+)?`)

// Logic copied from https://github.com/apache/spark/blob/5264164a67df498b73facae207eda12ee133be7d/common/network-common/src/main/java/org/apache/spark/network/util/JavaUtils.java#L276
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

// Logic copied from https://github.com/apache/spark/blob/c4bbfd177b4e7cb46f47b39df9fd71d2d9a12c6d/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/features/BasicDriverFeatureStep.scala
func memoryRequiredForSparkPod(spec so.SparkPodSpec, memoryOverheadFactor *string, appType so.SparkApplicationType, replicas int64) (int64, error) {
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
		if memoryOverheadFactor != nil {
			overheadFactorScope, err := strconv.ParseFloat(*memoryOverheadFactor, 64)
			if err != nil {
				return 0, err
			}
			overheadFactor = overheadFactorScope
		} else {
			if appType == so.JavaApplicationType {
				overheadFactor = defaultMemoryOverhead
			} else {
				overheadFactor = nonJvmDefaultMemoryOverhead
			}
		}
		memoryOverheadBytes = int64(math.Max(overheadFactor*float64(memoryBytes), minMemoryOverhead))
	}
	return (memoryBytes + memoryOverheadBytes) * replicas, nil
}

func resourceUsage(spec so.SparkApplicationSpec) (ResourceList, error) {
	driverMemoryOverheadFactor := spec.MemoryOverheadFactor
	executorMemoryOverheadFactor := spec.MemoryOverheadFactor
	driverMemory, err := memoryRequiredForSparkPod(spec.Driver.SparkPodSpec, driverMemoryOverheadFactor, spec.Type, 1)
	if err != nil {
		return ResourceList{}, err
	}

	var instances int64 = 1
	if spec.Executor.Instances != nil {
		instances = int64(*spec.Executor.Instances)
	}
	executorMemory, err := memoryRequiredForSparkPod(spec.Executor.SparkPodSpec, executorMemoryOverheadFactor, spec.Type, instances)
	if err != nil {
		return ResourceList{}, err
	}

	driverCores, err := coresRequiredForSparkPod(spec.Driver.SparkPodSpec, 1)
	if err != nil {
		return ResourceList{}, err
	}

	executorCores, err := coresRequiredForSparkPod(spec.Executor.SparkPodSpec, instances)
	if err != nil {
		return ResourceList{}, err
	}

	return ResourceList{
		cpu:    *resource.NewMilliQuantity(driverCores+executorCores, resource.DecimalSI),
		memory: *resource.NewQuantity(driverMemory+executorMemory, resource.DecimalSI),
	}, nil
}

func sparkApplicationResourceUsage(sparkApp so.SparkApplication) (ResourceList, error) {
	// A completed/failed SparkApplication consumes no resources
	if !sparkApp.Status.TerminationTime.IsZero() || sparkApp.Status.AppState.State == so.FailedState || sparkApp.Status.AppState.State == so.CompletedState {
		return ResourceList{}, nil
	}
	return resourceUsage(sparkApp.Spec)
}

func scheduledSparkApplicationResourceUsage(sparkApp so.ScheduledSparkApplication) (ResourceList, error) {
	// Failed validation, will consume no resources
	if sparkApp.Status.ScheduleState == so.FailedValidationState {
		return ResourceList{}, nil
	}
	return resourceUsage(sparkApp.Spec.Template)
}

func podResourceUsage(pod *corev1.Pod) ResourceList {
	spec := pod.Spec
	var initCores int64
	var initMemoryBytes int64
	completed := make(map[string]struct{})

	for _, containerStatus := range pod.Status.InitContainerStatuses {
		if containerStatus.State.Terminated != nil {
			completed[containerStatus.Name] = struct{}{}
		}
	}
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.State.Terminated != nil {
			completed[containerStatus.Name] = struct{}{}
		}
	}

	for _, container := range spec.InitContainers {
		if _, present := completed[container.Name]; !present {
			c, m := resourcesRequiredToSchedule(container.Resources)
			initCores = max(c, initCores)
			initMemoryBytes = max(m, initMemoryBytes)
		}
	}
	var cores int64
	var memoryBytes int64
	for _, container := range spec.Containers {
		if _, present := completed[container.Name]; !present {
			c, m := resourcesRequiredToSchedule(container.Resources)
			cores += c
			memoryBytes += m
		}
	}
	cores = max(initCores, cores)
	memoryBytes = max(initMemoryBytes, memoryBytes)
	return ResourceList{
		cpu:    *resource.NewMilliQuantity(cores, resource.DecimalSI),
		memory: *resource.NewQuantity(memoryBytes, resource.DecimalSI),
	}
}
