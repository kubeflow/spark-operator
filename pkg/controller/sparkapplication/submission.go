/*
Copyright 2017 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sparkapplication

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/golang/glog"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/apis/policy"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
)

const (
	sparkHomeEnvVar             = "SPARK_HOME"
	kubernetesServiceHostEnvVar = "KUBERNETES_SERVICE_HOST"
	kubernetesServicePortEnvVar = "KUBERNETES_SERVICE_PORT"
)

// submission includes information of a Spark application to be submitted.
type submission struct {
	namespace string
	name      string
	args      []string
}

func newSubmission(args []string, app *v1beta2.SparkApplication) *submission {
	return &submission{
		namespace: app.Namespace,
		name:      app.Name,
		args:      args,
	}
}

func runSparkSubmit(submission *submission) (bool, error) {
	sparkHome, present := os.LookupEnv(sparkHomeEnvVar)
	if !present {
		glog.Error("SPARK_HOME is not specified")
	}
	var command = filepath.Join(sparkHome, "/bin/spark-submit")

	cmd := execCommand(command, submission.args...)
	glog.V(2).Infof("spark-submit arguments: %v", cmd.Args)
	output, err := cmd.Output()
	glog.V(3).Infof("spark-submit output: %s", string(output))
	if err != nil {
		var errorMsg string
		if exitErr, ok := err.(*exec.ExitError); ok {
			errorMsg = string(exitErr.Stderr)
		}
		// The driver pod of the application already exists.
		if strings.Contains(errorMsg, podAlreadyExistsErrorCode) {
			glog.Warningf("trying to resubmit an already submitted SparkApplication %s/%s", submission.namespace, submission.name)
			return false, nil
		}
		if errorMsg != "" {
			return false, fmt.Errorf("failed to run spark-submit for SparkApplication %s/%s: %s", submission.namespace, submission.name, errorMsg)
		}
		return false, fmt.Errorf("failed to run spark-submit for SparkApplication %s/%s: %v", submission.namespace, submission.name, err)
	}

	return true, nil
}

func buildSubmissionCommandArgs(app *v1beta2.SparkApplication, driverPodName string, submissionID string) ([]string, error) {
	var args []string
	if app.Spec.MainClass != nil {
		args = append(args, "--class", *app.Spec.MainClass)
	}
	masterURL, err := getMasterURL()
	if err != nil {
		return nil, err
	}

	args = append(args, "--master", masterURL)
	args = append(args, "--deploy-mode", string(app.Spec.Mode))

	// Add proxy user
	if app.Spec.ProxyUser != nil {
		args = append(args, "--proxy-user", *app.Spec.ProxyUser)
	}

	args = append(args, "--conf", fmt.Sprintf("%s=%s", config.SparkAppNamespaceKey, app.Namespace))
	args = append(args, "--conf", fmt.Sprintf("%s=%s", config.SparkAppNameKey, app.Name))
	args = append(args, "--conf", fmt.Sprintf("%s=%s", config.SparkDriverPodNameKey, driverPodName))

	// Add application dependencies.
	args = append(args, addDependenciesConfOptions(app)...)

	if app.Spec.Image != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%s", config.SparkContainerImageKey, *app.Spec.Image))
	}
	if app.Spec.ImagePullPolicy != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%s", config.SparkContainerImagePullPolicyKey, *app.Spec.ImagePullPolicy))
	}
	if len(app.Spec.ImagePullSecrets) > 0 {
		secretNames := strings.Join(app.Spec.ImagePullSecrets, ",")
		args = append(args, "--conf", fmt.Sprintf("%s=%s", config.SparkImagePullSecretKey, secretNames))
	}
	if app.Spec.PythonVersion != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%s", config.SparkPythonVersion, *app.Spec.PythonVersion))
	}
	if app.Spec.MemoryOverheadFactor != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%s", config.SparkMemoryOverheadFactor, *app.Spec.MemoryOverheadFactor))
	}

	// Operator triggered spark-submit should never wait for App completion
	args = append(args, "--conf", fmt.Sprintf("%s=false", config.SparkWaitAppCompletion))

	// Add Spark configuration properties.
	for key, value := range app.Spec.SparkConf {
		// Configuration property for the driver pod name has already been set.
		if key != config.SparkDriverPodNameKey {
			args = append(args, "--conf", fmt.Sprintf("%s=%s", key, value))
		}
	}

	// Add Hadoop configuration properties.
	for key, value := range app.Spec.HadoopConf {
		args = append(args, "--conf", fmt.Sprintf("spark.hadoop.%s=%s", key, value))
	}

	// Add the driver and executor configuration options.
	// Note that when the controller submits the application, it expects that all dependencies are local
	// so init-container is not needed and therefore no init-container image needs to be specified.
	options, err := addDriverConfOptions(app, submissionID)
	if err != nil {
		return nil, err
	}
	for _, option := range options {
		args = append(args, "--conf", option)
	}
	options, err = addExecutorConfOptions(app, submissionID)
	if err != nil {
		return nil, err
	}
	for _, option := range options {
		args = append(args, "--conf", option)
	}

	options = addDynamicAllocationConfOptions(app)
	for _, option := range options {
		args = append(args, "--conf", option)
	}

	for key, value := range app.Spec.NodeSelector {
		conf := fmt.Sprintf("%s%s=%s", config.SparkNodeSelectorKeyPrefix, key, value)
		args = append(args, "--conf", conf)
	}

	if app.Spec.Volumes != nil {
		options, err = addLocalDirConfOptions(app)
		if err != nil {
			return nil, err
		}

		for _, option := range options {
			args = append(args, "--conf", option)
		}
	}

	if app.Spec.MainApplicationFile != nil {
		// Add the main application file if it is present.
		args = append(args, *app.Spec.MainApplicationFile)
	}

	// Add application arguments.
	for _, argument := range app.Spec.Arguments {
		args = append(args, argument)
	}

	return args, nil
}

func getMasterURL() (string, error) {
	kubernetesServiceHost := os.Getenv(kubernetesServiceHostEnvVar)
	if kubernetesServiceHost == "" {
		return "", fmt.Errorf("environment variable %s is not found", kubernetesServiceHostEnvVar)
	}
	kubernetesServicePort := os.Getenv(kubernetesServicePortEnvVar)
	if kubernetesServicePort == "" {
		return "", fmt.Errorf("environment variable %s is not found", kubernetesServicePortEnvVar)
	}
	return fmt.Sprintf("k8s://https://%s:%s", kubernetesServiceHost, kubernetesServicePort), nil
}

func getOwnerReference(app *v1beta2.SparkApplication) *metav1.OwnerReference {
	controller := true
	return &metav1.OwnerReference{
		APIVersion: v1beta2.SchemeGroupVersion.String(),
		Kind:       reflect.TypeOf(v1beta2.SparkApplication{}).Name(),
		Name:       app.Name,
		UID:        app.UID,
		Controller: &controller,
	}
}

func addDependenciesConfOptions(app *v1beta2.SparkApplication) []string {
	var depsConfOptions []string

	if len(app.Spec.Deps.Jars) > 0 {
		depsConfOptions = append(depsConfOptions, "--jars", strings.Join(app.Spec.Deps.Jars, ","))
	}
	if len(app.Spec.Deps.Files) > 0 {
		depsConfOptions = append(depsConfOptions, "--files", strings.Join(app.Spec.Deps.Files, ","))
	}
	if len(app.Spec.Deps.PyFiles) > 0 {
		depsConfOptions = append(depsConfOptions, "--py-files", strings.Join(app.Spec.Deps.PyFiles, ","))
	}
	if len(app.Spec.Deps.Packages) > 0 {
		depsConfOptions = append(depsConfOptions, "--packages", strings.Join(app.Spec.Deps.Packages, ","))
	}
	if len(app.Spec.Deps.ExcludePackages) > 0 {
		depsConfOptions = append(depsConfOptions, "--exclude-packages", strings.Join(app.Spec.Deps.ExcludePackages, ","))
	}
	if len(app.Spec.Deps.Repositories) > 0 {
		depsConfOptions = append(depsConfOptions, "--repositories", strings.Join(app.Spec.Deps.Repositories, ","))
	}

	return depsConfOptions
}

func addDriverConfOptions(app *v1beta2.SparkApplication, submissionID string) ([]string, error) {
	var driverConfOptions []string

	driverConfOptions = append(driverConfOptions,
		fmt.Sprintf("%s%s=%s", config.SparkDriverLabelKeyPrefix, config.SparkAppNameLabel, app.Name))
	driverConfOptions = append(driverConfOptions,
		fmt.Sprintf("%s%s=%s", config.SparkDriverLabelKeyPrefix, config.LaunchedBySparkOperatorLabel, "true"))
	driverConfOptions = append(driverConfOptions,
		fmt.Sprintf("%s%s=%s", config.SparkDriverLabelKeyPrefix, config.SubmissionIDLabel, submissionID))

	if app.Spec.Driver.Image != nil {
		driverConfOptions = append(driverConfOptions,
			fmt.Sprintf("%s=%s", config.SparkDriverContainerImageKey, *app.Spec.Driver.Image))
	}

	if app.Spec.Driver.Cores != nil {
		driverConfOptions = append(driverConfOptions,
			fmt.Sprintf("spark.driver.cores=%d", *app.Spec.Driver.Cores))
	}
	if app.Spec.Driver.CoreRequest != nil {
		driverConfOptions = append(driverConfOptions,
			fmt.Sprintf("%s=%s", config.SparkDriverCoreRequestKey, *app.Spec.Driver.CoreRequest))
	}
	if app.Spec.Driver.CoreLimit != nil {
		driverConfOptions = append(driverConfOptions,
			fmt.Sprintf("%s=%s", config.SparkDriverCoreLimitKey, *app.Spec.Driver.CoreLimit))
	}
	if app.Spec.Driver.Memory != nil {
		driverConfOptions = append(driverConfOptions,
			fmt.Sprintf("spark.driver.memory=%s", *app.Spec.Driver.Memory))
	}
	if app.Spec.Driver.MemoryOverhead != nil {
		driverConfOptions = append(driverConfOptions,
			fmt.Sprintf("spark.driver.memoryOverhead=%s", *app.Spec.Driver.MemoryOverhead))
	}

	if app.Spec.Driver.ServiceAccount != nil {
		driverConfOptions = append(driverConfOptions,
			fmt.Sprintf("%s=%s", config.SparkDriverServiceAccountName, *app.Spec.Driver.ServiceAccount))
	}

	if app.Spec.Driver.JavaOptions != nil {
		driverConfOptions = append(driverConfOptions,
			fmt.Sprintf("%s=%s", config.SparkDriverJavaOptions, *app.Spec.Driver.JavaOptions))
	}

	if app.Spec.Driver.KubernetesMaster != nil {
		driverConfOptions = append(driverConfOptions,
			fmt.Sprintf("%s=%s", config.SparkDriverKubernetesMaster, *app.Spec.Driver.KubernetesMaster))
	}

	//Populate SparkApplication Labels to Driver
	driverLabels := make(map[string]string)
	for key, value := range app.Labels {
		driverLabels[key] = value
	}
	for key, value := range app.Spec.Driver.Labels {
		driverLabels[key] = value
	}

	for key, value := range driverLabels {
		driverConfOptions = append(driverConfOptions,
			fmt.Sprintf("%s%s=%s", config.SparkDriverLabelKeyPrefix, key, value))
	}

	for key, value := range app.Spec.Driver.Annotations {
		driverConfOptions = append(driverConfOptions,
			fmt.Sprintf("%s%s=%s", config.SparkDriverAnnotationKeyPrefix, key, value))
	}

	for key, value := range app.Spec.Driver.EnvSecretKeyRefs {
		driverConfOptions = append(driverConfOptions,
			fmt.Sprintf("%s%s=%s:%s", config.SparkDriverSecretKeyRefKeyPrefix, key, value.Name, value.Key))
	}

	for key, value := range app.Spec.Driver.ServiceAnnotations {
		driverConfOptions = append(driverConfOptions,
			fmt.Sprintf("%s%s=%s", config.SparkDriverServiceAnnotationKeyPrefix, key, value))
	}

	driverConfOptions = append(driverConfOptions, config.GetDriverSecretConfOptions(app)...)
	driverConfOptions = append(driverConfOptions, config.GetDriverEnvVarConfOptions(app)...)

	return driverConfOptions, nil
}

func addExecutorConfOptions(app *v1beta2.SparkApplication, submissionID string) ([]string, error) {
	var executorConfOptions []string

	executorConfOptions = append(executorConfOptions,
		fmt.Sprintf("%s%s=%s", config.SparkExecutorLabelKeyPrefix, config.SparkAppNameLabel, app.Name))
	executorConfOptions = append(executorConfOptions,
		fmt.Sprintf("%s%s=%s", config.SparkExecutorLabelKeyPrefix, config.LaunchedBySparkOperatorLabel, "true"))
	executorConfOptions = append(executorConfOptions,
		fmt.Sprintf("%s%s=%s", config.SparkExecutorLabelKeyPrefix, config.SubmissionIDLabel, submissionID))

	if app.Spec.Executor.Instances != nil {
		conf := fmt.Sprintf("spark.executor.instances=%d", *app.Spec.Executor.Instances)
		executorConfOptions = append(executorConfOptions, conf)
	}

	if app.Spec.Executor.Image != nil {
		executorConfOptions = append(executorConfOptions,
			fmt.Sprintf("%s=%s", config.SparkExecutorContainerImageKey, *app.Spec.Executor.Image))
	}

	if app.Spec.Executor.Cores != nil {
		// Property "spark.executor.cores" does not allow float values.
		executorConfOptions = append(executorConfOptions,
			fmt.Sprintf("spark.executor.cores=%d", int32(*app.Spec.Executor.Cores)))
	}
	if app.Spec.Executor.CoreRequest != nil {
		executorConfOptions = append(executorConfOptions,
			fmt.Sprintf("%s=%s", config.SparkExecutorCoreRequestKey, *app.Spec.Executor.CoreRequest))
	}
	if app.Spec.Executor.CoreLimit != nil {
		executorConfOptions = append(executorConfOptions,
			fmt.Sprintf("%s=%s", config.SparkExecutorCoreLimitKey, *app.Spec.Executor.CoreLimit))
	}
	if app.Spec.Executor.Memory != nil {
		executorConfOptions = append(executorConfOptions,
			fmt.Sprintf("spark.executor.memory=%s", *app.Spec.Executor.Memory))
	}
	if app.Spec.Executor.MemoryOverhead != nil {
		executorConfOptions = append(executorConfOptions,
			fmt.Sprintf("spark.executor.memoryOverhead=%s", *app.Spec.Executor.MemoryOverhead))
	}

	if app.Spec.Executor.ServiceAccount != nil {
		executorConfOptions = append(executorConfOptions,
			fmt.Sprintf("%s=%s", config.SparkExecutorAccountName, *app.Spec.Executor.ServiceAccount))
	}

	if app.Spec.Executor.DeleteOnTermination != nil {
		executorConfOptions = append(executorConfOptions,
			fmt.Sprintf("%s=%t", config.SparkExecutorDeleteOnTermination, *app.Spec.Executor.DeleteOnTermination))
	}

	//Populate SparkApplication Labels to Executors
	executorLabels := make(map[string]string)
	for key, value := range app.Labels {
		executorLabels[key] = value
	}
	for key, value := range app.Spec.Executor.Labels {
		executorLabels[key] = value
	}
	for key, value := range executorLabels {
		executorConfOptions = append(executorConfOptions,
			fmt.Sprintf("%s%s=%s", config.SparkExecutorLabelKeyPrefix, key, value))
	}

	for key, value := range app.Spec.Executor.Annotations {
		executorConfOptions = append(executorConfOptions,
			fmt.Sprintf("%s%s=%s", config.SparkExecutorAnnotationKeyPrefix, key, value))
	}

	for key, value := range app.Spec.Executor.EnvSecretKeyRefs {
		executorConfOptions = append(executorConfOptions,
			fmt.Sprintf("%s%s=%s:%s", config.SparkExecutorSecretKeyRefKeyPrefix, key, value.Name, value.Key))
	}

	if app.Spec.Executor.JavaOptions != nil {
		executorConfOptions = append(executorConfOptions,
			fmt.Sprintf("%s=%s", config.SparkExecutorJavaOptions, *app.Spec.Executor.JavaOptions))
	}

	executorConfOptions = append(executorConfOptions, config.GetExecutorSecretConfOptions(app)...)
	executorConfOptions = append(executorConfOptions, config.GetExecutorEnvVarConfOptions(app)...)

	return executorConfOptions, nil
}

func addDynamicAllocationConfOptions(app *v1beta2.SparkApplication) []string {
	if app.Spec.DynamicAllocation == nil {
		return nil
	}

	dynamicAllocation := app.Spec.DynamicAllocation
	if !dynamicAllocation.Enabled {
		return nil
	}

	var options []string
	options = append(options, fmt.Sprintf("%s=true", config.SparkDynamicAllocationEnabled))
	// Turn on shuffle tracking if dynamic allocation is enabled.
	options = append(options, fmt.Sprintf("%s=true", config.SparkDynamicAllocationShuffleTrackingEnabled))
	if dynamicAllocation.InitialExecutors != nil {
		options = append(options, fmt.Sprintf("%s=%d", config.SparkDynamicAllocationInitialExecutors, *dynamicAllocation.InitialExecutors))
	}
	if dynamicAllocation.MinExecutors != nil {
		options = append(options, fmt.Sprintf("%s=%d", config.SparkDynamicAllocationMinExecutors, *dynamicAllocation.MinExecutors))
	}
	if dynamicAllocation.MaxExecutors != nil {
		options = append(options, fmt.Sprintf("%s=%d", config.SparkDynamicAllocationMaxExecutors, *dynamicAllocation.MaxExecutors))
	}
	if dynamicAllocation.ShuffleTrackingTimeout != nil {
		options = append(options, fmt.Sprintf("%s=%d", config.SparkDynamicAllocationShuffleTrackingTimeout, *dynamicAllocation.ShuffleTrackingTimeout))
	}

	return options
}

// addLocalDirConfOptions excludes local dir volumes, update SparkApplication and returns local dir config options
func addLocalDirConfOptions(app *v1beta2.SparkApplication) ([]string, error) {
	var localDirConfOptions []string

	sparkLocalVolumes := map[string]v1.Volume{}
	var mutateVolumes []v1.Volume

	// Filter local dir volumes
	for _, volume := range app.Spec.Volumes {
		if strings.HasPrefix(volume.Name, config.SparkLocalDirVolumePrefix) {
			sparkLocalVolumes[volume.Name] = volume
		} else {
			mutateVolumes = append(mutateVolumes, volume)
		}
	}
	app.Spec.Volumes = mutateVolumes

	// Filter local dir volumeMounts and set mutate volume mounts to driver and executor
	if app.Spec.Driver.VolumeMounts != nil {
		driverMutateVolumeMounts, driverLocalDirConfConfOptions := filterMutateMountVolumes(app.Spec.Driver.VolumeMounts, config.SparkDriverVolumesPrefix, sparkLocalVolumes)
		app.Spec.Driver.VolumeMounts = driverMutateVolumeMounts
		localDirConfOptions = append(localDirConfOptions, driverLocalDirConfConfOptions...)
	}

	if app.Spec.Executor.VolumeMounts != nil {
		executorMutateVolumeMounts, executorLocalDirConfConfOptions := filterMutateMountVolumes(app.Spec.Executor.VolumeMounts, config.SparkExecutorVolumesPrefix, sparkLocalVolumes)
		app.Spec.Executor.VolumeMounts = executorMutateVolumeMounts
		localDirConfOptions = append(localDirConfOptions, executorLocalDirConfConfOptions...)
	}

	return localDirConfOptions, nil
}

func filterMutateMountVolumes(volumeMounts []v1.VolumeMount, prefix string, sparkLocalVolumes map[string]v1.Volume) ([]v1.VolumeMount, []string) {
	var mutateMountVolumes []v1.VolumeMount
	var localDirConfOptions []string
	for _, volumeMount := range volumeMounts {
		if volume, ok := sparkLocalVolumes[volumeMount.Name]; ok {
			options := buildLocalVolumeOptions(prefix, volume, volumeMount)
			for _, option := range options {
				localDirConfOptions = append(localDirConfOptions, option)
			}
		} else {
			mutateMountVolumes = append(mutateMountVolumes, volumeMount)
		}
	}

	return mutateMountVolumes, localDirConfOptions
}

func buildLocalVolumeOptions(prefix string, volume v1.Volume, volumeMount v1.VolumeMount) []string {
	VolumeMountPathTemplate := prefix + "%s.%s.mount.path=%s"
	VolumeMountOptionTemplate := prefix + "%s.%s.options.%s=%s"

	var options []string
	switch {
	case volume.HostPath != nil:
		options = append(options, fmt.Sprintf(VolumeMountPathTemplate, string(policy.HostPath), volume.Name, volumeMount.MountPath))
		options = append(options, fmt.Sprintf(VolumeMountOptionTemplate, string(policy.HostPath), volume.Name, "path", volume.HostPath.Path))
		if volume.HostPath.Type != nil {
			options = append(options, fmt.Sprintf(VolumeMountOptionTemplate, string(policy.HostPath), volume.Name, "type", *volume.HostPath.Type))
		}
	case volume.EmptyDir != nil:
		options = append(options, fmt.Sprintf(VolumeMountPathTemplate, string(policy.EmptyDir), volume.Name, volumeMount.MountPath))
	case volume.PersistentVolumeClaim != nil:
		options = append(options, fmt.Sprintf(VolumeMountPathTemplate, string(policy.PersistentVolumeClaim), volume.Name, volumeMount.MountPath))
		options = append(options, fmt.Sprintf(VolumeMountOptionTemplate, string(policy.PersistentVolumeClaim), volume.Name, "claimName", volume.PersistentVolumeClaim.ClaimName))
	}

	return options
}
