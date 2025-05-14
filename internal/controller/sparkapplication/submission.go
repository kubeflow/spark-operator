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
	"strings"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/common"
	"github.com/kubeflow/spark-operator/pkg/util"
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

func runSparkSubmit(submission *submission) error {
	sparkHome, present := os.LookupEnv(common.EnvSparkHome)
	if !present {
		return fmt.Errorf("env %s is not specified", common.EnvSparkHome)
	}
	command := filepath.Join(sparkHome, "bin", "spark-submit")
	cmd := exec.Command(command, submission.args...)
	_, err := cmd.Output()
	if err != nil {
		var errorMsg string
		if exitErr, ok := err.(*exec.ExitError); ok {
			errorMsg = string(exitErr.Stderr)
		}
		// The driver pod of the application already exists.
		if strings.Contains(errorMsg, common.ErrorCodePodAlreadyExists) {
			return fmt.Errorf("driver pod already exist")
		}
		if errorMsg != "" {
			return fmt.Errorf("failed to run spark-submit: %s", errorMsg)
		}
		return fmt.Errorf("failed to run spark-submit: %v", err)
	}
	return nil
}

// buildSparkSubmitArgs builds the arguments for spark-submit.
func buildSparkSubmitArgs(app *v1beta2.SparkApplication) ([]string, error) {
	optionFuncs := []sparkSubmitOptionFunc{
		masterOption,
		deployModeOption,
		mainClassOption,
		nameOption,
		dependenciesOption,
		namespaceOption,
		imageOption,
		pythonVersionOption,
		memoryOverheadFactorOption,
		submissionWaitAppCompletionOption,
		sparkConfOption,
		hadoopConfOption,
		driverPodTemplateOption,
		driverPodNameOption,
		driverConfOption,
		driverEnvOption,
		driverSecretOption,
		driverVolumeMountsOption,
		executorPodTemplateOption,
		executorConfOption,
		executorEnvOption,
		executorSecretOption,
		executorVolumeMountsOption,
		nodeSelectorOption,
		dynamicAllocationOption,
		proxyUserOption,
		mainApplicationFileOption,
		applicationOption,
	}

	var args []string
	for _, optionFunc := range optionFuncs {
		option, err := optionFunc(app)
		if err != nil {
			return nil, err
		}
		args = append(args, option...)
	}

	return args, nil
}

type sparkSubmitOptionFunc func(*v1beta2.SparkApplication) ([]string, error)

func masterOption(_ *v1beta2.SparkApplication) ([]string, error) {
	masterURL, err := util.GetMasterURL()
	if err != nil {
		return nil, fmt.Errorf("failed to get master URL: %v", err)
	}
	args := []string{
		"--master",
		masterURL,
	}
	return args, nil
}

func deployModeOption(app *v1beta2.SparkApplication) ([]string, error) {
	args := []string{
		"--deploy-mode",
		string(app.Spec.Mode),
	}
	return args, nil
}

func mainClassOption(app *v1beta2.SparkApplication) ([]string, error) {
	if app.Spec.MainClass == nil {
		return nil, nil
	}
	args := []string{
		"--class",
		*app.Spec.MainClass,
	}
	return args, nil
}

func nameOption(app *v1beta2.SparkApplication) ([]string, error) {
	args := []string{"--name", app.Name}
	return args, nil
}

func namespaceOption(app *v1beta2.SparkApplication) ([]string, error) {
	args := []string{
		"--conf",
		fmt.Sprintf("%s=%s", common.SparkKubernetesNamespace, app.Namespace),
	}
	return args, nil
}

func driverPodNameOption(app *v1beta2.SparkApplication) ([]string, error) {
	args := []string{
		"--conf",
		fmt.Sprintf("%s=%s", common.SparkKubernetesDriverPodName, util.GetDriverPodName(app)),
	}
	return args, nil
}

func dependenciesOption(app *v1beta2.SparkApplication) ([]string, error) {
	var args []string

	if len(app.Spec.Deps.Jars) > 0 {
		args = append(args, "--jars", strings.Join(app.Spec.Deps.Jars, ","))
	}

	if len(app.Spec.Deps.Packages) > 0 {
		args = append(args, "--packages", strings.Join(app.Spec.Deps.Packages, ","))
	}

	if len(app.Spec.Deps.ExcludePackages) > 0 {
		args = append(args, "--exclude-packages", strings.Join(app.Spec.Deps.ExcludePackages, ","))
	}

	if len(app.Spec.Deps.Repositories) > 0 {
		args = append(args, "--repositories", strings.Join(app.Spec.Deps.Repositories, ","))
	}

	if len(app.Spec.Deps.PyFiles) > 0 {
		args = append(args, "--py-files", strings.Join(app.Spec.Deps.PyFiles, ","))
	}

	if len(app.Spec.Deps.Files) > 0 {
		args = append(args, "--files", strings.Join(app.Spec.Deps.Files, ","))
	}

	if len(app.Spec.Deps.Archives) > 0 {
		args = append(args, "--archives", strings.Join(app.Spec.Deps.Archives, ","))
	}

	return args, nil
}

func imageOption(app *v1beta2.SparkApplication) ([]string, error) {
	var args []string
	if app.Spec.Image != nil && *app.Spec.Image != "" {
		args = append(args,
			"--conf",
			fmt.Sprintf("%s=%s", common.SparkKubernetesContainerImage, *app.Spec.Image),
		)
	}

	if app.Spec.ImagePullPolicy != nil && *app.Spec.ImagePullPolicy != "" {
		args = append(args,
			"--conf",
			fmt.Sprintf("%s=%s", common.SparkKubernetesContainerImagePullPolicy, *app.Spec.ImagePullPolicy),
		)
	}

	if len(app.Spec.ImagePullSecrets) > 0 {
		secrets := strings.Join(app.Spec.ImagePullSecrets, ",")
		args = append(args,
			"--conf",
			fmt.Sprintf("%s=%s", common.SparkKubernetesContainerImagePullSecrets, secrets),
		)
	}

	return args, nil
}

func pythonVersionOption(app *v1beta2.SparkApplication) ([]string, error) {
	if app.Spec.PythonVersion == nil || *app.Spec.PythonVersion == "" {
		return nil, nil
	}
	args := []string{
		"--conf",
		fmt.Sprintf("%s=%s", common.SparkKubernetesPysparkPythonVersion, *app.Spec.PythonVersion),
	}
	return args, nil
}

func memoryOverheadFactorOption(app *v1beta2.SparkApplication) ([]string, error) {
	if app.Spec.MemoryOverheadFactor == nil || *app.Spec.MemoryOverheadFactor == "" {
		return nil, nil
	}
	args := []string{
		"--conf",
		fmt.Sprintf("%s=%s", common.SparkKubernetesMemoryOverheadFactor, *app.Spec.MemoryOverheadFactor),
	}
	return args, nil
}

func submissionWaitAppCompletionOption(_ *v1beta2.SparkApplication) ([]string, error) {
	// spark-submit triggered by Spark operator should never wait for app completion
	args := []string{
		"--conf",
		fmt.Sprintf("%s=false", common.SparkKubernetesSubmissionWaitAppCompletion),
	}
	return args, nil
}

func sparkConfOption(app *v1beta2.SparkApplication) ([]string, error) {
	if app.Spec.SparkConf == nil {
		return nil, nil
	}
	var args []string
	// Add Spark configuration properties.
	for key, value := range app.Spec.SparkConf {
		// Configuration property for the driver pod name has already been set.
		if key != common.SparkKubernetesDriverPodName {
			args = append(args, "--conf", fmt.Sprintf("%s=%s", key, value))
		}
	}
	return args, nil
}

func hadoopConfOption(app *v1beta2.SparkApplication) ([]string, error) {
	if app.Spec.HadoopConf == nil {
		return nil, nil
	}
	var args []string
	// Add Hadoop configuration properties.
	for key, value := range app.Spec.HadoopConf {
		args = append(args, "--conf", fmt.Sprintf("spark.hadoop.%s=%s", key, value))
	}
	return args, nil
}

func nodeSelectorOption(app *v1beta2.SparkApplication) ([]string, error) {
	var args []string
	for key, value := range app.Spec.NodeSelector {
		property := fmt.Sprintf(common.SparkKubernetesNodeSelectorTemplate, key)
		args = append(args, "--conf", fmt.Sprintf("%s=%s", property, value))
	}
	return args, nil
}

func driverConfOption(app *v1beta2.SparkApplication) ([]string, error) {
	var args []string
	var property string

	property = fmt.Sprintf(common.SparkKubernetesDriverLabelTemplate, common.LabelSparkAppName)
	args = append(args, "--conf", fmt.Sprintf("%s=%s", property, app.Name))

	property = fmt.Sprintf(common.SparkKubernetesDriverLabelTemplate, common.LabelLaunchedBySparkOperator)
	args = append(args, "--conf", fmt.Sprintf("%s=%s", property, "true"))

	// If Spark version is less than 3.0.0 or driver pod template is not defined, then the driver pod needs to be mutated by the webhook.
	if util.CompareSemanticVersion(app.Spec.SparkVersion, "3.0.0") < 0 || app.Spec.Driver.Template == nil {
		property = fmt.Sprintf(common.SparkKubernetesDriverLabelTemplate, common.LabelMutatedBySparkOperator)
		args = append(args, "--conf", fmt.Sprintf("%s=%s", property, "true"))
	}

	property = fmt.Sprintf(common.SparkKubernetesDriverLabelTemplate, common.LabelSubmissionID)
	args = append(args, "--conf", fmt.Sprintf("%s=%s", property, app.Status.SubmissionID))

	if app.Spec.Driver.Image != nil && *app.Spec.Driver.Image != "" {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%s", common.SparkKubernetesDriverContainerImage, *app.Spec.Driver.Image))
	} else if app.Spec.Image != nil && *app.Spec.Image != "" {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%s", common.SparkKubernetesDriverContainerImage, *app.Spec.Image))
	}

	if app.Spec.Driver.Cores != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%d", common.SparkDriverCores, *app.Spec.Driver.Cores))
	}

	if app.Spec.Driver.CoreRequest != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%s", common.SparkKubernetesDriverRequestCores, *app.Spec.Driver.CoreRequest))
	}

	if app.Spec.Driver.CoreLimit != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%s", common.SparkKubernetesDriverLimitCores, *app.Spec.Driver.CoreLimit))
	}

	if app.Spec.Driver.Memory != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%s", common.SparkDriverMemory, *app.Spec.Driver.Memory))
	}

	if app.Spec.Driver.MemoryOverhead != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%s", common.SparkDriverMemoryOverhead, *app.Spec.Driver.MemoryOverhead))
	}

	if app.Spec.Driver.ServiceAccount != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%s",
				common.SparkKubernetesAuthenticateDriverServiceAccountName, *app.Spec.Driver.ServiceAccount),
		)
	}

	if app.Spec.Driver.JavaOptions != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%s", common.SparkDriverExtraJavaOptions, *app.Spec.Driver.JavaOptions))
	}

	if app.Spec.Driver.KubernetesMaster != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%s", common.SparkKubernetesDriverMaster, *app.Spec.Driver.KubernetesMaster))
	}

	// Populate SparkApplication labels to driver pod
	for key, value := range app.Labels {
		property = fmt.Sprintf(common.SparkKubernetesDriverLabelTemplate, key)
		args = append(args, "--conf", fmt.Sprintf("%s=%s", property, value))
	}

	for key, value := range app.Spec.Driver.Labels {
		property = fmt.Sprintf(common.SparkKubernetesDriverLabelTemplate, key)
		args = append(args, "--conf", fmt.Sprintf("%s=%s", property, value))
	}

	for key, value := range app.Spec.Driver.Annotations {
		property = fmt.Sprintf(common.SparkKubernetesDriverAnnotationTemplate, key)
		args = append(args, "--conf", fmt.Sprintf("%s=%s", property, value))
	}

	for key, value := range app.Spec.Driver.ServiceLabels {
		property = fmt.Sprintf(common.SparkKubernetesDriverServiceLabelTemplate, key)
		args = append(args, "--conf", fmt.Sprintf("%s=%s", property, value))
	}

	for key, value := range app.Spec.Driver.ServiceAnnotations {
		property = fmt.Sprintf(common.SparkKubernetesDriverServiceAnnotationTemplate, key)
		args = append(args, "--conf", fmt.Sprintf("%s=%s", property, value))
	}

	for key, value := range app.Spec.Driver.EnvSecretKeyRefs {
		property = fmt.Sprintf(common.SparkKubernetesDriverSecretKeyRefTemplate, key)
		args = append(args, "--conf", fmt.Sprintf("%s=%s:%s", property, value.Name, value.Key))
	}

	return args, nil
}

// driverSecretOption returns a list of spark-submit arguments for mounting secrets to driver pod.
func driverSecretOption(app *v1beta2.SparkApplication) ([]string, error) {
	var args []string
	for _, secret := range app.Spec.Driver.Secrets {
		property := fmt.Sprintf(common.SparkKubernetesDriverSecretsTemplate, secret.Name)
		args = append(args, "--conf", fmt.Sprintf("%s=%s", property, secret.Path))
		if secret.Type == v1beta2.SecretTypeGCPServiceAccount {
			property := fmt.Sprintf(common.SparkKubernetesDriverEnvTemplate, common.EnvGoogleApplicationCredentials)
			conf := fmt.Sprintf("%s=%s", property, filepath.Join(secret.Path, common.ServiceAccountJSONKeyFileName))
			args = append(args, "--conf", conf)
		} else if secret.Type == v1beta2.SecretTypeHadoopDelegationToken {
			property := fmt.Sprintf(common.SparkKubernetesDriverEnvTemplate, common.EnvHadoopTokenFileLocation)
			conf := fmt.Sprintf("%s=%s", property, filepath.Join(secret.Path, common.HadoopDelegationTokenFileName))
			args = append(args, "--conf", conf)
		}
	}
	return args, nil
}

func driverVolumeMountsOption(app *v1beta2.SparkApplication) ([]string, error) {
	volumes := util.GetLocalVolumes(app)
	if volumes == nil {
		return nil, nil
	}

	volumeMounts := util.GetDriverLocalVolumeMounts(app)
	if volumeMounts == nil {
		return nil, nil
	}

	args := []string{}
	for _, volumeMount := range volumeMounts {
		volumeName := volumeMount.Name
		volume, ok := volumes[volumeName]
		if !ok {
			return args, fmt.Errorf("volume %s not found", volumeName)
		}

		var volumeType string
		switch {
		case volume.EmptyDir != nil:
			volumeType = common.VolumeTypeEmptyDir
		case volume.HostPath != nil:
			volumeType = common.VolumeTypeHostPath
		case volume.NFS != nil:
			volumeType = common.VolumeTypeNFS
		case volume.PersistentVolumeClaim != nil:
			volumeType = common.VolumeTypePersistentVolumeClaim
		default:
			return nil, fmt.Errorf("unsupported volume type")
		}

		if volumeMount.MountPath != "" {
			args = append(
				args,
				"--conf",
				fmt.Sprintf(
					"%s=%s",
					fmt.Sprintf(
						common.SparkKubernetesDriverVolumesMountPathTemplate,
						volumeType,
						volumeName,
					),
					volumeMount.MountPath,
				),
			)
		}

		if volumeMount.SubPath != "" {
			args = append(
				args,
				"--conf",
				fmt.Sprintf(
					"%s=%s",
					fmt.Sprintf(
						common.SparkKubernetesDriverVolumesMountSubPathTemplate,
						volumeType,
						volumeName,
					),
					volumeMount.SubPath,
				),
			)
		}

		if volumeMount.ReadOnly {
			args = append(
				args,
				"--conf",
				fmt.Sprintf(
					"%s=%s",
					fmt.Sprintf(
						common.SparkKubernetesDriverVolumesMountReadOnlyTemplate,
						volumeType,
						volumeName,
					),
					"true",
				),
			)
		}

		switch volumeType {
		case common.VolumeTypeEmptyDir:
			if volume.EmptyDir.SizeLimit != nil {
				args = append(
					args,
					"--conf",
					fmt.Sprintf(
						"%s=%s",
						fmt.Sprintf(
							common.SparkKubernetesDriverVolumesOptionsTemplate,
							common.VolumeTypeEmptyDir,
							volume.Name,
							"sizeLimit",
						),
						volume.EmptyDir.SizeLimit.String(),
					),
				)
			}
		case common.VolumeTypeHostPath:
			args = append(
				args,
				"--conf",
				fmt.Sprintf(
					"%s=%s",
					fmt.Sprintf(
						common.SparkKubernetesDriverVolumesOptionsTemplate,
						common.VolumeTypeHostPath,
						volume.Name,
						"path",
					),
					volume.HostPath.Path,
				),
			)

			if volume.HostPath.Type != nil {
				args = append(
					args,
					"--conf",
					fmt.Sprintf(
						"%s=%s",
						fmt.Sprintf(
							common.SparkKubernetesDriverVolumesOptionsTemplate,
							common.VolumeTypeHostPath,
							volume.Name,
							"type",
						),
						*volume.HostPath.Type,
					),
				)
			}

		case common.VolumeTypeNFS:
			args = append(
				args,
				"--conf",
				fmt.Sprintf(
					"%s=%s",
					fmt.Sprintf(
						common.SparkKubernetesDriverVolumesOptionsTemplate,
						common.VolumeTypeNFS,
						volume.Name,
						"path",
					),
					volume.NFS.Path,
				),
			)

			args = append(
				args,
				"--conf",
				fmt.Sprintf(
					"%s=%s",
					fmt.Sprintf(
						common.SparkKubernetesDriverVolumesOptionsTemplate,
						common.VolumeTypeNFS,
						volume.Name,
						"server",
					),
					volume.NFS.Server,
				),
			)

			if volume.NFS.ReadOnly {
				args = append(
					args,
					"--conf",
					fmt.Sprintf(
						"%s=%s",
						fmt.Sprintf(
							common.SparkKubernetesDriverVolumesOptionsTemplate,
							common.VolumeTypeNFS,
							volume.Name,
							"readOnly",
						),
						"true",
					),
				)
			}

		case common.VolumeTypePersistentVolumeClaim:
			args = append(
				args,
				"--conf",
				fmt.Sprintf(
					"%s=%s",
					fmt.Sprintf(
						common.SparkKubernetesDriverVolumesOptionsTemplate,
						common.VolumeTypePersistentVolumeClaim,
						volume.Name,
						"claimName",
					),
					volume.PersistentVolumeClaim.ClaimName,
				),
			)

			if volume.PersistentVolumeClaim.ReadOnly {
				args = append(
					args,
					"--conf",
					fmt.Sprintf(
						"%s=%s",
						fmt.Sprintf(
							common.SparkKubernetesDriverVolumesOptionsTemplate,
							common.VolumeTypePersistentVolumeClaim,
							volume.Name,
							"readOnly",
						),
						"true",
					),
				)
			}
		}
	}
	return args, nil
}

// driverEnvOption returns a list of spark-submit arguments for configuring driver environment variables.
func driverEnvOption(app *v1beta2.SparkApplication) ([]string, error) {
	var args []string
	for key, value := range app.Spec.Driver.EnvVars {
		property := fmt.Sprintf(common.SparkKubernetesDriverEnvTemplate, key)
		args = append(args, "--conf", fmt.Sprintf("%s=%s", property, value))
	}
	return args, nil
}

func executorConfOption(app *v1beta2.SparkApplication) ([]string, error) {
	var args []string
	var property string

	property = fmt.Sprintf(common.SparkKubernetesExecutorLabelTemplate, common.LabelSparkAppName)
	args = append(args, "--conf", fmt.Sprintf("%s=%s", property, app.Name))

	property = fmt.Sprintf(common.SparkKubernetesExecutorLabelTemplate, common.LabelLaunchedBySparkOperator)
	args = append(args, "--conf", fmt.Sprintf("%s=%s", property, "true"))

	// If Spark version is less than 3.0.0 or executor pod template is not defined, then the executor pods need to be mutated by the webhook.
	if util.CompareSemanticVersion(app.Spec.SparkVersion, "3.0.0") < 0 || app.Spec.Executor.Template == nil {
		property = fmt.Sprintf(common.SparkKubernetesExecutorLabelTemplate, common.LabelMutatedBySparkOperator)
		args = append(args, "--conf", fmt.Sprintf("%s=%s", property, "true"))
	}

	property = fmt.Sprintf(common.SparkKubernetesExecutorLabelTemplate, common.LabelSubmissionID)
	args = append(args, "--conf", fmt.Sprintf("%s=%s", property, app.Status.SubmissionID))

	if app.Spec.Executor.Instances != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%d", common.SparkExecutorInstances, *app.Spec.Executor.Instances))
	}

	if app.Spec.Executor.Image != nil && *app.Spec.Executor.Image != "" {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%s", common.SparkKubernetesExecutorContainerImage, *app.Spec.Executor.Image))
	} else if app.Spec.Image != nil && *app.Spec.Image != "" {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%s", common.SparkKubernetesExecutorContainerImage, *app.Spec.Image))
	}

	if app.Spec.Executor.Cores != nil {
		// Property "spark.executor.cores" does not allow float values.
		args = append(args, "--conf",
			fmt.Sprintf("%s=%d", common.SparkExecutorCores, *app.Spec.Executor.Cores))
	}
	if app.Spec.Executor.CoreRequest != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%s", common.SparkKubernetesExecutorRequestCores, *app.Spec.Executor.CoreRequest))
	}
	if app.Spec.Executor.CoreLimit != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%s", common.SparkKubernetesExecutorLimitCores, *app.Spec.Executor.CoreLimit))
	}
	if app.Spec.Executor.Memory != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%s", common.SparkExecutorMemory, *app.Spec.Executor.Memory))
	}
	if app.Spec.Executor.MemoryOverhead != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%s", common.SparkExecutorMemoryOverhead, *app.Spec.Executor.MemoryOverhead))
	}

	if app.Spec.Executor.ServiceAccount != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%s", common.SparkKubernetesAuthenticateExecutorServiceAccountName, *app.Spec.Executor.ServiceAccount))
	}

	if app.Spec.Executor.DeleteOnTermination != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%t", common.SparkKubernetesExecutorDeleteOnTermination, *app.Spec.Executor.DeleteOnTermination))
	}

	// Populate SparkApplication labels to executor pod
	for key, value := range app.Labels {
		property := fmt.Sprintf(common.SparkKubernetesExecutorLabelTemplate, key)
		args = append(args, "--conf", fmt.Sprintf("%s=%s", property, value))
	}
	for key, value := range app.Spec.Executor.Labels {
		property := fmt.Sprintf(common.SparkKubernetesExecutorLabelTemplate, key)
		args = append(args, "--conf", fmt.Sprintf("%s=%s", property, value))
	}

	for key, value := range app.Spec.Executor.Annotations {
		property := fmt.Sprintf(common.SparkKubernetesExecutorAnnotationTemplate, key)
		args = append(args, "--conf", fmt.Sprintf("%s=%s", property, value))
	}

	for key, value := range app.Spec.Executor.EnvSecretKeyRefs {
		property := fmt.Sprintf(common.SparkKubernetesExecutorSecretKeyRefTemplate, key)
		args = append(args, "--conf", fmt.Sprintf("%s=%s:%s", property, value.Name, value.Key))
	}

	if app.Spec.Executor.JavaOptions != nil {
		args = append(args, "--conf", fmt.Sprintf("%s=%s", common.SparkExecutorExtraJavaOptions, *app.Spec.Executor.JavaOptions))
	}

	return args, nil
}

func executorSecretOption(app *v1beta2.SparkApplication) ([]string, error) {
	var args []string
	for _, secret := range app.Spec.Executor.Secrets {
		property := fmt.Sprintf(common.SparkKubernetesExecutorSecretsTemplate, secret.Name)
		args = append(args, "--conf", fmt.Sprintf("%s=%s", property, secret.Path))
		switch secret.Type {
		case v1beta2.SecretTypeGCPServiceAccount:
			property := fmt.Sprintf(common.SparkExecutorEnvTemplate, common.EnvGoogleApplicationCredentials)
			args = append(args, "--conf", fmt.Sprintf("%s=%s", property,
				filepath.Join(secret.Path, common.ServiceAccountJSONKeyFileName)))
		case v1beta2.SecretTypeHadoopDelegationToken:
			property := fmt.Sprintf(common.SparkExecutorEnvTemplate, common.EnvHadoopTokenFileLocation)
			args = append(args, "--conf", fmt.Sprintf("%s=%s", property,
				filepath.Join(secret.Path, common.HadoopDelegationTokenFileName)))
		}
	}
	return args, nil
}

func executorVolumeMountsOption(app *v1beta2.SparkApplication) ([]string, error) {
	volumes := util.GetLocalVolumes(app)
	if volumes == nil {
		return nil, nil
	}

	volumeMounts := util.GetExecutorLocalVolumeMounts(app)
	if volumeMounts == nil {
		return nil, nil
	}

	args := []string{}
	for _, volumeMount := range volumeMounts {
		volumeName := volumeMount.Name
		volume, ok := volumes[volumeName]
		if !ok {
			return args, fmt.Errorf("volume %s not found", volumeName)
		}

		var volumeType string
		switch {
		case volume.EmptyDir != nil:
			volumeType = common.VolumeTypeEmptyDir
		case volume.HostPath != nil:
			volumeType = common.VolumeTypeHostPath
		case volume.NFS != nil:
			volumeType = common.VolumeTypeNFS
		case volume.PersistentVolumeClaim != nil:
			volumeType = common.VolumeTypePersistentVolumeClaim
		default:
			return nil, fmt.Errorf("unsupported volume type")
		}

		if volumeMount.MountPath != "" {
			args = append(
				args,
				"--conf",
				fmt.Sprintf(
					"%s=%s",
					fmt.Sprintf(
						common.SparkKubernetesExecutorVolumesMountPathTemplate,
						volumeType,
						volumeName,
					),
					volumeMount.MountPath,
				),
			)
		}

		if volumeMount.SubPath != "" {
			args = append(
				args,
				"--conf",
				fmt.Sprintf(
					"%s=%s",
					fmt.Sprintf(
						common.SparkKubernetesExecutorVolumesMountSubPathTemplate,
						volumeType,
						volumeName,
					),
					volumeMount.SubPath,
				),
			)
		}

		if volumeMount.ReadOnly {
			args = append(
				args,
				"--conf",
				fmt.Sprintf(
					"%s=%s",
					fmt.Sprintf(
						common.SparkKubernetesExecutorVolumesMountReadOnlyTemplate,
						volumeType,
						volumeName,
					),
					"true",
				),
			)
		}
		switch volumeType {
		case common.VolumeTypeEmptyDir:
			if volume.EmptyDir.SizeLimit != nil {
				args = append(
					args,
					"--conf",
					fmt.Sprintf(
						"%s=%s",
						fmt.Sprintf(
							common.SparkKubernetesExecutorVolumesOptionsTemplate,
							common.VolumeTypeEmptyDir,
							volume.Name,
							"sizeLimit",
						),
						volume.EmptyDir.SizeLimit.String(),
					),
				)
			}
		case common.VolumeTypeHostPath:
			args = append(
				args,
				"--conf",
				fmt.Sprintf(
					"%s=%s",
					fmt.Sprintf(
						common.SparkKubernetesExecutorVolumesOptionsTemplate,
						common.VolumeTypeHostPath,
						volume.Name,
						"path",
					),
					volume.HostPath.Path,
				),
			)

			if volume.HostPath.Type != nil {
				args = append(
					args,
					"--conf",
					fmt.Sprintf(
						"%s=%s",
						fmt.Sprintf(
							common.SparkKubernetesExecutorVolumesOptionsTemplate,
							common.VolumeTypeHostPath,
							volume.Name,
							"type",
						),
						*volume.HostPath.Type,
					),
				)
			}

		case common.VolumeTypeNFS:
			args = append(
				args,
				"--conf",
				fmt.Sprintf(
					"%s=%s",
					fmt.Sprintf(
						common.SparkKubernetesExecutorVolumesOptionsTemplate,
						common.VolumeTypeNFS,
						volume.Name,
						"path",
					),
					volume.NFS.Path,
				),
			)

			args = append(
				args,
				"--conf",
				fmt.Sprintf(
					"%s=%s",
					fmt.Sprintf(
						common.SparkKubernetesExecutorVolumesOptionsTemplate,
						common.VolumeTypeNFS,
						volume.Name,
						"server",
					),
					volume.NFS.Server,
				),
			)

			if volume.NFS.ReadOnly {
				args = append(
					args,
					"--conf",
					fmt.Sprintf(
						"%s=%s",
						fmt.Sprintf(
							common.SparkKubernetesExecutorVolumesOptionsTemplate,
							common.VolumeTypeNFS,
							volume.Name,
							"readOnly",
						),
						"true",
					),
				)
			}

		case common.VolumeTypePersistentVolumeClaim:
			args = append(
				args,
				"--conf",
				fmt.Sprintf(
					"%s=%s",
					fmt.Sprintf(
						common.SparkKubernetesExecutorVolumesOptionsTemplate,
						common.VolumeTypePersistentVolumeClaim,
						volume.Name,
						"claimName",
					),
					volume.PersistentVolumeClaim.ClaimName,
				),
			)

			if volume.PersistentVolumeClaim.ReadOnly {
				args = append(
					args,
					"--conf",
					fmt.Sprintf(
						"%s=%s",
						fmt.Sprintf(
							common.SparkKubernetesExecutorVolumesOptionsTemplate,
							common.VolumeTypePersistentVolumeClaim,
							volume.Name,
							"readOnly",
						),
						"true",
					),
				)
			}
		}
	}
	return args, nil
}

func executorEnvOption(app *v1beta2.SparkApplication) ([]string, error) {
	var args []string
	for key, value := range app.Spec.Executor.EnvVars {
		property := fmt.Sprintf(common.SparkExecutorEnvTemplate, key)
		args = append(args, "--conf", fmt.Sprintf("%s=%s", property, value))
	}
	return args, nil
}

func dynamicAllocationOption(app *v1beta2.SparkApplication) ([]string, error) {
	if app.Spec.DynamicAllocation == nil || !app.Spec.DynamicAllocation.Enabled {
		return nil, nil
	}

	var args []string
	dynamicAllocation := app.Spec.DynamicAllocation
	args = append(args, "--conf",
		fmt.Sprintf("%s=true", common.SparkDynamicAllocationEnabled))

	if dynamicAllocation.InitialExecutors != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%d", common.SparkDynamicAllocationInitialExecutors, *dynamicAllocation.InitialExecutors))
	}
	if dynamicAllocation.MinExecutors != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%d", common.SparkDynamicAllocationMinExecutors, *dynamicAllocation.MinExecutors))
	}
	if dynamicAllocation.MaxExecutors != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%d", common.SparkDynamicAllocationMaxExecutors, *dynamicAllocation.MaxExecutors))
	}
	shuffleTrackingEnabled := true
	if dynamicAllocation.ShuffleTrackingEnabled != nil {
		shuffleTrackingEnabled = *dynamicAllocation.ShuffleTrackingEnabled
	}
	args = append(args, "--conf",
		fmt.Sprintf("%s=%t", common.SparkDynamicAllocationShuffleTrackingEnabled, shuffleTrackingEnabled))
	if dynamicAllocation.ShuffleTrackingTimeout != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%d", common.SparkDynamicAllocationShuffleTrackingTimeout, *dynamicAllocation.ShuffleTrackingTimeout))
	}

	return args, nil
}

func proxyUserOption(app *v1beta2.SparkApplication) ([]string, error) {
	if app.Spec.ProxyUser == nil || *app.Spec.ProxyUser == "" {
		return nil, nil
	}
	args := []string{
		"--proxy-user",
		*app.Spec.ProxyUser,
	}
	return args, nil
}

func mainApplicationFileOption(app *v1beta2.SparkApplication) ([]string, error) {
	if app.Spec.MainApplicationFile == nil {
		return nil, nil
	}
	args := []string{*app.Spec.MainApplicationFile}
	return args, nil
}

// applicationOption returns the application arguments.
func applicationOption(app *v1beta2.SparkApplication) ([]string, error) {
	return app.Spec.Arguments, nil
}

// driverPodTemplateOption returns the driver pod template arguments.
func driverPodTemplateOption(app *v1beta2.SparkApplication) ([]string, error) {
	if app.Spec.Driver.Template == nil {
		return []string{}, nil
	}

	podTemplateFile := fmt.Sprintf("/tmp/spark/%s/driver-pod-template.yaml", app.Status.SubmissionID)
	if err := util.WriteObjectToFile(app.Spec.Driver.Template, podTemplateFile); err != nil {
		return []string{}, err
	}
	logger.V(1).Info("Created driver pod template file for SparkApplication", "name", app.Name, "namespace", app.Namespace, "file", podTemplateFile)

	args := []string{
		"--conf",
		fmt.Sprintf("%s=%s", common.SparkKubernetesDriverPodTemplateFile, podTemplateFile),
		"--conf",
		fmt.Sprintf("%s=%s", common.SparkKubernetesDriverPodTemplateContainerName, common.SparkDriverContainerName),
	}
	return args, nil
}

// executorPodTemplateOption returns the executor pod template arguments.
func executorPodTemplateOption(app *v1beta2.SparkApplication) ([]string, error) {
	if app.Spec.Executor.Template == nil {
		return []string{}, nil
	}

	podTemplateFile := fmt.Sprintf("/tmp/spark/%s/executor-pod-template.yaml", app.Status.SubmissionID)
	if err := util.WriteObjectToFile(app.Spec.Executor.Template, podTemplateFile); err != nil {
		return []string{}, err
	}
	logger.V(1).Info("Created executor pod template file for SparkApplication", "name", app.Name, "namespace", app.Namespace, "file", podTemplateFile)

	args := []string{
		"--conf",
		fmt.Sprintf("%s=%s", common.SparkKubernetesExecutorPodTemplateFile, podTemplateFile),
		"--conf",
		fmt.Sprintf("%s=%s", common.SparkKubernetesExecutorPodTemplateContainerName, common.Spark3DefaultExecutorContainerName),
	}
	return args, nil
}
