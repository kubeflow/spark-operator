package secret

import (
	"fmt"

	"k8s.io/api/core/v1"
)

const (
	// GoogleApplicationCredentialsEnvVar is the environment variable used by the
	// Application Default Credentials mechanism. More details can be found at
	// https://developers.google.com/identity/protocols/application-default-credentials.
	GoogleApplicationCredentialsEnvVar = "GOOGLE_APPLICATION_CREDENTIALS"
	// ServiceAccountJSONKeyFileName is the default name of the service account
	// Json key file. This name is added to the service account secret mount path to
	// form the path to the Json key file referred to by GOOGLE_APPLICATION_CREDENTIALS.
	ServiceAccountJSONKeyFileName = "key.json"
	// ServiceAccountSecretVolumeName is the name of the GCP service account secret volume.
	ServiceAccountSecretVolumeName     = "gcp-service-account-secret-volume"
	serviceAccountJSONKeyFileMountPath = "/etc/secrets/google"
)

// AddSecretVolumeToPod adds a secret volume for the secret with secretName into pod.
func AddSecretVolumeToPod(secretVolumeName string, secretName string, pod *v1.Pod) {
	volume := v1.Volume{
		Name: secretVolumeName,
		VolumeSource: v1.VolumeSource{
			Secret: &v1.SecretVolumeSource{
				SecretName: secretName,
			},
		},
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, volume)
}

// MountSecretToContainer mounts the secret volume with volumeName onto the mountPath into container.
func MountSecretToContainer(volumeName string, mountPath string, container *v1.Container) {
	volumeMount := v1.VolumeMount{
		Name:      volumeName,
		ReadOnly:  true,
		MountPath: mountPath,
	}
	container.VolumeMounts = append(container.VolumeMounts, volumeMount)
}

// MountServiceAccountSecretToContainer mounts the service account secret volume with volumeName onto
// the mountPath into container and also sets environment variable GOOGLE_APPLICATION_CREDENTIALS to
// the service account key file in the volume.
func MountServiceAccountSecretToContainer(volumeName string, container *v1.Container) {
	MountSecretToContainer(volumeName, serviceAccountJSONKeyFileMountPath, container)
	jsonKeyFilePath := fmt.Sprintf("%s/%s", serviceAccountJSONKeyFileMountPath, ServiceAccountJSONKeyFileName)
	appCredentialEnvVar := v1.EnvVar{Name: GoogleApplicationCredentialsEnvVar, Value: jsonKeyFilePath}
	container.Env = append(container.Env, appCredentialEnvVar)
}
