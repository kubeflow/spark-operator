package secret

import (
	"k8s.io/api/core/v1"
)

const (
	GoogleApplicationCredentialsEnvVar        = "GOOGLE_APPLICATION_CREDENTIALS"
	DefaultServiceAccountJsonKeyFileName      = "key.json"
	serviceAccountSecretVolumeName            = "google-service-account-secret-volume"
	DefaultServiceAccountJsonKeyFileMountPath = "/etc/secrets/google"
)

// AddSecretVolumeToPod adds a secret volume for the secret with secretName into pod.
func AddSecretVolumeToPod(secretName string, pod *v1.Pod) {
	volume := v1.Volume{
		Name: serviceAccountSecretVolumeName,
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
func MountServiceAccountSecretToContainer(volumeName string, mountPath string, container *v1.Container) {
	MountSecretToContainer(volumeName, mountPath, container)
	appCredentialEnvVar := v1.EnvVar{Name: GoogleApplicationCredentialsEnvVar, Value: mountPath}
	container.Env = append(container.Env, appCredentialEnvVar)
}
