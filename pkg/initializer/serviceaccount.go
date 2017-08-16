package initializer

import (
	"k8s.io/api/core/v1"
)

const (
	GoogleApplicationCredentialsEnvVar        = "GOOGLE_APPLICATION_CREDENTIALS"
	DefaultServiceAccountJsonKeyFileName      = "service_account_key.json"
	serviceAccountSecretVolumeName            = "google-service-account-secret-volume"
	DefaultServiceAccountJsonKeyFileMountPath = "/etc/secrets/google"
)

func AddServiceAccountSecretVolumeToPod(secretName string, pod *v1.Pod) {
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

func MountServiceAccountSecretToContainer(secretName string, mountPath string, container *v1.Container) {
	volumeMount := v1.VolumeMount{
		Name:      secretName,
		ReadOnly:  true,
		MountPath: mountPath,
	}
	container.VolumeMounts = append(container.VolumeMounts, volumeMount)
	appCredentialEnvVar := v1.EnvVar{Name: GoogleApplicationCredentialsEnvVar, Value: mountPath}
	container.Env = append(container.Env, appCredentialEnvVar)
}
