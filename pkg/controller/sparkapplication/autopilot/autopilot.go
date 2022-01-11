package autopilot

import (
	"fmt"
	"reflect"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"

	corev1 "k8s.io/api/core/v1"
	rolev1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
)

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

func GetDefaultDriverServiceAccount(app *v1beta2.SparkApplication) string {
	return fmt.Sprintf("%s-driver", app.Name)
}

func CreateOrUpdateDriverRBAC(app *v1beta2.SparkApplication, kubeClient clientset.Interface) error {
	saService := NewSAService(kubeClient)
	rbacService := NewRBACService(kubeClient)

	roleName := fmt.Sprintf("%s-driver-role", app.Name)
	roleBindingName := fmt.Sprintf("%s-driver-binding", app.Name)

	driverSA := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GetDefaultDriverServiceAccount(app),
			Namespace: app.Namespace,
			Labels: map[string]string{
				"control-plane": "google-spark-operator",
			},
			OwnerReferences: []metav1.OwnerReference{*getOwnerReference(app)},
		},
	}

	err := saService.CreateOrUpdateServiceAccount(app.Namespace, driverSA)
	if err != nil {
		return err
	}

	driverRole := &rolev1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      roleName,
			Namespace: app.Namespace,
			Labels: map[string]string{
				"control-plane": "google-spark-operator",
			},
			OwnerReferences: []metav1.OwnerReference{*getOwnerReference(app)},
		},
		Rules: []rolev1.PolicyRule{
			{
				Verbs:     []string{rolev1.VerbAll},
				APIGroups: []string{""},
				Resources: []string{corev1.ResourcePods.String()},
			},
			{
				Verbs:     []string{rolev1.VerbAll},
				APIGroups: []string{""},
				Resources: []string{corev1.ResourceServices.String()},
			},
			{
				Verbs:     []string{rolev1.VerbAll},
				APIGroups: []string{""},
				Resources: []string{corev1.ResourceConfigMaps.String()},
			},
			{
				Verbs:     []string{rolev1.VerbAll},
				APIGroups: []string{""},
				Resources: []string{corev1.ResourcePersistentVolumeClaims.String()},
			},
		},
	}

	err = rbacService.CreateOrUpdateRole(app.Namespace, driverRole)
	if err != nil {
		return err
	}

	driverBinding := &rolev1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      roleBindingName,
			Namespace: app.Namespace,
			Labels: map[string]string{
				"control-plane": "google-spark-operator",
			},
			OwnerReferences: []metav1.OwnerReference{*getOwnerReference(app)},
		},
		Subjects: []rolev1.Subject{
			{
				Kind:      rolev1.ServiceAccountKind,
				Namespace: app.Namespace,
				Name:      GetDefaultDriverServiceAccount(app),
			},
		},
		RoleRef: rolev1.RoleRef{
			Kind:     "Role",
			APIGroup: "rbac.authorization.k8s.io",
			Name:     roleName,
		},
	}

	err = rbacService.CreateOrUpdateRoleBinding(app.Namespace, driverBinding)
	if err != nil {
		return err
	}

	return nil
}
