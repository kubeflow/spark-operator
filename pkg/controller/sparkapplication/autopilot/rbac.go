package autopilot

import (
	"context"

	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
)

// RBAC is the service that knows how to interact with k8s to manage RBAC related resources.
type RBAC interface {
	GetClusterRole(name string) (*rbacv1.ClusterRole, error)
	GetRole(namespace, name string) (*rbacv1.Role, error)
	GetRoleBinding(namespace, name string) (*rbacv1.RoleBinding, error)
	CreateRole(namespace string, role *rbacv1.Role) error
	CreateRoleBinding(namespace string, binding *rbacv1.RoleBinding) error
	UpdateRole(namespace string, role *rbacv1.Role) error
	UpdateRoleBinding(namespace string, binding *rbacv1.RoleBinding) error
	DeleteRole(namespace, name string) error
	DeleteRoleBinding(namespace, name string) error
	CreateOrUpdateRole(namespace string, binding *rbacv1.Role) error
	CreateOrUpdateRoleBinding(namespace string, binding *rbacv1.RoleBinding) error
}

// RBACService is the RBAC service implementation using API calls to kubernetes.
type RBACService struct {
	kubeClient clientset.Interface
}

// NewRBACService returns a new RBAC KubeService.
func NewRBACService(kubeClient clientset.Interface) *RBACService {
	return &RBACService{
		kubeClient: kubeClient,
	}
}

func (r *RBACService) GetClusterRole(name string) (*rbacv1.ClusterRole, error) {
	return r.kubeClient.RbacV1().ClusterRoles().Get(context.TODO(), name, metav1.GetOptions{})
}

func (r *RBACService) GetRole(namespace, name string) (*rbacv1.Role, error) {
	return r.kubeClient.RbacV1().Roles(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

func (r *RBACService) GetRoleBinding(namespace, name string) (*rbacv1.RoleBinding, error) {
	return r.kubeClient.RbacV1().RoleBindings(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

func (r *RBACService) DeleteRole(namespace, name string) error {
	err := r.kubeClient.RbacV1().Roles(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (r *RBACService) CreateRole(namespace string, role *rbacv1.Role) error {
	_, err := r.kubeClient.RbacV1().Roles(namespace).Create(context.TODO(), role, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (s *RBACService) UpdateRole(namespace string, role *rbacv1.Role) error {
	_, err := s.kubeClient.RbacV1().Roles(namespace).Update(context.TODO(), role, metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	return err
}

func (r *RBACService) CreateOrUpdateRole(namespace string, role *rbacv1.Role) error {
	storedRole, err := r.GetRole(namespace, role.Name)
	if err != nil {
		// If no resource we need to create.
		if apierrors.IsNotFound(err) {
			return r.CreateRole(namespace, role)
		}
		return err
	}

	// Already exists, need to Update.
	// Set the correct resource version to ensure we are on the latest version. This way the only valid
	// namespace is our spec(https://github.com/kubernetes/community/blob/master/contributors/devel/api-conventions.md#concurrency-control-and-consistency),
	// we will replace the current namespace state.
	role.ResourceVersion = storedRole.ResourceVersion
	return r.UpdateRole(namespace, role)
}

func (r *RBACService) DeleteRoleBinding(namespace, name string) error {
	err := r.kubeClient.RbacV1().RoleBindings(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (r *RBACService) CreateRoleBinding(namespace string, binding *rbacv1.RoleBinding) error {
	_, err := r.kubeClient.RbacV1().RoleBindings(namespace).Create(context.TODO(), binding, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (r *RBACService) UpdateRoleBinding(namespace string, binding *rbacv1.RoleBinding) error {
	_, err := r.kubeClient.RbacV1().RoleBindings(namespace).Update(context.TODO(), binding, metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (r *RBACService) CreateOrUpdateRoleBinding(namespace string, binding *rbacv1.RoleBinding) error {
	storedBinding, err := r.GetRoleBinding(namespace, binding.Name)
	if err != nil {
		// If no resource we need to create.
		if apierrors.IsNotFound(err) {
			return r.CreateRoleBinding(namespace, binding)
		}
		return err
	}

	// Check if the role ref has changed, roleref updates are not allowed, if changed then delete and create again the role binding.
	// https://github.com/kubernetes/kubernetes/blob/0f0a5223dfc75337d03c9b80ae552ae8ef138eeb/pkg/apis/rbac/validation/validation.go#L157-L159
	if storedBinding.RoleRef != binding.RoleRef {
		if err := r.DeleteRoleBinding(namespace, binding.Name); err != nil {
			return err
		}
		return r.CreateRoleBinding(namespace, binding)
	}

	// Already exists, need to Update.
	// Set the correct resource version to ensure we are on the latest version. This way the only valid
	// namespace is our spec(https://github.com/kubernetes/community/blob/master/contributors/devel/api-conventions.md#concurrency-control-and-consistency),
	// we will replace the current namespace state.
	binding.ResourceVersion = storedBinding.ResourceVersion
	return r.UpdateRoleBinding(namespace, binding)
}
