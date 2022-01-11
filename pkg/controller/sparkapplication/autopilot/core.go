package autopilot

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
)

type SA interface {
	GetServiceAccount(name string) (*corev1.ServiceAccount, error)
	CreateServiceAccount(namespace string, sa *corev1.ServiceAccount) error
	UpdateServiceAccount(namespace string, sa *corev1.ServiceAccount) error
	DeleteServiceAccount(namespace, name string) error
	CreateOrUpdateServiceAccount(namespace string, sa *corev1.ServiceAccount) error
}

type SAService struct {
	kubeClient clientset.Interface
}

func NewSAService(kubeClient clientset.Interface) *SAService {
	return &SAService{
		kubeClient: kubeClient,
	}
}

func (r *SAService) GetServiceAccount(namespace, name string) (*corev1.ServiceAccount, error) {
	return r.kubeClient.CoreV1().ServiceAccounts(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

func (r *SAService) CreateServiceAccount(namespace string, sa *corev1.ServiceAccount) error {
	_, err := r.kubeClient.CoreV1().ServiceAccounts(namespace).Create(context.TODO(), sa, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (s *SAService) UpdateServiceAccount(namespace string, sa *corev1.ServiceAccount) error {
	_, err := s.kubeClient.CoreV1().ServiceAccounts(namespace).Update(context.TODO(), sa, metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	return err
}

func (r *SAService) CreateOrUpdateServiceAccount(namespace string, sa *corev1.ServiceAccount) error {
	storedSA, err := r.GetServiceAccount(namespace, sa.Name)
	if err != nil {
		// If no resource we need to create.
		if apierrors.IsNotFound(err) {
			return r.CreateServiceAccount(namespace, sa)
		}
		return err
	}

	// Already exists, need to Update.
	// Set the correct resource version to ensure we are on the latest version. This way the only valid
	// namespace is our spec(https://github.com/kubernetes/community/blob/master/contributors/devel/api-conventions.md#concurrency-control-and-consistency),
	// we will replace the current namespace state.
	sa.ResourceVersion = storedSA.ResourceVersion
	return r.UpdateServiceAccount(namespace, sa)
}

func (r *SAService) DeleteServiceAccount(namespace, name string) error {
	err := r.kubeClient.CoreV1().ServiceAccounts(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
	if err != nil {
		return err
	}
	return nil
}
