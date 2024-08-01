/*
Copyright 2024 The Kubeflow authors.

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

package certificate_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/kubeflow/spark-operator/pkg/certificate"
	"github.com/kubeflow/spark-operator/pkg/common"
)

var _ = Describe("Certificate Provider", func() {
	Context("Generate new certificates", func() {
		secretName := "spark-operator-webhook-secret"
		secretNamespace := "default"

		var cp *certificate.Provider

		BeforeEach(func() {
			By("Creating a new cert provider")
			cp = certificate.NewProvider(k8sClient, secretName, secretNamespace)
			Expect(cp).NotTo(BeNil())

			By("Generating new certificates")
			Expect(cp.Generate()).To(Succeed())
		})

		It("Should generate new CA key", func() {
			caKey, err := cp.CAKey()
			Expect(err).To(BeNil())
			Expect(caKey).NotTo(BeEmpty())
		})

		It("Should generate new CA certificate", func() {
			caCert, err := cp.CACert()
			Expect(err).To(BeNil())
			Expect(caCert).NotTo(BeEmpty())
		})

		It("Should generate new server key", func() {
			serverKey, err := cp.ServerKey()
			Expect(err).To(BeNil())
			Expect(serverKey).NotTo(BeEmpty())
		})

		It("Should generate new server certificate", func() {
			serverCert, err := cp.ServerCert()
			Expect(err).To(BeNil())
			Expect(serverCert).NotTo(BeEmpty())
		})

		It("Should generate new TLS config", func() {
			cfg, err := cp.ServerCert()
			Expect(err).To(BeNil())
			Expect(cfg).NotTo(BeEmpty())
		})
	})

	Context("The data of webhook secret is empty", func() {
		ctx := context.Background()
		secretName := "spark-operator-webhook-secret"
		secretNamespace := "default"
		key := types.NamespacedName{
			Namespace: secretNamespace,
			Name:      secretName,
		}
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: secretNamespace,
			},
		}

		BeforeEach(func() {
			By("Creating a new webhook secret with empty data")
			Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		})

		AfterEach(func() {
			By("Deleting the webhook secret")
			Expect(k8sClient.Delete(ctx, secret)).To(Succeed())
		})

		It("Should generate new certificates and update webhook secret", func() {
			By("Creating a new CertProvider")
			cp := certificate.NewProvider(k8sClient, secretName, secretNamespace)
			Expect(cp.SyncSecret(context.TODO(), secretName, secretNamespace)).To(Succeed())

			By("Checking out whether the data of webhook secret is populated")
			Expect(k8sClient.Get(ctx, key, secret)).To(Succeed())
			Expect(secret.Data[common.CAKeyPem]).NotTo(BeEmpty())
			Expect(secret.Data[common.CACertPem]).NotTo(BeEmpty())
			Expect(secret.Data[common.ServerKeyPem]).NotTo(BeEmpty())
			Expect(secret.Data[common.ServerCertPem]).NotTo(BeEmpty())
		})
	})

	Context("The data of webhook secret is already populated", func() {
		ctx := context.Background()
		secretName := "spark-operator-webhook-secret"
		secretNamespace := "default"
		key := types.NamespacedName{
			Name:      secretName,
			Namespace: secretNamespace,
		}
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: secretNamespace,
			},
		}

		BeforeEach(func() {
			By("Creating a new webhook secret with data populated")
			Expect(k8sClient.Create(ctx, secret)).To(Succeed())

			By("Creating a new CertProvider and synchronize generated certificates to webhook secret")
			cp := certificate.NewProvider(k8sClient, secretName, secretNamespace)
			Expect(cp.SyncSecret(context.TODO(), secretName, secretNamespace)).To(Succeed())

			By("Creating a new webhook secret with data populated")
			Expect(k8sClient.Get(ctx, key, secret)).To(Succeed())
			Expect(secret.Data[common.CAKeyPem]).NotTo(BeEmpty())
			Expect(secret.Data[common.CACertPem]).NotTo(BeEmpty())
			Expect(secret.Data[common.ServerKeyPem]).NotTo(BeEmpty())
			Expect(secret.Data[common.ServerCertPem]).NotTo(BeEmpty())
		})

		AfterEach(func() {
			By("Deleting the webhook secret")
			Expect(k8sClient.Delete(ctx, secret)).To(Succeed())
		})

		It("Should synchronize webhook certificates data", func() {
			By("Creating a new cert provider and synchronize generated certificates to webhook secret")
			cp := certificate.NewProvider(k8sClient, secretName, secretNamespace)
			Expect(cp.SyncSecret(context.TODO(), secretName, secretNamespace)).To(Succeed())

			By("Checking out whether the webhook certificates is synchronized into the cert provider")
			caKey, err := cp.CAKey()
			Expect(err).To(BeNil())
			Expect(caKey).To(Equal(secret.Data[common.CAKeyPem]))
			caCert, err := cp.CACert()
			Expect(err).To(BeNil())
			Expect(caCert).To(Equal(secret.Data[common.CACertPem]))
			serverKey, err := cp.ServerKey()
			Expect(err).To(BeNil())
			Expect(serverKey).To(Equal(secret.Data[common.ServerKeyPem]))
			serverCert, err := cp.ServerCert()
			Expect(err).To(BeNil())
			Expect(serverCert).To(Equal(secret.Data[common.ServerCertPem]))
		})
	})
})
