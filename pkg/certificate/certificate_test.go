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
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"time"

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
		var secret *corev1.Secret

		BeforeEach(func() {
			secret = &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      secretName,
					Namespace: secretNamespace,
				},
			}

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

		It("Should generate a new certificate when validation fails", func() {
			By("Creating a certificate with an expired NotAfter field")
			Expect(k8sClient.Get(ctx, key, secret)).To(Succeed())
			caKeyPem, _ := pem.Decode(secret.Data[common.CAKeyPem])
			caCertPem, _ := pem.Decode(secret.Data[common.CACertPem])
			serverKeyPem, _ := pem.Decode(secret.Data[common.ServerKeyPem])
			serverCertPem, _ := pem.Decode(secret.Data[common.ServerCertPem])
			caKey, _ := x509.ParsePKCS1PrivateKey(caKeyPem.Bytes)
			caCert, _ := x509.ParseCertificate(caCertPem.Bytes)
			serverKey, _ := x509.ParsePKCS1PrivateKey(serverKeyPem.Bytes)
			serverCert, _ := x509.ParseCertificate(serverCertPem.Bytes)

			serverCert.NotAfter = time.Now().AddDate(0, 0, -1)
			certBytes, err := x509.CreateCertificate(rand.Reader, serverCert, caCert, serverKey.Public(), caKey)
			Expect(err).To(BeNil())
			expiredPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certBytes})

			secret.Data[common.ServerCertPem] = expiredPEM
			Expect(k8sClient.Update(ctx, secret)).To(Succeed())

			By("Creating a new cert provider and synchronize should generate new certificates")
			cp := certificate.NewProvider(k8sClient, secretName, secretNamespace)
			Expect(cp.SyncSecret(context.TODO(), secretName, secretNamespace)).To(Succeed())

			By("Checking out whether the webhook certificate changed after syncronize")
			serverPEM, err := cp.ServerCert()
			Expect(err).To(BeNil())
			Expect(serverPEM).ShouldNot(Equal(expiredPEM))
		})
	})
})
