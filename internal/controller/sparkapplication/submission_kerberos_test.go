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

package sparkapplication

import (
	"reflect"
	"testing"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

func TestKerberosConfOption(t *testing.T) {
	tests := []struct {
		name     string
		app      *v1beta2.SparkApplication
		expected []string
	}{
		{
			name: "no kerberos configuration",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{},
			},
			expected: nil,
		},
		{
			name: "basic kerberos configuration with keytab",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Kerberos: &v1beta2.KerberosSpec{
						Principal:    stringPtr("spark@EXAMPLE.COM"),
						KeytabSecret: stringPtr("spark-keytab"),
						ConfigSecret: stringPtr("spark-config"),
					},
				},
			},
			expected: []string{
				"--conf", "spark.hadoop.hadoop.security.authentication=kerberos",
				"--conf", "spark.hadoop.hadoop.security.authorization=true",
				"--conf", "spark.kerberos.principal=spark@EXAMPLE.COM",
				"--conf", "spark.kerberos.renewal.credentials=keytab",
				"--conf", "spark.security.credentials.hadoopfs.enabled=true",
				"--conf", "spark.security.credentials.hbase.enabled=true",
				"--conf", "spark.security.credentials.hive.enabled=true",
				"--conf", "spark.kerberos.keytab=" + common.DefaultKerberosKeytabMountPath + "/" + common.KerberosKeytabFileName,
				"--conf", "spark.hadoop.hadoop.kerberos.principal=spark@EXAMPLE.COM",
				"--conf", "spark.hadoop.hadoop.kerberos.keytab=" + common.DefaultKerberosKeytabMountPath + "/" + common.KerberosKeytabFileName,
				"--conf", "spark.hadoop.java.security.krb5.conf=" + common.DefaultKerberosConfigMountPath + "/" + common.KerberosConfigFileName,
				"--conf", "spark.driver.extraJavaOptions=-Djava.security.krb5.conf=" + common.DefaultKerberosConfigMountPath + "/" + common.KerberosConfigFileName,
				"--conf", "spark.executor.extraJavaOptions=-Djava.security.krb5.conf=" + common.DefaultKerberosConfigMountPath + "/" + common.KerberosConfigFileName,
			},
		},
		{
			name: "kerberos with custom renewal credentials and enabled services",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Kerberos: &v1beta2.KerberosSpec{
						Principal:          stringPtr("spark@EXAMPLE.COM"),
						KeytabSecret:       stringPtr("spark-keytab"),
						ConfigSecret:       stringPtr("spark-config"),
						RenewalCredentials: stringPtr("ccache"),
						EnabledServices:    []string{"hadoopfs", "yarn"},
					},
				},
			},
			expected: []string{
				"--conf", "spark.hadoop.hadoop.security.authentication=kerberos",
				"--conf", "spark.hadoop.hadoop.security.authorization=true",
				"--conf", "spark.kerberos.principal=spark@EXAMPLE.COM",
				"--conf", "spark.kerberos.renewal.credentials=ccache",
				"--conf", "spark.security.credentials.hadoopfs.enabled=true",
				"--conf", "spark.security.credentials.yarn.enabled=true",
				"--conf", "spark.kerberos.keytab=" + common.DefaultKerberosKeytabMountPath + "/" + common.KerberosKeytabFileName,
				"--conf", "spark.hadoop.hadoop.kerberos.principal=spark@EXAMPLE.COM",
				"--conf", "spark.hadoop.hadoop.kerberos.keytab=" + common.DefaultKerberosKeytabMountPath + "/" + common.KerberosKeytabFileName,
				"--conf", "spark.hadoop.java.security.krb5.conf=" + common.DefaultKerberosConfigMountPath + "/" + common.KerberosConfigFileName,
				"--conf", "spark.driver.extraJavaOptions=-Djava.security.krb5.conf=" + common.DefaultKerberosConfigMountPath + "/" + common.KerberosConfigFileName,
				"--conf", "spark.executor.extraJavaOptions=-Djava.security.krb5.conf=" + common.DefaultKerberosConfigMountPath + "/" + common.KerberosConfigFileName,
			},
		},
		{
			name: "kerberos with hadoop configuration enables HDFS access",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Kerberos: &v1beta2.KerberosSpec{
						Principal:    stringPtr("spark@EXAMPLE.COM"),
						KeytabSecret: stringPtr("spark-keytab"),
						ConfigSecret: stringPtr("spark-config"),
					},
					HadoopConf: map[string]string{
						"hadoop.security.authentication": "kerberos",
					},
				},
			},
			expected: []string{
				"--conf", "spark.hadoop.hadoop.security.authentication=kerberos",
				"--conf", "spark.hadoop.hadoop.security.authorization=true",
				"--conf", "spark.kerberos.principal=spark@EXAMPLE.COM",
				"--conf", "spark.kerberos.renewal.credentials=keytab",
				"--conf", "spark.security.credentials.hadoopfs.enabled=true",
				"--conf", "spark.security.credentials.hbase.enabled=true",
				"--conf", "spark.security.credentials.hive.enabled=true",
				"--conf", "spark.kerberos.keytab=" + common.DefaultKerberosKeytabMountPath + "/" + common.KerberosKeytabFileName,
				"--conf", "spark.hadoop.hadoop.kerberos.principal=spark@EXAMPLE.COM",
				"--conf", "spark.hadoop.hadoop.kerberos.keytab=" + common.DefaultKerberosKeytabMountPath + "/" + common.KerberosKeytabFileName,
				"--conf", "spark.hadoop.java.security.krb5.conf=" + common.DefaultKerberosConfigMountPath + "/" + common.KerberosConfigFileName,
				"--conf", "spark.driver.extraJavaOptions=-Djava.security.krb5.conf=" + common.DefaultKerberosConfigMountPath + "/" + common.KerberosConfigFileName,
				"--conf", "spark.executor.extraJavaOptions=-Djava.security.krb5.conf=" + common.DefaultKerberosConfigMountPath + "/" + common.KerberosConfigFileName,
				"--conf", "spark.kerberos.access.hadoopFileSystems=hdfs",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := kerberosConfOption(tt.app)
			if err != nil {
				t.Errorf("kerberosConfOption() error = %v", err)
				return
			}
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("kerberosConfOption() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

// Helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}
