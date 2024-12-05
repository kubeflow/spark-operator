/*
Copyright 2024 The Kubeflow authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package delete

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/util"
)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a SparkApplication",
		Long:  "Delete a SparkApplication object with a given name",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			name := args[0]
			namespace := viper.GetString("namespace")

			k8sClient, err := util.GetK8sClient()
			if err != nil {
				return fmt.Errorf("failed to create client: %v", err)
			}

			app := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: namespace,
				},
			}
			if err := k8sClient.Delete(context.TODO(), app); err != nil {
				return fmt.Errorf("failed to delete SparkApplication %s: %v", name, err)
			}

			fmt.Printf("sparkapplication \"%s\" deleted\n", name)
			return nil
		},
	}

	return cmd
}
