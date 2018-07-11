/*
Copyright 2017 Google LLC

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

package cmd

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	clientset "k8s.io/client-go/kubernetes"

	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	crdclientset "k8s.io/spark-on-k8s-operator/pkg/client/clientset/versioned"
	"unicode/utf8"
)

const bufferSize = 1024

var UploadTo string
var Project string
var Public bool
var Override bool

var createCmd = &cobra.Command{
	Use:   "create <yaml file>",
	Short: "Create a SparkApplication object",
	Long:  `Create a SparkApplication from a given YAML file storing the application specification.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			fmt.Fprintln(os.Stderr, "must specify a YAML file of a SparkApplication")
			return
		}

		kubeClientset, err := getKubeClient()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get Kubernetes client: %v\n", err)
			return
		}

		crdClientset, err := getSparkApplicationClient()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get SparkApplication client: %v\n", err)
			return
		}

		if err := doCreate(args[0], kubeClientset, crdClientset); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
		}
	},
}

func init() {
	createCmd.Flags().StringVarP(&UploadTo, "upload-to", "u", "",
		"a URL of the remote location where local application dependencies are to be uploaded to")
	createCmd.Flags().StringVarP(&Project, "project", "p", "",
		"the GCP project to which the GCS bucket for hosting uploaded dependencies is associated")
	createCmd.Flags().BoolVarP(&Public, "public", "c", false,
		"whether to make uploaded files publicly available")
	createCmd.Flags().BoolVarP(&Override, "override", "o", false,
		"whether to override remote files with the same names")
}

func doCreate(yamlFile string, kubeClientset clientset.Interface, crdClientset crdclientset.Interface) error {
	app, err := loadFromYAML(yamlFile)
	if err != nil {
		return err
	}

	v1alpha1.SetSparkApplicationDefaults(app)
	if err = validateSpec(app.Spec); err != nil {
		return fmt.Errorf("validation failed for SparkApplication %s: %v", app.Name, err)
	}

	if err = handleLocalDependencies(app); err != nil {
		return err
	}

	if hadoopConfDir := os.Getenv("HADOOP_CONF_DIR"); hadoopConfDir != "" {
		fmt.Println("creating a ConfigMap for Hadoop configuration files in HADOOP_CONF_DIR")
		if err = handleHadoopConfiguration(app, hadoopConfDir, kubeClientset); err != nil {
			return err
		}
	}

	if _, err = crdClientset.SparkoperatorV1alpha1().SparkApplications(Namespace).Create(app); err != nil {
		return fmt.Errorf("failed to create SparkApplication %s: %v", app.Name, err)
	}

	fmt.Printf("SparkApplication \"%s\" created\n", app.Name)

	return nil
}

func loadFromYAML(yamlFile string) (*v1alpha1.SparkApplication, error) {
	file, err := os.Open(yamlFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder := yaml.NewYAMLOrJSONDecoder(file, bufferSize)
	app := &v1alpha1.SparkApplication{}
	err = decoder.Decode(app)
	if err != nil {
		return nil, err
	}

	return app, nil
}

func validateSpec(spec v1alpha1.SparkApplicationSpec) error {
	if spec.Image == nil && (spec.Driver.Image == nil || spec.Executor.Image == nil) {
		return fmt.Errorf("'spec.driver.image' and 'spec.executor.image' cannot be empty when 'spec.image' " +
			"is not set")
	}

	yes, err := hasNonContainerLocalFiles(spec)
	if err != nil {
		return err
	}
	if spec.Image == nil && spec.InitContainerImage == nil && yes {
		return fmt.Errorf("'spec.image' and 'spec.initContainerImage' cannot be both empty when " +
			"non-container-local dependencies are used")
	}

	return nil
}

func handleLocalDependencies(app *v1alpha1.SparkApplication) error {
	if app.Spec.MainApplicationFile != nil {
		isMainAppFileLocal, err := isLocalFile(*app.Spec.MainApplicationFile)
		if err != nil {
			return err
		}

		if isMainAppFileLocal {
			uploadedMainFile, err := uploadLocalDependencies(app, []string{*app.Spec.MainApplicationFile})
			if err != nil {
				return fmt.Errorf("failed to upload local main application file: %v", err)
			}
			app.Spec.MainApplicationFile = &uploadedMainFile[0]
		}
	}

	localJars, err := filterLocalFiles(app.Spec.Deps.Jars)
	if err != nil {
		return fmt.Errorf("failed to filter local jars: %v", err)
	}

	if len(localJars) > 0 {
		uploadedJars, err := uploadLocalDependencies(app, localJars)
		if err != nil {
			return fmt.Errorf("failed to upload local jars: %v", err)
		}
		app.Spec.Deps.Jars = uploadedJars
	}

	localFiles, err := filterLocalFiles(app.Spec.Deps.Files)
	if err != nil {
		return fmt.Errorf("failed to filter local files: %v", err)
	}

	if len(localFiles) > 0 {
		uploadedFiles, err := uploadLocalDependencies(app, localFiles)
		if err != nil {
			return fmt.Errorf("failed to upload local files: %v", err)
		}
		app.Spec.Deps.Files = uploadedFiles
	}

	return nil
}

func filterLocalFiles(files []string) ([]string, error) {
	var localFiles []string
	for _, file := range files {
		if isLocal, err := isLocalFile(file); err != nil {
			return nil, err
		} else if isLocal {
			localFiles = append(localFiles, file)
		}
	}

	return localFiles, nil
}

func isLocalFile(file string) (bool, error) {
	fileUrl, err := url.Parse(file)
	if err != nil {
		return false, err
	}

	if fileUrl.Scheme == "file" || fileUrl.Scheme == "" {
		return true, nil
	}

	return false, nil
}

func hasNonContainerLocalFiles(spec v1alpha1.SparkApplicationSpec) (bool, error) {
	var files []string
	if spec.MainApplicationFile != nil {
		files = append(files, *spec.MainApplicationFile)
	}

	files = append(files, spec.Deps.Jars...)
	files = append(files, spec.Deps.Files...)

	for _, file := range files {
		containerLocal, err := isContainerLocalFile(file)
		if err != nil {
			return containerLocal, err
		}
		if !containerLocal {
			return true, nil
		}
	}

	return false, nil
}

func isContainerLocalFile(file string) (bool, error) {
	fileUrl, err := url.Parse(file)
	if err != nil {
		return false, err
	}

	if fileUrl.Scheme == "local" {
		return true, nil
	}

	return false, nil
}

func uploadLocalDependencies(app *v1alpha1.SparkApplication, files []string) ([]string, error) {
	if UploadTo == "" {
		return nil, fmt.Errorf(
			"unable to upload local dependencies: no upload location specified via --upload-to")
	}

	uploadLocationUrl, err := url.Parse(UploadTo)
	if err != nil {
		return nil, err
	}

	switch uploadLocationUrl.Scheme {
	case "gs":
		if Project == "" {
			return nil, fmt.Errorf("--project must be specified to upload dependencies to GCS")
		}
		return uploadToGCS(uploadLocationUrl.Host, app.Namespace, app.Name, Project, files, Public, Override)
	case "s3":
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported upload location URL scheme: %s", uploadLocationUrl.Scheme)
	}
}

func handleHadoopConfiguration(
	app *v1alpha1.SparkApplication,
	hadoopConfDir string,
	kubeClientset clientset.Interface) error {
	configMap, err := buildHadoopConfigMap(app.Name, hadoopConfDir)
	if err != nil {
		return fmt.Errorf("failed to create a ConfigMap for Hadoop configuration files in %s: %v",
			hadoopConfDir, err)
	}

	err = kubeClientset.CoreV1().ConfigMaps(Namespace).Delete(configMap.Name, &metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete existing ConfigMap %s: %v", configMap.Name, err)
	}

	if configMap, err = kubeClientset.CoreV1().ConfigMaps(Namespace).Create(configMap); err != nil {
		return fmt.Errorf("failed to create ConfigMap %s: %v", configMap.Name, err)
	}

	app.Spec.HadoopConfigMap = &configMap.Name

	return nil
}

func buildHadoopConfigMap(appName string, hadoopConfDir string) (*apiv1.ConfigMap, error) {
	info, err := os.Stat(hadoopConfDir)
	if err != nil {
		return nil, err
	}

	if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", hadoopConfDir)
	}

	files, err := ioutil.ReadDir(hadoopConfDir)
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no Hadoop configuration file found in %s", hadoopConfDir)
	}

	hadoopStringConfigFiles := make(map[string]string)
	hadoopBinaryConfigFiles := make(map[string][]byte)
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		content, err := ioutil.ReadFile(filepath.Join(hadoopConfDir, file.Name()))
		if err != nil {
			return nil, err
		}

		if utf8.Valid(content) {
			hadoopStringConfigFiles[file.Name()] = string(content)
		} else {
			hadoopBinaryConfigFiles[file.Name()] = content
		}
	}

	configMap := &apiv1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      appName + "-hadoop-config",
			Namespace: Namespace,
		},
		Data:       hadoopStringConfigFiles,
		BinaryData: hadoopBinaryConfigFiles,
	}

	return configMap, nil
}
