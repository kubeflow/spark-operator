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

package create

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"unicode/utf8"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/common"
	"github.com/kubeflow/spark-operator/pkg/util"
)

const (
	bufferSize = 1024
)

var (
	k8sClient client.Client
	clientset kubernetes.Interface

	Namespace        string
	DeleteIfExists   bool
	RootPath         string
	UploadToPath     string
	UploadToEndpoint string
	UploadToRegion   string
	Public           bool
	S3ForcePathStyle bool
	Override         bool
	From             string
)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a SparkApplication from file or ScheduledSparkApplication",
		RunE: func(cmd *cobra.Command, args []string) error {
			if From != "" && len(args) != 1 {
				return fmt.Errorf("must specify the name of a ScheduledSparkApplication")
			}

			if len(args) != 1 {
				return fmt.Errorf("must specify a YAML file of a SparkApplication")
			}

			Namespace = viper.GetString("namespace")

			var err error
			k8sClient, err = util.GetK8sClient()
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %v", err)
			}

			clientset, err = util.GetClientset()
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes clientset: %v", err)
			}

			if From != "" {
				if err := createFromScheduledSparkApplication(args[0]); err != nil {
					return fmt.Errorf("failed to create SparkApplication %q from ScheduledSparkApplication %q: %v", args[0], From, err)
				}
				return nil
			}

			app, err := util.LoadSparkApplicationFromFile(args[0])
			if err != nil {
				return fmt.Errorf("failed to read SparkApplication from file %s: %v", args[0], err)
			}

			if err := createSparkApplication(app); err != nil {
				return fmt.Errorf("failed to create SparkApplication %s: %v", app.Name, err)
			}

			return nil
		},
	}

	cmd.Flags().BoolVarP(&DeleteIfExists, "delete", "d", false, "delete the SparkApplication if already exists")
	cmd.Flags().StringVarP(&UploadToPath, "upload-to", "u", "", "the name of the bucket where local application dependencies are to be uploaded")
	cmd.Flags().StringVarP(&RootPath, "upload-prefix", "p", "", "the prefix to use for the dependency uploads")
	cmd.Flags().StringVarP(&UploadToRegion, "upload-to-region", "r", "", "the GCS or S3 storage region for the bucket")
	cmd.Flags().StringVarP(&UploadToEndpoint, "upload-to-endpoint", "e", "https://storage.googleapis.com", "the GCS or S3 storage api endpoint url")
	cmd.Flags().BoolVarP(&Public, "public", "c", false, "whether to make uploaded files publicly available")
	cmd.Flags().BoolVar(&S3ForcePathStyle, "s3-force-path-style", false, "whether to force path style URLs for S3 objects")
	cmd.Flags().BoolVarP(&Override, "override", "o", false, "whether to override remote files with the same names")
	cmd.Flags().StringVarP(&From, "from", "f", "", "the name of ScheduledSparkApplication from which a forced SparkApplication run is created")

	return cmd
}

func createFromScheduledSparkApplication(name string) error {
	key := client.ObjectKey{
		Namespace: Namespace,
		Name:      From,
	}
	scheduledApp := &v1beta2.ScheduledSparkApplication{}
	if err := k8sClient.Get(context.TODO(), key, scheduledApp); err != nil {
		return fmt.Errorf("failed to get ScheduledSparkApplication %s: %v", From, err)
	}

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: Namespace,
			Name:      name,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: v1beta2.SchemeGroupVersion.String(),
					Kind:       reflect.TypeOf(v1beta2.ScheduledSparkApplication{}).Name(),
					Name:       scheduledApp.Name,
					UID:        scheduledApp.UID,
				},
			},
		},
		Spec: *scheduledApp.Spec.Template.DeepCopy(),
	}

	if err := createSparkApplication(app); err != nil {
		return fmt.Errorf("failed to create SparkApplication %s: %v", app.Name, err)
	}

	return nil
}

func createSparkApplication(app *v1beta2.SparkApplication) error {
	if DeleteIfExists {
		if err := k8sClient.Delete(context.TODO(), app); err != nil && !errors.IsNotFound(err) {
			return fmt.Errorf("failed to delete SparkApplication %s: %v", app.Name, err)
		}
	}

	v1beta2.SetSparkApplicationDefaults(app)
	if err := validateSpec(app.Spec); err != nil {
		return err
	}

	if err := handleLocalDependencies(app); err != nil {
		return err
	}

	if hadoopConfDir := os.Getenv(common.EnvHadoopConfDir); hadoopConfDir != "" {
		fmt.Printf("Creating a ConfigMap for Hadoop configuration files in %s\n", hadoopConfDir)
		if err := handleHadoopConf(app, hadoopConfDir); err != nil {
			return err
		}
	}

	if err := k8sClient.Create(context.TODO(), app); err != nil {
		return err
	}
	fmt.Printf("sparkapplication %q created\n", app.Name)

	return nil
}

func validateSpec(spec v1beta2.SparkApplicationSpec) error {
	if spec.Image == nil && (spec.Driver.Image == nil || spec.Executor.Image == nil) {
		return fmt.Errorf("'spec.driver.image' and 'spec.executor.image' cannot be empty when 'spec.image' " +
			"is not set")
	}

	return nil
}

func handleLocalDependencies(app *v1beta2.SparkApplication) error {
	// Upload the main application file to the cloud if it is a local file.
	mainAppFile := app.Spec.MainApplicationFile
	if mainAppFile != nil {
		isMainAppFileLocal, err := util.IsLocalFile(*mainAppFile)
		if err != nil {
			return err
		}

		if isMainAppFileLocal {
			uploadedMainFile, err := uploadLocalDependencies(app, []string{*mainAppFile})
			if err != nil {
				return fmt.Errorf("failed to upload local main application file: %v", err)
			}
			mainAppFile = &uploadedMainFile[0]
		}
	}

	// Filter out local jars and upload them to the cloud.
	localJars, err := util.FilterLocalFiles(app.Spec.Deps.Jars)
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

	// Filter out local files and upload them to the cloud.
	localFiles, err := util.FilterLocalFiles(app.Spec.Deps.Files)
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

	// Filter out local python files and upload them to the cloud.
	localPyFiles, err := util.FilterLocalFiles(app.Spec.Deps.PyFiles)
	if err != nil {
		return fmt.Errorf("failed to filter local pyfiles: %v", err)
	}

	if len(localPyFiles) > 0 {
		uploadedPyFiles, err := uploadLocalDependencies(app, localPyFiles)
		if err != nil {
			return fmt.Errorf("failed to upload local pyfiles: %v", err)
		}
		app.Spec.Deps.PyFiles = uploadedPyFiles
	}

	return nil
}

func handleHadoopConf(app *v1beta2.SparkApplication, hadoopConfDir string) error {
	configMap, err := buildHadoopConfigMap(app.Name, app.Namespace, hadoopConfDir)
	if err != nil {
		return fmt.Errorf("failed to build ConfigMap from files in %s: %v", hadoopConfDir, err)
	}

	if err := k8sClient.Delete(context.TODO(), configMap); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete existing ConfigMap %s: %v", configMap.Name, err)
	}

	if err := k8sClient.Create(context.TODO(), configMap); err != nil {
		return fmt.Errorf("failed to create ConfigMap %s: %v", configMap.Name, err)
	}

	app.Spec.HadoopConfigMap = &configMap.Name
	return nil
}

func buildHadoopConfigMap(appName string, appNamespace string, hadoopConfDir string) (*corev1.ConfigMap, error) {
	info, err := os.Stat(hadoopConfDir)
	if err != nil {
		return nil, err
	}

	if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", hadoopConfDir)
	}

	files, err := os.ReadDir(hadoopConfDir)
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no Hadoop configuration file found in %s", hadoopConfDir)
	}

	data := make(map[string]string)
	binaryData := make(map[string][]byte)
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filename := file.Name()
		bytes, err := os.ReadFile(filepath.Join(hadoopConfDir, filename))
		if err != nil {
			return nil, err
		}

		if utf8.Valid(bytes) {
			data[filename] = string(bytes)
		} else {
			binaryData[filename] = bytes
		}
	}

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-hadoop-conf", appName),
			Namespace: appNamespace,
		},
		Data:       data,
		BinaryData: binaryData,
	}

	return configMap, nil
}

type blobHandler interface {
	// TODO: With go-cloud supporting setting ACLs, remove implementations of interface
	setPublicACL(ctx context.Context, bucket string, filePath string) error
}

type uploadHandler struct {
	blob             blobHandler
	blobUploadBucket string
	blobEndpoint     string
	hdpScheme        string
	ctx              context.Context
	b                *blob.Bucket
}

func (uh uploadHandler) uploadToBucket(uploadPath, localFilePath string) (string, error) {
	fileName := filepath.Base(localFilePath)
	uploadFilePath := filepath.Join(uploadPath, fileName)

	// Check if exists by trying to fetch metadata
	reader, err := uh.b.NewRangeReader(uh.ctx, uploadFilePath, 0, 0, nil)
	if err == nil {
		reader.Close()
	}
	if (gcerrors.Code(err) == gcerrors.NotFound) || (err == nil && Override) {
		fmt.Printf("uploading local file: %s\n", fileName)

		// Prepare the file for upload.
		data, err := os.ReadFile(localFilePath)
		if err != nil {
			return "", fmt.Errorf("failed to read file: %s", err)
		}

		// Open Bucket
		w, err := uh.b.NewWriter(uh.ctx, uploadFilePath, nil)
		if err != nil {
			return "", fmt.Errorf("failed to obtain bucket writer: %s", err)
		}

		// Write data to bucket and close bucket writer
		_, writeErr := w.Write(data)
		if err := w.Close(); err != nil {
			return "", fmt.Errorf("failed to close bucket writer: %s", err)
		}

		// Check if write has been successful
		if writeErr != nil {
			return "", fmt.Errorf("failed to write to bucket: %s", err)
		}

		// Set public ACL if needed
		if Public {
			err := uh.blob.setPublicACL(uh.ctx, uh.blobUploadBucket, uploadFilePath)
			if err != nil {
				return "", err
			}

			endpointURL, err := url.Parse(uh.blobEndpoint)
			if err != nil {
				return "", err
			}
			// Public needs full bucket endpoint
			return fmt.Sprintf("%s://%s/%s/%s",
				endpointURL.Scheme,
				endpointURL.Host,
				uh.blobUploadBucket,
				uploadFilePath), nil
		}
	} else if err == nil {
		fmt.Printf("not uploading file %s as it already exists remotely\n", fileName)
	} else {
		return "", err
	}
	// Return path to file with proper hadoop-connector scheme
	return fmt.Sprintf("%s://%s/%s", uh.hdpScheme, uh.blobUploadBucket, uploadFilePath), nil
}

func uploadLocalDependencies(app *v1beta2.SparkApplication, files []string) ([]string, error) {
	if UploadToPath == "" {
		return nil, fmt.Errorf(
			"unable to upload local dependencies: no upload location specified via --upload-to")
	}

	uploadLocationURL, err := url.Parse(UploadToPath)
	if err != nil {
		return nil, err
	}
	uploadBucket := uploadLocationURL.Host

	var uh *uploadHandler
	ctx := context.Background()
	switch uploadLocationURL.Scheme {
	case "gs":
		uh, err = newGCSBlob(ctx, uploadBucket, UploadToEndpoint, UploadToRegion)
	case "s3":
		uh, err = newS3Blob(ctx, uploadBucket, UploadToEndpoint, UploadToRegion, S3ForcePathStyle)
	default:
		return nil, fmt.Errorf("unsupported upload location URL scheme: %s", uploadLocationURL.Scheme)
	}

	// Check if bucket has been successfully setup
	if err != nil {
		return nil, err
	}

	var uploadedFilePaths []string
	uploadPath := filepath.Join(RootPath, app.Namespace, app.Name)
	for _, localFilePath := range files {
		uploadFilePath, err := uh.uploadToBucket(uploadPath, localFilePath)
		if err != nil {
			return nil, err
		}

		uploadedFilePaths = append(uploadedFilePaths, uploadFilePath)
	}

	return uploadedFilePaths, nil
}
