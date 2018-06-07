/*
Copyright 2018 Google LLC

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
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"net/url"
)

type s3Uploader struct {
	client *s3manager.Uploader
	endpointURL string
	region string
	bucket string
	path   string
}

func (s *s3Uploader) upload(localFile string, public bool, override bool) (string, error) {
	file, err := os.Open(localFile)
	if err != nil {
		return "", fmt.Errorf("failed to open file %s: %v", localFile, err)
	}
	defer file.Close()

	bucket := s.bucket
	key := filepath.Join(s.path, filepath.Base(localFile))
	_, err = s.client.S3.HeadObject(&s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})

	if err == nil && !override {
		fmt.Printf("not uploading file %s as it already exists remotely\n", filepath.Base(localFile))
		return s.getRemoteFileUrl(key, public), nil
	}

	acl := "private"
	if public {
		acl = "public-read"
	}

	fmt.Printf("uploading local file: %s\n", localFile)
	_, err = s.client.Upload(&s3manager.UploadInput{
		Bucket: &bucket,
		Key: &key,
		Body: file,
		ACL: &acl,
	})

	if err != nil {
		return "", fmt.Errorf("failed to copy file %s to S3: %v", localFile, err)
	}

	return s.getRemoteFileUrl(key, public), nil
}

func (s *s3Uploader) getRemoteFileUrl(name string, public bool) string {
	if public {
		uploadLocationUrl, err := url.Parse(s.endpointURL)
		if err != nil {
			return ""
		}

		return fmt.Sprintf("%s://%s.%s/%s",
			uploadLocationUrl.Scheme,
			s.bucket,
			uploadLocationUrl.Host,
			name)
	}
	return fmt.Sprintf("s3a://%s/%s", s.bucket, name)
}

func newS3Uploader(endpointURL string, region string, bucket string, path string) (*s3Uploader, error) {
	// AWS SDK does require specifying regions, thus set it to default GCS region
	if region == "" {
		region = "us-east1"
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
		Endpoint: aws.String(endpointURL),
	})

	if err != nil {
		return nil, err
	}

	uploader := &s3Uploader{
		client: s3manager.NewUploader(sess),
		endpointURL: endpointURL,
		region: region,
		bucket: bucket,
		path:   path,
	}
	return uploader, nil
}

func uploadToS3(
	bucket string,
	appNamespace string,
	appName string,
	endpointURL string,
	region string,
	files []string,
	public bool,
	override bool) ([]string, error) {
	uploader, err := newS3Uploader(endpointURL, region, bucket, filepath.Join(rootPath, appNamespace, appName))
	if err != nil {
		return nil, err
	}

	var uploadedFiles []string
	for _, file := range files {
		remoteFile, err := uploader.upload(file, public, override)
		if err != nil {
			return nil, err
		}
		uploadedFiles = append(uploadedFiles, remoteFile)
	}

	return uploadedFiles, nil
}