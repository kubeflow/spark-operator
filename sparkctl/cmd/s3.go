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
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/go-cloud/blob/s3blob"
)

type blobS3 struct {
	s *session.Session
}

func (blob blobS3) setPublicACL(
	ctx context.Context,
	bucket string,
	filePath string) error {
	acl := "public-read"
	svc := s3.New(blob.s)

	if _, err := svc.PutObjectAcl(&s3.PutObjectAclInput{Bucket: &bucket, Key: &filePath, ACL: &acl}); err != nil {
		return fmt.Errorf("failed to set ACL on S3 object %s: %v", filePath, err)
	}

	return nil
}

func newS3Blob(
	ctx context.Context,
	bucket string,
	endpoint string,
	region string,
	forcePathStyle bool) (*uploadHandler, error) {
	// AWS SDK does require specifying regions, thus set it to default S3 region
	if region == "" {
		region = "us-east1"
	}
	c := &aws.Config{
		Region:           aws.String(region),
		Endpoint:         aws.String(endpoint),
		S3ForcePathStyle: aws.Bool(forcePathStyle),
	}
	sess := session.Must(session.NewSession(c))
	b, err := s3blob.OpenBucket(ctx, sess, bucket)
	return &uploadHandler{
		blob:             blobS3{s: sess},
		ctx:              ctx,
		b:                b,
		blobUploadBucket: bucket,
		blobEndpoint:     endpoint,
		hdpScheme:        "s3a",
	}, err
}
