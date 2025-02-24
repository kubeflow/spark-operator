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

package app

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"gocloud.dev/blob/s3blob"
)

type blobS3 struct {
	client *s3.Client
}

func (blob blobS3) setPublicACL(
	ctx context.Context,
	bucket string,
	filePath string) error {
	acl := types.ObjectCannedACLPublicRead
	if _, err := blob.client.PutObjectAcl(ctx, &s3.PutObjectAclInput{Bucket: &bucket, Key: &filePath, ACL: acl}); err != nil {
		return fmt.Errorf("failed to set ACL on S3 object %s: %v", filePath, err)
	}

	return nil
}

func newS3Blob(
	ctx context.Context,
	bucket string,
	endpoint string,
	region string,
	usePathStyle bool) (*uploadHandler, error) {
	// AWS SDK does require specifying regions, thus set it to default S3 region
	if region == "" {
		region = "us-east1"
	}
	endpointResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
		if service == s3.ServiceID && endpoint != "" {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           endpoint,
				SigningRegion: region,
			}, nil
		}
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})
	conf, err := config.LoadDefaultConfig(
		ctx, config.WithRegion(region),
		config.WithEndpointResolverWithOptions(endpointResolver),
	)
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(conf, func(o *s3.Options) {
		o.UsePathStyle = usePathStyle
	})
	b, err := s3blob.OpenBucketV2(ctx, client, bucket, nil)
	return &uploadHandler{
		blob:             blobS3{client: client},
		ctx:              ctx,
		b:                b,
		blobUploadBucket: bucket,
		blobEndpoint:     endpoint,
		hdpScheme:        "s3a",
	}, err
}
