package cmd

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
	endpointResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
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
