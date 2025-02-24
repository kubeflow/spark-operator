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

package app

import (
	"fmt"

	"cloud.google.com/go/storage"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/gcp"
	"golang.org/x/net/context"
)

type blobGCS struct {
	projectID string
	endpoint  string
	region    string
}

func (blob blobGCS) setPublicACL(
	ctx context.Context,
	bucket string,
	filePath string) error {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	handle := client.Bucket(bucket).UserProject(blob.projectID)
	if err = handle.Object(filePath).ACL().Set(ctx, storage.AllUsers, storage.RoleReader); err != nil {
		return fmt.Errorf("failed to set ACL on GCS object %s: %v", filePath, err)
	}
	return nil
}

func newGCSBlob(
	ctx context.Context,
	bucket string,
	endpoint string,
	region string) (*uploadHandler, error) {
	creds, err := gcp.DefaultCredentials(ctx)
	if err != nil {
		return nil, err
	}

	projectID, err := gcp.DefaultProjectID(creds)
	if err != nil {
		return nil, err
	}

	c, err := gcp.NewHTTPClient(gcp.DefaultTransport(), gcp.CredentialsTokenSource(creds))
	if err != nil {
		return nil, err
	}

	b, err := gcsblob.OpenBucket(ctx, c, bucket, nil)
	return &uploadHandler{
		blob:             blobGCS{endpoint: endpoint, region: region, projectID: string(projectID)},
		ctx:              ctx,
		b:                b,
		blobUploadBucket: bucket,
		blobEndpoint:     endpoint,
		hdpScheme:        "gs",
	}, err
}
