/*
Copyright 2026 The Kubeflow authors.

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

package certificate

import (
	"context"
	"errors"
	"fmt"
	"time"

	"k8s.io/apiserver/pkg/server/dynamiccertificates"
)

func acquireDynamicServingContent(
	ctx context.Context,
	certPath string,
	keyPath string,
	waitTimeout time.Duration,
	retryInterval time.Duration,
) (*dynamiccertificates.DynamicCertKeyPairContent, error) {
	if waitTimeout <= 0 {
		return nil, fmt.Errorf("certificate wait timeout must be positive")
	}
	if retryInterval <= 0 {
		return nil, fmt.Errorf("certificate retry interval must be positive")
	}

	waitCtx, cancel := context.WithTimeout(ctx, waitTimeout)
	defer cancel()

	var lastErr error
	for {
		if err := waitCtx.Err(); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil, err
			}
			return nil, fmt.Errorf("timed out waiting for certificate %q and key %q: %w", certPath, keyPath, lastErr)
		}

		provider, err := dynamiccertificates.NewDynamicServingContentFromFiles("webhook-serving-cert", certPath, keyPath)
		if err == nil {
			return provider, nil
		}
		lastErr = err

		timer := time.NewTimer(retryInterval)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			if errors.Is(waitCtx.Err(), context.Canceled) {
				return nil, waitCtx.Err()
			}
			return nil, fmt.Errorf("timed out waiting for certificate %q and key %q: %w", certPath, keyPath, lastErr)
		case <-timer.C:
		}
	}
}
