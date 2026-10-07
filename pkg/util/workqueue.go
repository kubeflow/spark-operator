/*
Copyright The Kubeflow Authors.

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

package util

import (
	"time"

	"golang.org/x/time/rate"
	"k8s.io/client-go/util/workqueue"
)

// This allow to create a new rate limiter while tuning the BucketRateLimiter parameters
// This also prevent a "bug" in the BucketRateLimiter due to the fact that a BucketRateLimiter does not have a maxDelay parameter
func NewRateLimiter[T comparable](qps int, bucketSize int, maxDelay time.Duration) workqueue.TypedRateLimiter[T] {
	return workqueue.NewTypedWithMaxWaitRateLimiter(
		workqueue.NewTypedMaxOfRateLimiter(
			workqueue.NewTypedItemExponentialFailureRateLimiter[T](5*time.Millisecond, 1000*time.Second),
			&workqueue.TypedBucketRateLimiter[T]{Limiter: rate.NewLimiter(rate.Limit(qps), bucketSize)},
		), maxDelay)
}
