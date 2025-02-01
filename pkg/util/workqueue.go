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
