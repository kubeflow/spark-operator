package util

import (
	"time"

	"golang.org/x/time/rate"
	"k8s.io/client-go/util/workqueue"
)

// This allow to create a new rate limiter while tuning the BucketRateLimiter parameters
// This also prevent a "bug" in the BucketRateLimiter due to the fact that a BucketRateLimiter does not have a maxDelay parameter
func NewRateLimiter(qps int, bucketSize int, maxDelay time.Duration) workqueue.RateLimiter {
	ratelimiter := workqueue.NewWithMaxWaitRateLimiter(
		workqueue.NewMaxOfRateLimiter(
			workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 1000*time.Second),
			&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(qps), bucketSize)},
		), maxDelay)
	return ratelimiter
}
