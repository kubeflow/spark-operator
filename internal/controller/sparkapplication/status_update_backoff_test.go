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

package sparkapplication

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
)

// TestStatusUpdateConflictBackoffOutlastsCacheLag verifies that
// statusUpdateConflictBackoff gives status update retries enough total time to ride
// out informer cache lag, unlike retry.DefaultRetry which is exhausted almost
// immediately. Without this backoff, a burst of executor pod events around
// application completion can leave the informer cache stale for close to a second,
// so every retry.DefaultRetry attempt reads the same stale object and fails with a
// conflict before the cache catches up.
func TestStatusUpdateConflictBackoffOutlastsCacheLag(t *testing.T) {
	const observedCacheLag = 900 * time.Millisecond

	totalBackoffDuration := func(b wait.Backoff) time.Duration {
		var total time.Duration
		step := b.Duration
		for i := 0; i < b.Steps; i++ {
			total += step
			step = time.Duration(float64(step) * b.Factor)
		}
		return total
	}

	defaultRetryTotal := totalBackoffDuration(retry.DefaultRetry)
	if defaultRetryTotal >= observedCacheLag {
		t.Fatalf("expected retry.DefaultRetry's total budget (%s) to be shorter than the observed cache lag (%s)", defaultRetryTotal, observedCacheLag)
	}

	statusUpdateTotal := totalBackoffDuration(statusUpdateConflictBackoff)
	if statusUpdateTotal <= observedCacheLag {
		t.Fatalf("expected statusUpdateConflictBackoff's total budget (%s) to exceed the observed cache lag (%s)", statusUpdateTotal, observedCacheLag)
	}
}
