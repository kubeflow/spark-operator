/*
Copyright 2024 The Kubeflow authors.

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

package webhook

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func assertMemory(memoryString string, expectedBytes int64, t *testing.T) {
	m, err := parseJavaMemoryString(memoryString)
	if err != nil {
		t.Error(err)
		return
	}
	if m != expectedBytes {
		t.Errorf("%s: expected %v bytes, got %v bytes", memoryString, expectedBytes, m)
		return
	}
}

func TestJavaMemoryString(t *testing.T) {
	assertMemory("1b", 1, t)
	assertMemory("100k", 100*1024, t)
	assertMemory("1gb", 1024*1024*1024, t)
	assertMemory("10TB", 10*1024*1024*1024*1024, t)
	assertMemory("10PB", 10*1024*1024*1024*1024*1024, t)
}

// TestValidateResourceQuota_SpecHardDivergence is a regression test that
// verifies validateResourceQuota enforces the limit from Status.Hard (the
// authoritative, controller-reconciled value) rather than Spec.Hard (the
// desired limit written by the administrator).
//
// Kubernetes' own ResourceQuota admission controller always checks
// Status.Used + request ≤ Status.Hard. The spark-operator webhook must mirror
// that behaviour, because Status.Hard is what the quota controller actually
// enforces and tracks Status.Used against.
//
// In steady state Status.Hard == Spec.Hard, so the bug is normally invisible.
// It surfaces when the two fields diverge, for example immediately after a
// quota spec update before the quota controller has reconciled the status.
func TestValidateResourceQuota_SpecHardDivergence(t *testing.T) {
	// Construct a ResourceQuota where Spec.Hard ≠ Status.Hard.
	//
	// Scenario: an administrator recently raised the CPU limit in Spec to 100,
	// but the quota controller has not yet reconciled Status.Hard, which still
	// reflects the old enforced limit of 4.
	//
	//   Status.Hard[cpu] = 4   ← what Kubernetes actually enforces
	//   Status.Used[cpu] = 0
	//   Spec.Hard[cpu]   = 100 ← desired new limit, not yet enforced
	//
	// A request for 5 CPU cores must be REJECTED, because 0 + 5 > Status.Hard(4).
	// The buggy implementation compares against Spec.Hard(100) and would ALLOW it.
	quota := corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "diverged-quota",
			Namespace: "default",
		},
		Spec: corev1.ResourceQuotaSpec{
			Hard: corev1.ResourceList{
				// Spec says 100 CPUs — not yet enforced.
				corev1.ResourceCPU: resource.MustParse("100"),
			},
		},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				// Status says 4 CPUs — what the quota controller actually enforces.
				corev1.ResourceCPU: resource.MustParse("4"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("0"),
			},
		},
	}

	// Request 5 CPUs: exceeds Status.Hard(4) but within Spec.Hard(100).
	request := corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("5"),
	}

	// validateResourceQuota must return false (reject), because the request
	// exceeds the enforced Status.Hard limit.
	//
	// On the BUGGY upstream code this returns true (allow), because the
	// comparison is done against Spec.Hard(100) instead of Status.Hard(4).
	if got := validateResourceQuota(request, quota); got != false {
		t.Errorf(
			"validateResourceQuota incorrectly returned %v (allow) for a request that exceeds the status hard limit",
			got,
		)
	}
}

