/*
Copyright 2026 Kubeflow Authors.

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

package sparkconnect

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
)

func TestMutateServerPodSetsRestartPolicyNever(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
	t.Setenv("KUBERNETES_SERVICE_PORT", "443")

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add scheme: %v", err)
	}

	reconciler := &Reconciler{scheme: scheme}
	conn := &v1alpha1.SparkConnect{
		Spec: v1alpha1.SparkConnectSpec{
			SparkVersion: "4.0.0",
			Image:        ptr.To("spark:4.0.0"),
			Server: v1alpha1.ServerSpec{
				SparkPodSpec: v1alpha1.SparkPodSpec{
					Template: &corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyAlways,
						},
					},
				},
			},
		},
	}
	pod := &corev1.Pod{}

	if err := reconciler.mutateServerPod(context.Background(), conn, pod); err != nil {
		t.Fatalf("mutateServerPod returned error: %v", err)
	}

	if pod.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Fatalf("restart policy = %q, want %q", pod.Spec.RestartPolicy, corev1.RestartPolicyNever)
	}
}

func TestRestartPolicyAllowsRestartForPod(t *testing.T) {
	maxAttempts := int32(2)
	tests := []struct {
		name string
		conn *v1alpha1.SparkConnect
		pod  *corev1.Pod
		want bool
	}{
		{
			name: "always restarts completed server pod",
			conn: &v1alpha1.SparkConnect{
				Spec: v1alpha1.SparkConnectSpec{
					RestartConfig: v1alpha1.RestartConfig{RestartPolicy: v1alpha1.SparkConnectRestartPolicyAlways},
				},
				Status: v1alpha1.SparkConnectStatus{State: v1alpha1.SparkConnectStateCompleted},
			},
			pod:  &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodSucceeded}},
			want: true,
		},
		{
			name: "on failure does not restart completed server pod",
			conn: &v1alpha1.SparkConnect{
				Spec: v1alpha1.SparkConnectSpec{
					RestartConfig: v1alpha1.RestartConfig{RestartPolicy: v1alpha1.SparkConnectRestartPolicyOnFailure},
				},
				Status: v1alpha1.SparkConnectStatus{State: v1alpha1.SparkConnectStateCompleted},
			},
			pod:  &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodSucceeded}},
			want: false,
		},
		{
			name: "on failure restarts failed server pod",
			conn: &v1alpha1.SparkConnect{
				Spec: v1alpha1.SparkConnectSpec{
					RestartConfig: v1alpha1.RestartConfig{RestartPolicy: v1alpha1.SparkConnectRestartPolicyOnFailure},
				},
				Status: v1alpha1.SparkConnectStatus{State: v1alpha1.SparkConnectStateFailed},
			},
			pod:  &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodFailed}},
			want: true,
		},
		{
			name: "never does not restart failed server pod",
			conn: &v1alpha1.SparkConnect{
				Spec: v1alpha1.SparkConnectSpec{
					RestartConfig: v1alpha1.RestartConfig{RestartPolicy: v1alpha1.SparkConnectRestartPolicyNever},
				},
				Status: v1alpha1.SparkConnectStatus{State: v1alpha1.SparkConnectStateFailed},
			},
			pod:  &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodFailed}},
			want: false,
		},
		{
			name: "finite attempts stop when exhausted",
			conn: &v1alpha1.SparkConnect{
				Spec: v1alpha1.SparkConnectSpec{
					RestartConfig: v1alpha1.RestartConfig{
						RestartPolicy:      v1alpha1.SparkConnectRestartPolicyAlways,
						MaxRestartAttempts: &maxAttempts,
					},
				},
				Status: v1alpha1.SparkConnectStatus{
					State:           v1alpha1.SparkConnectStateFailed,
					RestartAttempts: 2,
				},
			},
			pod:  &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodFailed}},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := restartPolicyAllowsRestartForPod(tt.conn, tt.pod); got != tt.want {
				t.Fatalf("restartPolicyAllowsRestartForPod() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCheckServerPodStatusSetsCompleted(t *testing.T) {
	conn := &v1alpha1.SparkConnect{}
	pod := &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodSucceeded}}

	applyServerPodStatus(conn, pod, checkServerPodStatus(pod), serverPodRestartDecision{})

	if conn.Status.State != v1alpha1.SparkConnectStateCompleted {
		t.Fatalf("state = %q, want %q", conn.Status.State, v1alpha1.SparkConnectStateCompleted)
	}
}

func TestApplyServerPodStatusSetsNotReadyWhenRestartScheduled(t *testing.T) {
	conn := &v1alpha1.SparkConnect{
		Spec: v1alpha1.SparkConnectSpec{},
	}
	pod := &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodFailed}}

	applyServerPodStatus(conn, pod, checkServerPodStatus(pod), serverPodRestartDecision{
		scheduled: true,
		reason:    serverPodRestartDecisionStartBackoff,
	})

	if conn.Status.State != v1alpha1.SparkConnectStateNotReady {
		t.Fatalf("state = %q, want %q", conn.Status.State, v1alpha1.SparkConnectStateNotReady)
	}
	condition := meta.FindStatusCondition(conn.Status.Conditions, string(v1alpha1.SparkConnectConditionServerPodReady))
	if condition == nil {
		t.Fatalf("condition %s not found", v1alpha1.SparkConnectConditionServerPodReady)
	}
	if condition.Reason != string(v1alpha1.SparkConnectConditionReasonRestarting) {
		t.Fatalf("condition reason = %s, want %s", condition.Reason, v1alpha1.SparkConnectConditionReasonRestarting)
	}
}

func TestApplyServerPodStatusSetsFailedWhenRestartAttemptsExhausted(t *testing.T) {
	maxAttempts := int32(1)
	conn := &v1alpha1.SparkConnect{
		Spec: v1alpha1.SparkConnectSpec{
			RestartConfig: v1alpha1.RestartConfig{
				RestartPolicy:      v1alpha1.SparkConnectRestartPolicyOnFailure,
				MaxRestartAttempts: &maxAttempts,
			},
		},
		Status: v1alpha1.SparkConnectStatus{
			RestartAttempts: 1,
		},
	}
	pod := &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodFailed}}

	decision := serverPodRestartDecision{reason: serverPodRestartDecisionLimitExceeded}
	applyServerPodStatus(conn, pod, checkServerPodStatus(pod), decision)

	if conn.Status.State != v1alpha1.SparkConnectStateFailed {
		t.Fatalf("state = %q, want %q", conn.Status.State, v1alpha1.SparkConnectStateFailed)
	}
}

func TestApplyServerPodStatusKeepsCompletedWhenRestartAttemptsExhausted(t *testing.T) {
	maxAttempts := int32(1)
	conn := &v1alpha1.SparkConnect{
		Spec: v1alpha1.SparkConnectSpec{
			RestartConfig: v1alpha1.RestartConfig{
				RestartPolicy:      v1alpha1.SparkConnectRestartPolicyAlways,
				MaxRestartAttempts: &maxAttempts,
			},
		},
		Status: v1alpha1.SparkConnectStatus{
			RestartAttempts: 1,
		},
	}
	pod := &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodSucceeded}}

	decision := serverPodRestartDecision{reason: serverPodRestartDecisionLimitExceeded}
	applyServerPodStatus(conn, pod, checkServerPodStatus(pod), decision)

	if conn.Status.State != v1alpha1.SparkConnectStateCompleted {
		t.Fatalf("state = %q, want %q", conn.Status.State, v1alpha1.SparkConnectStateCompleted)
	}
	condition := meta.FindStatusCondition(conn.Status.Conditions, string(v1alpha1.SparkConnectConditionServerPodReady))
	if condition == nil {
		t.Fatalf("condition %s not found", v1alpha1.SparkConnectConditionServerPodReady)
	}
	if condition.Reason != string(v1alpha1.SparkConnectConditionReasonCompleted) {
		t.Fatalf("condition reason = %s, want %s", condition.Reason, v1alpha1.SparkConnectConditionReasonCompleted)
	}
}

func TestReconcileServerPodRestartIncrementsAttemptsWhenPodDeleted(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add core scheme: %v", err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add spark connect scheme: %v", err)
	}

	maxAttempts := int32(1)
	backoffMillis := int64(0)
	conn := &v1alpha1.SparkConnect{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-connect",
			Namespace: "default",
		},
		Spec: v1alpha1.SparkConnectSpec{
			RestartConfig: v1alpha1.RestartConfig{
				RestartPolicy:        v1alpha1.SparkConnectRestartPolicyOnFailure,
				MaxRestartAttempts:   &maxAttempts,
				RestartBackoffMillis: &backoffMillis,
			},
		},
		Status: v1alpha1.SparkConnectStatus{
			State: v1alpha1.SparkConnectStateNotReady,
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GetServerPodName(conn),
			Namespace: conn.Namespace,
		},
		Status: corev1.PodStatus{Phase: corev1.PodFailed},
	}
	reconciler := &Reconciler{
		client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(pod).
			Build(),
	}

	decision := reconciler.reconcileServerPodRestart(ctx, conn, pod)
	if !decision.scheduled {
		t.Fatal("restart was not scheduled")
	}
	if decision.reason != serverPodRestartDecisionStartBackoff {
		t.Fatalf("restart decision = %s, want %s", decision.reason, serverPodRestartDecisionStartBackoff)
	}
	if conn.Status.RestartAttempts != 0 {
		t.Fatalf("restart attempts after scheduling = %d, want 0", conn.Status.RestartAttempts)
	}
	if !conn.Status.LastRestartTime.IsZero() {
		t.Fatal("last restart time should stay unset while scheduling restart")
	}
	_ = meta.SetStatusCondition(&conn.Status.Conditions, metav1.Condition{
		Type:               string(v1alpha1.SparkConnectConditionServerPodReady),
		Status:             metav1.ConditionFalse,
		Reason:             string(v1alpha1.SparkConnectConditionReasonRestarting),
		Message:            "Server pod restart scheduled",
		LastTransitionTime: metav1.Now(),
	})
	if err := reconciler.client.Get(ctx, types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, &corev1.Pod{}); err != nil {
		t.Fatalf("pod should still exist before restart backoff deletion: %v", err)
	}

	decision = reconciler.reconcileServerPodRestart(ctx, conn, pod)
	if !decision.scheduled {
		t.Fatal("restart deletion was not scheduled")
	}
	if decision.reason != serverPodRestartDecisionDeletePod {
		t.Fatalf("restart decision = %s, want %s", decision.reason, serverPodRestartDecisionDeletePod)
	}
	if !decision.deletePod {
		t.Fatal("restart decision should delete the pod")
	}
	if conn.Status.RestartAttempts != 0 {
		t.Fatalf("restart attempts after delete decision = %d, want 0", conn.Status.RestartAttempts)
	}
	if !conn.Status.LastRestartTime.IsZero() {
		t.Fatal("last restart time should stay unset")
	}
	if err := reconciler.client.Get(ctx, types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, &corev1.Pod{}); err != nil {
		t.Fatalf("restart decision should not delete pod, got err = %v", err)
	}
}

func TestReconcileServerPodRestartIncrementsAttemptsAcrossReplacementPods(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add core scheme: %v", err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add spark connect scheme: %v", err)
	}

	maxAttempts := int32(3)
	backoffMillis := int64(0)
	conn := &v1alpha1.SparkConnect{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-connect",
			Namespace: "default",
		},
		Spec: v1alpha1.SparkConnectSpec{
			RestartConfig: v1alpha1.RestartConfig{
				RestartPolicy:        v1alpha1.SparkConnectRestartPolicyOnFailure,
				MaxRestartAttempts:   &maxAttempts,
				RestartBackoffMillis: &backoffMillis,
			},
		},
		Status: v1alpha1.SparkConnectStatus{
			State: v1alpha1.SparkConnectStateFailed,
		},
	}
	reconciler := &Reconciler{
		client: fake.NewClientBuilder().
			WithScheme(scheme).
			Build(),
	}

	for attempt := int32(1); attempt <= maxAttempts; attempt++ {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      GetServerPodName(conn),
				Namespace: conn.Namespace,
			},
			Status: corev1.PodStatus{Phase: corev1.PodFailed},
		}
		if err := reconciler.client.Create(ctx, pod); err != nil {
			t.Fatalf("failed to create replacement pod for attempt %d: %v", attempt, err)
		}

		if conn.Status.LastRestartTime.IsZero() {
			decision := reconciler.reconcileServerPodRestart(ctx, conn, pod)
			if !decision.scheduled {
				t.Fatalf("restart attempt %d was not scheduled", attempt)
			}
			if decision.reason != serverPodRestartDecisionStartBackoff {
				t.Fatalf("restart decision attempt %d = %s, want %s", attempt, decision.reason, serverPodRestartDecisionStartBackoff)
			}
			_ = meta.SetStatusCondition(&conn.Status.Conditions, metav1.Condition{
				Type:               string(v1alpha1.SparkConnectConditionServerPodReady),
				Status:             metav1.ConditionFalse,
				Reason:             string(v1alpha1.SparkConnectConditionReasonRestarting),
				Message:            "Server pod restart scheduled",
				LastTransitionTime: metav1.Now(),
			})
		}

		decision := reconciler.reconcileServerPodRestart(ctx, conn, pod)
		if !decision.scheduled {
			t.Fatalf("restart deletion attempt %d was not scheduled", attempt)
		}
		if decision.reason != serverPodRestartDecisionDeletePod {
			t.Fatalf("restart decision attempt %d = %s, want %s", attempt, decision.reason, serverPodRestartDecisionDeletePod)
		}
		if !decision.deletePod {
			t.Fatalf("restart decision attempt %d should delete the pod", attempt)
		}
		if err := reconciler.client.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
			t.Fatalf("failed to delete pod for attempt %d: %v", attempt, err)
		}
		decision.incrementRestartAttempts = true
		applyServerPodStatus(conn, pod, checkServerPodStatus(pod), decision)

		if conn.Status.RestartAttempts != attempt {
			t.Fatalf("restart attempts after deleting pod = %d, want %d", conn.Status.RestartAttempts, attempt)
		}
		if err := reconciler.client.Get(ctx, types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, &corev1.Pod{}); !apierrors.IsNotFound(err) {
			t.Fatalf("pod should be deleted for attempt %d, got err = %v", attempt, err)
		}
	}
}

func TestReconcileServerPodRestartSetsFailedWhenRestartAttemptsExhausted(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add core scheme: %v", err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add spark connect scheme: %v", err)
	}

	maxAttempts := int32(1)
	conn := &v1alpha1.SparkConnect{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-connect",
			Namespace: "default",
		},
		Spec: v1alpha1.SparkConnectSpec{
			RestartConfig: v1alpha1.RestartConfig{
				RestartPolicy:      v1alpha1.SparkConnectRestartPolicyOnFailure,
				MaxRestartAttempts: &maxAttempts,
			},
		},
		Status: v1alpha1.SparkConnectStatus{
			State:           v1alpha1.SparkConnectStateNotReady,
			RestartAttempts: 1,
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GetServerPodName(conn),
			Namespace: conn.Namespace,
		},
		Status: corev1.PodStatus{Phase: corev1.PodFailed},
	}
	reconciler := &Reconciler{
		client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(pod).
			Build(),
	}

	decision := reconciler.reconcileServerPodRestart(ctx, conn, pod)
	if decision.scheduled {
		t.Fatal("exhausted restart attempts should not schedule another restart")
	}
	if decision.reason != serverPodRestartDecisionLimitExceeded {
		t.Fatalf("restart decision = %s, want %s", decision.reason, serverPodRestartDecisionLimitExceeded)
	}
	applyServerPodStatus(conn, pod, checkServerPodStatus(pod), decision)
	if conn.Status.State != v1alpha1.SparkConnectStateFailed {
		t.Fatalf("state = %q, want %q", conn.Status.State, v1alpha1.SparkConnectStateFailed)
	}
}

func TestReconcileServerPodRestartDoesNotMutateStatusWhenScheduling(t *testing.T) {
	ctx := context.Background()
	conn := &v1alpha1.SparkConnect{
		Spec: v1alpha1.SparkConnectSpec{
			RestartConfig: v1alpha1.RestartConfig{
				RestartPolicy: v1alpha1.SparkConnectRestartPolicyOnFailure,
			},
		},
	}
	pod := &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodFailed}}
	reconciler := &Reconciler{}

	decision := reconciler.reconcileServerPodRestart(ctx, conn, pod)
	if !decision.scheduled {
		t.Fatal("restart was not scheduled")
	}
	if decision.reason != serverPodRestartDecisionStartBackoff {
		t.Fatalf("restart decision = %s, want %s", decision.reason, serverPodRestartDecisionStartBackoff)
	}
	if conn.Status.State != "" {
		t.Fatalf("state = %q, want empty", conn.Status.State)
	}
	if len(conn.Status.Conditions) != 0 {
		t.Fatalf("conditions = %d, want 0", len(conn.Status.Conditions))
	}
	if conn.Status.RestartAttempts != 0 {
		t.Fatalf("restart attempts = %d, want 0", conn.Status.RestartAttempts)
	}
	if !conn.Status.LastRestartTime.IsZero() {
		t.Fatal("last restart time should stay unset")
	}
}

func TestReconcileKeepsRestartableFailedServerPodNotReady(t *testing.T) {
	ctx := context.Background()
	t.Setenv("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
	t.Setenv("KUBERNETES_SERVICE_PORT", "443")

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add core scheme: %v", err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add spark connect scheme: %v", err)
	}

	maxAttempts := int32(3)
	backoffMillis := int64(0)
	conn := &v1alpha1.SparkConnect{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "spark-connect",
			Namespace:  "default",
			Generation: 1,
		},
		Spec: v1alpha1.SparkConnectSpec{
			SparkVersion: "4.0.0",
			Image:        ptr.To("spark:4.0.0"),
			RestartConfig: v1alpha1.RestartConfig{
				RestartPolicy:        v1alpha1.SparkConnectRestartPolicyAlways,
				MaxRestartAttempts:   &maxAttempts,
				RestartBackoffMillis: &backoffMillis,
			},
			Server: v1alpha1.ServerSpec{
				SparkPodSpec: v1alpha1.SparkPodSpec{
					Template: &corev1.PodTemplateSpec{},
				},
			},
		},
		Status: v1alpha1.SparkConnectStatus{
			ObservedGeneration: 1,
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GetServerPodName(conn),
			Namespace: conn.Namespace,
		},
		Status: corev1.PodStatus{Phase: corev1.PodFailed},
	}
	reconciler := &Reconciler{
		scheme: scheme,
		client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&v1alpha1.SparkConnect{}).
			WithObjects(conn, pod).
			Build(),
	}

	if _, err := reconciler.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: conn.Name, Namespace: conn.Namespace},
	}); err != nil {
		t.Fatalf("failed to reconcile SparkConnect: %v", err)
	}

	latest := &v1alpha1.SparkConnect{}
	if err := reconciler.client.Get(ctx, types.NamespacedName{Name: conn.Name, Namespace: conn.Namespace}, latest); err != nil {
		t.Fatalf("failed to get SparkConnect: %v", err)
	}
	if latest.Status.State != v1alpha1.SparkConnectStateNotReady {
		t.Fatalf("state = %s, want %s", latest.Status.State, v1alpha1.SparkConnectStateNotReady)
	}
	if latest.Status.RestartAttempts != 0 {
		t.Fatalf("restart attempts = %d, want 0", latest.Status.RestartAttempts)
	}
}

func TestReconcileKeepsRestartableCompletedServerPodNotReady(t *testing.T) {
	ctx := context.Background()
	t.Setenv("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
	t.Setenv("KUBERNETES_SERVICE_PORT", "443")

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add core scheme: %v", err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add spark connect scheme: %v", err)
	}

	maxAttempts := int32(3)
	backoffMillis := int64(0)
	conn := &v1alpha1.SparkConnect{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "spark-connect",
			Namespace:  "default",
			Generation: 1,
		},
		Spec: v1alpha1.SparkConnectSpec{
			SparkVersion: "4.0.0",
			Image:        ptr.To("spark:4.0.0"),
			RestartConfig: v1alpha1.RestartConfig{
				RestartPolicy:        v1alpha1.SparkConnectRestartPolicyAlways,
				MaxRestartAttempts:   &maxAttempts,
				RestartBackoffMillis: &backoffMillis,
			},
			Server: v1alpha1.ServerSpec{
				SparkPodSpec: v1alpha1.SparkPodSpec{
					Template: &corev1.PodTemplateSpec{},
				},
			},
		},
		Status: v1alpha1.SparkConnectStatus{
			ObservedGeneration: 1,
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GetServerPodName(conn),
			Namespace: conn.Namespace,
		},
		Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
	}
	reconciler := &Reconciler{
		scheme: scheme,
		client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&v1alpha1.SparkConnect{}).
			WithObjects(conn, pod).
			Build(),
	}

	if _, err := reconciler.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: conn.Name, Namespace: conn.Namespace},
	}); err != nil {
		t.Fatalf("failed to reconcile SparkConnect: %v", err)
	}

	latest := &v1alpha1.SparkConnect{}
	if err := reconciler.client.Get(ctx, types.NamespacedName{Name: conn.Name, Namespace: conn.Namespace}, latest); err != nil {
		t.Fatalf("failed to get SparkConnect: %v", err)
	}
	if latest.Status.State != v1alpha1.SparkConnectStateNotReady {
		t.Fatalf("state = %s, want %s", latest.Status.State, v1alpha1.SparkConnectStateNotReady)
	}
	condition := meta.FindStatusCondition(latest.Status.Conditions, string(v1alpha1.SparkConnectConditionServerPodReady))
	if condition == nil {
		t.Fatalf("condition %s not found", v1alpha1.SparkConnectConditionServerPodReady)
	}
	if condition.Reason != string(v1alpha1.SparkConnectConditionReasonRestarting) {
		t.Fatalf("condition reason = %s, want %s", condition.Reason, v1alpha1.SparkConnectConditionReasonRestarting)
	}
}

func TestReconcileServerPodRestartDoesNotScheduleTerminatingPod(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add core scheme: %v", err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add spark connect scheme: %v", err)
	}

	maxAttempts := int32(3)
	backoffMillis := int64(0)
	conn := &v1alpha1.SparkConnect{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-connect",
			Namespace: "default",
		},
		Spec: v1alpha1.SparkConnectSpec{
			RestartConfig: v1alpha1.RestartConfig{
				RestartPolicy:        v1alpha1.SparkConnectRestartPolicyOnFailure,
				MaxRestartAttempts:   &maxAttempts,
				RestartBackoffMillis: &backoffMillis,
			},
		},
		Status: v1alpha1.SparkConnectStatus{
			State:           v1alpha1.SparkConnectStateFailed,
			RestartAttempts: 1,
		},
	}
	now := metav1.Now()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              GetServerPodName(conn),
			Namespace:         conn.Namespace,
			DeletionTimestamp: &now,
			Finalizers:        []string{"test.sparkoperator.k8s.io/finalizer"},
		},
	}
	reconciler := &Reconciler{
		client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(pod).
			Build(),
	}

	decision := reconciler.reconcileServerPodRestart(ctx, conn, pod)
	if !decision.scheduled {
		t.Fatal("terminating pod should requeue restart reconciliation")
	}
	if decision.reason != serverPodRestartDecisionTerminating {
		t.Fatalf("restart decision = %s, want %s", decision.reason, serverPodRestartDecisionTerminating)
	}
	if conn.Status.RestartAttempts != 1 {
		t.Fatalf("restart attempts = %d, want 1", conn.Status.RestartAttempts)
	}
	if !conn.Status.LastRestartTime.IsZero() {
		t.Fatal("last restart time should stay unset for terminating pod")
	}
}
