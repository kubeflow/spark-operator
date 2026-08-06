package sparkconnect

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

type serverPodStatus struct {
	state     v1alpha1.SparkConnectState
	condition metav1.Condition
}

func checkServerPodStatus(pod *corev1.Pod) serverPodStatus {
	switch pod.Status.Phase {
	case corev1.PodSucceeded:
		return serverPodStatus{
			state: v1alpha1.SparkConnectStateCompleted,
			condition: metav1.Condition{
				Type:    string(v1alpha1.SparkConnectConditionServerPodReady),
				Status:  metav1.ConditionFalse,
				Reason:  string(v1alpha1.SparkConnectConditionReasonCompleted),
				Message: "Server pod completed",
			},
		}
	case corev1.PodFailed:
		return serverPodStatus{
			state: v1alpha1.SparkConnectStateFailed,
			condition: metav1.Condition{
				Type:    string(v1alpha1.SparkConnectConditionServerPodReady),
				Status:  metav1.ConditionFalse,
				Reason:  string(v1alpha1.SparkConnectConditionReasonServerPodNotReady),
				Message: fmt.Sprintf("Server pod failed: %s", pod.Status.Message),
			},
		}
	default:
		ready := util.IsPodReady(pod)
		if ready {
			return serverPodStatus{
				state: v1alpha1.SparkConnectStateReady,
				condition: metav1.Condition{
					Type:    string(v1alpha1.SparkConnectConditionServerPodReady),
					Status:  metav1.ConditionTrue,
					Reason:  string(v1alpha1.SparkConnectConditionReasonServerPodReady),
					Message: "Server pod is ready",
				},
			}
		}
		return serverPodStatus{
			state: v1alpha1.SparkConnectStateNotReady,
			condition: metav1.Condition{
				Type:    string(v1alpha1.SparkConnectConditionServerPodReady),
				Status:  metav1.ConditionFalse,
				Reason:  string(v1alpha1.SparkConnectConditionReasonServerPodNotReady),
				Message: fmt.Sprintf("Server pod is not ready: %s", pod.Status.Message),
			},
		}
	}
}

type serverPodRestartDecisionReason string

const (
	serverPodRestartDecisionNone          serverPodRestartDecisionReason = ""
	serverPodRestartDecisionTerminating   serverPodRestartDecisionReason = "Terminating"
	serverPodRestartDecisionLimitExceeded serverPodRestartDecisionReason = "LimitExceeded"
	serverPodRestartDecisionStartBackoff  serverPodRestartDecisionReason = "StartBackoff"
	serverPodRestartDecisionWaitBackoff   serverPodRestartDecisionReason = "WaitBackoff"
	serverPodRestartDecisionDeletePod     serverPodRestartDecisionReason = "DeletePod"
)

type serverPodRestartDecision struct {
	result                   reconcile.Result
	scheduled                bool
	reason                   serverPodRestartDecisionReason
	deletePod                bool
	incrementRestartAttempts bool
}

func (r *Reconciler) reconcileServerPodRestart(_ context.Context, conn *v1alpha1.SparkConnect, pod *corev1.Pod) serverPodRestartDecision {
	if !pod.DeletionTimestamp.IsZero() {
		return serverPodRestartDecision{
			result:    reconcile.Result{Requeue: true},
			scheduled: true,
			reason:    serverPodRestartDecisionTerminating,
		}
	}
	if !util.ShouldRestartSparkConnectServerPod(conn, pod) {
		return serverPodRestartDecision{}
	}
	if conn.Spec.RestartPolicy.RestartPolicyType == v1alpha1.RestartPolicyTypeOnFailure && !util.HasSparkConnectRestartAttemptsRemaining(conn) {
		return serverPodRestartDecision{
			reason: serverPodRestartDecisionLimitExceeded,
		}
	}

	backoff := util.SparkConnectRestartBackoff(conn)
	backoffStart, backoffStarted := util.SparkConnectRestartBackoffStartTime(conn)
	if !backoffStarted {
		return serverPodRestartDecision{
			result:    reconcile.Result{RequeueAfter: backoff},
			scheduled: true,
			reason:    serverPodRestartDecisionStartBackoff,
		}
	}

	elapsed := time.Since(backoffStart)
	if elapsed < backoff {
		return serverPodRestartDecision{
			result:    reconcile.Result{RequeueAfter: backoff - elapsed},
			scheduled: true,
			reason:    serverPodRestartDecisionWaitBackoff,
		}
	}

	return serverPodRestartDecision{
		result:    reconcile.Result{Requeue: true},
		scheduled: true,
		reason:    serverPodRestartDecisionDeletePod,
		deletePod: true,
	}
}

func applyServerPodStatus(conn *v1alpha1.SparkConnect, pod *corev1.Pod, podStatus serverPodStatus, restartDecision serverPodRestartDecision) {
	conn.Status.Server.PodName = pod.Name
	conn.Status.Server.PodIP = pod.Status.PodIP

	if restartDecision.incrementRestartAttempts {
		conn.Status.RestartAttempts++
		conn.Status.LastRestartTime = metav1.Now()
	}

	switch restartDecision.reason {
	case serverPodRestartDecisionLimitExceeded:
		if podStatus.state == v1alpha1.SparkConnectStateCompleted {
			_ = meta.SetStatusCondition(&conn.Status.Conditions, podStatus.condition)
			conn.Status.State = podStatus.state
			return
		}
		condition := metav1.Condition{
			Type:    string(v1alpha1.SparkConnectConditionServerPodReady),
			Status:  metav1.ConditionFalse,
			Reason:  string(v1alpha1.SparkConnectConditionReasonRestartLimitExceeded),
			Message: "Server pod restart limit exceeded",
		}
		_ = meta.SetStatusCondition(&conn.Status.Conditions, condition)
		conn.Status.State = v1alpha1.SparkConnectStateFailed
	case serverPodRestartDecisionStartBackoff:
		now := metav1.Now()
		condition := metav1.Condition{
			Type:               string(v1alpha1.SparkConnectConditionServerPodReady),
			Status:             metav1.ConditionFalse,
			Reason:             string(v1alpha1.SparkConnectConditionReasonRestarting),
			Message:            "Server pod restart scheduled",
			LastTransitionTime: now,
		}
		_ = meta.SetStatusCondition(&conn.Status.Conditions, condition)
		conn.Status.State = v1alpha1.SparkConnectStateNotReady
	case serverPodRestartDecisionWaitBackoff, serverPodRestartDecisionDeletePod:
		conn.Status.State = v1alpha1.SparkConnectStateNotReady
	case serverPodRestartDecisionTerminating:
		if _, backoffStarted := util.SparkConnectRestartBackoffStartTime(conn); backoffStarted {
			conn.Status.State = v1alpha1.SparkConnectStateNotReady
			return
		}
		_ = meta.SetStatusCondition(&conn.Status.Conditions, podStatus.condition)
		conn.Status.State = podStatus.state
	default:
		_ = meta.SetStatusCondition(&conn.Status.Conditions, podStatus.condition)
		conn.Status.State = podStatus.state
	}
}
