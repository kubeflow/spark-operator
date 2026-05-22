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

package sparkapplication_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/internal/controller/sparkapplication"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

// noopSubmitter implements SparkApplicationSubmitter without actually
// running spark-submit. The submit path normally shells out to a JVM; for
// these tests we just want Reconcile() to succeed and reach the
// createDriverPDB call that follows.
type noopSubmitter struct{}

func (*noopSubmitter) Submit(_ context.Context, _ *v1beta2.SparkApplication) error {
	return nil
}

// pdbTestKeepAliveFinalizer prevents envtest from immediately removing a
// SparkApplication on Delete - we need it to linger with a non-zero
// DeletionTimestamp so the dispatcher routes the next reconcile through
// handleSparkApplicationDeletion. envtest does not run the kube garbage
// collector or finalizer machinery on its own.
const pdbTestKeepAliveFinalizer = "test.spark-operator.io/keep-alive"

// pdbReconcilerOptions configures the test reconciler. Tests that need to
// inject a wrapped client (for transient-failure simulation) override the
// default k8sClient via clientOverride.
type pdbReconcilerOptions struct {
	enableGate     bool
	clientOverride client.Client
}

func newPDBReconciler(enableGate bool) *sparkapplication.Reconciler {
	return newPDBReconcilerWithOptions(pdbReconcilerOptions{enableGate: enableGate})
}

func newPDBReconcilerWithOptions(opts pdbReconcilerOptions) *sparkapplication.Reconciler {
	c := opts.clientOverride
	if c == nil {
		c = k8sClient
	}
	return sparkapplication.NewReconciler(
		nil,
		k8sClient.Scheme(),
		c,
		record.NewFakeRecorder(10),
		nil,
		&noopSubmitter{},
		sparkapplication.Options{
			EnableDriverPDB: opts.enableGate,
			Namespaces:      []string{"default"},
			// Generous grace period so updateDriverState in the Submitted
			// state branch tolerates the in-grace "driver pod not found"
			// error path. Tests that exercise the reconcile-loop ensure
			// path put a Pending driver pod in place ahead of time, but
			// the grace period covers any race where reconcile runs first.
			DriverPodCreationGracePeriod: 5 * time.Minute,
		},
	)
}

func newPDBApp(name string, enabled *bool) *v1beta2.SparkApplication {
	return &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: v1beta2.SparkApplicationSpec{
			MainApplicationFile:       ptr.To("local:///dummy.jar"),
			DriverPodDisruptionBudget: enabled,
		},
	}
}

// pdbKey returns the NamespacedName the PDB lives at. createDriverPDBObject
// names the PDB after the driver pod so the lookup key collapses to the
// same value driver pods use.
func pdbKey(app *v1beta2.SparkApplication) types.NamespacedName {
	return types.NamespacedName{Name: util.GetDriverPodName(app), Namespace: app.Namespace}
}

// makeDriverPod builds a driver pod whose name and labels match what
// updateDriverState looks up via getDriverPod. Phase is supplied by the
// caller so tests can stage Submitted vs Running scenarios.
func makeDriverPod(app *v1beta2.SparkApplication, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.GetDriverPodName(app),
			Namespace: app.Namespace,
			Labels: map[string]string{
				common.LabelSparkAppName:            app.Name,
				common.LabelLaunchedBySparkOperator: "true",
				common.LabelSparkRole:               common.SparkRoleDriver,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: common.SparkDriverContainerName, Image: "spark-driver:test"},
			},
		},
		Status: corev1.PodStatus{Phase: phase},
	}
}

// stageSubmittedApp creates a SparkApplication, force-advances it to the
// Submitted state via a status update, and creates a Pending driver pod so
// updateDriverState in the Submitted-state branch keeps the app at
// Submitted long enough for ensureDriverPDB to run.
//
// The reconcile loop's New-state path used to be where the PDB got created
// (as a side effect of submitSparkApplication), but ensureDriverPDB now
// runs in the Submitted/Running branches instead. Tests no longer want to
// go through New -> Submitted via a real reconcile because that requires a
// successful spark-submit; staging the state directly is simpler and
// targets exactly the code path that matters.
func stageSubmittedApp(ctx context.Context, app *v1beta2.SparkApplication) {
	v1beta2.SetSparkApplicationDefaults(app)
	Expect(k8sClient.Create(ctx, app)).To(Succeed())

	stored := &v1beta2.SparkApplication{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: app.Name, Namespace: app.Namespace}, stored)).To(Succeed())
	stored.Status.AppState.State = v1beta2.ApplicationStateSubmitted
	stored.Status.SubmissionID = "test-submission-id"
	stored.Status.LastSubmissionAttemptTime = metav1.Now()
	stored.Status.DriverInfo.PodName = util.GetDriverPodName(app)
	Expect(k8sClient.Status().Update(ctx, stored)).To(Succeed())

	pod := makeDriverPod(app, corev1.PodPending)
	Expect(k8sClient.Create(ctx, pod)).To(Succeed())
}

// stageRunningApp creates a SparkApplication, force-advances it to the
// Running state via a status update, and creates a Running driver pod so
// updateDriverState keeps the app in the Running branch.
func stageRunningApp(ctx context.Context, app *v1beta2.SparkApplication) {
	v1beta2.SetSparkApplicationDefaults(app)
	Expect(k8sClient.Create(ctx, app)).To(Succeed())

	stored := &v1beta2.SparkApplication{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: app.Name, Namespace: app.Namespace}, stored)).To(Succeed())
	stored.Status.AppState.State = v1beta2.ApplicationStateRunning
	stored.Status.SubmissionID = "test-submission-id"
	stored.Status.LastSubmissionAttemptTime = metav1.Now()
	stored.Status.DriverInfo.PodName = util.GetDriverPodName(app)
	Expect(k8sClient.Status().Update(ctx, stored)).To(Succeed())

	pod := makeDriverPod(app, corev1.PodRunning)
	Expect(k8sClient.Create(ctx, pod)).To(Succeed())
}

// deleteApp removes the SparkApplication, its driver pod, and any PDB the
// test created. PDB cleanup is the controller's job and is asserted on by
// individual tests; this is the safety net for tests that exit before
// cleanup runs. Strips any test-only finalizer so envtest can actually
// delete the object even if a spec exited mid-flight.
func deleteApp(ctx context.Context, app *v1beta2.SparkApplication) {
	got := &v1beta2.SparkApplication{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: app.Name, Namespace: app.Namespace}, got); err == nil {
		if len(got.Finalizers) > 0 {
			got.Finalizers = nil
			_ = k8sClient.Update(ctx, got)
		}
		_ = k8sClient.Delete(ctx, got)
	}
	pod := &corev1.Pod{}
	if err := k8sClient.Get(ctx, pdbKey(app), pod); err == nil {
		_ = k8sClient.Delete(ctx, pod)
	}
	pdb := &policyv1.PodDisruptionBudget{}
	if err := k8sClient.Get(ctx, pdbKey(app), pdb); err == nil {
		_ = k8sClient.Delete(ctx, pdb)
	}
}

func reconcileApp(ctx context.Context, r *sparkapplication.Reconciler, app *v1beta2.SparkApplication) error {
	_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: app.Name, Namespace: app.Namespace}})
	return err
}

var _ = Describe("Driver PodDisruptionBudget create path", func() {
	ctx := context.Background()

	It("creates a PDB with the expected shape when both gates are open", func() {
		app := newPDBApp("pdb-create-enabled", ptr.To(true))
		stageSubmittedApp(ctx, app)
		defer deleteApp(ctx, app)

		r := newPDBReconciler(true)
		Expect(reconcileApp(ctx, r, app)).To(Succeed())

		got := &policyv1.PodDisruptionBudget{}
		Expect(k8sClient.Get(ctx, pdbKey(app), got)).To(Succeed())

		Expect(got.Name).To(Equal(util.GetDriverPodName(app)))
		Expect(got.Namespace).To(Equal(app.Namespace))
		Expect(got.Spec.MinAvailable).ToNot(BeNil())
		Expect(got.Spec.MinAvailable.Type).To(Equal(intstr.Int))
		Expect(got.Spec.MinAvailable.IntVal).To(Equal(int32(1)))
		Expect(got.Spec.Selector).ToNot(BeNil())
		Expect(got.Spec.Selector.MatchLabels[common.LabelSparkAppName]).To(Equal(app.Name))
		Expect(got.Spec.Selector.MatchLabels[common.LabelSparkRole]).To(Equal(common.SparkRoleDriver))
		Expect(got.Labels).To(HaveKeyWithValue(common.LabelLaunchedBySparkOperator, "true"))
		Expect(got.Labels).To(HaveKeyWithValue(common.LabelSparkAppName, app.Name))
		Expect(got.OwnerReferences).ToNot(BeEmpty())
		Expect(got.OwnerReferences[0].Name).To(Equal(app.Name))
		Expect(got.OwnerReferences[0].Kind).To(Equal("SparkApplication"))
	})

	It("does not create a PDB when the spec is nil", func() {
		app := newPDBApp("pdb-spec-nil", nil)
		stageSubmittedApp(ctx, app)
		defer deleteApp(ctx, app)

		r := newPDBReconciler(true)
		Expect(reconcileApp(ctx, r, app)).To(Succeed())

		got := &policyv1.PodDisruptionBudget{}
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, pdbKey(app), got))).To(BeTrue())
	})

	It("does not create a PDB when the spec is disabled", func() {
		app := newPDBApp("pdb-spec-disabled", ptr.To(false))
		stageSubmittedApp(ctx, app)
		defer deleteApp(ctx, app)

		r := newPDBReconciler(true)
		Expect(reconcileApp(ctx, r, app)).To(Succeed())

		got := &policyv1.PodDisruptionBudget{}
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, pdbKey(app), got))).To(BeTrue())
	})

	It("does not create a PDB when the operator gate is off even with spec.enabled=true", func() {
		app := newPDBApp("pdb-gate-off", ptr.To(true))
		stageSubmittedApp(ctx, app)
		defer deleteApp(ctx, app)

		r := newPDBReconciler(false)
		Expect(reconcileApp(ctx, r, app)).To(Succeed())

		got := &policyv1.PodDisruptionBudget{}
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, pdbKey(app), got))).To(BeTrue())
	})

	It("is idempotent when reconciled twice (AlreadyExists is treated as success)", func() {
		app := newPDBApp("pdb-idempotent", ptr.To(true))
		stageSubmittedApp(ctx, app)
		defer deleteApp(ctx, app)

		r := newPDBReconciler(true)
		Expect(reconcileApp(ctx, r, app)).To(Succeed())
		Expect(reconcileApp(ctx, r, app)).To(Succeed())

		got := &policyv1.PodDisruptionBudget{}
		Expect(k8sClient.Get(ctx, pdbKey(app), got)).To(Succeed())
	})
})

var _ = Describe("Driver PodDisruptionBudget reconcile-loop convergence", func() {
	ctx := context.Background()

	It("creates the PDB after operator restart with the gate flipped on while the app is Running", func() {
		// Models the user-raised scenario: operator was running with the
		// gate off, the app was submitted (no PDB created), then the
		// operator was restarted with --enable-driver-pdb=true. The
		// next reconcile of the already-Running app must create the PDB.
		app := newPDBApp("pdb-restart-gate-flipped-on", ptr.To(true))
		stageRunningApp(ctx, app)
		defer deleteApp(ctx, app)

		// First reconcile: gate is OFF. ensureDriverPDB is a no-op so no
		// PDB is created even though spec.enabled=true.
		Expect(reconcileApp(ctx, newPDBReconciler(false), app)).To(Succeed())
		got := &policyv1.PodDisruptionBudget{}
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, pdbKey(app), got))).To(BeTrue())

		// Operator "restart" with the gate ON: the next reconcile of the
		// same Running app must discover that no PDB exists and create one.
		Expect(reconcileApp(ctx, newPDBReconciler(true), app)).To(Succeed())
		Expect(k8sClient.Get(ctx, pdbKey(app), got)).To(Succeed())
		Expect(got.Spec.MinAvailable.IntVal).To(Equal(int32(1)))
	})

	It("creates the PDB when the spec is flipped from false to true on a Running app", func() {
		// Live convergence for spec edits on running apps. The user
		// changes spec.driverPodDisruptionBudget from nil/false to true
		// without restarting the driver - the next reconcile must create
		// the PDB.
		app := newPDBApp("pdb-spec-flipped-on", ptr.To(false))
		stageRunningApp(ctx, app)
		defer deleteApp(ctx, app)

		r := newPDBReconciler(true)
		Expect(reconcileApp(ctx, r, app)).To(Succeed())
		got := &policyv1.PodDisruptionBudget{}
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, pdbKey(app), got))).To(BeTrue())

		stored := &v1beta2.SparkApplication{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: app.Name, Namespace: app.Namespace}, stored)).To(Succeed())
		stored.Spec.DriverPodDisruptionBudget = ptr.To(true)
		Expect(k8sClient.Update(ctx, stored)).To(Succeed())

		Expect(reconcileApp(ctx, r, app)).To(Succeed())
		Expect(k8sClient.Get(ctx, pdbKey(app), got)).To(Succeed())
	})

	It("removes the PDB when the spec is flipped from true to false on a Running app (before termination)", func() {
		// Variant of the existing terminal-cleanup spec. The ensure loop
		// owns deletion when the spec flips off; cleanUpOnTermination is a
		// belt-and-braces safety net for the terminal case but should not
		// be the only place this works.
		app := newPDBApp("pdb-spec-flipped-off-running", ptr.To(true))
		stageRunningApp(ctx, app)
		defer deleteApp(ctx, app)

		r := newPDBReconciler(true)
		Expect(reconcileApp(ctx, r, app)).To(Succeed())
		got := &policyv1.PodDisruptionBudget{}
		Expect(k8sClient.Get(ctx, pdbKey(app), got)).To(Succeed())

		stored := &v1beta2.SparkApplication{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: app.Name, Namespace: app.Namespace}, stored)).To(Succeed())
		stored.Spec.DriverPodDisruptionBudget = ptr.To(false)
		Expect(k8sClient.Update(ctx, stored)).To(Succeed())

		Expect(reconcileApp(ctx, r, app)).To(Succeed())
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, pdbKey(app), got))).To(BeTrue())
	})

	It("retries a transient PDB Create failure on the next reconcile (bubble-up retry path)", func() {
		// Simulates an API server blip: the first PDB Create fails with
		// a generic 500. ensureDriverPDB bubbles the error up through
		// reconcileSubmittedSparkApplication, and controller-runtime's
		// retry-on-conflict / backoff drives a re-reconcile. The second
		// reconcile uses a clean client and succeeds.
		app := newPDBApp("pdb-transient-create-fail", ptr.To(true))
		stageSubmittedApp(ctx, app)
		defer deleteApp(ctx, app)

		failOnce := newFailingCreateClient(&policyv1.PodDisruptionBudget{}, 1)
		r := newPDBReconcilerWithOptions(pdbReconcilerOptions{enableGate: true, clientOverride: failOnce})

		err := reconcileApp(ctx, r, app)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("failed to ensure driver PDB"))

		got := &policyv1.PodDisruptionBudget{}
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, pdbKey(app), got))).To(BeTrue())

		// A subsequent reconcile against a healthy client succeeds.
		Expect(reconcileApp(ctx, newPDBReconciler(true), app)).To(Succeed())
		Expect(k8sClient.Get(ctx, pdbKey(app), got)).To(Succeed())
	})
})

var _ = Describe("Driver PodDisruptionBudget delete path", func() {
	ctx := context.Background()

	It("garbage-collects the PDB when the SparkApplication is deleted", func() {
		app := newPDBApp("pdb-delete", ptr.To(true))
		stageSubmittedApp(ctx, app)
		defer deleteApp(ctx, app)

		r := newPDBReconciler(true)
		req := reconcile.Request{NamespacedName: types.NamespacedName{Name: app.Name, Namespace: app.Namespace}}

		// First reconcile creates the PDB through the Submitted-state ensure path.
		_, err := r.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		got := &policyv1.PodDisruptionBudget{}
		Expect(k8sClient.Get(ctx, pdbKey(app), got)).To(Succeed())

		// Add a test-only finalizer so envtest does not immediately remove
		// the object on Delete - we need it to linger with DeletionTimestamp
		// set so the dispatcher routes the next reconcile through
		// handleSparkApplicationDeletion.
		stored := &v1beta2.SparkApplication{}
		Expect(k8sClient.Get(ctx, req.NamespacedName, stored)).To(Succeed())
		stored.Finalizers = append(stored.Finalizers, pdbTestKeepAliveFinalizer)
		Expect(k8sClient.Update(ctx, stored)).To(Succeed())

		// Mark the app for deletion. handleSparkApplicationDeletion will
		// run deleteSparkResources, which calls deleteDriverPDB.
		Expect(k8sClient.Delete(ctx, stored)).To(Succeed())

		// Re-reconcile to drive the deletion handler.
		_, err = r.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, pdbKey(app), got))).To(BeTrue())

		// Remove the finalizer so the object can finally be cleaned up.
		final := &v1beta2.SparkApplication{}
		Expect(k8sClient.Get(ctx, req.NamespacedName, final)).To(Succeed())
		final.Finalizers = nil
		Expect(k8sClient.Update(ctx, final)).To(Succeed())
	})

	It("returns success when the PDB does not exist (NotFound is treated as success)", func() {
		app := newPDBApp("pdb-delete-missing", ptr.To(true))
		stageSubmittedApp(ctx, app)
		defer deleteApp(ctx, app)

		// Add a finalizer before delete so the dispatcher actually routes
		// the next reconcile through handleSparkApplicationDeletion.
		stored := &v1beta2.SparkApplication{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: app.Name, Namespace: app.Namespace}, stored)).To(Succeed())
		stored.Finalizers = append(stored.Finalizers, pdbTestKeepAliveFinalizer)
		Expect(k8sClient.Update(ctx, stored)).To(Succeed())

		// Delete the app so deleteSparkResources runs on the next reconcile,
		// but no PDB has been created yet - exercises deleteDriverPDB's
		// NotFound branch.
		Expect(k8sClient.Delete(ctx, stored)).To(Succeed())

		r := newPDBReconciler(true)
		Expect(reconcileApp(ctx, r, app)).To(Succeed())

		// Remove the finalizer so the object can finally be cleaned up.
		final := &v1beta2.SparkApplication{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: app.Name, Namespace: app.Namespace}, final)).To(Succeed())
		final.Finalizers = nil
		Expect(k8sClient.Update(ctx, final)).To(Succeed())
	})

	It("removes the PDB when the app reaches a terminal state without being deleted", func() {
		// Reproduces the operator-drained case: a PDB exists from the
		// running phase, the operator misses the Succeeding -> Completed
		// transition, then a later reconcile of the already-Completed app
		// must still clean up the PDB. cleanUpOnTermination calls
		// deleteDriverPDB on every reconcile of a terminal app.
		app := newPDBApp("pdb-terminal-cleanup", ptr.To(true))
		stageSubmittedApp(ctx, app)
		defer deleteApp(ctx, app)

		r := newPDBReconciler(true)
		req := reconcile.Request{NamespacedName: types.NamespacedName{Name: app.Name, Namespace: app.Namespace}}

		// First reconcile creates the PDB through the Submitted-state ensure path.
		_, err := r.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		got := &policyv1.PodDisruptionBudget{}
		Expect(k8sClient.Get(ctx, pdbKey(app), got)).To(Succeed())

		// Force the app to Completed directly, simulating the operator
		// having missed the Succeeding -> Completed transition while the
		// PDB still exists in the cluster.
		stored := &v1beta2.SparkApplication{}
		Expect(k8sClient.Get(ctx, req.NamespacedName, stored)).To(Succeed())
		stored.Status.AppState.State = v1beta2.ApplicationStateCompleted
		Expect(k8sClient.Status().Update(ctx, stored)).To(Succeed())

		// Reconciling a Completed app must drive cleanUpOnTermination,
		// which removes the now-stale PDB.
		_, err = r.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, pdbKey(app), got))).To(BeTrue())
	})
})

// newFailingCreateClient returns a client that fails the first N Create
// calls for objects of the same GVK as objType, then delegates to a real
// client for every subsequent Create. This simulates a transient API
// server failure for one specific resource type without affecting other
// writes the reconciler performs in the same pass.
//
// The interceptor wraps client.WithWatch (not the plain client.Client that
// envtest hands tests via k8sClient), so we build a fresh WithWatch from
// the package-level cfg here rather than reusing k8sClient directly.
func newFailingCreateClient(objType client.Object, failures int) client.Client {
	base, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
	Expect(err).NotTo(HaveOccurred())
	targetGVK, err := base.GroupVersionKindFor(objType)
	Expect(err).NotTo(HaveOccurred())
	remaining := failures
	return interceptor.NewClient(base, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if remaining > 0 && objectGVK(c, obj) == targetGVK {
				remaining--
				return fmt.Errorf("simulated transient create failure for %s", targetGVK.Kind)
			}
			return c.Create(ctx, obj, opts...)
		},
	})
}

// objectGVK resolves obj's GroupVersionKind via the client's scheme.
// Returns the zero value if the lookup fails or yields no kinds.
func objectGVK(c client.Client, obj client.Object) schema.GroupVersionKind {
	gvks, _, err := c.Scheme().ObjectKinds(obj)
	if err != nil || len(gvks) == 0 {
		return schema.GroupVersionKind{}
	}
	return gvks[0]
}
