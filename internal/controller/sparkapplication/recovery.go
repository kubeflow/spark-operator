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
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/features"
	"github.com/kubeflow/spark-operator/v2/pkg/statestore"
)

// recoveryEnabled reports whether fenced, progress-preserving restarts apply
// to the given application.
func recoveryEnabled(app *v1beta2.SparkApplication) bool {
	return features.Enabled(features.FencedRestart) &&
		app.Spec.RestartPolicy.Recovery != nil
}

// prepareRecoveryForSubmission runs before every submission of a
// recovery-enabled application and performs the fencing protocol:
//
//   - First submission: ensure the fencing epoch exists (created at 0) and
//     record it in status.
//   - Rerun: atomically advance the epoch — from this instant, any zombie of
//     the previous driver is fenced and can never commit again — then locate
//     the latest committed progress marker to hand to the new driver.
//
// The order is the correctness core: fence -> (native) delete -> (native)
// resubmit. Deleting the old driver pod remains a cleanup step, not a
// correctness step; safety never depends on the old driver being gone, only
// on it being fenced.
//
// Errors freeze the transition (the reconcile is retried); a rerun must
// never be submitted unfenced.
func (r *Reconciler) prepareRecoveryForSubmission(ctx context.Context, app *v1beta2.SparkApplication, isRerun bool) error {
	if !recoveryEnabled(app) {
		return nil
	}
	logger := log.FromContext(ctx)

	if r.options.RecoveryStoreProfiles == nil {
		return fmt.Errorf("FencedRestart is enabled for SparkApplication %s/%s but no --recovery-store-profiles were configured", app.Namespace, app.Name)
	}
	store, err := r.options.RecoveryStoreProfiles.Store(app.Spec.RestartPolicy.Recovery.StoreProfile)
	if err != nil {
		r.recordRecoveryStoreError(app, err)
		return err
	}

	key := statestore.JobKey{Namespace: app.Namespace, Name: app.Name}

	if !isRerun {
		epoch, err := store.CurrentEpoch(ctx, key)
		if err != nil {
			r.recordRecoveryStoreError(app, err)
			return fmt.Errorf("failed to initialize fencing epoch: %w", err)
		}
		app.Status.RecoveryStatus = &v1beta2.RecoveryStatus{Epoch: epoch}
		logger.Info("Initialized fencing epoch for SparkApplication", "epoch", epoch)
		return nil
	}

	epoch, err := store.AdvanceEpoch(ctx, key)
	if err != nil {
		r.recordRecoveryStoreError(app, err)
		return fmt.Errorf("failed to advance fencing epoch: %w", err)
	}
	recoveryStatus := &v1beta2.RecoveryStatus{Epoch: epoch}
	r.recorder.Eventf(app, nil, corev1.EventTypeNormal,
		common.EventSparkApplicationEpochAdvanced, "Fencing",
		"Advanced fencing epoch to %d; previous driver can no longer commit state", epoch)

	snapshot, err := store.LatestSnapshot(ctx, key, epoch)
	switch {
	case err == nil:
		recoveryStatus.RestoredFromEpoch = &snapshot.Epoch
		r.recorder.Eventf(app, nil, corev1.EventTypeNormal,
			common.EventSparkApplicationMarkerRestored, "Restoring",
			"Restarting from progress marker committed at epoch %d (%d bytes)", snapshot.Epoch, len(snapshot.Data))
	case errors.Is(err, statestore.ErrNotFound):
		// Fenced restart-from-zero. Surface it loudly: protection the user
		// believes in but never receives is worse than no feature at all.
		r.recorder.Eventf(app, nil, corev1.EventTypeWarning,
			common.EventSparkApplicationNoMarkerObserved, "Restoring",
			"No progress marker was ever committed; restarting from zero (fenced). "+
				"Integrate progress reporting via the recovery agent to preserve work across restarts")
	default:
		r.recordRecoveryStoreError(app, err)
		return fmt.Errorf("failed to look up latest progress marker: %w", err)
	}

	app.Status.RecoveryStatus = recoveryStatus
	logger.Info("Fenced SparkApplication for rerun", "epoch", epoch, "restoredFromEpoch", recoveryStatus.RestoredFromEpoch)
	return nil
}

func (r *Reconciler) recordRecoveryStoreError(app *v1beta2.SparkApplication, err error) {
	r.recorder.Eventf(app, nil, corev1.EventTypeWarning,
		common.EventSparkApplicationRecoveryStoreError, "Fencing",
		"Recovery state store operation failed: %v", err)
}
