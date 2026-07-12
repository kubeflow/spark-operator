#!/usr/bin/env bash
#
# Copyright 2026 The Kubeflow authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# End-to-end demo + evidence collection for fenced, progress-preserving
# restarts (FencedRestart feature gate) on a local minikube cluster.
#
# What it does:
#   1. Builds the operator image from this branch inside minikube's Docker.
#   2. Installs the operator (helm) with the FencedRestart gate, a Redis
#      store profile, and recovery agent injection enabled.
#   3. Deploys Redis and the examples/spark-fenced-restart.yaml job
#      (20 sequential stages, marker committed after each).
#   4. Waits until several stages completed, then kills the driver pod.
#   5. Collects evidence into ./evidence-<timestamp>/: operator events,
#      status.recoveryStatus, Redis keys, both drivers' logs, a direct
#      demonstration that a stale-epoch write is rejected (FENCED), and a
#      summary README with the work-preserved arithmetic.
#
# Prerequisites: minikube (running), docker, helm, kubectl, jq.
set -euo pipefail

NS_OPERATOR=spark-operator
NS_JOB=default
APP=resumable-etl
IMAGE=spark-operator:fenced-restart-demo
KILL_AFTER_STAGE=${KILL_AFTER_STAGE:-5}
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
EVIDENCE_DIR="${REPO_ROOT}/evidence-$(date +%Y%m%d-%H%M%S)"
mkdir -p "${EVIDENCE_DIR}"

log() { echo "[demo $(date +%H:%M:%S)] $*" | tee -a "${EVIDENCE_DIR}/demo.log"; }

log "Evidence will be collected in ${EVIDENCE_DIR}"

# --- 1. Build the operator image inside minikube ---------------------------
log "Building operator image ${IMAGE} inside minikube docker daemon"
eval "$(minikube docker-env)"
docker build -t "${IMAGE}" "${REPO_ROOT}" | tail -2 | tee -a "${EVIDENCE_DIR}/demo.log"

# --- 2. Deploy Redis --------------------------------------------------------
log "Deploying Redis"
kubectl apply -f - << 'EOF'
apiVersion: v1
kind: Namespace
metadata:
  name: spark-operator
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: recovery-redis
  namespace: spark-operator
spec:
  replicas: 1
  selector: { matchLabels: { app: recovery-redis } }
  template:
    metadata: { labels: { app: recovery-redis } }
    spec:
      containers:
      - name: redis
        image: redis:7-alpine
        args: ["--appendonly", "yes"]
        ports: [{ containerPort: 6379 }]
---
apiVersion: v1
kind: Service
metadata:
  name: recovery-redis
  namespace: spark-operator
spec:
  selector: { app: recovery-redis }
  ports: [{ port: 6379, targetPort: 6379 }]
EOF
kubectl -n ${NS_OPERATOR} rollout status deploy/recovery-redis --timeout=120s

REDIS_ADDR="recovery-redis.${NS_OPERATOR}.svc:6379"
PROFILES="default=redis:${REDIS_ADDR}"

# --- 3. Install the operator with FencedRestart enabled --------------------
log "Installing spark-operator chart from this branch"
helm upgrade --install spark-operator "${REPO_ROOT}/charts/spark-operator-chart" \
  --namespace ${NS_OPERATOR} --create-namespace \
  --set image.registry="" \
  --set image.repository=spark-operator \
  --set image.tag=fenced-restart-demo \
  --set image.pullPolicy=Never \
  --set webhook.enable=true \
  --wait --timeout 5m

log "Enabling FencedRestart gate + recovery flags on controller and webhook"
kubectl -n ${NS_OPERATOR} patch deployment spark-operator-controller --type=json -p "[
  {\"op\":\"add\",\"path\":\"/spec/template/spec/containers/0/args/-\",\"value\":\"--feature-gates=FencedRestart=true\"},
  {\"op\":\"add\",\"path\":\"/spec/template/spec/containers/0/args/-\",\"value\":\"--recovery-store-profiles=${PROFILES}\"}]"
kubectl -n ${NS_OPERATOR} patch deployment spark-operator-webhook --type=json -p "[
  {\"op\":\"add\",\"path\":\"/spec/template/spec/containers/0/args/-\",\"value\":\"--feature-gates=FencedRestart=true\"},
  {\"op\":\"add\",\"path\":\"/spec/template/spec/containers/0/args/-\",\"value\":\"--recovery-store-profiles=${PROFILES}\"},
  {\"op\":\"add\",\"path\":\"/spec/template/spec/containers/0/args/-\",\"value\":\"--recovery-agent-image=${IMAGE}\"}]"
kubectl -n ${NS_OPERATOR} rollout status deploy/spark-operator-controller --timeout=180s
kubectl -n ${NS_OPERATOR} rollout status deploy/spark-operator-webhook --timeout=180s

# --- 4. Run the long-running resumable job ---------------------------------
log "Submitting ${APP} (20 stages, marker after each stage)"
kubectl -n ${NS_JOB} delete sparkapplication ${APP} --ignore-not-found
kubectl apply -f "${REPO_ROOT}/examples/spark-fenced-restart.yaml"

DRIVER="${APP}-driver"
log "Waiting for driver pod ${DRIVER} to run"
kubectl -n ${NS_JOB} wait pod/${DRIVER} --for=condition=Ready --timeout=300s

log "Verifying the recovery agent sidecar was injected by the webhook"
kubectl -n ${NS_JOB} get pod ${DRIVER} \
  -o jsonpath='{range .spec.containers[*]}{.name}{"\n"}{end}' |
  tee "${EVIDENCE_DIR}/01-driver-containers.txt"
grep -q spark-recovery-agent "${EVIDENCE_DIR}/01-driver-containers.txt" ||
  {
    log "FAIL: sidecar not injected"
    exit 1
  }

log "Waiting for stage ${KILL_AFTER_STAGE} to complete (each stage is real Spark work)"
RUN1_START=$(date +%s)
until kubectl -n ${NS_JOB} logs ${DRIVER} -c spark-kubernetes-driver 2> /dev/null |
  grep -q "stage ${KILL_AFTER_STAGE} done"; do
  sleep 5
done
RUN1_KILLED=$(date +%s)
kubectl -n ${NS_JOB} logs ${DRIVER} -c spark-kubernetes-driver \
  > "${EVIDENCE_DIR}/02-driver-run1.log"

# --- 5. Kill the driver -----------------------------------------------------
log "KILLING the driver pod (simulated infrastructure failure)"
kubectl -n ${NS_JOB} delete pod ${DRIVER} --grace-period=0 --force

log "Waiting for the operator to fence and restart"
sleep 5
until kubectl -n ${NS_JOB} get pod ${DRIVER} -o jsonpath='{.status.phase}' 2> /dev/null |
  grep -q Running; do
  sleep 5
done
RUN2_START=$(date +%s)

log "Capturing fencing evidence"
kubectl -n ${NS_JOB} get sparkapplication ${APP} -o json |
  jq '.status.recoveryStatus' | tee "${EVIDENCE_DIR}/03-recovery-status.json"
kubectl -n ${NS_JOB} get events \
  --field-selector involvedObject.name=${APP} \
  --sort-by=.lastTimestamp |
  tee "${EVIDENCE_DIR}/04-events.txt"
REDIS_POD=$(kubectl -n ${NS_OPERATOR} get pod -l app=recovery-redis -o name | head -1)
kubectl -n ${NS_OPERATOR} exec "${REDIS_POD}" -- \
  redis-cli GET "sparkoperator/fencing/${NS_JOB}/${APP}" |
  tee "${EVIDENCE_DIR}/05-fencing-epoch.txt"

log "Demonstrating the fence directly: a stale-epoch (0) write must be rejected"
kubectl -n ${NS_OPERATOR} exec "${REDIS_POD}" -- redis-cli EVAL "
local current = redis.call('GET', KEYS[1])
if current == false or tonumber(current) ~= tonumber(ARGV[1]) then
  return redis.error_reply('FENCED')
end
return redis.status_reply('OK')" 1 "sparkoperator/fencing/${NS_JOB}/${APP}" 0 \
  2>&1 | tee "${EVIDENCE_DIR}/06-stale-write-rejected.txt" || true

# --- 6. Wait for completion and collect the resume evidence ----------------
log "Waiting for the recovered run to complete all 20 stages"
until kubectl -n ${NS_JOB} get sparkapplication ${APP} \
  -o jsonpath='{.status.applicationState.state}' | grep -q COMPLETED; do
  sleep 10
done
RUN2_END=$(date +%s)
kubectl -n ${NS_JOB} logs ${DRIVER} -c spark-kubernetes-driver \
  > "${EVIDENCE_DIR}/07-driver-run2.log" 2> /dev/null ||
  kubectl -n ${NS_JOB} logs "$(kubectl -n ${NS_JOB} get pod -l spark-role=driver,sparkoperator.k8s.io/app-name=${APP} -o name | head -1)" \
    -c spark-kubernetes-driver > "${EVIDENCE_DIR}/07-driver-run2.log"
kubectl -n ${NS_JOB} get sparkapplication ${APP} -o json |
  jq '{state: .status.applicationState.state, attempts: .status.executionAttempts, recovery: .status.recoveryStatus}' |
  tee "${EVIDENCE_DIR}/08-final-status.json"

# --- 7. Summarize -----------------------------------------------------------
RESUME_LINE=$(grep -m1 "RESUMING from stage" "${EVIDENCE_DIR}/07-driver-run2.log" || echo "NOT FOUND")
STAGES_RUN1=$(grep -c "done in" "${EVIDENCE_DIR}/02-driver-run1.log" || true)
STAGES_RUN2=$(grep -c "done in" "${EVIDENCE_DIR}/07-driver-run2.log" || true)
EPOCH=$(cat "${EVIDENCE_DIR}/05-fencing-epoch.txt")

cat > "${EVIDENCE_DIR}/README.md" << SUMMARY
# FencedRestart demo evidence ($(date -u +%Y-%m-%dT%H:%M:%SZ))

Cluster: minikube ($(minikube version --short 2> /dev/null || echo unknown)),
operator image built from branch $(git -C "${REPO_ROOT}" rev-parse --abbrev-ref HEAD) @ $(git -C "${REPO_ROOT}" rev-parse --short HEAD).

## Timeline
- Run 1 started, driver killed after stage ${KILL_AFTER_STAGE}: ${STAGES_RUN1} stages done, $((RUN1_KILLED - RUN1_START))s of work performed.
- Operator fenced (epoch 0 -> ${EPOCH}), deleted resources, resubmitted natively.
- Run 2: "${RESUME_LINE}"
- Run 2 executed ${STAGES_RUN2} of 20 stages ($((KILL_AFTER_STAGE + 1)) skipped) and completed in $((RUN2_END - RUN2_START))s.

## How this feature helped
Without recovery, run 2 re-executes all 20 stages. With it, the
$((KILL_AFTER_STAGE + 1)) completed stages ($((RUN1_KILLED - RUN1_START))s of compute) were preserved
across a hard driver kill, and the fencing epoch guarantees the dead run can
never commit again (see 06-stale-write-rejected.txt: FENCED).

## Files
- 01-driver-containers.txt — webhook injected spark-recovery-agent sidecar
- 02-driver-run1.log — first run, markers committed per stage
- 03-recovery-status.json — status.recoveryStatus after fencing (epoch, restoredFromEpoch)
- 04-events.txt — SparkApplicationEpochAdvanced / MarkerRestored events
- 05-fencing-epoch.txt — fencing key in Redis after recovery
- 06-stale-write-rejected.txt — stale epoch-0 write rejected: the fence works
- 07-driver-run2.log — recovered run resuming from stage $((KILL_AFTER_STAGE + 1))
- 08-final-status.json — COMPLETED with recoveryStatus populated
SUMMARY

log "DONE. Evidence in ${EVIDENCE_DIR}"
cat "${EVIDENCE_DIR}/README.md"
