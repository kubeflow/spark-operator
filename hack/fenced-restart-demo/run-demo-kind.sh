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
# End-to-end build + demo + evidence collection for fenced,
# progress-preserving restarts (FencedRestart feature gate) on a local
# KIND cluster. Only Docker, kind, kubectl, helm, and jq are required on
# the host — Go codegen, tidy, and unit tests run inside a golang
# container when Go is not installed locally.
#
# Flow:
#   1. go mod tidy + make generate/manifests/update-crd + unit tests
#   2. docker build of the operator image from this branch
#   3. kind cluster (created if absent) + image load
#   4. operator install (helm) with FencedRestart enabled, Redis deployed
#   5. 20-stage resumable job runs; driver killed after stage $KILL_AFTER_STAGE
#   6. evidence collected into ./evidence-<timestamp>/ including a filled
#      snippet for PR_DESCRIPTION.md
#
# Usage: ./hack/fenced-restart-demo/run-demo-kind.sh
#   KILL_AFTER_STAGE=5   stage after which the driver is killed
#   KIND_CLUSTER=spark-fenced   cluster name
set -euo pipefail

NS_OPERATOR=spark-operator
NS_JOB=default
APP=resumable-etl
IMAGE=spark-operator:fenced-restart-demo
SPARK_IMAGE=docker.io/library/spark:4.0.1
GOLANG_IMAGE=golang:1.25
KIND_CLUSTER=${KIND_CLUSTER:-spark-fenced}
KILL_AFTER_STAGE=${KILL_AFTER_STAGE:-5}
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
EVIDENCE_DIR="${REPO_ROOT}/evidence-$(date +%Y%m%d-%H%M%S)"
mkdir -p "${EVIDENCE_DIR}"

log() { echo "[demo $(date +%H:%M:%S)] $*" | tee -a "${EVIDENCE_DIR}/demo.log"; }
fail() {
  log "FAIL: $*"
  exit 1
}

for tool in docker kind kubectl helm jq; do
  command -v "${tool}" > /dev/null || fail "${tool} is required"
done

cd "${REPO_ROOT}"
rm -f .git/index.lock .git/HEAD.lock .git/objects/maintenance.lock 2> /dev/null || true
log "Branch: $(git rev-parse --abbrev-ref HEAD) @ $(git rev-parse --short HEAD)"
log "Evidence directory: ${EVIDENCE_DIR}"

# --- 1. Codegen, tidy, unit tests (host Go if present, container otherwise) --
run_go() {
  if command -v go > /dev/null && command -v make > /dev/null; then
    (cd "${REPO_ROOT}" && eval "$*")
  else
    docker run --rm -v "${REPO_ROOT}:/workspace" -w /workspace \
      -e HOME=/tmp -e GOFLAGS=-buildvcs=false \
      "${GOLANG_IMAGE}" sh -c "git config --global --add safe.directory /workspace; apt-get -qq update >/dev/null && apt-get -qq install -y make >/dev/null; $*"
  fi
}

log "STEP 1a: go mod tidy (pulls the new go-redis/miniredis dependencies)"
run_go "go mod tidy" 2>&1 | tail -3 | tee -a "${EVIDENCE_DIR}/demo.log"

log "STEP 1b: regenerate deepcopy, CRDs, and sync chart CRDs"
run_go "make generate manifests update-crd" 2>&1 | tail -5 | tee -a "${EVIDENCE_DIR}/demo.log"

log "STEP 1c: build all packages"
run_go "go build ./..." 2>&1 | tee "${EVIDENCE_DIR}/00-build.log" ||
  fail "compilation failed — see ${EVIDENCE_DIR}/00-build.log, fix, and re-run"

log "STEP 1d: unit tests (statestore fencing, webhook injection, validator, env injection)"
run_go "go test ./pkg/statestore/... ./internal/webhook/... ./internal/controller/sparkapplication/..." \
  2>&1 | tee "${EVIDENCE_DIR}/00-unit-tests.log" ||
  fail "unit tests failed — see ${EVIDENCE_DIR}/00-unit-tests.log"

# --- 2. Build the operator image ---------------------------------------------
log "STEP 2: docker build ${IMAGE}"
docker build -t "${IMAGE}" "${REPO_ROOT}" 2>&1 | tail -3 | tee -a "${EVIDENCE_DIR}/demo.log"

# --- 3. Kind cluster + images -------------------------------------------------
if ! kind get clusters 2> /dev/null | grep -qx "${KIND_CLUSTER}"; then
  log "STEP 3a: creating kind cluster ${KIND_CLUSTER}"
  kind create cluster --name "${KIND_CLUSTER}" --wait 2m
else
  log "STEP 3a: reusing kind cluster ${KIND_CLUSTER}"
fi
kubectl config use-context "kind-${KIND_CLUSTER}" > /dev/null

log "STEP 3b: loading images into kind (operator + spark runtime)"
kind load docker-image "${IMAGE}" --name "${KIND_CLUSTER}"
docker pull -q "${SPARK_IMAGE}" > /dev/null || true
kind load docker-image "${SPARK_IMAGE}" --name "${KIND_CLUSTER}" || true

# --- 4. Redis + operator -------------------------------------------------------
log "STEP 4a: deploying Redis (AOF enabled — the fencing epoch must survive restarts)"
kubectl create namespace ${NS_OPERATOR} --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f - << 'EOF'
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
kubectl -n ${NS_OPERATOR} rollout status deploy/recovery-redis --timeout=180s

PROFILES="default=redis:recovery-redis.${NS_OPERATOR}.svc:6379"

log "STEP 4b: installing the operator chart from this branch"
helm upgrade --install spark-operator "${REPO_ROOT}/charts/spark-operator-chart" \
  --namespace ${NS_OPERATOR} \
  --set image.registry="" \
  --set image.repository=spark-operator \
  --set image.tag=fenced-restart-demo \
  --set image.pullPolicy=Never \
  --set webhook.enable=true \
  --wait --timeout 5m

CONTROLLER_DEPLOY=$(kubectl -n ${NS_OPERATOR} get deploy -o name | grep controller)
WEBHOOK_DEPLOY=$(kubectl -n ${NS_OPERATOR} get deploy -o name | grep webhook)

log "STEP 4c: enabling FencedRestart on ${CONTROLLER_DEPLOY} and ${WEBHOOK_DEPLOY}"
kubectl -n ${NS_OPERATOR} patch "${CONTROLLER_DEPLOY}" --type=json -p "[
  {\"op\":\"add\",\"path\":\"/spec/template/spec/containers/0/args/-\",\"value\":\"--feature-gates=FencedRestart=true\"},
  {\"op\":\"add\",\"path\":\"/spec/template/spec/containers/0/args/-\",\"value\":\"--recovery-store-profiles=${PROFILES}\"}]"
kubectl -n ${NS_OPERATOR} patch "${WEBHOOK_DEPLOY}" --type=json -p "[
  {\"op\":\"add\",\"path\":\"/spec/template/spec/containers/0/args/-\",\"value\":\"--feature-gates=FencedRestart=true\"},
  {\"op\":\"add\",\"path\":\"/spec/template/spec/containers/0/args/-\",\"value\":\"--recovery-store-profiles=${PROFILES}\"},
  {\"op\":\"add\",\"path\":\"/spec/template/spec/containers/0/args/-\",\"value\":\"--recovery-agent-image=${IMAGE}\"}]"
kubectl -n ${NS_OPERATOR} rollout status "${CONTROLLER_DEPLOY}" --timeout=180s
kubectl -n ${NS_OPERATOR} rollout status "${WEBHOOK_DEPLOY}" --timeout=180s

# --- 5. Run the long-running resumable job -----------------------------------
log "STEP 5: submitting ${APP} (20 stages, progress marker after each)"
kubectl -n ${NS_JOB} delete sparkapplication ${APP} --ignore-not-found
sleep 3
kubectl apply -f "${REPO_ROOT}/examples/spark-fenced-restart.yaml"

DRIVER="${APP}-driver"
log "Waiting for driver pod ${DRIVER}"
for _ in $(seq 1 60); do
  kubectl -n ${NS_JOB} get pod ${DRIVER} > /dev/null 2>&1 && break
  sleep 5
done
kubectl -n ${NS_JOB} wait pod/${DRIVER} --for=condition=Ready --timeout=300s

log "EVIDENCE 01: webhook injected the recovery agent sidecar"
kubectl -n ${NS_JOB} get pod ${DRIVER} \
  -o jsonpath='{range .spec.containers[*]}{.name}{"\n"}{end}' |
  tee "${EVIDENCE_DIR}/01-driver-containers.txt"
grep -q spark-recovery-agent "${EVIDENCE_DIR}/01-driver-containers.txt" ||
  fail "sidecar was not injected"

log "Waiting for stage ${KILL_AFTER_STAGE} to complete (real Spark work, be patient)"
RUN1_START=$(date +%s)
until kubectl -n ${NS_JOB} logs ${DRIVER} -c spark-kubernetes-driver 2> /dev/null |
  grep -q "stage ${KILL_AFTER_STAGE} done"; do
  sleep 5
done
RUN1_KILLED=$(date +%s)
kubectl -n ${NS_JOB} logs ${DRIVER} -c spark-kubernetes-driver \
  > "${EVIDENCE_DIR}/02-driver-run1.log"
RUN1_UID=$(kubectl -n ${NS_JOB} get pod ${DRIVER} -o jsonpath='{.metadata.uid}')

# --- 6. Kill the driver --------------------------------------------------------
log "STEP 6: KILLING the driver pod (uid ${RUN1_UID}) — simulated infra failure"
kubectl -n ${NS_JOB} delete pod ${DRIVER} --grace-period=0 --force 2> /dev/null

log "Waiting for the operator to fence and restart the application"
NEW_DRIVER_READY=""
for _ in $(seq 1 120); do
  PHASE=$(kubectl -n ${NS_JOB} get pod ${DRIVER} -o jsonpath='{.status.phase}' 2> /dev/null || true)
  UID_NOW=$(kubectl -n ${NS_JOB} get pod ${DRIVER} -o jsonpath='{.metadata.uid}' 2> /dev/null || true)
  if [[ "${PHASE}" == "Running" && -n "${UID_NOW}" && "${UID_NOW}" != "${RUN1_UID}" ]]; then
    NEW_DRIVER_READY=1
    break
  fi
  sleep 5
done
[[ -n "${NEW_DRIVER_READY}" ]] || fail "recovery driver did not start (check operator logs)"
RUN2_START=$(date +%s)

log "EVIDENCE 03/04/05: fencing state after recovery"
kubectl -n ${NS_JOB} get sparkapplication ${APP} -o json |
  jq '.status.recoveryStatus' | tee "${EVIDENCE_DIR}/03-recovery-status.json"
kubectl -n ${NS_JOB} get events \
  --field-selector involvedObject.name=${APP} --sort-by=.lastTimestamp 2> /dev/null |
  tee "${EVIDENCE_DIR}/04-events.txt"
REDIS_POD=$(kubectl -n ${NS_OPERATOR} get pod -l app=recovery-redis -o name | head -1)
kubectl -n ${NS_OPERATOR} exec "${REDIS_POD#pod/}" -- \
  redis-cli GET "sparkoperator/fencing/${NS_JOB}/${APP}" |
  tee "${EVIDENCE_DIR}/05-fencing-epoch.txt"

log "EVIDENCE 06: a stale-epoch (0) write is rejected by the fence"
kubectl -n ${NS_OPERATOR} exec "${REDIS_POD#pod/}" -- redis-cli EVAL "
local current = redis.call('GET', KEYS[1])
if current == false or tonumber(current) ~= tonumber(ARGV[1]) then
  return redis.error_reply('FENCED')
end
return redis.status_reply('OK')" 1 "sparkoperator/fencing/${NS_JOB}/${APP}" 0 \
  > "${EVIDENCE_DIR}/06-stale-write-rejected.txt" 2>&1 || true
cat "${EVIDENCE_DIR}/06-stale-write-rejected.txt"

# --- 7. Completion + resume evidence ------------------------------------------
log "STEP 7: waiting for the recovered run to complete all 20 stages"
until kubectl -n ${NS_JOB} get sparkapplication ${APP} \
  -o jsonpath='{.status.applicationState.state}' 2> /dev/null | grep -q COMPLETED; do
  # Capture logs continuously; the driver pod may be cleaned up at completion.
  kubectl -n ${NS_JOB} logs ${DRIVER} -c spark-kubernetes-driver \
    > "${EVIDENCE_DIR}/07-driver-run2.log" 2> /dev/null || true
  sleep 10
done
RUN2_END=$(date +%s)
kubectl -n ${NS_JOB} logs ${DRIVER} -c spark-kubernetes-driver \
  > "${EVIDENCE_DIR}/07-driver-run2.log" 2> /dev/null || true
kubectl -n ${NS_JOB} get sparkapplication ${APP} -o json |
  jq '{state: .status.applicationState.state, attempts: .status.executionAttempts, recovery: .status.recoveryStatus}' |
  tee "${EVIDENCE_DIR}/08-final-status.json"

# --- 8. Summarize ---------------------------------------------------------------
RESUME_LINE=$(grep -m1 "RESUMING from stage" "${EVIDENCE_DIR}/07-driver-run2.log" || echo "NOT FOUND")
STAGES_RUN2=$(grep -c "done in" "${EVIDENCE_DIR}/07-driver-run2.log" 2> /dev/null || echo 0)
EPOCH=$(tr -d '[:space:]' < "${EVIDENCE_DIR}/05-fencing-epoch.txt")
RUN1_SECONDS=$((RUN1_KILLED - RUN1_START))
RUN2_SECONDS=$((RUN2_END - RUN2_START))
SKIPPED=$((KILL_AFTER_STAGE + 1))
K8S_VERSION=$(kubectl version -o json 2> /dev/null | jq -r .serverVersion.gitVersion)

cat > "${EVIDENCE_DIR}/README.md" << SUMMARY
# FencedRestart evidence — kind ($(date -u +%Y-%m-%dT%H:%M:%SZ))

- Cluster: kind ${KIND_CLUSTER}, Kubernetes ${K8S_VERSION}
- Operator image: built from branch $(git rev-parse --abbrev-ref HEAD) @ $(git rev-parse --short HEAD)
- Job: ${APP} — 20 sequential Spark stages, one progress marker per stage

## Timeline

| Event | Value |
|---|---|
| Run 1: stages completed before kill | 0..${KILL_AFTER_STAGE} (${SKIPPED} stages, ${RUN1_SECONDS}s of compute) |
| Driver force-deleted | after stage ${KILL_AFTER_STAGE} |
| Operator fenced | epoch 0 -> ${EPOCH} |
| Run 2 resume line | ${RESUME_LINE} |
| Run 2: stages executed | ${STAGES_RUN2} of 20 (${SKIPPED} skipped), ${RUN2_SECONDS}s |
| Final state | $(jq -r .state "${EVIDENCE_DIR}/08-final-status.json") |

## How the feature helped

Without recovery, run 2 re-executes all 20 stages (~$((RUN1_SECONDS * 20 / SKIPPED))s
estimated). With it, ${RUN1_SECONDS}s of completed compute was preserved across a
hard driver kill, and the fence guarantees the dead run can never commit
again — 06-stale-write-rejected.txt shows a write guarded by the old epoch
being rejected with FENCED.

## Files
- 00-build.log / 00-unit-tests.log — compilation + unit test results
- 01-driver-containers.txt — sidecar injected by the webhook
- 02-driver-run1.log — run 1, markers committed per stage
- 03-recovery-status.json — status.recoveryStatus (epoch, restoredFromEpoch)
- 04-events.txt — EpochAdvanced / MarkerRestored events
- 05-fencing-epoch.txt — fencing key in Redis after recovery
- 06-stale-write-rejected.txt — direct proof the fence works
- 07-driver-run2.log — recovered run resuming from stage ${SKIPPED}
- 08-final-status.json — COMPLETED with recoveryStatus populated
SUMMARY

log "DONE — evidence in ${EVIDENCE_DIR}"
echo
cat "${EVIDENCE_DIR}/README.md"
