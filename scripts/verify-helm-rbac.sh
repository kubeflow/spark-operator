#!/usr/bin/env bash
#
# verify-helm-rbac.sh — End-to-end verification of tightened Helm RBAC.
#
# Usage:
#   ./scripts/verify-helm-rbac.sh          # full run (creates Kind cluster)
#   ./scripts/verify-helm-rbac.sh --skip-setup  # reuse existing cluster
#
# Prerequisites: docker, kind, helm, kubectl, go
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

SKIP_SETUP=false
[[ "${1:-}" == "--skip-setup" ]] && SKIP_SETUP=true

NAMESPACE="spark-operator"
CONTROLLER_SA="spark-operator-controller"
WEBHOOK_SA="spark-operator-webhook"
PASS=0
FAIL=0
RESULTS=()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
record() {
  local status="$1" msg="$2"
  if [[ "$status" == "PASS" ]]; then
    ((PASS++))
    RESULTS+=("[PASS] $msg")
  else
    ((FAIL++))
    RESULTS+=("[FAIL] $msg")
  fi
  echo "  [$status] $msg"
}

check_can_i() {
  local sa="$1" verb="$2" resource="$3" expected="$4" ns="${5:-default}"
  local result
  result=$(kubectl auth can-i "$verb" "$resource" \
    --as="system:serviceaccount:${NAMESPACE}:${sa}" \
    -n "$ns" 2>&1 || true)
  if [[ "$expected" == "yes" && "$result" == "yes" ]]; then
    record "PASS" "$sa CAN $verb $resource"
  elif [[ "$expected" == "no" && "$result" == "no" ]]; then
    record "PASS" "$sa CANNOT $verb $resource (removed)"
  else
    record "FAIL" "$sa $verb $resource — expected=$expected got=$result"
  fi
}

cleanup() {
  echo ""
  echo "Cleaning up test resources..."
  kubectl delete sparkapplication spark-pi-pvc-test -n default --ignore-not-found 2>/dev/null || true
  kubectl delete scheduledsparkapplication spark-pi-scheduled -n default --ignore-not-found 2>/dev/null || true
  if [[ "$SKIP_SETUP" == "false" ]]; then
    make kind-delete-cluster 2>/dev/null || true
  fi
}
trap cleanup EXIT

# ===================================================================
echo "=========================================="
echo " Helm RBAC Verification"
echo "=========================================="

# ===================================================================
# Phase 1: Static Validation
# ===================================================================
echo ""
echo "--- Phase 1: Static Validation ---"

echo "  Running helm template..."
if helm template test-release charts/spark-operator-chart \
    -f charts/spark-operator-chart/ci/ci-values.yaml > /dev/null 2>&1; then
  record "PASS" "Static: helm template"
else
  record "FAIL" "Static: helm template"
fi

echo "  Running helm lint..."
if helm lint charts/spark-operator-chart \
    -f charts/spark-operator-chart/ci/ci-values.yaml > /dev/null 2>&1; then
  record "PASS" "Static: helm lint"
else
  record "FAIL" "Static: helm lint"
fi

echo "  Running helm-unittest..."
if make helm-unittest > /dev/null 2>&1; then
  record "PASS" "Static: helm-unittest"
else
  record "FAIL" "Static: helm-unittest"
fi

# ===================================================================
# Phase 2: Cluster Setup
# ===================================================================
echo ""
echo "--- Phase 2: Cluster Setup ---"

if [[ "$SKIP_SETUP" == "true" ]]; then
  echo "  Skipping setup (--skip-setup). Using existing cluster."
else
  echo "  Creating Kind cluster..."
  make kind-create-cluster 2>&1 | tail -3

  echo "  Building operator image..."
  make docker-build IMAGE_TAG=local 2>&1 | tail -3

  echo "  Loading image into Kind..."
  make kind-load-image IMAGE_TAG=local 2>&1 | tail -3

  echo "  Deploying operator with tightened chart..."
  make deploy 2>&1 | tail -3
fi

echo "  Waiting for controller deployment to be ready..."
kubectl rollout status deployment/"${CONTROLLER_SA}" -n "$NAMESPACE" --timeout=120s 2>&1 | tail -1
echo "  Waiting for webhook deployment to be ready..."
kubectl rollout status deployment/spark-operator-webhook -n "$NAMESPACE" --timeout=120s 2>&1 | tail -1
record "PASS" "Cluster: operator deployed and ready"

# ===================================================================
# Phase 3: Permission Audit
# ===================================================================
echo ""
echo "--- Phase 3: Permission Audit ---"

echo "  Controller SA — removed permissions (should be 'no'):"
check_can_i "$CONTROLLER_SA" patch    pods                                                          no
check_can_i "$CONTROLLER_SA" deletecollection pods                                                  no
check_can_i "$CONTROLLER_SA" delete   configmaps                                                    no
check_can_i "$CONTROLLER_SA" get      persistentvolumeclaims                                        no
check_can_i "$CONTROLLER_SA" get      nodes                                                         no
check_can_i "$CONTROLLER_SA" list     ingresses.networking.k8s.io                                   no
check_can_i "$CONTROLLER_SA" update   sparkapplications.sparkoperator.k8s.io                        no
check_can_i "$CONTROLLER_SA" create   scheduledsparkapplications.sparkoperator.k8s.io               no
check_can_i "$CONTROLLER_SA" get      sparkapplications/status.sparkoperator.k8s.io                 no
check_can_i "$CONTROLLER_SA" patch    sparkapplications/status.sparkoperator.k8s.io                 no
check_can_i "$CONTROLLER_SA" patch    sparkapplications/finalizers.sparkoperator.k8s.io             no
check_can_i "$CONTROLLER_SA" patch    scheduledsparkapplications/finalizers.sparkoperator.k8s.io    no

echo ""
echo "  Controller SA — kept permissions (should be 'yes'):"
check_can_i "$CONTROLLER_SA" delete   pods                                                          yes
check_can_i "$CONTROLLER_SA" create   pods                                                          yes
check_can_i "$CONTROLLER_SA" patch    configmaps                                                    yes
check_can_i "$CONTROLLER_SA" update   services                                                      yes
check_can_i "$CONTROLLER_SA" patch    services                                                      yes
check_can_i "$CONTROLLER_SA" delete   services                                                      yes
check_can_i "$CONTROLLER_SA" get      ingresses.networking.k8s.io                                   yes
check_can_i "$CONTROLLER_SA" create   sparkapplications.sparkoperator.k8s.io                        yes
check_can_i "$CONTROLLER_SA" delete   sparkapplications.sparkoperator.k8s.io                        yes
check_can_i "$CONTROLLER_SA" get      scheduledsparkapplications.sparkoperator.k8s.io               yes
check_can_i "$CONTROLLER_SA" get      sparkconnects.sparkoperator.k8s.io                            yes
check_can_i "$CONTROLLER_SA" update   sparkapplications/status.sparkoperator.k8s.io                 yes
check_can_i "$CONTROLLER_SA" update   sparkapplications/finalizers.sparkoperator.k8s.io             yes
check_can_i "$CONTROLLER_SA" update   scheduledsparkapplications/finalizers.sparkoperator.k8s.io    yes
check_can_i "$CONTROLLER_SA" update   sparkconnects/finalizers.sparkoperator.k8s.io                 yes
check_can_i "$CONTROLLER_SA" update   scheduledsparkapplications/status.sparkoperator.k8s.io        yes
check_can_i "$CONTROLLER_SA" update   sparkconnects/status.sparkoperator.k8s.io                     yes

echo ""
echo "  Webhook SA — removed permissions (should be 'no'):"
check_can_i "$WEBHOOK_SA" get   pods                                               no
check_can_i "$WEBHOOK_SA" get   resourcequotas                                     no
check_can_i "$WEBHOOK_SA" watch resourcequotas                                     no
check_can_i "$WEBHOOK_SA" update sparkapplications.sparkoperator.k8s.io            no
check_can_i "$WEBHOOK_SA" delete sparkapplications.sparkoperator.k8s.io            no

echo ""
echo "  Webhook SA — kept permissions (should be 'yes'):"
check_can_i "$WEBHOOK_SA" list  resourcequotas                                     yes
check_can_i "$WEBHOOK_SA" get   sparkapplications.sparkoperator.k8s.io             yes
check_can_i "$WEBHOOK_SA" list  sparkapplications.sparkoperator.k8s.io             yes
check_can_i "$WEBHOOK_SA" watch sparkapplications.sparkoperator.k8s.io             yes

# ===================================================================
# Phase 4: Functional Tests
# ===================================================================
echo ""
echo "--- Phase 4: Functional Tests ---"

# 4a. Full e2e suite
echo "  Running full e2e suite (this may take several minutes)..."
if make e2e-test 2>&1 | tee /tmp/e2e-output.log | tail -5; then
  if grep -q "FAIL" /tmp/e2e-output.log 2>/dev/null && ! grep -q "Ran.*Passed" /tmp/e2e-output.log; then
    record "FAIL" "E2e suite"
  else
    record "PASS" "E2e suite"
  fi
else
  record "FAIL" "E2e suite"
fi

# 4b. PVC removal proof
echo ""
echo "  Running PVC removal test..."
cat <<'EOF' | kubectl apply -f -
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: spark-pi-pvc-test
  namespace: default
spec:
  type: Scala
  mode: cluster
  image: docker.io/library/spark:4.0.1
  imagePullPolicy: IfNotPresent
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: local:///opt/spark/examples/jars/spark-examples.jar
  sparkVersion: 4.0.1
  restartPolicy:
    type: Never
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: test-pvc-does-not-exist
  driver:
    cores: 1
    memory: 512m
    serviceAccount: spark-operator-spark
    volumeMounts:
    - name: data
      mountPath: /data
    securityContext:
      capabilities:
        drop: ["ALL"]
      runAsGroup: 185
      runAsUser: 185
      runAsNonRoot: true
      allowPrivilegeEscalation: false
      seccompProfile:
        type: RuntimeDefault
  executor:
    instances: 1
    cores: 1
    memory: 512m
    securityContext:
      capabilities:
        drop: ["ALL"]
      runAsGroup: 185
      runAsUser: 185
      runAsNonRoot: true
      allowPrivilegeEscalation: false
      seccompProfile:
        type: RuntimeDefault
EOF

echo "  Waiting for controller to process PVC app (30s)..."
sleep 30

PVC_STATE=$(kubectl get sparkapplication spark-pi-pvc-test -n default -o jsonpath='{.status.applicationState.state}' 2>/dev/null || echo "UNKNOWN")
echo "  PVC app state: $PVC_STATE"

CONTROLLER_POD=$(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/component=controller -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
PVC_FORBIDDEN=$(kubectl logs "$CONTROLLER_POD" -n "$NAMESPACE" 2>/dev/null | grep -ci "forbidden.*persistentvolumeclaim\|persistentvolumeclaim.*forbidden" || true)

if [[ "$PVC_FORBIDDEN" -eq 0 ]]; then
  record "PASS" "PVC test: controller processed app without PVC API access (state=$PVC_STATE)"
else
  record "FAIL" "PVC test: found $PVC_FORBIDDEN PVC-related Forbidden errors in controller logs"
fi

kubectl delete sparkapplication spark-pi-pvc-test -n default --ignore-not-found 2>/dev/null || true

# 4c. ScheduledSparkApplication test
echo ""
echo "  Running ScheduledSparkApplication test..."
kubectl apply -f examples/spark-pi-scheduled.yaml

echo "  Waiting for scheduled app to create a child SparkApplication (up to 4m)..."
CHILD_FOUND=false
for i in $(seq 1 24); do
  CHILD_COUNT=$(kubectl get sparkapplication -n default -l scheduledsparkapp-name=spark-pi-scheduled --no-headers 2>/dev/null | wc -l | tr -d ' ')
  if [[ "$CHILD_COUNT" -gt 0 ]]; then
    CHILD_FOUND=true
    break
  fi
  sleep 10
done

if [[ "$CHILD_FOUND" == "true" ]]; then
  record "PASS" "ScheduledSparkApp: child SparkApplication created (finalizer + status permissions work)"
else
  record "FAIL" "ScheduledSparkApp: no child SparkApplication created within 4 minutes"
fi

kubectl delete scheduledsparkapplication spark-pi-scheduled -n default --ignore-not-found 2>/dev/null || true

# ===================================================================
# Phase 5: Log Scan
# ===================================================================
echo ""
echo "--- Phase 5: Log Scan ---"

CONTROLLER_POD=$(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/component=controller -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
WEBHOOK_POD=$(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/component=webhook -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

CTRL_FORBIDDEN=0
WEBHOOK_FORBIDDEN=0

if [[ -n "$CONTROLLER_POD" ]]; then
  CTRL_LOGS=$(kubectl logs "$CONTROLLER_POD" -n "$NAMESPACE" 2>/dev/null || echo "")
  CTRL_FORBIDDEN=$(echo "$CTRL_LOGS" | grep -ci "forbidden\|\"code\":403\|cannot.*verb" || true)
  if [[ "$CTRL_FORBIDDEN" -eq 0 ]]; then
    record "PASS" "Log scan: no RBAC errors in controller logs"
  else
    record "FAIL" "Log scan: found $CTRL_FORBIDDEN potential RBAC errors in controller logs"
    echo "    Matching lines:"
    echo "$CTRL_LOGS" | grep -i "forbidden\|\"code\":403\|cannot.*verb" | head -10 | sed 's/^/      /'
  fi
else
  record "FAIL" "Log scan: controller pod not found"
fi

if [[ -n "$WEBHOOK_POD" ]]; then
  WH_LOGS=$(kubectl logs "$WEBHOOK_POD" -n "$NAMESPACE" 2>/dev/null || echo "")
  WEBHOOK_FORBIDDEN=$(echo "$WH_LOGS" | grep -ci "forbidden\|\"code\":403\|cannot.*verb" || true)
  if [[ "$WEBHOOK_FORBIDDEN" -eq 0 ]]; then
    record "PASS" "Log scan: no RBAC errors in webhook logs"
  else
    record "FAIL" "Log scan: found $WEBHOOK_FORBIDDEN potential RBAC errors in webhook logs"
    echo "    Matching lines:"
    echo "$WH_LOGS" | grep -i "forbidden\|\"code\":403\|cannot.*verb" | head -10 | sed 's/^/      /'
  fi
else
  record "FAIL" "Log scan: webhook pod not found"
fi

# ===================================================================
# Phase 6: Report
# ===================================================================
echo ""
echo "=========================================="
echo " Helm RBAC Verification Report"
echo "=========================================="
for r in "${RESULTS[@]}"; do
  echo "  $r"
done
echo "=========================================="
echo "  Total: $((PASS + FAIL)) checks — $PASS passed, $FAIL failed"
echo "=========================================="

if [[ "$FAIL" -gt 0 ]]; then
  echo "  Result: FAILED"
  exit 1
else
  echo "  Result: ALL PASSED"
  exit 0
fi
