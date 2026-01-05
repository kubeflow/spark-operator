#!/bin/bash
# This test verifies:
#   1. Spark Operator installs successfully from the Helm chart
#   2. fsGroup is NOT 185 (OpenShift security requirement)
#   3. jobNamespaces is configured correctly
#
# Usage:
#   ./test-operator-install.sh           # Install, test, and cleanup
#   CLEANUP=false ./test-operator-install.sh  # Keep operator for subsequent tests
#
# Prerequisites:
#   - kubectl configured with cluster access
#   - helm installed
#
# ============================================================================

set -euo pipefail

# ============================================================================
# Configuration
# ============================================================================
RELEASE_NAME="${RELEASE_NAME:-spark-operator-openshift}"
RELEASE_NAMESPACE="${RELEASE_NAMESPACE:-spark-operator-openshift}"
HELM_REPO_NAME="${HELM_REPO_NAME:-opendatahub-spark-operator}"
HELM_REPO_URL="${HELM_REPO_URL:-https://opendatahub-io.github.io/spark-operator}"
CHART_NAME="${CHART_NAME:-spark-operator}"
TIMEOUT="${TIMEOUT:-5m}"

# Expected jobNamespaces (docling-spark namespace for our tests)
EXPECTED_JOB_NAMESPACE="${EXPECTED_JOB_NAMESPACE:-docling-spark}"

# ============================================================================
# Helper Functions
# ============================================================================
log()  { echo "➡️  $1"; }
pass() { echo "✅ $1"; }
fail() { echo "❌ $1"; exit 1; }
warn() { echo "⚠️  $1"; }

cleanup() {
    # By default, CLEANUP the operator after tests
    # Set CLEANUP=false to keep operator for subsequent tests
    if [ "${CLEANUP:-true}" = "true" ]; then
        log "Cleaning up..."
        helm uninstall "$RELEASE_NAME" -n "$RELEASE_NAMESPACE" --wait 2>/dev/null || true
        kubectl delete namespace "$RELEASE_NAMESPACE" --ignore-not-found --wait=false || true
    else
        log "Keeping operator installed (CLEANUP=false)"
        log "To cleanup manually: helm uninstall $RELEASE_NAME -n $RELEASE_NAMESPACE"
    fi
}

# Cleanup on exit (if CLEANUP=true)
trap cleanup EXIT

# ============================================================================
# Setup: Install Spark Operator
# ============================================================================
log "Adding Helm repository: $HELM_REPO_URL"
if ! helm repo add "$HELM_REPO_NAME" "$HELM_REPO_URL" 2>/dev/null; then
    # Repo add failed - check if it already exists
    if helm repo list | grep -q "^$HELM_REPO_NAME"; then
        log "Helm repo '$HELM_REPO_NAME' already exists (OK)"
    else
        fail "Failed to add Helm repo: $HELM_REPO_URL"
    fi
fi
helm repo update

log "Creating job namespace: $EXPECTED_JOB_NAMESPACE"
kubectl create namespace "$EXPECTED_JOB_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

log "Installing Spark Operator..."
log "  Release:   $RELEASE_NAME"
log "  Namespace: $RELEASE_NAMESPACE"
log "  Chart:     $HELM_REPO_NAME/$CHART_NAME"
log "  Job Namespace: $EXPECTED_JOB_NAMESPACE"

helm install "$RELEASE_NAME" "$HELM_REPO_NAME/$CHART_NAME" \
    --namespace "$RELEASE_NAMESPACE" \
    --create-namespace \
    --set "spark.jobNamespaces={$EXPECTED_JOB_NAMESPACE}" \
    --wait \
    --timeout "$TIMEOUT"

pass "Spark Operator installed successfully"

# ============================================================================
# Wait for pods to be ready
# ============================================================================
log "Waiting for operator pods to be ready..."
kubectl wait --for=condition=Ready pod \
    -l app.kubernetes.io/instance="$RELEASE_NAME" \
    -n "$RELEASE_NAMESPACE" \
    --timeout=120s

pass "All operator pods are ready"

# ============================================================================
# Capture Pod Names (for use in all tests)
# ============================================================================
log "Capturing operator pod names..."

CONTROLLER_POD=$(kubectl get pods -n "$RELEASE_NAMESPACE" \
    -l app.kubernetes.io/component=controller \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

WEBHOOK_POD=$(kubectl get pods -n "$RELEASE_NAMESPACE" \
    -l app.kubernetes.io/component=webhook \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

if [ -z "$CONTROLLER_POD" ]; then
    fail "Controller pod not found"
fi

echo "  Controller: $CONTROLLER_POD"
echo "  Webhook:    ${WEBHOOK_POD:-not found}"

# ============================================================================
# Test 1: Verify fsGroup is NOT 185
# ============================================================================
log "TEST 1: Checking fsGroup on operator pods..."

# Check controller pod
FSGROUP=$(kubectl get pod "$CONTROLLER_POD" -n "$RELEASE_NAMESPACE" \
    -o jsonpath='{.spec.securityContext.fsGroup}' 2>/dev/null || echo "")
if [ "$FSGROUP" = "185" ]; then
    fail "Pod $CONTROLLER_POD has fsGroup=185 (not allowed for OpenShift)"
elif [ -z "$FSGROUP" ] || [ "$FSGROUP" = "null" ]; then
    echo "  $CONTROLLER_POD: fsGroup not set (OK for OpenShift)"
else
    echo "  $CONTROLLER_POD: fsGroup=$FSGROUP (OK)"
fi

# Check webhook pod (if exists)
if [ -n "$WEBHOOK_POD" ]; then
    FSGROUP=$(kubectl get pod "$WEBHOOK_POD" -n "$RELEASE_NAMESPACE" \
        -o jsonpath='{.spec.securityContext.fsGroup}' 2>/dev/null || echo "")
    if [ "$FSGROUP" = "185" ]; then
        fail "Pod $WEBHOOK_POD has fsGroup=185 (not allowed for OpenShift)"
    elif [ -z "$FSGROUP" ] || [ "$FSGROUP" = "null" ]; then
        echo "  $WEBHOOK_POD: fsGroup not set (OK for OpenShift)"
    else
        echo "  $WEBHOOK_POD: fsGroup=$FSGROUP (OK)"
    fi
fi

pass "TEST 1 PASSED: No operator pods have fsGroup=185"

# ============================================================================
# Test 2: Verify jobNamespaces configuration
# ============================================================================
log "TEST 2: Checking jobNamespaces configuration..."

# Get the --namespaces argument from the controller
NAMESPACES_ARG=$(kubectl get pod "$CONTROLLER_POD" -n "$RELEASE_NAMESPACE" \
    -o jsonpath='{.spec.containers[0].args}' | grep -oP '(?<=--namespaces=)[^"]*' || echo "")

if [ -z "$NAMESPACES_ARG" ]; then
    # Try getting from command instead of args
    NAMESPACES_ARG=$(kubectl get pod "$CONTROLLER_POD" -n "$RELEASE_NAMESPACE" \
        -o jsonpath='{.spec.containers[0].command}' | grep -oP '(?<=--namespaces=)[^"]*' || echo "")
fi

echo "  Configured namespaces: $NAMESPACES_ARG"

if echo "$NAMESPACES_ARG" | grep -q "$EXPECTED_JOB_NAMESPACE"; then
    pass "TEST 2 PASSED: jobNamespaces includes '$EXPECTED_JOB_NAMESPACE'"
else
    fail "TEST 2 FAILED: jobNamespaces does not include '$EXPECTED_JOB_NAMESPACE' (found: $NAMESPACES_ARG)"
fi

# ============================================================================
# Summary
# ============================================================================
echo ""
echo "============================================"
pass "ALL OPERATOR INSTALL TESTS PASSED!"
echo "============================================"
echo ""
echo "Operator will be cleaned up on exit (default behavior)."
echo ""
echo "To keep operator for subsequent tests, run with:"
echo "  CLEANUP=false ./test-operator-install.sh"
echo "  ./test-spark-pi.sh      # Then run Spark Pi test"
echo ""

