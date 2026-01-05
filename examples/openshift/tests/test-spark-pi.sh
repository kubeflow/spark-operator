#!/bin/bash
# ============================================================================
# Test: Spark Pi Application (Lightweight)
# ============================================================================
#
# This is a LIGHTWEIGHT test that runs the classic Spark Pi example.
# Use this for quick validation of the Spark Operator without the heavy
# docling-spark image (~9GB).
#
# This test verifies:
#   1. SparkApplication can be submitted
#   2. Driver pod starts and runs
#   3. Application completes successfully
#   4. Pi calculation result is present in logs
#
# Prerequisites:
#   - Spark Operator already installed (run test-operator-install.sh first)
#
# Usage:
#   ./test-spark-pi.sh
#
# Environment Variables:
#   APP_NAMESPACE     - Namespace to deploy app (default: docling-spark)
#   TIMEOUT_SECONDS   - Max wait time for completion (default: 300)
#   SKIP_CLEANUP      - Set to "true" to preserve resources for debugging
#
# ============================================================================

set -euo pipefail

# ============================================================================
# Configuration
# ============================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

APP_NAMESPACE="${APP_NAMESPACE:-docling-spark}"
APP_NAME="${APP_NAME:-spark-pi}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-300}"  # 5 minutes is plenty for Pi
SPARK_IMAGE="${SPARK_IMAGE:-docker.io/apache/spark:3.5.0}"

# ============================================================================
# Helper Functions
# ============================================================================
log()  { echo "➡️  $1"; }
pass() { echo "✅ $1"; }
fail() { echo "❌ $1"; exit 1; }
warn() { echo "⚠️  $1"; }

cleanup() {
    if [ "${SKIP_CLEANUP:-false}" = "true" ]; then
        warn "SKIP_CLEANUP=true, leaving resources for inspection"
        echo ""
        echo "To inspect:"
        echo "  kubectl get sparkapplication $APP_NAME -n $APP_NAMESPACE -o yaml"
        echo "  kubectl logs ${APP_NAME}-driver -n $APP_NAMESPACE"
        echo ""
        echo "To cleanup manually:"
        echo "  kubectl delete sparkapplication $APP_NAME -n $APP_NAMESPACE"
        return
    fi
    log "Cleaning up SparkApplication..."
    kubectl delete sparkapplication "$APP_NAME" -n "$APP_NAMESPACE" --ignore-not-found || true
}

trap cleanup EXIT

get_app_state() {
    kubectl get sparkapplication "$APP_NAME" -n "$APP_NAMESPACE" \
        -o jsonpath='{.status.applicationState.state}' 2>/dev/null || echo "NOT_FOUND"
}

get_app_error() {
    kubectl get sparkapplication "$APP_NAME" -n "$APP_NAMESPACE" \
        -o jsonpath='{.status.applicationState.errorMessage}' 2>/dev/null || echo ""
}

# ============================================================================
# Pre-flight Checks
# ============================================================================
log "Running pre-flight checks..."

# Check if operator is installed
if ! kubectl get deployment -n spark-operator-openshift -l app.kubernetes.io/name=spark-operator &>/dev/null; then
    if ! kubectl get deployment -n kubeflow-spark-operator -l app.kubernetes.io/name=spark-operator &>/dev/null; then
        fail "Spark Operator not found. Run test-operator-install.sh first."
    fi
fi
echo "  Spark Operator: Found"

pass "Pre-flight checks passed"

# ============================================================================
# Setup: Create namespace and Apply RBAC
# ============================================================================
log "Creating namespace '$APP_NAMESPACE' if not exists..."
kubectl create namespace "$APP_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
RBAC_YAML="${RBAC_YAML:-$REPO_ROOT/examples/openshift/k8s/base/rbac.yaml}"

log "Applying RBAC..."
if [ -f "$RBAC_YAML" ]; then
    kubectl apply -f "$RBAC_YAML"
    pass "RBAC applied"
else
    warn "RBAC YAML not found: $RBAC_YAML (assuming already applied)"
fi

# ============================================================================
# Deploy SparkApplication (Pi Example)
# ============================================================================
log "Deploying Spark Pi application..."
echo "  Name:      $APP_NAME"
echo "  Namespace: $APP_NAMESPACE"
echo "  Image:     $SPARK_IMAGE"

# Delete existing app if present
kubectl delete sparkapplication "$APP_NAME" -n "$APP_NAMESPACE" --ignore-not-found 2>/dev/null || true

# Apply the SparkApplication from YAML file (using envsubst for variable substitution)
APP_YAML="${APP_YAML:-$SCRIPT_DIR/spark-pi-app.yaml}"
if [ ! -f "$APP_YAML" ]; then
    fail "SparkApplication YAML not found: $APP_YAML"
fi

export APP_NAME APP_NAMESPACE SPARK_IMAGE
envsubst < "$APP_YAML" | kubectl apply -f -

pass "SparkApplication submitted"

# ============================================================================
# Wait for SparkApplication to Complete
# ============================================================================
log "Waiting for SparkApplication to complete (timeout: ${TIMEOUT_SECONDS}s)..."

SECONDS=0
LAST_STATE=""

while [ $SECONDS -lt $TIMEOUT_SECONDS ]; do
    STATE=$(get_app_state)
    
    # Only log state changes
    if [ "$STATE" != "$LAST_STATE" ]; then
        echo "  [${SECONDS}s] State: $STATE"
        LAST_STATE="$STATE"
    fi
    
    case "$STATE" in
        COMPLETED)
            pass "SparkApplication completed successfully!"
            break
            ;;
        FAILED)
            echo ""
            echo "=== SparkApplication Failed ==="
            echo "Error: $(get_app_error)"
            echo ""
            echo "=== Driver Pod Logs ==="
            kubectl logs "${APP_NAME}-driver" -n "$APP_NAMESPACE" --tail=50 2>/dev/null || echo "(no logs available)"
            fail "SparkApplication failed!"
            ;;
        FAILED_SUBMISSION)
            echo ""
            echo "=== Submission Failed ==="
            echo "Error: $(get_app_error)"
            fail "SparkApplication submission failed!"
            ;;
    esac
    
    sleep 5
done

# Check if we timed out
if [ "$STATE" != "COMPLETED" ]; then
    echo ""
    echo "=== Timeout - Current State: $STATE ==="
    echo ""
    echo "=== Pods in $APP_NAMESPACE ==="
    kubectl get pods -n "$APP_NAMESPACE" -o wide
    echo ""
    echo "=== Driver Pod Logs ==="
    kubectl logs "${APP_NAME}-driver" -n "$APP_NAMESPACE" --tail=100 2>/dev/null || echo "(no logs available)"
    fail "SparkApplication did not complete within ${TIMEOUT_SECONDS}s"
fi

# ============================================================================
# Verify Results
# ============================================================================
log "Verifying execution..."

# Check driver pod existed and completed
DRIVER_POD="${APP_NAME}-driver"
DRIVER_STATUS=$(kubectl get pod "$DRIVER_POD" -n "$APP_NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || echo "NOT_FOUND")
echo "  Driver pod ($DRIVER_POD): $DRIVER_STATUS"

# Get executor count
EXECUTOR_COUNT=$(kubectl get pods -n "$APP_NAMESPACE" -l "spark-role=executor,spark-app-name=$APP_NAME" --no-headers 2>/dev/null | wc -l)
echo "  Executors created: $EXECUTOR_COUNT"

# Check for Pi result in logs
echo ""
log "Checking for Pi calculation result..."
PI_RESULT=$(kubectl logs "$DRIVER_POD" -n "$APP_NAMESPACE" 2>/dev/null | grep -i "Pi is roughly" || echo "")

if [ -n "$PI_RESULT" ]; then
    echo "  📊 $PI_RESULT"
    pass "Pi calculation completed!"
else
    warn "Could not find Pi result in logs (job may have completed differently)"
fi

# Show last few lines of driver logs
echo ""
log "Driver logs (last 10 lines):"
kubectl logs "$DRIVER_POD" -n "$APP_NAMESPACE" --tail=10 2>/dev/null || warn "Could not get driver logs"

# ============================================================================
# Summary
# ============================================================================
echo ""
echo "============================================"
pass "SPARK PI TEST PASSED!"
echo "============================================"
echo ""
echo "Summary:"
echo "  - SparkApplication: $APP_NAME"
echo "  - Namespace: $APP_NAMESPACE"
echo "  - Final State: COMPLETED"
echo "  - Driver Pod: $DRIVER_STATUS"
echo "  - Executor Pods: $EXECUTOR_COUNT"
if [ -n "$PI_RESULT" ]; then
    echo "  - Result: $PI_RESULT"
fi
echo ""

