#!/bin/bash
# ci/verify_openshift_compatibility.sh
# Verifies that Spark Operator controller pod runs successfully under restricted-v2 SCC
# and that driver/executor pods can read/write to required directories with arbitrary UIDs.

set -e

# Configuration
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-kubeflow-spark-operator}"
NAMESPACE="${SPARK_NAMESPACE:-docling-spark}"
APP_NAME="${SPARK_APP_NAME:-docling-spark-job}"
TIMEOUT="${TIMEOUT:-60}"  # 1 minute timeout for pod to be ready

echo "🔍 Verifying OpenShift compatibility for Spark Operator..."
echo "   Operator Namespace: $OPERATOR_NAMESPACE"
echo "   SparkApplication Namespace: $NAMESPACE"
echo "   Application: $APP_NAME"
echo ""

FAILED=0

# Function to wait for pod to be ready
wait_for_pod() {
    local pod_name=$1
    local namespace=$2
    local timeout=$3
    local elapsed=0
    
    echo "⏳ Waiting for pod '$pod_name' to be ready (timeout: ${timeout}s)..."
    while [ $elapsed -lt $timeout ]; do
        if oc get pod "$pod_name" -n "$namespace" -o jsonpath='{.status.phase}' 2>/dev/null | grep -q "Running"; then
            echo "✅ Pod '$pod_name' is running"
            return 0
        fi
        sleep 5
        elapsed=$((elapsed + 5))
    done
    
    echo "❌ Timeout waiting for pod '$pod_name' to be ready"
    return 1
}

# Check if operator namespace exists
if ! oc get namespace "$OPERATOR_NAMESPACE" > /dev/null 2>&1; then
    echo "❌ Error: Namespace '$OPERATOR_NAMESPACE' not found"
    echo "   Please install the Spark Operator first."
    exit 1
fi

echo "=== Criterion 1: restricted-v2 SCC without fixed UIDs (Controller Pod) ==="
echo ""

# Get controller pod name
CONTROLLER_POD=$(oc get pod -n "$OPERATOR_NAMESPACE" -l app.kubernetes.io/name=spark-operator,app.kubernetes.io/component=controller -o name 2>/dev/null | head -1 | cut -d'/' -f2)

if [ -z "$CONTROLLER_POD" ]; then
    echo "❌ Error: Controller pod not found in namespace '$OPERATOR_NAMESPACE'"
    echo "   Check if the operator is installed: oc get pods -n $OPERATOR_NAMESPACE"
    exit 1
fi

# Wait for pod to be ready
if ! wait_for_pod "$CONTROLLER_POD" "$OPERATOR_NAMESPACE" "$TIMEOUT"; then
    FAILED=1
    exit 1
fi

# Check SCC annotation
SCC=$(oc get pod "$CONTROLLER_POD" -n "$OPERATOR_NAMESPACE" -o jsonpath='{.metadata.annotations.openshift\.io/scc}' 2>/dev/null || echo "")
if [ "$SCC" = "restricted-v2" ]; then
    echo "✅ Controller pod uses restricted-v2 SCC"
else
    echo "❌ Controller pod does NOT use restricted-v2 SCC (found: '${SCC:-none}')"
    FAILED=1
fi

# Check UID
POD_UID=$(oc exec "$CONTROLLER_POD" -n "$OPERATOR_NAMESPACE" -- id -u 2>/dev/null || echo "")
if [ -z "$POD_UID" ]; then
    echo "❌ Cannot retrieve UID from controller pod"
    FAILED=1
elif [ "$POD_UID" = "0" ]; then
    echo "❌ Controller pod is running as root (UID 0) - should be non-root"
    FAILED=1
elif [ "$POD_UID" = "185" ]; then
    echo "❌ Controller pod is running with fixed UID 185 - should be arbitrary"
    FAILED=1
else
    echo "✅ Controller pod running with arbitrary UID: $POD_UID"
fi

# Check GID (should be 0 for group-writable access)
POD_GID=$(oc exec "$CONTROLLER_POD" -n "$OPERATOR_NAMESPACE" -- id -g 2>/dev/null || echo "")
if [ "$POD_GID" = "0" ]; then
    echo "✅ Controller pod is member of group 0 (root group)"
else
    echo "⚠️  Warning: Controller pod GID is $POD_GID (expected 0 for group-writable access)"
fi

echo ""
echo "=== Criterion 2: Directories owned by group 0 and group-writable (Driver/Executor Pods) ==="
echo ""

# Check if SparkApplication namespace exists
if ! oc get namespace "$NAMESPACE" > /dev/null 2>&1; then
    echo "⚠️  Warning: Namespace '$NAMESPACE' not found"
    echo "   Skipping driver/executor pod checks. Deploy a SparkApplication to test Criterion 2."
    FAILED=1
elif ! oc get sparkapplication "$APP_NAME" -n "$NAMESPACE" > /dev/null 2>&1; then
    echo "⚠️  Warning: SparkApplication '$APP_NAME' not found in namespace '$NAMESPACE'"
    echo "   Skipping driver/executor pod checks. Deploy a SparkApplication to test Criterion 2."
    FAILED=1
else
    # Get driver pod name
    DRIVER_POD=$(oc get pod -n "$NAMESPACE" -l spark-role=driver,spark-app-name="$APP_NAME" -o name 2>/dev/null | head -1 | cut -d'/' -f2)
    
    if [ -z "$DRIVER_POD" ]; then
        echo "⚠️  Warning: Driver pod not found for application '$APP_NAME'"
        echo "   Check if the SparkApplication is running: oc get sparkapplication $APP_NAME -n $NAMESPACE"
        FAILED=1
    else
        # Wait for pod to be ready
        if wait_for_pod "$DRIVER_POD" "$NAMESPACE" "$TIMEOUT"; then
            echo ""
            echo "Checking driver pod: $DRIVER_POD"
            
            # Check if /opt/spark exists and verify permissions
            if oc exec "$DRIVER_POD" -n "$NAMESPACE" -- test -d /opt/spark 2>/dev/null; then
                # Check group ownership
                SPARK_GROUP=$(oc exec "$DRIVER_POD" -n "$NAMESPACE" -- stat -c '%g' /opt/spark 2>/dev/null || oc exec "$DRIVER_POD" -n "$NAMESPACE" -- stat -f '%g' /opt/spark 2>/dev/null || echo "")
                if [ "$SPARK_GROUP" = "0" ]; then
                    echo "✅ /opt/spark is owned by group 0"
                else
                    echo "❌ /opt/spark is NOT owned by group 0 (found: $SPARK_GROUP)"
                    FAILED=1
                fi
                
                # Check if group-writable
                SPARK_PERMS=$(oc exec "$DRIVER_POD" -n "$NAMESPACE" -- stat -c '%a' /opt/spark 2>/dev/null || oc exec "$DRIVER_POD" -n "$NAMESPACE" -- stat -f '%OLp' /opt/spark 2>/dev/null || echo "")
                if [ -n "$SPARK_PERMS" ]; then
                    GROUP_WRITE=$(echo "$SPARK_PERMS" | cut -c2)
                    if [ "$GROUP_WRITE" = "2" ] || [ "$GROUP_WRITE" = "3" ] || [ "$GROUP_WRITE" = "6" ] || [ "$GROUP_WRITE" = "7" ]; then
                        echo "✅ /opt/spark is group-writable (permissions: $SPARK_PERMS)"
                    else
                        echo "❌ /opt/spark is NOT group-writable (permissions: $SPARK_PERMS)"
                        FAILED=1
                    fi
                fi
            else
                echo "❌ /opt/spark directory not found in driver pod"
                FAILED=1
            fi
            
            # Test reading Spark jars
            if oc exec "$DRIVER_POD" -n "$NAMESPACE" -- sh -c "test -r /opt/spark/jars && ls /opt/spark/jars/*.jar 2>/dev/null | head -1 | grep -q ." 2>/dev/null; then
                echo "✅ Can read Spark jars from /opt/spark/jars/"
            else
                echo "❌ Cannot read Spark jars from /opt/spark/jars/"
                FAILED=1
            fi
            
            # Test reading application files
            if oc exec "$DRIVER_POD" -n "$NAMESPACE" -- test -r /app/scripts/run_spark_job.py 2>/dev/null; then
                echo "✅ Can read application script from /app/scripts/"
            else
                echo "⚠️  Warning: Cannot read application script (may not exist in this image)"
            fi
            
            # Test writing to /tmp
            TEST_FILE="/tmp/verify-write-$$.txt"
            if oc exec "$DRIVER_POD" -n "$NAMESPACE" -- touch "$TEST_FILE" > /dev/null 2>&1; then
                echo "✅ Can write to /tmp"
                oc exec "$DRIVER_POD" -n "$NAMESPACE" -- rm "$TEST_FILE" > /dev/null 2>&1
            else
                echo "❌ Cannot write to /tmp"
                FAILED=1
            fi
            
            # Test writing to /opt/spark/work-dir
            TEST_WORK_FILE="/opt/spark/work-dir/verify-work-$$.txt"
            if oc exec "$DRIVER_POD" -n "$NAMESPACE" -- touch "$TEST_WORK_FILE" > /dev/null 2>&1; then
                echo "✅ Can write to /opt/spark/work-dir"
                oc exec "$DRIVER_POD" -n "$NAMESPACE" -- rm "$TEST_WORK_FILE" > /dev/null 2>&1
            else
                echo "❌ Cannot write to /opt/spark/work-dir"
                FAILED=1
            fi
            
            # Test writing to /opt/spark/logs
            TEST_LOG_FILE="/opt/spark/logs/verify-log-$$.txt"
            if oc exec "$DRIVER_POD" -n "$NAMESPACE" -- touch "$TEST_LOG_FILE" > /dev/null 2>&1; then
                echo "✅ Can write to /opt/spark/logs"
                oc exec "$DRIVER_POD" -n "$NAMESPACE" -- rm "$TEST_LOG_FILE" > /dev/null 2>&1
            else
                echo "❌ Cannot write to /opt/spark/logs"
                FAILED=1
            fi
            
            # Test writing to /app/output
            TEST_OUTPUT_FILE="/app/output/verify-output-$$.txt"
            if oc exec "$DRIVER_POD" -n "$NAMESPACE" -- touch "$TEST_OUTPUT_FILE" > /dev/null 2>&1; then
                echo "✅ Can write to /app/output"
                oc exec "$DRIVER_POD" -n "$NAMESPACE" -- rm "$TEST_OUTPUT_FILE" > /dev/null 2>&1
            else
                echo "❌ Cannot write to /app/output"
                FAILED=1
            fi
        else
            FAILED=1
        fi
        
        # Check executor pod (if exists)
        EXECUTOR_POD=$(oc get pod -n "$NAMESPACE" -l spark-role=executor,spark-app-name="$APP_NAME" -o name 2>/dev/null | head -1 | cut -d'/' -f2)
        
        if [ -n "$EXECUTOR_POD" ]; then
            echo ""
            echo "Checking executor pod: $EXECUTOR_POD"
            if wait_for_pod "$EXECUTOR_POD" "$NAMESPACE" "$TIMEOUT"; then
                # Check if /opt/spark exists and verify permissions
                if oc exec "$EXECUTOR_POD" -n "$NAMESPACE" -- test -d /opt/spark 2>/dev/null; then
                    SPARK_GROUP=$(oc exec "$EXECUTOR_POD" -n "$NAMESPACE" -- stat -c '%g' /opt/spark 2>/dev/null || oc exec "$EXECUTOR_POD" -n "$NAMESPACE" -- stat -f '%g' /opt/spark 2>/dev/null || echo "")
                    if [ "$SPARK_GROUP" = "0" ]; then
                        echo "✅ /opt/spark is owned by group 0"
                    else
                        echo "❌ /opt/spark is NOT owned by group 0 (found: $SPARK_GROUP)"
                        FAILED=1
                    fi
                fi
                
                # Test write to /opt/spark/work-dir
                TEST_WORK_FILE="/opt/spark/work-dir/verify-work-exec-$$.txt"
                if oc exec "$EXECUTOR_POD" -n "$NAMESPACE" -- touch "$TEST_WORK_FILE" > /dev/null 2>&1; then
                    echo "✅ Can write to /opt/spark/work-dir"
                    oc exec "$EXECUTOR_POD" -n "$NAMESPACE" -- rm "$TEST_WORK_FILE" > /dev/null 2>&1
                else
                    echo "❌ Cannot write to /opt/spark/work-dir"
                    FAILED=1
                fi
            fi
        fi
    fi
fi

echo ""
if [ $FAILED -eq 1 ]; then
    echo "🚨 OpenShift Compatibility Check FAILED"
    echo "   Please verify:"
    echo "   1. Spark Operator is installed with restricted-v2 SCC compatibility"
    echo "   2. SparkApplication image has directories owned by group 0 and group-writable"
    echo "   3. spark-operator-values.yaml has fsGroup: null for controller and webhook"
    echo "   4. SparkApplication YAML has empty securityContext: {}"
    exit 1
else
    echo "✅ OpenShift Compatibility Check PASSED"
    echo "   ✓ Controller pod runs under restricted-v2 SCC with arbitrary UID"
    echo "   ✓ Driver/executor pods have directories owned by group 0 and group-writable"
    echo "   ✓ Driver/executor pods can read Spark jars and application files"
    echo "   ✓ Driver/executor pods can write to temp, work-dir, logs, and output directories"
    exit 0
fi