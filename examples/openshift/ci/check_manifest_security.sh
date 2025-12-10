#!/bin/bash
# ci/check_manifest_security.sh
# Verifies that Kubernetes manifests do not contain hardcoded security contexts
# incompatible with OpenShift restricted-v2 SCC.

set -e

FORBIDDEN_TERMS=("fsGroup" "runAsUser" "runAsGroup")
# Files or directories to check
CHECK_PATHS=("k8s/" "spark-operator-values.yaml")

echo "🔍 Checking manifests for forbidden security contexts..."
echo "   Forbidden terms: ${FORBIDDEN_TERMS[*]}"

FAILED=0

for path in "${CHECK_PATHS[@]}"; do
    if [ -d "$path" ]; then
        # Find all yaml files in directory
        FILES=$(find "$path" -name "*.yaml" -o -name "*.yml")
    elif [ -f "$path" ]; then
        FILES="$path"
    else
        echo "⚠️  Warning: Path '$path' not found. Skipping."
        continue
    fi

    for file in $FILES; do
        for term in "${FORBIDDEN_TERMS[@]}"; do
            if grep -q "$term" "$file"; then
                echo "❌ Error: Found forbidden term '$term' in '$file'"
                grep -H -n -C 1 "$term" "$file"
                FAILED=1
            fi
        done
    done
done

if [ $FAILED -eq 1 ]; then
    echo ""
    echo "🚨 Security Check FAILED"
    echo "   Please remove 'fsGroup', 'runAsUser', or 'runAsGroup' from your manifests."
    echo "   Let OpenShift assign these values automatically via SCC."
    exit 1
else
    echo ""
    echo "✅ Security Check PASSED"
    exit 0
fi

