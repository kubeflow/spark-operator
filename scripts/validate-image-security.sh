#!/bin/bash
#
# validate-image-security.sh
#
# Validates security remediations in a Docker image.
# Designed for CI/CD pipeline integration.
#
# Usage:
#   ./validate-image-security.sh <image_name>
#   ./validate-image-security.sh spark-operator:local
#   ./validate-image-security.sh myregistry.azurecr.io/spark:v1.0.0
#
# Exit codes:
#   0 - All validations passed
#   1 - One or more validations failed
#   2 - Error (image not found, etc.)
#

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

IMAGE_NAME="${1:-}"
VALIDATION_RESULTS=()
FAILED_COUNT=0

log_header() {
    echo -e "\n${BLUE}════════════════════════════════════════${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}════════════════════════════════════════${NC}\n"
}

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_pass() {
    echo -e "${GREEN}[✓ PASS]${NC} $1"
    VALIDATION_RESULTS+=("PASS: $1")
}

log_fail() {
    echo -e "${RED}[✗ FAIL]${NC} $1"
    VALIDATION_RESULTS+=("FAIL: $1")
    ((FAILED_COUNT++))
}

usage() {
    echo "Usage: $0 <image_name>"
    echo ""
    echo "Examples:"
    echo "  $0 spark-operator:local"
    echo "  $0 myregistry.azurecr.io/spark-operator:v1.0.0"
    echo ""
    echo "Validations performed:"
    echo "  1. CVE-2019-11358: jQuery removed from avro-ipc JAR"
    echo "  2. Stats folder removed from avro-ipc JAR"
    echo "  3. No JavaScript files in avro-ipc JAR"
    exit 2
}

check_image_exists() {
    log_info "Checking if image exists: $IMAGE_NAME"
    
    if ! docker image inspect "$IMAGE_NAME" &>/dev/null; then
        log_error "Image not found: $IMAGE_NAME"
        log_info "Available images:"
        docker images --format "  {{.Repository}}:{{.Tag}}" | head -10
        return 1
    fi
    
    log_info "Image found"
    return 0
}

validate_avro_ipc_jquery() {
    log_header "Validating CVE-2019-11358 Remediation"
    
    local container_id
    local jar_path="/opt/spark/jars"
    local avro_jar=""
    
    # Find avro-ipc JAR in the image
    log_info "Searching for avro-ipc JAR..."
    
    avro_jar=$(docker run --rm --entrypoint="" "$IMAGE_NAME" \
        find "$jar_path" -name "avro-ipc-*.jar" -type f 2>/dev/null | head -1) || true
    
    if [[ -z "$avro_jar" ]]; then
        log_warn "No avro-ipc JAR found in $jar_path"
        log_pass "Avro-IPC JAR not present (not applicable)"
        return 0
    fi
    
    log_info "Found: $avro_jar"
    
    # Check for jQuery files
    log_info "Checking for jQuery files..."
    local jquery_check
    jquery_check=$(docker run --rm --entrypoint="" "$IMAGE_NAME" \
        sh -c "unzip -l '$avro_jar' 2>/dev/null | grep -i jquery || true")
    
    if [[ -n "$jquery_check" ]]; then
        log_fail "jQuery files found in avro-ipc JAR"
        echo "$jquery_check" | while read -r line; do
            echo "    $line"
        done
        return 1
    else
        log_pass "No jQuery files in avro-ipc JAR"
    fi
    
    # Check for stats folder
    log_info "Checking for stats folder..."
    local stats_check
    stats_check=$(docker run --rm --entrypoint="" "$IMAGE_NAME" \
        sh -c "unzip -l '$avro_jar' 2>/dev/null | grep 'org/apache/avro/ipc/stats/' || true")
    
    if [[ -n "$stats_check" ]]; then
        log_fail "Stats folder found in avro-ipc JAR"
        echo "$stats_check" | while read -r line; do
            echo "    $line"
        done
        return 1
    else
        log_pass "No stats folder in avro-ipc JAR"
    fi
    
    # Check for any .js files
    log_info "Checking for JavaScript files..."
    local js_check
    js_check=$(docker run --rm --entrypoint="" "$IMAGE_NAME" \
        sh -c "unzip -l '$avro_jar' 2>/dev/null | grep '\.js$' || true")
    
    if [[ -n "$js_check" ]]; then
        log_warn "JavaScript files found (may be legitimate)"
        echo "$js_check" | while read -r line; do
            echo "    $line"
        done
        # Check if any are in stats folder
        if echo "$js_check" | grep -q "org/apache/avro/ipc/stats/"; then
            log_fail "JavaScript files in stats folder"
            return 1
        fi
    else
        log_pass "No JavaScript files in avro-ipc JAR"
    fi
    
    return 0
}

print_summary() {
    log_header "Validation Summary"
    
    echo "Image: $IMAGE_NAME"
    echo "Date: $(date -u +"%Y-%m-%d %H:%M:%S UTC")"
    echo ""
    echo "Results:"
    
    for result in "${VALIDATION_RESULTS[@]}"; do
        if [[ "$result" == PASS:* ]]; then
            echo -e "  ${GREEN}✓${NC} ${result#PASS: }"
        else
            echo -e "  ${RED}✗${NC} ${result#FAIL: }"
        fi
    done
    
    echo ""
    
    if [[ $FAILED_COUNT -eq 0 ]]; then
        echo -e "${GREEN}════════════════════════════════════════${NC}"
        echo -e "${GREEN}  ALL VALIDATIONS PASSED${NC}"
        echo -e "${GREEN}════════════════════════════════════════${NC}"
        return 0
    else
        echo -e "${RED}════════════════════════════════════════${NC}"
        echo -e "${RED}  $FAILED_COUNT VALIDATION(S) FAILED${NC}"
        echo -e "${RED}════════════════════════════════════════${NC}"
        return 1
    fi
}

generate_json_report() {
    local status="PASS"
    [[ $FAILED_COUNT -gt 0 ]] && status="FAIL"
    
    cat <<EOF
{
  "image": "$IMAGE_NAME",
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "status": "$status",
  "failed_count": $FAILED_COUNT,
  "validations": {
    "cve_2019_11358_jquery": "$(echo "${VALIDATION_RESULTS[*]}" | grep -q "PASS.*jQuery" && echo "PASS" || echo "FAIL")",
    "stats_folder_removed": "$(echo "${VALIDATION_RESULTS[*]}" | grep -q "PASS.*stats" && echo "PASS" || echo "FAIL")"
  }
}
EOF
}

main() {
    if [[ -z "$IMAGE_NAME" ]]; then
        usage
    fi
    
    echo ""
    echo "╔════════════════════════════════════════════════════╗"
    echo "║     Docker Image Security Validation               ║"
    echo "╚════════════════════════════════════════════════════╝"
    echo ""
    echo "Image: $IMAGE_NAME"
    echo "Date:  $(date)"
    echo ""
    
    # Check image exists
    if ! check_image_exists; then
        exit 2
    fi
    
    # Run validations
    validate_avro_ipc_jquery || true
    
    # Print summary
    echo ""
    if print_summary; then
        echo ""
        echo "JSON Report:"
        generate_json_report
        exit 0
    else
        echo ""
        echo "JSON Report:"
        generate_json_report
        exit 1
    fi
}

main "$@"
