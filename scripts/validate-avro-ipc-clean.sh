#!/bin/bash
#
# validate-avro-ipc-clean.sh
#
# Validates that the stats folder (containing vulnerable jQuery files) 
# has been removed from avro-ipc JAR.
#
# Usage:
#   ./validate-avro-ipc-clean.sh [JAR_PATH]
#   ./validate-avro-ipc-clean.sh                    # Auto-detect in /opt/spark/jars
#   ./validate-avro-ipc-clean.sh /path/to/avro-ipc-1.12.1.jar
#
# Exit codes:
#   0 - Clean: No vulnerable files found
#   1 - Vulnerable: jQuery/stats files found
#   2 - Error: JAR not found or other error
#
# For CI/CD integration, this script outputs machine-readable status.
#

set -euo pipefail

# Colors for terminal output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
SPARK_JARS_DIR="${SPARK_JARS_DIR:-/opt/spark/jars}"
AVRO_IPC_PATTERN="avro-ipc-*.jar"

# Vulnerable patterns to check
VULNERABLE_PATTERNS=(
    "org/apache/avro/ipc/stats/static/jquery"
    "org/apache/avro/ipc/stats/static/.*\.js"
    "org/apache/avro/ipc/stats/"
)

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[✓ PASS]${NC} $1"
}

log_fail() {
    echo -e "${RED}[✗ FAIL]${NC} $1"
}

find_avro_ipc_jar() {
    local search_path="$1"
    
    if [[ -f "$search_path" ]]; then
        # Direct path provided
        echo "$search_path"
        return 0
    elif [[ -d "$search_path" ]]; then
        # Directory provided, search for JAR
        local jar_path
        jar_path=$(find "$search_path" -name "$AVRO_IPC_PATTERN" -type f 2>/dev/null | head -1)
        if [[ -n "$jar_path" ]]; then
            echo "$jar_path"
            return 0
        fi
    fi
    
    return 1
}

check_jar_contents() {
    local jar_path="$1"
    local found_vulnerabilities=0
    local vulnerable_files=()
    
    log_info "Scanning JAR: $jar_path"
    log_info "JAR size: $(du -h "$jar_path" | cut -f1)"
    
    # List JAR contents
    local jar_contents
    jar_contents=$(unzip -l "$jar_path" 2>/dev/null || jar tf "$jar_path" 2>/dev/null)
    
    if [[ -z "$jar_contents" ]]; then
        log_error "Failed to read JAR contents"
        return 2
    fi
    
    # Check for stats folder
    if echo "$jar_contents" | grep -q "org/apache/avro/ipc/stats/"; then
        log_warn "Found stats folder in JAR"
        
        # List what's in the stats folder
        local stats_files
        stats_files=$(echo "$jar_contents" | grep "org/apache/avro/ipc/stats/" || true)
        
        if [[ -n "$stats_files" ]]; then
            log_error "Stats folder contents:"
            echo "$stats_files" | while read -r line; do
                echo "  - $line"
                vulnerable_files+=("$line")
            done
            found_vulnerabilities=1
        fi
    else
        log_info "Stats folder not found (good)"
    fi
    
    # Specifically check for jQuery files
    local jquery_files
    jquery_files=$(echo "$jar_contents" | grep -i "jquery" || true)
    
    if [[ -n "$jquery_files" ]]; then
        log_error "Found jQuery files:"
        echo "$jquery_files" | while read -r line; do
            echo "  - $line"
        done
        found_vulnerabilities=1
    else
        log_info "No jQuery files found (good)"
    fi
    
    # Check for any .js files in the JAR
    local js_files
    js_files=$(echo "$jar_contents" | grep "\.js$" || true)
    
    if [[ -n "$js_files" ]]; then
        log_warn "JavaScript files found in JAR:"
        echo "$js_files" | while read -r line; do
            echo "  - $line"
        done
        # JS files in stats folder are vulnerable
        if echo "$js_files" | grep -q "org/apache/avro/ipc/stats/"; then
            found_vulnerabilities=1
        fi
    else
        log_info "No JavaScript files found (good)"
    fi
    
    return $found_vulnerabilities
}

generate_report() {
    local jar_path="$1"
    local status="$2"
    local timestamp
    timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    
    # JSON output for CI/CD integration
    cat <<EOF
{
  "timestamp": "$timestamp",
  "jar_path": "$jar_path",
  "validation": "avro-ipc-jquery-removal",
  "status": "$status",
  "cve": "CVE-2019-11358",
  "description": "jQuery prototype pollution vulnerability check"
}
EOF
}

main() {
    local jar_path=""
    local search_path="${1:-$SPARK_JARS_DIR}"
    
    echo "========================================"
    echo "Avro-IPC jQuery Vulnerability Validator"
    echo "========================================"
    echo ""
    
    # Find the JAR
    jar_path=$(find_avro_ipc_jar "$search_path") || {
        log_error "Could not find avro-ipc JAR in: $search_path"
        log_info "Searched for pattern: $AVRO_IPC_PATTERN"
        
        if [[ -d "$search_path" ]]; then
            log_info "Available JARs in directory:"
            ls -la "$search_path"/*.jar 2>/dev/null | head -10 || echo "  (none found)"
        fi
        
        echo ""
        generate_report "NOT_FOUND" "ERROR"
        exit 2
    }
    
    log_info "Found JAR: $jar_path"
    echo ""
    
    # Check the JAR contents
    if check_jar_contents "$jar_path"; then
        echo ""
        echo "========================================"
        log_success "VALIDATION PASSED"
        echo "========================================"
        echo "The avro-ipc JAR is clean."
        echo "No vulnerable jQuery files detected."
        echo ""
        generate_report "$jar_path" "CLEAN"
        exit 0
    else
        echo ""
        echo "========================================"
        log_fail "VALIDATION FAILED"
        echo "========================================"
        echo "Vulnerable files detected in avro-ipc JAR!"
        echo "The remove-jquery-from-avro.sh script may not have run."
        echo ""
        echo "To fix, ensure the script runs during image build:"
        echo "  RUN /opt/remove-jquery-from-avro.sh"
        echo ""
        generate_report "$jar_path" "VULNERABLE"
        exit 1
    fi
}

# Run if executed directly (not sourced)
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
