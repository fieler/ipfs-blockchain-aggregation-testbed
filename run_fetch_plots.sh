#!/bin/bash

# Manages experiment data (CSVs and plot PNGs) on the middleware server.
#
# Modes:
#   (no flag)      Download PNGs + CSVs (no plot generation)
#   --generate     Generate plots on server, then download PNGs + CSVs
#   --fetch-csv    Download CSVs only — use before tearing down infrastructure
#   --upload-csv   Upload locally saved CSVs back to a fresh server — use after
#                  recreating infrastructure so experiments can continue

set -e
set -o pipefail

# Configuration
PROJECT_DIR="/mnt/c/Projects/Bachelorarbeit"
export ANSIBLE_CONFIG="${PROJECT_DIR}/ansible.cfg"
MIDDLEWARE_PORT=8000
OUTPUT_DIR="${PROJECT_DIR}/.tmp_plots"
CSV_FILES=("batch_metrics.csv" "e2e_metrics.csv")
SCENARIOS=("N" "V1" "V2" "V3" "Z1" "Z2" "Z3")
PNG_FILES=()
for s in "${SCENARIOS[@]}"; do
    PNG_FILES+=(
        "ff1_batch_duration_${s}.png"
        "ff1_chain_duration_${s}.png"
        "ff1_ipfs_duration_${s}.png"
        "ff2_e2e_finality_${s}.png"
        "ff2_propagation_${s}.png"
    )
done
PNG_FILES+=(
    "ff1_cpu_vs_load.png"
    "ff1_ram_vs_load.png"
    "ff1_tx_comparison.png"
)

# Colors
BLUE='\033[0;34m'
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

log()     { echo -e "${BLUE}[$(date +'%H:%M:%S')] INFO:${NC} $1"; }
success() { echo -e "${GREEN}[$(date +'%H:%M:%S')] SUCCESS:${NC} $1"; }
error()   { echo -e "${RED}[$(date +'%H:%M:%S')] ERROR:${NC} $1"; }

trap 'error "An error occurred. Exiting..."; exit 1' ERR

# Parse flags
GENERATE=false
FETCH_CSV=false
UPLOAD_CSV=false
for arg in "$@"; do
    case "$arg" in
        --generate)  GENERATE=true ;;
        --fetch-csv) FETCH_CSV=true ;;
        --upload-csv) UPLOAD_CSV=true ;;
    esac
done

# --upload-csv: no server IP needed until after local check
if [ "$UPLOAD_CSV" = true ]; then
    log "Checking local CSV files in ${OUTPUT_DIR}..."
    MISSING=()
    for file in "${CSV_FILES[@]}"; do
        [ -f "${OUTPUT_DIR}/${file}" ] || MISSING+=("$file")
    done
    if [ ${#MISSING[@]} -gt 0 ]; then
        error "Missing local files: ${MISSING[*]}"
        error "Run --fetch-csv first to save the CSVs locally."
        exit 1
    fi
fi

# Resolve gateway IP via Ansible inventory
log "Resolving gateway host from Hetzner inventory..."
GATEWAY_IP=$(ansible-inventory --list 2>/dev/null \
    | python3 -c "
import sys, json

def unwrap(v):
    if isinstance(v, dict) and '__ansible_unsafe' in v:
        return v['__ansible_unsafe']
    return v

inv = json.load(sys.stdin)
group = inv.get('hcloud_label_role_gateway', {})
hosts = group.get('hosts', [])
if not hosts:
    sys.exit(1)
host = unwrap(hosts[0])
hostvars = inv.get('_meta', {}).get('hostvars', {}).get(host, {})
print(unwrap(hostvars.get('ansible_host', host)))
")

if [ -z "$GATEWAY_IP" ]; then
    error "Could not resolve gateway IP. Is the environment deployed?"
    exit 1
fi

MIDDLEWARE_URL="http://${GATEWAY_IP}:${MIDDLEWARE_PORT}"
log "Middleware URL: ${MIDDLEWARE_URL}"

# ── MODE: upload-csv ─────────────────────────────────────────────────────────
if [ "$UPLOAD_CSV" = true ]; then
    UPLOADED=0
    for file in "${CSV_FILES[@]}"; do
        local_path="${OUTPUT_DIR}/${file}"
        log "Uploading ${file} ($(du -h "${local_path}" | cut -f1))..."
        http_code=$(curl -s -o /dev/null -w "%{http_code}" \
            -X POST "${MIDDLEWARE_URL}/upload-experiment-data/${file}" \
            -F "file=@${local_path}" \
            --max-time 120) || http_code="000"
        if [ "$http_code" = "200" ]; then
            success "  ${file} uploaded."
            UPLOADED=$((UPLOADED + 1))
        else
            error "  ${file} upload failed (HTTP ${http_code})."
        fi
    done
    success "Uploaded ${UPLOADED}/${#CSV_FILES[@]} CSV files to ${MIDDLEWARE_URL}."
    exit 0
fi

# ── HELPER: download a single file ───────────────────────────────────────────
download_file() {
    local file="$1"
    local dest="$2"
    local timeout="${3:-60}"
    log "Downloading ${file}..."
    local http_code
    http_code=$(curl -s -o "${dest}" -w "%{http_code}" \
        "${MIDDLEWARE_URL}/experiment-data/${file}" --max-time "${timeout}") || http_code="000"
    if [ "$http_code" = "200" ]; then
        success "  ${file} downloaded."
        return 0
    else
        rm -f "${dest}"
        log "  ${file} not available (HTTP ${http_code}), skipping."
        return 1
    fi
}

# ── MODE: fetch-csv ───────────────────────────────────────────────────────────
if [ "$FETCH_CSV" = true ]; then
    mkdir -p "$OUTPUT_DIR"
    DOWNLOADED=0
    MISSING_FILES=()
    for file in "${CSV_FILES[@]}"; do
        if download_file "${file}" "${OUTPUT_DIR}/${file}" 120; then
            DOWNLOADED=$((DOWNLOADED + 1))
        else
            MISSING_FILES+=("$file")
        fi
    done
    if [ "${#MISSING_FILES[@]}" -gt 0 ]; then
        error "Incomplete fetch: ${DOWNLOADED}/${#CSV_FILES[@]} downloaded. Missing: ${MISSING_FILES[*]}"
        exit 1
    fi
    success "All ${DOWNLOADED}/${#CSV_FILES[@]} CSV files downloaded to ${OUTPUT_DIR}"
    ls -lh "${OUTPUT_DIR}"/*.csv 2>/dev/null || true
    exit 0
fi

# ── MODE: generate + download (default) ──────────────────────────────────────
if [ "$GENERATE" = true ]; then
    log "Triggering plot generation on server..."
    RESULT=$(curl -sf -X POST "${MIDDLEWARE_URL}/generate-plots" \
        -H "Content-Type: application/json" \
        --max-time 120 2>&1) || {
        error "Plot generation failed. Is there CSV data from experiments?"
        exit 1
    }
    echo "$RESULT" | python3 -m json.tool 2>/dev/null || echo "$RESULT"
    success "Plot generation complete."
fi

rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

DOWNLOADED=0
for file in "${PNG_FILES[@]}"; do
    download_file "${file}" "${OUTPUT_DIR}/${file}" 30 && DOWNLOADED=$((DOWNLOADED + 1)) || true
done
for file in "${CSV_FILES[@]}"; do
    download_file "${file}" "${OUTPUT_DIR}/${file}" 120 && DOWNLOADED=$((DOWNLOADED + 1)) || true
done

if [ "$DOWNLOADED" -eq 0 ]; then
    error "No files downloaded. Run experiments and use --generate first."
    rm -rf "$OUTPUT_DIR"
    exit 1
fi

success "Downloaded ${DOWNLOADED} files to ${OUTPUT_DIR}"
ls -lh "$OUTPUT_DIR"
