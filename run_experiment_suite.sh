#!/bin/bash
# Automated full experiment suite runner.
#
# Iterates over all 7 scenarios, destroying and rebuilding the Hetzner
# testbed between each one. For every scenario the scheduler is loaded
# with 3 experiments (1 / 5 / 10 msg/s, 5 repetitions each = 15 runs)
# and the script blocks until the scheduler reaches 'completed'.
#
# Usage:
#   bash run_experiment_suite.sh [--start-from <LABEL>] [--dry-run]
#
#   --start-from N|V1|V2|V3|Z1|Z2|Z3   Resume from this scenario
#   --dry-run                           Print what would happen, no actions

set -eo pipefail

# ─── Configuration ────────────────────────────────────────────────────────────

PROJECT_DIR="/mnt/c/Projects/Bachelorarbeit"
WEB_UI_PORT=3333
NUM_SENSORS=50
WARMUP_MINUTES=5
EXPERIMENT_MINUTES=15
COOLDOWN_MINUTES=10
REPETITIONS=5

POLL_INTERVAL_SECONDS=60
MAX_WAIT_HOURS=10  # per scenario; ~7.5h expected + headroom

LOAD_RATES=(1.0 5.0 10.0)

# Scenario table: "LABEL  STRATEGY  PARAM_FIELD       PARAM_VALUE"
#   PARAM_FIELD is the middleware JSON key (batch_size_limit / batch_time_limit / -)
declare -a SCENARIOS=(
    "N   none    -                 0"
    "V1  volume  batch_size_limit  50"
    "V2  volume  batch_size_limit  100"
    "V3  volume  batch_size_limit  500"
    "Z1  time    batch_time_limit  10"
    "Z2  time    batch_time_limit  20"
    "Z3  time    batch_time_limit  60"
)

# ─── Argument parsing ─────────────────────────────────────────────────────────

START_FROM=""
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --start-from) START_FROM="$2"; shift 2 ;;
        --dry-run)    DRY_RUN=true; shift ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

# ─── Logging ──────────────────────────────────────────────────────────────────

LOG_FILE="${PROJECT_DIR}/suite_$(date +'%Y%m%d_%H%M%S').log"
BLUE='\033[0;34m'; GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'

log()     { echo -e "${BLUE}[$(date +'%H:%M:%S')] INFO:${NC} $*"    | tee -a "$LOG_FILE"; }
success() { echo -e "${GREEN}[$(date +'%H:%M:%S')] OK:${NC} $*"     | tee -a "$LOG_FILE"; }
error()   { echo -e "${RED}[$(date +'%H:%M:%S')] ERROR:${NC} $*"    | tee -a "$LOG_FILE"; }
warn()    { echo -e "${YELLOW}[$(date +'%H:%M:%S')] WARN:${NC} $*"  | tee -a "$LOG_FILE"; }
banner()  { echo -e "\n${GREEN}$(printf '═%.0s' {1..60})${NC}"      | tee -a "$LOG_FILE"
            echo -e "${GREEN}  $*${NC}"                              | tee -a "$LOG_FILE"
            echo -e "${GREEN}$(printf '═%.0s' {1..60})${NC}\n"      | tee -a "$LOG_FILE"; }

# ─── Helpers ──────────────────────────────────────────────────────────────────

_get_host_ip() {
    local group="$1"
    ansible-inventory --list 2>/dev/null \
        | python3 -c "
import sys, json

def unwrap(v):
    if isinstance(v, dict) and '__ansible_unsafe' in v:
        return v['__ansible_unsafe']
    return v

inv = json.load(sys.stdin)
group = inv.get('$group', {})
hosts = group.get('hosts', [])
if not hosts:
    sys.exit(1)
host = unwrap(hosts[0])
hostvars = inv.get('_meta', {}).get('hostvars', {}).get(host, {})
print(unwrap(hostvars.get('ansible_host', host)))
"
}

get_gateway_ip() { _get_host_ip hcloud_label_role_gateway; }
get_control_ip()  { _get_host_ip hcloud_label_role_control; }

build_queue_payload() {
    local label="$1" strategy="$2" param_field="$3" param_value="$4"
    python3 - "$label" "$strategy" "$param_field" "$param_value" \
              "$NUM_SENSORS" "$WARMUP_MINUTES" "$EXPERIMENT_MINUTES" \
              "$COOLDOWN_MINUTES" "$REPETITIONS" \
              "${LOAD_RATES[@]}" <<'EOF'
import sys, json

label, strategy, param_field, param_value = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
num_sensors   = int(sys.argv[5])
warmup        = int(sys.argv[6])
exp_duration  = int(sys.argv[7])
cooldown      = int(sys.argv[8])
repetitions   = int(sys.argv[9])
rates         = [float(r) for r in sys.argv[10:]]

mw_config = {"batching_strategy": strategy}
if param_field != "-":
    mw_config[param_field] = int(param_value)

experiments = []
for rate in rates:
    rate_label = f"{int(rate)}rps" if rate == int(rate) else f"{rate}rps"
    experiments.append({
        "label": f"{label}_{rate_label}",
        "middleware_config": mw_config,
        "simulator_config": {"num_sensors": num_sensors, "messages_per_second": rate},
        "warmup_duration_minutes": warmup,
        "experiment_duration_minutes": exp_duration,
        "repetitions": repetitions,
        "cooldown_minutes": cooldown,
    })

print(json.dumps({"experiments": experiments}, indent=2))
EOF
}

wait_for_web_ui() {
    local web_ui_url="$1"
    local timeout=300  # 5 minutes max
    local elapsed=0
    log "Waiting for Web UI to become ready..."
    until curl -sf --max-time 5 "${web_ui_url}/api/health" > /dev/null 2>&1; do
        if (( elapsed >= timeout )); then
            error "Web UI did not become ready within ${timeout}s"
            return 1
        fi
        log "  Web UI not ready yet, retrying in 10s... (${elapsed}s elapsed)"
        sleep 10
        elapsed=$(( elapsed + 10 ))
    done
    success "Web UI is ready (${elapsed}s after deploy)."
}

poll_scheduler() {
    local web_ui_url="$1"
    local max_seconds=$(( MAX_WAIT_HOURS * 3600 ))
    local elapsed=0

    while true; do
        local status_json
        status_json=$(curl -sf "${web_ui_url}/api/scheduler/status" 2>/dev/null) || {
            warn "Scheduler unreachable, retrying in ${POLL_INTERVAL_SECONDS}s..."
            sleep "$POLL_INTERVAL_SECONDS"
            elapsed=$(( elapsed + POLL_INTERVAL_SECONDS ))
            continue
        }

        local status cur_exp cur_run cur_phase progress
        status=$(   python3 -c "import sys,json; print(json.loads(sys.stdin.read())['status'])"          <<< "$status_json")
        cur_exp=$(  python3 -c "
import sys,json
d=json.loads(sys.stdin.read())
q=d['queue']; i=d['currentExperimentIndex']
print(q[i]['label'] if 0<=i<len(q) else '-')
"                                                                                                        <<< "$status_json" 2>/dev/null || echo "-")
        cur_run=$(  python3 -c "import sys,json; print(json.loads(sys.stdin.read())['currentRunIndex'])" <<< "$status_json" 2>/dev/null || echo "-")
        cur_phase=$(python3 -c "import sys,json; print(json.loads(sys.stdin.read())['currentPhase'] or '-')" <<< "$status_json" 2>/dev/null || echo "-")
        progress=$( python3 -c "
import sys,json
d=json.loads(sys.stdin.read())
p=d.get('phaseProgress')
print(f'{p[\"percent\"]:.0f}%' if p else '-')
"                                                                                                        <<< "$status_json" 2>/dev/null || echo "-")

        log "Scheduler: ${status} | exp=${cur_exp} run=${cur_run}/${REPETITIONS} phase=${cur_phase} ${progress} | ${elapsed}s elapsed"

        case "$status" in
            completed) return 0 ;;
            error)
                local reason
                reason=$(python3 -c "
import sys,json
d=json.loads(sys.stdin.read())
e=d.get('error') or {}
print(e.get('reason','unknown'))
"                        <<< "$status_json" 2>/dev/null || echo "unknown")
                error "Scheduler reported error: ${reason}"
                return 1
                ;;
            idle)
                warn "Scheduler is idle — was it stopped externally?"
                return 1
                ;;
        esac

        if (( elapsed >= max_seconds )); then
            error "Hard timeout: scheduler did not complete within ${MAX_WAIT_HOURS}h"
            return 1
        fi

        sleep "$POLL_INTERVAL_SECONDS"
        elapsed=$(( elapsed + POLL_INTERVAL_SECONDS ))
    done
}

# ─── Main ─────────────────────────────────────────────────────────────────────

main() {
    local per_scenario_minutes=$(( (WARMUP_MINUTES + EXPERIMENT_MINUTES + COOLDOWN_MINUTES) * REPETITIONS * ${#LOAD_RATES[@]} ))
    local total_hours=$(( ${#SCENARIOS[@]} * per_scenario_minutes / 60 ))

    banner "Experiment Suite — $(date +'%Y-%m-%d %H:%M')"
    log "Scenarios : ${#SCENARIOS[@]} (N V1 V2 V3 Z1 Z2 Z3)"
    log "Load rates: ${LOAD_RATES[*]} msg/s  ×  ${REPETITIONS} reps  ×  ${#LOAD_RATES[@]} rates"
    log "Timing    : warmup=${WARMUP_MINUTES}min  experiment=${EXPERIMENT_MINUTES}min  cooldown=${COOLDOWN_MINUTES}min"
    log "Estimated : ~${total_hours}h total  (~${per_scenario_minutes}min/scenario)"
    log "Log file  : ${LOG_FILE}"
    [[ -n "$START_FROM" ]] && warn "Resuming from scenario: ${START_FROM}"
    $DRY_RUN && warn "DRY RUN — no changes will be made"

    export ANSIBLE_CONFIG="${PROJECT_DIR}/ansible.cfg"

    local skip=true
    [[ -z "$START_FROM" ]] && skip=false

    local scenario_num=0 total=${#SCENARIOS[@]}
    for scenario_def in "${SCENARIOS[@]}"; do
        read -r label strategy param_field param_value <<< "$scenario_def"
        scenario_num=$(( scenario_num + 1 ))

        if $skip; then
            [[ "$label" == "$START_FROM" ]] && skip=false || { log "Skipping ${label} (before --start-from)"; continue; }
        fi

        banner "Scenario ${scenario_num}/${total}: ${label}  [strategy=${strategy}, param=${param_value}]"

        # ── 1. Cleanup ──────────────────────────────────────────────────────
        log "[1/6] Destroying Hetzner environment..."
        if $DRY_RUN; then
            log "  (dry-run) would run: run_cleanup_environment.sh"
        else
            bash "${PROJECT_DIR}/run_cleanup_environment.sh" 2>&1 | tee -a "$LOG_FILE"
            success "Environment destroyed."
        fi

        # ── 2. Deploy ───────────────────────────────────────────────────────
        log "[2/6] Deploying fresh environment..."
        if $DRY_RUN; then
            log "  (dry-run) would run: run_deploy_environment.sh"
        else
            bash "${PROJECT_DIR}/run_deploy_environment.sh" 2>&1 | tee -a "$LOG_FILE"
            success "Environment deployed."
        fi

        # ── 3. Resolve host IPs ─────────────────────────────────────────────
        log "[3/6] Resolving host IPs..."
        local control_ip gateway_ip web_ui_url
        if $DRY_RUN; then
            control_ip="<dry-run>"
            gateway_ip="<dry-run>"
        else
            control_ip=$(get_control_ip) || { error "Cannot resolve control node IP — aborting suite."; exit 1; }
            gateway_ip=$(get_gateway_ip)  || { error "Cannot resolve gateway IP — aborting suite."; exit 1; }
        fi
        web_ui_url="http://${control_ip}:${WEB_UI_PORT}"
        log "  Control node: ${control_ip}"
        log "  Gateway:      ${gateway_ip}"
        log "  Web UI:       ${web_ui_url}"

        if ! $DRY_RUN; then
            wait_for_web_ui "$web_ui_url" || exit 1
        fi

        # ── 4. Reset + load queue ───────────────────────────────────────────
        log "[4/6] Loading scheduler queue..."
        local queue_payload
        queue_payload=$(build_queue_payload "$label" "$strategy" "$param_field" "$param_value")
        log "  Queue payload preview:"
        echo "$queue_payload" | python3 -c "
import sys,json
for e in json.load(sys.stdin)['experiments']:
    print(f'    {e[\"label\"]:20s}  mw={e[\"middleware_config\"]}')
" | tee -a "$LOG_FILE"

        if $DRY_RUN; then
            log "  (dry-run) would POST to ${web_ui_url}/api/scheduler/queue"
        else
            curl -sf -X POST "${web_ui_url}/api/scheduler/reset" \
                -H "Content-Type: application/json" > /dev/null \
                || warn "Scheduler reset returned non-200 (may be fine on first run)"

            local queue_resp
            queue_resp=$(curl -sf -X POST "${web_ui_url}/api/scheduler/queue" \
                -H "Content-Type: application/json" \
                -d "$queue_payload") \
                || { error "Failed to set scheduler queue — aborting suite."; exit 1; }

            local queue_size
            queue_size=$(python3 -c "import sys,json; print(len(json.loads(sys.stdin.read())['queue']))" <<< "$queue_resp")
            success "Queue loaded: ${queue_size} experiments."
        fi

        # ── 5. Start scheduler ──────────────────────────────────────────────
        log "[5/6] Starting scheduler..."
        if $DRY_RUN; then
            log "  (dry-run) would POST to ${web_ui_url}/api/scheduler/start"
        else
            curl -sf -X POST "${web_ui_url}/api/scheduler/start" \
                -H "Content-Type: application/json" > /dev/null \
                || { error "Failed to start scheduler — aborting suite."; exit 1; }
            success "Scheduler running. Polling every ${POLL_INTERVAL_SECONDS}s..."

            if ! poll_scheduler "$web_ui_url"; then
                error "Scenario ${label} did not complete cleanly. Saving partial data and continuing."
            fi
        fi

        # ── 6. Fetch and archive CSVs ───────────────────────────────────────
        log "[6/6] Fetching CSV data before teardown..."
        local backup_dir="${PROJECT_DIR}/.tmp_plots/${label}"
        if $DRY_RUN; then
            log "  (dry-run) would run: run_fetch_plots.sh --fetch-csv"
            log "  (dry-run) would back up to: ${backup_dir}/"
        else
            log "  Waiting 30s for middleware to finish writing CSVs..."
            sleep 30

            local fetch_ok=false
            for attempt in 1 2 3 4 5; do
                log "  Fetch attempt ${attempt}/5..."
                if bash "${PROJECT_DIR}/run_fetch_plots.sh" --fetch-csv 2>&1 | tee -a "$LOG_FILE"; then
                    fetch_ok=true
                    break
                fi
                if [[ $attempt -lt 5 ]]; then
                    warn "  Fetch attempt ${attempt} failed, retrying in 60s..."
                    sleep 60
                fi
            done

            if ! $fetch_ok; then
                error "All 5 fetch attempts failed — CSV data not fully retrieved for scenario ${label}."
                error "Environment is preserved to protect data. Retrieve CSVs manually, then resume:"
                error "  curl http://${gateway_ip}:8000/experiment-data/batch_metrics.csv -o ${backup_dir}/batch_metrics.csv"
                error "  curl http://${gateway_ip}:8000/experiment-data/e2e_metrics.csv    -o ${backup_dir}/e2e_metrics.csv"
                error "  Then: bash run_experiment_suite.sh --start-from <NEXT_SCENARIO>"
                exit 1
            fi

            mkdir -p "$backup_dir"
            cp "${PROJECT_DIR}/.tmp_plots/batch_metrics.csv" \
               "${PROJECT_DIR}/.tmp_plots/e2e_metrics.csv" \
               "$backup_dir/"
            success "CSVs archived to ${backup_dir}/"
            ls -lh "${backup_dir}/"*.csv | tee -a "$LOG_FILE"
        fi

        success "Scenario ${label} finished at $(date +'%H:%M:%S')."
    done

    banner "Suite Complete — $(date +'%Y-%m-%d %H:%M')"
    success "All ${total} scenarios finished."
    log "Per-scenario CSVs saved under: ${PROJECT_DIR}/.tmp_plots/<LABEL>/"
    log "Full run log: ${LOG_FILE}"
    log ""
    log "Next steps:"
    log "  1. Review per-scenario CSVs in .tmp_plots/<LABEL>/"
    log "  2. Upload to a fresh environment and generate plots:"
    log "       bash run_deploy_environment.sh"
    log "       bash run_fetch_plots.sh --upload-csv   # for the combined CSV set"
    log "       bash run_fetch_plots.sh --generate"
}

main "$@"
