#!/usr/bin/env bash
# =============================================================================
# load-data.sh
#
# Interactive wrapper for load-data.js.
# Asks how many analytics events to push to Kafka, then calls the Node.js
# script which uses a direct KafkaJS producer (no HTTP, no app dependency).
#
# Events are distributed evenly across the 4 topics:
#   video_view, post_impression, video_watch_progress, post_likes
#
# Usage:
#   ./scripts/load-data.sh
#   TOTAL_RECORDS=5000 ./scripts/load-data.sh   # non-interactive override
# =============================================================================

set -euo pipefail

# ─── Locate project root ──────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ─── Colour helpers ───────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

banner()  { echo -e "\n${CYAN}${BOLD}══════════════════════════════════════════════${NC}"; \
            echo -e "${CYAN}${BOLD}  $*${NC}"; \
            echo -e "${CYAN}${BOLD}══════════════════════════════════════════════${NC}"; }
info()    { echo -e "${CYAN}[INFO]${NC} $*"; }
success() { echo -e "${GREEN}[OK]${NC}   $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
die()     { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

# ─── Prerequisites ────────────────────────────────────────────────────────────
command -v aws  >/dev/null 2>&1 || die "aws CLI not found — install it first"
command -v node >/dev/null 2>&1 || die "node not found — install Node.js first"
command -v ssh  >/dev/null 2>&1 || die "ssh not found"

[[ -f "${HOME}/.ssh/ec2-key-pair.pem" ]] ||
    die "PEM key not found at ~/.ssh/ec2-key-pair.pem"

[[ -f "${PROJECT_ROOT}/package.json" ]] ||
    die "package.json not found at ${PROJECT_ROOT} — wrong project root?"

LOADER="${SCRIPT_DIR}/load-data.js"
[[ -f "${LOADER}" ]] || die "load-data.js not found at ${LOADER}"

# ─── Banner ───────────────────────────────────────────────────────────────────
banner "SwipeNest Kafka Data Loader"
info "Project root : ${PROJECT_ROOT}"
info "Topics       : video_view, post_impression, video_watch_progress, post_likes"
echo ""

# ─── Ask: number of records ───────────────────────────────────────────────────
if [[ -n "${TOTAL_RECORDS:-}" ]]; then
    info "TOTAL_RECORDS is set via environment: ${TOTAL_RECORDS}"
else
    while true; do
        printf "  Number of records to load [default: 2000]: "
        read -r INPUT_RECORDS
        INPUT_RECORDS="${INPUT_RECORDS:-2000}"
        if [[ "$INPUT_RECORDS" =~ ^[1-9][0-9]*$ ]]; then
            break
        fi
        warn "  Please enter a positive integer (e.g. 500, 2000, 10000)."
    done
    TOTAL_RECORDS="$INPUT_RECORDS"
fi

# Per-topic breakdown (informational)
PER_TOPIC=$(( TOTAL_RECORDS / 4 ))
REMAINDER=$(( TOTAL_RECORDS % 4 ))
echo ""
info "  Total records   : ${TOTAL_RECORDS}"
info "  Base per topic  : ${PER_TOPIC}"
if [[ "$REMAINDER" -gt 0 ]]; then
    info "  Remainder ${REMAINDER} event(s) added to 'post_likes'"
fi

# ─── Confirm ──────────────────────────────────────────────────────────────────
echo ""
echo -e "  ${BOLD}Ready to push ${TOTAL_RECORDS} records to Kafka.${NC}"
printf "  Proceed? [Y/n]: "
read -r CONFIRM
if [[ "${CONFIRM,,}" == "n" || "${CONFIRM,,}" == "no" ]]; then
    info "Aborted by user."
    exit 0
fi

# ─── Run ──────────────────────────────────────────────────────────────────────
echo ""
info "Starting load-data.js..."
echo ""

export TOTAL_RECORDS
export AWS_REGION="${AWS_REGION:-ap-south-1}"
export NODE_ENV="test"

cd "${PROJECT_ROOT}"
if node "${LOADER}"; then
    echo ""
    success "Data load completed successfully."
else
    EXIT_CODE=$?
    echo ""
    die "Data load failed (exit code ${EXIT_CODE})."
fi
