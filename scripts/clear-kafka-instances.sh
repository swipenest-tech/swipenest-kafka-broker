#!/usr/bin/env bash
###############################################################################
# clear-instances.sh
#
# Lists all Kafka broker EC2 instances and terminates selected ones.
#
# Usage:
#   ./scripts/clear-instances.sh [--region ap-south-1]
#
# Flow:
#   1. Lists all EC2 instances tagged Role=kafka-broker (all non-terminated states)
#   2. Displays a numbered table: #, Instance ID, Name, Type, State, IPs, Launched
#   3. Prompts for comma-separated numbers (e.g. "1,3") or "all"
#   4. Shows selected instances and asks yes/no confirmation
#   5. Terminates selected instances
#   6. Removes scripts/kafka-instances.json if all brokers are gone
###############################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
INSTANCES_FILE="${SCRIPT_DIR}/kafka-instances.json"

AWS_REGION="${AWS_REGION:-ap-south-1}"

# ─── Colors ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERR]${NC}   $*" >&2; }
die()     { error "$*"; exit 1; }

# ─── Parse flags ──────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case $1 in
        --region) AWS_REGION="$2"; shift 2 ;;
        --help|-h)
            sed -n '2,20p' "$0" | sed 's/^# *//'
            exit 0 ;;
        *) die "Unknown argument: $1. Use --help." ;;
    esac
done

export AWS_DEFAULT_REGION="$AWS_REGION"

# ─── Header ───────────────────────────────────────────────────────────────────
echo -e "${BOLD}${BLUE}"
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║    SwipeNest — Kafka Instance Manager                            ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# ─── Preflight ────────────────────────────────────────────────────────────────
command -v aws &>/dev/null || die "aws CLI not found"
command -v jq  &>/dev/null || die "jq not found"
aws sts get-caller-identity &>/dev/null || die "AWS credentials not configured."

# ─── Fetch instances ──────────────────────────────────────────────────────────
echo -e "${BOLD}${BLUE}====== Kafka Broker Instances (${AWS_REGION}) ======${NC}"
echo ""

instance_json=$(aws ec2 describe-instances \
    --region "$AWS_REGION" \
    --filters \
        "Name=tag:Role,Values=kafka-broker" \
        "Name=instance-state-name,Values=pending,running,stopping,stopped" \
    --query "Reservations[].Instances[].{id:InstanceId,name:Tags[?Key=='Name']|[0].Value,type:InstanceType,state:State.Name,priv:PrivateIpAddress,pub:PublicIpAddress,launched:LaunchTime}" \
    --output json 2>/dev/null)

total=$(echo "$instance_json" | jq 'length')

if [[ "$total" -eq 0 ]]; then
    info "No Kafka broker instances found (tag Role=kafka-broker, region ${AWS_REGION})."
    exit 0
fi

# Extract fields into arrays
mapfile -t ALL_IDS      < <(echo "$instance_json" | jq -r '.[].id')
mapfile -t ALL_NAMES    < <(echo "$instance_json" | jq -r '.[].name // "unnamed"')
mapfile -t ALL_TYPES    < <(echo "$instance_json" | jq -r '.[].type // "unknown"')
mapfile -t ALL_STATES   < <(echo "$instance_json" | jq -r '.[].state // "unknown"')
mapfile -t ALL_PRIV_IPS < <(echo "$instance_json" | jq -r '.[].priv // "N/A"')
mapfile -t ALL_PUB_IPS  < <(echo "$instance_json" | jq -r '.[].pub  // "N/A"')
mapfile -t ALL_LAUNCH   < <(echo "$instance_json" | jq -r '.[].launched // ""')

# ─── Print table ──────────────────────────────────────────────────────────────
printf "  %-4s  %-22s  %-20s  %-13s  %-10s  %-16s  %-16s  %s\n" \
    "#" "Instance ID" "Name" "Type" "State" "Private IP" "Public IP" "Launched"
printf "  %-4s  %-22s  %-20s  %-13s  %-10s  %-16s  %-16s  %s\n" \
    "----" "----------------------" "--------------------" "-------------" \
    "----------" "----------------" "----------------" "----------"

for i in "${!ALL_IDS[@]}"; do
    num=$(( i + 1 ))
    name="${ALL_NAMES[$i]}"
    [[ "${#name}" -gt 18 ]] && name="${name:0:17}…"
    launch="${ALL_LAUNCH[$i]:0:10}"

    printf "  %-4s  %-22s  %-20s  %-13s  %-10s  %-16s  %-16s  %s\n" \
        "$num" \
        "${ALL_IDS[$i]}" \
        "$name" \
        "${ALL_TYPES[$i]}" \
        "${ALL_STATES[$i]}" \
        "${ALL_PRIV_IPS[$i]}" \
        "${ALL_PUB_IPS[$i]}" \
        "$launch"
done

echo ""
info "Found ${total} instance(s) in ${AWS_REGION}"
echo ""

# ─── Prompt for selection ─────────────────────────────────────────────────────
warn "Enter comma-separated numbers to TERMINATE (e.g. 1,3), or 'all' to terminate all."
warn "Press Enter or type 'none' to exit without changes."
echo ""
printf "Instances to terminate: "
read -r SELECTION

if [[ -z "$SELECTION" ]] || [[ "$SELECTION" == "none" ]]; then
    info "No instances selected. Exiting."
    exit 0
fi

# ─── Resolve selection ────────────────────────────────────────────────────────
SELECTED_IDS=()
SELECTED_NAMES=()
SELECTED_TYPES=()
SELECTED_STATES=()

if [[ "$SELECTION" == "all" ]]; then
    SELECTED_IDS=("${ALL_IDS[@]}")
    SELECTED_NAMES=("${ALL_NAMES[@]}")
    SELECTED_TYPES=("${ALL_TYPES[@]}")
    SELECTED_STATES=("${ALL_STATES[@]}")
else
    IFS=',' read -ra NUMS <<< "$SELECTION"
    for raw_num in "${NUMS[@]}"; do
        num=$(echo "$raw_num" | tr -d '[:space:]')
        [[ "$num" =~ ^[0-9]+$ ]] \
            || die "Invalid selection '${num}' — use numbers separated by commas"
        idx=$(( num - 1 ))
        [[ "$idx" -ge 0 ]] && [[ "$idx" -lt "$total" ]] \
            || die "Number ${num} is out of range (valid: 1–${total})"
        SELECTED_IDS+=("${ALL_IDS[$idx]}")
        SELECTED_NAMES+=("${ALL_NAMES[$idx]}")
        SELECTED_TYPES+=("${ALL_TYPES[$idx]}")
        SELECTED_STATES+=("${ALL_STATES[$idx]}")
    done
fi

[[ "${#SELECTED_IDS[@]}" -eq 0 ]] && { info "Nothing selected. Exiting."; exit 0; }

# ─── Show selected + confirm ──────────────────────────────────────────────────
echo ""
echo -e "${RED}${BOLD}The following instance(s) will be PERMANENTLY TERMINATED:${NC}"
echo ""
printf "  %-22s  %-20s  %-13s  %s\n" "Instance ID" "Name" "Type" "State"
printf "  %-22s  %-20s  %-13s  %s\n" "----------------------" "--------------------" "-------------" "----------"
for i in "${!SELECTED_IDS[@]}"; do
    printf "  %-22s  %-20s  %-13s  %s\n" \
        "${SELECTED_IDS[$i]}" \
        "${SELECTED_NAMES[$i]}" \
        "${SELECTED_TYPES[$i]}" \
        "${SELECTED_STATES[$i]}"
done
echo ""
warn "This action is IRREVERSIBLE. All data on terminated instances will be lost."
echo ""
printf "Are you sure? [y/N]: "
read -r CONFIRM

if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
    info "Aborted. No instances were terminated."
    exit 0
fi

# ─── Terminate ────────────────────────────────────────────────────────────────
echo ""
info "Terminating ${#SELECTED_IDS[@]} instance(s)..."

result=$(aws ec2 terminate-instances \
    --instance-ids "${SELECTED_IDS[@]}" \
    --output json 2>/dev/null)

echo "$result" | jq -r \
    '.TerminatingInstances[] | "  \(.InstanceId): \(.PreviousState.Name) → \(.CurrentState.Name)"'

echo ""
success "Termination initiated for ${#SELECTED_IDS[@]} instance(s)."
info "Instances will reach 'terminated' state within ~1 minute."

# ─── Clean up local kafka-instances.json ──────────────────────────────────────
if [[ -f "$INSTANCES_FILE" ]]; then
    remaining=$(( total - ${#SELECTED_IDS[@]} ))
    if [[ "$remaining" -le 0 ]]; then
        rm -f "$INSTANCES_FILE"
        info "Removed scripts/kafka-instances.json (all instances terminated)"
    else
        warn "scripts/kafka-instances.json still references old instances — re-run deploy-instances.sh to refresh."
    fi
fi
echo ""
