#!/usr/bin/env bash
###############################################################################
# check-data.sh
#
# Kafka cluster health and data report for SwipeNest.
#
# What this script shows (in table format):
#   SECTION 1 — Broker Info
#       Instance ID, Node ID, Private IP, Public IP, Instance Type,
#       Storage, AWS Region, SSH reachability
#
#   SECTION 2 — Topic / Partition / Data Summary
#       For every topic × partition:
#         - Leader broker (private IP)
#         - Replicas & ISR (private IPs)
#         - Low offset (start index)
#         - High offset (current index / end)
#         - Message count  (high - low)
#       Plus per-topic subtotals and grand total.
#
#   SECTION 3 — Consumer Group Lag
#       Per topic × partition:
#         - Committed offset (where consumer is)
#         - End offset (latest produced)
#         - Lag (how many messages behind)
#
# Reads broker metadata from:
#   ../brokers.json           — public + private IPs
#   scripts/kafka-instances.json — instance IDs, node IDs, instance type
#
# The JS report (check-data.js) is invoked after the broker table.
# It handles SSH tunnelling + KafkaJS Admin API for the live data.
#
# Usage:
#   ./scripts/check-data.sh
#   KAFKA_GROUP_ID=my-group ./scripts/check-data.sh
#   SKIP_LOOPBACK=1 ./scripts/check-data.sh   # single-broker only
###############################################################################

set -euo pipefail

# ─── Locate paths ─────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
BROKERS_JSON="${PROJECT_ROOT}/brokers.json"
INSTANCES_JSON="${SCRIPT_DIR}/kafka-instances.json"
PEM_KEY="${HOME}/.ssh/ec2-key-pair.pem"

# ─── Colours ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'

info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERR]${NC}   $*" >&2; }

# ─── Header ───────────────────────────────────────────────────────────────────
echo -e "\n${BOLD}${CYAN}"
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║       SwipeNest — Kafka Cluster Data Report                      ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# ─── Preflight ────────────────────────────────────────────────────────────────
for cmd in jq node aws ssh; do
    command -v "$cmd" &>/dev/null || { error "Required tool not found: $cmd"; exit 1; }
done

[[ -f "$BROKERS_JSON" ]] || { error "brokers.json not found at: ${BROKERS_JSON}"; exit 1; }

# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 1 — BROKER INFO TABLE
# ═══════════════════════════════════════════════════════════════════════════════
echo -e "${BOLD}${CYAN}══════ SECTION 1 — Broker Info ══════${NC}\n"

# Read brokers.json
mapfile -t PRIV_IPS < <(jq -r '.brokers[].privateIp' "$BROKERS_JSON")
mapfile -t PUB_IPS  < <(jq -r '.brokers[].publicIp'  "$BROKERS_JSON")
BROKER_COUNT="${#PRIV_IPS[@]}"

# Read kafka-instances.json if it exists
declare -A INST_ID_MAP  # privateIp → instance_id
declare -A NODE_ID_MAP  # privateIp → node_id
INSTANCE_TYPE="unknown"
STORAGE_GB="unknown"
AWS_REGION="ap-south-1"

if [[ -f "$INSTANCES_JSON" ]]; then
    INSTANCE_TYPE=$(jq -r '.instance_type // "unknown"' "$INSTANCES_JSON")
    STORAGE_GB=$(jq    -r '.storage_gb    // "unknown"' "$INSTANCES_JSON")
    AWS_REGION=$(jq    -r '.region        // "ap-south-1"' "$INSTANCES_JSON")
    inst_count=$(jq '.instances | length' "$INSTANCES_JSON")
    for (( i=0; i<inst_count; i++ )); do
        priv=$(jq -r ".instances[$i].private_ip" "$INSTANCES_JSON")
        inst=$(jq -r ".instances[$i].instance_id" "$INSTANCES_JSON")
        nid=$(jq  -r ".instances[$i].node_id"    "$INSTANCES_JSON")
        INST_ID_MAP["$priv"]="$inst"
        NODE_ID_MAP["$priv"]="$nid"
    done
fi

# SSH reachability check (synchronous, timeout 5s per broker)
declare -A SSH_STATUS
for pub_ip in "${PUB_IPS[@]}"; do
    if ssh -i "$PEM_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=5 \
           -o BatchMode=yes -o LogLevel=ERROR "ubuntu@${pub_ip}" \
           "echo ok" &>/dev/null; then
        SSH_STATUS["$pub_ip"]="${GREEN}UP${NC}"
    else
        SSH_STATUS["$pub_ip"]="${RED}DOWN${NC}"
    fi
done

# Print broker table
W=104
BAR=$(printf '═%.0s' $(seq 1 $W))
bar=$(printf '─%.0s' $(seq 1 $W))

printf "╔%s╗\n" "$BAR"
printf "║  %-5s  %-22s  %-16s  %-16s  %-14s  %-8s  %-10s  %-5s  ║\n" \
    "Node" "Instance ID" "Private IP" "Public IP" "Type" "Storage" "Region" "SSH"
printf "╠%s╣\n" "$BAR"

for i in "${!PRIV_IPS[@]}"; do
    priv="${PRIV_IPS[$i]}"
    pub="${PUB_IPS[$i]}"
    inst="${INST_ID_MAP[$priv]:-unknown}"
    nid="${NODE_ID_MAP[$priv]:-$(( i+1 ))}"
    ssh_stat="${SSH_STATUS[$pub]:-?}"
    printf "║  %-5s  %-22s  %-16s  %-16s  %-14s  %-8s  %-10s  " \
        "$nid" "$inst" "$priv" "$pub" "$INSTANCE_TYPE" "${STORAGE_GB}GB" "$AWS_REGION"
    echo -e "${ssh_stat}  ║"
done

printf "╠%s╣\n" "$BAR"
printf "║  %-${W}s║\n" "  Total brokers: ${BROKER_COUNT}   Region: ${AWS_REGION}   Instance type: ${INSTANCE_TYPE}   Storage: ${STORAGE_GB}GB per broker"
printf "╚%s╝\n" "$BAR"
echo ""

# ─── Quick Kafka process check on each broker ─────────────────────────────────
echo -e "${BOLD}Kafka process status per broker:${NC}"
for i in "${!PRIV_IPS[@]}"; do
    priv="${PRIV_IPS[$i]}"
    pub="${PUB_IPS[$i]}"
    nid="${NODE_ID_MAP[$priv]:-$(( i+1 ))}"
    printf "  node.id=%-3s  %s  →  " "$nid" "$priv"
    result=$(ssh -i "$PEM_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=5 \
        -o BatchMode=yes -o LogLevel=ERROR "ubuntu@${pub}" \
        "pgrep -f kafka.Kafka > /dev/null 2>&1 && \
         ss -tlnp 2>/dev/null | grep -q ':9092' && echo 'RUNNING (port 9092 open)' || \
         echo 'STOPPED (port 9092 not listening)'" 2>/dev/null \
        || echo "SSH_UNREACHABLE")
    if echo "$result" | grep -q "RUNNING"; then
        echo -e "${GREEN}${result}${NC}"
    elif echo "$result" | grep -q "SSH_UNREACHABLE"; then
        echo -e "${RED}SSH unreachable${NC}"
    else
        echo -e "${RED}${result}${NC}"
    fi
done
echo ""

# ─── Broker metadata from Kafka Admin ─────────────────────────────────────────
echo -e "${BOLD}Fetching broker metadata via SSH (server.properties):${NC}"

printf "  ╔%-50s╦%-20s╦%-16s╗\n" \
    "══════════════════════════════════════════════════" \
    "════════════════════" \
    "════════════════"
printf "  ║  %-48s║  %-18s║  %-14s║\n" "Property" "Value" "Broker (priv IP)"
printf "  ╠%-50s╬%-20s╬%-16s╣\n" \
    "══════════════════════════════════════════════════" \
    "════════════════════" \
    "════════════════"

for i in "${!PRIV_IPS[@]}"; do
    priv="${PRIV_IPS[$i]}"
    pub="${PUB_IPS[$i]}"
    props=$(ssh -i "$PEM_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=5 \
        -o BatchMode=yes -o LogLevel=ERROR "ubuntu@${pub}" \
        "grep -E '^(node.id|process.roles|listeners|log.dirs|log.retention.hours|num.partitions|offsets.topic.replication.factor)=' \
         /opt/kafka/config/kraft/server.properties 2>/dev/null | head -10" 2>/dev/null \
        || echo "unreachable=true")

    first=true
    while IFS='=' read -r key val; do
        [[ -z "$key" ]] && continue
        if [[ "$first" == "true" ]]; then
            printf "  ║  %-48s║  %-18s║  %-14s║\n" "${key}=${val}" "" "$priv"
            first=false
        else
            printf "  ║  %-48s║  %-18s║  %-14s║\n" "${key}=${val}" "" ""
        fi
    done <<< "$props"

    [[ $i -lt $(( BROKER_COUNT - 1 )) ]] && \
        printf "  ╠%-50s╬%-20s╬%-16s╣\n" \
            "──────────────────────────────────────────────────" \
            "────────────────────" \
            "────────────────"
done

printf "  ╚%-50s╩%-20s╩%-16s╝\n" \
    "══════════════════════════════════════════════════" \
    "════════════════════" \
    "════════════════"
echo ""

# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 2 + 3 — Topic data, partition offsets, consumer lag
#  (handled by check-data.js via SSH tunnels + KafkaJS Admin API)
# ═══════════════════════════════════════════════════════════════════════════════
echo -e "${BOLD}${CYAN}══════ SECTION 2 & 3 — Topics, Partition Data, Consumer Lag ══════${NC}"
echo -e "${DIM}  (Opening SSH tunnels → running KafkaJS Admin report...)${NC}\n"

cd "$PROJECT_ROOT"
node scripts/check-data.js

echo -e "\n${BOLD}${GREEN}Report complete.${NC}\n"
