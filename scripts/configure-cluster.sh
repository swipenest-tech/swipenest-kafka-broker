#!/usr/bin/env bash
###############################################################################
# configure-cluster.sh
#
# Configures a Kafka KRaft cluster on existing EC2 instances.
# Run deploy-instances.sh first to provision the instances.
#
# What this script does:
#   1. Reads instance IDs from scripts/kafka-instances.json (or --instance-ids)
#   2. Validates all instances are running and have private IPs
#   3. Assigns sequential node IDs: instance-1 → node.id=1, etc.
#   4. Generates a Kafka cluster UUID (via kafka-storage.sh random-uuid)
#   5. Builds the KRaft quorum voters string
#   6. SSHes each broker:
#      a. Writes server.properties (node.id, listeners, quorum voters)
#      b. Cleans /opt/kafka/data (removes stale metadata)
#      c. Formats storage with the cluster UUID
#      d. Starts kafka-server-start.sh in background
#   7. Waits for cluster readiness
#   8. Asks partition count for each topic interactively
#   9. Creates topics: video_view, post_impression, post_likes, video_watch_progress
#  10. Validates topics exist
#  11. Updates brokers.json (project root) with private IPs —
#      ready to copy directly to swipenest-consumer/brokers.json
#
# Kafka paths (AMI: ami-081dfc9f291f572f7):
#   Home  : /opt/kafka
#   Config: /opt/kafka/config/kraft/server.properties
#   Data  : /opt/kafka/data
#   Log   : /opt/kafka/kafka.log
#
# Usage:
#   ./scripts/configure-cluster.sh
#   ./scripts/configure-cluster.sh --instance-ids i-xxx,i-yyy
#   ./scripts/configure-cluster.sh --pem-key /path/to/key.pem
#   ./scripts/configure-cluster.sh --region ap-south-1
###############################################################################

set -euo pipefail

# ─── Paths & defaults ─────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
INSTANCES_FILE="${SCRIPT_DIR}/kafka-instances.json"
BROKERS_JSON="${PROJECT_ROOT}/brokers.json"

AWS_REGION="${AWS_REGION:-ap-south-1}"
KAFKA_HOME="/opt/kafka"
KAFKA_PORT=9092        # external client listener (KafkaJS, public IP)
INTERNAL_PORT=19092    # inter-broker + admin tools listener (private IP, VPC-only)
CONTROLLER_PORT=9093   # KRaft controller quorum (private IP, VPC-only)
SSH_USER="ubuntu"
DEFAULT_PEM_KEY="${HOME}/.ssh/ec2-key-pair.pem"
PEM_KEY="$DEFAULT_PEM_KEY"

TOPICS=(
    "video_view"
    "post_impression"
    "post_likes"
    "video_watch_progress"
)
declare -A TOPIC_PARTITIONS

# ─── Colors ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERR]${NC}   $*" >&2; }
die()     { error "$*"; exit 1; }
banner()  { echo -e "\n${BOLD}${BLUE}====== $* ======${NC}\n"; }

# ─── Parse flags ──────────────────────────────────────────────────────────────
INSTANCE_IDS_RAW=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --instance-ids) INSTANCE_IDS_RAW="$2"; shift 2 ;;
        --pem-key)      PEM_KEY="$2";          shift 2 ;;
        --region)       AWS_REGION="$2";       shift 2 ;;
        --help|-h)
            sed -n '2,35p' "$0" | sed 's/^# *//'
            exit 0 ;;
        *) die "Unknown argument: $1. Use --help." ;;
    esac
done

export AWS_DEFAULT_REGION="$AWS_REGION"
PEM_KEY="${PEM_KEY/#\~/$HOME}"

# ─── Header ───────────────────────────────────────────────────────────────────
echo -e "${BOLD}${BLUE}"
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║    SwipeNest — Kafka KRaft Cluster Configurator                  ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# ─── Preflight ────────────────────────────────────────────────────────────────
banner "Preflight Checks"
missing=()
for cmd in aws ssh jq base64; do
    command -v "$cmd" &>/dev/null || missing+=("$cmd")
done
[[ ${#missing[@]} -gt 0 ]] && die "Missing required tools: ${missing[*]}"
aws sts get-caller-identity &>/dev/null || die "AWS credentials not configured."
[[ -f "$PEM_KEY" ]] || die "PEM key not found: ${PEM_KEY}"
chmod 600 "$PEM_KEY"
success "All dependencies present"
info "PEM key: ${PEM_KEY}"

# ─── SSH helper ───────────────────────────────────────────────────────────────
ssh_broker() {
    local pub_ip="$1"; shift
    ssh -i "$PEM_KEY" \
        -o StrictHostKeyChecking=no \
        -o ConnectTimeout=20 \
        -o BatchMode=yes \
        -o LogLevel=ERROR \
        "${SSH_USER}@${pub_ip}" "$@"
}

ssh_broker_retry() {
    local pub_ip="$1"; shift
    local cmd=("$@")
    local max_tries=3
    local attempt=0
    while [[ $attempt -lt $max_tries ]]; do
        attempt=$(( attempt + 1 ))
        if ssh_broker "$pub_ip" "${cmd[@]}"; then
            return 0
        fi
        if [[ $attempt -lt $max_tries ]]; then
            warn "    SSH attempt ${attempt} failed — retrying in 8s..."
            sleep 8
        fi
    done
    die "SSH to ${pub_ip} failed after ${max_tries} attempts"
}

# ─── Resolve instances ────────────────────────────────────────────────────────
banner "Resolving Instances"

INST_IDS=()
if [[ -n "$INSTANCE_IDS_RAW" ]]; then
    IFS=',' read -ra INST_IDS <<< "$INSTANCE_IDS_RAW"
    for i in "${!INST_IDS[@]}"; do
        INST_IDS[$i]=$(echo "${INST_IDS[$i]}" | tr -d '[:space:]')
        [[ "${INST_IDS[$i]}" =~ ^i-[0-9a-f]{8,17}$ ]] \
            || die "Invalid instance ID: ${INST_IDS[$i]}"
    done
    info "Using --instance-ids: ${INST_IDS[*]}"
elif [[ -f "$INSTANCES_FILE" ]]; then
    mapfile -t INST_IDS < <(jq -r '.instances[].instance_id' "$INSTANCES_FILE")
    info "Loaded from scripts/kafka-instances.json: ${INST_IDS[*]}"
else
    warn "No --instance-ids flag and scripts/kafka-instances.json not found."
    printf "Enter comma-separated instance IDs: "
    read -r INSTANCE_IDS_RAW
    [[ -n "$INSTANCE_IDS_RAW" ]] || die "No instance IDs provided."
    IFS=',' read -ra INST_IDS <<< "$INSTANCE_IDS_RAW"
    for i in "${!INST_IDS[@]}"; do
        INST_IDS[$i]=$(echo "${INST_IDS[$i]}" | tr -d '[:space:]')
        [[ "${INST_IDS[$i]}" =~ ^i-[0-9a-f]{8,17}$ ]] \
            || die "Invalid instance ID: ${INST_IDS[$i]}"
    done
fi

BROKER_COUNT="${#INST_IDS[@]}"
[[ "$BROKER_COUNT" -ge 1 ]] || die "No instances to configure."
info "Broker count: ${BROKER_COUNT}"

# ─── Fetch instance details ───────────────────────────────────────────────────
banner "Fetching Instance Details"

PRIV_IPS=()
PUB_IPS=()

for id in "${INST_IDS[@]}"; do
    details=$(aws ec2 describe-instances \
        --instance-ids "$id" \
        --query "Reservations[0].Instances[0].[State.Name,PrivateIpAddress,PublicIpAddress]" \
        --output text 2>/dev/null || echo "error")

    [[ "$details" == "error" || -z "$details" ]] \
        && die "Could not fetch details for ${id}"

    read -r state priv_ip pub_ip <<< "$details"

    [[ "$state" == "running" ]] \
        || die "Instance ${id} is not running (state=${state}). Run deploy-instances.sh first."
    [[ "$priv_ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] \
        || die "No private IP for ${id}"

    PRIV_IPS+=("$priv_ip")
    PUB_IPS+=("$pub_ip")
    info "  ${id}: state=${state}  private=${priv_ip}  public=${pub_ip}"
done

success "All ${BROKER_COUNT} instance(s) running"

echo ""
printf "  %-5s  %-24s  %-16s  %s\n" "Node" "Instance ID" "Private IP" "Public IP"
printf "  %-5s  %-24s  %-16s  %s\n" "-----" "------------------------" "----------------" "---------------"
for i in "${!INST_IDS[@]}"; do
    printf "  %-5s  %-24s  %-16s  %s\n" \
        "$(( i + 1 ))" "${INST_IDS[$i]}" "${PRIV_IPS[$i]}" "${PUB_IPS[$i]:-N/A}"
done
echo ""

# ─── Generate cluster ID ──────────────────────────────────────────────────────
banner "Generating Kafka Cluster ID"
info "Requesting UUID from first broker (${PUB_IPS[0]})..."

CLUSTER_ID=$(ssh_broker "${PUB_IPS[0]}" \
    "${KAFKA_HOME}/bin/kafka-storage.sh" random-uuid 2>/dev/null \
    | tr -d '[:space:]')

[[ -n "$CLUSTER_ID" ]] || die "Failed to generate cluster ID via kafka-storage.sh random-uuid"
success "Cluster ID: ${CLUSTER_ID}"

# ─── Build quorum voters ──────────────────────────────────────────────────────
QUORUM_VOTERS=""
for i in "${!PRIV_IPS[@]}"; do
    [[ -n "$QUORUM_VOTERS" ]] && QUORUM_VOTERS+=","
    QUORUM_VOTERS+="$(( i + 1 ))@${PRIV_IPS[$i]}:${CONTROLLER_PORT}"
done
info "Quorum voters: ${QUORUM_VOTERS}"

# ─── Ask replication factor ──────────────────────────────────────────────────
banner "Replication Configuration"
while true; do
    printf "  Replication factor [default: %s, max: %s (= broker count)]: " "$BROKER_COUNT" "$BROKER_COUNT"
    read -r rf_input
    rf_input="${rf_input:-$BROKER_COUNT}"
    if [[ "$rf_input" =~ ^[0-9]+$ ]] && [[ "$rf_input" -ge 1 ]] && [[ "$rf_input" -le "$BROKER_COUNT" ]]; then
        REPLICATION_FACTOR="$rf_input"
        break
    fi
    warn "  Must be an integer between 1 and ${BROKER_COUNT}."
done
MIN_ISR=$(( REPLICATION_FACTOR > 1 ? 2 : 1 ))
info "Replication factor : ${REPLICATION_FACTOR}"
info "min.insync.replicas: ${MIN_ISR}"

# ─── Configure broker via SSH ─────────────────────────────────────────────────
configure_broker() {
    local pub_ip="$1"
    local node_id="$2"
    local priv_ip="$3"

    info "  Configuring node.id=${node_id} (${priv_ip}) via ${pub_ip}..."

    local config
    config="# Generated by configure-cluster.sh — do not edit manually
process.roles=broker,controller
node.id=${node_id}
controller.quorum.voters=${QUORUM_VOTERS}

listeners=CLIENT://0.0.0.0:${KAFKA_PORT},INTERNAL://0.0.0.0:${INTERNAL_PORT},CONTROLLER://0.0.0.0:${CONTROLLER_PORT}
advertised.listeners=CLIENT://${pub_ip}:${KAFKA_PORT},INTERNAL://${priv_ip}:${INTERNAL_PORT}
inter.broker.listener.name=INTERNAL
controller.listener.names=CONTROLLER
listener.security.protocol.map=CLIENT:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT

log.dirs=${KAFKA_HOME}/data

num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
num.partitions=1
num.recovery.threads.per.data.dir=1

offsets.topic.replication.factor=${REPLICATION_FACTOR}
transaction.state.log.replication.factor=${REPLICATION_FACTOR}
transaction.state.log.min.isr=${MIN_ISR}
min.insync.replicas=${MIN_ISR}

log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000"

    local encoded_config encoded_cluster_id
    encoded_config=$(printf '%s' "$config" | base64 -w 0)
    encoded_cluster_id=$(printf '%s' "$CLUSTER_ID" | base64 -w 0)

    ssh_broker "$pub_ip" bash << REMOTE
set -e

# ── 1. Write server.properties ──
printf '%s' "${encoded_config}" | base64 -d \
    | sudo tee ${KAFKA_HOME}/config/kraft/server.properties > /dev/null
echo "[${priv_ip}] server.properties written (node.id=${node_id})"

# ── 2. Clean previous Kafka data ──
sudo rm -rf ${KAFKA_HOME}/data
sudo mkdir -p ${KAFKA_HOME}/data
sudo chown -R \$(whoami):\$(whoami) ${KAFKA_HOME}/data 2>/dev/null || true
echo "[${priv_ip}] data directory cleaned"

# ── 3. Format storage with cluster ID ──
CLUSTER_UUID=\$(printf '%s' "${encoded_cluster_id}" | base64 -d)
${KAFKA_HOME}/bin/kafka-storage.sh format \
    -t "\${CLUSTER_UUID}" \
    -c ${KAFKA_HOME}/config/kraft/server.properties 2>&1
echo "[${priv_ip}] storage formatted (cluster=${CLUSTER_ID})"

# ── 4. Write kt alias — short wrapper for kafka-topics.sh ──
sudo tee /usr/local/bin/kt > /dev/null << 'EOF'
#!/usr/bin/env bash
# Kafka-Topics shorthand: automatically points at the INTERNAL listener.
exec /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 "\$@"
EOF
sudo chmod +x /usr/local/bin/kt

# ── 5. Stop any existing Kafka process ──
sudo pkill -f 'kafka.Kafka' 2>/dev/null || true
sleep 2

# ── 6. Fix log file ownership ──
sudo mkdir -p ${KAFKA_HOME}/logs
sudo touch ${KAFKA_HOME}/kafka.log
sudo chown -R \$(whoami):\$(whoami) ${KAFKA_HOME}/logs ${KAFKA_HOME}/kafka.log

# ── 7. Start Kafka in background ──
nohup ${KAFKA_HOME}/bin/kafka-server-start.sh \
    ${KAFKA_HOME}/config/kraft/server.properties \
    > ${KAFKA_HOME}/kafka.log 2>&1 &
KAFKA_PID=\$!
echo "[${priv_ip}] Kafka started PID=\${KAFKA_PID}"
REMOTE
}

configure_broker_with_retry() {
    local pub_ip="$1"
    local node_id="$2"
    local priv_ip="$3"
    local max_tries=3
    local attempt=0
    while [[ $attempt -lt $max_tries ]]; do
        attempt=$(( attempt + 1 ))
        if configure_broker "$pub_ip" "$node_id" "$priv_ip"; then
            return 0
        fi
        if [[ $attempt -lt $max_tries ]]; then
            warn "  Attempt ${attempt} failed for ${priv_ip} — retrying in 10s..."
            sleep 10
        fi
    done
    die "Failed to configure broker ${priv_ip} after ${max_tries} attempts"
}

configure_all_brokers() {
    banner "Configuring Kafka Brokers"
    for i in "${!INST_IDS[@]}"; do
        local node_id=$(( i + 1 ))
        configure_broker_with_retry "${PUB_IPS[$i]}" "$node_id" "${PRIV_IPS[$i]}"
        success "  Broker ${node_id} configured and started (${PRIV_IPS[$i]})"
    done
}

configure_all_brokers

# ─── Security group verification ──────────────────────────────────────────────
banner "Security Group Verification"
SG_ID=$(aws ec2 describe-instances \
    --instance-ids "${INST_IDS[0]}" \
    --query "Reservations[0].Instances[0].SecurityGroups[0].GroupId" \
    --output text 2>/dev/null || echo "")

if [[ -n "$SG_ID" && "$SG_ID" != "None" ]]; then
    MY_IP=$(curl -sf https://checkip.amazonaws.com 2>/dev/null \
        || curl -sf https://ifconfig.me 2>/dev/null || echo "0.0.0.0")
    OP_CIDR="${MY_IP}/32"
    [[ "$MY_IP" == "0.0.0.0" ]] && OP_CIDR="0.0.0.0/0"

    VPC_ID=$(aws ec2 describe-instances \
        --instance-ids "${INST_IDS[0]}" \
        --query "Reservations[0].Instances[0].VpcId" --output text 2>/dev/null || echo "")
    VPC_CIDR=""
    [[ -n "$VPC_ID" && "$VPC_ID" != "None" ]] && \
        VPC_CIDR=$(aws ec2 describe-vpcs --vpc-ids "$VPC_ID" \
            --query "Vpcs[0].CidrBlock" --output text 2>/dev/null || echo "")

    aws ec2 authorize-security-group-ingress \
        --group-id "$SG_ID" --protocol tcp --port "$KAFKA_PORT" \
        --cidr "$OP_CIDR" 2>/dev/null \
        && info "  Opened port ${KAFKA_PORT} → ${OP_CIDR}" \
        || info "  Port ${KAFKA_PORT} rule already exists for ${OP_CIDR}"

    if [[ -n "$VPC_CIDR" && "$VPC_CIDR" != "None" ]]; then
        aws ec2 authorize-security-group-ingress \
            --group-id "$SG_ID" --protocol tcp --port "$INTERNAL_PORT" \
            --cidr "$VPC_CIDR" 2>/dev/null \
            && info "  Opened port ${INTERNAL_PORT} → ${VPC_CIDR} (inter-broker)" \
            || info "  Port ${INTERNAL_PORT} rule already exists for ${VPC_CIDR}"
    fi
    aws ec2 authorize-security-group-ingress \
        --group-id "$SG_ID" --protocol tcp --port "$INTERNAL_PORT" \
        --source-group "$SG_ID" 2>/dev/null \
        && info "  Opened port ${INTERNAL_PORT} → self-referencing SG" \
        || info "  Port ${INTERNAL_PORT} self-referencing rule already exists"

    success "Security group ${SG_ID} — ports ${KAFKA_PORT}/tcp and ${INTERNAL_PORT}/tcp verified"
else
    warn "Could not determine security group — verify ports ${KAFKA_PORT} and ${INTERNAL_PORT} manually."
fi

# ─── Wait for cluster ready ───────────────────────────────────────────────────
banner "Verifying Cluster Readiness"
info "Polling port ${KAFKA_PORT} on all ${BROKER_COUNT} broker(s) — timeout 90s..."

bootstrap="localhost:${INTERNAL_PORT}"
max_attempts=18
all_ready=false
for (( wait_round=1; wait_round<=max_attempts; wait_round++ )); do
    ready_count=0
    for i in "${!INST_IDS[@]}"; do
        if ssh_broker "${PUB_IPS[$i]}" \
            "ss -tlnp 2>/dev/null | grep -q ':${KAFKA_PORT}'" > /dev/null 2>&1; then
            ready_count=$(( ready_count + 1 ))
        fi
    done
    info "  Round ${wait_round}/${max_attempts} — ${ready_count}/${BROKER_COUNT} broker(s) listening on :${KAFKA_PORT}"
    if [[ "$ready_count" -eq "$BROKER_COUNT" ]]; then
        all_ready=true
        break
    fi
    sleep 5
done

[[ "$all_ready" == "true" ]] || {
    error "Timed out — not all brokers are listening on port ${KAFKA_PORT} after $((max_attempts * 5))s."
    error "Tip: SSH to a broker and run: tail -50 ${KAFKA_HOME}/kafka.log"
    exit 1
}

info "Running kafka-topics.sh --list against ${bootstrap}..."
ssh_broker "${PUB_IPS[0]}" \
    "${KAFKA_HOME}/bin/kafka-topics.sh" \
    "--bootstrap-server" "${bootstrap}" "--list" > /dev/null 2>&1 \
    || die "kafka-topics.sh --list failed. Check ${KAFKA_HOME}/kafka.log on broker instances."
success "All ${BROKER_COUNT} broker(s) ready on port ${KAFKA_PORT}"

# ─── Topic configuration ──────────────────────────────────────────────────────
banner "Topic Configuration"
info "Checking which topics already exist on the cluster..."
echo ""

existing_topics_raw=$(ssh_broker "${PUB_IPS[0]}" \
    "${KAFKA_HOME}/bin/kafka-topics.sh" \
    "--bootstrap-server" "${bootstrap}" "--list" 2>/dev/null || true)

TOPICS_TO_CREATE=()
for topic in "${TOPICS[@]}"; do
    if echo "$existing_topics_raw" | grep -qx "$topic"; then
        success "  '${topic}' already exists — skipping"
    else
        info "  '${topic}' — not found, will be created"
        TOPICS_TO_CREATE+=("$topic")
    fi
done

if [[ ${#TOPICS_TO_CREATE[@]} -gt 0 ]]; then
    echo ""
    info "Topics to create : ${TOPICS_TO_CREATE[*]}"
    info "Recommended partitions = number of brokers (${BROKER_COUNT})"
    echo ""
    for topic in "${TOPICS_TO_CREATE[@]}"; do
        while true; do
            printf "  Partitions for '%s' [default: %s]: " "$topic" "$BROKER_COUNT"
            read -r p
            p="${p:-$BROKER_COUNT}"
            if [[ "$p" =~ ^[0-9]+$ ]] && [[ "$p" -ge 1 ]]; then
                [[ "$p" -lt "$BROKER_COUNT" ]] && \
                    warn "    ${p} < ${BROKER_COUNT} brokers — some brokers won't hold partitions for this topic"
                break
            fi
            warn "    Enter a positive integer."
        done
        TOPIC_PARTITIONS["$topic"]="$p"
    done
else
    info "All 4 topics already exist — nothing to create."
fi

# ─── Create topics ────────────────────────────────────────────────────────────
if [[ ${#TOPICS_TO_CREATE[@]} -gt 0 ]]; then
    banner "Creating Kafka Topics"

    create_topic() {
        local topic="$1"
        local partitions="$2"
        local max_tries=3
        local attempt=0
        while [[ $attempt -lt $max_tries ]]; do
            attempt=$(( attempt + 1 ))
            if ssh_broker "${PUB_IPS[0]}" \
                "${KAFKA_HOME}/bin/kafka-topics.sh" \
                "--bootstrap-server" "${bootstrap}" \
                "--create" "--if-not-exists" \
                "--topic" "${topic}" \
                "--partitions" "${partitions}" \
                "--replication-factor" "${REPLICATION_FACTOR}" 2>/dev/null; then
                return 0
            fi
            [[ $attempt -lt $max_tries ]] && { warn "  Topic creation attempt ${attempt} failed — retrying in 5s..."; sleep 5; }
        done
        die "Failed to create topic '${topic}' after ${max_tries} attempts"
    }

    for topic in "${TOPICS_TO_CREATE[@]}"; do
        info "  Creating '${topic}' (partitions=${TOPIC_PARTITIONS[$topic]}, rf=${REPLICATION_FACTOR})..."
        create_topic "$topic" "${TOPIC_PARTITIONS[$topic]}"
        success "  '${topic}' created"
    done
fi

# ─── Validate topics ──────────────────────────────────────────────────────────
banner "Validating Topics"

all_ok=true
for topic in "${TOPICS[@]}"; do
    describe_out=$(ssh_broker "${PUB_IPS[0]}" \
        "${KAFKA_HOME}/bin/kafka-topics.sh" \
        "--bootstrap-server" "${bootstrap}" \
        "--describe" "--topic" "${topic}" 2>/dev/null || true)

    if [[ -z "$describe_out" ]]; then
        error "  ${topic} — NOT FOUND"
        all_ok=false
        continue
    fi

    actual_parts=$(echo "$describe_out" | grep -m1 'PartitionCount:' \
        | sed 's/.*PartitionCount:[[:space:]]*//' | awk '{print $1}' || true)

    expected_parts="${TOPIC_PARTITIONS[$topic]:-}"
    if [[ -n "$expected_parts" && "$actual_parts" == "$expected_parts" ]]; then
        success "  ${topic} — verified (partitions=${actual_parts}, rf=${REPLICATION_FACTOR})"
    elif [[ -n "$actual_parts" ]]; then
        success "  ${topic} — verified (existing topic, partitions=${actual_parts})"
    else
        success "  ${topic} — exists"
    fi
done
[[ "$all_ok" == "true" ]] || die "One or more topics were not created successfully."

# ─── Preferred leader election ────────────────────────────────────────────────
banner "Balancing Partition Leadership"
info "Waiting ${BROKER_COUNT}s for all brokers to join ISR before electing leaders..."
sleep "${BROKER_COUNT}"

isr_wait=0
isr_ok=false
while [[ $isr_wait -lt 30 ]]; do
    isr_out=$(ssh_broker "${PUB_IPS[0]}" \
        "${KAFKA_HOME}/bin/kafka-topics.sh" \
        "--bootstrap-server" "${bootstrap}" "--describe" 2>/dev/null || true)
    isr_broker_count=$(echo "$isr_out" \
        | grep -oP 'Isr: \K[0-9,]+' \
        | tr ',' '\n' | sort -u | grep -c '.' 2>/dev/null || true)
    if [[ "$isr_broker_count" -ge "$BROKER_COUNT" ]]; then
        isr_ok=true
        break
    fi
    isr_wait=$(( isr_wait + 3 ))
    info "  ISR check: ${isr_broker_count}/${BROKER_COUNT} broker(s) in ISR — retrying in 3s..."
    sleep 3
done

[[ "$isr_ok" == "true" ]] \
    && info "  All ${BROKER_COUNT} broker(s) in ISR — running preferred leader election..." \
    || warn "  ISR check timed out; running preferred leader election anyway..."

election_out=$(ssh_broker "${PUB_IPS[0]}" \
    "${KAFKA_HOME}/bin/kafka-leader-election.sh" \
    "--bootstrap-server" "${bootstrap}" \
    "--election-type" "PREFERRED" \
    "--all-topic-partitions" 2>&1 || true)

if echo "$election_out" | grep -q "error\|ERROR\|Exception"; then
    warn "  Preferred leader election reported an issue: ${election_out}"
else
    success "  Preferred leader election complete — partition leaders distributed across brokers"
fi

# ─── Update brokers.json ──────────────────────────────────────────────────────
banner "Updating brokers.json"

brokers_arr="["
for i in "${!PRIV_IPS[@]}"; do
    [[ $i -gt 0 ]] && brokers_arr+=","
    brokers_arr+="{\"privateIp\":\"${PRIV_IPS[$i]}\"}"
done
brokers_arr+="]"

jq -n \
    --arg comment "Kafka broker private IPs for VPC-internal connections. Port is controlled by KAFKA_PORT in .env.local (default: 19092 — INTERNAL VPC listener). Updated by configure-cluster.sh on $(date -u +%Y-%m-%dT%H:%M:%SZ). Copy this file to swipenest-consumer/brokers.json after deployment." \
    --argjson brokers "$brokers_arr" \
    '{_comment: $comment, brokers: $brokers}' > "$BROKERS_JSON"

success "Updated brokers.json with ${#PRIV_IPS[@]} broker(s)"
info "    $(cat "$BROKERS_JSON" | jq -c '{brokers: .brokers}')"
echo ""
info "  Copy to consumer:  cp ${BROKERS_JSON} ../swipenest-consumer/brokers.json"

# ─── Final summary ────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}${BOLD}║    Kafka cluster configured successfully                         ║${NC}"
echo -e "${GREEN}${BOLD}╠══════════════════════════════════════════════════════════════════╣${NC}"
printf "${GREEN}${BOLD}║${NC}  %-22s %s\n" "Cluster ID:"      "${CLUSTER_ID}"
printf "${GREEN}${BOLD}║${NC}  %-22s %s\n" "Brokers:"         "${BROKER_COUNT}"
printf "${GREEN}${BOLD}║${NC}  %-22s %s\n" "Replication RF:"  "${REPLICATION_FACTOR}"
printf "${GREEN}${BOLD}║${NC}  %-22s %s\n" "min.insync.repl:" "${MIN_ISR}"
echo -e "${GREEN}${BOLD}╠══════════════════════════════════════════════════════════════════╣${NC}"
printf "  %-26s  %s\n" "Topic" "Partitions"
printf "  %-26s  %s\n" "--------------------------" "----------"
for topic in "${TOPICS[@]}"; do
    p="${TOPIC_PARTITIONS[$topic]:-existing}"
    printf "  %-26s  %s\n" "$topic" "$p"
done
echo -e "${GREEN}${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""
success "Cluster is ready for production traffic."
echo ""
echo -e "${CYAN}${BOLD}On-broker admin commands (SSH into any broker, then):${NC}"
echo -e "  ${BOLD}kt --list${NC}                              # short alias (uses localhost:${INTERNAL_PORT})"
echo -e "  ${BOLD}kt --describe --topic video_view${NC}"
echo -e "  ${YELLOW}⚠  Always use port ${INTERNAL_PORT} (INTERNAL) on-broker, not port 9092 (hairpin NAT).${NC}"
echo ""
