#!/usr/bin/env bash
###############################################################################
# deploy-kafka-brokers.sh
#
# End-to-end Kafka cluster deployment for SwipeNest.
# Combines deploy-instances.sh + configure-cluster.sh into a single run.
#
# What this script does:
#   ── PHASE 1: Provision ──────────────────────────────────────────────────────
#   1.  Asks for broker count, instance type, and storage size
#   2.  Creates (or reuses) security group with ports 22/9092/9093/19092
#   3.  Launches N EC2 instances from the Kafka AMI
#   4.  Waits until running + status checks pass
#   5.  Saves instance metadata to scripts/kafka-instances.json
#
#   ── PHASE 2: Configure ──────────────────────────────────────────────────────
#   6.  Generates a Kafka cluster UUID via kafka-storage.sh random-uuid
#   7.  Builds the KRaft quorum voters string
#   8.  Asks replication factor interactively
#   9.  SSHes each broker:
#         a. Writes server.properties (node.id, listeners, quorum voters)
#         b. Cleans /opt/kafka/data (removes stale metadata)
#         c. Formats storage with the cluster UUID
#         d. Starts kafka-server-start.sh in background
#   10. Polls until all brokers are listening on port 9092
#   11. Asks partition count for each topic interactively
#   12. Creates topics: video_view, post_impression, post_likes, video_watch_progress
#   13. Validates topics exist
#   14. Runs preferred leader election across all partitions
#
#   ── PHASE 3: Output ─────────────────────────────────────────────────────────
#   15. Writes brokers.json (project root) with privateIp + publicIp
#       — copy directly to swipenest-consumer/brokers.json after deployment
#       — swipenest-core reads this file automatically for KAFKA_BROKER
#
# AMI: ami-0018b1f38bf74ad62  (Ubuntu base — broker app cloned from GitHub at deploy time)
# Kafka port architecture:
#   CLIENT     9092   — external client access (public IP, KafkaJS, load scripts)
#   INTERNAL  19092   — inter-broker + in-VPC app servers (private IP, VPC only)
#   CONTROLLER 9093   — KRaft quorum (private IP, intra-cluster)
#
# Usage:
#   ./scripts/deploy-kafka-brokers.sh
#   ./scripts/deploy-kafka-brokers.sh --region ap-south-1
#   ./scripts/deploy-kafka-brokers.sh --dry-run
###############################################################################

set -euo pipefail

# ─── Defaults ─────────────────────────────────────────────────────────────────
AWS_REGION="${AWS_REGION:-ap-south-1}"
KAFKA_AMI="ami-0018b1f38bf74ad62"
GITHUB_REPO="https://github.com/swipenest-tech/swipenest-kafka-broker.git"
DEFAULT_KEY_NAME="ec2-key-pair"
DEFAULT_PEM_KEY="${HOME}/.ssh/ec2-key-pair.pem"
DEFAULT_SUBNET_ID="subnet-0da50cf2f3ebd9280"
KAFKA_SG_NAME="swipenest-kafka-sg"
KAFKA_PORT=9092
INTERNAL_PORT=19092
CONTROLLER_PORT=9093
KAFKA_HOME="/opt/kafka"
SSH_USER="ubuntu"
DRY_RUN=false

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
INSTANCES_FILE="${SCRIPT_DIR}/kafka-instances.json"
BROKERS_JSON="${PROJECT_ROOT}/brokers.json"

TOPICS=(
    "video_view"
    "post_impression"
    "post_likes"
    "video_watch_progress"
    "post_comments"
)
declare -A TOPIC_PARTITIONS

# Arrays populated during provisioning; reused in configuration
INST_IDS=()
PRIV_IPS=()
PUB_IPS=()

# ─── Colors ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERR]${NC}   $*" >&2; }
die()     { error "$*"; exit 1; }
banner()  { echo -e "\n${BOLD}${BLUE}══════ $* ══════${NC}\n"; }

# ─── Parse flags ──────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case $1 in
        --region)  AWS_REGION="$2"; shift 2 ;;
        --dry-run) DRY_RUN=true;    shift   ;;
        --help|-h)
            sed -n '2,35p' "$0" | sed 's/^# *//'
            exit 0 ;;
        *) die "Unknown argument: $1. Use --help." ;;
    esac
done

export AWS_DEFAULT_REGION="$AWS_REGION"

# ─── Header ───────────────────────────────────────────────────────────────────
echo -e "${BOLD}${BLUE}"
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║    SwipeNest — Kafka Cluster Deployment (end-to-end)             ║"
echo "║    Phase 1: Provision  →  Phase 2: Configure  →  Phase 3: Done  ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
[[ "$DRY_RUN" == "true" ]] && warn "DRY-RUN mode — no AWS resources will be created."

# ─── Preflight ────────────────────────────────────────────────────────────────
banner "Preflight Checks"
missing=()
for cmd in aws ssh jq base64; do
    command -v "$cmd" &>/dev/null || missing+=("$cmd")
done
[[ ${#missing[@]} -gt 0 ]] && die "Missing required tools: ${missing[*]}"
if [[ "$DRY_RUN" == "false" ]]; then
    aws sts get-caller-identity &>/dev/null || die "AWS credentials not configured."
fi
success "All dependencies present"

# ═══════════════════════════════════════════════════════════════════════════════
#  PHASE 1 — PROVISION EC2 INSTANCES
# ═══════════════════════════════════════════════════════════════════════════════

banner "PHASE 1 — Provision EC2 Instances"

# ─── Broker count ─────────────────────────────────────────────────────────────
echo "How many Kafka broker instances do you want to provision?"
while true; do
    printf "  Number of brokers [1-20]: "
    read -r BROKER_COUNT
    [[ "$BROKER_COUNT" =~ ^[0-9]+$ ]] \
        && [[ "$BROKER_COUNT" -ge 1 ]] \
        && [[ "$BROKER_COUNT" -le 20 ]] && break
    warn "  Enter an integer between 1 and 20."
done
if [[ "$BROKER_COUNT" -lt 3 ]]; then
    warn "Minimum 3 brokers is recommended for production."
    printf "  Continue with %s broker(s)? [y/N]: " "$BROKER_COUNT"
    read -r CNF
    [[ "$CNF" =~ ^[Yy]$ ]] || die "Aborted."
fi
success "Broker count: ${BROKER_COUNT}"

# ─── Instance type ────────────────────────────────────────────────────────────
echo ""
echo "Select EC2 instance type:"
printf "  %-4s %-14s %s\n" "1)" "t3.micro"   "2 vCPU,  1 GB RAM   (testing only)"
printf "  %-4s %-14s %s\n" "2)" "t3.small"   "2 vCPU,  2 GB RAM   (testing only)"
printf "  %-4s %-14s %s\n" "3)" "t3.medium"  "2 vCPU,  4 GB RAM"
printf "  %-4s %-14s %s\n" "4)" "t3.large"   "2 vCPU,  8 GB RAM"
printf "  %-4s %-14s %s\n" "5)" "m5.large"   "2 vCPU,  8 GB RAM   (recommended)"
printf "  %-4s %-14s %s\n" "6)" "m5.xlarge"  "4 vCPU, 16 GB RAM"
printf "  %-4s %-14s %s\n" "7)" "m5.2xlarge" "8 vCPU, 32 GB RAM"
printf "  %-4s %-14s %s\n" "8)" "Custom"     "(enter manually)"
while true; do
    printf "  Select [1-8]: "
    read -r ITYPE_CHOICE
    case "$ITYPE_CHOICE" in
        1) INSTANCE_TYPE="t3.micro";   break ;;
        2) INSTANCE_TYPE="t3.small";   break ;;
        3) INSTANCE_TYPE="t3.medium";  break ;;
        4) INSTANCE_TYPE="t3.large";   break ;;
        5) INSTANCE_TYPE="m5.large";   break ;;
        6) INSTANCE_TYPE="m5.xlarge";  break ;;
        7) INSTANCE_TYPE="m5.2xlarge"; break ;;
        8) printf "  Enter instance type: "
           read -r INSTANCE_TYPE
           [[ -n "$INSTANCE_TYPE" ]] && break ;;
        *) warn "  Enter a number between 1 and 8." ;;
    esac
done
success "Instance type: ${INSTANCE_TYPE}"

# ─── Storage ──────────────────────────────────────────────────────────────────
echo ""
while true; do
    printf "  EBS volume size per broker in GB [8-5120]: "
    read -r STORAGE_GB
    [[ "$STORAGE_GB" =~ ^[0-9]+$ ]] \
        && [[ "$STORAGE_GB" -ge 8 ]] \
        && [[ "$STORAGE_GB" -le 5120 ]] && break
    warn "  Storage must be between 8 and 5120 GB."
done
success "Storage: ${STORAGE_GB} GB gp3"

# ─── SSH key ──────────────────────────────────────────────────────────────────
echo ""
printf "  Key pair name [default: %s]: " "$DEFAULT_KEY_NAME"
read -r KEY_INPUT
KEY_NAME="${KEY_INPUT:-$DEFAULT_KEY_NAME}"

printf "  PEM key path [default: %s]: " "$DEFAULT_PEM_KEY"
read -r PEM_INPUT
PEM_KEY="${PEM_INPUT:-$DEFAULT_PEM_KEY}"
PEM_KEY="${PEM_KEY/#\~/$HOME}"

if [[ "$DRY_RUN" == "false" ]]; then
    [[ -f "$PEM_KEY" ]] || die "PEM key not found: ${PEM_KEY}"
    chmod 600 "$PEM_KEY"
fi
info "Key pair : ${KEY_NAME}"
info "PEM key  : ${PEM_KEY}"

# ─── Security group ───────────────────────────────────────────────────────────
banner "Security Group"
if [[ "$DRY_RUN" == "true" ]]; then
    KAFKA_SG_ID="sg-dryrun"
    warn "[DRY-RUN] Skipping security group setup."
else
    KAFKA_SG_ID=$(aws ec2 describe-security-groups \
        --filters "Name=group-name,Values=${KAFKA_SG_NAME}" \
        --query "SecurityGroups[0].GroupId" \
        --output text 2>/dev/null || echo "None")

    if [[ "$KAFKA_SG_ID" == "None" ]] || [[ -z "$KAFKA_SG_ID" ]]; then
        info "Creating security group '${KAFKA_SG_NAME}'..."
        VPC_ID=$(aws ec2 describe-subnets \
            --subnet-ids "$DEFAULT_SUBNET_ID" \
            --query "Subnets[0].VpcId" --output text)
        VPC_CIDR=$(aws ec2 describe-vpcs \
            --vpc-ids "$VPC_ID" \
            --query "Vpcs[0].CidrBlock" --output text)

        KAFKA_SG_ID=$(aws ec2 create-security-group \
            --group-name "$KAFKA_SG_NAME" \
            --description "SwipeNest Kafka: 9092 client, 9093 controller, 19092 internal, 22 SSH" \
            --vpc-id "$VPC_ID" \
            --query "GroupId" --output text)

        # 9092 — VPC-wide client access + intra-cluster
        aws ec2 authorize-security-group-ingress \
            --group-id "$KAFKA_SG_ID" --protocol tcp --port "$KAFKA_PORT" \
            --cidr "$VPC_CIDR" > /dev/null
        aws ec2 authorize-security-group-ingress \
            --group-id "$KAFKA_SG_ID" --protocol tcp --port "$KAFKA_PORT" \
            --source-group "$KAFKA_SG_ID" > /dev/null

        # 19092 — INTERNAL listener: VPC-wide + intra-cluster
        aws ec2 authorize-security-group-ingress \
            --group-id "$KAFKA_SG_ID" --protocol tcp --port "$INTERNAL_PORT" \
            --cidr "$VPC_CIDR" > /dev/null
        aws ec2 authorize-security-group-ingress \
            --group-id "$KAFKA_SG_ID" --protocol tcp --port "$INTERNAL_PORT" \
            --source-group "$KAFKA_SG_ID" > /dev/null

        # 9093 — KRaft controller (intra-cluster)
        aws ec2 authorize-security-group-ingress \
            --group-id "$KAFKA_SG_ID" --protocol tcp --port "$CONTROLLER_PORT" \
            --source-group "$KAFKA_SG_ID" > /dev/null

        # 22 + 9092 — operator's public IP (SSH access + external KafkaJS scripts)
        MY_IP=$(curl -sf https://checkip.amazonaws.com 2>/dev/null \
            || curl -sf https://ifconfig.me 2>/dev/null || echo "0.0.0.0")
        SSH_CIDR="${MY_IP}/32"
        [[ "$MY_IP" == "0.0.0.0" ]] && SSH_CIDR="0.0.0.0/0"
        aws ec2 authorize-security-group-ingress \
            --group-id "$KAFKA_SG_ID" --protocol tcp --port 22 \
            --cidr "$SSH_CIDR" > /dev/null
        aws ec2 authorize-security-group-ingress \
            --group-id "$KAFKA_SG_ID" --protocol tcp --port "$KAFKA_PORT" \
            --cidr "$SSH_CIDR" > /dev/null

        success "Created security group ${KAFKA_SG_ID} (VPC: ${VPC_ID}, operator: ${SSH_CIDR})"
    else
        success "Reusing existing security group: ${KAFKA_SG_ID}"
        MY_IP=$(curl -sf https://checkip.amazonaws.com 2>/dev/null \
            || curl -sf https://ifconfig.me 2>/dev/null || echo "0.0.0.0")
        OP_CIDR="${MY_IP}/32"
        [[ "$MY_IP" == "0.0.0.0" ]] && OP_CIDR="0.0.0.0/0"
        aws ec2 authorize-security-group-ingress \
            --group-id "$KAFKA_SG_ID" --protocol tcp --port 22 \
            --cidr "$OP_CIDR" 2>/dev/null || true
        aws ec2 authorize-security-group-ingress \
            --group-id "$KAFKA_SG_ID" --protocol tcp --port "$KAFKA_PORT" \
            --cidr "$OP_CIDR" 2>/dev/null || true
        aws ec2 authorize-security-group-ingress \
            --group-id "$KAFKA_SG_ID" --protocol tcp --port "$INTERNAL_PORT" \
            --source-group "$KAFKA_SG_ID" 2>/dev/null || true
        info "SSH + Kafka access ensured for: ${OP_CIDR}"
    fi
fi

# ─── Launch instances ─────────────────────────────────────────────────────────
banner "Launching EC2 Instances"
info "AMI    : ${KAFKA_AMI}"
info "Type   : ${INSTANCE_TYPE}"
info "Count  : ${BROKER_COUNT}"
info "Storage: ${STORAGE_GB} GB gp3"
info "Subnet : ${DEFAULT_SUBNET_ID}"

if [[ "$DRY_RUN" == "true" ]]; then
    warn "[DRY-RUN] Would launch ${BROKER_COUNT} x ${INSTANCE_TYPE}. Skipping."
    INST_IDS=("i-dryrun0001" "i-dryrun0002" "i-dryrun0003")
    PRIV_IPS=("10.0.1.101" "10.0.1.102" "10.0.1.103")
    PUB_IPS=("1.2.3.101" "1.2.3.102" "1.2.3.103")
else
    BLOCK_DEVICE=$(jq -n --argjson size "$STORAGE_GB" \
        '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":$size,"VolumeType":"gp3","DeleteOnTermination":true}}]')

    RUN_RESULT=$(aws ec2 run-instances \
        --image-id "$KAFKA_AMI" \
        --instance-type "$INSTANCE_TYPE" \
        --count "$BROKER_COUNT" \
        --key-name "$KEY_NAME" \
        --security-group-ids "$KAFKA_SG_ID" \
        --subnet-id "$DEFAULT_SUBNET_ID" \
        --associate-public-ip-address \
        --block-device-mappings "$BLOCK_DEVICE" \
        --tag-specifications \
            "ResourceType=instance,Tags=[{Key=Name,Value=Kafka-Broker},{Key=Cluster,Value=KafkaCluster},{Key=Project,Value=SwipeNest},{Key=Role,Value=kafka-broker}]" \
        --output json)

    mapfile -t INST_IDS < <(echo "$RUN_RESULT" | jq -r '.Instances[].InstanceId')
    success "Launched instances: ${INST_IDS[*]}"

    for i in "${!INST_IDS[@]}"; do
        aws ec2 create-tags \
            --resources "${INST_IDS[$i]}" \
            --tags "Key=Name,Value=Kafka-Broker-$(( i + 1 ))" > /dev/null
        info "  Tagged ${INST_IDS[$i]} → Kafka-Broker-$(( i + 1 ))"
    done

    # ── Wait for running ──────────────────────────────────────────────────────
    banner "Waiting for Instances to Start"
    info "Waiting for 'running' state..."
    aws ec2 wait instance-running --instance-ids "${INST_IDS[@]}"
    success "All instances running"
    info "Waiting 30s for SSH daemon to start..."
    sleep 30

    # ── Wait for status checks ────────────────────────────────────────────────
    banner "Waiting for Status Checks (2–3 min)"
    aws ec2 wait instance-status-ok --instance-ids "${INST_IDS[@]}"
    success "All instance status checks passed"

    # ── Fetch IPs ─────────────────────────────────────────────────────────────
    banner "Fetching Instance Details"
    mapfile -t PRIV_IPS < <(
        aws ec2 describe-instances \
            --instance-ids "${INST_IDS[@]}" \
            --query "Reservations[].Instances[].PrivateIpAddress" \
            --output text | tr '\t' '\n')
    mapfile -t PUB_IPS < <(
        aws ec2 describe-instances \
            --instance-ids "${INST_IDS[@]}" \
            --query "Reservations[].Instances[].PublicIpAddress" \
            --output text | tr '\t' '\n')

    success "Private IPs : ${PRIV_IPS[*]}"
    success "Public IPs  : ${PUB_IPS[*]}"

    # ── Save kafka-instances.json ─────────────────────────────────────────────
    instances_json='[]'
    for i in "${!INST_IDS[@]}"; do
        instances_json=$(echo "$instances_json" | jq \
            --arg   id   "${INST_IDS[$i]}" \
            --arg   priv "${PRIV_IPS[$i]:-}" \
            --arg   pub  "${PUB_IPS[$i]:-}" \
            --argjson n  "$(( i + 1 ))" \
            '. + [{"instance_id":$id,"private_ip":$priv,"public_ip":$pub,"node_id":$n}]')
    done
    jq -n \
        --argjson instances "$instances_json" \
        --arg region        "$AWS_REGION" \
        --arg ami           "$KAFKA_AMI" \
        --arg instance_type "$INSTANCE_TYPE" \
        --arg storage_gb    "$STORAGE_GB" \
        --arg created_at    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        '{instances:$instances,region:$region,ami:$ami,
          instance_type:$instance_type,storage_gb:$storage_gb,created_at:$created_at}' \
        > "$INSTANCES_FILE"
    success "Saved: scripts/kafka-instances.json"
fi

echo ""
printf "  %-5s  %-24s  %-16s  %s\n" "Node" "Instance ID" "Private IP" "Public IP"
printf "  %-5s  %-24s  %-16s  %s\n" "-----" "------------------------" "----------------" "---------------"
for i in "${!INST_IDS[@]}"; do
    printf "  %-5s  %-24s  %-16s  %s\n" \
        "$(( i + 1 ))" "${INST_IDS[$i]}" "${PRIV_IPS[$i]:-N/A}" "${PUB_IPS[$i]:-N/A}"
done
echo ""

# ═══════════════════════════════════════════════════════════════════════════════
#  PHASE 2 — CONFIGURE KAFKA CLUSTER
# ═══════════════════════════════════════════════════════════════════════════════

banner "PHASE 2 — Configure Kafka KRaft Cluster"

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
    local max_tries=3 attempt=0
    while [[ $attempt -lt $max_tries ]]; do
        attempt=$(( attempt + 1 ))
        if ssh_broker "$pub_ip" "$@"; then return 0; fi
        [[ $attempt -lt $max_tries ]] && { warn "    SSH attempt ${attempt} failed — retrying in 8s..."; sleep 8; }
    done
    die "SSH to ${pub_ip} failed after ${max_tries} attempts"
}

# ─── Install broker application from GitHub ───────────────────────────────────
install_broker() {
    local pub_ip="$1" node_id="$2"
    info "  Installing broker application on node.id=${node_id} (${pub_ip})..."

    ssh_broker "$pub_ip" bash << REMOTE
set -e

echo "[node.id=${node_id}] Installing prerequisites..."
sudo apt-get update -qq
sudo apt-get install -y git 2>&1 | tail -3
echo "[node.id=${node_id}] Prerequisites installed"

echo "[node.id=${node_id}] Cloning broker application from GitHub..."
sudo rm -rf /home/ubuntu/project/swipenest-kafka-broker
sudo mkdir -p /home/ubuntu/project
sudo chown ubuntu:ubuntu /home/ubuntu/project
git clone "${GITHUB_REPO}" /home/ubuntu/project/swipenest-kafka-broker
chown -R ubuntu:ubuntu /home/ubuntu/project/swipenest-kafka-broker
echo "[node.id=${node_id}] Repository cloned"

echo "[node.id=${node_id}] Installing npm dependencies..."
cd /home/ubuntu/project/swipenest-kafka-broker
npm install --production --no-audit --no-fund 2>&1 | tail -5
echo "[node.id=${node_id}] npm install complete"
REMOTE
}

banner "Installing Broker Application from GitHub"
[[ "$DRY_RUN" == "true" ]] && { warn "[DRY-RUN] Skipping broker application installation."; } || {
    for i in "${!INST_IDS[@]}"; do
        node_id=$(( i + 1 ))
        attempt=0
        while [[ $attempt -lt 3 ]]; do
            attempt=$(( attempt + 1 ))
            if install_broker "${PUB_IPS[$i]}" "$node_id"; then break; fi
            [[ $attempt -lt 3 ]] && { warn "  Attempt ${attempt} failed — retrying in 10s..."; sleep 10; }
            [[ $attempt -eq 3 ]] && die "Failed to install broker application on ${PUB_IPS[$i]} after 3 attempts"
        done
        success "  Broker ${node_id} application installed (${PRIV_IPS[$i]})"
    done
}

# ─── Generate cluster UUID ────────────────────────────────────────────────────
banner "Generating Kafka Cluster ID"
if [[ "$DRY_RUN" == "true" ]]; then
    CLUSTER_ID="dryrun-cluster-uuid-00000001"
    warn "[DRY-RUN] Using fake cluster ID: ${CLUSTER_ID}"
else
    info "Requesting UUID from first broker (${PUB_IPS[0]})..."
    CLUSTER_ID=$(ssh_broker "${PUB_IPS[0]}" \
        "${KAFKA_HOME}/bin/kafka-storage.sh" random-uuid 2>/dev/null \
        | tr -d '[:space:]')
    [[ -n "$CLUSTER_ID" ]] || die "Failed to generate cluster ID via kafka-storage.sh"
    success "Cluster ID: ${CLUSTER_ID}"
fi

# ─── Build quorum voters ──────────────────────────────────────────────────────
QUORUM_VOTERS=""
for i in "${!PRIV_IPS[@]}"; do
    [[ -n "$QUORUM_VOTERS" ]] && QUORUM_VOTERS+=","
    QUORUM_VOTERS+="$(( i + 1 ))@${PRIV_IPS[$i]}:${CONTROLLER_PORT}"
done
info "Quorum voters: ${QUORUM_VOTERS}"

# ─── Replication factor ───────────────────────────────────────────────────────
banner "Replication Configuration"
while true; do
    printf "  Replication factor [default: %s, max: %s]: " "$BROKER_COUNT" "$BROKER_COUNT"
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

# ─── Configure brokers via SSH ────────────────────────────────────────────────
configure_broker() {
    local pub_ip="$1" node_id="$2" priv_ip="$3"
    info "  Configuring node.id=${node_id} (${priv_ip}) via ${pub_ip}..."

    local config
    config="# Generated by deploy-kafka-brokers.sh — do not edit manually
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

    local enc_config enc_uuid
    enc_config=$(printf '%s' "$config" | base64 -w 0)
    enc_uuid=$(printf '%s' "$CLUSTER_ID" | base64 -w 0)

    ssh_broker "$pub_ip" bash << REMOTE
set -e

# 1. Write server.properties
printf '%s' "${enc_config}" | base64 -d \
    | sudo tee ${KAFKA_HOME}/config/kraft/server.properties > /dev/null
echo "[${priv_ip}] server.properties written (node.id=${node_id})"

# 2. Clean previous data directory
sudo rm -rf ${KAFKA_HOME}/data
sudo mkdir -p ${KAFKA_HOME}/data
sudo chown -R \$(whoami):\$(whoami) ${KAFKA_HOME}/data 2>/dev/null || true
echo "[${priv_ip}] data directory cleaned"

# 3. Format storage with cluster UUID
CLUSTER_UUID=\$(printf '%s' "${enc_uuid}" | base64 -d)
${KAFKA_HOME}/bin/kafka-storage.sh format \
    -t "\${CLUSTER_UUID}" \
    -c ${KAFKA_HOME}/config/kraft/server.properties 2>&1
echo "[${priv_ip}] storage formatted"

# 4. Write kt alias
sudo tee /usr/local/bin/kt > /dev/null << 'EOF'
#!/usr/bin/env bash
exec /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 "\$@"
EOF
sudo chmod +x /usr/local/bin/kt

# 5. Stop any existing Kafka process
sudo pkill -f 'kafka.Kafka' 2>/dev/null || true
sleep 2

# 6. Fix log ownership
sudo mkdir -p ${KAFKA_HOME}/logs
sudo touch ${KAFKA_HOME}/kafka.log
sudo chown -R \$(whoami):\$(whoami) ${KAFKA_HOME}/logs ${KAFKA_HOME}/kafka.log

# 7. Start Kafka
nohup ${KAFKA_HOME}/bin/kafka-server-start.sh \
    ${KAFKA_HOME}/config/kraft/server.properties \
    > ${KAFKA_HOME}/kafka.log 2>&1 &
echo "[${priv_ip}] Kafka started PID=\$!"
REMOTE
}

banner "Configuring All Brokers"
[[ "$DRY_RUN" == "true" ]] && { warn "[DRY-RUN] Skipping SSH configuration."; } || {
    for i in "${!INST_IDS[@]}"; do
        node_id=$(( i + 1 ))
        attempt=0
        while [[ $attempt -lt 3 ]]; do
            attempt=$(( attempt + 1 ))
            if configure_broker "${PUB_IPS[$i]}" "$node_id" "${PRIV_IPS[$i]}"; then break; fi
            [[ $attempt -lt 3 ]] && { warn "  Attempt ${attempt} failed — retrying in 10s..."; sleep 10; }
            [[ $attempt -eq 3 ]] && die "Failed to configure broker ${PRIV_IPS[$i]} after 3 attempts"
        done
        success "  Broker ${node_id} configured and started (${PRIV_IPS[$i]})"
    done
}

# ─── Verify security group ports (idempotent) ─────────────────────────────────
banner "Security Group Port Verification"
if [[ "$DRY_RUN" == "false" ]]; then
    SG_ID=$(aws ec2 describe-instances \
        --instance-ids "${INST_IDS[0]}" \
        --query "Reservations[0].Instances[0].SecurityGroups[0].GroupId" \
        --output text 2>/dev/null || echo "")
    if [[ -n "$SG_ID" && "$SG_ID" != "None" ]]; then
        MY_IP=$(curl -sf https://checkip.amazonaws.com 2>/dev/null \
            || curl -sf https://ifconfig.me 2>/dev/null || echo "0.0.0.0")
        OP_CIDR="${MY_IP}/32"; [[ "$MY_IP" == "0.0.0.0" ]] && OP_CIDR="0.0.0.0/0"
        VPC_ID=$(aws ec2 describe-instances --instance-ids "${INST_IDS[0]}" \
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
                && info "  Opened port ${INTERNAL_PORT} → ${VPC_CIDR}" \
                || info "  Port ${INTERNAL_PORT} rule already exists for ${VPC_CIDR}"
        fi
        aws ec2 authorize-security-group-ingress \
            --group-id "$SG_ID" --protocol tcp --port "$INTERNAL_PORT" \
            --source-group "$SG_ID" 2>/dev/null \
            && info "  Opened port ${INTERNAL_PORT} (intra-cluster)" \
            || info "  Port ${INTERNAL_PORT} intra-cluster rule already exists"
        success "Security group ${SG_ID} — ports verified"
    else
        warn "Could not determine security group — verify ports manually."
    fi
fi

# ─── Wait for cluster readiness ───────────────────────────────────────────────
banner "Verifying Cluster Readiness"
if [[ "$DRY_RUN" == "true" ]]; then
    warn "[DRY-RUN] Skipping cluster readiness check."
else
    info "Polling port ${KAFKA_PORT} on all ${BROKER_COUNT} broker(s) — timeout 90s..."
    all_ready=false
    for (( round=1; round<=18; round++ )); do
        ready_count=0
        for i in "${!INST_IDS[@]}"; do
            ssh_broker "${PUB_IPS[$i]}" \
                "ss -tlnp 2>/dev/null | grep -q ':${KAFKA_PORT}'" > /dev/null 2>&1 \
                && ready_count=$(( ready_count + 1 ))
        done
        info "  Round ${round}/18 — ${ready_count}/${BROKER_COUNT} broker(s) listening on :${KAFKA_PORT}"
        if [[ "$ready_count" -eq "$BROKER_COUNT" ]]; then all_ready=true; break; fi
        sleep 5
    done
    [[ "$all_ready" == "true" ]] || {
        error "Timed out — not all brokers are listening on port ${KAFKA_PORT} after 90s."
        error "SSH to a broker and run: tail -50 ${KAFKA_HOME}/kafka.log"
        exit 1
    }
    bootstrap="localhost:${INTERNAL_PORT}"
    ssh_broker "${PUB_IPS[0]}" \
        "${KAFKA_HOME}/bin/kafka-topics.sh" \
        "--bootstrap-server" "${bootstrap}" "--list" > /dev/null 2>&1 \
        || die "kafka-topics.sh --list failed. Check ${KAFKA_HOME}/kafka.log."
    success "All ${BROKER_COUNT} broker(s) ready"
fi

# ─── Topic partition counts ───────────────────────────────────────────────────
banner "Topic Configuration"
if [[ "$DRY_RUN" == "false" ]]; then
    bootstrap="localhost:${INTERNAL_PORT}"
    info "Checking which topics already exist..."
    existing_raw=$(ssh_broker "${PUB_IPS[0]}" \
        "${KAFKA_HOME}/bin/kafka-topics.sh" \
        "--bootstrap-server" "${bootstrap}" "--list" 2>/dev/null || true)
fi

TOPICS_TO_CREATE=()
for topic in "${TOPICS[@]}"; do
    if [[ "$DRY_RUN" == "false" ]] && echo "$existing_raw" | grep -qx "$topic"; then
        success "  '${topic}' already exists — skipping"
    else
        info "  '${topic}' — will be created"
        TOPICS_TO_CREATE+=("$topic")
    fi
done

if [[ ${#TOPICS_TO_CREATE[@]} -gt 0 ]]; then
    echo ""
    info "Recommended partitions = number of brokers (${BROKER_COUNT})"
    for topic in "${TOPICS_TO_CREATE[@]}"; do
        while true; do
            printf "  Partitions for '%s' [default: %s]: " "$topic" "$BROKER_COUNT"
            read -r p
            p="${p:-$BROKER_COUNT}"
            if [[ "$p" =~ ^[0-9]+$ ]] && [[ "$p" -ge 1 ]]; then
                [[ "$p" -lt "$BROKER_COUNT" ]] && \
                    warn "    ${p} < ${BROKER_COUNT} brokers — some brokers won't hold partitions"
                break
            fi
            warn "    Enter a positive integer."
        done
        TOPIC_PARTITIONS["$topic"]="$p"
    done
fi

# ─── Create topics ────────────────────────────────────────────────────────────
if [[ ${#TOPICS_TO_CREATE[@]} -gt 0 ]]; then
    banner "Creating Kafka Topics"
    if [[ "$DRY_RUN" == "true" ]]; then
        warn "[DRY-RUN] Skipping topic creation."
    else
        for topic in "${TOPICS_TO_CREATE[@]}"; do
            info "  Creating '${topic}' (partitions=${TOPIC_PARTITIONS[$topic]}, rf=${REPLICATION_FACTOR})..."
            attempt=0
            while [[ $attempt -lt 3 ]]; do
                attempt=$(( attempt + 1 ))
                if ssh_broker "${PUB_IPS[0]}" \
                    "${KAFKA_HOME}/bin/kafka-topics.sh" \
                    "--bootstrap-server" "${bootstrap}" \
                    "--create" "--if-not-exists" \
                    "--topic" "${topic}" \
                    "--partitions" "${TOPIC_PARTITIONS[$topic]}" \
                    "--replication-factor" "${REPLICATION_FACTOR}" \
                    "--config" "retention.ms=604800000" \
                    "--config" "cleanup.policy=delete" 2>/dev/null; then
                    break
                fi
                [[ $attempt -lt 3 ]] && { warn "  Attempt ${attempt} failed — retrying in 5s..."; sleep 5; }
                [[ $attempt -eq 3 ]] && die "Failed to create topic '${topic}' after 3 attempts"
            done
            success "  '${topic}' created"
        done
    fi
fi

# ─── Validate topics ──────────────────────────────────────────────────────────
banner "Validating Topics"
if [[ "$DRY_RUN" == "true" ]]; then
    warn "[DRY-RUN] Skipping validation."
else
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
        success "  ${topic} — verified (partitions=${actual_parts:-?}, rf=${REPLICATION_FACTOR})"
    done
    [[ "$all_ok" == "true" ]] || die "One or more topics were not created successfully."
fi

# ─── Preferred leader election ────────────────────────────────────────────────
banner "Balancing Partition Leadership"
if [[ "$DRY_RUN" == "false" ]]; then
    info "Waiting ${BROKER_COUNT}s for ISR before electing leaders..."
    sleep "${BROKER_COUNT}"

    isr_wait=0 isr_ok=false
    while [[ $isr_wait -lt 30 ]]; do
        isr_out=$(ssh_broker "${PUB_IPS[0]}" \
            "${KAFKA_HOME}/bin/kafka-topics.sh" \
            "--bootstrap-server" "${bootstrap}" "--describe" 2>/dev/null || true)
        isr_count=$(echo "$isr_out" | grep -oP 'Isr: \K[0-9,]+' \
            | tr ',' '\n' | sort -u | grep -c '.' 2>/dev/null || true)
        if [[ "$isr_count" -ge "$BROKER_COUNT" ]]; then isr_ok=true; break; fi
        isr_wait=$(( isr_wait + 3 ))
        info "  ISR: ${isr_count}/${BROKER_COUNT} — retrying in 3s..."
        sleep 3
    done

    [[ "$isr_ok" == "true" ]] \
        && info "  All ${BROKER_COUNT} broker(s) in ISR — running preferred leader election..." \
        || warn "  ISR check timed out — running election anyway..."

    election_out=$(ssh_broker "${PUB_IPS[0]}" \
        "${KAFKA_HOME}/bin/kafka-leader-election.sh" \
        "--bootstrap-server" "${bootstrap}" \
        "--election-type" "PREFERRED" \
        "--all-topic-partitions" 2>&1 || true)

    if echo "$election_out" | grep -q "error\|ERROR\|Exception"; then
        warn "  Leader election issue: ${election_out}"
    else
        success "  Partition leaders distributed across brokers"
    fi
fi

# ═══════════════════════════════════════════════════════════════════════════════
#  PHASE 3 — WRITE FINAL brokers.json
# ═══════════════════════════════════════════════════════════════════════════════

banner "PHASE 3 — Writing brokers.json"

brokers_arr="["
for i in "${!PRIV_IPS[@]}"; do
    [[ $i -gt 0 ]] && brokers_arr+=","
    brokers_arr+="{\"privateIp\":\"${PRIV_IPS[$i]}\",\"publicIp\":\"${PUB_IPS[$i]:-}\"}"
done
brokers_arr+="]"

jq -n \
    --arg comment "Kafka broker IPs for SwipeNest. Port is controlled by KAFKA_PORT in .env.local (default: 19092 — INTERNAL VPC listener). privateIp: used by in-VPC app servers (swipenest-core, swipenest-consumer). publicIp: used by external scripts (load-data.js, check-data.js). Written by deploy-kafka-brokers.sh on $(date -u +%Y-%m-%dT%H:%M:%SZ)." \
    --argjson brokers "$brokers_arr" \
    '{_comment: $comment, brokers: $brokers}' > "$BROKERS_JSON"

success "Written: ${BROKERS_JSON}"
info    "  $(jq -c '{brokers: [.brokers[] | {privateIp,publicIp}]}' "$BROKERS_JSON")"
echo ""
info "  Copy to consumer :  cp ${BROKERS_JSON} ../swipenest-kafka-consumer/brokers.json"
info "  swipenest-core reads this file automatically (no copy needed)"

# ─── Final summary ────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}${BOLD}║    Kafka Cluster Deployed Successfully                           ║${NC}"
echo -e "${GREEN}${BOLD}╠══════════════════════════════════════════════════════════════════╣${NC}"
printf "${GREEN}${BOLD}║${NC}  %-22s %s\n" "Cluster ID:"      "${CLUSTER_ID}"
printf "${GREEN}${BOLD}║${NC}  %-22s %s\n" "Region:"          "${AWS_REGION}"
printf "${GREEN}${BOLD}║${NC}  %-22s %s\n" "Instance type:"   "${INSTANCE_TYPE}"
printf "${GREEN}${BOLD}║${NC}  %-22s %s GB gp3\n" "Storage:"  "${STORAGE_GB}"
printf "${GREEN}${BOLD}║${NC}  %-22s %s\n" "Brokers:"         "${BROKER_COUNT}"
printf "${GREEN}${BOLD}║${NC}  %-22s %s\n" "Replication RF:"  "${REPLICATION_FACTOR}"
printf "${GREEN}${BOLD}║${NC}  %-22s %s\n" "min.insync.repl:" "${MIN_ISR}"
echo -e "${GREEN}${BOLD}╠══════════════════════════════════════════════════════════════════╣${NC}"
printf "  %-5s  %-24s  %-16s  %s\n" "Node" "Instance ID" "Private IP" "Public IP"
printf "  %-5s  %-24s  %-16s  %s\n" "-----" "------------------------" "----------------" "---------------"
for i in "${!INST_IDS[@]}"; do
    printf "  %-5s  %-24s  %-16s  %s\n" \
        "$(( i + 1 ))" "${INST_IDS[$i]}" "${PRIV_IPS[$i]:-N/A}" "${PUB_IPS[$i]:-N/A}"
done
echo -e "${GREEN}${BOLD}╠══════════════════════════════════════════════════════════════════╣${NC}"
echo -e "${GREEN}${BOLD}║${NC}  Topics created / verified:"
for topic in "${TOPICS[@]}"; do
    p="${TOPIC_PARTITIONS[$topic]:-existing}"
    printf "${GREEN}${BOLD}║${NC}    %-28s partitions=%s  rf=%s\n" "$topic" "$p" "${REPLICATION_FACTOR}"
done
echo -e "${GREEN}${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${CYAN}${BOLD}On-broker admin commands (SSH into any broker):${NC}"
echo -e "  ${BOLD}kt --list${NC}                            # alias: kafka-topics.sh --bootstrap-server localhost:19092"
echo -e "  ${BOLD}kt --describe --topic video_view${NC}"
echo -e "  ${YELLOW}⚠  Always use port 19092 (INTERNAL) on-broker, not 9092 (hairpin NAT issue).${NC}"
echo ""
echo -e "${CYAN}${BOLD}Load test data:${NC}"
echo -e "  ${BOLD}node scripts/load-data.js${NC}"
echo ""
echo -e "${CYAN}${BOLD}Terminate cluster when done:${NC}"
echo -e "  ${BOLD}./scripts/clear-instances.sh${NC}"
echo ""
