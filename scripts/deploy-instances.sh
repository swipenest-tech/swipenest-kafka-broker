#!/usr/bin/env bash
###############################################################################
# deploy-instances.sh
#
# Provisions EC2 instances for a Kafka cluster.
#
# What this script does:
#   1. Asks for broker count, instance type, and storage size
#   2. Creates (or reuses) a security group with ports 22/9092/9093/19092
#   3. Launches N EC2 instances from the Kafka AMI
#   4. Waits until running + status checks pass
#   5. Saves instance metadata to scripts/kafka-instances.json
#   6. Updates brokers.json (project root) with the private IPs —
#      ready to copy directly to swipenest-consumer/brokers.json
#
# AMI: ami-081dfc9f291f572f7  (Ubuntu, Kafka preinstalled at /opt/kafka)
#
# Usage:
#   ./scripts/deploy-instances.sh [--region ap-south-1] [--dry-run]
#
# After running, execute:
#   ./scripts/configure-cluster.sh
###############################################################################

set -euo pipefail

# ─── Defaults ─────────────────────────────────────────────────────────────────
AWS_REGION="${AWS_REGION:-ap-south-1}"
KAFKA_AMI="ami-081dfc9f291f572f7"
DEFAULT_KEY_NAME="ec2-key-pair"
DEFAULT_PEM_KEY="${HOME}/.ssh/ec2-key-pair.pem"
DEFAULT_SUBNET_ID="subnet-0da50cf2f3ebd9280"
KAFKA_SG_NAME="swipenest-kafka-sg"
KAFKA_PORT=9092
INTERNAL_PORT=19092
CONTROLLER_PORT=9093
DRY_RUN=false

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
OUTPUT_FILE="${SCRIPT_DIR}/kafka-instances.json"
BROKERS_JSON="${PROJECT_ROOT}/brokers.json"

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
while [[ $# -gt 0 ]]; do
    case $1 in
        --region)  AWS_REGION="$2"; shift 2 ;;
        --dry-run) DRY_RUN=true;    shift   ;;
        --help|-h)
            sed -n '2,30p' "$0" | sed 's/^# *//'
            exit 0 ;;
        *) die "Unknown argument: $1. Use --help." ;;
    esac
done

export AWS_DEFAULT_REGION="$AWS_REGION"

# ─── Header ───────────────────────────────────────────────────────────────────
echo -e "${BOLD}${BLUE}"
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║    SwipeNest — Kafka EC2 Instance Provisioner                    ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
[[ "$DRY_RUN" == "true" ]] && warn "DRY-RUN mode — no AWS resources will be created."

# ─── Preflight ────────────────────────────────────────────────────────────────
banner "Preflight Checks"
missing=()
for cmd in aws jq; do
    command -v "$cmd" &>/dev/null || missing+=("$cmd")
done
[[ ${#missing[@]} -gt 0 ]] && die "Missing required tools: ${missing[*]}"
if [[ "$DRY_RUN" == "false" ]]; then
    aws sts get-caller-identity &>/dev/null || die "AWS credentials not configured."
fi
success "All dependencies present"

# ─── Broker count ─────────────────────────────────────────────────────────────
ask_broker_count() {
    banner "Broker Count"
    while true; do
        printf "Number of brokers to provision [1-20]: "
        read -r BROKER_COUNT
        [[ "$BROKER_COUNT" =~ ^[0-9]+$ ]] \
            && [[ "$BROKER_COUNT" -ge 1 ]] \
            && [[ "$BROKER_COUNT" -le 20 ]] && break
        warn "Enter an integer between 1 and 20."
    done
    if [[ "$BROKER_COUNT" -lt 3 ]]; then
        warn "Minimum 3 brokers is strongly recommended for production."
        printf "Continue with %s broker(s)? [y/N]: " "$BROKER_COUNT"
        read -r CNF
        [[ "$CNF" =~ ^[Yy]$ ]] || die "Aborted."
    fi
    success "Broker count: ${BROKER_COUNT}"
}

# ─── Instance type ────────────────────────────────────────────────────────────
ask_instance_type() {
    banner "Instance Type"
    echo "  Low capacity:"
    printf "  %-4s %-14s %s\n" "1)" "t3.micro"   "2 vCPU,  1 GB RAM   (testing only)"
    printf "  %-4s %-14s %s\n" "2)" "t3.small"   "2 vCPU,  2 GB RAM   (testing only)"
    echo ""
    echo "  Medium capacity:"
    printf "  %-4s %-14s %s\n" "3)" "t3.medium"  "2 vCPU,  4 GB RAM"
    printf "  %-4s %-14s %s\n" "4)" "t3.large"   "2 vCPU,  8 GB RAM"
    echo ""
    echo "  High capacity:"
    printf "  %-4s %-14s %s\n" "5)" "m5.large"   "2 vCPU,  8 GB RAM   (recommended)"
    printf "  %-4s %-14s %s\n" "6)" "m5.xlarge"  "4 vCPU, 16 GB RAM"
    printf "  %-4s %-14s %s\n" "7)" "m5.2xlarge" "8 vCPU, 32 GB RAM"
    printf "  %-4s %-14s %s\n" "8)" "Custom"     "(enter manually)"
    echo ""
    while true; do
        printf "Select instance type [1-8]: "
        read -r ITYPE_CHOICE
        case "$ITYPE_CHOICE" in
            1) INSTANCE_TYPE="t3.micro";   break ;;
            2) INSTANCE_TYPE="t3.small";   break ;;
            3) INSTANCE_TYPE="t3.medium";  break ;;
            4) INSTANCE_TYPE="t3.large";   break ;;
            5) INSTANCE_TYPE="m5.large";   break ;;
            6) INSTANCE_TYPE="m5.xlarge";  break ;;
            7) INSTANCE_TYPE="m5.2xlarge"; break ;;
            8) printf "Enter instance type (e.g. r6i.large): "
               read -r INSTANCE_TYPE
               [[ -n "$INSTANCE_TYPE" ]] && break ;;
            *) warn "Enter a number between 1 and 8." ;;
        esac
    done
    success "Instance type: ${INSTANCE_TYPE}"
}

# ─── Storage ──────────────────────────────────────────────────────────────────
ask_storage_size() {
    banner "Storage Configuration"
    while true; do
        printf "EBS volume size per broker in GB [8-5120]: "
        read -r STORAGE_GB
        [[ "$STORAGE_GB" =~ ^[0-9]+$ ]] \
            && [[ "$STORAGE_GB" -ge 8 ]] \
            && [[ "$STORAGE_GB" -le 5120 ]] && break
        warn "Storage must be between 8 and 5120 GB."
    done
    success "Storage: ${STORAGE_GB} GB gp3"
}

# ─── SSH key ──────────────────────────────────────────────────────────────────
ask_key_config() {
    banner "SSH Key Configuration"
    printf "Key pair name [default: %s]: " "$DEFAULT_KEY_NAME"
    read -r KEY_INPUT
    KEY_NAME="${KEY_INPUT:-$DEFAULT_KEY_NAME}"

    printf "PEM key path [default: %s]: " "$DEFAULT_PEM_KEY"
    read -r PEM_INPUT
    PEM_KEY="${PEM_INPUT:-$DEFAULT_PEM_KEY}"
    PEM_KEY="${PEM_KEY/#\~/$HOME}"

    if [[ "$DRY_RUN" == "false" ]]; then
        [[ -f "$PEM_KEY" ]] || die "PEM key not found: ${PEM_KEY}"
        chmod 600 "$PEM_KEY"
    fi
    info "Key pair : ${KEY_NAME}"
    info "PEM key  : ${PEM_KEY}"
}

# ─── Security group ───────────────────────────────────────────────────────────
ensure_security_group() {
    banner "Security Group"

    local sg_id
    sg_id=$(aws ec2 describe-security-groups \
        --filters "Name=group-name,Values=${KAFKA_SG_NAME}" \
        --query "SecurityGroups[0].GroupId" \
        --output text 2>/dev/null)

    if [[ "$sg_id" == "None" ]] || [[ -z "$sg_id" ]]; then
        info "Creating security group '${KAFKA_SG_NAME}'..."

        local vpc_id vpc_cidr
        vpc_id=$(aws ec2 describe-subnets \
            --subnet-ids "$DEFAULT_SUBNET_ID" \
            --query "Subnets[0].VpcId" --output text)
        vpc_cidr=$(aws ec2 describe-vpcs \
            --vpc-ids "$vpc_id" \
            --query "Vpcs[0].CidrBlock" --output text)

        sg_id=$(aws ec2 create-security-group \
            --group-name "$KAFKA_SG_NAME" \
            --description "SwipeNest Kafka: 9092 client, 9093 controller, 19092 internal, 22 SSH" \
            --vpc-id "$vpc_id" \
            --query "GroupId" --output text)

        # 9092 — VPC-wide client access (app servers inside VPC)
        aws ec2 authorize-security-group-ingress \
            --group-id "$sg_id" --protocol tcp --port "$KAFKA_PORT" \
            --cidr "$vpc_cidr" > /dev/null

        # 9092 — intra-cluster (self-referencing)
        aws ec2 authorize-security-group-ingress \
            --group-id "$sg_id" --protocol tcp --port "$KAFKA_PORT" \
            --source-group "$sg_id" > /dev/null

        # 19092 — INTERNAL listener: inter-broker replication (VPC-wide)
        aws ec2 authorize-security-group-ingress \
            --group-id "$sg_id" --protocol tcp --port "$INTERNAL_PORT" \
            --cidr "$vpc_cidr" > /dev/null

        # 19092 — INTERNAL listener: intra-cluster (self-referencing)
        aws ec2 authorize-security-group-ingress \
            --group-id "$sg_id" --protocol tcp --port "$INTERNAL_PORT" \
            --source-group "$sg_id" > /dev/null

        # 9093 — KRaft controller election (intra-cluster)
        aws ec2 authorize-security-group-ingress \
            --group-id "$sg_id" --protocol tcp --port "$CONTROLLER_PORT" \
            --source-group "$sg_id" > /dev/null

        # 22 — SSH from current public IP only
        local my_ip
        my_ip=$(curl -sf https://checkip.amazonaws.com 2>/dev/null \
            || curl -sf https://ifconfig.me 2>/dev/null \
            || echo "0.0.0.0")
        local ssh_cidr="${my_ip}/32"
        [[ "$my_ip" == "0.0.0.0" ]] && ssh_cidr="0.0.0.0/0"
        aws ec2 authorize-security-group-ingress \
            --group-id "$sg_id" --protocol tcp --port 22 \
            --cidr "$ssh_cidr" > /dev/null

        # 9092 — Kafka client access from operator's external IP (scripts + KafkaJS)
        aws ec2 authorize-security-group-ingress \
            --group-id "$sg_id" --protocol tcp --port "$KAFKA_PORT" \
            --cidr "$ssh_cidr" > /dev/null

        success "Created security group ${sg_id} (VPC: ${vpc_id}, operator cidr: ${ssh_cidr})"
    else
        success "Reusing existing security group: ${sg_id}"
        local my_ip
        my_ip=$(curl -sf https://checkip.amazonaws.com 2>/dev/null \
            || curl -sf https://ifconfig.me 2>/dev/null \
            || echo "0.0.0.0")
        local op_cidr="${my_ip}/32"
        [[ "$my_ip" == "0.0.0.0" ]] && op_cidr="0.0.0.0/0"
        aws ec2 authorize-security-group-ingress \
            --group-id "$sg_id" --protocol tcp --port 22 \
            --cidr "$op_cidr" 2>/dev/null || true
        aws ec2 authorize-security-group-ingress \
            --group-id "$sg_id" --protocol tcp --port "$KAFKA_PORT" \
            --cidr "$op_cidr" 2>/dev/null || true
        aws ec2 authorize-security-group-ingress \
            --group-id "$sg_id" --protocol tcp --port "$INTERNAL_PORT" \
            --source-group "$sg_id" 2>/dev/null || true
        info "SSH + Kafka access ensured for: ${op_cidr}"
    fi

    KAFKA_SG_ID="$sg_id"
}

# ─── Launch instances ─────────────────────────────────────────────────────────
launch_instances() {
    banner "Launching EC2 Instances"
    info "AMI    : ${KAFKA_AMI}"
    info "Type   : ${INSTANCE_TYPE}"
    info "Count  : ${BROKER_COUNT}"
    info "Storage: ${STORAGE_GB} GB gp3"
    info "Subnet : ${DEFAULT_SUBNET_ID}"

    if [[ "$DRY_RUN" == "true" ]]; then
        warn "[DRY-RUN] Would launch ${BROKER_COUNT} x ${INSTANCE_TYPE}. Skipping."
        INSTANCE_IDS=("i-dryrun0001" "i-dryrun0002" "i-dryrun0003")
        return
    fi

    local block_device
    block_device=$(jq -n --argjson size "$STORAGE_GB" \
        '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":$size,"VolumeType":"gp3","DeleteOnTermination":true}}]')

    local run_result
    run_result=$(aws ec2 run-instances \
        --image-id "$KAFKA_AMI" \
        --instance-type "$INSTANCE_TYPE" \
        --count "$BROKER_COUNT" \
        --key-name "$KEY_NAME" \
        --security-group-ids "$KAFKA_SG_ID" \
        --subnet-id "$DEFAULT_SUBNET_ID" \
        --associate-public-ip-address \
        --block-device-mappings "$block_device" \
        --tag-specifications \
            "ResourceType=instance,Tags=[{Key=Name,Value=Kafka-Broker},{Key=Cluster,Value=KafkaCluster},{Key=Project,Value=SwipeNest},{Key=Role,Value=kafka-broker}]" \
        --output json)

    mapfile -t INSTANCE_IDS < <(echo "$run_result" | jq -r '.Instances[].InstanceId')
    success "Launched instances: ${INSTANCE_IDS[*]}"

    for i in "${!INSTANCE_IDS[@]}"; do
        local idx=$(( i + 1 ))
        aws ec2 create-tags \
            --resources "${INSTANCE_IDS[$i]}" \
            --tags "Key=Name,Value=Kafka-Broker-${idx}" > /dev/null
        info "  Tagged ${INSTANCE_IDS[$i]} → Name=Kafka-Broker-${idx}"
    done
}

# ─── Wait running ─────────────────────────────────────────────────────────────
wait_for_running() {
    banner "Waiting for Running State"
    [[ "$DRY_RUN" == "true" ]] && { warn "[DRY-RUN] Skipping."; return; }
    info "Waiting for all instances to reach 'running'..."
    aws ec2 wait instance-running --instance-ids "${INSTANCE_IDS[@]}"
    success "All instances are running"
    info "Waiting 30s for SSH daemon to start..."
    sleep 30
}

# ─── Wait status checks ───────────────────────────────────────────────────────
wait_for_status_ok() {
    banner "Waiting for Status Checks (2–3 min)"
    [[ "$DRY_RUN" == "true" ]] && { warn "[DRY-RUN] Skipping."; return; }
    info "Waiting for instance status checks to pass..."
    aws ec2 wait instance-status-ok --instance-ids "${INSTANCE_IDS[@]}"
    success "All instance status checks passed"
}

# ─── Fetch IPs ────────────────────────────────────────────────────────────────
get_instance_details() {
    banner "Fetching Instance Details"
    if [[ "$DRY_RUN" == "true" ]]; then
        PRIVATE_IPS=("10.0.1.101" "10.0.1.102" "10.0.1.103")
        PUBLIC_IPS=("1.2.3.101" "1.2.3.102" "1.2.3.103")
        return
    fi

    mapfile -t PRIVATE_IPS < <(
        aws ec2 describe-instances \
            --instance-ids "${INSTANCE_IDS[@]}" \
            --query "Reservations[].Instances[].PrivateIpAddress" \
            --output text | tr '\t' '\n')

    mapfile -t PUBLIC_IPS < <(
        aws ec2 describe-instances \
            --instance-ids "${INSTANCE_IDS[@]}" \
            --query "Reservations[].Instances[].PublicIpAddress" \
            --output text | tr '\t' '\n')

    success "Private IPs : ${PRIVATE_IPS[*]}"
    success "Public IPs  : ${PUBLIC_IPS[*]}"
}

# ─── Save output ──────────────────────────────────────────────────────────────
save_output() {
    banner "Saving Output"

    local instances_json='[]'
    for i in "${!INSTANCE_IDS[@]}"; do
        local node_id=$(( i + 1 ))
        instances_json=$(echo "$instances_json" | jq \
            --arg   id   "${INSTANCE_IDS[$i]}" \
            --arg   priv "${PRIVATE_IPS[$i]:-}" \
            --arg   pub  "${PUBLIC_IPS[$i]:-}" \
            --argjson n  "$node_id" \
            '. + [{"instance_id":$id,"private_ip":$priv,"public_ip":$pub,"node_id":$n}]')
    done

    jq -n \
        --argjson instances "$instances_json" \
        --arg region       "$AWS_REGION" \
        --arg ami          "$KAFKA_AMI" \
        --arg instance_type "$INSTANCE_TYPE" \
        --arg storage_gb   "$STORAGE_GB" \
        --arg created_at   "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        '{
            instances:     $instances,
            region:        $region,
            ami:           $ami,
            instance_type: $instance_type,
            storage_gb:    $storage_gb,
            created_at:    $created_at
        }' > "$OUTPUT_FILE"

    success "Saved: scripts/kafka-instances.json"

    # ── Update brokers.json (project root) ───────────────────────────────────
    # Same format as swipenest-consumer/brokers.json — copy directly after deploy.
    local brokers_arr="["
    for i in "${!INSTANCE_IDS[@]}"; do
        [[ $i -gt 0 ]] && brokers_arr+=","
        brokers_arr+="{\"privateIp\":\"${PRIVATE_IPS[$i]:-}\"}"
    done
    brokers_arr+="]"

    jq -n \
        --arg comment "Kafka broker private IPs for VPC-internal connections (INTERNAL listener, port 19092). Updated by deploy-instances.sh on $(date -u +%Y-%m-%dT%H:%M:%SZ). Copy this file to swipenest-consumer/brokers.json after deployment." \
        --argjson brokers "$brokers_arr" \
        '{_comment: $comment, brokers: $brokers}' > "$BROKERS_JSON"

    success "Updated: brokers.json  ← copy to swipenest-consumer/brokers.json"
}

# ─── Summary ──────────────────────────────────────────────────────────────────
print_summary() {
    echo ""
    echo -e "${GREEN}${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}${BOLD}║    Kafka EC2 instances provisioned successfully                  ║${NC}"
    echo -e "${GREEN}${BOLD}╠══════════════════════════════════════════════════════════════════╣${NC}"
    printf "${GREEN}${BOLD}║${NC}  %-18s %s\n" "Region:"  "$AWS_REGION"
    printf "${GREEN}${BOLD}║${NC}  %-18s %s\n" "AMI:"     "$KAFKA_AMI"
    printf "${GREEN}${BOLD}║${NC}  %-18s %s\n" "Type:"    "$INSTANCE_TYPE"
    printf "${GREEN}${BOLD}║${NC}  %-18s %s GB gp3\n" "Storage:" "$STORAGE_GB"
    echo -e "${GREEN}${BOLD}╠══════════════════════════════════════════════════════════════════╣${NC}"
    printf "  %-4s  %-24s  %-16s  %s\n" "Node" "Instance ID" "Private IP" "Public IP"
    printf "  %-4s  %-24s  %-16s  %s\n" "----" "------------------------" "----------------" "---------------"
    for i in "${!INSTANCE_IDS[@]}"; do
        printf "  %-4s  %-24s  %-16s  %s\n" \
            "$(( i + 1 ))" \
            "${INSTANCE_IDS[$i]}" \
            "${PRIVATE_IPS[$i]:-N/A}" \
            "${PUBLIC_IPS[$i]:-N/A}"
    done
    echo -e "${GREEN}${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "${YELLOW}${BOLD}NEXT STEP:${NC} Configure Kafka on these instances:"
    echo -e "  ${CYAN}./scripts/configure-cluster.sh${NC}"
    echo -e "  (reads scripts/kafka-instances.json automatically)"
    echo ""
    echo -e "${CYAN}brokers.json has been updated — copy to swipenest-consumer when ready:${NC}"
    echo -e "  cp ${BROKERS_JSON} ../swipenest-consumer/brokers.json"
    echo ""
}

# ─── Main ─────────────────────────────────────────────────────────────────────
main() {
    ask_broker_count
    ask_instance_type
    ask_storage_size
    ask_key_config

    if [[ "$DRY_RUN" == "false" ]]; then
        ensure_security_group
    else
        KAFKA_SG_ID="sg-dryrun"
    fi

    launch_instances
    wait_for_running
    wait_for_status_ok
    get_instance_details
    save_output
    print_summary
}

main
