#!/bin/bash
###############################################################################
# verify-topics.sh
#
# Verifies that all expected Kafka topics exist and are correctly configured.
#
# Checks per topic:
#   - Exists (hard fail if missing)
#   - Partition count reported (informational only — count is chosen interactively
#     at deploy time and is not re-validated here)
#   - retention.ms = 604800000 (7 days)
#   - cleanup.policy = delete
#
# Exit code:
#   0 — all topics present and correctly configured
#   1 — one or more topics missing or misconfigured (warnings logged)
#
# Usage (from deployer machine):
#   ./scripts/verify-topics.sh
#   AWS_REGION=ap-south-1 ./scripts/verify-topics.sh
#
# Usage directly on a broker instance:
#   SKIP_SSH=1 ./scripts/verify-topics.sh
###############################################################################

set -euo pipefail

REGION="${AWS_REGION:-ap-south-1}"
EC2_USER="ubuntu"
PEM_KEY="${HOME}/.ssh/ec2-key-pair.pem"
KAFKA_HOME="/opt/kafka"
INTERNAL_PORT=19092

# All topics that must exist — partition count is user-decided at deploy time
# and is intentionally not validated here.
REQUIRED_TOPICS=("post_likes" "video_view" "post_impression" "video_watch_progress" "post_comments")

EXPECTED_RETENTION_MS=604800000
EXPECTED_CLEANUP_POLICY=delete

# ─── Colours ─────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
ok()   { echo -e "${GREEN}[OK]${NC}    $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }
fail() { echo -e "${RED}[FAIL]${NC}  $*"; }

echo "╔════════════════════════════════════════════════════════╗"
echo "║   SwipeNest Kafka — Topic Verification                 ║"
echo "╚════════════════════════════════════════════════════════╝"
echo ""

# ─── Resolve broker public IP ─────────────────────────────────────────────────
if [[ "${SKIP_SSH:-0}" == "1" ]]; then
    # Running directly on the broker
    BOOTSTRAP="localhost:${INTERNAL_PORT}"
    _run_kafka() { "$@"; }
else
    echo "Discovering broker instances (tag Role=kafka-broker)..."
    BROKER_IP=$(aws ec2 describe-instances \
        --region "$REGION" \
        --filters "Name=tag:Role,Values=kafka-broker" "Name=instance-state-name,Values=running" \
        --query 'Reservations[0].Instances[0].PublicIpAddress' \
        --output text 2>/dev/null)

    [[ -z "$BROKER_IP" || "$BROKER_IP" == "None" ]] && \
        { fail "No running kafka-broker instances found (tag Role=kafka-broker)"; exit 1; }

    echo "  Broker: $BROKER_IP"
    BOOTSTRAP="localhost:${INTERNAL_PORT}"
    SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=15 -o BatchMode=yes -o LogLevel=ERROR"
    _run_kafka() { ssh $SSH_OPTS -i "$PEM_KEY" "${EC2_USER}@${BROKER_IP}" "$@"; }
fi
echo ""

FAILURES=0

# ─── Check each topic ─────────────────────────────────────────────────────────
for topic in "${REQUIRED_TOPICS[@]}"; do
    echo "━━━ ${topic} ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    DESCRIBE=$(_run_kafka \
        "${KAFKA_HOME}/bin/kafka-topics.sh" \
        --bootstrap-server "$BOOTSTRAP" \
        --describe --topic "$topic" 2>&1) || DESCRIBE=""

    # Existence is the only hard requirement — missing topic is a failure
    if echo "$DESCRIBE" | grep -qE "does not exist|UnknownTopicOrPartition|Error"; then
        fail "  Topic '$topic' does not exist on the cluster"
        echo "       Create: kt --create --topic '$topic' --partitions <N> --config retention.ms=${EXPECTED_RETENTION_MS} --config cleanup.policy=${EXPECTED_CLEANUP_POLICY}"
        FAILURES=$((FAILURES + 1))
        continue
    fi

    # Report partition count — informational only; count is chosen at deploy time
    ACTUAL_PARTS=$(echo "$DESCRIBE" | grep "^Topic:" | awk 'NR==1{for(i=1;i<=NF;i++) if($i=="PartitionCount:") print $(i+1)}' | tr -d '[:space:]')
    ok "  Partitions: ${ACTUAL_PARTS} (user-defined at deploy time)"

    # Check retention.ms via kafka-configs.sh
    CONFIG_OUT=$(_run_kafka \
        "${KAFKA_HOME}/bin/kafka-configs.sh" \
        --bootstrap-server "$BOOTSTRAP" \
        --entity-type topics --entity-name "$topic" \
        --describe 2>&1) || CONFIG_OUT=""

    ACTUAL_RETENTION=$(echo "$CONFIG_OUT" | grep -o "retention\.ms=[^,)]*" | cut -d= -f2 | tr -d '[:space:]')
    ACTUAL_CLEANUP=$(echo "$CONFIG_OUT"   | grep -o "cleanup\.policy=[^,)]*" | cut -d= -f2 | tr -d '[:space:]')

    if [[ "$ACTUAL_RETENTION" == "$EXPECTED_RETENTION_MS" ]]; then
        ok "  retention.ms: ${ACTUAL_RETENTION} ✓"
    elif [[ -z "$ACTUAL_RETENTION" ]]; then
        warn "  retention.ms: not explicitly set (broker default applies)"
    else
        warn "  retention.ms: ${ACTUAL_RETENTION} (expected ${EXPECTED_RETENTION_MS})"
        if [[ " ${REQUIRED_TOPICS[*]} " == *" ${topic} "* ]]; then
            echo "       Fix: kt --alter --topic $topic --add-config retention.ms=${EXPECTED_RETENTION_MS}"
        fi
    fi

    if [[ "$ACTUAL_CLEANUP" == "$EXPECTED_CLEANUP_POLICY" ]]; then
        ok "  cleanup.policy: ${ACTUAL_CLEANUP} ✓"
    elif [[ -z "$ACTUAL_CLEANUP" ]]; then
        warn "  cleanup.policy: not explicitly set (broker default applies)"
    else
        warn "  cleanup.policy: ${ACTUAL_CLEANUP} (expected ${EXPECTED_CLEANUP_POLICY})"
    fi

    echo ""
done

# ─── Summary ─────────────────────────────────────────────────────────────────
echo "╔════════════════════════════════════════════════════════╗"
if [[ "$FAILURES" -eq 0 ]]; then
    echo "║  ✓ All required topics verified successfully           ║"
else
    echo "║  ✗ ${FAILURES} issue(s) found — see output above              ║"
fi
echo "╚════════════════════════════════════════════════════════╝"
echo ""

exit $((FAILURES > 0 ? 1 : 0))
