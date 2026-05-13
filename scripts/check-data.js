/**
 * check-data.js
 *
 * Kafka health and distribution report — read-only, no produce.
 *
 * What this script does:
 *   1. Auto-discovers live Kafka broker EC2 instances (tag Role=kafka-broker)
 *   2. SSHes each broker to read its node.id
 *   3. Opens SSH tunnels with loopback aliases (private-IP-based, same as prod)
 *   4. Connects via KafkaJS Admin API and prints:
 *      a. Partition distribution table — topic / partition / leader / replicas / ISR / message count
 *      b. Per-broker leader summary   — partitions led, topics, % of total messages
 *      c. Consumer group lag report   — per topic/partition: committed offset, end offset, lag
 *   5. Closes SSH tunnels and loopback aliases cleanly
 *
 * Usage:
 *   node scripts/check-data.js
 *   KAFKA_GROUP_ID=my-group node scripts/check-data.js
 *   AWS_REGION=ap-south-1 node scripts/check-data.js
 *   SKIP_LOOPBACK=1 node scripts/check-data.js   # single-broker only
 */

'use strict';

const path = require('path');
// Load .env.local for local overrides
try {
    require('dotenv').config({ path: path.join(__dirname, '..', '.env.local') });
} catch (_) { /* dotenv is optional */ }

const { execSync, spawn } = require('child_process');
const { Kafka, logLevel } = require('kafkajs');

// ─── Config ───────────────────────────────────────────────────────────────────
const AWS_REGION      = process.env.AWS_REGION      || 'ap-south-1';
const KAFKA_GROUP_ID  = process.env.KAFKA_GROUP_ID  || 'swipenest-consumer-group';
const EC2_USER        = 'ubuntu';
const PEM_KEY         = path.join(process.env.HOME, '.ssh/ec2-key-pair.pem');
const TOPICS          = ['video_view', 'post_impression', 'video_watch_progress', 'post_likes', 'post_comments'];

// ─── SSH tunnel state ─────────────────────────────────────────────────────────
const _sshProcs     = [];
const _loopbackIps  = [];
let   LOCAL_BROKERS = [];

// ─── Helpers ──────────────────────────────────────────────────────────────────
const sleep = ms => new Promise(r => setTimeout(r, ms));
const SSH_OPTS = `-i ${PEM_KEY} -o StrictHostKeyChecking=no -o ConnectTimeout=10 -o BatchMode=yes -o LogLevel=ERROR`;

// ─── AWS broker discovery ─────────────────────────────────────────────────────
function discoverBrokers() {
    console.log('[Discover] Querying AWS for running kafka-broker instances...');
    const raw = execSync(
        `aws ec2 describe-instances \
          --region ${AWS_REGION} \
          --filters "Name=tag:Role,Values=kafka-broker" "Name=instance-state-name,Values=running" \
          --query "Reservations[].Instances[].[PublicIpAddress,PrivateIpAddress,InstanceId]" \
          --output json`,
        { encoding: 'utf8' }
    );
    const list = JSON.parse(raw);
    if (!list.length) throw new Error('No running kafka-broker instances found (tag Role=kafka-broker).');
    const brokers = list.map(([pub, priv, id]) => ({ publicIp: pub, privateIp: priv, instanceId: id }));
    brokers.forEach(b =>
        console.log(`  ${b.instanceId}  private=${b.privateIp}  public=${b.publicIp}`)
    );
    return brokers;
}

// ─── Read node.id from each broker via SSH ────────────────────────────────────
function getNodeId(publicIp) {
    const out = execSync(
        `ssh ${SSH_OPTS} ${EC2_USER}@${publicIp} ` +
        `"sudo grep -m1 '^node.id=' /opt/kafka/data/meta.properties 2>/dev/null || ` +
        `grep -m1 '^node.id=' /opt/kafka/config/kraft/server.properties"`,
        { encoding: 'utf8', timeout: 15000 }
    ).trim();
    const m = out.match(/node\.id=(\d+)/);
    if (!m) throw new Error(`node.id not found in output for ${publicIp}: ${out}`);
    return parseInt(m[1], 10);
}

// ─── SSH tunnels with loopback aliases ────────────────────────────────────────
//
// Production brokers advertise PRIVATE IP on port 9092 (INTERNAL listener).
// To reach them from a dev machine / CI:
//   1. Add loopback alias:  sudo ip addr add <privateIp>/32 dev lo
//   2. SSH tunnel binding to that alias:
//        ssh -L <privateIp>:9092:<privateIp>:9092 ubuntu@<publicIp>
//   3. Set KAFKA_BROKER = privateIp:9092,...
//
// KafkaJS metadata responses reference private IPs; the loopback alias + tunnel
// makes those IPs reachable locally. SKIP_LOOPBACK=1 disables alias setup
// (single-broker use only — metadata redirects to other brokers will fail).
//
async function openTunnels(brokers) {
    console.log('\n[Tunnel] Reading node.id from each broker...');
    for (const b of brokers) {
        b.nodeId = getNodeId(b.publicIp);
        console.log(`  node.id=${b.nodeId}  private=${b.privateIp}  public=${b.publicIp}`);
    }

    const skipLoopback = process.env.SKIP_LOOPBACK === '1';

    if (!skipLoopback) {
        console.log('[Tunnel] Adding loopback aliases (requires sudo)...');
        for (const b of brokers) {
            try {
                execSync(`sudo ip addr add ${b.privateIp}/32 dev lo 2>/dev/null || true`,
                    { encoding: 'utf8', stdio: 'pipe' });
                _loopbackIps.push(b.privateIp);
                console.log(`  added lo alias ${b.privateIp}`);
            } catch (err) {
                console.warn(`[Tunnel] Warning: loopback alias for ${b.privateIp} failed: ${err.message}`);
            }
        }
    }

    for (const b of brokers) {
        const localBind = skipLoopback
            ? `127.0.0.1:${9091 + b.nodeId}`
            : `${b.privateIp}:9092`;

        const proc = spawn('ssh', [
            '-i', PEM_KEY,
            '-o', 'StrictHostKeyChecking=no',
            '-o', 'ExitOnForwardFailure=yes',
            '-N', '-L', `${localBind}:${b.privateIp}:9092`,
            `${EC2_USER}@${b.publicIp}`,
        ], { stdio: 'ignore', detached: false });
        proc.on('error', err => console.error(`[Tunnel] node.id=${b.nodeId} error:`, err.message));
        _sshProcs.push(proc);
        b.localAddr = localBind;
        console.log(`[Tunnel] node.id=${b.nodeId}: ${localBind} → ${b.publicIp}:9092`);
    }

    LOCAL_BROKERS = brokers.map(b => b.localAddr);
    process.env.KAFKA_BROKER = LOCAL_BROKERS.join(',');

    console.log('[Tunnel] Waiting 5s for tunnels to establish...');
    await sleep(5000);
    console.log(`[Tunnel] ${brokers.length} tunnel(s) ready → KAFKA_BROKER=${process.env.KAFKA_BROKER}\n`);
}

function closeTunnels() {
    for (const proc of _sshProcs) {
        try { proc.kill('SIGTERM'); } catch (_) {}
    }
    _sshProcs.length = 0;
    for (const ip of _loopbackIps) {
        try {
            execSync(`sudo ip addr del ${ip}/32 dev lo 2>/dev/null || true`,
                { encoding: 'utf8', stdio: 'pipe' });
        } catch (_) {}
    }
    _loopbackIps.length = 0;
    console.log('[Tunnel] Closed — loopback aliases removed');
}

// ─── Partition distribution report ────────────────────────────────────────────
async function printPartitionReport(admin, brokerIdToIp) {
    console.log('\n[Report] Fetching partition + broker distribution...');
    const meta = await admin.fetchTopicMetadata({ topics: TOPICS });

    const offsetMap = {};
    for (const topic of TOPICS) {
        offsetMap[topic] = {};
        const offs = await admin.fetchTopicOffsets(topic);
        for (const p of offs) {
            offsetMap[topic][p.partition] = {
                low:   parseInt(p.low,  10),
                high:  parseInt(p.high, 10),
                count: Math.max(0, parseInt(p.high, 10) - parseInt(p.low, 10)),
            };
        }
    }

    const H = '═'.repeat(96);
    const h = '─'.repeat(96);

    console.log(`\n╔${H}╗`);
    console.log(`║  ${'KAFKA PARTITION DISTRIBUTION REPORT'.padEnd(94)}║`);
    console.log(`╠${H}╣`);
    console.log(
        `║  ${'Topic'.padEnd(26)}${'Part'.padEnd(6)}${'Leader IP'.padEnd(18)}${'Replicas'.padEnd(22)}${'ISR'.padEnd(16)}${'Messages'.padStart(8)}  ║`
    );
    console.log(`╠${H}╣`);

    let grandTotal = 0;
    const topicTotals = {};

    for (const topicMeta of meta.topics) {
        const topic = topicMeta.name;
        topicTotals[topic] = 0;
        const sorted = [...topicMeta.partitions].sort((a, b) => a.partitionId - b.partitionId);

        for (const p of sorted) {
            const leaderIp   = brokerIdToIp[p.leader] || `broker-${p.leader}`;
            const replicaStr = p.replicas.map(r => brokerIdToIp[r] || `b${r}`).join(',');
            const isrStr     = p.isr.map(r => brokerIdToIp[r] || `b${r}`).join(',');
            const count      = offsetMap[topic]?.[p.partitionId]?.count ?? 0;
            topicTotals[topic] += count;
            grandTotal         += count;

            console.log(
                `║  ${topic.padEnd(26)}${String(p.partitionId).padEnd(6)}${leaderIp.padEnd(18)}` +
                `${replicaStr.substring(0, 20).padEnd(22)}${isrStr.substring(0, 14).padEnd(16)}${String(count).padStart(8)}  ║`
            );
        }

        console.log(`╠${h}╣`);
        console.log(`║  ${'  Total for ' + topic}${' '.repeat(Math.max(0, 72 - topic.length - 12))}${String(topicTotals[topic]).padStart(8)}  ║`);
        console.log(`╠${H}╣`);
    }

    console.log(`║  ${'GRAND TOTAL (all topics)'.padEnd(88)}${String(grandTotal).padStart(6)}  ║`);
    console.log(`╚${H}╝\n`);

    // Per-broker leader summary
    const brokerLeadParts = {};
    const brokerLeadTopics = {};
    const brokerMsgs = {};

    for (const topicMeta of meta.topics) {
        const topic = topicMeta.name;
        for (const p of topicMeta.partitions) {
            const lid = p.leader;
            brokerLeadParts[lid]  = (brokerLeadParts[lid]  || 0) + 1;
            brokerLeadTopics[lid] = brokerLeadTopics[lid] || new Set();
            brokerLeadTopics[lid].add(topic);
            brokerMsgs[lid] = (brokerMsgs[lid] || 0) + (offsetMap[topic]?.[p.partitionId]?.count ?? 0);
        }
    }

    console.log(`╔${H}╗`);
    console.log(`║  ${'PER-BROKER LEADER SUMMARY'.padEnd(94)}║`);
    console.log(`╠${H}╣`);
    console.log(
        `║  ${'Private IP'.padEnd(18)}${'Node ID'.padEnd(10)}${'Instance ID'.padEnd(22)}${'Parts Led'.padEnd(10)}${'Topics'.padEnd(8)}${'% of Total'.padEnd(12)}${'Messages'.padStart(12)}  ║`
    );
    console.log(`╠${H}╣`);

    for (const [nodeIdStr, msgs] of Object.entries(brokerMsgs)
        .sort((a, b) => parseInt(a[0]) - parseInt(b[0]))) {
        const nodeId     = parseInt(nodeIdStr, 10);
        const ip         = brokerIdToIp[nodeId] || `broker-${nodeId}`;
        const pct        = grandTotal > 0 ? ((msgs / grandTotal) * 100).toFixed(1) + '%' : '0.0%';
        const numParts   = brokerLeadParts[nodeId] || 0;
        const numTopics  = (brokerLeadTopics[nodeId] || new Set()).size;
        const instanceId = Object.values(brokerIdToIp).find(v => v === ip) || 'unknown';
        // Find the actual broker object for the instance ID
        console.log(
            `║  ${ip.padEnd(18)}${String(nodeId).padEnd(10)}${'n/a'.padEnd(22)}` +
            `${String(numParts).padEnd(10)}${String(numTopics).padEnd(8)}${pct.padEnd(12)}${String(msgs).padStart(12)}  ║`
        );
    }
    console.log(`╚${H}╝`);

    return { grandTotal, topicTotals, offsetMap };
}

// ─── Consumer group lag report ────────────────────────────────────────────────
async function printConsumerLag(admin) {
    console.log(`\n[Lag] Fetching consumer group offsets for '${KAFKA_GROUP_ID}'...`);

    // Fetch consumer group committed offsets for all topics
    let groupOffsets;
    try {
        groupOffsets = await admin.fetchOffsets({ groupId: KAFKA_GROUP_ID, topics: TOPICS });
    } catch (err) {
        console.warn(`[Lag] Could not fetch offsets for group '${KAFKA_GROUP_ID}': ${err.message}`);
        console.warn('[Lag] Consumer group has no committed offsets on this cluster yet (group is new or never consumed here).');
        return;
    }

    // Fetch end offsets (high watermarks) for all topics
    const endOffsets = {};
    for (const topic of TOPICS) {
        endOffsets[topic] = {};
        try {
            const offs = await admin.fetchTopicOffsets(topic);
            for (const p of offs) {
                endOffsets[topic][p.partition] = parseInt(p.high, 10);
            }
        } catch (err) {
            console.warn(`[Lag] Could not fetch end offsets for ${topic}: ${err.message}`);
        }
    }

    // Build lag map
    const lagMap = {};     // topic → partition → lag
    let totalLag  = 0;
    let totalMsgs = 0;

    for (const { topic, partitions } of groupOffsets) {
        lagMap[topic] = {};
        for (const { partition, offset, metadata } of partitions) {
            const committed = parseInt(offset, 10);
            const endOffset = endOffsets[topic]?.[partition] ?? 0;
            // offset = -1 means the partition has not been committed yet
            const committedAdj = committed < 0 ? 0 : committed;
            const lag = Math.max(0, endOffset - committedAdj);
            lagMap[topic][partition] = {
                committed: committedAdj,
                endOffset,
                lag,
            };
            totalLag  += lag;
            totalMsgs += endOffset;
        }
    }

    // Print table
    const H = '═'.repeat(80);
    const h = '─'.repeat(80);

    console.log(`\n╔${H}╗`);
    console.log(`║  ${'CONSUMER GROUP LAG REPORT'.padEnd(78)}║`);
    console.log(`║  ${'Group: ' + KAFKA_GROUP_ID}${' '.repeat(Math.max(0, 78 - KAFKA_GROUP_ID.length - 7))}║`);
    console.log(`╠${H}╣`);
    console.log(
        `║  ${'Topic'.padEnd(28)}${'Part'.padEnd(6)}${'Committed'.padEnd(14)}${'End Offset'.padEnd(12)}${'Lag'.padStart(10)}  ║`
    );
    console.log(`╠${H}╣`);

    let anyLag = false;
    for (const topic of TOPICS) {
        if (!lagMap[topic]) continue;
        const partNums = Object.keys(lagMap[topic]).map(Number).sort((a, b) => a - b);
        for (const part of partNums) {
            const { committed, endOffset, lag } = lagMap[topic][part];
            if (lag > 0) anyLag = true;
            console.log(
                `║  ${topic.padEnd(28)}${String(part).padEnd(6)}${String(committed).padEnd(14)}` +
                `${String(endOffset).padEnd(12)}${String(lag).padStart(10)}  ║`
            );
        }
        console.log(`╠${h}╣`);
    }

    const lagPct = totalMsgs > 0 ? ((totalLag / totalMsgs) * 100).toFixed(2) : '0.00';
    console.log(`║  ${'TOTAL LAG'.padEnd(60)}${String(totalLag).padStart(8)}  ║`);
    console.log(`║  ${'LAG AS % OF TOTAL MESSAGES'.padEnd(58)}${(lagPct + '%').padStart(10)}  ║`);
    console.log(`╚${H}╝`);

    if (!anyLag) {
        console.log('\n✅  Consumer group is fully caught up — no lag.');
    } else {
        console.log(`\n⚠️   Total lag: ${totalLag} messages (${lagPct}% of total)`);
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────────
(async () => {
    let exitCode = 0;
    let admin;
    let kafka;

    console.log('\n[Config] AWS region    :', AWS_REGION);
    console.log('[Config] Consumer group:', KAFKA_GROUP_ID);
    console.log('[Config] Topics        :', TOPICS.join(', '), '\n');

    let brokers;
    try {
        brokers = discoverBrokers();
    } catch (err) {
        console.error('[Fatal] Broker discovery failed:', err.message);
        process.exit(1);
    }

    try {
        await openTunnels(brokers);

        kafka = new Kafka({
            clientId:          'swipenest-broker-checker',
            brokers:           LOCAL_BROKERS,
            logLevel:          logLevel.ERROR,
            connectionTimeout: 15000,
        });
        admin = kafka.admin();
        await admin.connect();

        // Build nodeId → privateIp map from our SSH-discovered node IDs
        const brokerIdToIp = {};
        for (const b of brokers) {
            if (b.nodeId !== undefined) brokerIdToIp[b.nodeId] = b.privateIp;
        }

        await printPartitionReport(admin, brokerIdToIp);
        await printConsumerLag(admin);

    } catch (err) {
        console.error('\n[Fatal]', err.stack || err.message);
        exitCode = 1;
    } finally {
        if (admin) {
            try { await admin.disconnect(); } catch (_) {}
        }
        closeTunnels();
        await sleep(300);
    }

    console.log(`\n[Result] ${exitCode === 0 ? '✅  Check completed' : '❌  Check completed with errors'}\n`);
    process.exit(exitCode);
})();
