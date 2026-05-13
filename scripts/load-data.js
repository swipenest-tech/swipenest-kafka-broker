/**
 * load-data.js
 *
 * Standalone Kafka data loader — no dependency on swipenest-core services.
 * Produces N analytics events directly via KafkaJS (idempotent producer).
 *
 * Called by load-data.sh which sets:
 *   TOTAL_RECORDS  — total number of events to push (default: 2000)
 *
 * Records are distributed evenly across 5 topics:
 *   video_view, post_impression, video_watch_progress, post_likes, post_comments
 *
 * Infrastructure:
 *   - Discovers live EC2 kafka-broker instances via AWS CLI (tag Role=kafka-broker)
 *   - SSHes into each broker to verify Kafka is running and read node.id
 *   - Connects directly to public IPs on port 9092 (no SSH tunnels needed)
 *   - Prints a per-broker partition distribution table after ingestion
 *
 * Run directly (optional):
 *   TOTAL_RECORDS=5000 node scripts/load-data.js
 */

'use strict';

const path = require('path');
// Load .env.local for local overrides (AWS_REGION, KAFKA_GROUP_ID, etc.)
try {
    require('dotenv').config({ path: path.join(__dirname, '..', '.env.local') });
} catch (_) { /* dotenv is optional — env vars may be set externally */ }

const { execSync }                   = require('child_process');
const { Kafka, logLevel, Partitioners, CompressionTypes } = require('kafkajs');
const crypto                          = require('crypto');

// ─── Config ───────────────────────────────────────────────────────────────────
const AWS_REGION    = process.env.AWS_REGION || 'ap-south-1';
const EC2_USER      = 'ubuntu';
const PEM_KEY       = path.join(process.env.HOME, '.ssh/ec2-key-pair.pem');
const KAFKA_PORT    = 9092;   // CLIENT listener — public IP, external access
const TOTAL_RECORDS = parseInt(process.env.TOTAL_RECORDS || '2000', 10);
const BATCH_SIZE    = 100;
const TOPICS        = ['video_view', 'post_impression', 'video_watch_progress', 'post_likes', 'post_comments'];

// HMAC-SHA256 fingerprint — same secret as crypto-mgt.js in swipenest-core
const _USER_HASH_SECRET = 'swipenest-uid-hmac-k9x2';
function hashUserId(id) {
    return crypto.createHmac('sha256', _USER_HASH_SECRET).update(String(id)).digest('hex');
}

// Distribute records evenly; last topic absorbs any remainder
const PER_TOPIC          = Math.floor(TOTAL_RECORDS / TOPICS.length);
const REMAINDER          = TOTAL_RECORDS - PER_TOPIC * TOPICS.length;
const EVENTS_PER_TOPIC   = TOPICS.map((_, i) =>
    i === TOPICS.length - 1 ? PER_TOPIC + REMAINDER : PER_TOPIC
);

// ─── Helpers ──────────────────────────────────────────────────────────────────
const sleep   = ms => new Promise(r => setTimeout(r, ms));
const rand    = arr => arr[Math.floor(Math.random() * arr.length)];
const randInt = (lo, hi) => Math.floor(Math.random() * (hi - lo + 1)) + lo;

const PLATFORMS     = ['android', 'ios', 'web'];
const NETWORK_TYPES = ['4g', '5g', 'wifi', '3g'];
const SURFACES      = ['home', 'explore', 'profile', 'search'];

// Message format matches analytics.service.js → kafka-producer.js output
function makeMessage(topic, idx, sessionCtx) {
    const base = {
        event_id:           `${topic[0]}${idx}_${process.hrtime.bigint()}`,
        event:              topic,
        viewer_id:          hashUserId(sessionCtx.mockUserId),
        creator_id:         `user_${randInt(1, 200)}`,
        content_id:         `vid_${randInt(1, 500)}`,
        content_type:       'video',
        surface:            rand(SURFACES),
        platform:           sessionCtx.platform,
        network_type:       sessionCtx.networkType,
        analyticsSessionId: sessionCtx.sessionId,
        server_timestamp:   new Date().toISOString(),
        schema_version:     1,
    };
    if (topic === 'video_watch_progress') {
        base.watched_ms  = randInt(0, 60000);
        base.watched_per = randInt(0, 100);
        base.autoplay    = Math.random() > 0.5;
    }
    if (topic === 'post_impression') {
        base.content_type = rand(['video', 'image']);
    }
    if (topic === 'post_likes') {
        base.content_type = rand(['video', 'image', 'reel']);
        base.action       = 'like';
    }
    if (topic === 'post_comments') {
        base.content_type = rand(['video', 'image', 'reel']);
        base.action       = 'comment';
        base.comment_id   = `cmt_${randInt(1, 10000)}`;
    }
    return base;
}

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

// ─── Verify Kafka + read node.id via SSH ──────────────────────────────────────
function checkBroker(publicIp) {
    const SSH_OPTS = `-i ${PEM_KEY} -o StrictHostKeyChecking=no -o ConnectTimeout=10 -o BatchMode=yes -o LogLevel=ERROR`;
    // Check port 9092 is open
    const portCheck = execSync(
        `ssh ${SSH_OPTS} ${EC2_USER}@${publicIp} ` +
        `"ss -tlnp 2>/dev/null | grep -q ':9092' && echo RUNNING=yes || echo RUNNING=no"`,
        { encoding: 'utf8', timeout: 20000 }
    ).trim();
    const running = /RUNNING=yes/.test(portCheck);

    // Read node.id from server.properties
    let nodeId;
    try {
        const out = execSync(
            `ssh ${SSH_OPTS} ${EC2_USER}@${publicIp} ` +
            `"grep -m1 '^node.id=' /opt/kafka/config/kraft/server.properties 2>/dev/null || echo node.id=unknown"`,
            { encoding: 'utf8', timeout: 15000 }
        ).trim();
        const m = out.match(/node\.id=(\d+)/);
        nodeId = m ? parseInt(m[1], 10) : undefined;
    } catch (_) {
        nodeId = undefined;
    }
    return { running, nodeId };
}

async function setupBrokers(brokers) {
    console.log('\n[Connect] Verifying Kafka is running on each broker...');
    const notRunning = [];
    for (const b of brokers) {
        let result;
        try {
            result = checkBroker(b.publicIp);
        } catch (err) {
            throw new Error(`SSH check failed for ${b.publicIp}: ${err.message}`);
        }
        b.nodeId = result.nodeId;
        if (!result.running) {
            console.log(`  ${b.instanceId}  public=${b.publicIp}  kafka=NOT RUNNING ✗`);
            notRunning.push(b.instanceId);
        } else {
            console.log(`  ${b.instanceId}  public=${b.publicIp}  kafka=running ✓  nodeId=${b.nodeId ?? 'n/a'}`);
        }
    }
    if (notRunning.length > 0) {
        throw new Error(
            `Kafka is not running on ${notRunning.length} broker(s): ${notRunning.join(', ')}\n` +
            `  → Run './scripts/configure-cluster.sh' first.`
        );
    }
    // Connect directly to public IPs — configure-cluster.sh advertises public IP on CLIENT listener
    process.env.KAFKA_BROKER = brokers.map(b => `${b.publicIp}:${KAFKA_PORT}`).join(',');
    console.log(`[Connect] KAFKA_BROKER=${process.env.KAFKA_BROKER}\n`);
}

// ─── KafkaJS producer singleton ───────────────────────────────────────────────
let _kafka, _producer, _producerConnected = false;

async function getProducer() {
    if (_producerConnected) return _producer;
    _kafka = new Kafka({
        clientId:          'swipenest-broker-loader',
        brokers:           process.env.KAFKA_BROKER.split(',').map(b => b.trim()),
        logLevel:          logLevel.ERROR,
        connectionTimeout: 15000,
        retry: {
            initialRetryTime: 300,
            retries:          8,
        },
    });
    _producer = _kafka.producer({
        idempotent:                   true,
        maxInFlightRequests:          5,
        transactionalId:              undefined,
        allowAutoTopicCreation:       false,
        createPartitioner:            Partitioners.DefaultPartitioner,
    });
    await _producer.connect();
    _producerConnected = true;
    return _producer;
}

async function disconnectProducer() {
    if (_producerConnected && _producer) {
        try { await _producer.disconnect(); } catch (_) {}
        _producerConnected = false;
    }
}

// ─── Produce events ───────────────────────────────────────────────────────────
async function produceEvents() {
    const producer = await getProducer();

    const sessionCtx = {
        mockUserId:  '1001',
        sessionId:   `load_${Date.now()}`,
        platform:    rand(PLATFORMS),
        networkType: rand(NETWORK_TYPES),
    };

    console.log(`[Ingest] Total records : ${TOTAL_RECORDS}`);
    console.log(`[Ingest] Distribution  : ${TOPICS.map((t, i) => `${t}=${EVENTS_PER_TOPIC[i]}`).join(', ')}`);
    console.log(`[Ingest] Broker(s)     : ${process.env.KAFKA_BROKER}\n`);

    const breakdown    = Object.fromEntries(TOPICS.map(t => [t, 0]));
    let totalProduced  = 0;
    let errors         = 0;
    const t0           = Date.now();

    // Build per-topic cursors
    const cursors = Object.fromEntries(
        TOPICS.map((t, i) => [t, { sent: 0, target: EVENTS_PER_TOPIC[i] }])
    );

    let batchNum = 0;
    let allDone  = false;
    while (!allDone) {
        allDone = true;
        const topicMessages = [];

        for (const topic of TOPICS) {
            const c = cursors[topic];
            if (c.sent >= c.target) continue;
            allDone = false;
            const take = Math.min(BATCH_SIZE, c.target - c.sent);
            const messages = [];
            for (let i = 0; i < take; i++) {
                const msg = makeMessage(topic, c.sent + i, sessionCtx);
                let key;
                if (topic === 'video_view') {
                    key = `${msg.viewer_id}:${msg.content_id}`;
                } else if (topic === 'post_impression') {
                    key = `${msg.viewer_id}:${msg.content_type}:${msg.content_id}`;
                }
                messages.push({ key, value: JSON.stringify(msg) });
            }
            topicMessages.push({ topic, messages });
            c.sent += take;
        }
        if (topicMessages.length === 0) break;
        batchNum++;

        try {
            await producer.sendBatch({
                topicMessages,
                compression: CompressionTypes.None,
                acks:        -1,   // wait for all in-sync replicas
            });
            for (const { topic, messages } of topicMessages) {
                breakdown[topic] = (breakdown[topic] || 0) + messages.length;
                totalProduced    += messages.length;
            }
            process.stdout.write(
                `\r[Ingest] ${totalProduced}/${TOTAL_RECORDS}  ` +
                TOPICS.map(t => `${t.replace(/_./g, m => m[1].toUpperCase())}:${breakdown[t]}`).join('  ') + '   '
            );
        } catch (err) {
            errors++;
            if (errors <= 5) console.error(`\n[Ingest] Batch ${batchNum} error:`, err.message);
            if (errors > 20) throw new Error(`Too many errors (${errors}) — aborting`);
        }
    }

    const elapsed = ((Date.now() - t0) / 1000).toFixed(2);
    console.log(`\n\n[Ingest] Done — ${totalProduced}/${TOTAL_RECORDS} produced in ${elapsed}s  errors: ${errors}`);
    return { totalProduced, errors, breakdown };
}

// ─── Per-broker distribution table ────────────────────────────────────────────
async function printBrokerStats(brokers) {
    console.log('\n[Stats] Fetching broker distribution from Kafka Admin API...');
    const kafka = new Kafka({
        clientId:          'swipenest-broker-loader-admin',
        brokers:           process.env.KAFKA_BROKER.split(',').map(b => b.trim()),
        logLevel:          logLevel.ERROR,
        connectionTimeout: 10000,
    });
    const admin = kafka.admin();
    await admin.connect();

    const meta       = await admin.fetchTopicMetadata({ topics: TOPICS });
    const offsetData = {};
    for (const topic of TOPICS) {
        const offs = await admin.fetchTopicOffsets(topic);
        offsetData[topic] = Object.fromEntries(
            offs.map(p => [p.partition, Math.max(0, parseInt(p.high, 10) - parseInt(p.low, 10))])
        );
    }
    await admin.disconnect();

    // Build nodeId → broker map
    const nodeMap = {};
    for (const b of brokers) {
        if (b.nodeId !== undefined) nodeMap[b.nodeId] = b;
    }
    for (const b of (meta.brokers || [])) {
        if (!nodeMap[b.nodeId]) nodeMap[b.nodeId] = { nodeId: b.nodeId, privateIp: b.host, instanceId: 'unknown' };
    }

    // Collect partition leadership rows per broker
    const rows = {};
    for (const topicMeta of meta.topics) {
        const topic = topicMeta.name;
        for (const p of topicMeta.partitions) {
            if (!rows[p.leader]) rows[p.leader] = [];
            rows[p.leader].push({
                topic,
                partition: p.partitionId,
                msgs:      offsetData[topic]?.[p.partitionId] ?? 0,
            });
        }
    }

    const brokerTotals = {};
    let grandTotal = 0;
    const W = 108;

    console.log(`\n╔${'═'.repeat(W)}╗`);
    console.log(`║  ${'KAFKA BROKER DISTRIBUTION REPORT'.padEnd(W - 2)}║`);
    console.log(`╠${'═'.repeat(20)}╦${'═'.repeat(14)}╦${'═'.repeat(10)}╦${'═'.repeat(26)}╦${'═'.repeat(11)}╦${'═'.repeat(14)}╦${'═'.repeat(9)}╣`);
    console.log(
        `║  ${'Instance ID'.padEnd(18)}║  ${'Private IP'.padEnd(12)}║  ${'Node ID'.padEnd(8)}║  ${'Topic'.padEnd(24)}║  ${'Partition'.padEnd(9)}║  ${'Role'.padEnd(12)}║  ${'Messages'.padStart(7)}  ║`
    );
    console.log(`╠${'═'.repeat(20)}╬${'═'.repeat(14)}╬${'═'.repeat(10)}╬${'═'.repeat(26)}╬${'═'.repeat(11)}╬${'═'.repeat(14)}╬${'═'.repeat(9)}╣`);

    for (const [nodeIdStr, partRows] of Object.entries(rows).sort((a, b) => a[0] - b[0])) {
        const nodeId = parseInt(nodeIdStr, 10);
        const b      = nodeMap[nodeId] || {};
        const brokerTotal = partRows.reduce((s, r) => s + r.msgs, 0);
        brokerTotals[nodeId] = brokerTotal;
        grandTotal += brokerTotal;

        partRows.sort((a, b) => a.topic.localeCompare(b.topic) || a.partition - b.partition);
        partRows.forEach((r, i) => {
            const inst = i === 0 ? (b.instanceId || 'unknown').padEnd(18) : ''.padEnd(18);
            const pip  = i === 0 ? (b.privateIp  || 'unknown').padEnd(12) : ''.padEnd(12);
            const nid  = i === 0 ? String(nodeId).padEnd(8)                : ''.padEnd(8);
            console.log(
                `║  ${inst}║  ${pip}║  ${nid}║  ${r.topic.padEnd(24)}║  ${String(r.partition).padEnd(9)}║  ${'leader'.padEnd(12)}║  ${String(r.msgs).padStart(7)}  ║`
            );
        });
        console.log(`╠${'═'.repeat(20)}╬${'═'.repeat(14)}╬${'═'.repeat(10)}╬${'═'.repeat(26)}╬${'═'.repeat(11)}╬${'═'.repeat(14)}╬${'═'.repeat(9)}╣`);
    }

    // Broker summary
    console.log(`║  ${'BROKER SUMMARY'.padEnd(W - 2)}║`);
    console.log(`╠${'═'.repeat(20)}╬${'═'.repeat(14)}╬${'═'.repeat(10)}╦${'═'.repeat(26)}╦${'═'.repeat(11)}╦${'═'.repeat(14)}╦${'═'.repeat(9)}╣`);
    console.log(
        `║  ${'Instance ID'.padEnd(18)}║  ${'Private IP'.padEnd(12)}║  ${'Node ID'.padEnd(8)}║  ${'Partitions Led'.padEnd(24)}║  ${'Topics'.padEnd(9)}║  ${'% of Total'.padEnd(12)}║  ${'Messages'.padStart(7)}  ║`
    );
    console.log(`╠${'═'.repeat(20)}╬${'═'.repeat(14)}╬${'═'.repeat(10)}╬${'═'.repeat(26)}╬${'═'.repeat(11)}╬${'═'.repeat(14)}╬${'═'.repeat(9)}╣`);

    for (const [nodeIdStr] of Object.entries(rows).sort((a, b) => a[0] - b[0])) {
        const nodeId   = parseInt(nodeIdStr, 10);
        const b        = nodeMap[nodeId] || {};
        const msgs     = brokerTotals[nodeId] || 0;
        const pct      = grandTotal > 0 ? ((msgs / grandTotal) * 100).toFixed(1) : '0.0';
        const numParts = (rows[nodeId] || []).length;
        const numTopics = new Set((rows[nodeId] || []).map(r => r.topic)).size;
        console.log(
            `║  ${(b.instanceId || 'unknown').padEnd(18)}║  ${(b.privateIp || 'unknown').padEnd(12)}║  ${String(nodeId).padEnd(8)}║  ${String(numParts).padEnd(24)}║  ${String(numTopics).padEnd(9)}║  ${(pct + '%').padEnd(12)}║  ${String(msgs).padStart(7)}  ║`
        );
    }
    console.log(`╠${'═'.repeat(20)}╩${'═'.repeat(14)}╩${'═'.repeat(10)}╩${'═'.repeat(26)}╩${'═'.repeat(11)}╩${'═'.repeat(14)}╬${'═'.repeat(9)}╣`);
    console.log(`║  ${'GRAND TOTAL'.padEnd(W - 12)}║  ${String(grandTotal).padStart(7)}  ║`);
    console.log(`╚${'═'.repeat(W - 10)}╩${'═'.repeat(9)}╝`);
}

// ─── Main ─────────────────────────────────────────────────────────────────────
(async () => {
    let exitCode = 0;
    let brokers;

    console.log(`\n[Config] Total records : ${TOTAL_RECORDS}`);
    console.log(`[Config] Per topic     : ${TOPICS.map((t, i) => `${t}=${EVENTS_PER_TOPIC[i]}`).join(', ')}\n`);

    try {
        brokers = discoverBrokers();
    } catch (err) {
        console.error('[Fatal] Broker discovery failed:', err.message);
        process.exit(1);
    }

    try {
        await setupBrokers(brokers);

        const { errors } = await produceEvents();

        if (errors > TOTAL_RECORDS * 0.1) {
            console.error(`[Warn] High error rate (${errors}) — stats may be incomplete`);
            exitCode = 1;
        }

        console.log('[Stats] Waiting 2s for brokers to flush...');
        await sleep(2000);

        await printBrokerStats(brokers);

    } catch (err) {
        console.error('\n[Fatal]', err.stack || err.message);
        exitCode = 1;
    } finally {
        await disconnectProducer();
    }

    console.log(`\n[Result] ${exitCode === 0 ? '✅  Completed successfully' : '❌  Completed with errors'}\n`);
    process.exit(exitCode);
})();
