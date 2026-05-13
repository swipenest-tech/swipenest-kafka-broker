# SwipeNest Kafka Broker — Architecture Diagram

## 1. Cluster Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                   SwipeNest Analytics Infrastructure (ap-south-1)            │
│                                                                               │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                    Kafka Cluster (KRaft Mode, EC2)                    │   │
│  │                                                                        │   │
│  │  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐                 │   │
│  │  │  Broker 1   │   │  Broker 2   │   │  Broker 3   │                 │   │
│  │  │  node.id=1  │   │  node.id=2  │   │  node.id=3  │                 │   │
│  │  │             │   │             │   │             │                 │   │
│  │  │ CLIENT:9092 │   │ CLIENT:9092 │   │ CLIENT:9092 │   (public IP)   │   │
│  │  │ INTERNAL:   │   │ INTERNAL:   │   │ INTERNAL:   │   (private IP)  │   │
│  │  │ 19092       │   │ 19092       │   │ 19092       │                 │   │
│  │  │ CONTROLLER: │   │ CONTROLLER: │   │ CONTROLLER: │   (private IP)  │   │
│  │  │ 9093        │   │ 9093        │   │ 9093        │                 │   │
│  │  └──────┬──────┘   └──────┬──────┘   └──────┬──────┘                 │   │
│  │         │                 │                  │                        │   │
│  │         └─────────────────┼──────────────────┘                        │   │
│  │              KRaft quorum replication (port 9093, private IPs)         │   │
│  │              Inter-broker replication (port 19092, private IPs)        │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                               │
│         ▲ port 9092 (public)                 ▲ port 19092 (private VPC)     │
│         │                                    │                               │
│  ┌──────┴──────────────────┐   ┌─────────────┴────────────────────────┐    │
│  │  swipenest-core (EC2)    │   │  swipenest-kafka-consumer (EC2)       │    │
│  │  KafkaJS producer        │   │  KafkaJS consumer                    │    │
│  │  KAFKA_BROKER from       │   │  brokers from brokers.json           │    │
│  │  brokers.json (private   │   │  (uses INTERNAL listener 19092)      │    │
│  │  IPs:19092) or env var   │   │  Consumer group:                     │    │
│  └─────────────────────────┘   │  swipenest-analytics-consumer        │    │
│                                  └──────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Topic Partition Architecture

```
Kafka Cluster (3 brokers, example: 6 partitions per topic)

topic: video_view
  Partition 0 → Leader: Broker 1, Replicas: [1, 2], ISR: [1, 2]
  Partition 1 → Leader: Broker 2, Replicas: [2, 3], ISR: [2, 3]
  Partition 2 → Leader: Broker 3, Replicas: [3, 1], ISR: [3, 1]
  Partition 3 → Leader: Broker 1, Replicas: [1, 3], ISR: [1, 3]
  Partition 4 → Leader: Broker 2, Replicas: [2, 1], ISR: [2, 1]
  Partition 5 → Leader: Broker 3, Replicas: [3, 2], ISR: [3, 2]

Partition key routing (consistent hashing by KafkaJS):
  video_view:       hash("{viewer_id}:{content_id}") % 6   → partition N
  post_impression:  hash("{viewer_id}:{content_type}:{content_id}") % 6
  others:           hash("{event_id}") % 6

Effect: Same viewer watching same video always goes to same partition
  → Consumer dedup key will be seen on same consumer instance
  → Reduces cross-instance dedup inconsistency
```

---

## 3. Deployment Flow Diagram

```
Developer Workstation
        │
        │ npm run deploy-brokers
        │
        ▼
deploy-kafka-brokers.sh
        │
        ├── Phase 1: AWS CLI
        │   │
        │   ├── aws ec2 create-security-group (swipenest-kafka-sg)
        │   ├── aws ec2 authorize-security-group-ingress (22, 9092, 9093, 19092)
        │   ├── aws ec2 run-instances (N instances, ami-081dfc9f291f572f7)
        │   └── aws ec2 wait instance-status-ok
        │
        ├── Phase 2: SSH Configuration
        │   │
        │   ├── broker1: kafka-storage.sh random-uuid → cluster_uuid
        │   │
        │   └── For each broker (parallel SSH):
        │       ├── Write server.properties (KRaft config)
        │       ├── rm -rf /opt/kafka/data/*
        │       ├── kafka-storage.sh format -t {uuid}
        │       ├── Write /usr/local/bin/kt alias
        │       └── kafka-server-start.sh -daemon
        │
        └── Phase 3: Topics + Output
            ├── Poll brokers port 9092 (wait for ready)
            ├── kafka-topics.sh --create (5 topics)
            ├── kafka-leader-election.sh --all-topic-partitions
            └── Write brokers.json
                   │
                   └── Copy instruction → consumer + core read this file
```

---

## 4. Network Connectivity Matrix

```
                    CLIENT:9092      INTERNAL:19092    CONTROLLER:9093
                    (public IP)      (private IP)      (private IP)
                  ─────────────    ──────────────    ────────────────
swipenest-core     ✓ Producer       ✓ Producer         ✗
                   (if VPC: 19092)  (preferred)
                   
swipenest-consumer ✗                ✓ Consumer         ✗
                                    (always VPC)

Broker ↔ Broker    ✗                ✓ Replication      ✓ KRaft quorum

Developer scripts   ✓ check-data     via SSH tunnel     ✗
                    load-data        (loopback alias)

SSH access          22/tcp           ✗                  ✗
```

---

## 5. Repository Structure

```
swipenest-kafka-broker/
├── brokers.json                 ← IP registry (auto-generated by deploy script)
├── CLAUDE.md                    ← Operational guide for Claude Code
├── .env.local.example           ← Template for AWS_REGION, KAFKA_PORT, KAFKA_GROUP_ID
├── package.json                 ← Scripts: deploy-brokers, clear, load-data, check-data
├── scripts/
│   ├── deploy-kafka-brokers.sh  ← 3-phase EC2 provisioning + KRaft configuration
│   ├── clear-kafka-instances.sh ← Interactive EC2 termination
│   ├── load-data.sh             ← Interactive wrapper for load-data.js
│   ├── load-data.js             ← KafkaJS test data producer
│   ├── check-data.sh            ← Section 1: broker info table (bash)
│   ├── check-data.js            ← Section 2+3: partition + lag reports (KafkaJS + SSH tunnels)
│   └── kafka-instances.json     ← Local cache of deployed EC2 metadata (auto-generated)
└── README.md
```

---

## 6. Kafka Message Schema

All messages produced by `swipenest-core/src/services/analytics.service.js` and mirrored in `scripts/load-data.js`:

```json
{
  "event_id":          "unique-uuid-per-event",
  "event":             "video_view | post_impression | video_watch_progress | post_likes | post_comments",
  "viewer_id":         "hmac-sha256(userId, 'swipenest-uid-hmac-k9x2')",
  "creator_id":        "content-creator-uuid",
  "content_id":        "content-uuid",
  "content_type":      "video | image | reel",
  "surface":           "home | explore | profile",
  "platform":          "android | ios",
  "network_type":      "4g | wifi | 5g",
  "analyticsSessionId": "session-uuid",
  "server_timestamp":  "2026-05-13T10:00:00.000Z",
  "schema_version":    1,

  // video_watch_progress only:
  "watched_ms":        15000,
  "watched_per":       25,
  "autoplay":          true,

  // post_comments only:
  "action":            "comment",
  "comment_id":        "comment-uuid",

  // post_likes only:
  "action":            "like"
}
```

**Security note:** `viewer_id` is HMAC-SHA256 of `userId` — never the raw userId. The HMAC key `swipenest-uid-hmac-k9x2` must match between `swipenest-core/src/utils/crypto-mgt.js` (`hashUserId`) and `scripts/load-data.js`. This ensures dedup works correctly across producer + consumer without exposing user identity in the analytics stream.

---

## 7. EC2 Instance Tags & AWS Resources

| Resource | Value |
|---------|-------|
| EC2 AMI | `ami-081dfc9f291f572f7` (Ubuntu, Kafka at `/opt/kafka`) |
| Subnet | `subnet-0da50cf2f3ebd9280` |
| Security Group | `swipenest-kafka-sg` (created on first deploy) |
| Key Pair | `ec2-key-pair` (key: `~/.ssh/ec2-key-pair.pem`) |
| EC2 Tag | `Role=kafka-broker` (used by AWS CLI queries) |
| Kafka Install Path | `/opt/kafka` |
| Data Directory | `/opt/kafka/data` |
| Config File | `/opt/kafka/config/server.properties` |
| On-broker alias | `/usr/local/bin/kt` → `kafka-topics.sh --bootstrap-server localhost:19092` |
