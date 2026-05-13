# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo does

Tooling for deploying, configuring, and managing SwipeNest's Kafka cluster on AWS EC2. No application logic lives here — it's purely ops scripts plus a KafkaJS data loader/inspector.

The cluster runs **KRaft mode** (no ZooKeeper). All brokers act as both broker and controller.

## Prerequisites

- `aws`, `ssh`, `jq`, `node`, `base64` must be in PATH
- AWS credentials configured (`aws sts get-caller-identity`)
- SSH key at `~/.ssh/ec2-key-pair.pem` (chmod 600)
- `npm install` to pull in `kafkajs` and `dotenv`
- Copy `.env.local.example` → `.env.local` and fill in values

## Commands

```bash
# Full cluster lifecycle
npm run deploy-brokers   # interactive: provision EC2 + configure KRaft + create topics
npm run clear            # list + selectively terminate EC2 instances

# Data operations
npm run load-data        # interactive: push N analytics events to Kafka
TOTAL_RECORDS=5000 npm run load-data   # non-interactive override

# Inspection
npm run check-data       # broker health table + partition report + consumer lag (bash + JS)
npm run check-data:js    # JS-only report (SSH tunnels + KafkaJS Admin API)
```

Dry-run the deploy without touching AWS:
```bash
./scripts/deploy-kafka-brokers.sh --dry-run
```

Override consumer group for lag reporting:
```bash
KAFKA_GROUP_ID=my-group npm run check-data
```

## Architecture

### Kafka port layout

| Listener   | Port  | Bound to   | Purpose |
|------------|-------|------------|---------|
| CLIENT     | 9092  | public IP  | KafkaJS producers from app servers; external load/check scripts |
| INTERNAL   | 19092 | private IP | In-VPC consumers (`swipenest-consumer`); inter-broker replication |
| CONTROLLER | 9093  | private IP | KRaft quorum voters (intra-cluster only) |

**On-broker admin tools must use port 19092** (`kt` alias = `kafka-topics.sh --bootstrap-server localhost:19092`). Using 9092 on-broker hits a hairpin NAT issue.

### brokers.json

The root-level `brokers.json` is the single source of truth for broker IPs. It's written by `deploy-kafka-brokers.sh` and read by:
- `check-data.sh` for the broker info table (section 1)
- `swipenest-core` (reads automatically, no copy needed)
- Must be **manually copied** to `swipenest-consumer` after each deployment: `cp brokers.json ../swipenest-kafka-consumer/brokers.json`

`scripts/kafka-instances.json` is a local cache of EC2 metadata (instance IDs, node IDs, instance type) written during provisioning. It is removed automatically when all instances are terminated.

### Kafka topics

Five topics created during deployment:
- `video_view`
- `post_impression`
- `post_likes`
- `video_watch_progress`
- `post_comments`

**Partition key:** No explicit Kafka message key is set in `load-data.js` — messages are pushed as `{ value }` only, so Kafka distributes them **round-robin** across partitions. The number of partitions per topic is chosen interactively during `deploy-brokers` (default = broker count).

### check-data.sh — two-stage design

`check-data.sh` handles Section 1 (broker info table) itself, reading IPs from `brokers.json` and metadata from `scripts/kafka-instances.json`. It then delegates Sections 2 & 3 (partition data + consumer lag) to `check-data.js` at the end. Both require `sudo` for loopback alias setup unless `SKIP_LOOPBACK=1` is set.

### check-data.js — SSH tunnel approach

Because KafkaJS metadata responses return **private IPs**, `check-data.js` cannot connect directly from outside the VPC. It works around this by:
1. Adding loopback aliases (`sudo ip addr add <privateIp>/32 dev lo`)
2. Opening SSH port-forward tunnels: `<privateIp>:9092 → broker:9092`
3. Connecting KafkaJS to the private IPs (which now resolve via loopback)

`SKIP_LOOPBACK=1` disables this for single-broker use only. Broker discovery uses AWS CLI (tag `Role=kafka-broker`), not `brokers.json`.

### load-data.js — direct public IP connection

The loader connects directly to public IPs on port 9092 (CLIENT listener, no tunnels). It discovers broker IPs via AWS CLI (`tag:Role=kafka-broker`) rather than reading `brokers.json`, then verifies each broker via SSH before producing.

### Event message format

Messages match the schema produced by `analytics.service.js → kafka-producer.js` in `swipenest-core`. The HMAC secret for `viewer_id` hashing (`swipenest-uid-hmac-k9x2`) must match `crypto-mgt.js` in that repo.

## AMI and infrastructure defaults

`ami-081dfc9f291f572f7` — Ubuntu with Kafka pre-installed at `/opt/kafka`. Default region: `ap-south-1`.

The deploy script hardcodes `subnet-0da50cf2f3ebd9280` and security group name `swipenest-kafka-sg`. To deploy in a different VPC or region, update `DEFAULT_SUBNET_ID` and `AWS_REGION` at the top of `scripts/deploy-kafka-brokers.sh` (or pass `--region <region>`).
