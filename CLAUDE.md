# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo does

Ops tooling for deploying, configuring, and managing SwipeNest's Kafka cluster on AWS EC2. No application logic lives here — it is purely deploy scripts, a KafkaJS data loader, and a KafkaJS health/inspection tool.

The cluster runs **KRaft mode** (no ZooKeeper). Every broker acts as both broker and controller.

## Prerequisites

- `aws`, `ssh`, `jq`, `node`, `base64` in PATH
- AWS credentials configured (`aws sts get-caller-identity`)
- SSH key at `~/.ssh/ec2-key-pair.pem` (chmod 600)
- `npm install` to pull `kafkajs` and `dotenv`
- Copy `.env.local.example` → `.env.local` and fill in values

## Commands

```bash
# Full cluster lifecycle
npm run deploy-brokers   # interactive: provision EC2 + configure KRaft + create topics
npm run clear            # list + selectively terminate broker EC2 instances (tag Role=kafka-broker)

# Data operations
npm run load-data                        # interactive: push N analytics events to Kafka
TOTAL_RECORDS=5000 npm run load-data     # non-interactive override

# Inspection
npm run check-data       # broker info table + partition report + consumer lag (bash + JS)
npm run check-data:js    # JS-only report (SSH tunnels + KafkaJS Admin API)

# Topic verification — checks all required topics exist with correct retention + cleanup policy
# Partition count is informational only (chosen interactively at deploy time, not validated)
./scripts/verify-topics.sh
SKIP_SSH=1 ./scripts/verify-topics.sh   # run directly on a broker instance
```

Dry-run the deploy without touching AWS:
```bash
./scripts/deploy-kafka-brokers.sh --dry-run
./scripts/deploy-kafka-brokers.sh --region ap-southeast-1
```

Override consumer group for lag reporting:
```bash
KAFKA_GROUP_ID=my-group npm run check-data
```

After a broker redeployment, push the new `brokers.json` to all running app instances:
```bash
# swipenest-core (producers)
cp brokers.json ../swipenest-core/brokers.json
cd ../swipenest-core && ./scripts/refresh-brokers.sh

# swipenest-kafka-consumer (analytics consumer)
cp brokers.json ../swipenest-kafka-consumer/brokers.json
cd ../swipenest-kafka-consumer && ./scripts/refresh-brokers.sh
```

## Architecture

### Kafka port layout

| Listener   | Port  | Bound to   | Purpose |
|------------|-------|------------|---------|
| CLIENT     | 9092  | public IP  | External KafkaJS clients (`swipenest-core` producers, load/check scripts) |
| INTERNAL   | 19092 | private IP | In-VPC consumers (`swipenest-consumer`); inter-broker replication |
| CONTROLLER | 9093  | private IP | KRaft quorum voters (intra-cluster only) |

**On-broker admin tools must use port 19092** — the `kt` alias (`kafka-topics.sh --bootstrap-server localhost:19092`) is written to `/usr/local/bin/kt` on each broker by the deploy script. Using port 9092 on-broker hits a hairpin NAT issue.

### brokers.json

`brokers.json` (project root) is the single source of truth for broker IPs. Written by `deploy-kafka-brokers.sh` at the end of Phase 3.

Read by:
- `check-data.sh` — Section 1 broker info table
- `swipenest-core` — copy to repo root then run `./scripts/refresh-brokers.sh`
- `swipenest-kafka-consumer` — copy to repo root then run `./scripts/refresh-brokers.sh`

Both `refresh-brokers.sh` scripts discover running instances via AWS tags, overwrite `brokers.json` on each instance, and restart PM2 only when the broker private IPs actually changed.

Format:
```json
{ "_comment": "...", "brokers": [{ "privateIp": "10.0.1.40", "publicIp": "13.x.x.x" }] }
```

`scripts/kafka-instances.json` is a local cache of EC2 metadata (instance IDs, node IDs, instance type, storage) written during provisioning. Auto-removed by `clear-kafka-instances.sh` when all instances are terminated.

### Kafka topics

Five topics created during deployment (configured interactively per-topic partition count):

| Topic | Description |
|---|---|
| `video_view` | A user watched a video |
| `post_impression` | A post was shown in feed |
| `post_likes` | A post was liked |
| `video_watch_progress` | Watch progress milestone |
| `post_comments` | A comment was posted |

Topics are created with `retention.ms=604800000` (7 days) and `cleanup.policy=delete`. Partition count is chosen interactively per topic at deploy time — it is never hardcoded.

Run `./scripts/verify-topics.sh` after any broker deployment to confirm all topics exist and are correctly configured. The script hard-fails if a topic is missing; partition count is reported as informational only.

### Partition key format

Set by `swipenest-core/src/services/analytics.service.js` and mirrored in `scripts/load-data.js`:

| Topic | Partition key |
|---|---|
| `video_view` | `{viewer_id}:{content_id}` |
| `post_impression` | `{viewer_id}:{content_type}:{content_id}` |
| `post_likes`, `video_watch_progress`, `post_comments` | `{event_id}` (no dedup routing needed) |

## deploy-kafka-brokers.sh — three phases

**Phase 1 — Provision:**
1. Prompts for broker count (1–20), instance type, EBS storage (GB), SSH key.
2. Creates (or reuses) security group `swipenest-kafka-sg` with ports 22/9092/9093/19092.
3. Launches N EC2 instances from AMI `ami-0018b1f38bf74ad62` (Ubuntu base).
4. Waits for `instance-running` + status checks OK.
5. Saves metadata to `scripts/kafka-instances.json`.

**Phase 1.5 — Install broker application from GitHub:**
- SSHes each broker: installs `git`, clones `https://github.com/swipenest-tech/swipenest-kafka-broker.git` to `/home/ubuntu/project/swipenest-kafka-broker`, runs `npm install --production`.
- Retries up to 3 times per broker on failure.

**Phase 2 — Configure (SSH into each broker):**
1. Generates Kafka cluster UUID via `kafka-storage.sh random-uuid` on broker 1.
2. Prompts for replication factor (1–N).
3. SSHes each broker: writes `server.properties` (KRaft, node.id, quorum voters, listeners), wipes `/opt/kafka/data`, formats storage with the cluster UUID, writes `kt` alias, starts `kafka-server-start.sh`.
4. Polls until all brokers listen on port 9092.
5. Prompts for per-topic partition count (default = broker count).
6. Creates all 5 topics via `kafka-topics.sh --create --if-not-exists`.
7. Validates topics exist via `--describe`.
8. Runs preferred leader election (`kafka-leader-election.sh --all-topic-partitions`).

**Phase 3 — Output:**
- Writes `brokers.json` with `privateIp` + `publicIp` for all brokers.
- Prints copy instruction for `swipenest-consumer`.

## load-data.js / load-data.sh

`load-data.sh` is an interactive wrapper: prompts for record count (default 2000), confirms, then runs `load-data.js` with `TOTAL_RECORDS` exported.

`load-data.js` is a standalone KafkaJS producer — no dependency on `swipenest-core`.

**How it works:**
1. Discovers live broker EC2 instances via AWS CLI (tag `Role=kafka-broker`).
2. SSHes each broker to verify port 9092 is open and reads `node.id` from `server.properties`.
3. Connects directly to **public IPs on port 9092** (CLIENT listener — no SSH tunnels needed for produce).
4. Distributes records evenly across 5 topics (last topic absorbs remainder).
5. Produces in batches of 100 with `acks: -1` (all in-sync replicas) and idempotent delivery.
6. Prints a per-broker partition distribution table after ingestion using the Admin API.

**Message schema** matches `analytics.service.js → kafka-producer.js` in `swipenest-core`:

```js
{
  event_id, event, viewer_id,    // viewer_id = HMAC-SHA256 of userId
  creator_id, content_id,
  content_type,                  // 'video' for video_view/video_watch_progress
                                 // rand(['video','image']) for post_impression
                                 // rand(['video','image','reel']) for post_likes/post_comments
  surface, platform, network_type,
  analyticsSessionId, server_timestamp, schema_version,
  // video_watch_progress only:
  watched_ms, watched_per, autoplay,
  // post_comments only:
  action: 'comment', comment_id,
  // post_likes only:
  action: 'like',
}
```

The HMAC secret for `viewer_id` hashing is `swipenest-uid-hmac-k9x2` — must match `crypto-mgt.js` in `swipenest-core`.

## check-data.sh / check-data.js

`check-data.sh` is a two-stage script:

**Section 1 (bash):** Reads `brokers.json` + `scripts/kafka-instances.json`, SSH-checks each broker's reachability, prints broker info table (instance ID, node ID, IPs, type, storage, region), then SSHes each broker to dump key lines from `server.properties` (node.id, roles, listeners, log dirs, replication factor).

**Sections 2 + 3 (delegated to `check-data.js`):** Opens SSH tunnels then runs KafkaJS Admin API reports.

`check-data.js` — SSH tunnel approach (because KafkaJS metadata returns **private IPs** which are unreachable from outside the VPC):
1. Discovers brokers via AWS CLI (tag `Role=kafka-broker`).
2. SSHes each to read `node.id`.
3. Adds loopback aliases: `sudo ip addr add <privateIp>/32 dev lo`.
4. Opens SSH port-forwards: `<privateIp>:9092 → broker:9092` via public IP.
5. Connects KafkaJS Admin to the private IPs (now reachable via loopback + tunnel).
6. Prints:
   - **Partition distribution table** — topic / partition / leader IP / replicas / ISR / message count + per-topic and grand total.
   - **Per-broker leader summary** — partitions led, topics, % of total messages.
   - **Consumer group lag report** — per topic/partition: committed offset, end offset, lag (group ID from `KAFKA_GROUP_ID` env var, default `swipenest-consumer-group`).
7. Closes tunnels and removes loopback aliases on exit.

`SKIP_LOOPBACK=1` disables loopback alias setup — for single-broker use only (metadata redirects to other brokers will fail with multiple brokers).

## clear-kafka-instances.sh

Lists all EC2 instances tagged `Role=kafka-broker` (pending/running/stopping/stopped states). Prints a numbered table, prompts for comma-separated numbers or `all`, asks yes/no confirmation, then terminates selected instances. Removes `scripts/kafka-instances.json` if all instances are gone.

```bash
npm run clear                    # interactive
./scripts/clear-kafka-instances.sh --region ap-south-1
```

## Configuration (.env.local)

| Variable | Default | Purpose |
|---|---|---|
| `AWS_REGION` | `ap-south-1` | AWS region for EC2 and CLI commands |
| `KAFKA_PORT` | `19092` | INTERNAL listener port (used by VPC consumers) |
| `KAFKA_GROUP_ID` | `swipenest-analytics-consumer` | Consumer group ID for lag reporting in `check-data.js` |

## AWS infrastructure defaults (ap-south-1)

- **Broker AMI:** `ami-0018b1f38bf74ad62` — Ubuntu base; broker app cloned from GitHub at deploy time
- **Subnet:** `subnet-0da50cf2f3ebd9280`
- **Security group name:** `swipenest-kafka-sg` (created on first deploy if absent)
- **Key pair:** `ec2-key-pair` (`~/.ssh/ec2-key-pair.pem`)

To deploy in a different VPC or region, update `DEFAULT_SUBNET_ID` and `AWS_REGION` at the top of `scripts/deploy-kafka-brokers.sh` or pass `--region <region>`.
