# SwipeNest Kafka Broker — Complete Technical Reference

> Generated from a full source scan of `swipenest-kafka-broker` (2,375 LOC across 7 scripts).
> Package name: `swipenest-broker` · Private package · No application entry point — this repo is invoked via npm scripts.

---

## Table of Contents

1. [Project Introduction](#1-project-introduction)
2. [Technologies Used](#2-technologies-used)
3. [Authentication Mechanism Used](#3-authentication-mechanism-used)
4. [Cryptography Used](#4-cryptography-used)
5. [Module Index](#5-module-index)
6. [Module Deep Dives](#6-module-deep-dives)
7. [Cluster Architecture](#7-cluster-architecture)
8. [Topics & Partition Keys](#8-topics--partition-keys)
9. [Data Files & Contracts](#9-data-files--contracts)
10. [Configuration & AWS Defaults](#10-configuration--aws-defaults)
11. [Cross-Repo Integration](#11-cross-repo-integration)
12. [Known Issues & Gotchas](#12-known-issues--gotchas)

---

## 1. Project Introduction

**SwipeNest Kafka Broker** is the operations tooling repository for SwipeNest's Kafka cluster on AWS EC2. It contains **no application logic and no long-running service** — it is a collection of deploy scripts, a KafkaJS data loader, and KafkaJS health/inspection tooling, all invoked manually by an operator.

The cluster runs in **KRaft mode** (no ZooKeeper). Every broker acts as both **broker and controller** simultaneously.

### What this repo is responsible for

| Responsibility | Delivered by |
|---|---|
| Provisioning broker EC2 instances | `deploy-kafka-brokers.sh` (Phase 1) |
| Installing the broker application | `deploy-kafka-brokers.sh` (Phase 1.5, clones from GitHub) |
| Configuring KRaft, listeners, storage | `deploy-kafka-brokers.sh` (Phase 2) |
| Creating the five analytics topics | `deploy-kafka-brokers.sh` (Phase 2) |
| Publishing broker IPs to consumers | `deploy-kafka-brokers.sh` (Phase 3) → `brokers.json` |
| Verifying topic configuration | `verify-topics.sh` |
| Load-testing the cluster | `load-data.sh` → `load-data.js` |
| Health, partition and lag inspection | `check-data.sh` → `check-data.js` |
| Tearing down instances | `clear-kafka-instances.sh` |

### Role in the platform

This repo sits between the producer and the consumers in the analytics pipeline:

```
swipenest-android / ios  ──►  swipenest-core  ──►  KAFKA CLUSTER  ──►  swipenest-kafka-consumer  ──►  Redis / MongoDB
   (client events)          (produces events)     (this repo)          (durable writes)
```

| Repo | Relationship |
|---|---|
| **swipenest-core** | Produces to this cluster on the CLIENT listener (public IP :9092). Receives `brokers.json`. |
| **swipenest-kafka-consumer** | Consumes from this cluster on the INTERNAL listener (private IP :19092). Receives `brokers.json`. |
| **swipenest-deployment** | Also writes `brokers.json` — the checked-in copy was last written by the deployment portal, not by this repo's deploy script. |

> **Full cross-repo flow:** see `../SWIPENEST_KAFKA_PROJECT_FLOW.md` at the repo-set root.

### Operating model

Everything here is **interactive and operator-driven**. There is no CI trigger, no scheduler, and no daemon. Scripts prompt for broker count, instance type, EBS size, SSH key, replication factor and per-topic partition counts, then confirm before acting. `--dry-run` is available on the deploy script to rehearse without touching AWS.

---

## 2. Technologies Used

### Runtime dependencies

| Technology | Version | Purpose |
|---|---|---|
| **KafkaJS** | `^2.2.4` | Producer (`load-data.js`) and Admin API (`check-data.js`) |
| **dotenv** | `^16.4.5` | Loads `.env.local` |
| **Node.js** | *(unpinned — no `engines` field)* | Runs the two `.js` tools |

> The dependency surface is deliberately tiny — two packages. Unlike `swipenest-core`, there is no AWS SDK here; all AWS interaction goes through the **AWS CLI** shelled out from bash and from Node via `execSync`.

### External tooling (must be in `PATH`)

| Tool | Used for |
|---|---|
| **`aws`** (AWS CLI v2) | EC2 provisioning, instance discovery, security groups, termination |
| **`ssh`** | Broker configuration, remote command execution, port forwarding |
| **`jq`** | JSON parsing in bash scripts |
| **`node`** | Running `load-data.js` / `check-data.js` |
| **`base64`** | Encoding the cluster UUID for safe SSH transport |
| **`bash`** | All five shell scripts |

### Kafka-side tooling (on the broker instances)

| Binary | Purpose |
|---|---|
| `kafka-storage.sh` | Generates the cluster UUID; formats KRaft storage |
| `kafka-server-start.sh` | Starts the broker process |
| `kafka-topics.sh` | Topic create / describe / validate |
| `kafka-configs.sh` | Topic config inspection (retention, cleanup policy) |
| `kafka-leader-election.sh` | Preferred-leader rebalancing |
| `kt` | Convenience alias written to `/usr/local/bin/kt` on each broker — wraps `kafka-topics.sh --bootstrap-server localhost:19092` |

### AWS services

| Service | Usage |
|---|---|
| **EC2** | Broker instances (AMI `ami-0018b1f38bf74ad62`, Ubuntu base) |
| **EBS** | Broker storage — size prompted at deploy time |
| **Security Groups** | `swipenest-kafka-sg` — ports 22, 9092, 9093, 19092 |
| **VPC / Subnet** | `subnet-0da50cf2f3ebd9280` (ap-south-1 default) |

### Language breakdown

| Language | Files | LOC |
|---|---|---|
| **Bash** | 5 scripts | 1,540 |
| **JavaScript (Node)** | 2 scripts | 835 |
| **Total** | 7 | **2,375** |

---

## 3. Authentication Mechanism Used

This repo has **no application-level authentication** — there are no users, sessions, tokens or logins. It authenticates to *infrastructure*, in three distinct ways.

### 3.1 SSH key-pair authentication (operator → brokers)

Every broker interaction is over SSH with a PEM private key.

| Setting | Value |
|---|---|
| Key path (default) | `~/.ssh/ec2-key-pair.pem` |
| AWS key-pair name | `ec2-key-pair` |
| Remote user | `ubuntu` |
| Required permissions | `chmod 600` |

**SSH options used** (`check-data.js:48`, mirrored in the bash scripts):

```
-i <PEM_KEY>
-o StrictHostKeyChecking=no    # accept unknown host keys without prompting
-o ConnectTimeout=10
-o BatchMode=yes               # never prompt for a password — fail instead
-o LogLevel=ERROR
```

> **`StrictHostKeyChecking=no` disables host-key verification**, which removes protection against man-in-the-middle interception of the SSH session. It is used so freshly-provisioned instances don't block the script on an interactive host-key prompt. The traffic carried over these sessions includes the Kafka cluster UUID and full broker configuration. On a trusted VPC path the practical exposure is low, but this is a deliberate weakening — see [Known Issues](#12-known-issues--gotchas).

The deploy script wraps SSH in two helpers with retry semantics:

| Function | File | Purpose |
|---|---|---|
| `ssh_broker()` | `scripts/deploy-kafka-brokers.sh:401` | Single SSH command to one broker |
| `ssh_broker_retry()` | `scripts/deploy-kafka-brokers.sh:411` | Same, with up to **3 attempts** for transient failures |

### 3.2 AWS IAM authentication (operator → AWS)

All AWS calls use the **ambient AWS CLI credential chain** — environment variables, `~/.aws/credentials`, or an instance profile. No credentials are stored in this repo.

Preflight verifies identity before doing anything destructive:

```bash
aws sts get-caller-identity
```

Region resolution: `AWS_REGION` env var → `.env.local` → hardcoded `ap-south-1`. The deploy and clear scripts also accept `--region <region>`.

### 3.3 Kafka client authentication (tools → cluster)

**There is none.** The KafkaJS clients in `load-data.js` and `check-data.js` connect with **no SASL, no TLS, and no ACLs**:

| Client | `clientId` | Connects to |
|---|---|---|
| Producer | `swipenest-broker-loader` | Public IPs : 9092 (CLIENT listener) |
| Producer admin | `swipenest-broker-loader-admin` | Public IPs : 9092 |
| Inspector | `swipenest-broker-checker` | Private IPs : 9092 via SSH tunnel |

Access control is therefore **entirely network-level**, enforced by the `swipenest-kafka-sg` security group. Anyone who can reach port 9092 on a broker can produce to or consume from any topic without credentials — so the security group's ingress CIDRs are the *only* access boundary.

Those CIDRs are scoped rather than open (`deploy-kafka-brokers.sh:240–271`):

| Port | Source | Purpose |
|---|---|---|
| `9092` (CLIENT) | `$VPC_CIDR` + self-referencing SG + operator's `/32` | VPC-wide client access, intra-cluster, operator scripts |
| `19092` (INTERNAL) | `$VPC_CIDR` + self-referencing SG | In-VPC consumers, inter-broker replication |
| `9093` (CONTROLLER) | Self-referencing SG only | KRaft quorum — intra-cluster, never external |
| `22` (SSH) | Operator's `/32` | Operator access only |

The operator's address is detected at deploy time:

```bash
MY_IP=$(curl -sf https://checkip.amazonaws.com || curl -sf https://ifconfig.me || echo "0.0.0.0")
SSH_CIDR="${MY_IP}/32"
[[ "$MY_IP" == "0.0.0.0" ]] && SSH_CIDR="0.0.0.0/0"
```

> **The fallback is the risk, not the default.** If both IP-detection calls fail — no outbound internet, DNS failure, or both endpoints unreachable — `SSH_CIDR` silently becomes `0.0.0.0/0`, opening **port 22 and port 9092 to the entire internet** on a cluster with no SASL, no TLS and no ACLs. The deploy prints the resulting CIDR (`success "Created security group ... operator: ${SSH_CIDR}"`), so the failure is visible in the log, but nothing halts the deploy. See [Known Issues](#12-known-issues--gotchas).

Note also that these rules are only applied **when the security group is created**. On a redeploy the script reuses the existing group unchanged (line 274–275) — so an operator whose IP has changed since the first deploy will find SSH blocked, and a stale `/32` from a previous operator persists until manually removed.

---

## 4. Cryptography Used

Cryptographic surface here is **much smaller than in `swipenest-core`** — there is no encryption of data at rest or in transit within the cluster, and no key management. Three uses exist.

### 4.1 HMAC-SHA256 — `viewer_id` fingerprinting

`scripts/load-data.js:44-48`

```js
const _USER_HASH_SECRET = 'swipenest-uid-hmac-k9x2';

function hashUserId(id) {
    return crypto.createHmac('sha256', _USER_HASH_SECRET)
                 .update(String(id))
                 .digest('hex');
}
```

Generated test events carry a `viewer_id` that is an HMAC-SHA256 hash of a synthetic user ID, exactly matching how `swipenest-core` derives the same field.

> **This secret must stay identical to `_USER_HASH_SECRET` in `swipenest-core/src/utils/crypto-mgt.js`.** If they drift, load-test events will hash to a different keyspace than production events, and consumer-side dedup and per-viewer aggregation will silently treat them as distinct users. The value is **hardcoded in both repos** rather than sourced from shared config — a duplication worth knowing about before changing either.

Uses Node's built-in `crypto` module. No third-party crypto libraries are present.

### 4.2 SSH transport encryption

All operator→broker traffic — configuration writes, remote command execution, and the port-forwards used by `check-data.js` — is encrypted by the SSH protocol using the EC2 key pair. Cipher selection is left to the system SSH client defaults; nothing is pinned or overridden.

### 4.3 Base64 encoding — cluster UUID transport

`scripts/deploy-kafka-brokers.sh:558`

```bash
CLUSTER_UUID=\$(printf '%s' "${enc_uuid}" | base64 -d)
```

The Kafka cluster UUID (generated by `kafka-storage.sh random-uuid` on broker 1) is base64-encoded before being embedded in the remote configuration heredoc, then decoded on the broker.

> **Base64 is an encoding, not encryption.** Its purpose here is purely to prevent shell metacharacters and quoting issues from corrupting the UUID as it passes through nested SSH heredocs — it provides no confidentiality. The UUID's confidentiality comes from the SSH channel carrying it.

### 4.4 What is *not* encrypted

| Surface | State |
|---|---|
| Kafka wire protocol (producer → broker) | **Plaintext** — no TLS on the CLIENT listener |
| Kafka wire protocol (broker → broker) | **Plaintext** — no TLS on the INTERNAL listener |
| KRaft controller traffic | **Plaintext** — no TLS on the CONTROLLER listener |
| Kafka data at rest (`/opt/kafka/data`) | **Unencrypted** — no EBS encryption flag set at launch |
| `brokers.json` | Plaintext IPs, committed to the repo |

Analytics event payloads contain HMAC-hashed viewer IDs rather than raw user IDs, so a network observer sees pseudonymous rather than directly identifying data — but content IDs, creator IDs, timestamps, platform and network type all travel in the clear.

---

## 5. Module Index

| Sr | Module Name | Description (Purpose) |
|:--:|---|---|
| 1 | **Cluster Deployment** | End-to-end cluster provisioning — EC2 launch, security group, broker app install from GitHub, KRaft configuration, storage format, topic creation, leader election, and `brokers.json` output. The largest module (864 LOC). |
| 2 | **Topic Verification** | Post-deployment assertion that all five required topics exist with the correct retention and cleanup policy; hard-fails on a missing topic. |
| 3 | **Data Loading** | Standalone KafkaJS idempotent producer that generates and pushes N synthetic analytics events across all five topics, then reports partition distribution. |
| 4 | **Data Loading Wrapper** | Interactive bash front-end for the loader — prompts for record count, confirms, exports `TOTAL_RECORDS`. |
| 5 | **Cluster Inspection (bash)** | Broker reachability check and broker info table; dumps key `server.properties` lines from each broker, then delegates to the JS reporter. |
| 6 | **Cluster Inspection (JS)** | SSH-tunnelled KafkaJS Admin reporting — partition distribution, per-broker leader summary, and consumer group lag. |
| 7 | **Instance Teardown** | Lists all `Role=kafka-broker` EC2 instances, prompts for a selection, confirms, and terminates; cleans up the local metadata cache. |

> **Note on structure:** this repo has no `route → service → repository` layering — there are no HTTP endpoints and no database. The deep-dive tables below therefore map the equivalent three stages of an ops workflow:
>
> | `swipenest-core` layer | Equivalent here | Meaning |
> |---|---|---|
> | Route | **Entry Point** | How the operation is invoked (npm script / CLI) |
> | Service | **Orchestration** | The function that implements the logic |
> | Repository | **Target Layer** | What it ultimately talks to — AWS CLI, SSH, or the Kafka API |

---

## 6. Module Deep Dives

> Every Entry Point / Orchestration / Target Layer cell is prefixed with the **relative file path** (from the project root) where that logic lives, followed by the function or command.

---

### Module 1 — Cluster Deployment

**File:** `scripts/deploy-kafka-brokers.sh` (864 LOC)
**Invocation:** `npm run deploy-brokers` → `bash scripts/deploy-kafka-brokers.sh`
**Flags:** `--dry-run` (rehearse without touching AWS) · `--region <region>`

| Sr | Operation | Entry Point (file + invocation) | Orchestration (file + function) | Target Layer (file + operation) |
|:--:|---|---|---|---|
| 1 | Preflight checks | `package.json`<br>`npm run deploy-brokers`<br>`scripts/deploy-kafka-brokers.sh:119`<br>`banner "Preflight Checks"` | `scripts/deploy-kafka-brokers.sh`<br>`info()`, `success()`, `warn()`, `error()`, `die()` (lines 88–92) | **AWS CLI**<br>`aws sts get-caller-identity`<br>verifies `aws`, `ssh`, `jq`, `node`, `base64` in `PATH` |
| 2 | Prompt for cluster shape | `scripts/deploy-kafka-brokers.sh:134`<br>`banner "PHASE 1 — Provision EC2 Instances"` | `scripts/deploy-kafka-brokers.sh:198–205`<br>reads broker count (1–20), instance type, EBS GB, `KEY_NAME`, `PEM_KEY` | **stdin** — interactive prompts with defaults `ec2-key-pair` / `~/.ssh/ec2-key-pair.pem` |
| 3 | Create / reuse security group | `scripts/deploy-kafka-brokers.sh:215`<br>`banner "Security Group"` | `scripts/deploy-kafka-brokers.sh:221–235`<br>looks up `KAFKA_SG_NAME`, creates if absent | **AWS CLI**<br>`aws ec2 describe-security-groups --filters Name=group-name,Values=swipenest-kafka-sg`<br>`aws ec2 create-security-group`<br>opens ports 22 / 9092 / 9093 / 19092 |
| 4 | Launch EC2 instances | `scripts/deploy-kafka-brokers.sh:294`<br>`banner "Launching EC2 Instances"` | `scripts/deploy-kafka-brokers.sh:316`<br>launches N instances into `DEFAULT_SUBNET_ID` | **AWS CLI**<br>`aws ec2 run-instances --subnet-id subnet-0da50cf2f3ebd9280`<br>AMI `ami-0018b1f38bf74ad62`, tag `Role=kafka-broker` |
| 5 | Wait for readiness | `scripts/deploy-kafka-brokers.sh:334, 342`<br>`banner "Waiting for Instances to Start"` / `"Waiting for Status Checks (2–3 min)"` | `scripts/deploy-kafka-brokers.sh:334–347`<br>blocking waiters, then fetch instance details | **AWS CLI**<br>`aws ec2 wait instance-running`<br>`aws ec2 wait instance-status-ok`<br>`aws ec2 describe-instances` |
| 6 | Persist instance metadata | `scripts/deploy-kafka-brokers.sh:347`<br>`banner "Fetching Instance Details"` | `scripts/deploy-kafka-brokers.sh:80–82`<br>populates `INST_IDS[]`, `PRIV_IPS[]`, `PUB_IPS[]` | **Local file**<br>`scripts/kafka-instances.json` (`INSTANCES_FILE`) — instance IDs, node IDs, type, storage |
| 7 | Install broker app (Phase 1.5) | `scripts/deploy-kafka-brokers.sh:450`<br>`banner "Installing Broker Application from GitHub"` | `scripts/deploy-kafka-brokers.sh:423`<br>`install_broker()`<br>via `ssh_broker_retry()` (line 411, 3 attempts) | **SSH → broker**<br>`apt install git`<br>`git clone https://github.com/swipenest-tech/swipenest-kafka-broker.git`<br>→ `/home/ubuntu/project/swipenest-kafka-broker`<br>`npm install --production` |
| 8 | Generate cluster UUID | `scripts/deploy-kafka-brokers.sh:466`<br>`banner "Generating Kafka Cluster ID"` | `scripts/deploy-kafka-brokers.sh:558`<br>base64-encodes for transport, decodes on broker | **SSH → broker 1**<br>`/opt/kafka/bin/kafka-storage.sh random-uuid` |
| 9 | Prompt replication factor | `scripts/deploy-kafka-brokers.sh:488`<br>`banner "Replication Configuration"` | `scripts/deploy-kafka-brokers.sh:499`<br>`MIN_ISR=$(( REPLICATION_FACTOR > 1 ? 2 : 1 ))` | **stdin** — accepts 1–N (N = broker count) |
| 10 | Configure every broker | `scripts/deploy-kafka-brokers.sh:588`<br>`banner "Configuring All Brokers"` | `scripts/deploy-kafka-brokers.sh:504`<br>`configure_broker()`<br>builds `QUORUM_VOTERS` (line 480) | **SSH → each broker**<br>writes `/opt/kafka/config/server.properties`<br>wipes `/opt/kafka/data`<br>`kafka-storage.sh format --cluster-id <UUID>`<br>writes `kt` alias to `/usr/local/bin/kt`<br>`kafka-server-start.sh` |
| 11 | Verify SG ports | `scripts/deploy-kafka-brokers.sh:604`<br>`banner "Security Group Port Verification"` | `scripts/deploy-kafka-brokers.sh:604–644` | **AWS CLI** — confirms 22 / 9092 / 9093 / 19092 are open |
| 12 | Poll cluster readiness | `scripts/deploy-kafka-brokers.sh:645`<br>`banner "Verifying Cluster Readiness"` | `scripts/deploy-kafka-brokers.sh:645–675`<br>polls until all brokers listen | **SSH / TCP** — port 9092 listener check per broker |
| 13 | Prompt partition counts | `scripts/deploy-kafka-brokers.sh:676`<br>`banner "Topic Configuration"` | `scripts/deploy-kafka-brokers.sh:685`<br>`TOPICS_TO_CREATE[]`, `TOPIC_PARTITIONS[]` | **stdin** — per-topic partition count, default = broker count |
| 14 | Create topics | `scripts/deploy-kafka-brokers.sh:716`<br>`banner "Creating Kafka Topics"` | `scripts/deploy-kafka-brokers.sh:730`<br>iterates `TOPICS[]` (line 70) | **SSH → broker**<br>`kafka-topics.sh --create --if-not-exists --partitions ${TOPIC_PARTITIONS[$topic]}`<br>`--config retention.ms=604800000 --config cleanup.policy=delete` |
| 15 | Validate topics | `scripts/deploy-kafka-brokers.sh:745`<br>`banner "Validating Topics"` | `scripts/deploy-kafka-brokers.sh:745–767` | **SSH → broker**<br>`kafka-topics.sh --describe` |
| 16 | Balance leadership | `scripts/deploy-kafka-brokers.sh:768`<br>`banner "Balancing Partition Leadership"` | `scripts/deploy-kafka-brokers.sh:768–803` | **SSH → broker**<br>`kafka-leader-election.sh --all-topic-partitions --election-type PREFERRED` |
| 17 | Write `brokers.json` | `scripts/deploy-kafka-brokers.sh:807`<br>`banner "PHASE 3 — Writing brokers.json"` | `scripts/deploy-kafka-brokers.sh:804–864` | **Local file**<br>`brokers.json` (`BROKERS_JSON`) — `privateIp` + `publicIp` per broker |

**Script-level constants** (`scripts/deploy-kafka-brokers.sh:51–68`):

| Constant | Value |
|---|---|
| `AWS_REGION` | `${AWS_REGION:-ap-south-1}` |
| `KAFKA_AMI` | `ami-0018b1f38bf74ad62` |
| `GITHUB_REPO` | `https://github.com/swipenest-tech/swipenest-kafka-broker.git` |
| `DEFAULT_KEY_NAME` | `ec2-key-pair` |
| `DEFAULT_PEM_KEY` | `${HOME}/.ssh/ec2-key-pair.pem` |
| `DEFAULT_SUBNET_ID` | `subnet-0da50cf2f3ebd9280` |
| `KAFKA_SG_NAME` | `swipenest-kafka-sg` |
| `KAFKA_PORT` / `INTERNAL_PORT` / `CONTROLLER_PORT` | `9092` / `19092` / `9093` |
| `KAFKA_HOME` | `/opt/kafka` |
| `SSH_USER` | `ubuntu` |
| `INSTANCES_FILE` | `scripts/kafka-instances.json` |
| `BROKERS_JSON` | `brokers.json` (project root) |

**`server.properties` keys written by `configure_broker()`:**

`node.id` · `process.roles` · `controller.quorum.voters` · `listeners` · `advertised.listeners` · `listener.security.protocol.map` · `inter.broker.listener.name` · `log.dirs` · `min.insync.replicas` · `offsets.topic.replication.factor` · `transaction.state.log.replication.factor` · `transaction.state.log.min.isr`

---

### Module 2 — Topic Verification

**File:** `scripts/verify-topics.sh` (140 LOC)
**Invocation:** `./scripts/verify-topics.sh` · `SKIP_SSH=1 ./scripts/verify-topics.sh` (run directly on a broker)
**Not wired to an npm script** — invoked by path.

| Sr | Operation | Entry Point (file + invocation) | Orchestration (file + function) | Target Layer (file + operation) |
|:--:|---|---|---|---|
| 1 | Resolve bootstrap server | `scripts/verify-topics.sh`<br>`./scripts/verify-topics.sh` | `scripts/verify-topics.sh:53–69`<br>branches on `SKIP_SSH` | **SSH or localhost**<br>`SKIP_SSH=1` → `BOOTSTRAP=localhost:19092`<br>otherwise SSH to a broker first |
| 2 | Assert each topic exists | `scripts/verify-topics.sh:78`<br>`for topic in "${REQUIRED_TOPICS[@]}"` | `scripts/verify-topics.sh:78–89`<br>`ok()`, `warn()`, `fail()` (lines 43–45) | **Kafka CLI**<br>`kafka-topics.sh --describe`<br>on miss, prints the exact `kt --create ...` remedy |
| 3 | Assert `retention.ms` | `scripts/verify-topics.sh:105` | `scripts/verify-topics.sh:105–117`<br>compares against `EXPECTED_RETENTION_MS` | **Kafka CLI**<br>`kafka-configs.sh` output parsed with `grep -o "retention\.ms=[^,)]*"`<br>on mismatch prints `kt --alter --add-config` remedy |
| 4 | Assert `cleanup.policy` | `scripts/verify-topics.sh:106` | `scripts/verify-topics.sh:119–128`<br>compares against `EXPECTED_CLEANUP_POLICY` | **Kafka CLI**<br>parsed with `grep -o "cleanup\.policy=[^,)]*"` |
| 5 | Report partition count | `scripts/verify-topics.sh:114` | `scripts/verify-topics.sh:114`<br>membership test against `REQUIRED_TOPICS` | **Informational only** — never validated, since partition count is chosen interactively at deploy time |

**Constants** (`scripts/verify-topics.sh:28–39`):

| Constant | Value |
|---|---|
| `REGION` | `${AWS_REGION:-ap-south-1}` |
| `PEM_KEY` | `${HOME}/.ssh/ec2-key-pair.pem` |
| `KAFKA_HOME` | `/opt/kafka` |
| `INTERNAL_PORT` | `19092` |
| `REQUIRED_TOPICS` | `post_likes`, `video_view`, `post_impression`, `video_watch_progress`, `post_comments` |
| `EXPECTED_RETENTION_MS` | `604800000` (7 days) |
| `EXPECTED_CLEANUP_POLICY` | `delete` |
| `FAILURES` | Counter — non-zero exit when a topic is missing |

**Behaviour** — a **missing topic is a hard failure**; a wrong retention or cleanup policy is a **warning with a printed fix command**. Partition count is reported but never asserted.

---

### Module 3 — Data Loading (KafkaJS producer)

**File:** `scripts/load-data.js` (430 LOC)
**Invocation:** `npm run load-data` (via wrapper) · `TOTAL_RECORDS=5000 npm run load-data` · `node scripts/load-data.js`
**Standalone** — no dependency on `swipenest-core`.

| Sr | Operation | Entry Point (file + invocation) | Orchestration (file + function) | Target Layer (file + operation) |
|:--:|---|---|---|---|
| 1 | Discover brokers | `scripts/load-data.sh`<br>`npm run load-data` | `scripts/load-data.js:103`<br>`discoverBrokers()` | **AWS CLI** via `execSync`<br>`aws ec2 describe-instances --filters Name=tag:Role,Values=kafka-broker` |
| 2 | Verify each broker | `scripts/load-data.js:123`<br>`checkBroker(publicIp)` | `scripts/load-data.js:123–148`<br>confirms port 9092 open; reads `node.id` | **SSH → broker**<br>reads `node.id` from `/opt/kafka/config/server.properties` |
| 3 | Assemble broker list | — | `scripts/load-data.js:149`<br>`setupBrokers(brokers)` | Builds the bootstrap list from **public IPs : 9092** (CLIENT listener — no tunnels needed) |
| 4 | Hash viewer IDs | — | `scripts/load-data.js:46`<br>`hashUserId(id)` | **Node `crypto`**<br>`createHmac('sha256', 'swipenest-uid-hmac-k9x2')` |
| 5 | Build event payloads | — | `scripts/load-data.js:67`<br>`makeMessage(topic, idx, sessionCtx)`<br>helper `randInt(lo, hi)` (line 60) | In-memory — draws from `PLATFORMS`, `NETWORK_TYPES`, `SURFACES` (lines 62–64) |
| 6 | Connect producer | — | `scripts/load-data.js:181`<br>`getProducer()` | **KafkaJS**<br>`clientId: 'swipenest-broker-loader'`<br>`idempotent: true`<br>`createPartitioner: Partitioners.DefaultPartitioner` |
| 7 | Produce events | — | `scripts/load-data.js:213`<br>`produceEvents()` | **KafkaJS**<br>batches of `BATCH_SIZE = 100`, `acks: -1` (all ISRs)<br>distributes per `EVENTS_PER_TOPIC[]` (line 53) |
| 8 | Report distribution | — | `scripts/load-data.js:292`<br>`printBrokerStats(brokers)` | **KafkaJS Admin**<br>`clientId: 'swipenest-broker-loader-admin'`<br>per-broker partition distribution table |
| 9 | Disconnect | — | `scripts/load-data.js:205`<br>`disconnectProducer()` | **KafkaJS** — graceful producer shutdown |

**Constants** (`scripts/load-data.js:36–64`):

| Constant | Value |
|---|---|
| `AWS_REGION` | `${AWS_REGION:-ap-south-1}` |
| `EC2_USER` | `ubuntu` |
| `PEM_KEY` | `~/.ssh/ec2-key-pair.pem` |
| `KAFKA_PORT` | `9092` (CLIENT listener — public IP) |
| `TOTAL_RECORDS` | `${TOTAL_RECORDS:-2000}` |
| `BATCH_SIZE` | `100` |
| `TOPICS` | the five analytics topics |
| `_USER_HASH_SECRET` | `swipenest-uid-hmac-k9x2` |
| `PLATFORMS` | `android`, `ios`, `web` |
| `NETWORK_TYPES` | `4g`, `5g`, `wifi`, `3g` |
| `SURFACES` | `home`, `explore`, `profile`, `search` |

**Record distribution** — `TOTAL_RECORDS` is divided evenly across the five topics; the **last topic absorbs the remainder** (`PER_TOPIC + REMAINDER`, lines 51–54).

**Message schema** (matches `analytics.service.js → kafka-producer.js` in `swipenest-core`):

```js
{
  event_id, event, viewer_id,    // viewer_id = HMAC-SHA256 of userId
  creator_id, content_id,
  content_type,                  // 'video' for video_view / video_watch_progress
                                 // rand(['video','image']) for post_impression
                                 // rand(['video','image','reel']) for post_likes / post_comments
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

---

### Module 4 — Data Loading Wrapper

**File:** `scripts/load-data.sh` (108 LOC)
**Invocation:** `npm run load-data`

| Sr | Operation | Entry Point (file + invocation) | Orchestration (file + function) | Target Layer (file + operation) |
|:--:|---|---|---|---|
| 1 | Prompt record count | `package.json`<br>`npm run load-data` → `bash scripts/load-data.sh` | `scripts/load-data.sh`<br>`banner()` (line 27), `info()` (30), `success()` (31), `warn()` (32), `die()` (33) | **stdin** — record count, default `2000` |
| 2 | Confirm | `scripts/load-data.sh` | `scripts/load-data.sh` — yes/no gate | **stdin** |
| 3 | Delegate to loader | `scripts/load-data.sh` | exports `TOTAL_RECORDS` | **Node**<br>`node scripts/load-data.js` |

`TOTAL_RECORDS=5000 npm run load-data` bypasses the prompt for non-interactive use.

---

### Module 5 — Cluster Inspection (bash)

**File:** `scripts/check-data.sh` (220 LOC)
**Invocation:** `npm run check-data`

| Sr | Operation | Entry Point (file + invocation) | Orchestration (file + function) | Target Layer (file + operation) |
|:--:|---|---|---|---|
| 1 | Read broker inventory | `package.json`<br>`npm run check-data` → `bash scripts/check-data.sh` | `scripts/check-data.sh`<br>`info()` (53), `success()` (54), `warn()` (55), `error()` (56) | **Local files**<br>`brokers.json` + `scripts/kafka-instances.json` |
| 2 | SSH reachability check | `scripts/check-data.sh` | `scripts/check-data.sh` — per-broker probe | **SSH → each broker** |
| 3 | Broker info table | `scripts/check-data.sh` | `scripts/check-data.sh` — Section 1 | **stdout** — instance ID, node ID, private/public IP, type, storage, region |
| 4 | Dump `server.properties` | `scripts/check-data.sh` | `scripts/check-data.sh` — Section 1 | **SSH → each broker**<br>greps `node.id`, `process.roles`, listeners, `log.dirs`, replication factor |
| 5 | Delegate Sections 2 + 3 | `scripts/check-data.sh` | hands off to the JS reporter | **Node**<br>`node scripts/check-data.js` |

---

### Module 6 — Cluster Inspection (JS)

**File:** `scripts/check-data.js` (405 LOC)
**Invocation:** `npm run check-data:js` · `KAFKA_GROUP_ID=my-group npm run check-data` · `SKIP_LOOPBACK=1 node scripts/check-data.js`

| Sr | Operation | Entry Point (file + invocation) | Orchestration (file + function) | Target Layer (file + operation) |
|:--:|---|---|---|---|
| 1 | Discover brokers | `package.json`<br>`npm run check-data:js` → `node scripts/check-data.js` | `scripts/check-data.js:51`<br>`discoverBrokers()` | **AWS CLI** via `execSync`<br>`aws ec2 describe-instances --filters Name=tag:Role,Values=kafka-broker` |
| 2 | Read node IDs | — | `scripts/check-data.js:71`<br>`getNodeId(publicIp)` | **SSH → broker**<br>reads `node.id` from `server.properties` |
| 3 | Open SSH tunnels | — | `scripts/check-data.js:96`<br>`openTunnels(brokers)`<br>tracked in `_sshProcs[]`, `_loopbackIps[]` (42–43) | **OS + SSH**<br>`sudo ip addr add <privateIp>/32 dev lo`<br>`ssh -L <privateIp>:9092:<privateIp>:9092 ubuntu@<publicIp>` |
| 4 | Partition report | — | `scripts/check-data.js:161`<br>`printPartitionReport(admin, brokerIdToIp)` | **KafkaJS Admin**<br>`clientId: 'swipenest-broker-checker'`<br>`fetchTopicMetadata({ topics: TOPICS })`<br>→ topic / partition / leader IP / replicas / ISR / message count |
| 5 | Per-broker leader summary | — | `scripts/check-data.js:327` | **KafkaJS Admin** — partitions led, topics, % of total messages |
| 6 | Consumer lag report | — | `scripts/check-data.js:263`<br>`printConsumerLag(admin)` | **KafkaJS Admin**<br>`fetchOffsets({ groupId: KAFKA_GROUP_ID, topics: TOPICS })`<br>→ committed offset, end offset, lag |
| 7 | Teardown | — | `scripts/check-data.js:145`<br>`closeTunnels()` | **OS + SSH** — kills forwards, removes loopback aliases on exit |

**Why the tunnels exist** — KafkaJS metadata returns broker **private IPs**, which are unreachable from outside the VPC. The script adds loopback aliases for those private IPs locally and port-forwards each through the broker's public IP, so KafkaJS can connect to the private addresses it was handed.

`SKIP_LOOPBACK=1` skips alias setup — **single-broker use only**, since with multiple brokers the metadata redirect to another broker's private IP will fail.

**Constants** (`scripts/check-data.js:35–48`):

| Constant | Value |
|---|---|
| `AWS_REGION` | `${AWS_REGION:-ap-south-1}` |
| `KAFKA_GROUP_ID` | `${KAFKA_GROUP_ID:-swipenest-analytics-consumer}` |
| `EC2_USER` | `ubuntu` |
| `PEM_KEY` | `~/.ssh/ec2-key-pair.pem` |
| `TOPICS` | the five analytics topics |
| `SSH_OPTS` | `-i <PEM> -o StrictHostKeyChecking=no -o ConnectTimeout=10 -o BatchMode=yes -o LogLevel=ERROR` |

---

### Module 7 — Instance Teardown

**File:** `scripts/clear-kafka-instances.sh` (208 LOC)
**Invocation:** `npm run clear` **(currently broken — see [Known Issues](#12-known-issues--gotchas))** · `./scripts/clear-kafka-instances.sh` · `--region ap-south-1`

| Sr | Operation | Entry Point (file + invocation) | Orchestration (file + function) | Target Layer (file + operation) |
|:--:|---|---|---|---|
| 1 | List broker instances | `./scripts/clear-kafka-instances.sh` | `scripts/clear-kafka-instances.sh`<br>`info()` (31), `success()` (32), `warn()` (33), `error()` (34), `die()` (35) | **AWS CLI**<br>`aws ec2 describe-instances --filters Name=tag:Role,Values=kafka-broker`<br>states: pending, running, stopping, stopped |
| 2 | Prompt selection | `scripts/clear-kafka-instances.sh` | numbered table + parser | **stdin** — comma-separated numbers or `all` |
| 3 | Confirm | `scripts/clear-kafka-instances.sh` | yes/no gate | **stdin** |
| 4 | Terminate | `scripts/clear-kafka-instances.sh` | iterates the selection | **AWS CLI**<br>`aws ec2 terminate-instances --instance-ids ...` |
| 5 | Clean metadata cache | `scripts/clear-kafka-instances.sh` | removes cache when the fleet is empty | **Local file**<br>deletes `scripts/kafka-instances.json` |

---

## 7. Cluster Architecture

### KRaft mode

The cluster runs **KRaft** — no ZooKeeper. Every broker carries both roles via `process.roles`, and the controller quorum is formed from all brokers (`controller.quorum.voters`, built at `deploy-kafka-brokers.sh:480`).

### Listener layout

| Listener | Port | Bound to | Purpose |
|---|---|---|---|
| **CLIENT** | `9092` | Public IP | External KafkaJS clients — `swipenest-core` producers, `load-data.js`, `check-data.js` |
| **INTERNAL** | `19092` | Private IP | In-VPC consumers (`swipenest-kafka-consumer`); inter-broker replication |
| **CONTROLLER** | `9093` | Private IP | KRaft quorum voters — intra-cluster only |

> **On-broker admin tools must use port 19092.** The `kt` alias written to `/usr/local/bin/kt` wraps `kafka-topics.sh --bootstrap-server localhost:19092` for exactly this reason — using port 9092 from *on* a broker hits a hairpin NAT issue and fails.

### Replication

Replication factor is prompted at deploy time (1 to N). `min.insync.replicas` is derived:

```bash
MIN_ISR=$(( REPLICATION_FACTOR > 1 ? 2 : 1 ))    # deploy-kafka-brokers.sh:499
```

So a multi-broker cluster requires 2 in-sync replicas per write, while a single-broker cluster degrades to 1. The producer uses `acks: -1`, meaning every write waits for all in-sync replicas.

### Storage

`log.dirs` points at `/opt/kafka/data`. **`configure_broker()` wipes this directory** before formatting storage with the cluster UUID — re-running Phase 2 against a live cluster destroys all existing message data.

---

## 8. Topics & Partition Keys

### The five topics

All created during deployment with `retention.ms=604800000` (7 days) and `cleanup.policy=delete`.

| Topic | Description |
|---|---|
| `video_view` | A user watched a video |
| `post_impression` | A post was shown in feed |
| `post_likes` | A post was liked |
| `video_watch_progress` | Watch progress milestone |
| `post_comments` | A comment was posted |

Partition count is chosen **interactively per topic** at deploy time (default = broker count) and is never hardcoded — which is why `verify-topics.sh` reports it as informational rather than asserting it.

### Partition keys

Set by the producers in `swipenest-core`, not by this repo — but the cluster's ordering guarantees depend on them.

| Topic | Partition key | Why |
|---|---|---|
| `video_view` | `{viewer_id}:{content_id}` | Dedup-aware routing — same user+post always lands on the same partition |
| `post_impression` | `{viewer_id}:{content_type}:{content_id}` | Same as above |
| `video_watch_progress` | `{viewer_id}:{content_id}` | Same viewer+video on one partition → high-water-mark updates stay ordered |
| `post_likes` | `{post_id}:{content_type}:{liked_by}` | All like/unlike events from one user on one post are strictly sequential → prevents a like→unlike reordering from corrupting the count or status |
| `post_comments` | `{post_id}:{content_type}` | All comments for a post are processed in order on one partition |

**Producing files in `swipenest-core`:**

| Topic | Produced by |
|---|---|
| `video_view`, `post_impression`, `video_watch_progress` | `src/services/analytics.service.js` |
| `post_likes`, `post_comments` | `src/services/profile.image.service.js` / `src/services/profile.video.service.js` |

---

## 9. Data Files & Contracts

### `brokers.json` (project root)

The **single source of truth for broker IPs**. Written by `deploy-kafka-brokers.sh` at the end of Phase 3.

```json
{
  "_comment": "Kafka broker IPs. Written by swipenest-deployment on 2026-06-20T06:43:15.073Z",
  "brokers": [
    { "privateIp": "10.0.1.178", "publicIp": "13.207.188.98" }
  ]
}
```

> **The checked-in copy was written by `swipenest-deployment`, not by this repo's deploy script** — the `_comment` field records the writer. Two separate systems write this file, so treat the committed contents as a snapshot rather than authoritative.

**Consumed by:**

| Consumer | How |
|---|---|
| `scripts/check-data.sh` | Section 1 broker info table |
| `swipenest-core` | Copy to repo root, then `./scripts/refresh-brokers.sh` |
| `swipenest-kafka-consumer` | Copy to repo root, then `./scripts/refresh-brokers.sh` |

### `scripts/kafka-instances.json`

Local cache of EC2 metadata (instance IDs, node IDs, instance type, storage) written during provisioning. Auto-removed by `clear-kafka-instances.sh` once all instances are terminated. **Not committed** — regenerated per deploy.

### Message schema contract

`load-data.js` deliberately mirrors the production event schema so load tests exercise the same consumer code paths as real traffic. The contract is enforced only by convention — there is no shared schema file or registry between this repo and `swipenest-core`. See [Module 3](#module-3--data-loading-kafkajs-producer) for the full shape.

---

## 10. Configuration & AWS Defaults

### `.env.local`

Copy `.env.local.example` → `.env.local`.

| Variable | Default in `.env.local.example` | Purpose |
|---|---|---|
| `AWS_REGION` | `ap-south-1` | AWS region for EC2 and CLI commands |
| `KAFKA_PORT` | `19092` | INTERNAL listener port (VPC consumers) |
| `KAFKA_GROUP_ID` | `swipenest-consumer-group` | Consumer group ID for lag reporting |

> **`KAFKA_GROUP_ID` has two different defaults.** `.env.local.example` ships `swipenest-consumer-group`, but the code fallback in `check-data.js:36` is `swipenest-analytics-consumer`. Whichever applies depends on whether `.env.local` was created from the example. If a lag report shows zero committed offsets across every partition, this mismatch is the first thing to check — see [Known Issues](#12-known-issues--gotchas).

### Environment overrides

| Variable | Applies to | Effect |
|---|---|---|
| `TOTAL_RECORDS` | `load-data.js` | Record count, bypasses the interactive prompt |
| `KAFKA_GROUP_ID` | `check-data.js` | Consumer group for the lag report |
| `SKIP_LOOPBACK` | `check-data.js` | Skips loopback alias setup — single-broker only |
| `SKIP_SSH` | `verify-topics.sh` | Runs against `localhost:19092` — for use on a broker |
| `AWS_REGION` | all | Region override |

### AWS infrastructure defaults (ap-south-1)

| Setting | Value |
|---|---|
| Broker AMI | `ami-0018b1f38bf74ad62` — Ubuntu base; broker app cloned from GitHub at deploy time |
| Subnet | `subnet-0da50cf2f3ebd9280` |
| Security group | `swipenest-kafka-sg` — created on first deploy if absent, **reused unchanged thereafter** |
| Ingress: 9092 (CLIENT) | `$VPC_CIDR` + self-referencing SG + operator `/32` |
| Ingress: 19092 (INTERNAL) | `$VPC_CIDR` + self-referencing SG |
| Ingress: 9093 (CONTROLLER) | Self-referencing SG only |
| Ingress: 22 (SSH) | Operator `/32` (falls back to `0.0.0.0/0` if IP detection fails) |
| Key pair | `ec2-key-pair` (`~/.ssh/ec2-key-pair.pem`) |
| Instance tag | `Role=kafka-broker` — used by every discovery path |
| Kafka home | `/opt/kafka` |
| Data directory | `/opt/kafka/data` |

To deploy into a different VPC or region, update `DEFAULT_SUBNET_ID` and `AWS_REGION` at the top of `scripts/deploy-kafka-brokers.sh`, or pass `--region <region>`.

### Commands

```bash
# Full cluster lifecycle
npm run deploy-brokers                    # provision + configure + create topics
./scripts/deploy-kafka-brokers.sh --dry-run
./scripts/deploy-kafka-brokers.sh --region ap-southeast-1
./scripts/clear-kafka-instances.sh        # teardown (npm run clear is broken)

# Data operations
npm run load-data
TOTAL_RECORDS=5000 npm run load-data

# Inspection
npm run check-data                        # bash broker table + JS partition/lag report
npm run check-data:js                     # JS-only
KAFKA_GROUP_ID=my-group npm run check-data
SKIP_LOOPBACK=1 node scripts/check-data.js

# Topic verification
./scripts/verify-topics.sh
SKIP_SSH=1 ./scripts/verify-topics.sh     # on a broker instance
```

---

## 11. Cross-Repo Integration

### After a broker redeployment

Broker IPs change on every redeploy. Both downstream repos must be refreshed:

```bash
# swipenest-core (producers)
cp brokers.json ../swipenest-core/brokers.json
cd ../swipenest-core && ./scripts/refresh-brokers.sh

# swipenest-kafka-consumer (analytics consumer)
cp brokers.json ../swipenest-kafka-consumer/brokers.json
cd ../swipenest-kafka-consumer && ./scripts/refresh-brokers.sh
```

Both `refresh-brokers.sh` scripts discover running instances via AWS tags, overwrite `brokers.json` on each instance, and **restart PM2 only when the broker private IPs actually changed**.

### Which listener each consumer uses

| Repo | Listener | Address | Reason |
|---|---|---|---|
| `swipenest-core` | CLIENT | Public IP : 9092 | App servers may sit outside the broker VPC |
| `swipenest-kafka-consumer` | INTERNAL | Private IP : 19092 | Runs in-VPC — private path is faster and never leaves the VPC |
| `load-data.js` | CLIENT | Public IP : 9092 | Run from an operator laptop |
| `check-data.js` | CLIENT | Private IP : 9092 **via SSH tunnel** | KafkaJS metadata hands back private IPs |

### Shared secret

`_USER_HASH_SECRET = 'swipenest-uid-hmac-k9x2'` is duplicated in:

- `swipenest-kafka-broker/scripts/load-data.js:45`
- `swipenest-core/src/utils/crypto-mgt.js`

Both must match for load-test events to share a keyspace with production events.

---

## 12. Known Issues & Gotchas

### Broken npm scripts *(verified against the filesystem)*

Three `package.json` scripts point at files that **do not exist**:

| npm script | Target | Status |
|---|---|---|
| `npm run deploy` | `scripts/deploy-instances.sh` | **MISSING** |
| `npm run configure` | `scripts/configure-cluster.sh` | **MISSING** |
| `npm run clear` | `scripts/clear-instances.sh` | **MISSING** — actual file is `clear-kafka-instances.sh` |

`npm run clear` is documented in `CLAUDE.md` as the teardown command, but it fails. **Use `./scripts/clear-kafka-instances.sh` directly** until `package.json` is corrected. The other two appear to be remnants of a since-consolidated deploy split — `deploy-kafka-brokers.sh` now covers both provisioning and configuration.

### `KAFKA_GROUP_ID` default mismatch

`.env.local.example` ships `swipenest-consumer-group`; the code fallback in `check-data.js:36` is `swipenest-analytics-consumer`. A lag report against the wrong group returns no committed offsets and reads as "no consumer activity" rather than as a configuration error.

### Security posture

| Property | State | Verified | Impact |
|---|---|---|---|
| Kafka SASL / TLS | **None** | `grep -E "sasl\|ssl" scripts/*.js` → no matches | Anyone reaching :9092 can produce or consume any topic |
| Kafka ACLs | **None** | No ACL commands in the deploy script | No per-topic authorization |
| Broker data at rest | **Unencrypted** | No `Encrypted`/`kms` in `run-instances` | EBS volumes are plaintext |
| `StrictHostKeyChecking` | **Disabled** | `check-data.js:48` | SSH MITM protection removed |
| SG ingress CIDRs | **Scoped** — VPC CIDR + operator `/32` | `deploy-kafka-brokers.sh:240–271` | Reasonable default (see §3.3) |

**The ingress rules are properly scoped by default** — VPC CIDR for the Kafka listeners, a self-referencing SG rule for intra-cluster traffic, controller port restricted to the SG alone, and SSH limited to the operator's detected `/32`. This is a better posture than the plaintext-protocol picture alone suggests.

Two specific weaknesses remain:

1. **The `0.0.0.0/0` fallback.** If IP detection fails at deploy time, ports 22 and 9092 open to the internet on a cluster with no authentication of any kind. The chosen CIDR is printed but the deploy does not stop. Worth converting to a hard failure — an unattended or network-degraded deploy is exactly the situation where nobody reads the log line.

2. **Rules are create-only.** The script reuses an existing `swipenest-kafka-sg` without reconciling its rules (line 274–275), so a stale operator `/32` from an earlier deploy persists indefinitely, and a group that was once created with the `0.0.0.0/0` fallback stays wide open through every subsequent redeploy.

Given the network boundary is the *entire* access-control model, both are worth addressing before the auth model itself.

### Operational hazards

| Hazard | Detail |
|---|---|
| **Phase 2 wipes data** | `configure_broker()` deletes `/opt/kafka/data` before formatting. Re-running the deploy against a live cluster destroys all messages. |
| **No idempotent redeploy** | The deploy script is written for fresh provisioning, not in-place upgrade. |
| **Interactive by default** | No unattended path except `TOTAL_RECORDS` for the loader. Not CI-runnable as-is. |
| **Partition count is unverifiable** | Chosen interactively and never recorded, so `verify-topics.sh` cannot assert it. Two clusters deployed from the same script can differ silently. |
| **`SKIP_LOOPBACK=1` is single-broker only** | With multiple brokers, metadata redirects to unreachable private IPs and the report fails. |
| **`brokers.json` has two writers** | Both this repo and `swipenest-deployment` write it; the committed copy came from the latter. |
| **Node version unpinned** | No `engines` field, unlike `swipenest-core` which requires `>= 24.0.0`. |
| **7-day retention** | Messages are deleted after `604800000` ms. A consumer offline longer than a week loses data permanently. |

### Documentation drift

`CLAUDE.md` describes `npm run clear` as working and lists `KAFKA_GROUP_ID` default as `swipenest-analytics-consumer` in the config table while `.env.local.example` ships a different value. Both are corrected in this document against the source.

---

*End of reference.*
