# SwipeNest Kafka Broker — End-to-End Technical & Business Flow

## 1. Product Overview

`swipenest-kafka-broker` is a **DevOps/ops tooling repository** — not an application. It contains shell scripts and Node.js utilities to deploy, configure, manage, and inspect SwipeNest's Kafka cluster on AWS EC2 in KRaft mode (no ZooKeeper).

**There is no running application process in this repo.** It is purely infrastructure management code.

**Runtime context:** Kafka cluster runs on EC2 instances in `ap-south-1`, KRaft mode (broker + controller combined). Kafka is pre-installed at `/opt/kafka` on `ami-081dfc9f291f572f7`.

---

## 2. Business Context

SwipeNest uses Kafka to stream analytics events from `swipenest-core` (producer) to `swipenest-kafka-consumer` (consumer). The broker cluster is the backbone of the real-time analytics pipeline.

### Data Flow

```
swipenest-core (API)
  │
  │  POST /api/analytics/ingest
  │  Events: video_view, post_impression, video_watch_progress, post_likes, post_comments
  │
  ▼
Kafka Cluster (EC2, KRaft)
  │
  ▼
swipenest-kafka-consumer
  │
  ▼
MongoDB (analytics_events) + Redis (count:*) + MongoDB (content_analytics, creator_analytics)
```

---

## 3. Port Layout

| Listener | Port | Bound To | Used By |
|---------|------|----------|---------|
| CLIENT | 9092 | Public IP | External producers (`swipenest-core`), deploy/check scripts |
| INTERNAL | 19092 | Private IP | In-VPC consumers (`swipenest-consumer`), inter-broker replication |
| CONTROLLER | 9093 | Private IP | KRaft quorum voters (intra-cluster only) |

**Why two ports?** Brokers advertise their INTERNAL listener (19092) as the metadata endpoint for in-VPC services. External tools (scripts from developer machines) connect via port 9092 on public IPs. Scripts that SSH-tunnel must always use port 9092 on public IPs — using 9092 on the broker itself causes hairpin NAT issues, so on-broker tools use port 19092.

---

## 4. Cluster Deployment Flow (`deploy-kafka-brokers.sh`)

### Phase 1 — Provision EC2 Instances

```
Script prompts for:
  - Broker count (1–20)
  - Instance type (e.g., t3.medium)
  - EBS storage in GB
  - SSH key name

1. Create (or reuse) security group: swipenest-kafka-sg
   - Inbound rules: 22/tcp (SSH), 9092/tcp (CLIENT), 9093/tcp (CONTROLLER), 19092/tcp (INTERNAL)
   - Source: 0.0.0.0/0 for 22/9092; VPC CIDR only for 19092/9093 ideally

2. Launch EC2 instances:
   aws ec2 run-instances
     --image-id ami-081dfc9f291f572f7   (Ubuntu, Kafka pre-installed at /opt/kafka)
     --instance-type {type}
     --key-name {key}
     --security-group-ids {sg}
     --subnet-id subnet-0da50cf2f3ebd9280
     --block-device-mappings [{EBS size}]
     --count N

3. Wait for: aws ec2 wait instance-running
4. Wait for: aws ec2 wait instance-status-ok  (2/2 status checks)
5. Collect: instance IDs, public IPs, private IPs
6. Save to: scripts/kafka-instances.json
```

### Phase 2 — Configure KRaft Cluster

```
1. Generate cluster UUID on broker 1:
   ssh broker1: /opt/kafka/bin/kafka-storage.sh random-uuid
   → cluster_uuid="xxxxxxxxxxxxxxxxxx"

2. Prompt for replication factor (1–N)

3. For each broker (parallel SSH):
   a. Assign node.id (1, 2, 3, ...)
   b. Build quorum voters string:
      1@10.0.1.40:9093,2@10.0.1.41:9093,3@10.0.1.42:9093
   c. Write server.properties:
      process.roles=broker,controller
      node.id={node_id}
      controller.quorum.voters={voters_string}
      listeners=CLIENT://{publicIp}:9092,INTERNAL://{privateIp}:19092,CONTROLLER://{privateIp}:9093
      advertised.listeners=CLIENT://{publicIp}:9092,INTERNAL://{privateIp}:19092
      listener.security.protocol.map=CLIENT:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
      inter.broker.listener.name=INTERNAL
      controller.listener.names=CONTROLLER
      log.dirs=/opt/kafka/data
      log.retention.ms=604800000  (7 days)
      cleanup.policy=delete
   
   d. Wipe and format storage:
      rm -rf /opt/kafka/data/*
      /opt/kafka/bin/kafka-storage.sh format -t {cluster_uuid} -c server.properties
   
   e. Write /usr/local/bin/kt alias:
      kafka-topics.sh --bootstrap-server localhost:19092
   
   f. Start Kafka:
      /opt/kafka/bin/kafka-server-start.sh -daemon server.properties

4. Poll until all brokers respond on port 9092 (retry every 5s, timeout 120s)
```

### Phase 3 — Create Topics & Output

```
1. Prompt for per-topic partition count (default = broker_count for optimal distribution)

2. Create all 5 analytics topics:
   For each topic in [video_view, post_impression, post_likes, video_watch_progress, post_comments]:
     kafka-topics.sh --create --topic {topic}
       --bootstrap-server broker1:9092
       --partitions {count}
       --replication-factor {replication_factor}
       --if-not-exists

3. Validate topics exist:
   kafka-topics.sh --describe --topic {topic} --bootstrap-server broker1:9092

4. Run preferred leader election:
   kafka-leader-election.sh --all-topic-partitions --bootstrap-server broker1:9092

5. Write brokers.json (project root):
   {
     "_comment": "Auto-generated by deploy script",
     "brokers": [
       { "privateIp": "10.0.1.40", "publicIp": "13.x.x.x" },
       { "privateIp": "10.0.1.41", "publicIp": "13.x.x.y" }
     ]
   }

6. Print instruction:
   cp brokers.json ../swipenest-kafka-consumer/brokers.json
   (swipenest-core reads brokers.json automatically from its project root)
```

---

## 5. Test Data Loading Flow (`load-data.sh` / `load-data.js`)

### Purpose
Load synthetic analytics events into Kafka for testing the consumer pipeline.

```
npm run load-data  (or: TOTAL_RECORDS=5000 npm run load-data)

1. load-data.sh: prompts for record count (default 2000), confirms, runs load-data.js

2. load-data.js:
   a. Discover live broker EC2 instances:
      aws ec2 describe-instances --filters "Name=tag:Role,Values=kafka-broker" "Name=instance-state-name,Values=running"
   
   b. SSH to each broker: read node.id from server.properties, verify port 9092 open
   
   c. Connect KafkaJS producer to public IPs:9092 (CLIENT listener)
      { clientId: 'load-test-producer', brokers: [publicIps], idempotent: true, acks: -1 }
   
   d. Distribute total records evenly across 5 topics
   
   e. For each topic batch (100 messages at a time):
      Generate synthetic event message matching analytics.service.js schema:
      {
        event_id:           uuid(),
        event:              topic_name,
        viewer_id:          hmac_sha256(random_userId, 'swipenest-uid-hmac-k9x2'),
        creator_id:         uuid(),
        content_id:         uuid(),
        content_type:       'video' | 'image' | 'reel' (topic-specific)
        surface:            rand(['home', 'explore', 'profile']),
        platform:           rand(['android', 'ios']),
        network_type:       rand(['4g', 'wifi', '5g']),
        analyticsSessionId: uuid(),
        server_timestamp:   new Date().toISOString(),
        schema_version:     1,
        // video_watch_progress extras:
        watched_ms:         random,
        watched_per:        random 1-100,
        autoplay:           true/false,
        // post_comments extras:
        action:             'comment',
        comment_id:         uuid()
      }
      
      Partition key:
        video_view:      "{viewer_id}:{content_id}"
        post_impression: "{viewer_id}:{content_type}:{content_id}"
        others:          event_id
      
      producer.sendBatch({ topicMessages, acks: -1 })
   
   f. Print per-broker partition distribution table via Admin API:
      { topic, partition, leader, replicas, ISR, messageCount }
```

---

## 6. Cluster Inspection Flow (`check-data.sh` / `check-data.js`)

### `check-data.sh` (bash sections)

**Section 1 — Broker Info Table:**
```
1. Read brokers.json + scripts/kafka-instances.json
2. For each broker:
   - AWS: describe-instances (instance type, AZ, state)
   - SSH: ping, verify port 9092 open
   - SSH: grep key fields from server.properties
3. Print table: instance-id | node-id | public-ip | private-ip | type | storage | region
4. SSH: cat server.properties (node.id, roles, listeners, log.dirs, replication.factor)
```

**Section 2+3 — Delegate to check-data.js**

### `check-data.js` — SSH Tunnel Approach

**Why tunnels?** KafkaJS `fetchTopicMetadata()` returns INTERNAL listener (private IP). Private IPs are unreachable from outside the VPC, so tunnels + loopback aliases make them reachable:

```
1. Discover live brokers via AWS CLI (tag Role=kafka-broker)
2. SSH each broker: read node.id from server.properties
3. Add loopback alias for each broker's private IP:
   sudo ip addr add {privateIp}/32 dev lo
4. Open SSH port-forward:
   ssh -L {privateIp}:9092:{publicIp}:9092 (via localhost → broker public IP → private IP:9092)
5. Connect KafkaJS Admin to private IPs (now routed via loopback + SSH tunnel)
6. Fetch metadata: admin.fetchTopicMetadata(), admin.listOffsets()

Reports:
  a. Partition Distribution:
     topic | partition | leader | replicas | ISR | earliest_offset | latest_offset | message_count
     Per-topic total: sum of message counts
     Grand total
  
  b. Per-Broker Leader Summary:
     broker | partitions_led | topics | % of total messages
  
  c. Consumer Group Lag (KAFKA_GROUP_ID env var, default 'swipenest-consumer-group'):
     admin.fetchOffsets({ groupId, topics })
     topic | partition | committed_offset | end_offset | lag
  
7. Cleanup:
   - Kill SSH tunnels (background processes)
   - Remove loopback aliases: sudo ip addr del {privateIp}/32 dev lo
```

---

## 7. Cluster Teardown Flow (`clear-kafka-instances.sh`)

```
./scripts/clear-kafka-instances.sh
(or: npm run clear)

1. List all EC2 instances tagged Role=kafka-broker in states: pending/running/stopping/stopped
2. Print numbered table with instance-id, state, public-ip, private-ip, instance-type
3. Prompt: "Enter instance numbers to terminate (comma-separated) or 'all':"
4. Confirm: "Are you sure? (yes/no)"
5. aws ec2 terminate-instances --instance-ids {selected_ids}
6. If ALL terminated: rm scripts/kafka-instances.json
```

---

## 8. Key Configuration (`.env.local`)

| Variable | Default | Purpose |
|---------|---------|---------|
| `AWS_REGION` | `ap-south-1` | AWS region for all operations |
| `KAFKA_PORT` | `19092` | INTERNAL listener port (for consumer) |
| `KAFKA_GROUP_ID` | `swipenest-consumer-group` | Consumer group for lag reporting |

> Set `KAFKA_GROUP_ID=swipenest-analytics-consumer` to match the actual consumer group used by `swipenest-kafka-consumer`.

---

## 9. `brokers.json` — Source of Truth for IPs

```json
{
  "_comment": "Written by deploy-kafka-brokers.sh. Read by swipenest-core at startup.",
  "brokers": [
    { "privateIp": "10.0.1.40", "publicIp": "13.233.100.1" },
    { "privateIp": "10.0.1.41", "publicIp": "13.233.100.2" },
    { "privateIp": "10.0.1.42", "publicIp": "13.233.100.3" }
  ]
}
```

**Consumers of `brokers.json`:**
- `swipenest-core/src/constants/project-constants.js` — reads automatically (KAFKA_BROKER resolution step 2)
- `swipenest-kafka-consumer/src/config.js` — reads automatically (broker resolution step 1)
- `check-data.sh` — Section 1 broker info table
- `load-data.js` — broker discovery

**After each broker deployment:** Manually copy to consumer: `cp brokers.json ../swipenest-kafka-consumer/brokers.json`

---

## 10. Kafka Topics Summary

| Topic | Purpose | Messages/Day (est.) | Partition Key | Dedup |
|-------|---------|--------------------|----|-------|
| `video_view` | User watched a video | Very high | `{viewer_id}:{content_id}` | Yes (consumer) |
| `post_impression` | Post shown in feed | Very high | `{viewer_id}:{content_type}:{content_id}` | Yes (consumer) |
| `video_watch_progress` | Watch milestone events | High | `{event_id}` | No |
| `post_likes` | Like/unlike actions | Medium | `{event_id}` | No |
| `post_comments` | Comment events | Medium | `{event_id}` | No |

**Retention:** 7 days (604800000ms)  
**Cleanup policy:** delete  
**Default partitions:** equal to broker count (for balanced distribution)  
**Replication factor:** 1 (single broker) or 2+ (multi-broker, prompted during deploy)
