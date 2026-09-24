# Section 1 — Written answers

## 1.1 Durability basics (`acks=all`, RF=3, `min.insync.replicas=2`)

A producer writes with (**acks=all**), and the topic is configured with:

* **Replication Factor (RF): 3**

* **min.insync.replicas = 2**

This configuration provides strong durability guarantees.

### 1.1.1 What `acks=all` waits for.
* `acks=all` (or `acks=-1`) means: The leader will acknowledge the write only after all in‑sync replicas (ISR) have successfully written the message to their logs.

* With `min.insync.replicas=2`, Kafka requires:

Leader + **at least one follower** to confirm the write before acknowledging the producer.

If fewer than 2 replicas are in the ISR, the write is rejected.
### Flow Diagram

**sequence Diagram**

    participant Producer
    participant Leader
    participant Follower1
    participant Follower2

    Producer->>Leader: Send message
    Leader->>Leader: Write to local log
    Leader->>Follower1: Replicate
    Follower1->>Leader: ACK
    Leader->>Producer: ACK (acks=all satisfied)


### 1.1.2 What happens when one broker holding a replica goes offline ?

  With `RF=3`, suppose one follower goes offline:

* **ISR** shrinks from **[Leader, F1, F2] → [Leader, F1]**
   The ISR shrinks from 3 to 2 (if the dead broker led a partition, the controller elects a new leader from the ISR).

* **ISR** still has **2 replicas**, which satisfies `min.insync.replicas=2`

   2 >= min ISR, so writes keep succeeding, now acknowledged by the two survivors. There is a short stall first: until the dead follower is dropped from the ISR (`replica.lag.time.max.ms`, 30 s by default), `acks=all` is still waiting on it. `UnderReplicatedPartitions` goes above 0. That is an alert, not an outage.
* Producer writes with acks=all continue normally

* The offline broker becomes an out-of-sync replica (OSR)
#### Differences:

|   Before failure:    | After one broker offline: |
|        ---           |          ---              |            
|   ISR = [L, F1, F2]  |      ISR = [L, F1]        |
|   OSR = []           |      OSR = [F2]           |
|   Writes allowed     |      Writes allowed       |

Kafka prioritizes durability over availability.

### 1.1.3 What happens when two brokers holding a replica goes offline ?

If two brokers fail:

ISR shrinks to only the leader → ISR = [Leader]

**ISR** count = 1, which is less than `min.insync.replicas=2`

Kafka rejects all writes with **NOT_ENOUGH_REPLICAS** error

Producer cannot write with `acks=all`

The ISR falls to 1, which is below the minimum, so produce requests fail with `NotEnoughReplicasException`; the producer retries and then surfaces the error. Nothing is silently accepted with weaker durability. If the survivor was in the ISR it becomes leader and consumers can still read. If it was *not* in the ISR (it was lagging), the partition is offline unless `unclean.leader.election.enable=true`, which I would leave off because it can lose acknowledged data. We pick consistency over availability.

### 1.1.4 Why `min.insync.replicas=2`, not 3.

Setting `min.insync.replicas=2` strikes the right balance between:

#### 1. Durability
Ensures every acknowledged write is stored on at least two brokers

Protects against single‑broker failure

#### 2. Availability
If `min.insync.replicas=3`:

All three replicas must be in sync for writes to succeed

Even one broker going offline would block all writes

This is too strict for most production environments

#### 3. Practicality
With `RF=3`, requiring 2 in‑sync replicas is the industry standard

Allows the cluster to tolerate one broker failure without impacting producers

#### Summary Table

|          Setting          | Durability | Availability | Practical? |
|   ---                     |    ---     |     ---      |       ---     |
| **min.insync.replicas=1** | Weak       |    High      |   Risky |
| **min.insync.replicas=2** | Strong     |    Good      | ✔ Common choice |
| **min.insync.replicas=3** | Very strong |    Low      | ✘ Too strict |

## 1.2 Confluent Cloud on GCP — network path

#### 1.2.1 Path from a GCE client in your VPC to the Confluent Cloud Dedicated cluster (via PSC)

* A GCE VM in the client VPC reaches the Confluent Cloud Dedicated cluster through a **Private Service Connect (PSC) endpoint**.
When the Kafka client resolves the bootstrap hostname, DNS returns a **private PSC IP** instead of a public endpoint.
The client first connects to the PSC endpoint, which forwards traffic privately over Google’s backbone to Confluent Cloud in **us‑central1**.
During bootstrap, the client receives the list of broker hostnames, and each broker hostname again resolves to a PSC private IP.
The client then establishes **per‑broker connections** through PSC, ensuring all Kafka traffic stays inside Google’s private network.

```text
GCE VM (client VPC)
    |
    | DNS lookup for Kafka bootstrap hostname
    v
Private Cloud DNS zone
    |
    | resolves to PSC endpoint IP
    v
GCP Private Service Connect endpoint
    |
    v
Confluent Cloud network
    |
    v
Kafka bootstrap endpoint
    |
    | Metadata response
    v
Per-broker hostnames
    |
    +----> Broker 1 connection
    +----> Broker 2 connection
    +----> Broker 3 connection
```

* The client first resolves the bootstrap hostname through the private DNS configuration and connects to the PSC endpoint. Kafka then returns broker metadata, after which the client opens separate connections to the advertised broker endpoints. For GCP PSC, Confluent uses private DNS names under the cluster's Confluent Cloud DNS domain; the private DNS zone must be associated with the VPC that contains the PSC endpoint. A common misconfiguration is associating the private zone with a different VPC. I would detect it with `dig`/`nslookup` from the GCE VM and verify that the result is the expected PSC endpoint address.

#### 1.2.2 Private DNS zone convention and required VPC attachment

* Confluent Cloud creates a private DNS zone using the naming pattern: `<cluster-id>.gcp.confluent.cloud`
This zone contains private A records that map each broker hostname to the PSC endpoint IP.
The private DNS zone **must be attached to the same VPC where the GCE Kafka clients run**, because DNS resolution must occur locally within that VPC.
If the zone is attached to a different VPC (for example, a shared-services VPC), clients will not resolve the PSC private IPs unless DNS peering is explicitly configured.

#### 1.2.3 One common misconfiguration and how to detect it

* A common misconfiguration is attaching the Confluent private DNS zone to the **wrong VPC**, causing broker hostnames to resolve to **public Confluent endpoints** instead of **PSC private IPs**.
This leads to TLS handshake failures, public egress attempts, or clients failing to connect entirely.
You can detect this by running `dig` or `nslookup` from the GCE VM and checking whether broker hostnames resolve to **10.x.x.x PSC IPs**.
If they resolve to public IPs, the DNS zone is not attached correctly.
Fixing the issue requires attaching the private DNS zone to the client VPC or configuring Cloud DNS peering properly.

## 1.3 Producer latency triage

* Producer **p99 latency** increased from **30 ms → ~400 ms**, while **throughput is unchanged** and **broker CPU is flat**.
This usually indicates a bottleneck in the producer path, network, or broker I/O rather than CPU saturation.

Given unchanged throughput and flat broker CPU, I would investigate:
#### 1.3.1 Likely Causes (Ranked Most → Least Likely)

**1. Network latency/retransmissions** — check producer request latency and TCP retransmissions.

**2. Broker disk/storage latency** — check produce request queue/processing latency and disk `await`/ `I/O` utilization.

**3. ISR/replication pressure** — check `UnderReplicatedPartitions`, **ISR** changes, and produce request `errors`/`retries`.

**4. Producer batching/retry behavior** — check `linger.ms`, batch size, retries and producer request latency.

### 1.3.2 For each cause, name one metric or one log line you would check.

**1. Network RTT increase or intermittent packet loss**
Even small RTT spikes can push p99 latency up without affecting throughput. Metric/log to check:

* `network_rtt_ms` or `socket-timeouts` in producer logs

* Broker metric: `kafka.network.RequestMetrics.RequestQueueTimeMs.p99`

**2. Broker I/O slowdown (disk latency, page cache pressure)**
Broker CPU may be flat, but disk I/O latency can delay fsync and replication. Metric/log to check:

* Broker metric: `kafka.server.ReplicaFetcherManager.ReplicaFetcherLag`

* OS metric: iostat -x (await, svctm)

**3. ISR shrink causing replication delays**
If ISR shrinks, the leader waits longer for follower acknowledgments (`acks=all`). Metric/log to check:

* UnderReplicatedPartitions

* Broker log: ReplicaFetcherThread warnings

**4. Producer-side batching or buffer exhaustion**
If the producer is hitting `buffer.memory` or `batch.size limits`, p99 latency spikes. Metric/log to check:

* Producer metric: `record-send-total`, `bufferpool-wait-time-ns`

* robProducer log: `BufferExhaustedException`

#### 1.3.3 Shell Command to Check Recent GC Pauses on Broker Host

A useful broker-side check for recent JVM GC pauses is:

```bash
grep -Ei 'GC pause|Full GC|Pause Young|Pause Full|gc' /var/log/kafka/server.log | tail -100
```

If Kafka is configured with a dedicated JVM GC log, I would inspect that configured file instead of assuming a log path.

## 1.4 Ansible change gone wrong

`serial: 3` is unsafe because three of six brokers can be unavailable simultaneously. Depending on partition placement and leadership, that can remove multiple replicas or leaders for many partitions at once and cause ISR shrinkage. The play should first verify the cluster is healthy and `UnderReplicatedPartitions` is zero. It should then use `serial: 1` and wait for the restarted broker to rejoin before touching another broker. A post-restart health check should verify broker availability, ISR recovery and offline partitions.

**Recovery order:**

1. Stop the Ansible rollout.
2. Identify unavailable brokers and affected partitions.
3. Bring failed brokers back online.
4. Wait for replicas to catch up and ISR to recover.
5. Confirm `UnderReplicatedPartitions=0` and no offline partitions.
6. Check client/leader-election errors.
7. Resume changes one broker at a time only after the cluster is healthy.

## 1.5 ZooKeeper vs KRaft

ZooKeeper provides metadata coordination and controller-election functionality in the traditional Kafka architecture. KRaft replaces ZooKeeper with Kafka's own Raft-based metadata quorum. KRaft keeps cluster metadata management inside Kafka instead of requiring a separate ZooKeeper system. For a new cluster today, I would use KRaft because it is Kafka's native metadata architecture and removes the separate ZooKeeper dependency. The final choice should still consider the Kafka version and compatibility requirements of the target platform.
