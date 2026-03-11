# Replay System — Design Document

---

## 1. Architecture Overview

### High-Level Diagram

```
                        ┌─────────────────────────────────────────────────┐
                        │                  replay-system                  │
                        │                                                 │
  REST Client           │  ┌─────────────┐     ┌──────────────────────┐  │
      │  HTTP :8080      │  │MinimalHttp  │     │   JobsHandler        │  │
      └─────────────────►│  │Server       ├────►│   + JobValidator     │  │
                        │  └─────────────┘     └──────────┬───────────┘  │
                        │                                 │               │
                        │                    ┌────────────▼─────────┐    │
                        │                    │     JobManager        │    │
                        │                    │   (Pekko guardian)    │    │
                        │                    └────────────┬──────────┘    │
                        │                                 │ 1 per job     │
                        │                    ┌────────────▼──────────┐   │
                        │                    │   ReplayJobActor      │   │
                        │                    │   + WorkPlanner       │   │
                        │                    └────────────┬──────────┘   │
                        │                                 │               │
                        │                    ┌────────────▼──────────┐   │
                        │                    │   WorkerPoolActor     │   │
                        │                    └──┬─────┬──────┬───────┘   │
                        │                       │     │      │ N workers  │
                        │           ┌───────────▼─┐ ┌─▼──┐ ┌─▼──┐       │
                        │           │PacketWorker │ │ .. │ │ .. │       │
                        │           └──────┬──────┘ └────┘ └────┘       │
                        │                  │                              │
                        └──────────────────┼──────────────────────────────┘
                                           │
                   ┌───────────────────────┼────────────────────────┐
                   │                       │                        │
          ┌────────▼────────┐   ┌──────────▼──────────┐  ┌─────────▼────────┐
          │  Apache Iceberg │   │    Apache Kafka      │  │  Downstream REST │
          │  (Parquet/HDFS) │   │  (topic, cid key)    │  │  (target_topic)  │
          └─────────────────┘   └─────────────────────┘  └──────────────────┘
```

### Component Descriptions

| Component | Responsibility | Why this design |
|-----------|---------------|-----------------|
| `MinimalHttpServer` | Thin HTTP layer built on JDK's `HttpServer`. Routes `GET`/`POST` to handlers. | No framework dependency — keeps the JAR lean and startup < 1 s. Sufficient for a replay-control plane that has low request concurrency. |
| `JobsHandler` | Parses and validates inbound JSON, translates to domain commands, serialises responses. | Separating validation (`JobValidator`) from routing keeps each class single-purpose and unit-testable without spinning up HTTP. |
| `JobManager` | Pekko guardian actor. Owns the actor hierarchy; spawns one `ReplayJobActor` per job, receives completion callbacks. | A single guardian prevents races when two HTTP requests arrive for the same job simultaneously. The actor mailbox serialises those messages naturally. |
| `ReplayJobActor` | Plans work (delegates to `WorkPlanner`), spawns `WorkerPoolActor`, writes status to the repository. | Per-job isolation: a crash in one job's actor tree cannot corrupt another job's state. |
| `WorkerPoolActor` | Manages N `PacketWorkerActor`s; fans out pause/resume/cancel signals. | Fan-out to workers is O(N) messages — simple, no shared state. |
| `PacketWorkerActor` | Reads one `WorkPacket` from the data lake (batch by batch), dual-publishes to Kafka and REST, checkpoints at batch boundary. | Backpressure is natural: the next fetch is only dispatched after the previous publish completes. Virtual-thread executor keeps blocking I/O off the Pekko dispatcher. |
| `WorkPlanner` | Analyses Iceberg partition metadata, splits oversized partitions, applies Longest-Remaining-Time (LRT) bin-packing to assign packets to workers. | Planning is O(partitions × workers) — fast enough to be negligible compared to I/O. Decoupled from actors so it is trivially unit-testable. |
| `MetricsRegistry` | Lock-free per-job counters (`LongAdder`), 60-second EPS sliding window, P99 from a 1 000-sample ring buffer. | No Prometheus/Micrometer dependency; the design keeps the binary self-contained while still surfacing actionable metrics. |
| `JobRepository` | Interface with two implementations: `InMemoryJobRepository` (default) and `PostgresReplayJobRepository`. | Swappable via the `POSTGRES_URL` environment variable — no code change to move from local testing to production persistence. |

### Data Flow

```
POST /api/v1/replay/jobs
  │
  ├─ JobValidator validates fields
  ├─ JobsHandler persists PENDING job → JobRepository
  └─ JobManager.tell(StartJob)
       │
       └─ ReplayJobActor
            ├─ WorkPlanner.plan() — Iceberg partition scan (off-thread)
            │     returns List<WorkPacket> assigned by LRT to N workers
            └─ WorkerPoolActor.start(packets)
                 └─ for each PacketWorker (parallel):
                      loop:
                        1. DataLakeReader.readBatch(from, to, batchIndex, 500)
                           ← Iceberg predicate push-down, Parquet column prune
                        2. concurrently:
                           a. KafkaEventPublisher.publish(topic, events)
                              key = cid (customer_id)
                           b. DownstreamClient.post(events)
                        3. MetricsRegistry.recordBatch(...)
                        4. batchIndex++
                        5. if pause_requested → WAITING_RESUME
                      until events.isEmpty() → PacketDone
```

### Technology Stack Justification

| Technology | Version | Rationale |
|-----------|---------|-----------|
| **Java 21** | 21 LTS | Virtual threads (Project Loom) enable blocking I/O (Iceberg reads, HTTP posts) on cheap threads without a reactive framework. Records reduce boilerplate in the domain model. |
| **Apache Pekko (Typed Actors)** | 1.x | Provides supervision, location transparency, and backpressure-friendly message passing without the complexity of reactive streams. Typed API catches actor-protocol mismatches at compile time. |
| **Apache Iceberg** | 1.5 | Partition pruning, snapshot isolation, and schema evolution built-in. The `planFiles()` API returns file-level metadata before any data is read, which `WorkPlanner` uses to balance work without touching Parquet files. |
| **Apache Kafka** | 3.7 (KRaft) | Durable, ordered, partitioned log. KRaft mode removes the Zookeeper dependency, simplifying the Kubernetes deployment. |
| **Parquet** | (via Iceberg) | Columnar format enables projection push-down — we read only the columns needed per batch, reducing I/O by 40–60 % vs. row formats. |
| **PostgreSQL 16** | optional | Persistent job storage for production. Iceberg metadata reads are idempotent so re-planned jobs after a restart are safe. |
| **Kustomize + Kubernetes** | 1.28+ | Namespace isolation, per-component resource limits, and a single `kubectl apply -k` deployment path. |

---

## 2. API Design

### Complete Specification

#### `GET /health`
```
200 OK
{"status":"OK","timestamp":"2024-03-11T10:00:00Z"}
```

#### `POST /api/v1/replay/jobs` — Create and start a job

**Request**
```json
{
  "source_table":     "security_events",
  "target_topic":     "replay-output",
  "from_time":        "2024-01-01T00:00:00Z",
  "to_time":          "2024-01-02T00:00:00Z",
  "speed_multiplier": 1.0
}
```

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `source_table` | string | yes | Iceberg table name in the warehouse |
| `target_topic` | string | yes | Kafka topic name (`[a-zA-Z0-9._-]`, max 249 chars) |
| `from_time` | ISO-8601 UTC | yes | Must be strictly before `to_time` |
| `to_time` | ISO-8601 UTC | yes | Window ≤ 366 days |
| `speed_multiplier` | float | no | `> 0`, ≤ `1000.0`; defaults to `1.0` |

**Response `201 Created`**
```json
{
  "id":               "b3d1e2f4-...",
  "status":           "PENDING",
  "source_table":     "security_events",
  "target_topic":     "replay-output",
  "from_time":        "2024-01-01T00:00:00Z",
  "to_time":          "2024-01-02T00:00:00Z",
  "speed_multiplier": 1.0,
  "created_at":       "2024-03-11T10:00:00Z"
}
```

**Validation error `400 Bad Request`**
```json
{
  "errors": [
    { "field": "target_topic", "message": "required, must not be blank" },
    { "field": "from_time",    "message": "required (ISO-8601, e.g. 2024-01-01T00:00:00Z)" }
  ]
}
```

All validation errors are returned in one response — no round-trips required.

---

#### `GET /api/v1/replay/jobs` — List all jobs
```
200 OK  →  JSON array, most recent first
```

#### `GET /api/v1/replay/jobs/{id}` — Get job by ID
```
200 OK  |  404 Not Found
```

#### `GET /api/v1/replay/jobs/{id}/status`
```json
{
  "job_id":            "b3d1e2f4-...",
  "status":            "RUNNING",
  "events_published":  12500,
  "packets_completed": 5,
  "packets_total":     20,
  "progress_pct":      25.0,
  "elapsed_seconds":   10.4,
  "eta_seconds":       31,
  "started_at":        "2024-03-11T10:00:00Z",
  "updated_at":        "2024-03-11T10:00:10Z"
}
```

#### `GET /api/v1/replay/jobs/{id}/metrics`
```json
{
  "job_id":               "b3d1e2f4-...",
  "total_events":         12500,
  "total_batches":        25,
  "events_per_second":    1250.3,
  "avg_fetch_ms":         12.4,
  "avg_publish_ms":       8.7,
  "p99_batch_latency_ms": 45.0,
  "read_errors":          0,
  "publish_errors":       0
}
```

#### `POST /api/v1/replay/jobs/{id}/pause` | `/resume` | `/cancel`
```
200 OK  →  {"id": "...", "status": "PAUSED"|"RUNNING"|"CANCELLED"}
409 Conflict  →  job is not in the required state
404 Not Found
```

### Job Lifecycle

```
               ┌──────────┐
  POST /jobs   │  PENDING  │
  ─────────►   └────┬─────┘
                    │  actors ready, work planned
                    ▼
               ┌──────────┐ ◄── /resume
               │  RUNNING  │ ──► /pause ──► PAUSED
               └────┬─────┘
                    │  all packets done        /cancel
                    ▼                         (any state)
               ┌───────────┐               ┌───────────┐
               │ COMPLETED │               │ CANCELLED │
               └───────────┘               └───────────┘
                    │  any worker failure
                    ▼
               ┌──────────┐
               │  FAILED  │
               └──────────┘
```

### Job State Storage

- **In-memory** (default): `ConcurrentHashMap<UUID, ReplayJob>` in `InMemoryJobRepository`. Zero dependencies; lost on restart.
- **Postgres** (production): `PostgresReplayJobRepository` writes to a `replay_jobs` table. The actor system re-plans from Iceberg metadata on startup, so in-flight jobs resume cleanly after a pod restart.

Live metrics (EPS, P99, batch counts) are stored in `MetricsRegistry` (in-memory only) because they are ephemeral and recomputed automatically as workers run.

### Error Handling Approach

| Layer | Strategy |
|-------|---------|
| HTTP request validation | All fields validated before any state is written. Full error list returned in one `400` response. |
| Actor failures | `PacketWorkerActor` reports `PacketFailed` to `WorkerPoolActor` on any read or publish error; the pool marks the job `FAILED` and stops remaining workers. |
| Kafka publish failure | Surfaced as a failed `CompletableFuture`; the worker records `publish_errors++` in `MetricsRegistry` and stops. |
| Iceberg read failure | Same path as Kafka — `read_errors++`, worker stops, job transitions to `FAILED`. |
| Unknown job ID | `404` returned synchronously by `JobsHandler` before any actor message is sent. |
| State conflict (pause a completed job) | `409 Conflict` returned synchronously. |

### API Design Choices

- **No streaming endpoint** — poll-based status is sufficient for a control-plane API with sub-second update latency. Server-Sent Events would add complexity for marginal UX gain.
- **All validation up-front** — `JobValidator` is stateless and called before any persistence write. This keeps the repository free of partial/invalid records.
- **`speed_multiplier` is optional** — it defaults to `1.0` server-side so callers don't need to include it for the common real-time case.
- **`target_topic` over `target_endpoint`** — the primary delivery path is Kafka; the REST downstream is secondary and configured at the infrastructure level via `DOWNSTREAM_URL`.

---

## 3. Data Strategy

### Source Partitioning Strategy

**Partition key:** `event_time` (day granularity)
**Partition scheme:** `PartitionSpec.builderFor(schema).day("event_time")`

Iceberg stores one directory per calendar day:
```
security_events/
  data/
    event_time_day=2024-01-01/  ← ISO date string
    event_time_day=2024-01-02/
    …
    event_time_day=2024-01-30/
```

**Why day partitioning?**

Security event replay is always bounded by a time window. Day partitioning lets `WorkPlanner` skip irrelevant partitions entirely (Iceberg pushes the time predicate to the partition layer — no data files are opened for days outside the requested range). This makes planning for a 1-day window equally fast whether the table contains 1 month or 10 years of data.

**Trade-offs:**

| Pro | Con |
|-----|-----|
| Partition pruning eliminates irrelevant I/O at planning time | A single "hot" day with millions of events creates a large packet that must be split explicitly |
| Aligns naturally with replay time windows | Sub-day queries (e.g. 6-hour windows) still open a full day's partition |
| Simple, human-readable partition paths | Over-partitioning at hour granularity would create too many small files for 55k events |

**Large-partition splitting:** `WorkPlanner` splits any partition exceeding `MAX_EVENTS_PER_PACKET = 10 000` into equal time slices, capping per-worker latency and enabling parallelism within a single day.

### Target Partitioning Strategy (Kafka)

**Partition key:** `cid` (customer ID)
**Mechanism:** `KafkaEventPublisher` passes `cid` as the Kafka record key. Kafka's default partitioner uses `murmur2(key) % numPartitions` to assign each customer deterministically to one partition.

**Why `cid` as the key?**

- **Per-customer ordering is preserved** — all events for `cust-001` arrive at consumers in the same order they were replayed.
- **Skew resilience** — even when `cust-001` generates 18 % of all events (Zipf distribution), it is routed to exactly one partition, not causing a hotspot rebalancing across other partitions. Consumers that need `cust-001`'s events know exactly which partition to read.
- **Deterministic routing** — given the same `cid` and `numPartitions`, the target partition is always the same, enabling idempotent replay verification.

**Trade-off:** A single very active customer (whale) can make one partition significantly busier than others. For a replay system this is acceptable — lag accumulates on the consumer side, not the producer, and per-customer ordering is more valuable than perfect partition balance.

### REST Target Batching

Each `PacketWorkerActor` POSTs batches of up to `BATCH_SIZE = 500` events as a JSON array to the downstream endpoint. This caps individual request payload size at roughly 150–200 KB while keeping HTTP overhead low (< 0.2 % of payload).

### Test Data Characteristics

| Property | Value | Rationale |
|----------|-------|-----------|
| Total events | 55 000 | Exceeds 50k requirement; enough to exercise multi-packet parallelism |
| Date range | 2024-01-01 → 2024-01-30 | 30 day-partitions; tests both pruning and multi-packet planning |
| Customers | 100 (`cust-001` … `cust-100`) | Realistic scale for partition key distribution testing |
| Distribution | Zipf (fixed seed 42) | `cust-001` ≈ 18 %, `cust-002` ≈ 9 %, top-10 ≈ 60 % of events |
| Events per day | ~1 833 | Uniform across days; one packet per day (< 10 000 threshold) |
| Parquet file size | ~70 KB per file | Well within Parquet's sweet spot; Iceberg metadata stays small |

**Additional schema fields beyond the minimum:**

| Field | Type | Why included |
|-------|------|-------------|
| `event_id` | UUID string | Enables deduplication on the consumer side |
| `event_timestamp` | timestamp | Ingestion time (0–300 s after `event_time`); represents when the system received the event vs. when it occurred |
| `source_ip` | string | Realistic security event attribute; tests string column pruning |
| `target_host` | string | Same rationale; 10-value pool creates realistic cardinality |
| `severity` | string | Weighted pool (LOW 40 %, MEDIUM 35 %, HIGH 20 %, CRITICAL 5 %) mimics real-world alert distributions |
| `event_type` | string | 12 types (LOGIN_SUCCESS, PORT_SCAN, DATA_EXFIL, etc.) add domain authenticity |

---

## 4. Key Design Decisions

### How do you handle data skew?

Three complementary mechanisms:

1. **Skew detection at planning time.** `WorkPlanner.computeSkewScore()` measures the max-to-average file-size ratio within a day-partition. A high score signals that one or a few heavy customers (Zipf effect) dominate that partition.

2. **Large-partition splitting.** Any partition exceeding 10 000 estimated events is split into equal time slices. This converts one overloaded worker assignment into N balanced assignments.

3. **LRT bin-packing across workers.** After splitting, packets are sorted largest-first and assigned to the worker with the lowest cumulative load (Longest-Remaining-Time heuristic). This minimises makespan: heavy packets are distributed first when assignment options are widest.

### How do you distribute work efficiently?

- `WorkPlanner` queries only Iceberg file metadata (no data reads) to plan the work. Planning for a 30-day window completes in < 100 ms.
- LRT assignment is O(P × W) where P = packets and W = workers — negligible.
- Each `PacketWorkerActor` is independent; workers proceed at their own pace without synchronisation beyond the `PacketDone` / `PacketFailed` signals sent back to the pool.
- Virtual-thread-per-task executor (`Executors.newVirtualThreadPerTaskExecutor()`) in each worker: blocking Iceberg and HTTP calls never consume carrier threads.

### How do you handle failures and job recovery?

- **Worker failure:** `PacketWorkerActor` catches all exceptions in its async completions and reports `PacketFailed` to `WorkerPoolActor`. The pool stops all remaining workers and sets the job status to `FAILED`.
- **Partial failure visibility:** `read_errors` and `publish_errors` counters in `MetricsRegistry` are visible via `GET /metrics` even while the job is still running.
- **Restart recovery (Postgres mode):** On application restart, `RUNNING` and `PAUSED` jobs in the repository are re-planned from Iceberg metadata and re-started. Because `WorkPlanner` re-reads file metadata, any new files written during the outage are naturally included.
- **No retry loops:** Workers fail fast and surface errors rather than masking them with silent retries. This keeps failure visibility high.

### How do you manage concurrent jobs?

- `JobManager` maintains a `Map<UUID, ActorRef<ReplayJobCommand>>` of all live job actors. Each job is fully isolated in its own actor subtree.
- There is no global concurrency cap by design — concurrent jobs compete for the shared Kafka producer and Iceberg reader, which are both thread-safe. Resource contention is visible through per-job metrics.
- Control operations (pause, resume, cancel) are routed by `JobManager` to the correct job actor using the job UUID as the key — no actor receives another job's commands.

### Trade-offs

| Decision | Trade-off accepted |
|----------|--------------------|
| In-memory job store (default) | Simple for dev/test; jobs lost on restart. Postgres store adds a deploy dependency. |
| Batch-boundary checkpointing | At-least-once semantics per batch (last batch before pause could be re-sent if the process crashes mid-publish). True exactly-once would require Kafka transactions and is deferred. |
| No speed-multiplier enforcement in replay | `speed_multiplier` is stored and surfaced in status but the current implementation publishes as fast as the sink allows. Throttling requires a `ScheduledExecutor` delay per batch (not yet implemented). |
| Single Kafka producer shared across workers | Efficient (one TCP connection, batching), but a single misconfigured topic blocks all workers for that job. Per-worker producers would improve isolation at a higher connection cost. |
| Poll-based status | Simpler than SSE/WebSocket; adds 1–5 s latency to status updates under heavy load. Acceptable for a control-plane API. |

---

## 5. Limitations & Future Work

### Missing for Production

| Gap | Impact |
|-----|--------|
| `speed_multiplier` is not enforced | Replay always runs at maximum speed regardless of the requested multiplier |
| At-least-once only | A crash between Kafka `ack` and the batch-index increment can cause duplicate delivery of the last batch |
| No authentication / authorisation | The HTTP API is open; any client can create, pause, or cancel any job |
| In-memory metrics lost on restart | `MetricsRegistry` is not persisted; metrics for completed jobs disappear after a pod restart |
| Single-node Kafka (KRaft) | No replication; a Kafka pod restart loses unacknowledged messages |
| No Iceberg catalog integration | `HadoopTables` requires direct filesystem access; a Hive/Nessie/REST catalog would be needed for a shared data lake |
| Hard-coded `BATCH_SIZE = 500` | Should be tunable per-job to accommodate events of varying payload size |
| No dead-letter queue | Failed batches are counted but the events themselves are dropped |

### What Would Be Improved with More Time

1. **Speed-multiplier throttling** — add a per-batch sleep computed from `(events_in_batch / speed_multiplier) - actual_elapsed_ms` to honour wall-clock replay speed.
2. **Idempotency key on Kafka records** — include `event_id` as the Kafka header and enable idempotent producers + transactional commits to achieve exactly-once delivery.
3. **Persistent metrics** — flush `MetricsRegistry` snapshots to Postgres at job completion so historical metrics survive restarts.
4. **REST API authentication** — add API-key or JWT bearer token validation in `MinimalHttpServer`.
5. **Dynamic worker count** — let `WorkerPoolActor` adjust the number of live workers based on observed Kafka producer lag, preventing the replay job from overwhelming a downstream consumer.
6. **SSE / WebSocket status stream** — push status updates to clients instead of requiring polling.
7. **Multi-table support** — allow a single job to span multiple Iceberg tables (e.g. `security_events` + `audit_logs`) with a unified time window.
8. **Horizontal scaling** — partition jobs across multiple `replay-system` instances using a distributed lock (Redis/Postgres advisory lock) on job assignment, enabling scale-out beyond a single pod.

### Known Limitations

- The `WorkPlanner` skew score is based on file sizes, not record counts per customer — it is a heuristic, not an exact measure.
- Iceberg predicate push-down works correctly only when `event_time` values are written as `LocalDateTime` (the fix applied in this codebase). Third-party writers that use epoch-microsecond longs would bypass pruning.
- The `data-init` Kubernetes job is not idempotent if interrupted mid-write — it overwrites the table on restart, which is correct for test data but destructive for production data.
- Partition path format (`event_time_day=yyyy-MM-dd`) is tied to Iceberg's internal `DateType` serialisation; changing the partition spec requires a full table migration.
