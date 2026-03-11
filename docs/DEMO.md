# Demo Walkthrough (~20 minutes)

This script demonstrates the full Replay System lifecycle: start, monitor, pause/resume, cancel, and inspect metrics.

---

## Prerequisites

Choose one:

**Option A — Local JVM (fastest setup)**
```bash
# Terminal 1: start Kafka and Postgres (Docker Compose or local installs)
# Then build and run:
mvn -q package -DskipTests
./scripts/deploy.sh
```

**Option B — Kubernetes (minikube)**
```bash
minikube start --memory=4096 --cpus=2
./scripts/k8s-deploy.sh
export BASE_URL=$(minikube service replay-api -n replay-system --url)
```

For local JVM, set:
```bash
export BASE_URL=http://localhost:8080
```

---

## 1. Health Check (1 min)

```bash
curl -s $BASE_URL/health | jq .
```

Expected:
```json
{ "status": "ok" }
```

---

## 2. Create a Replay Job (2 min)

```bash
JOB=$(curl -s -X POST $BASE_URL/api/v1/replay/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "source_table":     "security_events",
    "start_time":       "2024-01-01T00:00:00Z",
    "end_time":         "2024-01-02T00:00:00Z",
    "speed_multiplier": 1.0,
    "target_endpoint":  "http://httpbin.org/post"
  }')
echo $JOB | jq .
export JOB_ID=$(echo $JOB | jq -r '.id')
echo "Job ID: $JOB_ID"
```

Point out:
- The job is immediately `PENDING`, then transitions to `RUNNING`
- `speed_multiplier: 1.0` replays at wall-clock speed; try `10.0` to go fast

**Validation error demo** — show what bad input looks like:
```bash
curl -s -X POST $BASE_URL/api/v1/replay/jobs \
  -H 'Content-Type: application/json' \
  -d '{"source_table": "security_events"}' | jq .
```

---

## 3. List All Jobs (1 min)

```bash
curl -s $BASE_URL/api/v1/replay/jobs | jq .
```

---

## 4. Live Status (3 min)

Poll the status endpoint a few times to show progress:

```bash
watch -n 2 "curl -s $BASE_URL/api/v1/replay/jobs/$JOB_ID/status | jq ."
```

Highlight:
- `progress_pct` climbing
- `events_per_second` stabilising
- `eta_seconds` counting down

---

## 5. Live Metrics (2 min)

```bash
curl -s $BASE_URL/api/v1/replay/jobs/$JOB_ID/metrics | jq .
```

Talk through:
- `avg_fetch_ms` vs `avg_publish_ms` — fetch is usually slower (Iceberg scan)
- `p99_batch_latency_ms` — tail latency visible even under low load
- `read_errors` / `publish_errors` — both `0` in the happy path

---

## 6. Pause and Resume (3 min)

```bash
# Pause mid-flight
curl -s -X POST $BASE_URL/api/v1/replay/jobs/$JOB_ID/pause | jq .

# Status shows PAUSED; progress is frozen
curl -s $BASE_URL/api/v1/replay/jobs/$JOB_ID/status | jq .

# Resume — continues from exact checkpoint (no events re-sent or lost)
curl -s -X POST $BASE_URL/api/v1/replay/jobs/$JOB_ID/resume | jq .
```

Key point to demo: total_events in `/metrics` does **not** jump or repeat after resume — the checkpoint is at batch-boundary granularity.

---

## 7. Cancel a Job (2 min)

Start a fresh job and cancel it immediately:
```bash
JOB2=$(curl -s -X POST $BASE_URL/api/v1/replay/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "source_table":     "security_events",
    "start_time":       "2024-01-01T00:00:00Z",
    "end_time":         "2024-01-31T00:00:00Z",
    "speed_multiplier": 1.0,
    "target_endpoint":  "http://httpbin.org/post"
  }')
JOB2_ID=$(echo $JOB2 | jq -r '.id')

curl -s -X POST $BASE_URL/api/v1/replay/jobs/$JOB2_ID/cancel | jq .
curl -s $BASE_URL/api/v1/replay/jobs/$JOB2_ID/status | jq .
```

---

## 8. Concurrent Jobs (2 min)

Show the system handling multiple jobs simultaneously:
```bash
for i in 1 2 3; do
  curl -s -X POST $BASE_URL/api/v1/replay/jobs \
    -H 'Content-Type: application/json' \
    -d "{
      \"source_table\":     \"security_events\",
      \"start_time\":       \"2024-0${i}-01T00:00:00Z\",
      \"end_time\":         \"2024-0${i}-02T00:00:00Z\",
      \"speed_multiplier\": 5.0,
      \"target_endpoint\":  \"http://httpbin.org/post\"
    }" | jq -r '.id'
done

# List all — three RUNNING jobs
curl -s $BASE_URL/api/v1/replay/jobs | jq '[.[] | {id, status}]'
```

---

## 9. Architecture Walk-through (4 min)

Open `docs/architecture.png` and walk the diagram:

1. **HTTP layer** — `ReplayRoutes` → `JobsHandler` (blocking ask for control ops, non-blocking tell for create)
2. **Actor hierarchy** — `JobManager` (guardian) → `ReplayJobActor` (per job) → `WorkerPoolActor` → N × `PacketWorkerActor`
3. **Partitioning strategy** — each `PacketWorkerActor` publishes with `customer_id` as the Kafka key, ensuring per-customer ordering even under Zipf-skewed workloads (one whale customer dominates 80 % of events)
4. **Dual-emission** — Kafka + downstream REST run concurrently via `thenCombine`; failure in either surfaces as `PoolFailed`
5. **Pause/resume checkpoint** — workers pause only at batch boundaries; `batchIndex` is the checkpoint; resume continues from `batchIndex + 1`
6. **Metrics** — lock-free `LongAdder`/`AtomicInteger` in `MetricsRegistry`; 60-second EPS sliding window; P99 from bounded 1 000-sample deque
