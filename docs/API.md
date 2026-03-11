# API Reference

Base URL: `http://localhost:8080`

---

## Health

### `GET /health`

Returns 200 when the server is up.

**Response**
```json
{ "status": "ok" }
```

---

## Jobs

### `POST /api/v1/replay/jobs`

Create and start a new replay job.

**Request body**
```json
{
  "source_table":     "security_events",
  "start_time":       "2024-01-01T00:00:00Z",
  "end_time":         "2024-01-02T00:00:00Z",
  "speed_multiplier": 2.0,
  "target_endpoint":  "http://downstream.example.com/ingest"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source_table` | string | yes | Iceberg table name |
| `start_time` | ISO-8601 UTC | yes | Replay window start (inclusive) |
| `end_time` | ISO-8601 UTC | yes | Replay window end (exclusive) |
| `speed_multiplier` | float > 0 | yes | `1.0` = real-time, `2.0` = 2× speed |
| `target_endpoint` | URL | yes | Downstream REST endpoint for events |

**Response `201 Created`**
```json
{
  "id":               "b3d1e2f4-...",
  "status":           "PENDING",
  "source_table":     "security_events",
  "start_time":       "2024-01-01T00:00:00Z",
  "end_time":         "2024-01-02T00:00:00Z",
  "speed_multiplier": 2.0,
  "target_endpoint":  "http://downstream.example.com/ingest",
  "created_at":       "2024-03-11T10:00:00Z"
}
```

**Errors**

| Status | Reason |
|--------|--------|
| 400 | Missing or invalid field (body contains `errors` array) |

---

### `GET /api/v1/replay/jobs`

List all jobs (most recent first).

**Response `200 OK`**
```json
[
  { "id": "...", "status": "RUNNING", ... },
  { "id": "...", "status": "COMPLETED", ... }
]
```

---

### `GET /api/v1/replay/jobs/{id}`

Get a single job by ID.

**Response `200 OK`** — same shape as the create response.

**Errors**

| Status | Reason |
|--------|--------|
| 404 | Job not found |

---

### `POST /api/v1/replay/jobs/{id}/pause`

Pause a running job. The job finishes its current batch before stopping; no events are lost.

**Response `200 OK`**
```json
{ "id": "...", "status": "PAUSED" }
```

**Errors**

| Status | Reason |
|--------|--------|
| 404 | Job not found |
| 409 | Job is not in RUNNING state |

---

### `POST /api/v1/replay/jobs/{id}/resume`

Resume a paused job from its last checkpoint.

**Response `200 OK`**
```json
{ "id": "...", "status": "RUNNING" }
```

**Errors**

| Status | Reason |
|--------|--------|
| 404 | Job not found |
| 409 | Job is not in PAUSED state |

---

### `POST /api/v1/replay/jobs/{id}/cancel`

Cancel a job. Inflight batches are abandoned; already-published events are not rolled back.

**Response `200 OK`**
```json
{ "id": "...", "status": "CANCELLED" }
```

---

### `GET /api/v1/replay/jobs/{id}/status`

Live status summary including progress and ETA.

**Response `200 OK`**
```json
{
  "job_id":             "b3d1e2f4-...",
  "status":             "RUNNING",
  "packets_total":      20,
  "packets_completed":  8,
  "progress_pct":       40.0,
  "events_per_second":  1250.3,
  "eta_seconds":        18
}
```

| Field | Description |
|-------|-------------|
| `packets_total` | Total work-packets for this job |
| `packets_completed` | Packets fully published |
| `progress_pct` | `packets_completed / packets_total × 100` |
| `events_per_second` | Sliding 60-second EPS window |
| `eta_seconds` | Estimated seconds remaining (`null` when EPS = 0 or job done) |

---

### `GET /api/v1/replay/jobs/{id}/metrics`

Detailed throughput and latency metrics.

**Response `200 OK`**
```json
{
  "job_id":              "b3d1e2f4-...",
  "total_events":        120000,
  "total_batches":       240,
  "events_per_second":   1250.3,
  "avg_fetch_ms":        12.4,
  "avg_publish_ms":      8.7,
  "p99_batch_latency_ms": 45.0,
  "read_errors":         0,
  "publish_errors":      0
}
```

| Field | Description |
|-------|-------------|
| `total_events` | Cumulative events published to both sinks |
| `total_batches` | Number of batches processed |
| `events_per_second` | Sliding 60-second window |
| `avg_fetch_ms` | Mean Iceberg read time per batch |
| `avg_publish_ms` | Mean dual-sink publish time per batch |
| `p99_batch_latency_ms` | 99th-percentile batch latency (last 1 000 samples) |
| `read_errors` | Batches that failed during Iceberg read |
| `publish_errors` | Batches that failed during Kafka/REST publish |
