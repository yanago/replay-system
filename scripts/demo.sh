#!/usr/bin/env bash
# =============================================================================
# demo.sh  —  End-to-end Replay System demo
# =============================================================================
# Generates 55k+ events, starts the application, then drives the full job
# lifecycle: create → start → pause → resume → (wait for completion).
#
# Prerequisites:
#   • Java 21+ on $PATH
#   • Maven 3.8+ on $PATH
#   • curl and jq on $PATH
#
# Usage:
#   ./scripts/demo.sh
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BASE_URL="http://localhost:8080"
DATA_DIR="$REPO_ROOT/data/security_events"
APP_PID=""

cleanup() {
    if [[ -n "$APP_PID" ]]; then
        echo ""
        echo "==> Stopping application (PID $APP_PID) …"
        kill "$APP_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

cd "$REPO_ROOT"

# ---------------------------------------------------------------------------
# 1. Build
# ---------------------------------------------------------------------------
echo "=================================================================="
echo " Replay System Demo"
echo "=================================================================="
echo ""
echo "==> Building fat JAR …"
mvn -q package -DskipTests
JAR="$(ls target/replay-system-*.jar | grep -v original | head -1)"
echo "    JAR: $JAR"

# ---------------------------------------------------------------------------
# 2. Generate data
# ---------------------------------------------------------------------------
echo ""
echo "==> Generating 55 000+ security events …"
java -cp "$JAR" com.example.replay.tools.DataGenerator "$DATA_DIR"
echo "    Table: $DATA_DIR"

# ---------------------------------------------------------------------------
# 3. Start application in background
# ---------------------------------------------------------------------------
echo ""
echo "==> Starting Replay System (in-memory storage) …"
java -jar "$JAR" &
APP_PID=$!
echo "    PID: $APP_PID"

# Wait for the health endpoint to be ready
echo -n "    Waiting for /health "
for i in $(seq 1 30); do
    if curl -sf "$BASE_URL/health" >/dev/null 2>&1; then
        echo " ✓"
        break
    fi
    echo -n "."
    sleep 1
done

echo ""
curl -s "$BASE_URL/health" | jq .

# ---------------------------------------------------------------------------
# 4. Create a replay job
# ---------------------------------------------------------------------------
echo ""
echo "=================================================================="
echo " POST /api/v1/replay/jobs  — create job (PENDING)"
echo "=================================================================="
JOB=$(curl -s -X POST "$BASE_URL/api/v1/replay/jobs" \
    -H "Content-Type: application/json" \
    -d "{
      \"source_table\":     \"$DATA_DIR\",
      \"target_topic\":     \"replay-output\",
      \"from_time\":        \"2024-01-01T00:00:00Z\",
      \"to_time\":          \"2024-01-31T23:59:59Z\",
      \"speed_multiplier\": 1.0
    }")
echo "$JOB" | jq .
JOB_ID=$(echo "$JOB" | jq -r .job_id)
echo "    Job ID: $JOB_ID"

# ---------------------------------------------------------------------------
# 5. Start the job
# ---------------------------------------------------------------------------
echo ""
echo "=================================================================="
echo " POST /api/v1/replay/jobs/$JOB_ID/start  — RUNNING"
echo "=================================================================="
curl -s -X POST "$BASE_URL/api/v1/replay/jobs/$JOB_ID/start" | jq .
sleep 2

# ---------------------------------------------------------------------------
# 6. Check status
# ---------------------------------------------------------------------------
echo ""
echo "=================================================================="
echo " GET /api/v1/replay/jobs/$JOB_ID  — status check"
echo "=================================================================="
curl -s "$BASE_URL/api/v1/replay/jobs/$JOB_ID" | jq '{job_id, status, events_published}'

# ---------------------------------------------------------------------------
# 7. Pause
# ---------------------------------------------------------------------------
echo ""
echo "=================================================================="
echo " POST /api/v1/replay/jobs/$JOB_ID/pause  — PAUSED"
echo "=================================================================="
curl -s -X POST "$BASE_URL/api/v1/replay/jobs/$JOB_ID/pause" | jq '{job_id, status, events_published}'
sleep 1

# ---------------------------------------------------------------------------
# 8. Resume
# ---------------------------------------------------------------------------
echo ""
echo "=================================================================="
echo " POST /api/v1/replay/jobs/$JOB_ID/resume  — RUNNING"
echo "=================================================================="
curl -s -X POST "$BASE_URL/api/v1/replay/jobs/$JOB_ID/resume" | jq '{job_id, status, events_published}'

# ---------------------------------------------------------------------------
# 9. Poll until completion
# ---------------------------------------------------------------------------
echo ""
echo "=================================================================="
echo " Polling until job completes …"
echo "=================================================================="
for i in $(seq 1 60); do
    STATUS=$(curl -s "$BASE_URL/api/v1/replay/jobs/$JOB_ID" | jq -r .status)
    EVENTS=$(curl -s "$BASE_URL/api/v1/replay/jobs/$JOB_ID" | jq -r .events_published)
    echo "  [$i] status=$STATUS  events_published=$EVENTS"
    if [[ "$STATUS" == "COMPLETED" || "$STATUS" == "FAILED" ]]; then
        break
    fi
    sleep 2
done

# ---------------------------------------------------------------------------
# 10. Final summary
# ---------------------------------------------------------------------------
echo ""
echo "=================================================================="
echo " Final job state"
echo "=================================================================="
curl -s "$BASE_URL/api/v1/replay/jobs/$JOB_ID" | jq .

echo ""
echo "==> Demo complete."
