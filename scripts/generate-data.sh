#!/usr/bin/env bash
# =============================================================================
# generate-data.sh  —  Build the fat JAR (if needed) and run DataGenerator
# =============================================================================
# Usage:
#   ./scripts/generate-data.sh [output-directory]
#
# Default output directory: data/security_events  (relative to repo root)
#
# The generator creates an Apache Iceberg table with 55 000+ synthetic security
# events spanning January 2024, partitioned by day.  Top-10 customers receive
# ~60 % of traffic (Zipf distribution).
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DATA_DIR="${1:-$REPO_ROOT/data/security_events}"

cd "$REPO_ROOT"

# ---------------------------------------------------------------------------
# 1. Build the fat JAR (skip tests to keep it fast)
# ---------------------------------------------------------------------------
JAR="$(ls target/replay-system-*.jar 2>/dev/null | grep -v original | head -1 || true)"
if [[ -z "$JAR" ]]; then
    echo "==> Building fat JAR …"
    mvn -q package -DskipTests
    JAR="$(ls target/replay-system-*.jar | grep -v original | head -1)"
fi
echo "==> Using JAR: $JAR"

# ---------------------------------------------------------------------------
# 2. Generate the data
# ---------------------------------------------------------------------------
echo "==> Generating data to: $DATA_DIR"
java -cp "$JAR" com.example.replay.tools.DataGenerator "$DATA_DIR"

echo ""
echo "==> Done!  Table location: $DATA_DIR"
echo "    Pass this path as source_table when submitting a replay job:"
echo ""
echo '    curl -s -X POST http://localhost:8080/api/v1/replay/jobs \'
echo "         -H 'Content-Type: application/json' \\"
echo "         -d '{\"source_table\":\"$DATA_DIR\",\"target_topic\":\"replay-out\",\"from_time\":\"2024-01-01T00:00:00Z\",\"to_time\":\"2024-01-31T23:59:59Z\"}'"
