#!/usr/bin/env bash
# =============================================================================
# deploy.sh  —  One-command build + deploy of the Replay System
# =============================================================================
# Usage:
#   ./scripts/deploy.sh
#
# Required environment variables (PostgreSQL — optional, falls back to in-memory):
#   POSTGRES_URL       e.g. jdbc:postgresql://localhost:5432/replay
#   POSTGRES_USER      e.g. replay_user
#   POSTGRES_PASSWORD  (your password)
#
# Optional environment variables:
#   HTTP_PORT          HTTP listen port          (default: 8080)
#   DATA_LAKE_TABLE    Iceberg table path        (default: ./data/security_events)
#   JAVA_OPTS          Extra JVM flags
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$REPO_ROOT"

# ---------------------------------------------------------------------------
# 1. Build
# ---------------------------------------------------------------------------
echo "==> Building Replay System …"
mvn -q package -DskipTests
JAR="$(ls target/replay-system-*.jar | grep -v original | head -1)"
echo "==> JAR ready: $JAR"

# ---------------------------------------------------------------------------
# 2. Environment summary
# ---------------------------------------------------------------------------
HTTP_PORT="${HTTP_PORT:-8080}"

echo ""
echo "==> Configuration:"
echo "    HTTP_PORT       = $HTTP_PORT"
if [[ -n "${POSTGRES_URL:-}" ]]; then
    echo "    POSTGRES_URL    = $POSTGRES_URL"
    echo "    POSTGRES_USER   = ${POSTGRES_USER:-<not set>}"
    echo "    Storage backend = PostgreSQL"
else
    echo "    Storage backend = in-memory (set POSTGRES_URL for persistence)"
fi
echo ""

# ---------------------------------------------------------------------------
# 3. Launch
# ---------------------------------------------------------------------------
echo "==> Starting Replay System on port $HTTP_PORT …"
exec java \
    ${JAVA_OPTS:-} \
    -Xss512k \
    -jar "$JAR"
