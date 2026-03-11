#!/usr/bin/env bash
# =============================================================================
# k8s-deploy.sh  —  One-script end-to-end deployment on minikube or kind
# =============================================================================
# Usage:
#   ./scripts/k8s-deploy.sh [--kind] [--skip-build] [--postgres-user USER] [--postgres-password PASS]
#
# Flags:
#   --kind                Use kind instead of minikube (default: minikube)
#   --skip-build          Skip Docker image build (image must already exist in cluster)
#   --postgres-user       Postgres username (default: replay_user)
#   --postgres-password   Postgres password (default: changeme)
#
# Prerequisites:
#   minikube OR kind + kubectl, kustomize, docker, mvn, java 21
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── Defaults ─────────────────────────────────────────────────────────────────
CLUSTER_DRIVER="minikube"
SKIP_BUILD=false
POSTGRES_USER="replay_user"
POSTGRES_PASSWORD="changeme"

# ── Parse args ────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --kind)               CLUSTER_DRIVER="kind" ;;
    --skip-build)         SKIP_BUILD=true ;;
    --postgres-user)      POSTGRES_USER="$2"; shift ;;
    --postgres-password)  POSTGRES_PASSWORD="$2"; shift ;;
    *) echo "Unknown flag: $1"; exit 1 ;;
  esac
  shift
done

cd "$REPO_ROOT"

# ── Step 1: Build fat JAR ─────────────────────────────────────────────────────
if [[ "$SKIP_BUILD" == false ]]; then
  echo "==> [1/6] Building JAR …"
  mvn -q package -DskipTests
  echo "    JAR built."
else
  echo "==> [1/6] Skipping JAR build."
fi

# ── Step 2: Build Docker image ────────────────────────────────────────────────
if [[ "$SKIP_BUILD" == false ]]; then
  echo "==> [2/6] Building Docker image replay-system:latest …"
  if [[ "$CLUSTER_DRIVER" == "minikube" ]]; then
    # Build directly into minikube's Docker daemon so no registry push is needed
    eval "$(minikube docker-env)"
  fi
  docker build -t replay-system:latest .
  if [[ "$CLUSTER_DRIVER" == "kind" ]]; then
    echo "    Loading image into kind cluster …"
    kind load docker-image replay-system:latest
  fi
  echo "    Image ready."
else
  echo "==> [2/6] Skipping Docker build."
fi

# ── Step 3: Create namespace ──────────────────────────────────────────────────
echo "==> [3/6] Applying namespace …"
kubectl apply -f k8s/namespace.yaml

# ── Step 4: Create secret (idempotent) ────────────────────────────────────────
echo "==> [4/6] Creating secret replay-secret …"
kubectl -n replay-system create secret generic replay-secret \
  --from-literal=POSTGRES_USER="$POSTGRES_USER" \
  --from-literal=POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  --dry-run=client -o yaml | kubectl apply -f -

# ── Step 5: Apply all manifests via Kustomize ─────────────────────────────────
echo "==> [5/6] Applying Kubernetes manifests (kustomize) …"
# Skip secret.yaml from kustomize — we created it manually above with real values
kubectl apply -k k8s/ --prune \
  -l app.kubernetes.io/part-of=replay-system 2>/dev/null || \
  kubectl apply -k k8s/

# ── Step 6: Wait for rollouts ────────────────────────────────────────────────
echo "==> [6/6] Waiting for deployments to become ready …"
kubectl -n replay-system rollout status deployment/postgres   --timeout=120s
kubectl -n replay-system rollout status deployment/kafka      --timeout=120s

echo "    Waiting for data-init Job to complete …"
kubectl -n replay-system wait --for=condition=complete job/data-init --timeout=180s || \
  echo "    WARNING: data-init job did not complete in time — check: kubectl -n replay-system logs job/data-init"

kubectl -n replay-system rollout status deployment/replay-api --timeout=120s

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo "========================================================="
echo "  Deployment complete!"
echo "========================================================="

if [[ "$CLUSTER_DRIVER" == "minikube" ]]; then
  API_URL="$(minikube service replay-api -n replay-system --url 2>/dev/null || echo 'http://$(minikube ip):30080')"
else
  API_URL="http://localhost:30080"
  echo "  (for kind, run: kubectl port-forward -n replay-system svc/replay-api 8080:8080)"
fi

echo ""
echo "  API base URL : $API_URL"
echo ""
echo "  Quick smoke test:"
echo "    curl $API_URL/health"
echo "    curl -s -X POST $API_URL/api/v1/replay/jobs \\"
echo "         -H 'Content-Type: application/json' \\"
echo "         -d '{\"source_table\":\"security_events\",\"target_topic\":\"replay-output\",\"from_time\":\"2024-01-01T00:00:00Z\",\"to_time\":\"2024-01-02T00:00:00Z\",\"speed_multiplier\":1.0}'"
echo ""
echo "  Useful commands:"
echo "    kubectl -n replay-system get pods"
echo "    kubectl -n replay-system logs -f deployment/replay-api"
echo "    kubectl -n replay-system logs -f deployment/kafka"
echo "    kubectl -n replay-system logs job/data-init"
echo "========================================================="
