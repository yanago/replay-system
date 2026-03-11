# Kubernetes Deployment Guide

Deploy the Replay System on a local **minikube** or **kind** cluster.

---

## Prerequisites

| Tool | Version |
|------|---------|
| Docker | ≥ 24 |
| minikube **or** kind | latest |
| kubectl | ≥ 1.28 |
| Java | 21 |
| Maven | 3.9 |

---

## Quick Start (minikube)

```bash
# 1. Start minikube (4 GB RAM recommended)
minikube start --memory=4096 --cpus=2

# 2. Deploy everything in one command
./scripts/k8s-deploy.sh

# 3. Get the API URL
minikube service replay-api -n replay-system --url
```

## Quick Start (kind)

```bash
# 1. Create a kind cluster
kind create cluster --name replay

# 2. Deploy (loads image directly into kind)
./scripts/k8s-deploy.sh --kind

# 3. Forward the API port
kubectl port-forward -n replay-system svc/replay-api 8080:8080
```

---

## Script Flags

```
./scripts/k8s-deploy.sh [OPTIONS]

  --kind                    Use kind instead of minikube
  --skip-build              Skip Maven + Docker build (image must already exist)
  --postgres-user   USER    Postgres username  (default: replay_user)
  --postgres-password PASS  Postgres password  (default: changeme)
```

---

## Manifest Structure

```
k8s/
├── kustomization.yaml       # Kustomize entry point (kubectl apply -k k8s/)
├── namespace.yaml           # replay-system namespace
├── configmap.yaml           # HTTP_PORT, POSTGRES_URL, KAFKA_BOOTSTRAP_SERVERS, …
├── secret.yaml              # Template — real secret created by k8s-deploy.sh
│
├── postgres/
│   ├── pvc.yaml             # 5 Gi ReadWriteOnce volume
│   ├── deployment.yaml      # postgres:16-alpine, Recreate strategy
│   └── service.yaml         # ClusterIP :5432
│
├── kafka/
│   ├── deployment.yaml      # bitnami/kafka:3.7 KRaft (no Zookeeper)
│   └── service.yaml         # ClusterIP :9092
│
├── data/
│   ├── pvc.yaml             # 2 Gi shared volume for Iceberg warehouse
│   └── init-job.yaml        # One-shot DataGenerator Job (runs before API)
│
└── api/
    ├── deployment.yaml      # replay-system:latest, mounts iceberg-data-pvc
    └── service.yaml         # NodePort :30080 (minikube) / port-forward (kind)
```

---

## Smoke Tests

After deployment, run these to verify the stack is healthy:

```bash
BASE_URL=$(minikube service replay-api -n replay-system --url)
# or: BASE_URL=http://localhost:8080  (kind + port-forward)

# Health check
curl -sf "$BASE_URL/health" | jq .

# Create a replay job
JOB=$(curl -sf -X POST "$BASE_URL/api/v1/replay/jobs" \
  -H 'Content-Type: application/json' \
  -d '{
    "source_table":     "security_events",
    "target_topic":     "replay-output",
    "from_time":        "2024-01-01T00:00:00Z",
    "to_time":          "2024-01-02T00:00:00Z",
    "speed_multiplier": 1.0
  }')
JOB_ID=$(echo "$JOB" | jq -r '.id')
echo "Created job: $JOB_ID"

# Poll status
curl -sf "$BASE_URL/api/v1/replay/jobs/$JOB_ID/status" | jq .

# Fetch metrics
curl -sf "$BASE_URL/api/v1/replay/jobs/$JOB_ID/metrics" | jq .

# Pause / resume
curl -sf -X POST "$BASE_URL/api/v1/replay/jobs/$JOB_ID/pause"
curl -sf -X POST "$BASE_URL/api/v1/replay/jobs/$JOB_ID/resume"

# Cancel
curl -sf -X POST "$BASE_URL/api/v1/replay/jobs/$JOB_ID/cancel"
```

---

## Useful kubectl Commands

```bash
# Watch all pods
kubectl -n replay-system get pods -w

# API server logs
kubectl -n replay-system logs -f deployment/replay-api

# Kafka logs
kubectl -n replay-system logs -f deployment/kafka

# Data-init job logs
kubectl -n replay-system logs job/data-init

# Describe a failing pod
kubectl -n replay-system describe pod <pod-name>

# Exec into the API container
kubectl -n replay-system exec -it deployment/replay-api -- sh
```

---

## Teardown

```bash
# Delete all resources in the namespace
kubectl delete namespace replay-system

# minikube: stop the cluster
minikube stop

# kind: delete the cluster
kind delete cluster --name replay
```
