#!/usr/bin/env bash
# End-to-end test: run the real zensu-agent image inside a real Kubernetes
# cluster (kind), pointed at an in-cluster mock receiver, and assert that a
# heartbeat POST arrives for an annotated Deployment — exercising the actual
# shipping artifacts (image, Helm chart, RBAC, in-cluster config, client-go
# against a real API server, and the outbound POST).
#
# Usage: KIND=/path/to/kind bash e2e/kind-e2e.sh   (KIND defaults to `kind`)
set -euo pipefail

CLUSTER="${CLUSTER:-zensu-agent-e2e}"
NS="${NS:-zensu-agent-e2e}"
IMAGE="zensu-agent:e2e"
KIND="${KIND:-kind}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

cleanup() { "$KIND" delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true; }
# Cluster is torn down on success (end of script) and on a heartbeat-assertion
# failure; it is intentionally left running on an agent-startup failure so the
# pod can be inspected.

echo "==> [1/7] create kind cluster ($CLUSTER)"
"$KIND" delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
"$KIND" create cluster --name "$CLUSTER" --wait 120s

echo "==> [2/7] build + load agent image"
docker build -t "$IMAGE" "$ROOT"
"$KIND" load docker-image "$IMAGE" --name "$CLUSTER"

echo "==> [3/7] namespace"
kubectl create namespace "$NS"

echo "==> [4/7] mock receiver (logs every request body to stdout)"
kubectl -n "$NS" apply -f - <<'YAML'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mock-backend
spec:
  replicas: 1
  selector: { matchLabels: { app: mock-backend } }
  template:
    metadata: { labels: { app: mock-backend } }
    spec:
      containers:
        - name: echo
          image: mendhak/http-https-echo:31
          env:
            - { name: HTTP_PORT, value: "8080" }
          ports: [ { containerPort: 8080 } ]
---
apiVersion: v1
kind: Service
metadata:
  name: mock-backend
spec:
  selector: { app: mock-backend }
  ports: [ { port: 80, targetPort: 8080 } ]
YAML
kubectl -n "$NS" rollout status deploy/mock-backend --timeout=120s

echo "==> [5/7] annotated workload under test (demo-api, 2 replicas)"
kubectl -n "$NS" apply -f - <<'YAML'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: demo-api
  annotations:
    zensu.dev/service: demo-api
spec:
  replicas: 2
  selector: { matchLabels: { app: demo-api } }
  template:
    metadata: { labels: { app: demo-api } }
    spec:
      containers:
        - name: app
          image: registry.k8s.io/pause:3.9
YAML
kubectl -n "$NS" rollout status deploy/demo-api --timeout=120s

echo "==> [6/7] install agent via Helm (deployment mode, 5s interval)"
helm install zensu-agent "$ROOT/helm/zensu-agent" \
  --namespace "$NS" \
  --set fullnameOverride=zensu-agent \
  --set image.repository=zensu-agent \
  --set image.tag=e2e \
  --set image.pullPolicy=IfNotPresent \
  --set zensu.apiUrl=http://mock-backend \
  --set zensu.productId=00000000-0000-0000-0000-000000000000 \
  --set zensu.apiKey=zsk_e2e \
  --set agent.mode=deployment \
  --set agent.intervalSeconds=5 \
  --set "agent.namespaces[0]=$NS"
if ! kubectl -n "$NS" rollout status deploy/zensu-agent --timeout=90s; then
  echo "--- agent did not become ready; diagnostics (cluster left running) ---"
  kubectl -n "$NS" get pods -o wide
  POD="$(kubectl -n "$NS" get pod -l app.kubernetes.io/name=zensu-agent -o name | head -1)"
  kubectl -n "$NS" describe "$POD" | sed -n '/Events:/,$p'
  kubectl -n "$NS" logs "$POD" --tail=40 2>&1 || true
  kubectl -n "$NS" logs "$POD" --previous --tail=40 2>&1 || true
  exit 1
fi

echo "==> [7/7] wait for a heartbeat to reach the mock receiver"
ok=false
for _ in $(seq 1 30); do
  if kubectl -n "$NS" logs deploy/mock-backend 2>/dev/null | grep -q 'runtime/heartbeat'; then ok=true; break; fi
  sleep 5
done

if ! $ok; then
  echo "FAIL: no heartbeat POST observed within timeout"
  echo "--- agent logs ---"; kubectl -n "$NS" logs deploy/zensu-agent --tail=40 || true
  exit 1
fi

LOGS="$(kubectl -n "$NS" logs deploy/mock-backend)"
echo "--- captured request (mock receiver) ---"
echo "$LOGS" | grep -A30 'runtime/heartbeat' | head -40

fail=0
grep -q 'runtime/heartbeat' <<<"$LOGS" || { echo "MISS: heartbeat path"; fail=1; }
grep -q 'demo-api'          <<<"$LOGS" || { echo "MISS: demo-api slug"; fail=1; }
grep -q '"up"'              <<<"$LOGS" || { echo "MISS: status up"; fail=1; }
grep -q 'restartCount'      <<<"$LOGS" || { echo "MISS: restartCount field"; fail=1; }
[ "$fail" -eq 0 ] || { echo "FAIL: heartbeat payload assertions"; exit 1; }

echo "PASS: agent posted a heartbeat for demo-api (up, with restartCount) from inside the cluster"
cleanup
echo "==> cluster torn down"
