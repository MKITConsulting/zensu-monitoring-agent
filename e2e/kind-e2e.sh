#!/usr/bin/env bash
# End-to-end test: run the real zensu-monitoring-agent image inside a real Kubernetes
# cluster (kind), pointed at an in-cluster mock receiver, and assert that a
# heartbeat POST arrives for an annotated Deployment — exercising the actual
# shipping artifacts (image, Helm chart, RBAC, in-cluster config, client-go
# against a real API server, and the outbound POST).
#
# It also installs metrics-server into the kind cluster and asserts the agent
# reads per-service CPU/memory from metrics.k8s.io and includes a `metrics`
# array (cpu_millicores / memory_bytes) in the heartbeat. metrics-server can be
# slow to publish its first scrape in kind; if it does not become usable within
# the budget, the script falls back to asserting the *graceful-degrade* path
# deterministically (heartbeat still posts, just without `metrics`) — the
# behaviour required on self-hosted clusters that lack metrics-server. Either
# way the heartbeat itself must arrive. Set REQUIRE_METRICS=1 to force the
# metrics assertion (no degrade fallback).
#
# Usage: KIND=/path/to/kind bash e2e/kind-e2e.sh   (KIND defaults to `kind`)
set -euo pipefail

CLUSTER="${CLUSTER:-zensu-monitoring-agent-e2e}"
NS="${NS:-zensu-monitoring-agent-e2e}"
IMAGE="zensu-monitoring-agent:e2e"
KIND="${KIND:-kind}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# Where to pull the metrics-server manifest from, and how long to wait for it to
# publish usable metrics before falling back to the degrade assertion.
METRICS_SERVER_URL="${METRICS_SERVER_URL:-https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml}"
METRICS_READY_BUDGET="${METRICS_READY_BUDGET:-150}"
REQUIRE_METRICS="${REQUIRE_METRICS:-0}"

cleanup() { "$KIND" delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true; }
# Cluster is torn down on success (end of script) and on a heartbeat-assertion
# failure; it is intentionally left running on an agent-startup failure so the
# pod can be inspected.

echo "==> [1/8] create kind cluster ($CLUSTER)"
"$KIND" delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
"$KIND" create cluster --name "$CLUSTER" --wait 120s

echo "==> [2/8] build + load agent image"
docker build -t "$IMAGE" "$ROOT"
"$KIND" load docker-image "$IMAGE" --name "$CLUSTER"

echo "==> [3/8] namespace"
kubectl create namespace "$NS"

# Install metrics-server so the agent can read metrics.k8s.io. kubelet in kind
# serves its summary API with a self-signed cert, so metrics-server needs
# --kubelet-insecure-tls; we patch that flag onto the upstream manifest. This is
# best-effort: METRICS_AVAILABLE drives whether we assert metrics or degrade.
METRICS_AVAILABLE=0
echo "==> [4/8] install metrics-server (best-effort; --kubelet-insecure-tls patch)"
if kubectl apply -f "$METRICS_SERVER_URL"; then
  # Append the insecure-tls flag to the metrics-server container args.
  kubectl -n kube-system patch deployment metrics-server --type=json \
    -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]' || true
  # Wait for the Deployment to roll out, then for `kubectl top` to actually
  # return data (rollout-ready precedes the first usable scrape).
  if kubectl -n kube-system rollout status deploy/metrics-server --timeout=120s; then
    echo "    waiting up to ${METRICS_READY_BUDGET}s for metrics-server to publish a scrape..."
    deadline=$(( $(date +%s) + METRICS_READY_BUDGET ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
      if kubectl top pods -n kube-system >/dev/null 2>&1; then
        METRICS_AVAILABLE=1
        echo "    metrics-server is serving metrics"
        break
      fi
      sleep 5
    done
  fi
fi
if [ "$METRICS_AVAILABLE" -ne 1 ]; then
  if [ "$REQUIRE_METRICS" = "1" ]; then
    echo "FAIL: REQUIRE_METRICS=1 but metrics-server never published metrics"; exit 1
  fi
  echo "    metrics-server not usable within budget; will assert graceful-degrade path"
fi

echo "==> [5/8] mock receiver (logs every request body to stdout)"
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

echo "==> [6/8] annotated workload under test (demo-api, 2 replicas)"
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

echo "==> [7/8] install agent via Helm (deployment mode, 5s interval)"
helm install zensu-monitoring-agent "$ROOT/helm/zensu-monitoring-agent" \
  --namespace "$NS" \
  --set fullnameOverride=zensu-monitoring-agent \
  --set image.repository=zensu-monitoring-agent \
  --set image.tag=e2e \
  --set image.pullPolicy=IfNotPresent \
  --set zensu.apiUrl=http://mock-backend \
  --set zensu.productId=00000000-0000-0000-0000-000000000000 \
  --set zensu.apiKey=zsk_e2e \
  --set agent.mode=deployment \
  --set agent.intervalSeconds=5 \
  --set "agent.namespaces[0]=$NS"
if ! kubectl -n "$NS" rollout status deploy/zensu-monitoring-agent --timeout=90s; then
  echo "--- agent did not become ready; diagnostics (cluster left running) ---"
  kubectl -n "$NS" get pods -o wide
  POD="$(kubectl -n "$NS" get pod -l app.kubernetes.io/name=zensu-monitoring-agent -o name | head -1)"
  kubectl -n "$NS" describe "$POD" | sed -n '/Events:/,$p'
  kubectl -n "$NS" logs "$POD" --tail=40 2>&1 || true
  kubectl -n "$NS" logs "$POD" --previous --tail=40 2>&1 || true
  exit 1
fi

echo "==> [8/8] wait for a heartbeat to reach the mock receiver"
# When metrics-server is usable we additionally wait for a heartbeat that
# actually carries the metrics array — the agent's first tick can fire before
# metrics-server completes its first scrape of the demo-api pods.
ok=false
for _ in $(seq 1 36); do
  RAW="$(kubectl -n "$NS" logs deploy/mock-backend 2>/dev/null || true)"
  if grep -q 'runtime/heartbeat' <<<"$RAW"; then
    if [ "$METRICS_AVAILABLE" -ne 1 ] || grep -q '"metrics"' <<<"$RAW"; then ok=true; break; fi
  fi
  sleep 5
done

if ! $ok; then
  echo "FAIL: no heartbeat POST observed within timeout"
  echo "--- agent logs ---"; kubectl -n "$NS" logs deploy/zensu-monitoring-agent --tail=40 || true
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

if [ "$METRICS_AVAILABLE" -eq 1 ]; then
  # metrics-server was serving: assert the agent read it and shipped a metrics
  # array carrying both known keys.
  grep -q '"metrics"'       <<<"$LOGS" || { echo "MISS: metrics array";         fail=1; }
  grep -q 'cpu_millicores'  <<<"$LOGS" || { echo "MISS: cpu_millicores metric"; fail=1; }
  grep -q 'memory_bytes'    <<<"$LOGS" || { echo "MISS: memory_bytes metric";   fail=1; }
  RESULT="up, restartCount, and CPU/memory metrics"
else
  # Graceful-degrade path: metrics-server absent/not-ready -> heartbeat must
  # still post, and must NOT carry a metrics array (no partial/garbage metrics).
  if grep -q '"metrics"' <<<"$LOGS"; then echo "MISS: metrics present despite no usable metrics-server"; fail=1; fi
  RESULT="up and restartCount (graceful-degrade: no metrics-server, no metrics array)"
fi
[ "$fail" -eq 0 ] || { echo "FAIL: heartbeat payload assertions"; exit 1; }

echo "PASS: agent posted a heartbeat for demo-api ($RESULT) from inside the cluster"
cleanup
echo "==> cluster torn down"
