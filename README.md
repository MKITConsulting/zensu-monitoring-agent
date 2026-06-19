# zensu-agent

[![License: FSL-1.1-Apache-2.0](https://img.shields.io/badge/License-FSL--1.1--Apache--2.0-blue.svg)](LICENSE)

Outbound **push/heartbeat** agent that reports the runtime status of your
Kubernetes workloads to [Zensu](https://zensu.dev) — powering the
"Is everything running?" view with per-service **up / degraded / down** status
and uptime.

It is the source-available half of Zensu's split-trust model: this agent
([FSL-1.1-Apache-2.0](LICENSE)) runs in **your** cluster and only ever makes
**outbound** calls to the Zensu API.
Zensu never reaches into your network. Don't want to run it? The heartbeat API is
a documented public contract — point anything at it (see [Contract](#contract)).

## How it works

1. You annotate the workloads you want tracked.
2. The agent lists those Deployments (read-only) on an interval, and the Pods
   behind each one to total their container restart counts.
3. If [metrics-server](https://github.com/kubernetes-sigs/metrics-server) is
   installed, it also reads current per-service **CPU** (millicores) and
   **memory** (bytes) from the `metrics.k8s.io` API, summed across all
   containers of all matching Pods.
4. It POSTs each service's status to `${ZENSU_API_URL}/api/runtime/heartbeat`
   with an `X-API-Key`.

Status is derived from replica counts: all ready → `up`, some ready →
`degraded`, none ready → `down`. Each heartbeat also carries the service's
summed container `restartCount` and, when available, a `metrics` array of
typed `{key, value}` samples (`cpu_millicores`, `memory_bytes`).

**Graceful degrade.** Reading CPU/memory is best-effort and entirely optional.
On clusters without metrics-server (common for self-hosted installs), the
`metrics.k8s.io` API is simply absent: the agent logs a single warning, omits
the `metrics` array, and keeps sending heartbeats — status and `restartCount`
are unaffected. A transient metrics-server error skips metrics for that one tick
only; the heartbeat still goes out.

**Enabling CPU/memory.** metrics-server is not a Kubernetes default. Managed clusters
(GKE, AKS, k3s) usually ship it; EKS, kubeadm, and most bare-metal clusters do not. If
it is absent and you want CPU/memory (uptime needs nothing), install the standard,
free add-on:

```sh
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
```

On clusters that standardize on Prometheus instead, a Prometheus-backed source is on the
roadmap (read the customer's existing Prometheus over PromQL, no extra cluster
privilege).

## Trust / security

- **Least privilege (cluster).** The bundled RBAC grants only `get/list/watch` on
  `deployments` and `pods` (Pods are read solely to total restart counts) plus
  `get/list` on `metrics.k8s.io` `pods` (read-only CPU/memory usage; harmless if
  metrics-server is absent). The agent cannot create, update, patch, or delete
  anything — see
  [`helm/zensu-agent/templates/rbac.yaml`](helm/zensu-agent/templates/rbac.yaml).
- **Least privilege (API key).** The heartbeat endpoint accepts the dedicated
  narrow **`runtime`** scope, so mint the agent's key with *only* that scope
  (Zensu → Settings → API Keys → check **runtime**, leave read/write unchecked).
  Such a key can reach **only** `POST /api/runtime/heartbeat` — every other
  endpoint returns `403 forbidden`, so a leaked agent key cannot mutate your data.
  A broad `write` (or `admin`) key still works for backward compatibility, but
  over-privileges an ingest-only client and is discouraged.
- **Outbound-only egress.** The single network call is the heartbeat POST to the
  `ZENSU_API_URL` you configure — no inbound ports, no other destinations. See
  [`internal/agent/reporter.go`](internal/agent/reporter.go).
- **Hardened container.** Distroless `nonroot`, read-only root filesystem, all
  Linux capabilities dropped.
- **Auditable.** A few hundred lines of Go. Read it.

## Quickstart (Helm)

```bash
helm install zensu-agent ./helm/zensu-agent \
  --namespace zensu-agent --create-namespace \
  --set zensu.apiUrl=https://api.zensu.dev \
  --set zensu.productId=<your-product-uuid> \
  --set zensu.apiKey=zsk_xxx
```

Then annotate any Deployment you want reported:

```yaml
metadata:
  annotations:
    zensu.dev/service: auth-api
```

## Configuration

| Env | Required | Default | Description |
|---|---|---|---|
| `ZENSU_API_URL` | yes | — | Zensu API base URL |
| `ZENSU_API_KEY` | yes | — | API key (`zsk_...`); mint with only the `runtime` scope — see [Trust / security](#trust--security) |
| `ZENSU_PRODUCT_ID` | yes | — | Product UUID to report to |
| `ZENSU_AGENT_INTERVAL` | no | `60s` | Heartbeat cadence (Go duration) |
| `ZENSU_AGENT_NAMESPACES` | no | `default` | Comma-separated namespaces to scan |
| `ZENSU_AGENT_SOURCE` | no | `k8s-agent` | Source label attached to heartbeats |
| `ZENSU_AGENT_METRICS_ENABLED` | no | `true` | Serve the Prometheus `/metrics` endpoint (deployment mode only) |
| `ZENSU_AGENT_METRICS_ADDR` | no | `:2112` | Listen address for the `/metrics` endpoint |

Helm values mirror these under `zensu.*` / `agent.*`, and the metrics toggles
live under `metrics.*` (the metrics env vars are wired into the Deployment
automatically when `metrics.enabled=true`) — see
[`values.yaml`](helm/zensu-agent/values.yaml). Supply the API key out-of-band with
`zensu.existingSecret` (a Secret holding key `ZENSU_API_KEY`) instead of
`zensu.apiKey`.

### Deploy modes

Both modes run **inside** the cluster and authenticate via the mounted
ServiceAccount token (in-cluster config):

- `agent.mode=deployment` (default): long-running ticker.
- `agent.mode=cronjob`: one-shot per `agent.schedule` (runs the binary with
  `--once`).

Running off-cluster (the binary on a VM cron host talking to a remote cluster
via a kubeconfig) is not supported yet, and a `--probe-url` mode for
non-Kubernetes targets is planned. Until then, point your own producer at the
heartbeat [contract](#contract).

## Metrics

In **deployment** mode the agent serves a Prometheus `/metrics` endpoint (default
`:2112`) so you can monitor the agent itself. It is opt-in via Helm and runs only
in deployment mode — a `cronjob` pod is one-shot and exits before a scrape, so no
endpoint is served there.

Exposed series (plus the standard `go_*` / `process_*` collectors):

| Metric | Type | Meaning |
|---|---|---|
| `zensu_agent_heartbeat_total{result="success\|error"}` | counter | Heartbeat POSTs by outcome |
| `zensu_agent_last_success_timestamp_seconds` | gauge | Unix time of the last successful POST (alert on staleness) |
| `zensu_agent_post_duration_seconds` | histogram | Heartbeat POST latency |
| `zensu_agent_services_reported` | gauge | Services in the last successful batch |

Enable scraping one of two ways:

```bash
# Prometheus Operator (kube-prometheus-stack): create a ServiceMonitor
helm upgrade ... \
  --set metrics.serviceMonitor.enabled=true \
  --set metrics.serviceMonitor.interval=30s
# match your stack's discovery label, e.g.:
#   --set metrics.serviceMonitor.additionalLabels.release=kube-prometheus-stack

# Non-operator Prometheus: add prometheus.io scrape annotations to the pod
helm upgrade ... --set metrics.podAnnotations=true
```

Disable entirely with `--set metrics.enabled=false`. A useful staleness alert is
`time() - zensu_agent_last_success_timestamp_seconds > 300` (no successful
heartbeat in 5 minutes). Note that the deployment's readiness probe targets
`/metrics`, so `metrics.enabled=false` leaves the agent with no readiness probe
(it has no other health surface).

`/metrics` is unauthenticated (standard for Prometheus) and binds all interfaces,
exposing only counters/gauges/histograms — no API key, product ID, or heartbeat
payloads. To restrict which pods may scrape it, enable the opt-in NetworkPolicy
and set `metrics.networkPolicy.from` to standard NetworkPolicy peers:

```yaml
metrics:
  networkPolicy:
    enabled: true
    from:
      - namespaceSelector:
          matchLabels:
            kubernetes.io/metadata.name: monitoring
```

Two cautions: with an empty `from` the policy restricts only the port (not the
source), so set `from` to actually scope who may scrape. And since the readiness
probe is a kubelet-issued HTTP check (originating from the node, not a pod), a
`from` that excludes the node can flap the pod to NotReady — allow the node/kubelet
source when you restrict ingress.

## Contract

The agent only needs to POST this shape — so you can write your own producer
(a CI step, a sidecar, a one-line `curl`) instead of running it:

```
POST {ZENSU_API_URL}/api/runtime/heartbeat
X-API-Key: zsk_...
Content-Type: application/json

{
  "productId": "<uuid>",
  "source": "k8s-agent",
  "services": [
    { "slug": "auth-api", "name": "Auth API", "status": "up",
      "readyReplicas": 3, "desiredReplicas": 3, "restartCount": 0,
      "intervalSeconds": 60,
      "metrics": [
        { "key": "cpu_millicores", "value": 1234 },
        { "key": "memory_bytes", "value": 530000000 }
      ] }
  ]
}
```

`status` is one of `up`, `degraded`, `down`. Services are auto-registered by
`productId` + `slug` on first heartbeat; omitting `name` preserves a previously
stored name.

`metrics` is optional and may be omitted entirely (e.g. no metrics-server). Each
entry is a typed `{key, value}` sample where `value` is a JSON number; the
backend recognizes `cpu_millicores` and `memory_bytes` today and silently skips
any unknown key, so producers can add samples without coordinating a backend
change.

## Build from source

```bash
go build ./cmd/zensu-agent
go test ./...
docker build -t zensu-agent .
```

## License

[FSL-1.1-Apache-2.0](LICENSE) © Zensu — source-available under the Functional
Source License, converting to Apache-2.0 two years after each release (same
license as the [zensu-claude-code](https://github.com/MKITConsulting/zensu-claude-code) plugin).
