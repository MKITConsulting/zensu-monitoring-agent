# zensu-monitoring-agent

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
3. It reads current per-service **CPU** (millicores) and **memory** (bytes) from
   a resource source — [metrics-server](https://github.com/kubernetes-sigs/metrics-server)
   or the Prometheus exposition of an OpenTelemetry Collector — summed across
   all matching Pods. See [Resource metric sources](#resource-metric-sources).
4. It POSTs each service's status to `${ZENSU_API_URL}/api/runtime/heartbeat`
   with an `X-API-Key`.

Status is derived from replica counts: all ready → `up`, some ready →
`degraded`, none ready → `down`. Each heartbeat also carries the service's
summed container `restartCount` and, when available, a `metrics` array of
typed `{key, value}` samples (`cpu_millicores`, `memory_bytes`).

**Graceful degrade.** Reading CPU/memory is best-effort and entirely optional,
and it degrades per source. On a cluster without metrics-server the
`metrics.k8s.io` API is simply absent: the agent logs one warning and, if a
collector exposition is configured, reads CPU/memory from there instead — see
[Resource metric sources](#resource-metric-sources). With no configured
fallback it omits the `metrics` array and keeps sending heartbeats. Either way
status and `restartCount` are unaffected, and a transient failure of whichever
source is in use costs metrics for that one tick only.

## Resource metric sources

Uptime never depends on any of this — it comes from Deployment status alone. CPU and
memory need a source, and there are two.

**metrics-server** is the default, but it is not a Kubernetes default. Managed clusters
(GKE, AKS, k3s) usually ship it; EKS, kubeadm, and most bare-metal clusters do not. If it
is absent and you want CPU/memory, install the standard, free add-on:

```sh
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
```

**A collector exposition** is the alternative for clusters that already run an
OpenTelemetry Collector. Point the agent at the collector's Prometheus exposition and it
reads CPU/memory from there — no metrics-server, no extra cluster privilege, one more
outbound HTTP call:

```bash
--set resourceMetrics.scrapeUrl=http://otel-collector.observability:8889/metrics
```

`resourceMetrics.source` picks the behaviour: `auto` (default) reads metrics-server and
falls through to the exposition **only** when the cluster has no `metrics.k8s.io` API and
a scrape URL is set; `metrics-server` and `exposition` pin one source; `none` disables
resource metrics entirely. The fallthrough is one-way and logged once.

**Which metrics are read.** The agent probes the exposition for known names and takes the
first match, so no configuration is needed for the common pipelines. Whether a series is
a gauge or a counter is read from its `# TYPE` line, not configured. Unit suffixes are
ignored when matching, so `k8s_pod_memory_working_set` and
`k8s_pod_memory_working_set_bytes` are the same metric — but only one exposition family
ever wins, so a pipeline exposing both a gauge and a cumulative counter under the same
base name is never mixed into one reading.

Counters are converted to a per-second rate, which needs two consecutive scrapes of the
same series in the same process. A service therefore reports no CPU until every one of
its pods has been seen twice: the first tick after start, and the tick after a pod is
replaced or a counter resets. A partial reading is withheld rather than reported, because
a fraction of a service's real usage is worse than a gap. A cumulative **memory** family
is refused outright rather than rated — bytes per second is not a byte count.

In `agent.mode=cronjob` the process exits after one tick, so a counter never gets its
second scrape: a cronjob install whose CPU metric resolves to a counter reports memory
but never CPU. The agent warns about this at startup. Use `deployment` mode, or a gauge
CPU metric such as `k8s_pod_cpu_usage`.

| Role | Probed in order | From |
|---|---|---|
| CPU | `k8s_pod_cpu_usage`, `k8s_pod_cpu_time`, `container_cpu_usage_seconds_total` | kubeletstats receiver, then cAdvisor |
| Memory | `k8s_pod_memory_working_set`, `container_memory_working_set_bytes` | kubeletstats receiver, then cAdvisor |

For a container-scoped family the agent sums the per-container rows and skips that
pod's own pod-level row, which would otherwise count the pod twice; cAdvisor's
`container="POD"` pause rows are excluded entirely. A pod that exposes only a
pod-level row keeps it. So a `zensu` CPU figure is deliberately not the same number
as a bare `sum(container_cpu_usage_seconds_total)` over the same pods.

Set `resourceMetrics.cpuMetric` / `resourceMetrics.memoryMetric` only if your pipeline
exposes none of these. An override is expected to report CPU in cores (or cumulative
seconds) and memory in bytes.

**Telling the agent which service a sample belongs to.** Best is an explicit label whose
value is the Zensu service slug. Your Deployments already carry the
`zensu.dev/service` annotation for uptime discovery, so project it in the collector:

```yaml
processors:
  k8sattributes:
    extract:
      annotations:
        - key: zensu.dev/service
          tag_name: zensu_service
          from: pod
```

Without that label the agent falls back to matching the sample's `pod`/`k8s_pod_name` and
`namespace`/`k8s_namespace_name` labels against the Pods it already discovered, which
works with no collector configuration at all but cannot resolve a pod name that is
ambiguous across namespaces. Samples that match no tracked service are dropped. Watch
`zensu_monitoring_agent_resource_services_mapped` on the agent's own `/metrics`: a
successful scrape with zero mapped services means the attribution, not the scrape, is
what needs fixing.

**Graceful degrade applies to every source.** A failed scrape, an unreachable collector,
or an exposition without any known metric costs the metrics for that tick only — status
and `restartCount` still ship.

## Trust / security

- **Least privilege (cluster).** The bundled RBAC grants only `get/list/watch` on
  `deployments` and `pods` (Pods are read solely to total restart counts) plus
  `get/list` on `metrics.k8s.io` `pods` (read-only CPU/memory usage; harmless if
  metrics-server is absent). The agent cannot create, update, patch, or delete
  anything — see
  [`helm/zensu-monitoring-agent/templates/rbac.yaml`](helm/zensu-monitoring-agent/templates/rbac.yaml).
- **Least privilege (API key).** The heartbeat endpoint accepts the dedicated
  narrow **`runtime`** scope, so mint the agent's key with *only* that scope
  (Zensu → Settings → API Keys → check **runtime**, leave read/write unchecked).
  Such a key can reach **only** `POST /api/runtime/heartbeat` — every other
  endpoint returns `403 forbidden`, so a leaked agent key cannot mutate your data.
  A broad `write` (or `admin`) key still works for backward compatibility, but
  over-privileges an ingest-only client and is discouraged.
- **Outbound-only egress.** By default the single outbound call is the heartbeat POST to
  the `ZENSU_API_URL` you configure — no other destinations. Neither HTTP client follows
  redirects, so no endpoint can send the agent somewhere you did not configure. The one
  inbound listener is the agent's own `/metrics` port, on by default in deployment mode and
  disabled with `metrics.enabled=false`; restrict reach to it with
  `metrics.networkPolicy.enabled` **and** `metrics.networkPolicy.from`, which names the
  allowed sources — `from` is mandatory, and enabling the policy without it fails the
  render rather than deploying a policy that admits everyone. See
  [`internal/agent/reporter.go`](internal/agent/reporter.go). Setting
  `resourceMetrics.scrapeUrl` adds exactly one more outbound destination: a plain HTTP GET
  to that endpoint, once per heartbeat interval, sending no credentials. It stays exactly
  one — the scrape client refuses to follow redirects, so the endpoint cannot send the
  agent somewhere else. The bundled NetworkPolicy governs **ingress** to the agent's own
  `/metrics` port only and does not restrict egress, so an egress policy of your own is
  yours to add. The scrape sends no authentication, so the exposition is expected to be a
  cluster-internal endpoint; there is no bearer-token or client-certificate option yet, and
  a protected exposition is not supported rather than something to unauthenticate.
  Userinfo credentials in either URL are refused — see
  [Configuration](#configuration) for the exact rule. A token carried as a query
  parameter is NOT detected and would sit in the ConfigMap in clear text. Note also that the agent trusts the scrape target
  to say which service each measurement belongs to, so a compromised or misconfigured
  collector can misattribute usage. One availability note: the agent buffers a body it
  does not control up to `scrapeMaxBytes` and hands it to the parser whole, so peak
  memory is that cap times the parser's expansion factor rather than the cap itself; the
  series ceiling bounds what is retained between scrapes, not that peak. `agent.goMemLimit`
  keeps the runtime collecting under pressure; it, `resources.limits.memory` and
  `resourceMetrics.scrapeMaxBytes` are raised together, never one alone.
- **Hardened container.** Distroless `nonroot`, read-only root filesystem, all
  Linux capabilities dropped.
- **Auditable.** A few hundred lines of Go. Read it.

## Quickstart (Helm)

```bash
helm install zensu-monitoring-agent ./helm/zensu-monitoring-agent \
  --namespace zensu-monitoring-agent --create-namespace \
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
| `ZENSU_API_URL` | yes | — | Zensu API base URL. Needs an `http://` or `https://` scheme and a host; userinfo credentials (`https://user:pass@host`) are refused. Both URLs are rendered into a ConfigMap in clear text, so the chart **fails the render** before anything reaches the cluster, and the agent refuses to start as well. |
| `ZENSU_API_KEY` | yes | — | API key (`zsk_...`); mint with only the `runtime` scope — see [Trust / security](#trust--security) |
| `ZENSU_PRODUCT_ID` | yes | — | Product UUID to report to |
| `ZENSU_MONITORING_AGENT_INTERVAL` | no | `60s` | Heartbeat cadence (Go duration) |
| `ZENSU_MONITORING_AGENT_NAMESPACES` | no | `default` | Comma-separated namespaces to scan |
| `ZENSU_MONITORING_AGENT_SOURCE` | no | `k8s-agent` | Source label attached to heartbeats |
| `ZENSU_MONITORING_AGENT_METRICS_ENABLED` | no | `true` | Serve the Prometheus `/metrics` endpoint (deployment mode only) |
| `ZENSU_MONITORING_AGENT_METRICS_ADDR` | no | `:2112` | Listen address for the `/metrics` endpoint |
| `ZENSU_MONITORING_AGENT_RESOURCE_SOURCE` | no | `auto` | Where CPU/memory come from: `auto`, `metrics-server`, `exposition`, `none` — see [Resource metric sources](#resource-metric-sources) |
| `ZENSU_MONITORING_AGENT_SCRAPE_URL` | no | — | Prometheus exposition to scrape, e.g. an OpenTelemetry Collector. Held to the same rule as `ZENSU_API_URL`, and checked whenever it is set — the ConfigMap key is written whatever `RESOURCE_SOURCE` says, so a credential in an unused scrape URL is exposed just the same. |
| `ZENSU_MONITORING_AGENT_SCRAPE_SLUG_LABEL` | no | `zensu_service` | Label on the scraped series whose value is the service slug |
| `ZENSU_MONITORING_AGENT_SCRAPE_CPU_METRIC` | no | — | Override the auto-detected CPU metric name |
| `ZENSU_MONITORING_AGENT_SCRAPE_MEMORY_METRIC` | no | — | Override the auto-detected memory metric name |
| `ZENSU_MONITORING_AGENT_SCRAPE_TIMEOUT` | no | `10s` | Per-scrape HTTP timeout (Go duration), covering the whole request |
| `ZENSU_MONITORING_AGENT_SCRAPE_MAX_BYTES` | no | `8388608` (8 MiB) | Cap on the exposition body in bytes. Values above 16 MiB fall back to the default. A scrape carrying more than 50 000 series **of the families the agent matches** is refused regardless; families it does not match are dropped before they count. Raising this cap is only safe together with `resources.limits.memory` and `agent.goMemLimit`. |

`METRICS_*` configures the agent's **own** endpoint; `RESOURCE_SOURCE` / `SCRAPE_*`
configure where it **reads** service CPU/memory. The two are unrelated.

Helm values mirror these under `zensu.*` / `agent.*` / `resourceMetrics.*`, and the
metrics toggles live under `metrics.*` (the metrics env vars are wired into the
Deployment automatically when `metrics.enabled=true`) — see
[`values.yaml`](helm/zensu-monitoring-agent/values.yaml). Supply the API key out-of-band with
`zensu.existingSecret` (a Secret holding key `ZENSU_API_KEY`) instead of
`zensu.apiKey`.

### Deploy modes

Both modes run **inside** the cluster and authenticate via the mounted
ServiceAccount token (in-cluster config):

- `agent.mode=deployment` (default): long-running ticker.
- `agent.mode=cronjob`: one-shot per `agent.schedule` (runs the binary with
  `--once`).
- `agent.goMemLimit` becomes the container's `GOMEMLIMIT`, a soft heap ceiling that
  keeps the Go runtime collecting under pressure instead of being OOM-killed. It
  sits below `resources.limits.memory` on purpose. The default, the accepted syntax
  and the reason for both are on the value itself in
  [`values.yaml`](helm/zensu-monitoring-agent/values.yaml); the chart refuses a
  value the Go runtime would reject rather than crash-looping the pod.

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
| `zensu_monitoring_agent_heartbeat_total{result="success\|error"}` | counter | Heartbeat POSTs by outcome |
| `zensu_monitoring_agent_last_success_timestamp_seconds` | gauge | Unix time of the last successful POST (alert on staleness) |
| `zensu_monitoring_agent_post_duration_seconds` | histogram | Heartbeat POST latency |
| `zensu_monitoring_agent_services_reported` | gauge | Services in the last successful batch |
| `zensu_monitoring_agent_scrape_total{result="success\|error"}` | counter | Collector-exposition scrapes by outcome (exposition source only) |
| `zensu_monitoring_agent_scrape_duration_seconds` | histogram | Collector-exposition scrape latency |
| `zensu_monitoring_agent_resource_services_mapped` | gauge | Services that received CPU/memory in the last tick, whatever the source |

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
`time() - zensu_monitoring_agent_last_success_timestamp_seconds > 300` (no successful
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

Two cautions: `from` is mandatory once the policy is enabled — the chart refuses
to render with an empty list rather than shipping a policy that scopes the port
but not the source. **This is breaking for an existing release** that runs
`metrics.networkPolicy.enabled=true` with the default empty `from`: the next
`helm upgrade` fails until you supply `from` (that release was admitting every
source anyway, which is what the guard exists to stop). And since the readiness
probe is a kubelet-issued HTTP check (originating from the node, not a pod), a
`from` that excludes the node can flap the pod to NotReady — allow the node/kubelet
source when you restrict ingress.

**The URL rules are breaking for an existing release too.** An install whose
`zensu.apiUrl` carries basic-auth userinfo has been working and now fails the
render — see [Configuration](#configuration). Move the credential to a proxy or a
Secret-backed header, or drop it. (An install that omitted the scheme was never
delivering heartbeats, because the HTTP client cannot send such a request; it now
fails loudly instead of silently.)

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
go build ./cmd/zensu-monitoring-agent
go test ./...
docker build -t zensu-monitoring-agent .
```

## License

[FSL-1.1-Apache-2.0](LICENSE) © Zensu — source-available under the Functional
Source License, converting to Apache-2.0 two years after each release (same
license as the [zensu-claude-code](https://github.com/MKITConsulting/zensu-claude-code) plugin).
