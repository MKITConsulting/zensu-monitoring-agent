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
2. The agent lists those Deployments (read-only) on an interval.
3. It POSTs each service's status to `${ZENSU_API_URL}/api/runtime/heartbeat`
   with an `X-API-Key`.

Status is derived from replica counts: all ready → `up`, some ready →
`degraded`, none ready → `down`.

## Trust / security

- **Least privilege.** The bundled RBAC grants only `get/list/watch` on
  `deployments` and `pods`. The agent cannot create, update, patch, or delete
  anything — see [`helm/zensu-agent/templates/rbac.yaml`](helm/zensu-agent/templates/rbac.yaml).
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
| `ZENSU_API_KEY` | yes | — | API key (`zsk_...`) |
| `ZENSU_PRODUCT_ID` | yes | — | Product UUID to report to |
| `ZENSU_AGENT_INTERVAL` | no | `60s` | Heartbeat cadence (Go duration) |
| `ZENSU_AGENT_NAMESPACES` | no | `default` | Comma-separated namespaces to scan |
| `ZENSU_AGENT_SOURCE` | no | `k8s-agent` | Source label attached to heartbeats |

Helm values mirror these under `zensu.*` / `agent.*` — see
[`values.yaml`](helm/zensu-agent/values.yaml). Supply the API key out-of-band with
`zensu.existingSecret` (a Secret holding key `ZENSU_API_KEY`) instead of
`zensu.apiKey`.

### Deploy modes

- `agent.mode=deployment` (default): long-running ticker.
- `agent.mode=cronjob`: one-shot per `agent.schedule` (runs the binary with
  `--once`).

Not on Kubernetes? Run the binary with `--once` from any cron host — it works
anywhere it can reach the Zensu API and a kubeconfig is available.

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
      "readyReplicas": 3, "desiredReplicas": 3, "intervalSeconds": 60 }
  ]
}
```

`status` is one of `up`, `degraded`, `down`. Services are auto-registered by
`productId` + `slug` on first heartbeat; omitting `name` preserves a previously
stored name.

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
