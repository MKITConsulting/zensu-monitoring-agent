# TDD Plan: Collector exposition as a resource-metric source

## Context

The agent reads per-service CPU/memory only from metrics-server (`metrics.k8s.io`).
metrics-server is not a Kubernetes default — it is frequently absent on EKS, kubeadm,
bare-metal and hardened clusters, where the Zensu resource charts stay empty while uptime
keeps working. Many of those clusters already run an OpenTelemetry Collector that has the
numbers. This makes a Collector's Prometheus exposition a second, equal source for the two
metric keys the backend registry already knows (`cpu_millicores`, `memory_bytes`).

Backend, wire contract and DB schema are untouched: the heartbeat `metrics[]` field is
already source-agnostic. Out of scope: OTLP receiver in the backend, remote-write,
app-level metrics (latency, req/s), auth headers for protected expositions, a live
Collector in the kind e2e, chart/app version bump.

Upstream design record: `docs/plans/2026-06-24_metrics-source-strategy.md` in the
zensu monorepo (section 4 rank 2, section 6). That record sketches a PromQL read against
`/api/v1/query`; a bare Collector has no query API, only a scrape exposition, so this
implements scrape instead.

**Approach**: Vanilla implementation (strict TDD discipline not in effect for this chain)
| **Tech Stack**: Go 1.23 (toolchain go1.26.3), `prometheus/client_golang`, `prometheus/common/expfmt`, client-go, Helm
| **Coverage**: `go test -coverprofile` @ 90% lines (default-90%)

Harness notes for this run:
- Code lands in the `zensu-monitoring-agent` repo, worktree
  `.claude/worktrees/collector-source` (branch `claude/collector-exposition-source`,
  baseline `b7ef28af5a659788451fe5a25208e94f79c45910` = `origin/main`). The Claude session's
  project dir is the zensu monorepo, so the Edit Landing Audit and diff enumeration are
  pointed at the worktree explicitly.
- `TaskCreate`/`TaskUpdate` are unavailable in this session (MCP server disconnected), so
  the Task channel of Principle 3 cannot be served. Log + plan are the tracking channels.

## Requirements

| ID | Requirement | Source |
|----|-------------|--------|
| FR-001 | `internal/scrape` fetches a Prometheus exposition over HTTP with a body size cap and decodes it via `expfmt` into typed series | spec |
| FR-002 | Metric kind (gauge vs counter) is derived from the exposition's `# TYPE` metadata, never from configuration | spec |
| FR-003 | Metric selection uses an ordered candidate list matched on base names with unit suffixes (`_bytes`, `_seconds`, `_total`) stripped | spec |
| FR-004 | Counter series convert to a per-second rate with counter-reset detection; the first observation of a series yields no sample | spec |
| FR-005 | `MetricSource` interface in `internal/agent` with `metricsServerSource` and `expositionSource`; `Agent.Collect` is two-phase and calls the source once per tick | spec |
| FR-006 | Slug resolution: configured slug label first, pod/namespace matching fallback with tolerant label names, otherwise discard | spec |
| FR-007 | Per-service reduction sums samples across all pods of a service | spec |
| FR-008 | Env configuration `ZENSU_MONITORING_AGENT_RESOURCE_SOURCE` plus `SCRAPE_URL`, `SCRAPE_SLUG_LABEL`, `SCRAPE_CPU_METRIC`, `SCRAPE_MEMORY_METRIC`, `SCRAPE_TIMEOUT` | spec |
| FR-009 | Helm values and configmap expose the new settings | spec |
| FR-010 | Three scrape metrics in `internal/metrics` with nil-receiver guards | spec |
| FR-011 | README documents the source, the `k8sattributes` snippet, the fallback chain and the egress implication | spec |
| AC-001 | With `RESOURCE_SOURCE=auto` and no scrape URL, behaviour is identical to today: metrics-server path, single warn latch on absence | spec |
| AC-002 | With metrics-server unavailable and a scrape URL set, `auto` falls through to the exposition source | spec |
| AC-003 | A gauge CPU metric in cores yields `cpu_millicores` = cores x 1000 | spec |
| AC-004 | A counter CPU metric in seconds yields `cpu_millicores` from the rate x 1000; first tick yields none; a counter reset skips one tick | spec |
| AC-005 | A memory gauge yields `memory_bytes` verbatim | spec |
| AC-006 | Suffixed and unsuffixed metric names resolve to the same candidate | spec |
| AC-007 | An env override for the CPU/memory metric name wins over the auto-pick | spec |
| AC-008 | A sample carrying the slug label resolves to that slug without any pod knowledge | spec |
| AC-009 | A sample without the slug label resolves via pod+namespace against the known pod set; both tolerant label spellings work | spec |
| AC-010 | A sample resolving to no known service is discarded and never reaches the heartbeat | spec |
| AC-011 | Multiple pods of one service sum into a single value per metric key | spec |
| AC-012 | A scrape failure (transport error or HTTP 500) does not fail the tick; uptime and restartCount still ship | spec |
| AC-013 | Scrape counter, histogram and gauge appear on the agent's own `/metrics` endpoint after a scrape | spec |
| AC-014 | `helm template` renders the new env keys | spec |

## Preconditions

| Name | Type | Verification | Status | Decision |
|------|------|--------------|--------|----------|
| go | CLI | `go version` | present (go1.26.3) | — |
| helm | CLI | `helm version --short` | present (v3.14.4) | — |
| `prometheus/common@v0.66.1/expfmt` | fixture | module cache probe | present (already an indirect dep, promote to direct) | — |

No missing preconditions; no escalation was required.

## Cross-Layer Value Flow Pairings

No pairings. The change is confined to the agent; the heartbeat wire contract, the backend
handler and the DB schema are untouched, so no new value crosses into an unchanged layer.

## Status Legend
| [ ] Not started | [R] RED test | [I] Implemented | [G] GREEN | [RF] Refactored | [!] Blocked | [W] Wired |

## Steps

| Step | Type | Description | Test File | Depends On | Status | Attempts | Covers |
|------|------|-------------|-----------|------------|--------|----------|--------|
| S1 | Feature | `internal/scrape`: HTTP fetch with body cap + `expfmt` decode into typed `Series` | `internal/scrape/scrape_test.go`, `internal/scrape/decode_test.go` | — | [!] | 1 | FR-001, FR-002 |
| S2 | Feature | `internal/scrape`: candidate resolution, unit-suffix stripping, env override | `internal/scrape/resolve_test.go` | S1 | [I] | 1 | FR-003, AC-006, AC-007 |
| S3 | Feature | `internal/scrape`: counter to per-second rate, reset detection, first-observation suppression | `internal/scrape/rate_test.go` | S1 | [I] | 1 | FR-004, AC-004 |
| S4 | Refactor | `internal/agent`: `MetricSource` interface, extract `metricsServerSource`, two-phase `Collect` | `internal/agent/source_test.go` | — | [I] | 1 | FR-005, AC-001 |
| S5 | Feature | `internal/agent`: `expositionSource` — slug resolution (label, then pod/namespace), per-service sum, unit scaling | `internal/agent/source_test.go` | S1, S2, S3, S4 | [I] | 1 | FR-006, FR-007, AC-003, AC-005, AC-008, AC-009, AC-010, AC-011 |
| S6 | Feature | `internal/agent`: source selection `auto`/`metrics-server`/`exposition`/`none` + graceful degrade | `internal/agent/source_test.go` | S4, S5 | [I] | 1 | FR-005, AC-002, AC-012 |
| S7 | Feature | `internal/metrics`: scrape counter, duration histogram, services-mapped gauge, nil-safe | `internal/agent/source_test.go` | S5 | [I] | 1 | FR-010, AC-013 |
| S8 | Integration | `cmd/zensu-monitoring-agent/main.go`: env parsing and source construction | `cmd/zensu-monitoring-agent/main_test.go` | S6, S7 | [W] | 1 | FR-008 |
| S9 | Integration | Helm `values.yaml` + `templates/configmap.yaml`: new keys | — | S8 | [W] | 1 | FR-009, AC-014 |
| S10 | Integration | README: source section, `k8sattributes` snippet, fallback chain, egress note | — | S9 | [W] | 1 | FR-011 |

**S1 is `[!]` by the Edit Landing Audit rule, not because the step failed.** Every code
claim of S1 landed; the step's `files:` list also named `go.sum`, which git shows
unchanged — promoting three modules from indirect to direct rewrites `go.mod` only, since
their hashes were already present. That one claim is withdrawn and the finding stands.

**Deviation from the plan.** The services-mapped gauge is named
`zensu_monitoring_agent_resource_services_mapped`, not `..._scrape_services_mapped`: it is
set on every tick for whichever source ran, so naming it after scraping would have been
wrong. S7's tests live in `internal/agent/source_test.go` rather than
`instrument_test.go`, next to the source that emits them.

### Step S1 — Exposition client
- **Covers**: FR-001, FR-002

### Step S2 — Candidate resolution
- **Covers**: FR-003, AC-006, AC-007

Candidate table. Names marked verified were read from the upstream
`receiver/kubeletstatsreceiver/metadata.yaml` during planning; the cAdvisor names are
conventional and are marked for verification against a real cAdvisor exposition.

| Candidate | Kind | Unit | Origin | Status |
|---|---|---|---|---|
| `k8s_pod_cpu_usage` | gauge | cores | kubeletstats `k8s.pod.cpu.usage` | verified upstream |
| `k8s_pod_cpu_time` | counter | seconds | kubeletstats `k8s.pod.cpu.time` | verified upstream |
| `container_cpu_usage_seconds_total` | counter | seconds | cAdvisor | RECON |
| `k8s_pod_memory_working_set` | gauge | bytes | kubeletstats `k8s.pod.memory.working_set` | verified upstream |
| `container_memory_working_set_bytes` | gauge | bytes | cAdvisor | RECON |

`k8s.pod.cpu.utilization` does not exist upstream (only `k8s.pod.cpu.node.utilization`,
disabled by default) and is deliberately absent from the list.

### Step S3 — Counter rate
- **Covers**: FR-004, AC-004

### Step S4 — MetricSource seam
- **Covers**: FR-005, AC-001

### Step S5 — Exposition source
- **Covers**: FR-006, FR-007, AC-003, AC-005, AC-008, AC-009, AC-010, AC-011

### Step S6 — Source selection
- **Covers**: FR-005, AC-002, AC-012

`auto` tries metrics-server and only falls through to the exposition on
`ErrMetricsAPIUnavailable` with a scrape URL configured, so no existing deployment changes
behaviour.

### Step S7 — Instrumentation
- **Covers**: FR-010, AC-013

### Step S8 — Env wiring
- **Covers**: FR-008

`RESOURCE_SOURCE`, not `METRICS_SOURCE`: `ZENSU_MONITORING_AGENT_METRICS_ENABLED` and
`_ADDR` are taken and refer to the agent's own exposition endpoint.

### Step S9 — Helm
- **Covers**: FR-009, AC-014

### Step S10 — README
- **Covers**: FR-011

**Checkpoint**: `go build ./...` + `go vet ./...` + `go test ./...` pass

## Final Verification
- All test suites pass
- `helm template` renders with the new keys
- Coverage report generated for changed files (threshold: 90% lines)
