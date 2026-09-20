# TDD Plan: Resource-source defects surfaced by the PR #8 review

## Context

Three production defects found by the review chain over PR #8 and still present on merged
main (`4f9f472`). This is a behaviour-fix chain, not a coverage pass: each defect gets a test
that fails against today's code first.

Baseline verified in this session on `4f9f472`: `go build ./...` exit 0, `go vet ./...` exit 0,
`gofmt -l ./internal ./cmd` no output, `go test ./... -count=1` exit 0 across five packages,
coverage 96.1%.

**Defect 1** — `internal/agent/k8s.go:82`, `clientsetLister.PodMetricsForSelector`. All four
return paths set `available` to exactly `err == nil`, so an empty PodMetrics list returns
`(0, 0, true, nil)`. `internal/agent/source.go:106-109` then writes CPU=0 and memory=0 into the
heartbeat instead of skipping the target. A running container reporting 0 bytes of memory is
not a measurement that can occur; metrics-server needs a scrape window before PodMetrics
objects exist for freshly started pods, so a rollout publishes a fabricated zero. Both
`ClusterReader` test doubles (`internal/agent/source_test.go:301`,
`internal/agent/agent_test.go:100-106`) already model `(0, 0, false, nil)` as normal — the state
the real implementation can never emit.

**Defect 2 — SCOPE CORRECTED DURING PHASE 1.** The review reported that `isMetricsAPIMissing`
being group-blind strands the agent on the fallback. Reading `internal/agent/source.go:85-112`
and `:145-186` in full disproves the strand: `Samples` re-queries the API every tick, and
`autoSource` re-probes on `autoRetryInterval` (5 minutes), so **both modes recover on their
own**. Evidence for the classification itself, from
`k8s.io/apimachinery@v0.31.3/pkg/api/errors/errors.go`: `IsNotFound` is true both when
`Reason == StatusReasonNotFound` and when the reason is unknown and the code is 404 — so a
narrower discriminator is possible in principle, but which shape an unregistered
`metrics.k8s.io` actually produces is NOT establishable from the module source and would need a
live cluster. Per the spec's own instruction for that case, this plan does not guess at a
discriminator, and `isMetricsAPIMissing` is left unchanged. The residual defect that IS
establishable: `metricsServerSource.warned` (`source.go:68`) is a lifetime latch, so a
metrics-server outage that recurs after a recovery is logged exactly once ever and every later
outage is silent. That is the defect S2 fixes.

**Defect 3** — `internal/metrics/metrics.go:176-182`, `SetResourceSource`. The loop sets 1 only
on an exact match against `ResourceSources` (`metrics.go:41`); an unlisted `active` falls through
with every label at 0, no error and no panic, defeating the doc comment's promise at
`metrics.go:167-171` that the active source is answerable from one scrape. Latent today: the only
call site is `internal/agent/agent.go:149` passing `a.source.Name()`, and all three distinct
return values are listed. The undocumented invariant holding it together is that
`autoSource.Name()` must never return `SourceAuto`. `internal/metrics` imports nothing internal,
so `internal/agent` can depend on it and the two lists can be made one.

Also established this session: `go test ./internal/metrics/... -count=1` passes even when
`SetResourceSource` is mutated to set every source to 1 — the only assertions of these setters'
non-nil behaviour live in `internal/agent` (`instrument_test.go:153,156,183,187` and
`exposition_test.go:1326,1334`), through the rendered exposition text. `internal/metrics` cannot
verify its own contract.

**Approach**: Strict Red/Green TDD | **Tech Stack**: Go 1.23.0, stdlib `testing`, table-driven, `prometheus/client_golang` + `testutil` | **Coverage**: `go test ./... -coverpkg=./... -coverprofile=cover.out -count=1` @ 90% lines (default-90%)

**Task channel unavailable**: `TaskCreate`/`TaskUpdate` are not exposed in this session
(`ToolSearch select:TaskCreate,TaskUpdate` returned no matching deferred tools). The run log and
this plan's Status column are the only tracking channels for this chain.

## Requirements
| ID | Requirement | Source |
|----|-------------|--------|
| AC-001 | An empty PodMetrics list is reported as no data (`available=false`), so the caller skips the target instead of publishing a fabricated zero | spec (defect 1) |
| AC-002 | A metrics-server outage that recurs after a recovery is logged again, rather than silenced for the agent's lifetime by a one-way latch | spec (defect 2, scope corrected in Phase 1) |
| AC-003 | The resource-source label set in `internal/metrics` cannot drift from the source names `internal/agent` can emit | spec (defect 3) |
| AC-004 | A source name outside the canonical set is observable in the scrape instead of silently leaving every series at 0 | spec (defect 3) |
| FR-001 | `internal/metrics` asserts the non-nil behaviour of `SetResourceSamples`, `SetResourceSource` and `SetRolesWithheld` in its own package | spec (defect 3) |

## Preconditions
| Name | Type | Verification | Status | Decision |
|------|------|--------------|--------|----------|
| go toolchain | CLI | `command -v go` | present | — |

No external CLI, secret, service endpoint or input fixture is named by the spec beyond the Go
toolchain, which the baseline runs already exercised.

## Cross-Layer Value Flow Pairings

No pairings required. S3 is the only step whose value crosses a package boundary, and it changes
BOTH sides — `internal/metrics` (the label set) and `internal/agent` (the constants that feed it)
— so the consuming layer has its own IMPL step in this plan, which is an explicit non-trigger
under Principle 2.

| Feature Step | New Value | Unchanged Layer (file / module) | Characterization Step | Seam Asserted |
|--------------|-----------|---------------------------------|------------------------|----------------|

## Status Legend
| [ ] Not started | [R] RED test | [I] Implemented | [G] GREEN | [RF] Refactored | [!] Blocked | [W] Wired |

## Steps
| Step | Type | Description | Test File | Depends On | Status | Attempts | Covers |
|------|------|-------------|-----------|------------|--------|----------|--------|
| S1 | Bug Fix | Empty PodMetrics list reports no data instead of a zero measurement | internal/agent/k8s_test.go | — | [G] | 1 | AC-001 |
| S2 | Bug Fix | The metrics-server warn latch re-arms after a successful read | internal/agent/source_test.go | — | [!] | 1 | AC-002 |
| S3 | Bug Fix | An unlisted source name is observable instead of silently zeroing | internal/metrics/metrics_test.go | — | [G] | 2 | AC-004 |
| S3b | Refactoring | One canonical source-name set shared across the two packages | internal/agent/source_test.go | S3 | [RF] | 1 | AC-003 |
| S4 | Wired | Package-local assertions for the three setters' non-nil behaviour | internal/metrics/metrics_test.go | S3 | [W] | 1 | FR-001 |

**S2 carries `[!]` for the mtime discipline audit, not for a functional defect.** Its RED
genuinely preceded its IMPL (run-log lines 8, 9, 10 record RED, IMPL, GREEN in that order), but
S3b later edited the same test file, pushing `source_test.go`'s mtime past `source.go`'s. The
mtime heuristic cannot see per-step ordering once a later step touches an earlier step's test
file. Recorded as the audit produced it rather than suppressed; both tests for AC-002 are green.

**S3 took 2 attempts** because a mutation-sensitivity check for S4 reverted
`internal/metrics/metrics.go` with `git checkout -- <file>`, which restores HEAD and so also
discarded S3's production change. It was re-applied and re-verified.

### Step S1 — Empty PodMetrics list reports no data instead of a zero measurement
- **Covers**: AC-001
- **RED**: Test `TestPodMetricsForSelectorReportsNoDataForAnEmptyList` — drive `PodMetricsForSelector`
  through the metrics fake with a reactor returning an empty `PodMetricsList`, assert
  `available == false` with a nil error. Fails today because `k8s.go:83` returns `true`
  unconditionally on a successful list.
- **GREEN**: Report an empty item list as no data rather than as a measurement of zero.
  `source.go:103` already handles that correctly via `if !available { continue }`.

### Step S2 — The metrics-server warn latch re-arms after a successful read
- **Covers**: AC-002
- **RED**: Test `TestMetricsServerSourceWarnsAgainAfterRecovery` — a reader that answers
  `ErrMetricsAPIUnavailable`, then succeeds, then answers unavailable again; assert the warning is
  logged on BOTH outages. Fails today because `warned` (`source.go:68`) is a one-way
  `CompareAndSwap(false, true)` that is never reset.
- **GREEN**: Clear the latch on a successful read, so the once-per-outage anti-spam property the
  doc comment at `source.go:64-66` promises is kept while a recurrence stays observable.

### Step S3 — One canonical source-name set, and an unlisted name is observable
- **Covers**: AC-003, AC-004
- **RED**: Two assertions. (a) `TestResourceSourcesCoverEverySourceName` in `internal/agent` —
  assert every source name `Name()` can return is present in `metrics.ResourceSources`;
  (b) `TestSetResourceSourceFlagsAnUnlistedName` in `internal/metrics` — assert an unlisted
  `active` is observable in the scrape rather than leaving every series at 0. (b) fails today
  because `metrics.go:176-182` falls through silently.
- **GREEN**: Export the canonical names from `internal/metrics`, build `ResourceSources` from
  them, have `internal/agent`'s `Source*` constants reference them so the two lists cannot drift,
  and record an unlisted name on its own pre-initialised series so drift shows up in one scrape.

### Step S4 — Package-local assertions for the three setters' non-nil behaviour
- **Covers**: FR-001
- **RED**: Test `TestResourceSettersRecordTheirValues` in `internal/metrics` — assert
  `SetResourceSamples` per role, `SetResourceSource` setting 1 for the active name and 0 for the
  rest, and `SetRolesWithheld` per role, via `testutil.ToFloat64` on the package's own gauges.
- **GREEN**: No production change expected — this step closes the verification gap that let
  `go test ./internal/metrics/...` pass over a mutated `SetResourceSource`.

**Checkpoint**: `go test ./internal/agent/... ./internal/metrics/... -count=1` over this phase's changed files and the suites that import or invoke them + `go vet ./...` pass (the full suite runs in the Phase 6 audit, not here — unless the Phase 5 fallback fires)

## Final Verification
- All test suites pass
- Coverage report generated for changed files (threshold: 90% lines)
