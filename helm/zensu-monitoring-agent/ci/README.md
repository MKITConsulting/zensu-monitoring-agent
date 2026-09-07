# CI render shapes

One values file per template conditional the chart carries, so adding a
conditional means adding a shape here in the same pull request. The workflow
renders every `*-values.yaml` in this directory and asserts what the first-line
`# expect:` marker names. Required values come from the workflow, not from these
files.

Markers:

- `gomemlimit` / `no-gomemlimit` — whether the render must carry a GOMEMLIMIT
  env entry. Declared rather than inferred, so a shape cannot flip the
  expectation by accident.
- `metrics-on` / `metrics-off` — whether the agent's own endpoint, its Service
  and its container port must be present.
- `cronjob` — the workload is a CronJob and must carry `--once`.
- `networkpolicy` — a NetworkPolicy must render and must carry a `from`
  selector, because an empty one admits every source.
- `podannotations` — the Prometheus scrape annotations must be on the pod
  template.
- `servicemonitor` — a ServiceMonitor must render.
- `no-rbac` — no Role, RoleBinding or ServiceAccount is created, and the
  workload falls back to the `default` ServiceAccount. That fallback is the one
  branch of the pair with real cluster consequences, so it is asserted rather
  than left to the reader.
- `existing-secret` — the chart creates no Secret of its own and the workload
  reads the operator's.

Every conditional in `templates/` now has a shape. When you add one, add a shape
and its marker arm in the workflow in the same pull request.
