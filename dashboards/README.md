# Dashboards

- `researcher-dashboards.json` — Slurm/Ray/serving researcher view (Kueue +
  KubeRay metrics only). See its own `__comment` field for scope and
  validation status.
- `operator-dashboard.json` — operator/on-call view (backlog A3): bridge and
  ray-bridge internals, Slurm API health, JobSet lifecycle, safety-guard
  counters, and Kueue admission context. Companion to the seven alerts in
  `deploy/chart/k8s-bridge/templates/prometheusrule.yaml`; see
  `docs/operations.md`'s "Dashboards" subsection.

## Import

1. In Grafana: **Dashboards → New → Import**, paste or upload the JSON file.
2. Point it at a Prometheus datasource scraping the k8s-bridge/ray-bridge
   `/metrics` endpoints and Kueue's `/metrics` (see
   `deploy/monitoring/README.md` for the `ServiceMonitor` + RBAC binding
   Kueue's metrics need).
3. Both dashboards are dashboard-as-code: edit the JSON directly and
   re-import rather than editing in the Grafana UI and exporting back.
