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

## Durable provisioning (recommended over manual import)

A dashboard imported through the Grafana UI or API lives in Grafana's database,
which kube-prometheus-stack does not persist by default — a Grafana pod restart
loses it (observed live 2026-07-25). Provision them as ConfigMaps instead and
the stack's sidecar loads them within seconds, surviving restarts:

```bash
for f in operator-dashboard researcher-dashboards; do
  kubectl -n monitoring create configmap "wm-$f" \
    --from-file="$f.json=dashboards/$f.json" --dry-run=client -o yaml \
  | kubectl label -f - --local -o yaml grafana_dashboard=1 \
  | kubectl apply -f -
done
```

Two things the panels need in order to show anything:

- **Kueue metrics**: the `ServiceMonitor` + RBAC binding in
  `deploy/monitoring/` (verified to work as committed against a default
  kube-prometheus-stack install — the Prometheus ServiceAccount name and
  `serviceMonitorSelector` matched without edits).
- **Bridge metrics**: the chart's own ServiceMonitor, which must carry the
  label your Prometheus selects on, e.g.
  `--set serviceMonitor.enabled=true --set serviceMonitor.labels.release=kube-prometheus-stack`.
  Without that label the ServiceMonitor is created and then silently ignored:
  no target, no error, empty panels.

Validated live on GKE 2026-07-25: 0 query errors across all 38 panel
expressions; with jobs flowing, 22/26 operator-dashboard queries and 7/12
researcher-dashboard queries returned data (the remainder cover Ray, serving
and preemption paths that run did not exercise).
