# Kueue metrics for the researcher Grafana dashboards (backlog L10)

## Problem this solves

`dashboards/researcher-dashboards.json` renders correctly in Grafana (render
validated in testing, see `docs/VALIDATION.md`), but every panel is empty
because nothing scrapes Kueue's `/metrics` endpoint. Kueue's metrics are
served over HTTPS with bearer-token auth via `kube-rbac-proxy`, so a plain
Prometheus scrape config is not enough — it needs a `ServiceMonitor` with TLS
and auth settings, plus RBAC for the Prometheus ServiceAccount to read
`/metrics`.

This directory adds the two pieces kube-prometheus-stack needs and does not
get from Kueue's own release manifest.

## Prerequisites

1. **kube-prometheus-stack** installed (via the `prometheus-community` Helm
   chart), which provides:
   - the Prometheus Operator and the `ServiceMonitor` CRD
     (`monitoring.coreos.com/v1`)
   - a `Prometheus` custom resource whose `serviceMonitorSelector` matches a
     `release: <helm-release-name>` label (the chart's default) — this is
     why `kueue-servicemonitor.yaml` carries that label.
2. **Kueue v0.18.2** installed via the upstream release manifest, as this
   project already does (see `experiments/10-ray-bridge/README.md`):
   ```bash
   kubectl apply --server-side -f \
     https://github.com/kubernetes-sigs/kueue/releases/download/v0.18.2/manifests.yaml
   ```
   That manifest creates the `metrics-reader` `ClusterRole` (nonResourceURL
   `/metrics`, verb `get`) but does **not** bind it to anything — Kueue
   leaves that to the cluster operator. `kueue-metrics-reader-binding.yaml`
   below supplies that binding for the Prometheus ServiceAccount.

## What's targeted, and why (verified against Kueue 0.18)

Kueue's release manifest is a kustomize build of `config/default` (branch
`release-0.18` of `kubernetes-sigs/kueue`), which applies `namespace:
kueue-system` and `namePrefix: kueue-` to
`config/default/metrics_service.yaml`. The resulting, actually-deployed
Service is:

| Field | Value | Source |
|---|---|---|
| Service name | `kueue-controller-manager-metrics-service` | `namePrefix: kueue-` + `metrics_service.yaml`'s `controller-manager-metrics-service` |
| Namespace | `kueue-system` | `config/default/kustomization.yaml` |
| Port name / number | `https` / `8443` | `config/default/metrics_service.yaml` |
| Scheme | HTTPS (served via `kube-rbac-proxy` in front of the controller-manager) | same file, port named `https` |
| Selector | `control-plane: controller-manager` | same file |
| Extra labels (patched on) | `app.kubernetes.io/name: kueue`, `app.kubernetes.io/component: metrics-service` | label patch block in `config/default/kustomization.yaml` |
| Auth | Bearer token (ServiceAccount token), checked by `kube-rbac-proxy` via SubjectAccessReview against `nonResourceURLs: ["/metrics"]` | `config/components/rbac/metrics_reader_role.yaml` |

The `ServiceMonitor`'s `endpoints` block (port `https`, path `/metrics`,
scheme `https`, `bearerTokenFile`, `tlsConfig.insecureSkipVerify: true`)
mirrors Kueue's own example, `config/components/prometheus/monitor.yaml` —
which upstream ships but does **not** wire into the default release manifest
(the `[PROMETHEUS]` overlay in `config/default/kustomization.yaml` is
commented out by default). That's why we carry our own copy here instead of
depending on an upstream flag.

This project's actual deployment (confirmed in
`experiments/10-ray-bridge/README.md`) uses exactly the
`v0.18.2/manifests.yaml` release, so the service name/namespace/port above
match the live cluster, not just the docs.

`insecureSkipVerify: true` is a **prototype-only** shortcut — Kueue's
`config/default` also has a commented-out `[CERTMANAGER]` overlay
(`cert_metrics_manager_patch.yaml`) that would let Prometheus verify a real
cert. Flagged for the production engineering handoff.

## Files

- `kueue-servicemonitor.yaml` — the `ServiceMonitor` that tells
  kube-prometheus-stack's Prometheus to scrape Kueue.
- `kueue-metrics-reader-binding.yaml` — `ClusterRoleBinding` granting the
  Prometheus ServiceAccount `get` on `/metrics`, bound to the `metrics-reader`
  `ClusterRole` that Kueue's own `manifests.yaml` already creates.

## Apply

1. Edit `kueue-servicemonitor.yaml`: set the `release: kube-prometheus-stack`
   label under `metadata.labels` to match your actual Helm release name.
   Check with:
   ```bash
   helm list -n <monitoring-namespace>
   helm get values <release-name> -n <monitoring-namespace> -o json \
     | jq '.prometheus.prometheusSpec.serviceMonitorSelector'
   ```
2. Edit `kueue-metrics-reader-binding.yaml`: set `subjects[0].name` /
   `namespace` to your Prometheus ServiceAccount. Check with:
   ```bash
   kubectl get prometheus -A -o jsonpath='{.items[0].spec.serviceAccountName}'
   ```
3. Apply both:
   ```bash
   kubectl apply -f deploy/monitoring/kueue-metrics-reader-binding.yaml
   kubectl apply -f deploy/monitoring/kueue-servicemonitor.yaml
   ```
4. Confirm Prometheus picked up the target:
   ```bash
   kubectl -n <monitoring-namespace> port-forward svc/<prometheus-svc> 9090
   # open http://localhost:9090/targets and look for kueue-controller-manager-metrics-monitor
   ```

Both manifests were syntax- and schema-validated with
`kubectl apply --dry-run=server` against a throwaway local `kind` cluster
with the `ServiceMonitor` CRD installed (no GCP cost; cluster deleted
immediately after). No live cluster was left running as part of this change.

## Which dashboard panels this feeds

From `dashboards/researcher-dashboards.json` (uid `wm-researchers`):

| Panel | Metric(s) queried |
|---|---|
| Pending vs admitted workloads per queue | `kueue_pending_workloads`, `kueue_admitted_active_workloads` |
| Team quota: nominal vs used vs borrowed (CPU) | `kueue_cohort_subtree_quota`, `kueue_local_queue_resource_usage`, `kueue_cohort_subtree_resource_reservations` |
| Admission wait p90 (why-am-I-waiting, seconds) | `kueue_admission_wait_time_seconds_bucket` |
| Preemptions / evictions (10m) | `kueue_evicted_workloads_total` |
| Growth blocked by quota (pending pod workloads) | `kueue_pending_workloads` |
| Serving admissions and batch displacements | `kueue_admitted_workloads_total`, `kueue_evicted_workloads_total{reason="Preempted"}` |

All of the above are `kueue_*` metrics exposed by the controller-manager
process itself, so this one `ServiceMonitor` covers all six panels.

**Not covered by this change:** the "Ray cluster desired vs available
workers" panel queries `kuberay_cluster_desired_worker_replicas` /
`kuberay_cluster_available_worker_replicas`, which come from the **KubeRay
operator**, not Kueue. That needs a separate ServiceMonitor (or the
`kuberay-operator` Helm chart's own `metrics.serviceMonitor.enabled` flag, if
present in the installed chart version) — out of scope for L10, noting it
here so it isn't mistaken for solved.
