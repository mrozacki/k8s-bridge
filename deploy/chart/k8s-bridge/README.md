# k8s-bridge Helm chart

Deploys the k8s-bridge controller: a single-replica Deployment that polls
slurmrestd for held jobs on managed Slurm partitions and translates them into
Kueue-admitted JobSets, so Kueue becomes the shared admission point for mixed
Slurm + Kubernetes workloads. See `docs/architecture.md`
in the parent repo for the design this implements.

This chart is experimental (prototype phase, chart version 0.1.0) — expect
breaking changes between minor versions until a 1.0 is cut.

## What this chart deploys

- `Deployment` — the controller (`replicas: 1` by default; ADR-0011 added
  controller-runtime Manager + leader election, `leaderElect: true` by
  default, so a second replica is now SAFE to run — it sits idle until it
  wins the `coordination.k8s.io` Lease — bump the `replicas` value if you
  want a hot standby).
- `Service` — always rendered, fronts the controller's metrics/health port
  (8080), which already serves `/healthz`, `/readyz` and `/metrics`.
- `ServiceMonitor` — optional (`serviceMonitor.enabled=false` by default),
  requires the prometheus-operator CRDs.
- `PrometheusRule` — optional (`prometheusRule.enabled=false` by default),
  translates the alerts in `docs/operations.md` into alerting rules; also
  requires the prometheus-operator CRDs, and a few individual rules need
  extra exporters (kube-state-metrics, a Slurm node-state exporter) beyond
  what this chart ships — see `templates/prometheusrule.yaml`.
- `PodDisruptionBudget` — optional (`podDisruptionBudget.enabled=false` by
  default), useful once `replicas` > 1.
- `ServiceAccount` + `Role`/`RoleBinding` (or `ClusterRole`/`ClusterRoleBinding`
  when `rbac.namespaced=false`) — least-privilege access to JobSets, Kueue
  Workloads, Events, Leases, and (if using CR config) WorkloadMixing
  resources.
- `ConfigMap` — the controller's YAML config, only when `configSource=file`.
- `crds/workloadmixing-crd.yaml` — the `WorkloadMixing` CRD, installed
  automatically by Helm. **Canonical source is `deploy/crd/` in the parent
  repo**; this is a manually kept-in-sync copy (see the file's header
  comment). Helm never upgrades or deletes CRDs on `helm upgrade`/`uninstall`
  — see the [Helm CRD docs](https://helm.sh/docs/chart_best_practices/custom_resource_definitions/).
- `PriorityClass` — optional (`priorityClass.create=false` by default,
  backlog P6): keeps the controller from being evicted under the very node
  pressure it manages.
- `NetworkPolicy` — optional (`networkPolicy.enabled=false` by default),
  default-deny egress except DNS, kube-apiserver, and slurmrestd.

## Prerequisites

- A Kubernetes cluster with:
  - **Kueue** installed, with a `ClusterQueue`/`LocalQueue` and
    `WorkloadPriorityClass` objects matching `config.localQueue` and
    `config.partitionMappings[].workloadPriorityClass`.
  - **JobSet operator** installed (the controller creates
    `jobset.x-k8s.io/v1alpha2` JobSets).
- **slurmrestd reachable from the cluster** at `config.slurmRestURL`, with a
  Slurm partition matching each entry in `config.partitionMappings`.
- **A Secret holding the slurmrestd JWT**, named by `slurmTokenSecret`
  (default key expected at `/var/run/secrets/slurm/token` inside the
  container — see `config.slurmTokenFile`). Create it before installing, e.g.:

  ```
  kubectl create secret generic slurm-rest-token \
    --from-file=token=./slurm.jwt \
    -n <release-namespace>
  ```

  This chart does not create the token Secret itself — the token is a
  credential, not a chart-managed value.
- If `configSource: cr` — WorkloadMixing custom resource(s) applied
  separately (see `deploy/crd/workloadmixing-sample.yaml` for the shape).
  With `workloadmixing.name` EMPTY (the default) the controller supervises
  EVERY WorkloadMixing in the release namespace, one reconcile loop per CR
  (ADR-0015 Phase A); with it set, the controller binds to that single CR
  (the pre-ADR-0015 compatibility path). Either way the controller watches
  the CR(s) and hot-reloads on spec changes — no restart needed.

## Install

```
# From the parent repo root:
helm install k8s-bridge deploy/chart/k8s-bridge \
  --namespace slurm-jobs --create-namespace \
  --set image.repository=<your-registry>/k8s-bridge \
  --set image.tag=<your-tag>
```

Or with a values file:

```
helm install k8s-bridge deploy/chart/k8s-bridge -f my-values.yaml
```

To use CR-based config instead of the rendered ConfigMap (supervisor mode:
every WorkloadMixing CR created in the release namespace gets its own
reconcile loop — ADR-0015):

```
helm install k8s-bridge deploy/chart/k8s-bridge \
  --namespace slurm-jobs --create-namespace \
  --set configSource=cr
```

Or bound to exactly one CR (compatibility path):

```
helm install k8s-bridge deploy/chart/k8s-bridge \
  --set configSource=cr \
  --set workloadmixing.namespace=slurm-jobs \
  --set workloadmixing.name=playground
```

## Values

| Key | Type | Default | Description |
|---|---|---|---|
| `image.repository` | string | `ghcr.io/owner/k8s-bridge` | Placeholder — set to your registry/repo before installing. |
| `image.tag` | string | `""` | Image tag; falls back to `.Chart.AppVersion` when empty. |
| `image.digest` | string | `""` | Image digest (e.g. `sha256:...`); wins over `tag` when set — prefer this for immutability. |
| `image.pullPolicy` | string | `IfNotPresent` | Standard pull policy. |
| `imagePullSecrets` | list | `[]` | e.g. `[{name: my-registry-cred}]`. |
| `nameOverride` | string | `""` | Overrides the chart name used in labels. |
| `fullnameOverride` | string | `""` | Overrides the computed release name, letting two installs coexist. |
| `allowedSlurmdImages` | list | `["ghcr.io/slinkyproject/"]` | Controller-level allowlist of image-ref prefixes permitted to run as the privileged slurmd node. Deliberately **not** part of `config`/the CR — the CR author is exactly who this defends against. Empty disables the check (logs a warning). |
| `allowedTokenPaths` | list | `["/var/run/secrets/", "/etc/k8s-bridge/"]` | Controller-level allowlist of directory prefixes a `WorkloadMixing` CR's `slurmTokenFile`/`slurmCACertFile` may resolve under in supervisor mode. Deliberately **not** part of `config`/the CR — the CR author is exactly who this defends against (ADR-0017). Empty disables the check (logs a warning). |
| `allowInsecureTLS` | bool | `false` | Controller-level gate: a CR's `slurmInsecureSkipTLSVerify` is honored in supervisor mode only when this is `true`. Deliberately **not** part of `config`/the CR — same rationale as `allowedTokenPaths` (ADR-0017). |
| `resources` | object | requests `50m`/`64Mi`, limits `200m`/`1Gi` | Pod resource requests/limits. Measured baseline: ~88Mi RSS at 5000 held jobs, ~1.6% CPU. The memory limit was raised from an earlier `256Mi` to `1Gi` after the 20k-job scale run (suite E, 2026-07) OOM-killed the pod at `256Mi` — see `docs/operations.md`'s "Memory OOM at >5,000 backlog jobs" runbook. `values-gke-test.yaml` raises this further (`2Gi`/`1` CPU) for deliberate scale testing. |
| `replicas` | int | `1` | Deployment replica count. Only useful above `1` when `leaderElect` is on (the default) — extra replicas sit idle until they win the Lease, giving a hot standby instead of double-processing. |
| `nodeSelector` | object | `{}` | Standard pod `nodeSelector`, passed through verbatim. |
| `tolerations` | list | `[]` | Standard pod `tolerations`, passed through verbatim. |
| `affinity` | object | `{}` | Standard pod `affinity`, passed through verbatim. |
| `topologySpreadConstraints` | list | `[]` | Standard pod `topologySpreadConstraints`, passed through verbatim. Only meaningful once `replicas` > 1. |
| `podAnnotations` | object | `{}` | Extra annotations merged onto the pod template (not the Deployment object itself). |
| `podLabels` | object | `{}` | Extra labels merged onto the pod template (not the Deployment object itself). |
| `podDisruptionBudget.enabled` | bool | `false` | Renders a `PodDisruptionBudget`. Off by default: on a single-replica Deployment a PDB only blocks voluntary eviction without buying availability. Enable once `replicas` > 1. |
| `podDisruptionBudget.minAvailable` | string/int | `""` | Mutually exclusive with `maxUnavailable` — a real `PodDisruptionBudgetSpec` rejects both being set. Takes precedence when non-empty. |
| `podDisruptionBudget.maxUnavailable` | string/int | `1` | Ignored when `minAvailable` is set to a non-empty value. |
| `service.type` | string | `ClusterIP` | Type of the always-rendered metrics `Service`. |
| `service.port` | int | `8080` | Port of the always-rendered metrics `Service` (matches the container's single port). |
| `serviceMonitor.enabled` | bool | `false` | Renders a prometheus-operator `ServiceMonitor` targeting the metrics Service. Requires the `monitoring.coreos.com/v1` CRDs installed (e.g. via kube-prometheus-stack) — `helm install` fails outright otherwise. |
| `serviceMonitor.interval` | string | `30s` | Scrape interval. |
| `serviceMonitor.scrapeTimeout` | string | `10s` | Scrape timeout. |
| `serviceMonitor.labels` | object | `{}` | Extra labels on the `ServiceMonitor` object — many kube-prometheus-stack installs gate their Prometheus's `serviceMonitorSelector` on a `release: <helm-release-name>` label; set it here if yours does. |
| `prometheusRule.enabled` | bool | `false` | Renders a prometheus-operator `PrometheusRule` translating the 7 alerts in `docs/operations.md` into alerting rules. Same CRD prerequisite as `serviceMonitor`. Some rules additionally require kube-state-metrics or a Slurm node-state exporter — see `templates/prometheusrule.yaml`. |
| `prometheusRule.labels` | object | `{}` | Extra labels on the `PrometheusRule` object. |
| `prometheusRule.rules` | object | per-alert defaults matching `docs/operations.md` | Per-alert `for`/`window`/`severity` overrides; see `templates/prometheusrule.yaml` for the full set of keys. |
| `leaderElect` | bool | `true` | Passes `--leader-elect` (ADR-0011): the controller uses a `coordination.k8s.io` Lease before starting its reconcile loop, so a second replica is safe to run (it sits idle until elected). RBAC for leases is granted unconditionally. |
| `leaseDurationSeconds` | int | `15` | Passed as `--leader-lease-duration`. Must be greater than `renewDeadlineSeconds`. Kept short (L9) so leadership hands over quickly on a rolling restart, paired with the Deployment's `Recreate` strategy. |
| `renewDeadlineSeconds` | int | `10` | Passed as `--leader-renew-deadline`. Must be less than `leaseDurationSeconds`. |
| `priorityClass.create` | bool | `false` | Renders a dedicated `PriorityClass` (backlog P6) and sets it on the Deployment's pod spec. Off by default since PriorityClass is cluster-scoped, like the `rbac.namespaced=false` path. |
| `priorityClass.value` | int | `1000000000` | The PriorityClass's `value`. Comfortably above ordinary workload priorities, below `system-cluster-critical` (2000000000) — the bridge should outlast workload pods under pressure without claiming cluster-critical status. |
| `priorityClass.name` | string | `""` | PriorityClass name; defaults to the chart's fullname when empty. |
| `rbac.namespaced` | bool | `true` | `true` grants a `Role`/`RoleBinding` per managed namespace (least privilege). `false` grants a cluster-scoped `ClusterRole`/`ClusterRoleBinding` — only for genuinely multi-namespace installs. |
| `rbac.extraNamespaces` | list | `[]` | Additional namespaces (besides `config.namespace`) to grant a `Role`/`RoleBinding` in, when `rbac.namespaced=true`. |
| `networkPolicy.enabled` | bool | `false` | Renders a default-deny `NetworkPolicy` scoped to DNS, kube-apiserver, and slurmrestd. Requires a NetworkPolicy-capable CNI. |
| `configSource` | string | `file` | `file` renders `config` into a ConfigMap and passes `--config`. `cr` reads WorkloadMixing CR(s) instead and skips the ConfigMap entirely: supervisor mode (all CRs in the release namespace, ADR-0015) when `workloadmixing.name` is empty, single-CR binding via `--workloadmixing` when set. |
| `workloadmixing.namespace` | string | `""` | Namespace of the single bound `WorkloadMixing` CR; defaults to the release namespace. Only used when `configSource=cr` AND `workloadmixing.name` is set (supervisor mode always watches the release namespace; setting a different namespace without `name` fails at template time). |
| `workloadmixing.name` | string | `""` | Name of the `WorkloadMixing` CR to bind to. Only used when `configSource=cr`. EMPTY (default) selects supervisor mode — one reconcile loop per CR in the release namespace. |
| `slurmTokenSecret` | string | `"slurm-rest-token"` | Name of the pre-existing Secret holding the slurmrestd JWT; mounted read-only at `/var/run/secrets/slurm`. |
| `config.slurmRestURL` | string | `https://slurm-restapi.slurm.svc.cluster.local:6820` | slurmrestd base URL. Must be `https://` unless `allowInsecureHTTP: true` — the token is bearer-equivalent. |
| `config.allowInsecureHTTP` | bool | unset | Explicit opt-in to a plaintext `http://` `slurmRestURL` (development only). |
| `config.slurmCACertFile` | string | unset | Path to a mounted CA bundle verifying slurmrestd's TLS cert; empty uses system roots. |
| `config.slurmInsecureSkipTLSVerify` | bool | unset | Disables slurmrestd TLS verification. Development escape hatch only. |
| `config.slurmTokenFile` | string | `/var/run/secrets/slurm/token` | Path the controller reads the mounted JWT from — must match where `slurmTokenSecret` is mounted (it does, by default). |
| `config.slurmUser` | string | `""` | Sent as `X-SLURM-USER-NAME`. Empty (the default) omits the header entirely, so slurmrestd uses the user the mounted JWT was minted for — the deliberately safe choice (an unknown/fake user 422s every job update, found live in e2e testing). Set this only to a dedicated low-privilege Slurm user you have actually provisioned and whose token you mount — never an arbitrary name. |
| `config.namespace` | string | `slurm-jobs` | Namespace where slurmd JobSets are created. |
| `config.localQueue` | string | `main` | Kueue LocalQueue JobSets are submitted to. |
| `config.pollInterval` | string | `10s` | Reconcile tick interval (Go duration). Enforced minimum: `1s`. |
| `config.enablePrioritySync` | bool | unset | Experimental Slurm&lt;-&gt;Workload priority mirror; default off (see ADR-0009). |
| `config.maxUserPriority` | int | `10000` | Caps user-originated priority requests so no job owner can jump the whole mixed queue. `0` disables the extra cap. |
| `config.slurmRequestTimeout` | string | `30s` (default when unset) | Per-request slurmrestd HTTP timeout (Go duration). Controller enforces `1s`..`10m`. The `GET /jobs` payload grows with total queue depth (no server-side paging), so raise this for deliberately large backlogs. |
| `config.slurmRequestsPerSecond` | number | `0` (unlimited) | Client-side rate limit toward slurmrestd itself — distinct from `kubeClient.qps`/`.burst` below, which limits calls to kube-apiserver. Set this on shared/rate-limited slurmrestd deployments. |
| `config.createWorkers` | int | `8` (default when unset) | Bounded worker pool size for parallelizing JobSet creation across held jobs (P5). The client-go QPS/Burst ceiling still bounds actual throughput regardless of pool size. |
| `config.partitionMappings` | list | one entry: `mixing` → `normal-priority` | Slurm partition → Kueue `WorkloadPriorityClass` mappings; at least one required. Each entry may also set `localQueue` (backlog A1b) to route that partition's JobSets to a different Kueue `LocalQueue` than `config.localQueue` — multi-team support; omit to use the global queue. |
| `config.slurmd.image` | string | `ghcr.io/slinkyproject/slurmd:26.05-ubuntu26.04` | Privileged slurmd image; must match an `allowedSlurmdImages` prefix. Digest-pin before production. |
| `config.slurmd.confServer` | string | `slurm-controller.slurm.svc.cluster.local:6817` | slurmctld address slurmd registers against. |
| `config.slurmd.authSecretName` | string | `slurm-auth-slurm` | Secret holding `slurm.key`, mounted into slurmd pods. |
| `config.slurmd.gpuResourceName` | string | `nvidia.com/gpu` (default when unset) | The Kubernetes extended resource requested when a Slurm job asks for GRES GPUs. |
| `config.slurmd.privileged` | bool | `true` (default when unset) | Whether slurmd pods run privileged (needs writable cgroups); set `false` to trial a minimal-capability slurmd image. |
| `config.slurmd.sharedStorage` | object | unset | `{nfsServer, nfsPath, mountPath}` — mounts a shared filesystem (e.g. `/home`) into every slurmd pod. |
| `config.topology` | object | unset | `{requiredLevel, preferredLevel}` node-label keys for Kueue Topology-Aware Scheduling translation. |

All `config.*` rows above are only used when `configSource=file`; ignored
(and the ConfigMap not rendered) when `configSource=cr` — in that mode the
equivalent fields live on the `WorkloadMixing` CR's `spec` instead.

| `kubeClient.qps` | number | `20` | `--kube-api-qps` — steady-state rate ceiling for the controller's Kubernetes API client. Always in effect regardless of `configSource`. Lower on clusters whose admission webhooks struggle under a JobSet-creation burst. |
| `kubeClient.burst` | int | `40` | `--kube-api-burst`. Must be `>= kubeClient.qps`. |

## Uninstall

```
helm uninstall k8s-bridge -n slurm-jobs
```

The `WorkloadMixing` CRD is left behind (Helm's CRD lifecycle policy); remove
it manually with `kubectl delete crd workloadmixings.k8s-bridge.x-k8s.io` if
no longer needed. The slurmrestd token Secret is not chart-managed and is
also left behind.
