# ray-bridge Helm chart

Deploys the ray-bridge controller: the Ray-ecosystem sibling of the
k8s-bridge chart (ADR-0012 — separate binary, separate Deployment, separate
least-privilege RBAC). ray-bridge brings the *inner workloads* of a shared,
long-lived KubeRay `RayCluster` under Kueue admission, using the pin-gate
model (ADR-0013) — see `docs/adr/0002-ray-and-inference-scope.md` for scope
and `docs/adr/0013-ray-pin-gate-admission.md` for the admission mechanism.

This chart is experimental (prototype phase, chart version 0.2.0) — expect
breaking changes between minor versions until a 1.0 is cut. It supersedes
the plain manifests in `deploy/ray-bridge/` (`manifests.yaml`, `webhook.yaml`),
which are now deprecated in this chart's favor — see the header comments on
those two files.

## ⚠️ Without the webhook, Kueue admission is opt-in, not enforced

`webhook.enabled` is **`false` by default**. Read this before you install:

Kueue admission gates an inner RayJob **only because the job carries a pin**
(`spec.entrypointResources: '{"wm-job-<name>": 1}'`) that ray-bridge's
Kueue-admitted worker JobSet satisfies. The pin is not something Kubernetes
or Kueue forces onto a RayJob automatically — it is either:

- **injected by this chart's mutating admission webhook** at submit time
  (`webhook.enabled: true`), or
- **set by the submitter themselves**, by hand, in the RayJob manifest.

**If `webhook.enabled=false` and a submitter simply omits
`spec.entrypointResources`, that inner RayJob's driver schedules onto the
shared `RayCluster` immediately — no Kueue admission wait, no quota check,
no pool mapping, nothing.** It is not denied, not queued, not throttled: it
runs exactly as if ray-bridge were not installed at all. The webhook is the
**only** enforcement point in this design; without it, `poolMappings` and
Kueue quota are a convention submitters can opt out of by doing nothing.
Enable `webhook.enabled: true` (and label your managed namespaces
`ray-bridge.x-k8s.io/managed: "true"`, which the webhook's
`namespaceSelector` requires) for any environment where you need this
enforced rather than merely offered. See "Submitting an inner workload" in
`deploy/ray-bridge/README.md` for the full pin-gate mechanics and the
without-webhook manual procedure.

## What this chart deploys

- `Deployment` — the controller (`replicas: 1` by default; `leaderElect: true`
  by default, so extra replicas sit idle until they win the
  `coordination.k8s.io` Lease instead of double-reconciling).
- `ServiceAccount` + `Role`/`RoleBinding` — namespaced RBAC in both the
  release namespace (leader-election Lease) and `config.namespace` (inner
  RayJobs, worker JobSets, Kueue Workloads) — see the extensive rationale
  comment in `templates/rbac.yaml` on why this can't currently be narrowed
  further.
- `ConfigMap` — the controller's YAML config, always rendered (ray-bridge has
  no CR-based config mode, unlike k8s-bridge).
- When `webhook.enabled: true`:
  - `Service` — fronts the webhook server's port (9443 in-container, 443 on
    the Service).
  - `MutatingWebhookConfiguration` — injects the pin into inner RayJobs at
    `CREATE` time; see the warning above.
  - When additionally `webhook.certManager.enabled: true` (the default once
    the webhook is on) — a self-signed `Issuer` + `Certificate`
    (`cert-manager.io/v1`) providing the webhook's serving cert. Set this
    `false` if your cluster has no cert-manager and you provision the
    `<fullname>-webhook-tls` Secret (`tls.crt`/`tls.key`, same name
    cert-manager would have used — see `ray-bridge.webhookCertSecretName` in
    `templates/_helpers.tpl`) yourself instead.

## Prerequisites

- A Kubernetes cluster with:
  - **KubeRay** installed (the `ray.io/v1` `RayJob`/`RayCluster` CRDs and
    operator), with a shared `RayCluster` already running, matching
    `config.managedClusters[].headAddress`.
  - **JobSet operator** installed (ray-bridge creates
    `jobset.x-k8s.io/v1alpha2` JobSets for dedicated worker capacity).
  - **Kueue** installed, with a `ClusterQueue`/`LocalQueue` and
    `WorkloadPriorityClass` objects matching `config.localQueue` and
    `config.poolMappings[].workloadPriorityClass`.
- If `webhook.enabled: true` and `webhook.certManager.enabled: true` (the
  default combination) — **cert-manager** installed, so the chart's
  `Issuer`/`Certificate` objects actually get reconciled into a serving cert.
- If `webhook.enabled: true` — the namespaces you want gated must carry the
  label `ray-bridge.x-k8s.io/managed: "true"` (the webhook's
  `namespaceSelector`); label them yourself, this chart does not.

## Install

```
# From the parent repo root:
helm install ray-bridge deploy/chart/ray-bridge \
  --namespace ray --create-namespace \
  --set image.repository=<your-registry>/ray-bridge \
  --set image.tag=<your-tag>
```

With the enforcement webhook (requires cert-manager, see Prerequisites):

```
helm install ray-bridge deploy/chart/ray-bridge \
  --namespace ray --create-namespace \
  --set image.repository=<your-registry>/ray-bridge \
  --set image.tag=<your-tag> \
  --set webhook.enabled=true

kubectl label namespace ray ray-bridge.x-k8s.io/managed=true
```

Or with a values file:

```
helm install ray-bridge deploy/chart/ray-bridge -f my-values.yaml
```

## Values

| Key | Type | Default | Description |
|---|---|---|---|
| `image.repository` | string | `ghcr.io/owner/ray-bridge` | Placeholder — set to your registry/repo before installing. |
| `image.tag` | string | `""` | Image tag; falls back to `.Chart.AppVersion` when empty. |
| `image.digest` | string | `""` | Image digest (e.g. `sha256:...`); wins over `tag` when set. |
| `image.pullPolicy` | string | `IfNotPresent` | Standard pull policy. |
| `imagePullSecrets` | list | `[]` | e.g. `[{name: my-registry-cred}]`. Placed on the `ServiceAccount`, matching k8s-bridge's chart. |
| `nameOverride` | string | `""` | Overrides the chart name used in labels. |
| `fullnameOverride` | string | `""` | Overrides the computed release name, letting two installs coexist. |
| `allowedWorkerImages` | list | `["rayproject/"]` | Controller-level allowlist of image-ref prefixes permitted to run as the Ray worker. Empty disables the check (logs a warning). |
| `resources` | object | requests `50m`/`64Mi`, limits `200m`/`128Mi` | Pod resource requests/limits. |
| `replicas` | int | `1` | Deployment replica count. Renamed from an earlier, undeclared `replicaCount` for consistency with the k8s-bridge chart. Only useful above `1` when `leaderElect` is on (the default). |
| `nodeSelector` | object | `{}` | Standard pod `nodeSelector`, passed through verbatim. |
| `tolerations` | list | `[]` | Standard pod `tolerations`, passed through verbatim. |
| `affinity` | object | `{}` | Standard pod `affinity`, passed through verbatim. |
| `podAnnotations` | object | `{}` | Extra annotations merged onto the pod template. |
| `podLabels` | object | `{}` | Extra labels merged onto the pod template. |
| `leaderElect` | bool | `true` | Passes `--leader-elect`: the controller uses a `coordination.k8s.io` Lease so a second replica sits idle instead of double-reconciling. |
| `leaseDurationSeconds` | int | `15` | `--leader-lease-duration`. Kept short (L9) so leadership hands over quickly on a rolling restart. |
| `renewDeadlineSeconds` | int | `10` | `--leader-renew-deadline`. |
| `webhook.enabled` | bool | `false` | Renders the Service + `MutatingWebhookConfiguration` (and, unless `certManager.enabled=false`, the Issuer/Certificate) and passes `--enable-webhook`. **See the warning above before leaving this off in production.** |
| `webhook.certManager.enabled` | bool | `true` | Renders the self-signed `Issuer` + `Certificate`. Only consulted when `webhook.enabled=true`. Set `false` to supply the serving-cert Secret yourself. |
| `webhook.failurePolicy` | string | `Ignore` | `MutatingWebhookConfiguration.webhooks[].failurePolicy`. `Ignore` (not `Fail`) so a webhook outage doesn't block RayJob submission cluster-wide; tighten to `Fail` once the webhook is proven highly available. |
| `rbac.create` | bool | `true` | Renders the `Role`/`RoleBinding`. |
| `config.namespace` | string | `ray` | Namespace where inner RayJobs live and worker JobSets are created. |
| `config.localQueue` | string | `ray-main` | Kueue LocalQueue worker JobSets are submitted to (unless a pool overrides it — see the `ray-bridge.x-k8s.io/local-queue` annotation in `deploy/ray-bridge/README.md`). |
| `config.managedClusters` | list | one entry: `shared-cluster` | `{name, headAddress}` — the shared `RayCluster`(s) ray-bridge manages inner workloads for. At least one required. |
| `config.poolMappings` | list | `batch`→`normal-priority`, `interactive`→`high-priority` | Pool name → Kueue `WorkloadPriorityClass` mappings. At least one required; an inner RayJob with an unmapped pool is denied by the webhook (if enabled) or simply never picked up (if not). |
| `config.worker.image` | string | `rayproject/ray:2.9.0` | Worker pod image; must match an `allowedWorkerImages` prefix. |
| `config.worker.gpuResourceName` | string | `nvidia.com/gpu` | Kubernetes extended resource requested for `ray-bridge.x-k8s.io/worker-gpus`. |
| `config.worker.defaultWorkers` | int | `1` | Default dedicated worker pod count when a RayJob omits `ray-bridge.x-k8s.io/workers`. |
| `config.worker.defaultCpus` | number | `1` | Default CPU cores per worker pod. |
| `config.worker.defaultMemoryMB` | int | `1024` | Default memory (MiB) per worker pod. |

## Uninstall

```
helm uninstall ray-bridge -n ray
```

The webhook's `MutatingWebhookConfiguration` is cluster-scoped but IS
chart-managed (unlike k8s-bridge's `PriorityClass`/CRD, which are
deliberately left behind) and is removed by `helm uninstall` along with
everything else this chart creates.

## Related reading

- `deploy/ray-bridge/README.md` — the full pin-gate submission mechanics,
  RayJob annotations, and the without-webhook manual procedure.
- `docs/adr/0013-ray-pin-gate-admission.md` — the ADR the pin-gate model
  implements.
- `internal/raywebhook/` (parent repo) — the webhook's
  actual decision logic (`Decide()`) and HTTP handler this chart wires up.
