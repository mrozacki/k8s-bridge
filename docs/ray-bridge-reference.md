# ray-bridge configuration reference

Audience: platform operators configuring ray-bridge, and the engineering
team taking it toward production. ray-bridge brings the *inner workloads* of
a shared, long-lived KubeRay `RayCluster` under Kueue admission — the Ray
mirror of k8s-bridge for Slurm (ADR-0006, ADR-0012, ADR-0013). This document
is the field-by-field reference; `docs/installation.md` covers install
steps and `docs/architecture.md`/the ADRs cover design rationale.

**Read this first if you are deciding whether to enable the webhook:** see
§5, "Enforcement caveat" — it is the single most security-relevant decision
in this controller.

## 1. Config file reference (`internal/rayconfig.Config`)

Loaded from YAML (`--config`, default `config/ray-example-config.yaml`), or
rendered from the Helm chart's `config` values
(`deploy/chart/ray-bridge/values.yaml`) into a ConfigMap. Unlike
`internal/config` (the Slurm side), there is no `WorkloadMixing`-CR mode yet
and no `pollInterval` — a RayJob is a native, watchable Kubernetes object,
so ray-bridge is event-driven (ADR-0012), not polling.

| Field | Type | Required | Default | Notes |
|---|---|---|---|---|
| `namespace` | string | yes | — | Namespace ray-bridge watches inner RayJobs in and creates worker JobSets in. |
| `localQueue` | string | yes | — | Global Kueue `LocalQueue` for worker JobSets; a `poolMappings[].localQueue` or the RayJob's own `local-queue` annotation may override it. |
| `managedClusters` | list of `{name, headAddress}` | yes, ≥1 entry | — | The shared `RayCluster`(s) under management. `name` must match the value an inner RayJob puts in `spec.clusterSelector["ray.io/cluster"]`; `headAddress` is the Ray head's GCS `host:port`, used verbatim in the dedicated worker's `ray start --address=<headAddress>`. Both `name` and `headAddress` are required per entry. |
| `poolMappings` | list of `{pool, workloadPriorityClass, localQueue?}` | yes, ≥1 entry | — | Maps an inner RayJob's `ray-bridge.x-k8s.io/pool` annotation to a Kueue `WorkloadPriorityClass` — the exact analog of the Slurm side's `partitionMappings`. `pool` and `workloadPriorityClass` are required per entry; `localQueue` optionally overrides the global `localQueue` for that pool's worker JobSets (multi-team routing). |
| `worker.image` | string | yes | — | Container image running `ray start` in the dedicated worker pods. Must match an `allowedWorkerImages` prefix (chart/controller trust anchor, not settable from this config). |
| `worker.gpuResourceName` | string | no | `nvidia.com/gpu` | Kubernetes extended resource requested when an inner workload asks for GPUs. |
| `worker.defaultWorkers` | int32 | no | `1` | Worker pod count used when the RayJob omits `ray-bridge.x-k8s.io/workers`. Must not be negative (rejected at load). |
| `worker.defaultCpus` | int64 | no | `1` | CPU cores per worker pod, used when the RayJob omits `ray-bridge.x-k8s.io/worker-cpus`. Must not be negative. |
| `worker.defaultMemoryMB` | int64 | no | `1024` | Memory (MB) per worker pod, used when the RayJob omits `ray-bridge.x-k8s.io/worker-memory`. Must not be negative. |
| `worker.privileged` | bool | no | `false` | Whether worker pods run privileged. Unlike Slurm's `slurmd` (which needs privileged cgroup access), a Ray worker does not need this — exposed only for parity/escape-hatch. |
| `worker.preferredTopology` | string | no | unset | Node-label topology level (e.g. a rack label) applied as a Kueue TAS `podset-preferred-topology` to every worker JobSet that does not request a hard topology via the RayJob's `required-topology` annotation (R8, mirrors the Slurm side's `topology.preferredLevel`, ADR-0008). |

**Validation** (`rayconfig.Config.Validate`, run after `ApplyDefaults`):
`namespace`/`localQueue` non-empty; at least one `managedClusters` entry with
non-empty `name`/`headAddress`; at least one `poolMappings` entry with
non-empty `pool`/`workloadPriorityClass`; `worker.image` non-empty;
`worker.defaultWorkers`/`defaultCpus`/`defaultMemoryMB` not negative (a
config file with a literal negative survives `ApplyDefaults`, which only
replaces exact zeros, so `Validate` catches it explicitly rather than
letting it silently ride on a downstream per-job clamp).

## 2. The annotation contract (`internal/ray/rayjob.go`)

An inner RayJob declares its dedicated worker capacity via annotations — the
`sbatch` flags analog. All are optional except `pool` (which is required in
effect: an unmapped/missing pool is denied by the webhook, or simply never
picked up by the reconciler without it).

| Annotation | Constant | Type | Default when absent | Meaning |
|---|---|---|---|---|
| `ray-bridge.x-k8s.io/pool` | `PoolAnnotation` | string | empty (unmanaged) | Selects the `WorkloadPriorityClass` via `poolMappings`. Required in practice — see above. |
| `ray-bridge.x-k8s.io/workers` | `WorkersAnnotation` | base-10 integer | `worker.defaultWorkers` | Dedicated worker pod count. Clamped to a minimum of 1 if a non-negative value below 1 sneaks through. |
| `ray-bridge.x-k8s.io/worker-cpus` | `WorkerCPUsAnnotation` | base-10 integer | `worker.defaultCpus` | CPU cores per worker pod. Clamped to a minimum of 1. |
| `ray-bridge.x-k8s.io/worker-gpus` | `WorkerGPUsAnnotation` | base-10 integer | `0` | GPUs per worker pod. Clamped to a minimum of 0 (negative values are floored, not rejected). |
| `ray-bridge.x-k8s.io/worker-memory` | `WorkerMemoryAnnotation` | Kubernetes quantity (e.g. `4Gi`) | `worker.defaultMemoryMB` | Memory per worker pod; parsed with `resource.ParseQuantity` and converted to MB (floored at 1 MB). |
| `ray-bridge.x-k8s.io/local-queue` | `LocalQueueAnnotation` | string | pool's/global `localQueue` | Overrides the target Kueue `LocalQueue` for this job's worker JobSet. |
| `ray-bridge.x-k8s.io/required-topology` | `RequiredTopologyAnnotation` | string | unset (no hard constraint) | Node-label topology level the dedicated workers must all share — translated to a Kueue TAS `podset-required-topology` (R8, the analog of the Slurm side's `--switches`, ADR-0008). Validated live on GKE 2026-07-07: co-locates worker pods in one rack. |
| `ray-bridge.x-k8s.io/worker-retries` | `WorkerRetriesAnnotation` | base-10 integer | `0` | **Written BY ray-bridge onto the RayJob**, not read from the submitter — counts how many times a Failed worker JobSet has been retried (§4). Not a submission-time input; documented here because operators will see it on the RayJob. |

**Parsing discipline** (`ray.FromUnstructured`): a *missing* annotation
silently takes the default; a *present but malformed* value (non-integer for
the count fields, unparseable quantity for memory) is a hard error — a typo
surfaces instead of silently mis-sizing capacity. Integer parsing is strict
base-10 over the whole trimmed string (`strconv.ParseInt` on the whole
field, not `fmt.Sscan`'s partial-match semantics), so `"3abc"` or `"0x10"`
are rejected rather than silently interpreted as `3` or `16`.

An inner RayJob is only in scope for ray-bridge when it targets an existing
`RayCluster` via `spec.clusterSelector["ray.io/cluster"]`
(`RayJob.IsInnerWorkload()`) — a RayJob with an inline `rayClusterSpec` (its
own cluster) is already covered by Kueue's native RayJob integration and is
untouched by ray-bridge or its webhook.

## 3. The pin-gate mechanism, in short (ADR-0013)

ray-bridge does **not** gate inner workloads with `spec.suspend` — KubeRay
forbids `spec.suspend` in `clusterSelector` mode outright (a live finding
that superseded the original ADR-0006/0012 design). Instead:

1. The inner RayJob's `spec.entrypointResources` must request one unit of
   `wm-job-<metadata.name>` (a JSON-string-encoded object per KubeRay's API
   shape, e.g. `'{"wm-job-my-job": 1}'`). This is the pin.
2. ray-bridge creates a dedicated worker JobSet, submitted to Kueue. Once
   Kueue admits it, its pods `ray start` into the shared cluster advertising
   exactly `wm-job-<name>`.
3. Ray's own scheduler cannot place the driver until a worker advertising
   that resource exists — so the job waits for Kueue admission without any
   custom suspend/hold state machine.
4. On RayJob termination, ray-bridge deletes the worker JobSet.

The pin name is computed by `raytranslate.PinResource()` as
`fmt.Sprintf("wm-job-%s", job.Name)` — always derived from
`metadata.name`, never an independently settable field. Renaming a RayJob
(delete + recreate under a new name) requires updating the pin string too,
with or without the webhook.

## 4. Bounded retry of failed worker JobSets

If a dedicated worker JobSet reaches a `Failed=True` condition (e.g. its own
`FailurePolicy.MaxRestarts` was exhausted), the reconciler
(`internal/raybridge/reconciler.go`, `handleFailedWorkerJobSet`) retries it
up to `MaxWorkerRetries` (**3**) times:

- **Retry attempt (retries < 3):** increments `ray_bridge_worker_jobsets_failed_total`,
  bumps the RayJob's `ray-bridge.x-k8s.io/worker-retries` annotation, emits a
  `WorkerJobSetFailed` Warning Event naming the JobSet and the failure
  reason, then deletes the failed JobSet so the next reconcile recreates a
  fresh one.
- **Budget exhausted (retries == 3):** counts the final failure, bumps the
  annotation to the "already signalled" sentinel value, emits a final
  `WorkerRetriesExhausted` Warning Event, and — deliberately — **leaves the
  failed JobSet in place** for operator inspection (its pods/conditions are
  evidence; deleting it would erase that). Recovery: inspect the JobSet,
  then remove the RayJob's `worker-retries` annotation and delete the failed
  JobSet yourself to re-arm the retry loop.

Identity is checked via the RayJob-UID label before any delete: a stale
JobSet left by a deleted-and-recreated RayJob of the same name is never
touched by this path.

## 5. Webhook behavior (`internal/raywebhook`)

### Enforcement caveat — read this before deciding to skip the webhook

**The admission webhook is the only enforcement point for the pin gate.**
`--enable-webhook` defaults to **false**. With it off, `cmd/ray-bridge`
logs a prominent startup warning:

> ADMISSION WEBHOOK DISABLED (--enable-webhook=false, the default): nothing
> injects the pin resource into inner RayJobs, so any inner RayJob that does
> not self-declare entrypointResources will run on the shared RayCluster
> WITHOUT passing Kueue admission, while the bridge still creates dedicated
> workers for it.

In other words: without the webhook, an honest submitter who hand-writes
`spec.entrypointResources` is still gated correctly, but nothing stops a
different (or careless) submitter from omitting it and running unpinned,
straight past Kueue. Enable the webhook (`docs/installation.md` §5.1) in any
environment where you cannot trust every submitter to self-declare the pin.

### `Decide()` semantics (`internal/raywebhook/webhook.go`)

`Decide(job, cfg)` is a pure function, evaluated in this order:

1. **Not an inner workload** (no `clusterSelector`) → `Decision{}` — allowed
   untouched (own-cluster RayJobs are Kueue-native and out of scope).
2. **Inner workload targeting an unmanaged cluster** (not in
   `managedClusters`) → `Decision{}` — allowed untouched (not this
   deployment's cluster).
3. **Inner workload targeting a managed cluster, pool unmapped or missing**
   → `Decision{Deny: true, Reason: ...}` — denied outright, so a
   misconfigured submission fails fast at `kubectl apply` instead of
   silently sitting unpicked-up.
4. **Inner workload targeting a managed cluster, pool mapped** →
   `Decision{InjectPin: true, PinName: "wm-job-<name>"}` — the pin is merged
   into `spec.entrypointResources` (preserving any resources the submitter
   already requested; a submitter-provided `entrypointResources` that is
   **not valid JSON** is denied rather than silently overwritten, so a typo
   surfaces instead of losing the submitter's own resource requests).

Every `Handle()` call increments `ray_bridge_webhook_decisions_total{decision}`
with exactly one of three label values (`raybridge.WebhookDecision*`
constants):

| Decision label | When |
|---|---|
| `pinned` | The pin was successfully injected/merged (case 4 above). |
| `denied` | The RayJob was rejected — unmapped/missing pool (case 3), a malformed worker-shape annotation caught during parsing, or non-JSON `entrypointResources`. |
| `skipped` | The RayJob was allowed untouched because it is out of scope (cases 1–2). |

A flat, unchanging `pinned` rate alongside ongoing RayJob submissions is the
operational signature of the webhook-disabled bypass in §5's caveat: inner
jobs are reaching the cluster without ever going through a `pinned`
decision. (Server-side decode/marshal errors inside `Handle` — as opposed to
a decision about the RayJob — are deliberately **not** counted here;
controller-runtime's own webhook response-code metrics already cover those.)

The webhook only intercepts `CREATE` operations on `ray.io/v1` `RayJob`
objects in namespaces labeled `ray-bridge.x-k8s.io/managed: "true"`, per its
`MutatingWebhookConfiguration`'s `namespaceSelector`/`rules`.

## 6. `cmd/ray-bridge` flags

| Flag | Default | Meaning |
|---|---|---|
| `--config` | `config/ray-example-config.yaml` | Path to the YAML config file (§1). |
| `--metrics-addr` | `:8080` | Prometheus `/metrics` listen address; empty disables it. |
| `--health-addr` | `:8081` | `/healthz`/`/readyz` listen address; empty disables both. |
| `--log-level` | `info` | `debug`, `info`, `warn`, or `error`. |
| `--leader-elect` | `true` | `coordination.k8s.io` Lease before starting the reconciler — a second replica sits idle until it wins. |
| `--leader-lease-duration` | `15s` | Leader-election lease duration (short, per the L9 rollout-deadlock fix — pairs with the chart's `Recreate` Deployment strategy). |
| `--leader-renew-deadline` | `10s` | Leader-election renew deadline. |
| `--allowed-worker-images` | empty (allow any) | Comma-separated allowlist of permitted Ray worker image prefixes — a controller-level trust anchor, deliberately not settable via `config`. |
| `--enable-webhook` | `false` | Serve the pin-injecting admission webhook on `:9443`. Requires a serving certificate. **See §5's enforcement caveat before leaving this off in production.** |
| `--kube-api-qps` | `20` | client-go QPS ceiling for the Kubernetes API client. |
| `--kube-api-burst` | `40` | client-go burst ceiling; must be ≥ `--kube-api-qps`. |

## 7. Metrics

Served on `--metrics-addr` (default `:8080`), registered on
controller-runtime's own metrics registry — so `/metrics` also carries the
CNCF-standard `rest_client_*`, `workqueue_*`, and
`controller_runtime_reconcile_*` series for free. Domain metrics
(`internal/raybridge/metrics.go`), namespaced `ray_bridge_*` (distinct from
the Slurm side's `k8s_bridge_*` — these are separate binaries with separate
metric namespaces):

- `ray_bridge_worker_jobsets_created_total` (counter) — worker JobSets
  created, including retry recreations.
- `ray_bridge_worker_jobsets_failed_total` (counter) — worker JobSet
  `Failed` conditions the reconciler handled (retried or declared
  exhausted) — see §4.
- `ray_bridge_reconcile_errors_total` (counter) — reconcile passes that
  returned an error (and were requeued by controller-runtime).
- `ray_bridge_webhook_decisions_total{decision}` (counter) — admission
  decisions by outcome (`pinned`/`denied`/`skipped`, §5); only populated
  when `--enable-webhook` is set.

## 8. Probe endpoints

| Endpoint | Port (default) | Behavior |
|---|---|---|
| `/healthz` | `:8081` (`--health-addr`) | Pure process liveness (`healthz.Ping`) — mirrors the Slurm side's deliberately permissive liveness stance; a struggling dependency should page the operator, not restart the pod. |
| `/readyz` | `:8081` | Gates on real startup milestones, not a bare ping: the manager's RayJob and JobSet informer caches must finish their initial LIST+WATCH sync on *this* replica (leader or standby — informers are created eagerly before the manager starts, so a standby's readiness is meaningful too); when `--enable-webhook` is set, it additionally waits for the webhook server to actually be listening (`StartedChecker`). A pod that never reaches Ready usually means the apiserver/RBAC is unreachable for the informer sync, or — with the webhook enabled — the serving certificate is missing/invalid. |
| `/metrics` | `:8080` (`--metrics-addr`) | Prometheus metrics, §7. |
| webhook server | `:9443` | Only listens when `--enable-webhook` is set; needs a serving certificate mounted at the standard controller-runtime webhook cert path. |

_Related reading: `docs/installation.md` §5 (install steps, chart-native
cert-manager automation for the webhook), `docs/operations.md` (metrics
reference alongside the Slurm side's, alert guidance), `docs/adr/0006`,
`docs/adr/0012`, `docs/adr/0013` (design rationale and alternatives
considered), `deploy/ray-bridge/README.md` (submission-convention examples,
gotchas)._
