# ray-bridge deployment

This directory holds the deployment manifests for **ray-bridge**, the Ray
mirror of k8s-bridge: it brings the *inner workloads* of a shared, long-lived
KubeRay `RayCluster` under Kueue admission, the same way k8s-bridge does for
Slurm jobs. Background: `docs/adr/0002-ray-and-inference-scope.md`
(scope), `docs/adr/0013-ray-pin-gate-admission.md` (the admission mechanism
this doc explains how to use).

| File | Purpose |
|---|---|
| `manifests.yaml` | Core controller: ServiceAccount, RBAC, ConfigMap, Deployment. Always required. |
| `webhook.yaml` | Optional `MutatingWebhookConfiguration` that auto-injects the pin (see below). Needs a serving certificate, so it is deployed separately from the core controller. |
| `inner-rayjob-sample.yaml` | Sample inner `RayJob` for the **with-webhook** case. |

See also `experiments/10-ray-bridge/manifests/inner-rayjob-annotated.yaml` for
the fully-commented **without-webhook** (manual) sample, and
`experiments/10-ray-bridge/README.md` for a validated end-to-end walkthrough
on `kind`.

## Submitting an inner workload

An "inner workload" is a `RayJob` that attaches to an **existing** shared
`RayCluster` via `spec.clusterSelector` (as opposed to a `RayJob` that
provisions its own cluster from an inline `rayClusterSpec` — that case is
already covered by Kueue's native RayJob integration and is out of scope for
ray-bridge; see `internal/ray.RayJob.IsInnerWorkload()`).

ray-bridge does **not** gate inner workloads with `spec.suspend`. An early
design (ADR-0006/0012) assumed it would, but a live run against real KubeRay
falsified that: **KubeRay forbids `spec.suspend` in `clusterSelector` mode
outright** ("the ClusterSelector mode doesn't support the suspend
operation"). ADR-0013 replaced it with the **pin-gate model**, described
below. This is the only supported submission convention — there is no
suspend-based fallback.

### The pin-gate mechanism, in short

1. The inner `RayJob` carries `spec.entrypointResources:
   '{"wm-job-<name>": 1}'`, where `<name>` is the RayJob's own
   `metadata.name`. This is a request for one unit of a custom Ray resource
   that (initially) nobody advertises.
2. ray-bridge creates a dedicated worker `JobSet` for this RayJob, submitted
   to Kueue for admission. Once Kueue admits it, its pods `ray start` into
   the shared cluster's head advertising exactly `wm-job-<name>` (sized to
   their CPU request).
3. Ray's own scheduler cannot place the RayJob's driver until a worker
   advertising `wm-job-<name>` exists — so the job **waits for Kueue
   admission** even though nothing about it says "suspended". No custom hold
   state machine needed; Kueue quota is the gate, the pin resource is the
   coupling.
4. When the RayJob reaches a terminal state, ray-bridge deletes the worker
   JobSet; its pods leave the cluster and the pin resource disappears with
   them.

The pin name is computed by `raytranslate.PinResource()`
(`internal/raytranslate/translate.go`) as `fmt.Sprintf("wm-job-%s",
job.Name)` — i.e. **always** `wm-job-<metadata.name>`, never anything else.

### (a) Without the webhook — set the pin and annotations yourself

This is the common case: the webhook needs a serving certificate
(cert-manager or a manually mounted one), which many environments won't have
provisioned yet. Without it, you are responsible for:

1. **The pin.** Set `spec.entrypointResources` yourself:

   ```yaml
   spec:
     entrypointResources: '{"wm-job-<metadata.name>": 1}'
   ```

   Note this is a **JSON string**, not a nested YAML object — a KubeRay API
   quirk, not a ray-bridge one.

2. **The ray-bridge capacity annotations**, which tell ray-bridge how much
   dedicated worker capacity to stand up for this job (the RayJob-side
   analog of Slurm's `--partition` / `--ntasks` / `--cpus-per-task`):

   | Annotation | Meaning | Required? |
   |---|---|---|
   | `ray-bridge.x-k8s.io/pool` | Pool name, must map to a `WorkloadPriorityClass` in the ray-bridge config's `poolMappings` | Effectively yes — an inner workload with no mapped pool is never picked up (and is **denied** at submit time if the webhook is enabled) |
   | `ray-bridge.x-k8s.io/workers` | Number of dedicated worker pods | No — defaults to the config's `worker.defaultWorkers` |
   | `ray-bridge.x-k8s.io/worker-cpus` | CPU cores per worker pod | No — defaults to `worker.defaultCpus` |
   | `ray-bridge.x-k8s.io/worker-gpus` | GPUs per worker pod | No — defaults to 0 |
   | `ray-bridge.x-k8s.io/worker-memory` | Memory per worker pod (k8s quantity, e.g. `4Gi`) | No — defaults to `worker.defaultMemoryMB` |
   | `ray-bridge.x-k8s.io/local-queue` | Override the target Kueue `LocalQueue` | No — defaults to the pool's mapped queue or the config's global `localQueue` |

   See `internal/ray/rayjob.go` for the exact annotation constants and
   defaulting/validation rules (a present-but-malformed value is a hard
   error; a missing one silently takes the default).

A complete, heavily commented example:
`experiments/10-ray-bridge/manifests/inner-rayjob-annotated.yaml`.

### (b) With the webhook — auto-injected

If `deploy/ray-bridge/webhook.yaml` is installed (serving certificate
provisioned, ray-bridge built/run with the webhook wired in via
`raywebhook.Handler.SetupWithManager`, and your namespace carries the
`ray-bridge.x-k8s.io/managed: "true"` label the webhook's
`namespaceSelector` requires), you can omit `spec.entrypointResources`
entirely. On `CREATE`, the webhook (`internal/raywebhook/handler.go`):

- parses the RayJob and applies the same `Decide()` rules described above
  (`internal/raywebhook/webhook.go`);
- if the pool is unmapped or missing, **denies** the RayJob outright, so a
  misconfigured submission fails fast at `kubectl apply` time instead of
  silently sitting unpicked-up;
- otherwise, computes `wm-job-<metadata.name>` and **merges** it into
  `spec.entrypointResources` (preserving any Ray resources you did request
  yourself).

You still set the `ray-bridge.x-k8s.io/*` capacity annotations yourself —
the webhook only injects the pin, not the worker shape.

Example: `deploy/ray-bridge/inner-rayjob-sample.yaml`.

> Note: `webhook.yaml`'s header comment currently describes an older
> "auto-suspend" design (patching `spec.suspend=true`). The deployed
> `Handler.Handle` logic has moved on to pin injection per ADR-0013; the
> comment block is stale and tracked for a docs cleanup. This README
> reflects the current, actual behavior.

### (c) Gotcha: the pin name must equal the RayJob name

`wm-job-<name>` is derived **only** from `metadata.name` — there is no
independent "pin id" field. Two consequences:

- If you copy-paste a manifest and rename `metadata.name` but forget to
  update the `entrypointResources` string to match, the driver will pend
  **forever** with no error: it is waiting for a pin that ray-bridge's
  workers will never advertise (they advertise a pin derived from the *new*
  name). This is the single most common manual-submission mistake.
- Renaming a RayJob (i.e. deleting and recreating under a new name) requires
  updating the pin string too, even with the webhook — the webhook computes
  the pin fresh from whatever `metadata.name` is on that particular request,
  it does not remember a prior name.

### (d) Gotcha: it must NOT be suspended

Never set `spec.suspend` on an inner RayJob. KubeRay validates this at
admission and **rejects** the RayJob outright in `clusterSelector` mode,
regardless of `shutdownAfterJobFinishes`. This is a hard KubeRay limitation,
not a ray-bridge choice — see ADR-0013 for the exact validation errors hit
during live testing. Both sample manifests in this repo omit
`spec.suspend` on purpose; do not add it back.

## Related reading

- `docs/adr/0013-ray-pin-gate-admission.md` — the ADR this doc implements;
  read it for the full context, alternatives considered, and open items
  (notably: the pin gates the *driver* only, not every task/actor the job
  spawns — full isolation is a follow-up).
- `docs/VALIDATION.md` — the live evidence behind ADR-0013.
- `experiments/10-ray-bridge/README.md` — validated `kind` walkthrough,
  including the exact `kubectl`/`helm` commands to reproduce the pin-gate
  behavior end to end.
