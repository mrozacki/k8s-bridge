# ADR-0012: ray-bridge topology — separate binary, shared library, watch-based

- **Status:** Accepted (2026-07-06). The topology
  decisions (separate binary, shared library, event-driven) stand; the
  `spec.suspend` ADMISSION MECHANISM in Decision 3 / the open items is
  superseded by **ADR-0013** (pin-gate) after a live finding (2026-07-07):
  KubeRay forbids suspend in clusterSelector mode.
- **Implements:** ADR-0006 (Ray inner-workload admission — the *why*). This ADR
  records the *how*: packaging, control model, and the boundary of the code
  shared with the Slurm-side k8s-bridge.
- **Related:** ADR-0002 (Ray scope), ADR-0011 (controller-runtime Manager).

## Context

ADR-0006 established the target: a shared, long-lived `RayCluster` must have
its **inner workloads** admitted through Kueue at per-job granularity — the
same pattern k8s-bridge applies to individual Slurm jobs, not the Slurm
cluster. Nothing was built (the prototype is Slurm-only). This ADR captures
the three packaging/control decisions taken to start the build, and why each
alternative was set aside. We explicitly chose to go straight to a code
MVP (skipping a local de-risk spike) and to skip the interim
capacity-gating variant (Kueue pod integration) in favour of the full
per-inner-workload model.

## Decisions

### 1. Separate `ray-bridge` binary + a shared internal library

`ray-bridge` ships as its **own binary and Deployment**, distinct from
`k8s-bridge`, sharing a common library with it. Concretely, in this repo that
is realised as a **second `cmd/` in the same Go module** (`cmd/ray-bridge`
alongside `cmd/k8s-bridge`), with the system-agnostic code promoted into
shared `internal/` packages (starting with `internal/kueue` for the Kueue
admission mechanics both bridges depend on). Two binaries, two Deployments,
two independent reconcile lifecycles — one module, one shared library.

- **Alternative — one binary hosting two controllers (rejected by owner).**
  Simplest single-admission-point story (both controllers literally share one
  process, one ClusterQueue, one metrics endpoint). Rejected in favour of
  independent lifecycle/rollout per bridge and a cleaner separation of RBAC
  (the Ray binary never needs slurmrestd credentials; the Slurm binary never
  needs RayJob permissions).
- **Alternative — a genuinely separate Go module (`prototype/ray-bridge/`
  with its own `go.mod`) importing a published shared module.** Cleanest
  boundary on paper, but forces the shared code to become a versioned/`replace`
  module and doubles dependency-management ceremony. Deferred: the
  second-cmd-same-module layout already delivers two binaries + a shared
  library; splitting modules can happen later if the two bridges ever need to
  release from separate repos.

**Shared vs bridge-specific — the boundary.** The genuinely system-agnostic
part is the **Kueue mechanics**: reading a Workload's `Admitted` condition
(the admission gate), rendering admission status, the Workload GVK, and the
watch-source used to nudge a reconcile. Everything else is
system-specific — slurmrestd polling and JobSet-of-slurmd on one side, RayJob
watching and JobSet-of-Ray-workers on the other.

**Transitional duplication — RESOLVED (2026-07-06).** The shared
`internal/kueue` package was introduced and consumed by `ray-bridge` first, to
avoid destabilising the live-validated Slurm path while standing it up. The
follow-up migration then landed the same batch: `internal/bridge` now delegates
`IsAdmitted`, the Workload GVKs and the watch source to `internal/kueue` (the
thin-surface guard confines the Kueue API there), verified by the full Slurm
unit + envtest suite. No duplication remains.

### 2. Event-driven (watch), not polling

The Slurm side must poll because slurmrestd cannot be watched (MVD §9 names
polling as technical debt). Ray's inner workloads are **`RayJob` custom
resources — native Kubernetes objects, watchable**. `ray-bridge` is therefore
a standard controller-runtime reconciler triggered by RayJob events, with only
a periodic resync as a floor. This fixes at the source the limitation the MVD
had to accept for Slurm.

- **Alternative — mirror the Slurm polling loop for symmetry.** Rejected:
  polling a watchable resource is strictly worse (latency + API load) and
  would copy a known piece of debt into new code.

### 3. Admission unit — a JobSet of dedicated Ray worker pods

For each admitted inner workload, `ray-bridge` creates a **`JobSet` of
dedicated worker pods** that `ray start` into the shared cluster advertising a
unique Ray custom resource `wm-job-<id>`, labelled for Kueue exactly like the
Slurm-side slurmd JobSet. Kueue admits that JobSet against the shared
ClusterQueue (competing with Slurm JobSets — the single admission point); the
RayJob is unsuspended only after those workers join, with its
`entrypointResources` requesting `wm-job-<id>` so Ray's scheduler pins it to
exactly that capacity.

- **Alternative — patch a `workerGroupSpec` into the shared RayCluster CR.**
  KubeRay would create the pods, but they would not be a per-job Kueue
  workload (only pod-integration capacity gating — the interim variant the
  owner declined). Rejected: loses per-inner-job priority/preemption, which
  is the whole point of ADR-0006.

## Consequences

- **Two Deployments, two Helm values sections / charts, two RBAC sets.** The
  Ray RBAC needs `ray.io` RayJob get/list/watch/patch and JobSet + (cached)
  Workload access; it needs no slurmrestd secret.
- **RayJob handled as `unstructured`** (thin-surface principle, matching how
  `internal/bridge` treats Kueue Workloads) — no heavy KubeRay Go-module
  dependency in `go.mod`.
- **Open validation items carried over from skipping the R0 spike** — to be
  confirmed against a live KubeRay cluster before this is called done:
  1. A bare pod running `ray start --address=<head>` reliably joins an
     existing RayCluster as a worker, and its readiness signal (used for
     Kueue gang scheduling) is well-defined.
  2. `entrypointResources={"wm-job-<id>": N}` pins the job's work to the
     dedicated workers — and how completely (entrypoint task only vs. all
     tasks/actors the job spawns).
  3. KubeRay's autoscaler does not fight externally-added workers (the shared
     cluster's mixing worker groups likely need autoscaling disabled).
- **A submission convention is required** (the JobSubmit-plugin analog):
  inner workloads must arrive as `RayJob` CRs with `suspend: true` targeting a
  managed cluster via `clusterSelector`. MVP relies on convention; a
  validating webhook that auto-suspends and rejects unsupported shapes is a
  hardening follow-up.
