# ADR-0013: Ray inner-workload admission via a pin resource, not suspend

- **Status:** Accepted (live finding, 2026-07-07)
- **Supersedes:** the `spec.suspend`-based admission mechanism proposed in
  ADR-0006 and detailed in ADR-0012. The *goal* (per-inner-workload Kueue
  admission on a shared RayCluster) is unchanged; the *mechanism* changes.
- **Evidence:** live e2e on kind (the suspend finding + gating) and on
  **GKE** (multi-node: run-to-completion, preemption, topology) — see
  `docs/VALIDATION.md`.
- **Validation status (GKE, 2026-07-07):** run-to-completion ✅; preemption by
  Kueue priority ✅ both Ray-vs-Ray AND **cross-type** (a high-pri Ray inner
  workload preempted a low-pri plain K8s batch JobSet in one ClusterQueue —
  Kueue is a single admission point across workload types); topology co-location
  via TAS ✅. **Requeue caveat:** a preempted plain JobSet (K8s/Slurm) is
  Kueue-suspended and requeues gracefully, but a preempted clusterSelector Ray
  inner workload stays FAILED — KubeRay forbids BOTH `suspend` AND `backoffLimit`
  in clusterSelector mode, so it cannot auto-retry. Ray-inner-workload requeue
  needs bridge-driven resubmit or the scheduling-gate alternative below (R5/R7).

## Context

ADR-0006/0012 assumed inner workloads would be submitted as **suspended**
`RayJob`s (`spec.suspend: true`) targeting the shared cluster via
`clusterSelector`, and that ray-bridge would unsuspend them once Kueue admitted
their dedicated worker capacity. The first live run against a real KubeRay
operator **falsified this assumption** with two hard validation errors:

1. `a RayJob with shutdownAfterJobFinishes set to false is not allowed to be
   suspended`
2. `the ClusterSelector mode doesn't support the suspend operation`

KubeRay **does not support `spec.suspend` in `clusterSelector` mode at all.**
The suspend-based mechanism cannot work. (This is also why Kueue's own native
RayJob integration does not cover the in-cluster/clusterSelector case — the
same limitation, noted in ADR-0006.)

The same live run **validated the rest of the model**: a bare pod running
`ray start --address=<head> --resources='{"wm-job-x": N}'` joins the shared
cluster and advertises the custom resource (`ray status`: `0.0/2.0 wm-job-x`);
`ray health-check` is a valid worker readiness signal (exit 0); and Kueue
admits the dedicated worker JobSet.

## Decision: the pin resource IS the admission gate

Drop `spec.suspend` entirely. Gate the inner workload through the **Ray custom
resource** instead:

1. The inner `RayJob` (clusterSelector, **not** suspended) carries
   `entrypointResources: {"wm-job-<id>": 1}` — injected by the admission
   webhook (`internal/raywebhook`) or set by the submitter.
2. ray-bridge creates the dedicated worker `JobSet` (Kueue-labelled) whose
   workers advertise exactly `wm-job-<id>`.
3. Because the RayJob's driver requires `wm-job-<id>`, and that resource only
   exists once **Kueue admits the worker JobSet and the workers join**, the
   driver **cannot be scheduled until admission happens**. Kueue quota gates
   the workers; the pin resource couples the job to them. No suspend needed.
4. On RayJob terminal → ray-bridge deletes the worker JobSet (its pods leave
   the cluster; the pin resource disappears).

The reconciler is now much simpler: ensure the worker JobSet exists per inner
RayJob; clean up on terminal. No hold/release/re-suspend state machine (all of
which relied on the forbidden suspend).

## Consequences

- **The webhook mutates `entrypointResources`, not `spec.suspend`.** It merges
  the pin into any submitter-provided resources.
- **Live-confirmed gating**: a non-suspended RayJob whose pin resource is not
  yet advertised has its driver pend (validated — the job did not run until a
  worker advertising the resource joined).
- **The bypass is still closed**: the concern in ADR-0006 (a clusterSelector
  RayJob runs regardless of quota) is addressed because the driver pends on the
  pin resource, which requires Kueue admission of the workers.
- **Open items carried forward:**
  - **Pin completeness.** `entrypointResources` gates the *driver*. Tasks/actors
    the job spawns do not automatically request `wm-job-<id>`, so they could
    schedule on other cluster capacity once the driver runs. Confining *all* of
    a job's work needs the job to request the resource per-task (job-side
    cooperation) or a Ray scheduling hook — tracked as an open item. The driver
    gate is sufficient for *admission* (no work starts before admission); full
    *isolation* is the follow-up.
  - **Completion not live-validated in kind.** `ray job submit` (used by
    KubeRay's submitter) failed with `No available agent to submit job` — a Ray
    job-agent flakiness in this single-node kind setup, unrelated to the gate.
    End-to-end completion needs a sturdier Ray environment.
- **ADR-0012's "worker join / readiness" open item is now CLOSED** (validated
  live: join works, `ray health-check` is the signal). Its "suspend mechanics"
  are void, replaced by this ADR.

## Alternatives considered

- **Scheduling gate on the RayJob submitter pod** (hold the K8sJobMode submitter
  Job via a scheduling gate until workers are admitted). Works without suspend
  too, but is more moving parts than the pin, which we already need for
  placement. Kept as a fallback if the pin-as-gate proves insufficient.
- **Keep trying to make suspend work** (e.g. set `shutdownAfterJobFinishes:
  true`) — rejected: KubeRay explicitly forbids suspend in clusterSelector mode
  regardless of that flag (validated).
