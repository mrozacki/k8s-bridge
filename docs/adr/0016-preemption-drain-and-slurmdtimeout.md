# ADR-0016: Proactive node drain on Kueue preemption; SlurmdTimeout as a backstop

- **Status:** Accepted — implemented behind `drainOnPreemption` (default off);
  live preemption validation pending (owner). See Consequences.
- **Date:** 2026-07-13 (implemented 2026-07-14)

## Context

When Kueue preempts an admitted workload, it suspends the JobSet and deletes
its pods. For a bridge-managed Slurm job that has already been released onto its
dynamic nodes (ADR-0005), those pods **are** the job's `slurmd` nodes. From
slurmctld's point of view the job is still `RUNNING` on nodes that just went
silent. Slurm only reacts once `SlurmdTimeout` elapses: it marks the nodes
`down*` and requeues the job (`JobRequeue=1`).

This was observed directly in the 2026-07-13 consolidated test session
(TC-C1, see `docs/VALIDATION.md`): a high-priority job preempted a
low-priority one, and *"Slurm marked the evicted
nodes `down*` only after `SlurmdTimeout=60` and then requeued the job."*

Two problems follow, both raised by the engineering team:

1. **Resource leak for up to `SlurmdTimeout` seconds.** Kubernetes frees the
   quota the instant the pods die, but Slurm keeps the job's allocation pinned
   to the now-dead nodes until the timeout. At scale, preemption is routine, so
   this leak is continuous, not incidental.

2. **`SlurmdTimeout` is caught in a bind.** The value that governs preemption
   cleanup latency is the *same* value that governs how quickly slurmctld
   declares a transiently-unresponsive node dead. The `60` in
   `test/e2e/slurm/slurm.conf` and
   `experiments/01-gke-playground/manifests/slurm-values.yaml` was chosen for a
   CI budget (its own comment says *"default 300s is too slow for a CI budget"*)
   — **not** as a production recommendation. Adopting a low value in production
   would let a large, healthy job with a brief heartbeat hiccup be declared dead
   and requeued. Raising it protects large jobs but lengthens the leak above.

The bridge today has no preemption-reactive path. `cleanupFinishedJobs`
(`internal/bridge/reconciler.go`) deletes a job's Slurm node records only when
the Slurm job is terminal or its JobSet failed/vanished — never on the "job
still running, nodes evicted" state that preemption produces. Cleanup latency is
therefore entirely at the mercy of `SlurmdTimeout`.

## Decision

**Decouple preemption cleanup from `SlurmdTimeout` by making the bridge drain a
preempted job's dynamic nodes proactively, and treat `SlurmdTimeout` as a
high-valued backstop rather than the primary cleanup mechanism.**

1. **Proactive drain.** When a tick observes that an admitted, already-released
   job's Workload has been evicted by Kueue (the `Evicted` Workload condition,
   corroborated by the JobSet being suspended / its pods gone), the bridge
   deletes that job's dynamic node records via the existing
   `slurm.Client.DeleteNode` (names are deterministic — `translate.NodeNames`).
   Removing the nodes makes slurmctld free the allocation and requeue the job
   immediately, instead of after `SlurmdTimeout`. The requeued job re-enters the
   held/pending flow and is re-admitted when Kueue next grants quota — the same
   path a fresh job takes.

2. **`SlurmdTimeout` becomes an operator-facing backstop.** It is promoted to a
   documented chart/config value with a production-oriented default
   (recommend ≥ 300s, i.e. Slurm's own default) so that a transient heartbeat
   loss on a large job does not requeue it. The low CI value stays confined to
   the e2e `slurm.conf`. The backstop still catches the cases proactive drain
   cannot see (bridge down during the eviction, a node lost without a Kueue
   eviction event).

3. **Guard rails, mirroring the orphan-cancellation guards (A9).** Drain acts on
   the *absence*/eviction of a Kubernetes object, which a read fault can mimic,
   so it is gated: off by default behind a config flag, requires a positive
   `Evicted` signal (not merely "pods missing"), and does not fire when the
   informer cache looks blind/partial. Draining wrongly only costs one requeue
   cycle (the job re-admits), but the guards keep a cache glitch from
   mass-requeuing a healthy fleet.

## Alternatives considered

- **Status quo — rely on `SlurmdTimeout` alone.** Simplest, and it *works*
  ("smrodek, ale zadziała okej" — the engineering feedback's own words). Rejected
  as the target state because it forces the single-knob trade-off between leak
  duration and false-death of large jobs; at scale neither end of that knob is
  acceptable.

- **Globally low `SlurmdTimeout`, no drain.** Cleans up fast, needs no new code.
  Rejected: it is exactly the setting that endangers large jobs on transient
  hiccups — the team's core worry — and the danger grows with job size and
  cluster scale.

- **`scontrol update NodeName=… State=DRAIN` instead of `DeleteNode`.** Draining
  keeps the node record (visible in `sinfo`, reusable). Rejected as the default:
  our dynamic nodes are ephemeral and their identities are deterministically
  recreated on the next admission (the identity-reuse property, see ADR-0005 /
  the A3 anchor), so a lingering `DRAIN`ed record is just a ghost to reap later —
  `DeleteNode` is the clean-slate operation cleanup already uses. `DRAIN` remains
  a fallback if a Slurm version rejects deleting a node with a running step.

- **Re-suspend-to-requeue on the Kubernetes side (the ray-bridge pattern).**
  ray-bridge re-suspends the JobSet on eviction. For the Slurm bridge the job's
  authority lives in slurmctld, not the JobSet, so the equivalent lever is the
  node record; suspending the JobSet without freeing the Slurm allocation would
  still leak on the Slurm side. Not applicable as-is.

## Consequences

- **Implemented behind a default-off flag (2026-07-14); live validation is the
  remaining step.** The drain path ships: `drainOnPreemption` (config +
  WorkloadMixing CR spec, default off), `kueue.IsEvicted`, and
  `Bridge.drainPreemptedNodes` (called from the reconcile tick for a released,
  not-held job whose Workload is `isEvicted && !isAdmitted`). It deletes the
  job's dynamic node records (`translate.NodeNames`), never the JobSet, emits a
  `PreemptionDrain` Event, and increments `k8s_bridge_preemption_drains_total`
  only on a tick that actually removed a record (repeat ticks over an
  already-drained eviction are silent, stateless). Unit tests cover the happy
  path, all three guards (flag off / admitted-not-evicted / held), and the
  idempotent 404 path. **`SlurmdTimeout` is guidance, not a bridge knob** — the
  bridge does not deploy the Slurm control plane, so "keep it high in prod" lives
  in this ADR and the Slurm chart values, not in the k8s-bridge chart. The `60`
  in the e2e `slurm.conf` stays a CI value. Still open: the live GKE preemption
  test below (owner runs it — backlog F1).
- **New dependency on a truthful eviction signal.** The bridge must read Kueue's
  `Evicted`/preemption condition on the Workload. If Kueue's condition semantics
  change, the drain trigger must follow — pin it with an integration test.
- **A benign race.** If drain and a Kueue re-admission interleave, worst case is
  one extra requeue cycle; the guards keep this from amplifying across the fleet.
- **Backstop still required.** Proactive drain is best-effort (needs the bridge
  up and a visible eviction event); `SlurmdTimeout` must stay configured as the
  catch-all. The design goal is "fast common path, safe worst case," not removing
  the timeout.
- **Regression coverage.** Landed: `preemption_drain_test.go` (evicted-but-live
  job drains every node record; the three guards; idempotent 404) and
  `kueue/evicted_test.go` (`IsEvicted` shapes). Documented note that
  `SlurmdTimeout` is a backstop lives in this ADR.

## How to validate live (owner runs this)

Goal: prove the fast path (drain requeues in ~one tick, not after
`SlurmdTimeout`) AND the safe path (a healthy large job under a *high*
`SlurmdTimeout` is not requeued by a transient hiccup).

Prereqs: a GKE cluster with the Slinky Slurm stack + Kueue + k8s-bridge, the
same setup as experiment 01 / the TC-C1 preemption test. Enable the feature:
set `drainOnPreemption: true` on the `WorkloadMixing` CR (or `--set` it on the
chart), and set the Slurm control plane's `SlurmdTimeout` **high** (e.g. 300s)
so the difference between "drain" and "wait for timeout" is unmistakable.

1. **Fast-path (drain works).**
   - Fill the `mixing` quota with a low-priority job (`JOB_LOW`, partition
     `mixing`) so it is admitted and running on its dynamic nodes
     (`sinfo` shows them registered).
   - Submit a higher-priority job (`JOB_HIGH`, `mixing-high`) that forces
     Kueue to preempt `JOB_LOW` (same shape as TC-C1).
   - **Assert:** within ~1–2 bridge poll intervals of the Kueue `Evicted`
     event, `JOB_LOW`'s dynamic nodes disappear from `sinfo` (drained), it
     requeues to `PENDING`, and a `PreemptionDrain` Event is on its JobSet;
     `k8s_bridge_preemption_drains_total` incremented by 1. It must NOT take
     the ~`SlurmdTimeout` (300s) that TC-C1 measured with drain off. Compare
     directly by re-running TC-C1 with `drainOnPreemption: false` — the node
     should linger ~300s (the `down*` behaviour), with drain on, ~seconds.
   - Confirm re-admission still works: free the quota again and check
     `JOB_LOW` re-admits and its nodes re-register under the SAME names.
2. **Safe-path (high SlurmdTimeout protects large jobs).** With drain enabled
   and `SlurmdTimeout=300`, simulate a transient slurmd unresponsiveness on a
   running, NON-preempted job (e.g. briefly pause the slurmd container, under
   the timeout). **Assert:** the job is NOT requeued (no Kueue eviction → no
   drain; the high timeout rides out the hiccup). This is the whole point of
   demoting `SlurmdTimeout` to a backstop.
3. **Guard sanity.** With `drainOnPreemption: false` (default), repeat step 1
   and confirm NO `PreemptionDrain` Event and the old `SlurmdTimeout` wait —
   i.e. the feature is truly opt-in.

If the `Evicted` condition shape differs on the deployed Kueue (v0.18.x) and
the drain never fires, capture the actual Workload `status.conditions` (that is
the `NEEDS-LIVE-VALIDATION` note on `kueue.IsEvicted`) and adjust the matcher.
