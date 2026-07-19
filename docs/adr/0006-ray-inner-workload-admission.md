# ADR-0006: Ray admission granularity — inner workloads, not clusters

- **Status:** Proposed (scope correction, 2026-07-04;
  supporting experiments in `docs/VALIDATION.md`).
  The "RayService / inference position" section is refined by ADR-0007
  (serving IS admitted through Kueue, per-pod, in the two-tier model).
- **Supersedes:** the RayCluster-as-workload framing implied by ADR-0002
- **Amended:** the `spec.suspend`-based admission MECHANISM below is superseded
  by **ADR-0013** (pin-gate) — KubeRay forbids suspend in clusterSelector mode
  (live finding, 2026-07-07). The GOAL (per-inner-workload Kueue admission) is
  unchanged and validated live.

## Context

Experiment 01 demonstrated a `RayCluster` admitted by Kueue as ONE workload.
We clarified the actual product intent: customers running
long-lived, shared Ray clusters (KubeRay) need admission at the granularity
of the **workloads running inside** the cluster — the same way k8s-bridge
queues individual Slurm jobs, not the Slurm cluster.

Cluster-level admission has exactly the pathology our strategy document
criticizes in Slurm Bridge's slurmd-in-pod mode: an admitted-but-idle
cluster **hoards quota** for its entire lifetime, and every inner job is
invisible to the platform's admission authority (no cross-workload
priorities, no preemption, no fair sharing against Slurm/K8s batch).

Verified gap: a `RayJob` submitted into an existing cluster via
`clusterSelector` bypasses Kueue entirely — it starts immediately regardless
of quota, and Kueue's RayJob integration does not cover this mode.

## Decision (proposed target model)

Adopt the **k8s-bridge pattern for Ray**: per-inner-job dynamic capacity,
admitted through Kueue, pinned to its job.

| k8s-bridge (Slurm) | ray-bridge (proposed) |
|---|---|
| held Slurm job in mixing partition | suspended `RayJob` (KubeRay native `suspend`) targeting the shared cluster |
| JobSet of slurmd pods | worker pod(s) for a dedicated worker group |
| pods register as dynamic Slurm nodes | pods join the Ray cluster as workers |
| `Features=nodes-for-<id>` pins job↔nodes | Ray **custom resource** `{"wm-job-<id>": N}` on those workers + `entrypoint_resources` on the job |
| release hold after registration | unsuspend after workers join |
| delete node records + JobSet | drain + remove workers |

Interim/pragmatic variant (lower fidelity, near zero build cost): enable
Kueue's **pod integration** on Ray worker pods, so any capacity growth of
the shared cluster is quota-gated at pod creation. Inner jobs then wait for
capacity implicitly. Loses per-job priority/preemption semantics — capacity,
not workloads, is what queues.

## RayService / inference position

1. A `RayService` is treated as **one unit** from the platform's
   perspective: a microservice that scales its Serve replicas up and down
   (validated live). It does not go through Kueue;
   like other serving, it is protected by scheduler `PriorityClass` and can
   preempt batch at the node level.
2. **Intra-service differentiated priorities** (fragments of a service
   queued separately) — assessed and NOT recommended as a Kueue problem:
   Serve replicas are elastic and latency-driven; queueing them would fight
   the Serve autoscaler. Where criticality differs inside one service, use
   separate worker groups with distinct K8s PriorityClasses and Serve
   placement via custom resources. Revisit only on concrete customer demand.
3. A RayService running inside a shared RayCluster reduces to the
   capacity-gating variant above (its scale-ups are worker pods).

## Alternatives considered

- **Keep cluster-as-workload (ADR-0002 demo model)** — rejected as the
  target: quota hoarding, no inner-job visibility. Remains valid for
  ephemeral per-job clusters (Kueue's native RayJob mode).
- **Teach Kueue about Ray's internal scheduler** (mirror Ray logical
  resources into quota) — rejected: two sources of truth for the same
  capacity, the exact "two masters" problem the strategy doc rejects.
- **GKE-level solution only (node pools per tenant + PriorityClasses)** —
  simpler but abandons unified quotas/fair-sharing across Slurm/Ray/K8s,
  which is the product's core promise.

## Consequences

- k8s-bridge and ray-bridge share an architecture pattern (hold → admit
  dedicated capacity → pin → release → clean up); a future common library
  is plausible ("bridge SDK").
- Requires validation of Ray-side mechanics: dynamic worker join/drain
  latency, custom-resource pinning, autoscaler interference with manually
  managed worker groups (first PoC).
- ADR-0002 remains valid for scope (Ray in, RayJob-standalone via native
  Kueue, inference in scope); its admission mechanism for shared clusters
  is replaced by this ADR.

## Clarification (2026-07-06): scope is RayCluster ONLY; nothing built yet

Owner clarification: of KubeRay's three controllers, only **RayCluster** has the
many-inner-workloads-to-one-cluster structure this ADR addresses. **RayJob**
(native Kueue integration) and **RayService** (serving, ADR-0007) run
natively and have NO inner Kueue workloads — they are out of scope for a
ray-bridge. The inner-workload admission work is exclusively the shared,
long-lived RayCluster case.

Implementation status: **none.** The prototype has zero Ray code (Slurm
only); this ADR is Proposed; "ray-bridge" is the top phase-C engineering
item. The Slurm + RayCluster mixed e2e (many inner workloads, distinct
priorities/researchers/ClusterQueues, one admission point) cannot run until
the ray-bridge is built. Deferred to a dedicated session.
