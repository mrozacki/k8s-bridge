# ADR-0007: Serving workloads are admitted through Kueue (two-tier model)

- **Status:** Accepted (2026-07-04); validated live
  in experiment 04 (2026-07-04)
- **Refines:** ADR-0006 (which placed RayService/serving outside Kueue)

## Context

ADR-0006 initially kept serving outside Kueue, protected only by scheduler
`PriorityClass`, on the argument that queueing would fight the autoscaler.
Further review surfaced the stronger counter-argument:
serving that bypasses Kueue is **invisible to the quota ledger** — Kueue can
admit batch that the scheduler then cannot place (observed on the
playground), and priorities/allocations end up configured in two disjoint
systems. Kueue meanwhile has first-class support for serving workloads:
`deployment`/`statefulset`/`leaderworkerset`/`pod` integrations where **each
replica pod is admitted as its own Workload**.

The original concern conflated two control layers that Kueue deliberately
separates: *how many replicas* (an autoscaler decision) vs. *whether
capacity is granted and at whose expense* (an admission decision).

## Decision

Adopt a **two-tier model** — all capacity flows through Kueue; replica
counts stay with the autoscalers:

| Layer | Owner | Serving | Batch |
|---|---|---|---|
| Replica/task count | HPA / Ray Serve / user | autoscaler decides | sbatch / RayJob |
| Capacity admission, priorities, preemption | **Kueue (single place)** | per-pod (deployment/pod integration), high `WorkloadPriorityClass` | per-workload (JobSet, RayJob) |
| Node placement | kube-scheduler | — | — |

Specifics:

1. Serving Deployments/StatefulSets carry `kueue.x-k8s.io/queue-name` and a
   high `WorkloadPriorityClass`; a scale-up admits by **preempting batch**
   rather than waiting, so latency-criticality is expressed as priority,
   not as bypassing admission.
2. Ray-based services: the admission unit is the **worker pod** (Serve
   replicas are actors inside pods and stay invisible to Kueue by design).
   This is the same pod-gating used for the ray-bridge model (ADR-0006).
3. Scheduler `PriorityClass` remains as defense-in-depth at placement time,
   kept consistent with the WorkloadPriorityClass mapping.

## Alternatives considered

- **Serving outside Kueue (ADR-0006 v1)** — rejected: split-brain quota
  ledger, duplicated priority configuration, observed admission errors.
- **Kueue also driving replica counts** — rejected: not Kueue's job; the
  autoscaler layer already owns it and does it well.

## Consequences and required validation (experiment 04)

- Every resource-consuming pod in the mixing domain is now Kueue-visible —
  one ledger, one priority scale, complete fair-sharing.
- New availability coupling: scale-ups of serving depend on Kueue's health
  (running replicas are unaffected). Production needs Kueue HA and alerting.
- To validate live before marking this fully proven:
  1. rolling update of a queued Deployment under a tight quota (surge pods
     need admission headroom — deadlock risk to characterize);
  2. latency of scale-up → batch preemption → replica Ready, vs. the
     PriorityClass-only baseline;
  3. behavior when quota is exhausted by same-priority serving (no victim
     available) — expected: pod pends with a clear Workload condition.
