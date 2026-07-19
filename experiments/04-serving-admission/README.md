# Experiment 04 — serving through Kueue (two-tier model, ADR-0007)

- **Status:** executed 2026-07-04 — PASSED; see
  [docs/VALIDATION.md](../../docs/VALIDATION.md)

## Goal

Validate the two-tier model live: serving Deployment admitted per-pod via
Kueue with a high WorkloadPriorityClass, autoscaled by HPA, on the same
quota as Slurm-bridge and Ray workloads.

## Scenarios

1. Queued inference Deployment scale-up while batch fills the quota →
   expect immediate preemption of the bridge JobSet; measure time-to-Ready.
2. Rolling update under tight quota (surge headroom) — characterize the
   deadlock risk and the minimum headroom rule of thumb.
3. Quota exhausted by same-priority serving → expect clean pending Workload
   condition (no victim available).
4. Baseline comparison: the same scale-up with scheduler PriorityClass only
   (scenario C from experiment 01).
