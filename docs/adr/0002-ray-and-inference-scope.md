# ADR-0002: Extend workload-mixing scope to the Ray ecosystem and inference

- **Status:** Accepted (scope); admission mechanism for shared RayClusters
  superseded by ADR-0006 (inner-workload admission, 2026-07-04)
- **Date:** 2026-07-03

## Context

The original design documents frame k8s-bridge as mixing Slurm batch jobs with
generic Kubernetes workloads under Kueue. Early customer signals indicate that
users running **Ray on Kubernetes** (via KubeRay) would also find value in a
single admission point that arbitrates between Slurm, Ray, and other
Kubernetes workloads on one pool of accelerators. Inference/serving is the
other workload class customers want to co-locate with batch training.

A key fact shaping this decision: **Kueue already integrates natively with
KubeRay** (`RayCluster` and `RayJob` are supported Kueue workload types), so
the bridge itself needs no Ray-specific code — the value is in the shared
quota/priority domain.

## Decision

1. **`RayCluster` (KubeRay) is in scope.** Ray clusters are admitted through
   Kueue and share ClusterQueue quota, priorities, and preemption with Slurm
   jobs translated by k8s-bridge. Our experiments must include a RayCluster
   competing for the same resources as Slurm jobs.
2. **`RayJob` is out of scope for the bridge.** RayJobs are already fully
   Kubernetes-native and directly Kueue-integrated; every RayJob is visible in
   the cluster without any bridging. Nothing to build.
3. **`RayService` is under evaluation.** Serving workloads are long-running
   and are not queued the way batch is. A plausible case exists when multiple
   services (more than a single baseline service) compete for accelerator
   capacity and need admission control; we will validate this with
   experiments before committing.
4. **Inference workloads are part of the mixing story.** The target picture is
   batch (Slurm, K8s Jobs, Ray) and serving sharing one pool, with priorities
   and preemption protecting serving latency while batch soaks up idle
   capacity.

## Alternatives considered

- **Ray-specific translation inside k8s-bridge** — rejected: duplicates
  Kueue's existing KubeRay integration; the bridge stays Slurm-focused.
- **Declaring serving out of scope entirely** — rejected: co-locating
  inference with batch is one of the main utilization arguments for choosing
  the Kueue-centric architecture over Slurm Bridge.

## Consequences

- Experiment 01 installs KubeRay alongside slurm-operator, Kueue, and JobSet,
  and includes a RayCluster and an inference workload in the mixing scenarios.
- Open question to resolve experimentally: how serving workloads participate
  in quota (Kueue plain-Pod integration vs. plain scheduling with
  PriorityClasses) — a topic for a future ADR once we have data.
