# Experiment 03 — open items: preemption, mem/GRES, Ray admission

- **Status:** executed 2026-07-04
- **Results:** see [docs/VALIDATION.md](../../docs/VALIDATION.md)

Manifests used by these experiments:

| File | Purpose |
|---|---|
| `manifests/preempt-high-priority-job.yaml` | High-priority job that forces Kueue to evict the bridge's slurmd JobSet |
| `manifests/ray-shared-cluster.yaml` | Long-lived shared RayCluster (deliberately NOT queued) |
| `manifests/ray-inner-job.yaml` | RayJob into the shared cluster — demonstrates the admission gap (ADR-0006) |
| `manifests/ray-service.yaml` | RayService with Serve autoscaling (inference as one elastic unit) |

The ray-bridge pinning PoC (Kueue-gated worker pod + custom-resource-pinned
Ray task) is a two-command procedure documented in the findings note §5.
