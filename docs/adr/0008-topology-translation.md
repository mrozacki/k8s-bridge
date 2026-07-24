# ADR-0008: Translate Slurm topology requests to Kueue TAS levels

- **Status:** Accepted (validated live in experiment 05)
- **Date:** 2026-07-04

## Context

HPC workloads are latency-sensitive to network topology: a distributed
training job wants all its nodes under one leaf switch/rack. Slurm users
express this with `--switches=N` (topology/tree plugin); topology-aware
scheduling is a P1 post-MVP milestone. Kueue provides
Topology-Aware Scheduling (TAS, beta): a cluster-scoped `Topology` CR
declares hierarchy levels as node label keys, a `ResourceFlavor` binds to
it, and workloads constrain placement via podset annotations
(`podset-required-topology` / `podset-preferred-topology`).

Key insight making experiments cheap: TAS reads ONLY node labels, so a
simulated hierarchy (labels applied by a script) is indistinguishable from
a real GKE block/subblock/host topology to the whole admission stack.

## Decision

1. `sbatch --switches=N` (any N>0) translates to a **hard** constraint:
   `kueue.x-k8s.io/podset-required-topology: <configured required level>`
   on the slurmd pod template. The switch COUNT is not representable in
   TAS (one domain per podset) — any positive value means "one domain";
   documented as a semantic narrowing.
2. Bridge jobs WITHOUT a topology request get
   `podset-preferred-topology: <configured preferred level>` — best-effort
   gang locality by default, since it is practically always wanted for
   multi-node HPC jobs and costs nothing when capacity is scattered.
3. Levels are configuration (`topology.requiredLevel/preferredLevel`), not
   hardcoded: real clusters will use provider labels
   (e.g. GKE `cloud.google.com/gce-topology-block`), the playground uses
   `example.com/block|rack`.

## Alternatives considered

- **Ignore topology (status quo of the MVD)** — rejected: it is a P1 item
  and the translation cost turned out to be ~30 lines plus configuration.
- **Model switch counts >1 as multiple domains** — rejected for now: TAS
  admits a podset into one domain subtree; faithful `--switches=2`
  semantics would need Kueue-side changes. Narrowing documented instead.
- **Slurm-side topology plugin driving K8s placement** — rejected: Slurm
  has no authority over K8s nodes in this architecture (the whole point);
  locality must be enforced by the admission layer that owns placement.

## Consequences

- Slurm users keep their familiar flag; the platform team maps it to the
  right level per cluster.
- Dynamic slurmd pods inherit datacenter locality, which transfers to the
  Slurm job running on them — Slurm never needs to know the topology.
- The preferred-by-default policy slightly biases packing; revisit if it
  measurably delays admission on fragmented clusters.
