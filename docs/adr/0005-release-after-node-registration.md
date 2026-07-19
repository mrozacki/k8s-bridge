# ADR-0005: Release held jobs only after dynamic-node registration

- **Status:** Accepted (deviation from the MVD, validated live 2026-07-04)
- **Date:** 2026-07-04

## Context

The Minimal Viable Design orders the bridge workflow as: set the
`Features=nodes-for-<id>` constraint on the held job, create the JobSet, then
release the hold — the job would wait in the queue until its nodes appear.

Live validation against Slurm 26.05 showed this order is impossible: Slurm
validates job constraints against the set of features currently advertised by
registered nodes. A constraint naming a not-yet-existing feature is rejected
("Invalid feature specification" via scontrol, HTTP 422 via slurmrestd).
Dynamic nodes only advertise `nodes-for-<id>` once their slurmd pods are
running and registered — which happens after JobSet creation and Kueue
admission.

## Decision

The bridge applies the constraint and releases the hold **after** the dynamic
nodes register, using Slurm's own constraint validation as the readiness
signal:

1. Create the JobSet (idempotent, deterministic name).
2. Each reconcile tick, attempt `constraints=nodes-for-<id>` on the held job.
   Rejection means the nodes are not registered yet — retry next tick.
3. On success, release the hold. Slurm schedules the job onto its
   feature-matched nodes immediately.

## Alternatives considered

- **Pre-declaring the feature domain in slurm.conf** (e.g. a NodeName template
  advertising all possible `nodes-for-*` features) — rejected: requires
  config regeneration per job or wildcard tricks Slurm does not support
  cleanly, and couples slurm.conf to bridge internals.
- **Polling node registration state via REST, then setting the constraint** —
  viable, but adds a second status-tracking code path; the
  validation-as-readiness approach gets the same signal from the call we must
  make anyway. A dedicated readiness check may still be added later for
  observability (metrics on registration latency).
- **Releasing without any feature constraint** — rejected: nothing would stop
  Slurm from scheduling the job onto other idle nodes (including the static
  nodeset), breaking the one-job-one-JobSet resource model.

## Consequences

- The MVD document should be corrected (its JobSubmit-plugin flow implies the
  old order; upstream doc owners notified via this ADR).
- The retry is self-healing and adds at most one poll interval of latency
  between node registration and job release.
- Failure telemetry must distinguish "nodes not ready yet" (expected,
  transient) from real update errors — currently both surface as a retried
  tick; refine when metrics land (see MVD observability section).
