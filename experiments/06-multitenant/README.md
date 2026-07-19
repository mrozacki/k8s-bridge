# Experiment 06 — multitenant: cohorts, borrowing, reclaim + TAS

- **Status:** executed 2026-07-04 — PASSED; see
  [docs/VALIDATION.md](../../docs/VALIDATION.md)

## Goal

Validate that Kueue cohorts compose with Topology-Aware Scheduling: can two
teams share a quota pool (borrowing idle capacity, reclaiming it back) while
both queues stay bound to a TAS flavor?

Setup: `manifests/cohort-queues.yaml` — team-a and team-b each nominally own
6 CPU, sharing cohort `research`; team-b sets `lendingLimit: 2` so it always
keeps a 4-CPU floor (the pattern ADR-0006/0007 prescribe for shared
base capacity). Both queues use the TAS flavor from experiment 05.

## Headline results

**Cohorts + borrowing + TAS compose.** team-b borrowed 1.5 CPU of team-a's
idle quota through the `research` cohort on a TAS-bound flavor — TAS and
cohort borrowing do not conflict. Caveat found live: a TAS flavor routes
even un-annotated workloads through topology assignment with all-or-nothing
gang semantics (a domain that "allows to fit only 3 out of 4 pods" blocks
the whole gang, not just the 4th pod).

**Reclaim works, at whole-workload granularity.** team-a submitted a 6-CPU
Slurm job and Kueue evicted team-b's borrowing workload to give it back —
the event spelled out the reason ("due to reclamation within the cohort;
preemptor path: /research/team-a; preemptee path: /research/team-b").
Eviction is all-or-nothing for the borrowing workload, not partial.

Setup: ran on a shared 4-node GKE cluster (Slinky 1.2.0,
Kueue v0.18.2); no dedicated cluster spun up for this experiment.
