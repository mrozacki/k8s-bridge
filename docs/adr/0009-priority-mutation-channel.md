# ADR-0009: Priority mutation channel — Workload patch now, lua directive later

- **Status:** Accepted (2026-07-04;
  live failure documented below)
- **Date:** 2026-07-04

## Context

The product requires mutable job priorities, including for RUNNING
jobs. Kueue supports this natively: `Workload.spec.priority` is mutable and
re-ranks preemption-victim selection (validated live: an admitted workload
accepted a patch to 900 and stayed admitted).

The first bridge implementation used Slurm's own `priority` field as the
user-facing channel with a three-way mirror. Live testing failed hard:
Slurm continuously recomputes that field (age factor), its value 0 means
HOLD, and running jobs' values are scheduler-internal — the mirror churned
and re-held a running job mid-execution.

## Decision

1. **Supported now:** priority mutation happens on the Kubernetes side —
   `kubectl patch workload <name> --type=merge -p '{"spec":{"priority":N}}'`
   (or the researcher dashboard once built). Works for pending and running.
2. **Slurm-native channel (designed, not yet implemented):** the lua
   JobSubmit plugin's `slurm_job_modify` hook intercepts
   `scontrol update priority=N`, records the request as a bridge directive
   (admin_comment), REJECTS the raw field change, and the bridge applies
   the directive to the Workload. Slurm's internal field is never used as a
   data channel.
3. The experimental mirror stays in the code behind `enablePrioritySync`
   (default **off**) as a cautionary artifact until the lua channel lands.

## Consequences

- Regression risk removed; running jobs can no longer be re-held by sync.
- Slurm users temporarily need the K8s-side path (or an admin) to change
  priorities — acceptable until the lua directive ships.
- Lesson recorded for the engineering team: **never repurpose a
  scheduler-owned field as an API** — Slurm's priority is an output of its
  scheduler, not an input channel, exactly like Kueue's own status fields.
