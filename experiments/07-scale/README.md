# Experiment 07 — scale: 500 processed + ~5000-job backlog

- **Status:** executed 2026-07-05; full narrative in
  [docs/VALIDATION.md](../../docs/VALIDATION.md)
- **Setup:** 3-node spot cluster, ~3 h run

Scripts: `scripts/backlog-slurm.sh N` (in-pod sbatch loop),
`scripts/backlog-k8s.sh N` (suspended Jobs, mixed queues/priorities).
Artifacts in `results/`: CPU + heap pprof captured under ~3000-job load,
tail of tick durations.

Headlines: bridge is I/O-bound (1.6% CPU, 88 MB RSS at 5k jobs; heap
~12 MB live); two scaling defects found & fixed (per-tick LIST/CREATE
storm — pre-fixed; pin-attempts on unadmitted backlog — 2m53s→43s ticks
after gating on Kueue admission); Kueue 271m/238Mi and slurmctld
22m/134Mi at ~3000 objects, no crashes. Throughput ~3-4 jobs/min under
full backlog pressure on 7 physical slots — poll latency dominates
(backlog P1: watches). Next tier: larger-scale runs (S1–S5, see `docs/VALIDATION.md`).
