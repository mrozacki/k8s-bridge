# Experiment 02 — manual bridge: validate the core mechanism by hand

- **Status:** executed 2026-07-04 — PASSED; see Results below and
  [docs/VALIDATION.md](../../docs/VALIDATION.md)
- **Prerequisite:** environment from experiment 01 up and running (no extra
  resources — reuses the experiment 01 cluster)

## Goal

Play the role of the k8s-bridge controller **by hand**, end to end, before
writing any controller code. This de-risks the single most speculative part of
the architecture: *can pods running `slurmd -Z` register as dynamic Slurm
nodes, run exactly one held job, and disappear cleanly?*

If this works manually, phase 3 is "just" automation. If it does not, we have
saved ourselves from automating a broken idea — and produced precise feedback
for the next design iteration.

## The choreography (what the controller will automate)

| Step | Actor (today: us) | Command sketch |
|------|-------------------|----------------|
| 1. Submit a held Slurm job | user | `sbatch --hold --ntasks=2 --cpus-per-task=1 hello.sbatch` → note the job ID |
| 2. "Discover" it | bridge | `squeue --states=PD -h` / REST `GET /slurm/v0.0.44/jobs` |
| 3. Constrain the job to its future nodes | bridge | `scontrol update job <id> Features=nodes-for-<id>` |
| 4. Create the JobSet | bridge | edit `manifests/slurmd-jobset.yaml` (job ID, ntasks→parallelism, cpus→requests), `kubectl apply` |
| 5. Wait for Kueue admission + node registration | Kueue/slurmd | `kubectl -n slurm-jobs get workloads` then `sinfo -N` shows new dynamic nodes |
| 6. Release the job | bridge | `scontrol release <id>` → Slurm schedules it onto the Feature-matched nodes |
| 7. Observe execution & completion | — | `squeue`, `sacct -j <id>`, pod logs |
| 8. Clean up | bridge | `scontrol delete nodename=<dynamic-nodes>`; `kubectl delete jobset slurm-job-<id>` |

## What to validate (checklist)

- [ ] `slurmd -Z` from the Slinky image registers against slurmctld from a
      vanilla pod (auth key + conf-server are sufficient).
- [ ] The `Feature=nodes-for-<id>` constraint pins the job to its dedicated
      nodes — and prevents *other* Slurm jobs from landing on them.
- [ ] Kueue quota is actually consumed by the JobSet (check ClusterQueue
      status) — this is the mixing guarantee.
- [ ] Preemption drill: submit a `high-priority` workload that exceeds the
      remaining quota → Kueue should evict the slurmd JobSet → what does the
      Slurm job look like then? (Expected per design: requeue.)
- [ ] Cleanup leaves no ghost nodes in `sinfo` and no stray pods.
- [ ] Timing: how long from JobSet admission to node registration?
      (This becomes the controller's readiness metric.)

## Known unknowns

- Dynamic-node auth: the Slinky chart may use `auth/slurm` keys or JWT — the
  exact secret and mount layout must be lifted from a running slurmd pod
  (`kubectl -n slurm get pod <nodeset-pod> -o yaml`).
- Whether the login pod's Slurm version tolerates dynamic nodes joining
  partitions on the fly (partition membership of `-Z` nodes is configured via
  the node's `--conf`, e.g. adding it to a dedicated `mixing` partition).
- Node deregistration: epilog vs. manual `scontrol delete` — for the manual
  run we accept manual deletion; the controller will own this later.

## Results (executed 2026-07-04, overnight run)

**The core mechanism works.** Two slurmd pods registered as dynamic nodes with
`Features=nodes-for-2`, the held job was pinned and released, ran to
completion on its dedicated pod-nodes, and manual cleanup left no ghosts.
Checklist outcomes: registration ✔ (after adding a privileged security
context — unprivileged slurmd cannot mount cgroups), feature pinning ✔ (but
only AFTER node registration; Slurm rejects constraints on unknown features —
a design-order reversal), quota accounting ✔, cleanup ✔, timing: registration
≈ 10-20 s after JobSet admission. Resource-sizing gotcha: slurmd advertises
host resources unless told otherwise (`CPUs=`, `RealMemory=`). Preemption
drill deferred to a later run. Full findings in
[docs/VALIDATION.md](../../docs/VALIDATION.md).
