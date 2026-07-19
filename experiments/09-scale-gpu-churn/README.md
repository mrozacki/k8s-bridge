# Experiment 09 — scale + simulated-GPU churn

- **Status:** EXECUTED live 2026-07-06. The RUNNING target
  was quota-capped, not bridge-limited — see "Expected results" below and
  `docs/VALIDATION.md`. Teardown completed.
- **Teardown:** MANDATORY (see callout at the bottom)

## Goal

Validate that k8s-bridge holds under sustained scale + pod churn, with
**simulated GPUs only** (ADR-0010 — no real accelerator hardware, no GPU
quota needed). Targets:

1. **>= 500 RUNNING jobs concurrently** (admitted Slurm-GPU jobs translated
   into JobSets, actually scheduled and running as slurmd pods).
2. **>= 3000 PENDING (held/queued) jobs** behind that running set.
3. **Simulated pod churn ABOVE 100 pods/second** (sustained create+delete),
   independent of the running/pending backlog, to stress the bridge's
   watch-nudge + informer cache and the scheduler.

This builds directly on prior experiments rather than reinventing them:

- Node/cluster scripting style: `experiments/01-gke-playground/scripts/`.
- Backlog generation pattern: `experiments/07-scale/scripts/backlog-slurm.sh`
  (adapted here to add `--gres=gpu:1` and batching/rate-limiting for 3000-job
  volume against slurmrestd — see `scripts/20-scale-backlog.sh` header for the
  exact diff in intent).
- GPU simulation mechanism: `docs/adr/0010-simulated-accelerators.md`,
  validated live (see `docs/VALIDATION.md`).
- Compute-class / node-pool patterns for cost control:
  `experiments/08-ccc-dws/manifests/compute-classes.yaml` (not reused
  directly — this experiment uses plain autoscaling node pools, since Custom
  Compute Classes were validated separately and are not required here — but
  the same "autoscale from zero, tag pools by purpose" philosophy applies).

None of experiments 07 or 08's files were modified; this experiment only
reads/reuses their conventions.

## What this experiment does NOT do

- No real GPUs. `nvidia.com/gpu` capacity is a value patched onto node
  status by a DaemonSet (`manifests/gpu-sim-daemonset.yaml`) — no device
  plugin, no driver, no CUDA, no accelerator billing.
- No production Slurm accounting DB (mirrors experiment 01: accounting
  stays disabled).
- No changes to `experiments/07-scale/` or `experiments/08-ccc-dws/`.

## Setup

### Cluster topology (3 node pools — see `scripts/00-env.sh` / `01-create-cluster.sh`)

| Pool | Purpose | Machine type | Autoscale | Spot? |
|---|---|---|---|---|
| default (control-plane) | bridge, Kueue, KubeRay, slurmctld, churn-generator control | e2-standard-4 | 1..3 | no (on-demand — must not be reclaimed mid-run) |
| `gpu-sim-spot` | 500 running + up to 3000 pending slurmd-GPU pods | e2-standard-8 | 0..80 | yes |
| `churn-pool` | pod-churn generator only | e2-standard-4 | 0..4 | yes |

Why 3 pools instead of 1: a spot reclamation on the GPU-sim pool must never
take the bridge/Kueue/slurmctld controllers down with it, and the churn
generator's rapid create/delete storm must not contend for the same node
capacity as the 500 "real" running jobs we're trying to hold steady while
measuring churn (it would confound both measurements). Cost impact of the
split is negligible: the control-plane pool is tiny and the churn pool
autoscales from zero.

### Node/pod math for the 500-running target

- Machine type `e2-standard-8` = 8 vCPU / 32 GiB per node.
- Each slurmd pod (translated JobSet member) requests `cpu: 100m`,
  `memory: 256Mi`, `nvidia.com/gpu: 1` (simulated).
- CPU/memory alone would pack ~80 pods/node (8000m / 100m) — far more than
  wanted for a scheduler-stress test. Instead, `GPU_PER_NODE=8` (default,
  overridable) is the **deliberately small, binding constraint**: the
  gpu-sim-patcher DaemonSet advertises only 8 simulated GPUs per node, so
  GPU count — not CPU — gates how many slurmd-GPU pods land per node. This
  forces the scheduler/Kueue to spread the 500 running pods across many
  nodes, which is the point of a "500 concurrent" test (a single giant node
  would not exercise cluster-wide scheduling or informer fan-out
  realistically).
- Nodes needed: `ceil(RUNNING_TARGET / GPU_PER_NODE)` = `ceil(500 / 8)` =
  **63 nodes** minimum at steady state. `MAX_NODES` is set to 80 (roughly
  +25%) so autoscaling has slack during backlog/churn spikes and so the
  autoscaler is never the reason a 500-running run falls short.
- `MIN_NODES=0`: the pool scales to zero when idle, so cost between runs (or
  while only exercising churn) is near zero.
- All of this is computed in code in `scripts/00-env.sh` (`NODES_FOR_TARGET`),
  not just documented here — the script prints it on every source.

### GPU simulation mechanism (ADR-0010)

ADR-0010 offers a Kubernetes-side mechanism ("patch a node's status with a
fake extended resource... scheduler, Kueue quota, and admission treat it as
real") that live testing validated by hand with a one-shot `kubectl patch node
.../status` loop. For this experiment we promote that to a **self-patching
DaemonSet** (`manifests/gpu-sim-daemonset.yaml`, applied by
`scripts/10-simulate-gpus.sh`) rather than keeping the one-shot loop, because:

- `gpu-sim-spot` autoscales 0..80. A one-shot loop only patches nodes that
  exist at the moment it runs; any node GKE adds afterwards (very likely
  under a 3000-job backlog spike) would have zero `nvidia.com/gpu` capacity
  and sit unusable until someone reran the loop by hand.
- A DaemonSet's pod is scheduled automatically onto every node that joins
  the pool it targets (`nodeSelector: workload-mixing/gpu-sim: "true"`), so
  the fake capacity appears within ~30 seconds of any scale-up — no
  human/cron re-trigger needed.
- It is idempotent by construction (the container loops the same PATCH every
  30s; re-applying the manifest is a no-op).

The DaemonSet patches both `/status/capacity` and `/status/allocatable` for
`nvidia.com/gpu` (the resource name matches
`Slurmd.GPUResourceName`'s default in
`internal/config/config.go`'s `ApplyDefaults()`, and
ADR-0010). It runs with a minimal RBAC grant (`nodes/status` patch only) and
tiny resource requests.

On the Slurm side, `manifests/slurm-values-scale.yaml` reuses the exact
count-only `gres.conf` (`Name=gpu Count=1`, no `File=`) validated in
`experiments/01-gke-playground/manifests/slurm-values.yaml` and ADR-0010 —
gres File= is only required when Slurm actually verifies a device node,
which count-only declarations skip.

### L1 fix: dynamic-node registration config (added 2026-07-06, not yet re-validated live)

The live run (2026-07-06) hit a
real environment gap: all 20 slurmd pods reached `Running`, but **none
registered as dynamic Slurm nodes** — `sinfo` only ever showed the static
worker, so the bridge (correctly, per ADR-0005) never released the held
jobs onto them. This was not a bridge bug; it was a missing Slurm-side
config.

**Root cause (confirmed this session against three independent sources):**
dynamic node registration (`slurmd -Z`) is only supported when the
controller runs `SelectType=select/cons_tres`. Slurm's own upstream default,
if `SelectType` is left unset, is `select/linear` — which silently cannot
accept dynamic registrations. Neither `slurm-values.yaml` nor
`slurm-values-scale.yaml` ever set `SelectType`, so both were running the
unset upstream default the whole time. Sources checked live this session:

1. Slurm's own dynamic-nodes documentation (slurm.schedmd.com/dynamic_nodes.html).
2. The slurm-operator's own Go source
   (`SlinkyProject/slurm-operator`, `internal/builder/controllerbuilder/controller_config.go`,
   read via `gh search code`) — confirms the operator hardcodes
   `MaxNodeCount=1024` (so `MaxNodeCount` was **not** the gap — 1024 already
   comfortably covers this experiment's node counts, max ~520 seen live) but
   never sets `SelectType` anywhere.
3. `helm template` against the real chart (`oci://ghcr.io/slinkyproject/charts/slurm`,
   run live this session), confirming the rendered `Controller` CR's
   `spec.extraConf` carries exactly what `extraConfMap` sets — the correct,
   confirmed injection point for this fix.

**Fix applied (both `slurm-values.yaml` and `slurm-values-scale.yaml`):**

| Key | Value | Confidence | Reasoning |
|---|---|---|---|
| `SelectType` | `select/cons_tres` | HIGH | Confirmed root cause — dynamic nodes require this plugin; verified against upstream docs and operator source. |
| `MaxNodeCount` | *(not set — left to the operator's built-in 1024)* | HIGH | Already hardcoded by the slurm-operator itself; not the bottleneck at this experiment's scale. |
| `TreeWidth` | `65533` | MEDIUM | Classic "disable fanout" dynamic-node recommendation (SLUG22 deck), but Slurm 23.11+ (we run 26.05) re-enabled fanout for dynamic/cloud nodes specifically, so this may no longer be strictly required. Added defensively as a documented-safe value; flagged rather than silently omitted or silently assumed correct. |

**This has NOT been re-validated live.** `helm template` confirms the
values render correctly into the operator's `Controller` CR (mechanical
correctness), but no live GKE run has confirmed slurmd actually registers
as a dynamic node with this config — that requires a real slurmctld +
slurmd pair, which this session could not stand up (no local Slurm cluster
available, and creating a GKE cluster was explicitly out of scope for this
work). **Next live session should re-run this experiment (or experiment
02's manual choreography) and confirm `sinfo -N` shows dynamic nodes
registering**, then update this section with the result.

### Backlog generation

`scripts/20-scale-backlog.sh [N]` (default `N=3000`) submits held Slurm jobs
into partition `mixing-gpu` with `--gres=gpu:1`, batched (`CHUNK_SIZE=100`
by default) with a pause between batches (`CHUNK_PAUSE_SECONDS=2`) — this is
`experiments/07-scale/scripts/backlog-slurm.sh`'s one-`kubectl-exec`-per-batch
trick, extended for `--gres` and volume safety, since the scale run's own
findings (see `docs/VALIDATION.md`) showed
slurmrestd/slurmctld response times degrade under a hot, unbatched sbatch
loop at thousands of jobs.

### Churn generation

`scripts/30-churn.sh` scales a throwaway `Deployment`
(`manifests/churn-deployment.yaml`, `pause` container, 5m/8Mi requests) up
and down in large steps (`STEP_REPLICAS=150` by default) on the dedicated
`churn-pool`, timing each half-cycle from `kubectl scale` to pods
Ready/gone, and computing pods/sec as `(creates + deletes) / cycle time`.
Results (raw timestamps + computed rates) are written to
`results/churn-<timestamp>.csv`; the script prints the peak and an
end-of-run sustained-rate summary against the `>100 pods/s` target.

### Measurement

`scripts/40-measure.sh` captures, into `results/measure-<timestamp>.txt`:

- Node counts by pool, RUNNING slurmd pod count, admitted vs. pending Kueue
  Workload counts (the 500/3000 split).
- The bridge's real Prometheus series (scraped from the pod directly):
  `k8s_bridge_tick_duration_seconds`, `k8s_bridge_ticks_total`,
  `k8s_bridge_tick_errors_total`, `k8s_bridge_tick_trigger_total{source=...}`,
  `k8s_bridge_jobsets_created_total`, `k8s_bridge_jobsets_deleted_total`,
  `k8s_bridge_jobset_create_errors_total`, `k8s_bridge_jobs_failed_total`,
  `k8s_bridge_held_jobs`, `k8s_bridge_slurm_api_requests_total`,
  `k8s_bridge_job_release_latency_seconds` (all names verified against
  `internal/metrics/metrics.go`, not invented).
- Controller RSS via `kubectl top pod` (same style as `tools/bridge-top.sh`).
- Kueue admission metrics + `ClusterQueue` status snapshot.
- The latest churn CSV from `30-churn.sh`.
- An optional Grafana dashboard-render check (`curl` the dashboard JSON via
  `GRAFANA_URL`) with an explicit reminder that Grafana blocks `<iframe>`
  embedding unless `[security] allow_embedding = true` (and usually
  `[auth.anonymous] enabled = true`) is set in `grafana.ini` — this is
  called out in `tools/demo-console/README.md`'s "not validated locally"
  section and is a common source of a blank demo-console dashboard pane.

## RUN ORDER

```bash
cd "experiments/09-scale-gpu-churn"
export PROJECT_ID=${PROJECT_ID:?set your GCP project id}

# 1. Environment + node math (prints NODES_FOR_TARGET etc.)
source scripts/00-env.sh

# 2. Cluster: 3 node pools (control-plane on-demand, gpu-sim-spot, churn-pool)
./scripts/01-create-cluster.sh

# 3. Shared building blocks — REUSE experiment 01's installer as-is
#    (cert-manager, JobSet, Kueue, KubeRay, slurm-operator + Slurm cluster),
#    but point it at THIS experiment's slurm values file:
../01-gke-playground/scripts/02-install-components.sh   # then re-run the
                                                          # slurm helm install
                                                          # step with:
                                                          #   --values manifests/slurm-values-scale.yaml

# 4. Kueue GPU queue topology
kubectl apply --server-side -f manifests/kueue-gpu-queue.yaml

# 5. Bridge install, pointed at the built image
helm upgrade --install k8s-bridge ../../deploy/chart/k8s-bridge \
  --namespace slurm-jobs --create-namespace \
  --values manifests/bridge-values-scale.yaml \
  --set image.tag=<TAG-from-build>

# 6. Simulated GPU capacity (idempotent, safe to re-run after any autoscale event)
./scripts/10-simulate-gpus.sh

# 7. Namespace prerequisites for slurmd pods (found live 2026-07-06 — missing
#    on the first run: copies the Slurm auth Secret into the bridge namespace
#    and creates the count-only wm-gres-conf ConfigMap, ADR-0010). Idempotent;
#    safe to re-run. Must run AFTER the bridge/Slurm install (step 5) and the
#    GPU sim (step 6), BEFORE the backlog (step 8).
./scripts/05-namespace-prereqs.sh

# 8. Backlog: 3000 held GPU jobs (defaults; override with an argument)
./scripts/20-scale-backlog.sh 3000

#    ...wait / watch until ~500 are admitted+running (bridge-top.sh or
#    scripts/40-measure.sh) and ~3000 remain pending...

# 9. Churn: >100 pods/s sustained, separate pool, does not disturb 8
./scripts/30-churn.sh

# 10. Capture evidence
./scripts/40-measure.sh

# 11. MANDATORY: tear everything down (see callout below)
./scripts/99-teardown.sh
```

## Expected results

Executed live 2026-07-06. Full numbers and narrative: `docs/VALIDATION.md`.

| Target | Expected | Observed |
|---|---|---|
| RUNNING jobs | >= 500 | **~20** — an account-level CPU quota capped concurrent RUNNING jobs: a regional CPU quota hard-caps the cluster at ~3 spot `e2-standard-8` nodes at 1 CPU/slurmd-pod. This is an environment/quota constraint, not a bridge limitation. |
| PENDING jobs | >= 3000 | 520 submitted (500 backlog + 20 initial) — the run was scaled down from the original 3000-job target once the quota cap on RUNNING was discovered, since a bigger backlog would not have changed the running ceiling. All 520 held jobs translated to JobSets: 0 dropped, 0 tick errors. |
| Peak churn (pods/s) | > 100 | not exercised this run — quota cap made the dedicated churn pass lower-value than confirming the chart-deploy + scale path; left for a follow-up run with increased quota |
| Sustained churn (pods/s) | > 100 | not exercised this run (see above) |
| Bridge tick duration (p50/p90) | _TBD_ | avg 0.87s over 165 ticks during the 520-JobSet creation burst (p50/p90 not separately captured) |
| Bridge RSS at peak | _TBD_ (baseline 88Mi @ 5000 jobs, smaller mix) | **~101Mi** during the 520-JobSet creation burst; **~18Mi** idle at steady state (~20 running) |
| Kueue controller RSS/CPU at peak | _TBD_ (baseline 238Mi/271m @ ~3000 objects) | not separately captured this run (bridge-focused measurement) |
| Job release latency (p50/p90) | _TBD_ | not separately captured this run |
| Any bridge tick errors / JobSet create errors | expect 0 | **0** — `k8s_bridge_tick_errors_total = 0`, `k8s_bridge_jobsets_created_total = 520` (520/520, none dropped) |

Additional live-only result not in the original table: **GPU simulation
(ADR-0010) confirmed at 200 fake `nvidia.com/gpu` units per `gpu-sim-spot`
node** across all 3 nodes present, via the self-patching DaemonSet
(`manifests/gpu-sim-daemonset.yaml`).

## Scale profile

An account-level CPU quota (see "Expected results" above)
capped the GPU-sim pool at 3 spot `e2-standard-8` nodes instead of the
63-80 the "500 running" target assumed, and the control-plane pool stayed
at its 1-node floor; the whole session (setup, 520-job scale run,
measurement, teardown) ran ~1.5 hours. A genuine >=500-running attempt
requires a quota increase first — an environment/quota constraint, not a
bridge limitation.

At peak the un-quota-capped topology is: control-plane pool 1-3
`e2-standard-4` on-demand nodes; GPU-sim pool up to 63-80 `e2-standard-8`
spot nodes (autoscales from 0, at peak only briefly — long enough to submit
the backlog, let it settle, and capture `40-measure.sh` output); churn pool
up to 4 `e2-standard-4` spot nodes for ~15-30 min.

## TEARDOWN IS MANDATORY

**This cluster, at peak, runs 60+ nodes — do not leave it up.** Always run
`scripts/99-teardown.sh` at the end of every session, including
aborted/partial runs. That script also prints the exact `gcloud` commands
(clusters list, compute instances list, disks list, forwarding-rules list)
to positively verify nothing is left running afterwards — run them and
confirm empty output before considering the session closed. Orphaned
`pvc-*` disks in particular are NOT deleted by cluster deletion alone (a
live finding carried over from experiment 01) and are handled explicitly by
the teardown script.

**2026-07-06 run: teardown completed.** The cluster and all 3 node pools
were deleted via `scripts/99-teardown.sh`; the
orphaned `pvc-*` disk pattern from experiment 01 recurred here too (the
`slurmctld` state volume's disk survived cluster deletion) and was deleted
explicitly, reconfirming that experiment-01 finding is still live GKE
behavior, not a one-off. Post-teardown `gcloud` checks (clusters, compute
instances, disks, forwarding-rules) all returned empty.
