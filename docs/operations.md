# Day-2 operations guide

Fills the gap identified in the day-2 audit: the design docs specified
metrics but no SLOs, alerts, or on-call procedures. Runbooks below encode
behavior we VALIDATED live, not theory. Metrics, leader election, and
Kubernetes Events are **shipped and live-validated** (ADR-0011, backlog
AUD2) — not a future phase.

## SLO proposals (to be ratified with live-scraped data)

| SLI | Target (initial) | Source |
|---|---|---|
| held-job → JobSet released onto its nodes | p99 < 2× pollInterval | `k8s_bridge_job_release_latency_seconds` (histogram, shipped — time from first-seen held+pending to release) |
| JobSet admitted → nodes registered | p90 < 60 s (image cached) | observed 10-20 s |
| preemption → job requeued | < SlurmdTimeout + 30 s (validated at 60 s setting) | Slurm + bridge |
| bridge error-tick ratio | < 1% over 1 h | `k8s_bridge_tick_errors_total` / `k8s_bridge_ticks_total` |
| watch-nudge share of ticks | informational, not yet an SLO target | `k8s_bridge_tick_trigger_total{source="timer"\|"watch"}` |

## Alerts (minimum viable set)

These seven alerts now ship as an **optional** `PrometheusRule` in the
k8s-bridge chart (`prometheusRule.enabled`, off by default — it requires
the prometheus-operator CRDs, same prerequisite as `serviceMonitor.enabled`;
see `deploy/chart/k8s-bridge/templates/prometheusrule.yaml`). Several rules
depend on exporters this chart does not ship (a Slurm node-state exporter
for alert 3, `kube-state-metrics` for alerts 4 and 5) — the template's
per-rule comments say exactly what each one needs to actually fire.

1. Bridge tick failures > 3 consecutive (Slurm URL, token expiry — token
   TTL is finite; rotation is an admin duty until the operator handles it).
2. Any Workload Pending > 30 min with QuotaReserved=False in a mixing
   queue (capacity starvation or misconfigured quota coverage — remember
   the GPU-not-covered incident).
3. Dynamic node in DOWN > 5 min (ReturnToService misconfig regression).
4. slurm-controller-0 not Ready, kueue-controller-manager not Ready.
5. RayCluster head pod restart (SPOF — kills all in-cluster Ray jobs).
6. `k8s_bridge_jobs_failed_total` increasing (D1: a JobSet died before its
   Slurm job ran — worth knowing even though the job itself is now failed
   cleanly instead of hanging forever).
7. `leader_election_master_status == 0` on every replica simultaneously
   (no replica holds the Lease — the reconcile loop is not running at all).

## Runbooks (each validated in an experiment)

**Bridge down.** Symptom: jobs stay held, comment stuck at "held for
admission". Impact: SAFE — nothing breaks, nothing leaks state; running
jobs continue. Action: restart bridge; it reconstructs everything from
Slurm + JobSet labels (idempotent by naming).

**Slurm believes a job is RUNNING but pods are gone.** That is the
preemption/eviction window: Slurm notices after SlurmdTimeout (we run 60 s)
and requeues; Kueue resumes the JobSet when capacity allows; nodes
re-register under the SAME names. Verify `ReturnToService=2` — without it
nodes stay DOWN and the job pends on ReqNodeNotAvail (manual fix:
`scontrol update nodename=... state=resume`).

**Workload pending with a topology message** ("doesn't allow to fit").
Not an error — TAS found no domain with room for the whole gang. Check the
TOPOLOGY panel/dashboard; either capacity frees up or the user relaxes
--switches. Remember: TAS-bound flavors apply all-or-nothing placement to
EVERY workload in the queue, annotated or not.

**Whole team stalled, other team busy.** Check borrowing: `kubectl get
clusterqueue -o wide` + events mentioning "reclamation within the cohort".
Reclaim evicts the ENTIRE borrowing workload — a big borrowed gang means a
big displacement; tune lendingLimit if a team needs a floor (shared
RayCluster base capacity MUST have lendingLimit: 0).

**JobSet dead, Slurm job pending forever (backlog D1 — fixed 2026-07-06).**
Symptom (pre-fix): a JobSet reaches `Failed`/`DeadlineExceeded` (e.g.
`activeDeadlineSeconds` fired before the Slurm job's nodes ever registered)
but the Slurm job stayed pending with `BadConstraints` forever. **Current
behavior:** `jobSetFailed`/`failJobForDeadJobSet`
(`internal/bridge/reconciler.go`) detect the `Failed` condition, write an
explanatory Slurm comment, cancel the Slurm job via `CancelJob`, increment
`k8s_bridge_jobs_failed_total`, and emit a `JobSetFailed` Warning Event —
`kubectl describe jobset <name>` now shows why. If slurmrestd is
unreachable when the cancel is attempted, the JobSet is deliberately kept
so the next tick retries the whole failure path; if you still see a job
stuck with no Event and no `jobs_failed_total` increment, check
slurmrestd reachability first. Live reproduction and original defect:
`docs/VALIDATION.md`.

**Token expiry.** Symptom: every tick fails with 401. Mint a new JWT
(`scontrol token`), replace the secret/file; the bridge re-reads it per
request — no restart needed.

**slurmrestd TLS-vs-plain-HTTP mismatch (found live).** Symptom:
bridge logs "server gave HTTP response to HTTPS client" on every tick. The
bridge defaults to `https://` for slurmrestd by design (SEC1's
threat-model decision) — production must front slurmrestd with TLS. A
plain-HTTP test/dev slurmrestd (e.g. the stock Slinky chart's
`slurm-restapi` service on :6820) needs the config/CR's
`slurmRestURL: http://...` **and** `allowInsecureHTTP: true` set
explicitly; this is not something to "fix" by weakening the default.

**Token Secret unreadable ("permission denied") in the deployed chart
(found live, bug B2).** Symptom: the bridge pod starts but every slurmrestd
call fails reading the token file, even though the Secret exists and is
mounted. Cause: the container runs as the distroless-nonroot image's UID
`65532`, which cannot read a `root`-owned, mode-`0400` file. Fix already in
the chart: pod-level `fsGroup: 65532` plus the token Secret's
`defaultMode: 0440` (group-readable). If you hand-roll a Secret/mount
outside the chart's defaults, replicate both settings.

**Leader election forbidden / pod never reaches Ready (found live, bug
B1/B8).** Symptom: the pod stays not-Ready indefinitely, with (once
`ctrl.SetLogger` is confirmed wired — see below) log lines like "forbidden
... at the cluster scope" or a Lease-related RBAC error. Two independent
causes were found in the first chart deploy, both already fixed in
`main.go`: (1) the leader-election Lease must live in the pod's own
namespace (`POD_NAMESPACE` downward-API env var, which the chart injects) —
not `default`; (2) the Manager's informer cache must be scoped to
`POD_NAMESPACE`, not cluster scope, to match the chart's namespaced-only
RBAC (`SEC2`). If you see this on a hand-rolled deployment (not the chart),
check both the Lease namespace and the cache scope against the actual RBAC
granted.

**Teardown discipline.** Always `99-teardown.sh`; it also sweeps orphaned
`pvc-*` disks (cluster deletion does NOT remove them — learned the
billable way). The sweep is scoped by `labels.goog-k8s-cluster-name` and
`--zones` so it only deletes disks belonging to the torn-down cluster — a
must in shared GCP projects, where an unscoped `pvc-*` filter would delete
other engineers' volumes (2026-07-08 finding).

## Local developer toolchain (managed workstation images)

**A distro `helm` package may not be upstream Helm 3.** On some corporate or
otherwise managed Linux workstation images, the `helm` found on `PATH` is a
different binary, and chart deployments fail on standard charts and flags with
confusing errors (or `helm: command not found`). Do NOT `apt-get install
helm`. Install upstream Helm 3 into your user PATH without root:

```sh
curl -fsSL https://get.helm.sh/helm-v3.16.0-linux-amd64.tar.gz \
  | tar -xz --strip-components=1 -C ~/.local/bin linux-amd64/helm
helm version   # confirm ~/.local/bin/helm precedes any system helm on PATH
```

## Logging, metrics & Events (backlog AUD2, ADR-0011 — shipped)

The bridge emits structured `slog` **JSON** to **stdout** (`cmd/k8s-bridge/main.go`),
ready for Cloud Logging or any JSON-aware log pipeline, at a level set by
`--log-level` (`debug`/`info`/`warn`/`error`, default `info`) — one line per
state transition per job (submit/translate/admit/release/cleanup) so a job's
lifecycle is a single grep/filter query. Genuine failures that used to log at
Info (kueue.go priority-patch, comment-propagation, annotation-update
failures) are now Warn, visible to a `level>=WARN` filter; the noisy
per-tick-per-job "dynamic nodes not registered yet" line is Debug.

Prometheus metrics are served on `--metrics-addr` (default `:8080`) at
`/metrics`, registered on controller-runtime's own metrics registry
(`internal/metrics` — ADR-0011, so `/metrics` also carries the
CNCF-standard `rest_client_*` and `leader_election_master_status` series
for free):

- `k8s_bridge_tick_duration_seconds` (histogram) — one full reconcile tick's wall-clock time.
- `k8s_bridge_ticks_total` / `k8s_bridge_tick_errors_total` (counters) — the error-tick ratio SLO's numerator/denominator.
- `k8s_bridge_tick_trigger_total{source="timer"|"watch"}` (counter) — which triggered each tick (ADR-0011 watch-nudge observability).
- `k8s_bridge_jobsets_created_total` / `k8s_bridge_jobsets_deleted_total` / `k8s_bridge_jobset_create_errors_total` (counters).
- `k8s_bridge_jobs_failed_total` (counter) — Slurm jobs failed by the bridge because their JobSet reported `Failed` (D1).
- `k8s_bridge_held_jobs` (gauge) — held+pending Slurm jobs observed in the most recent tick.
- `k8s_bridge_last_successful_tick_timestamp_seconds` (gauge) — unix time of
  the last tick that completed without error; graph staleness as
  `time() - k8s_bridge_last_successful_tick_timestamp_seconds` (the operator
  dashboard's "Since last good tick" stat). Added because a stalled counter's
  scrape timestamp reflects Prometheus's clock, not the bridge's last useful
  work.
- `k8s_bridge_job_release_latency_seconds` (histogram) — time from a job first observed held+pending to being released onto its dynamic nodes (feeds the release-latency SLO above; cross-tick `seenAt` map, pruned every tick).
- `k8s_bridge_slurm_api_requests_total{method,code}` (counter) — every slurmrestd call, via the `slurm.Client.OnRequest` callback (kept out of `internal/slurm`, which stays stdlib-only).
- `k8s_bridge_slurm_request_duration_seconds` (histogram) — per-request
  slurmrestd call latency, labeled the same way as the requests-total
  counter; use it alongside `slurmRequestTimeout`/`slurmRequestsPerSecond`
  (below) to see whether a raised timeout or a client-side rate limit is
  actually needed for your slurmrestd's observed latency.
- `k8s_bridge_jobset_identity_conflicts_total` (counter) — a JobSet with the
  bridge's deterministic name already exists but does not match the Slurm
  job UID the bridge expects (identity mismatch, e.g. a stale/terminating
  JobSet from a prior job that reused the same ID). Watch this alongside the
  D1 (`k8s_bridge_jobs_failed_total`) alert; a nonzero, growing count points
  at a naming/cleanup race rather than a Slurm-side failure.
- `k8s_bridge_slurm_jobs_listed` (gauge) — total jobs returned by the most
  recent `/jobs` poll, ALL states (not just held/pending like
  `k8s_bridge_held_jobs`) — tracks how much queue the bridge must parse each
  tick; alert on sustained growth before payload size outpaces memory limits.
- `k8s_bridge_orphaned_jobs_cancelled_total` (counter) — Slurm jobs cancelled
  because their JobSet disappeared (D2, gated by `cancelOrphanedJobs`).
  Distinct from `k8s_bridge_jobs_failed_total` (that one fires on a JobSet
  reaching a `Failed` *condition*; this one fires on a JobSet's *absence*).
- `k8s_bridge_orphan_cancellations_refused_total` (counter) — ticks where the
  bridge refused to cancel any orphan candidates because too large a
  fraction of managed jobs looked orphaned at once (the signature of a
  partially-visible informer cache, not genuine mass orphanhood). Any
  sustained rate here needs investigation of cache/RBAC health, not more
  orphan cancellations.

ray-bridge exposes its own metrics on the same `/metrics`-style surface
(`--metrics-addr`, default `:8080`) from the `ray-bridge` binary, not
`k8s-bridge` — do not expect these on the k8s-bridge Deployment's Service:

- `ray_bridge_worker_jobsets_created_total` (counter) — dedicated worker
  JobSets created for inner RayJobs, including recreations after a bounded
  retry; a growing gap versus `ray_bridge_worker_jobsets_failed_total` flags
  a create/fail loop.
- `ray_bridge_worker_jobsets_failed_total` (counter) — the D1 analog for
  ray-bridge: every worker JobSet `Failed` condition the reconciler handled.
  A failed worker JobSet is retried up to 3 times
  (`raybridge.MaxWorkerRetries`), each retry deleting and recreating the
  JobSet and emitting a `WorkerJobSetFailed` Warning Event on the RayJob;
  once the retry budget is spent the reconciler emits a final
  `WorkerRetriesExhausted` Warning Event and deliberately leaves the failed
  JobSet in place for inspection (`kubectl describe rayjob <name>` shows
  both). Recover by removing the RayJob's
  `ray-bridge.x-k8s.io/worker-retries` annotation (and the failed JobSet) to
  re-arm the retry loop.
- `ray_bridge_reconcile_errors_total` (counter) — errors from the
  `raybridge.Reconciler` reconcile loop.
- `ray_bridge_webhook_decisions_total{decision}` (counter) — one increment
  per admission-webhook `Decide()` outcome (see `docs/ray-bridge-reference.md`
  for the `pinned`/`denied`/`skipped` decision semantics); only populated
  when the webhook is enabled (`--enable-webhook`, off by default).

### Dashboards

Two Grafana dashboards ship in `dashboards/`: `researcher-dashboards.json`
(Slurm/Ray/serving researcher view — Kueue + KubeRay metrics only, zero
bridge-sourced panels by design) and `operator-dashboard.json`
(operator/on-call view, backlog A3). The operator dashboard is a companion
to the seven `PrometheusRule` alerts above — it puts the metric behind each
alert (tick failures, control-plane readiness, jobs-failed, leader election)
on screen, plus the bridge/ray-bridge internals and safety-guard counters
(`k8s_bridge_jobset_identity_conflicts_total`,
`k8s_bridge_orphaned_jobs_cancelled_total`,
`k8s_bridge_orphan_cancellations_refused_total`) an on-call engineer checks
once an alert fires. See `dashboards/README.md` for import instructions.

Kubernetes Events (Normal `Created`/`Released`, Warning
`JobSetFailed`/`TranslationFailed`) land on the JobSet object itself via
the Manager's `EventRecorder`, so `kubectl describe jobset <name>` is now
part of the on-call toolkit alongside `/metrics` and logs.

Upgrades: pin chart+operator versions in values; the bridge itself is
stateless — rolling replace is safe (leader election means a second
replica, if run, takes over cleanly); Slurm chart upgrades restart
slurmctld (running jobs survive, submissions briefly queue). Backups:
slurmctld StateSaveLocation PVC snapshot before chart upgrades (accounting
DB once phase B lands).

## High availability & leader failover (validated in envtest, 2026-07-12)

Recommended HA posture: **`replicas: 2` with the default lease timings**.
Leader election is on by default (`--leader-elect=true`); the standby
replica runs its Manager and informer caches but its reconcile loop never
starts until it wins the `k8s-bridge-leader` Lease, so a second replica is
always safe — it cannot double-create or double-cancel anything. Both
failover paths are pinned by
`internal/bridge/failover_integration_test.go` (two
Managers, one real kube-apiserver, distinct lease identities, run via
`make test-integration`): the standby never ticks while the Lease is held,
takes over within the bounds below, and the old leader never ticks again
after the takeover (no overlapping leadership windows).

**Expected takeover time.** `cmd/k8s-bridge/main.go` does **not** set
`LeaderElectionReleaseOnCancel`, so the Lease is never voluntarily
released — every handover (crash, node loss, OOM-kill, and today even a
graceful `SIGTERM` rollout) waits for lease **expiry**. With the defaults
(`--leader-lease-duration=15s`, `--leader-renew-deadline=10s`, retry
period 2s — controller-runtime's default, not exposed as a flag) the
standby acquires within roughly `LeaseDuration` of the dead leader's last
renewal, observed with up to one retry period of lag, plus one retry
period of acquire granularity: **expect ~15 s, worst case ~19 s** of
no-leader gap. That gap is benign: no ticks means no JobSet churn, and
Slurm jobs simply stay held a little longer. If a faster rolling handover
is ever needed, `LeaderElectionReleaseOnCancel` (takeover in under one
retry period; the release path is already validated by the same test)
is the knob — safe here because the process exits immediately after
`mgr.Start` returns, which is the precondition controller-runtime's docs
attach to that option.

**What failover forgets (in-memory-only state).** All durable state lives
in the API server and in Slurm; the tick loop is level-based, so the new
leader's first tick re-derives everything that matters. Three cross-tick
memories in `internal/bridge/reconciler.go` are process-local and reset on
failover, each with a bounded, fail-safe consequence:

- `orphanSeen` (orphan-cancellation grace counters): the count of
  consecutive ticks a released job has been seen without its JobSet
  restarts from zero, so an in-progress orphan countdown begins again —
  cancellation is **delayed** by up to `orphanGraceTicks x pollInterval`,
  never issued prematurely. The guard fails in the safe direction.
- `commentState` (Slurm comment-rewrite throttle): the new leader does not
  know what it last wrote, so it may rewrite each tracked job's comment
  **once** immediately after takeover (one redundant `scontrol update` per
  job), after which throttling resumes normally.
- `seenAt` (release-latency first-seen timestamps): lost, so jobs already
  pending at failover produce **no**
  `k8s_bridge_job_release_latency_seconds` sample when they are later
  released. The histogram loses samples across a failover; it never
  reports wrong values.

**Standby readiness is 503 by design.** `/readyz` requires a recent
successful tick and the standby never ticks, so the non-leader pod reports
`no successful tick yet since startup` indefinitely while staying live on
`/healthz`. Do not alert on a single unready bridge pod when
`replicas: 2`; the signal that matters is alert 7 in the minimum set
(`leader_election_master_status == 0` on **every** replica
simultaneously), which fires exactly when no takeover happened within the
bounds above.

**Readiness in supervisor mode (ADR-0015 Phase A).** With `configSource=cr`
and no `workloadmixing.name`, one controller runs a reconcile loop per
WorkloadMixing CR, and `/readyz` becomes an AGGREGATE: 200 iff **every
running loop** is Ready, with per-CR detail in the JSON body (`bridges`
keyed by `namespace/name`). Semantics to know when reading the probe:

- **Zero CRs is READY** ("no WorkloadMixing objects...") — an empty
  namespace is not a failure, and a freshly installed controller must not
  sit unready until someone creates the first CR.
- **The standby replica stays 503** ("supervisor not started: this replica
  has not won leader election") — the same posture as single-CR mode
  above, so alert rules keep one shape across modes.
- **Blocked CRs do not fail the probe.** A CR refused for
  `ConflictingSpec` / `InvalidSpec` / `StartFailed` is listed in the
  body's `blocked` map but never flips the verdict: failing every healthy
  CR's probe over one misconfigured CR would be the wrong trade. Alert on
  those via the CR's `Ready` condition and the Warning events instead
  (`kubectl get wm` shows Ready per CR).
- **One CR's dead Slurm cluster DOES fail the probe** (its running loop
  goes unready), which is the honest signal — but per-CR attribution lives
  in the body and the CR conditions, not in the status code.

**Metrics stay process-global aggregates for now.** The per-CR
`workloadmixing` metric label (ADR-0015 §Decision 4) is deliberately
deferred — it re-shapes every `k8s_bridge_*` series into a labeled vector
and ripples into the dashboards and alert rules — so with several CRs the
existing series read as sums across all loops, and
`k8s_bridge_last_successful_tick_timestamp` reflects the most recent
success of ANY loop (it can mask one stuck CR: use the per-CR Ready
conditions for that until the label lands).

## Large-scale & DWS production runbook (validated 2026-07-08)

**Memory OOM at >5,000 backlog jobs.** When holding thousands of pending jobs in Slurm / Kueue, `kueue-controller-manager` and `k8s-bridge` will exceed the default `512Mi` memory limit during preemption and reconciliation loops (measured steady-state RSS of 307 MiB, with working set spikes above 512 MiB during unpaginated JSON decoding). For production deployments with backlogs up to 10k jobs, configure pod memory limits to at least `1Gi` (`--limits=memory=1Gi` or Helm values `resources.limits.memory: 1Gi`).

**DWS (Dynamic Workload Scheduler) with GPU node pools.** GKE rejects `ProvisioningRequest` objects for CPU-only node pools (`Resize requests without accelerators are not supported`). To utilize DWS with Kueue:
- Node pools MUST be provisioned with `--enable-queued-provisioning` and `--reservation-affinity=none` (the `--flex-start` flag alone is insufficient for Kueue integration).
- Workloads targeting GKE GPU pools MUST include an explicit toleration for `nvidia.com/gpu:NoSchedule`, matching GKE's default automated GPU node taints.

**Multi-partition / multi-cluster fan-out (WorkloadMixing).** When running `k8s-bridge` in local file mode (`--config /etc/k8s-bridge/config.yaml`), the controller ignores `WorkloadMixing` CRs by design. Since ADR-0015 Phase A the CR-mode default is the supervisor: `--config-source=cr` (chart: `configSource=cr` with `workloadmixing.name` empty) runs one reconcile loop per WorkloadMixing CR in the controller's own namespace — one Deployment fans out across partitions and Slurm clusters, and creating a CR is self-serve. `--workloadmixing <namespace>/<cr-name>` remains the explicit single-CR binding (compatibility path).
