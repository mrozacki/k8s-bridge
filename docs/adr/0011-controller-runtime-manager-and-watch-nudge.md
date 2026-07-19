# ADR-0011: controller-runtime Manager adoption, hybrid poll + watch-nudge

- **Status:** Accepted
- **Date:** 2026-07-06

## Context

The bridge's reconcile loop (`internal/bridge/reconciler.go`, `Bridge.Run`)
is a plain ticker: every `pollInterval`, `tick()` lists Slurm jobs via
slurmrestd, snapshots owned JobSets and Kueue Workloads, admits/releases
held jobs, and cleans up finished ones. This has been the design since the
MVD (`docs/architecture.md`) — accepted tech debt,
because **slurmrestd has no watch/event API**: the Slurm side of the bridge
can only ever be polled. Two problems remain unaddressed by that polling
loop alone, both flagged by the 2026-07-05 project audit:

1. **Latency (backlog P1).** Once a JobSet is admitted, the bridge does not
   learn its dynamic nodes are ready (or that admission itself landed) until
   the *next* poll tick — up to a full `pollInterval` of pure waiting, the
   dominant throughput lever the profiling run measured (~3-4
   jobs/min under load). Kubernetes can push this information the moment it
   changes; the bridge was not using that channel at all.
2. **Observability, leader election, and cache cost (AUD2 + P8).** The
   bridge had no CNCF-standard control-plane primitives: no leader election
   (a second replica would double-process every job), no informer cache (P8:
   every tick issued a live LIST against the apiserver for both JobSets and
   Kueue Workloads), and metrics/health lived on a bespoke HTTP server
   disconnected from the ecosystem's own instrumentation
   (`sigs.k8s.io/controller-runtime/pkg/metrics`, workqueue/rest_client
   series, `leader_election_master_status`).

`sigs.k8s.io/controller-runtime` was already a direct dependency (for
`client.Client`, `ctrl.GetConfig()`, `ctrl.SetupSignalHandler()`) but only
for its client and bootstrap helpers — the Manager itself was never
constructed.

## Decision

Adopt a controller-runtime `manager.Manager` in `cmd/k8s-bridge/main.go`,
but **keep `Bridge.Run`'s polling loop as the correctness backbone**.
Concretely:

1. **Manager for infrastructure, not for replacing the loop.** The Manager
   supplies: a cached `client.Client` (structured AND unstructured reads —
   `client.Options.Cache.Unstructured: true` — so the per-tick JobSet and
   Kueue Workload LISTs in `internal/bridge/kueue.go` hit the informer cache
   instead of the apiserver, closing P8); leader election
   (`LeaderElection: true` by default, lock name `k8s-bridge-leader`, a
   `coordination.k8s.io` Lease in the WorkloadMixing CR's namespace, or
   `default` in file mode); and graceful shutdown wired to the existing
   `ctrl.SetupSignalHandler()` context.
2. **`Bridge.Run` becomes a `manager.RunnableFunc`.** It is registered via
   `mgr.Add(manager.RunnableFunc(b.Run))` and deliberately does NOT
   implement `manager.LeaderElectionRunnable` — per controller-runtime's own
   `runnables.Add` grouping (`pkg/manager/runnable_group.go`), any Runnable
   that is not a `LeaderElectionRunnable` defaults into the `LeaderElection`
   runnable group, which the Manager starts only *after* the `Caches` group
   has synced (`WaitForCacheSync`) AND this replica has won the election.
   This was verified by reading `controllerManager.Start`'s ordering
   (`pkg/manager/internal.go`: `Caches.Start` → `Others.Start` →
   `LeaderElection.Start`) and confirmed live by
   `TestBridgeRunGatedByLeaderElection` (envtest — a competing Lease holder
   blocks all ticks; freeing it lets this replica win and start ticking).
   `tick()`'s own logic is completely unchanged.
3. **Watches NUDGE the poll loop; they never replace it.** A small
   `Bridge.Nudge()` method sends on a buffered-size-1 channel; `Run`'s
   `select` now races the periodic timer against that channel, so a
   relevant Kubernetes event runs `tick()` immediately instead of waiting
   out the rest of the interval, while the timer remains the unconditional
   floor. Two watches drive it (`internal/bridge/watch.go`,
   `cmd/k8s-bridge/main.go`): JobSets (label-filtered to
   `k8s-bridge.x-k8s.io/managed-by=k8s-bridge`, the same filter
   `snapshot()`/cleanup already use) and Kueue Workloads (unfiltered — see
   the thin-surface note below). A new metric,
   `k8s_bridge_tick_trigger_total{source="timer|watch"}`, makes the nudge's
   effect observable.
4. **One HTTP surface, not two.** controller-runtime's Manager ships its own
   metrics server and healthz/readyz server, but as **two separate
   listeners** with no supported way to merge them onto one port — and the
   Helm chart's probes/Service assume exactly one (`:8080`). Rather than
   split the surface, the Manager's own servers are disabled
   (`Metrics.BindAddress: "0"`, `HealthProbeBindAddress: "0"`) and the
   bridge's existing consolidated mux (`cmd/k8s-bridge/main.go`,
   `internal/bridge/health.go`) keeps serving `/metrics`, `/healthz`,
   `/readyz` on `--metrics-addr`. The bridge's Prometheus collectors
   (`internal/metrics`) now register on controller-runtime's own
   `sigs.k8s.io/controller-runtime/pkg/metrics.Registry` instead of a
   private one, so that single `/metrics` endpoint also carries the
   CNCF-standard controller-runtime series (`rest_client_*`,
   `leader_election_master_status`, and any future `workqueue_*` once a
   controller's queue is non-trivial) for free — the health/ready semantics
   (`Ready` = recent successful tick) are untouched.
5. **Events (AUD2 remainder).** `mgr.GetEventRecorderFor("k8s-bridge")`
   feeds a `record.EventRecorder` on `Bridge.Recorder`, which is nil-safe
   (every call site checks). Normal `Created`/`Released` and Warning
   `JobSetFailed`/`TranslationFailed` Events land on the JobSet object.
6. **WorkloadMixing hot-reload (A1) rides on the same Manager.** A small
   `ConfigReconciler` (`internal/bridge/crdconfig.go`) watches the one
   WorkloadMixing CR the bridge was started against and calls
   `Bridge.setCfg` (an `atomic.Pointer[config.Config]`) on every spec
   change — no restart. `tick()` snapshots the config once per tick
   (`cfgSnapshot()`) so a reload landing mid-tick can never mix fields from
   two config generations. File mode never constructs this reconciler at
   all (no CR to watch), preserving its one-shot-load behavior exactly.

## Alternatives considered

- **Full event-driven reconcile, replacing polling entirely.** Rejected:
  slurmrestd cannot be watched — there is no event source for "a Slurm job
  became held" or "a Slurm job was cancelled". A pure watch-driven design
  would still need *some* polling loop against slurmrestd underneath,
  making "replace polling" a relabeling exercise at best and, at worst, a
  rewrite of the correctness-critical `tick()` logic (already
  well-tested, `-race`-clean, and validated live on GKE) for no behavioral
  gain. Too risky for the reward.
- **Pure polling (status quo).** The safe, already-shipped baseline — but it
  leaves the P1 latency gap and the P8/AUD2 gaps entirely open. Rejected as
  "leave it broken" once the audit had already named the fix.
- **Merge the Manager's metrics/health server with the bridge's mux by
  reverse-proxying or fronting both.** Considered and rejected: it would
  mean running two listeners internally and stitching them with a proxy,
  more moving parts than disabling one and keeping the collectors on a
  shared registry. The chosen approach gets the same "one registry, one
  endpoint, controller-runtime metrics included" outcome with less code.
- **Watch Kueue Workloads with a fully filtered, indexed cache (deeper P8
  fix).** Rejected for this iteration: Kueue does not stamp a
  server-side-filterable "bridge-owned" label onto Workload objects (the
  same limitation `snapshot()`'s LIST comment already documents), so a tight
  filter would require either guessing at a naming convention (fragile) or
  a client-side field index inside `kueue.go` (more surface, deferred). An
  unfiltered Workload watch is cheap and safe here specifically *because*
  Nudge coalesces: an event from an unrelated Workload costs at most one
  extra, harmless tick.

## Consequences

- **Latency**: JobSet/Workload events now wake the loop immediately instead
  of waiting out `pollInterval`; the timer remains the floor for
  slurmrestd-side changes and as a safety net if watches lag or disconnect.
- **Cost**: the per-tick JobSet and Workload LISTs are now served from the
  informer cache, removing the apiserver LIST cost the audit flagged (P8).
- **Safety at scale**: leader election makes running >1 replica of the
  bridge safe (idle standby) for the first time; the chart still defaults to
  1 replica, but the RBAC and flag are in place for an operator who wants a
  hot standby.
- **Observability**: one `/metrics` endpoint now carries both bridge and
  controller-runtime series; Events give Slurm-adjacent operators a
  `kubectl describe jobset` story they did not have before.
- **New moving parts to watch**: the Manager's own leader-election retry
  cadence (default `LeaseDuration`/`RenewDeadline`/`RetryPeriod`) governs
  failover time on a bridge crash — not yet tuned or documented as an SLO;
  a future session should decide whether the defaults (15s/10s/2s) are
  acceptable or need shortening for this workload.
- **Thin-surface preserved**: all Kueue GVK knowledge (the Workload watch
  source, `WorkloadNudgeSource`) stays in `internal/bridge/kueue.go`;
  `scripts/verify-thin-surface.sh` remains green.
- **Rollback path**: remove the Manager entirely from `main.go` — construct
  a plain `client.New(...)` as before, call `b.Run(ctx)` directly, and drop
  `internal/bridge/watch.go`'s wiring calls. `tick()`, `admitHeldJobs()`,
  `cleanupFinishedJobs()`, and every other piece of reconcile logic are
  unchanged by this ADR, so rollback is purely a `main.go`/wiring revert,
  not a logic rewrite. The `Bridge.Nudge()`/`cfgSnapshot()`/`setCfg()`
  plumbing can stay even without a Manager (Nudge simply never fires,
  cfgSnapshot always returns the one config set at construction) — nothing
  breaks if the watches are never wired up.

## Live validation (2026-07-06)

The Manager adoption was deployed in-cluster for the first time via the
Helm chart (backlog A4, `experiments/09-scale-gpu-churn`). It works — the
bridge reached `Ready`, held leader election, and translated 520 held jobs
into 520 JobSets with zero tick errors. Getting there required correcting
two consequences of this ADR that a direct-binary run never exercises
(full detail: `docs/VALIDATION.md`, bugs B7/B8):

1. **The cache must be namespace-scoped, not cluster-scoped.** The
   Manager's informer cache defaults to cluster scope, but the chart grants
   only a namespaced `Role` (SEC2's least-privilege default). A
   cluster-scoped cache's LIST/WATCH came back forbidden, the cache never
   synced, and `Run` — gated on `WaitForCacheSync` per decision 2 above —
   never started, so the pod never became `Ready`. Fixed by scoping the
   cache to `POD_NAMESPACE` (`cacheScopedToNamespace()` in `main.go`), which
   is safe because the chart guarantees `config.namespace == release
   namespace == POD_NAMESPACE`.
2. **Caching unstructured reads (Kueue Workloads) deadlocked the first
   tick.** Decision 1 above set `Client.Cache.Unstructured: true`, intending
   to cache both JobSet and Workload LISTs. JobSets (typed) cached cleanly;
   Workloads (unstructured — no generated Go types for Kueue's API in this
   repo, per the thin-surface constraint) went through a lazily-started
   unstructured informer whose first LIST never completed, hanging the
   tick forever. Fixed by reverting to `Client.Cache.Unstructured: false`:
   Kueue Workload reads are live/uncached again (the exact pre-ADR-0011
   behavior), while JobSets remain cache-served. P8-for-Workloads is
   deferred, not abandoned — the correct fix is a pre-warmed typed
   informer for the Workload GVK, started and synced explicitly at Manager
   construction rather than lazily on first read.

Neither correction changes this ADR's core decision (Manager for
infrastructure, polling loop for correctness, watches as a nudge) — both
are scoping/caching fixes within decision 1's implementation, and the
"Cost" consequence above (informer-cache-served LISTs) now applies to
JobSets only, not Workloads, until the P8-for-Workloads follow-up lands.
