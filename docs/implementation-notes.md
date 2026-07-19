# Implementation details distilled from prototype code comments

The k8s-bridge prototype's Go source carries a second layer of documentation
beyond the usual doc-comments: inline notes tagged "Live finding", "run-N
finding", "session-N", "audit ...", "e2e iteration", "scale finding", and
similar, each recording something the team learned only by running the
prototype against a real (or real-ish) Slurm control plane, slurmrestd, Kueue,
and Kubernetes. Those notes are easy to miss on a first read and easy to lose
if the prototype is ever rewritten from a clean-room design doc instead of
read line by line. This note pulls them out of `internal/**`
and `test/e2e/slurm/slurm.conf` into one place, grouped by
theme, so a production reimplementation does not have to rediscover the same
failure modes.

This note is **descriptive, not normative**: it records what the comments say
was observed and why the prototype's code is shaped the way it is. It does not
prescribe the production architecture — that is `docs/architecture.md`
and the ADRs. Treat every bullet as "this bit us once; know about it," not as
a requirement.

## Dynamic-node registration (slurmd -Z, cons_tres, configless)

- Dynamic node registration (`slurmd -Z`) is supported **only** under the
  `select/cons_tres` plugin — confirmed against upstream docs, the
  slurm-operator source, and a live cluster (root cause tagged "L1").
  `test/e2e/slurm/slurm.conf:20` (header), `:74-78`
- `enable_configless` lets the bridge's slurmd pods run `slurmd -Z
  --conf-server slurmctld:6817` and fetch `slurm.conf` from slurmctld at
  startup rather than carrying their own copy.
  `test/e2e/slurm/slurm.conf:65-67`
- `MaxNodeCount` must leave headroom above the statically configured node
  count for dynamic nodes to register into (the slurm-operator hardcodes
  1024). `test/e2e/slurm/slurm.conf:79-82`
- `ReturnToService=2` is required so a dynamic node that vanished and
  re-registered does not stay stuck `DOWN` (live finding, observed in testing).
  `SlurmdTimeout=60` tightens dead-node detection versus the 300s default.
  `test/e2e/slurm/slurm.conf:83-87`
- `TreeWidth=65533` disables message fanout between slurmds — the classic
  dynamic-node setting. Slurm 23.11+ handles fanout for dynamic nodes on its
  own, so this is defensive/medium-confidence rather than strictly required.
  `test/e2e/slurm/slurm.conf:89-93`
- A partition needs at least one statically configured node (even a `FUTURE`
  placeholder that never actually runs anything) so it has nonzero capacity
  at submit time — with zero configured nodes, slurmctld can reject
  `sbatch --hold` outright before the bridge ever sees the job. Whether Slurm
  26.05 actually accepts a held job into a partition whose only node is
  `FUTURE` is flagged as needing runner validation; the fallback is a
  `State=CLOUD` node definition instead.
  `test/e2e/slurm/slurm.conf:116-127`
- The registered dynamic-node name is the pod's own deterministic hostname
  (`<jobset>-<replicatedJob>-0-<completionIndex>`), which is what lets the
  bridge delete node records without tracking pods directly (live finding,
  experiment 02). `internal/translate/translate.go:76-79`
- slurmd autodetects the **host's** hardware, not the pod's cgroup limits —
  the node's advertised resources (CPUs, RealMemory, GRES) must be passed
  explicitly via `--conf`, or Slurm packs multiple tasks onto one
  under-advertised pod (live finding, experiment 02).
  `internal/translate/translate.go:141-144`
- The slurmd image's entrypoint word-splits its arguments, so a
  multi-parameter `--conf` string must be wrapped in inner single quotes
  (matching what the slurm-operator itself does: `--conf "'Features=slinky'"`)
  — without them only the first `key=value` pair survives and the node
  advertises host-sized resources (live finding, observed in testing).
  `internal/translate/translate.go:384-388`,
  `:389-400`
- Setting a Slurm `Features` constraint that names a feature no node
  advertises yet fails with "Invalid feature specification" — this makes the
  originally designed order ("set feature, then wait, then release")
  impossible. The prototype instead treats a successful constraint-set call
  as the readiness signal itself: nodes must already be registered for it to
  succeed (live finding, experiment 02).
  `internal/bridge/reconciler.go:575-581`

## Auth & credentials (auth/slurm, CredType, jwt, capabilities)

- Internal daemon auth uses `auth/slurm` with a per-run generated
  `slurm.key`; the identical key is loaded as a Kubernetes Secret and mounted
  by `translate.ToJobSet` into every slurmd pod at `/etc/slurm/slurm.key`.
  `test/e2e/slurm/slurm.conf:43-49`,
  `internal/translate/translate.go:161-176`
- `CredType` must be pinned to match `AuthType` explicitly (`cred/slurm`
  alongside `auth/slurm`). Without it, the credential subsystem silently fell
  back to a munge-flavored default: slurmctld logged
  `cred_p_create_net_cred: _encode() failure` on every RPC to the registered
  slurmd (no munged existed in these containers), marked the node
  not-responding/DOWN mid-job, and the running job was requeued to PENDING
  (observed in testing). `test/e2e/slurm/slurm.conf:50-56`
- `use_client_ids` (`AuthInfo`) is needed so slurmrestd, running as an
  unprivileged local user, can convey the REST-authenticated user's identity
  to slurmctld. `test/e2e/slurm/slurm.conf:47-49`
- REST auth is `auth/jwt` with a per-run HS256 key; the bridge's token is
  minted with `scontrol token` and mounted into the bridge pod.
  `test/e2e/slurm/slurm.conf:58-62`
- **slurmUser identity, not payload shape, caused a persistent 422** on
  `SetJobComment`/job updates. Root cause (solved, documented in
  `docs/VALIDATION.md`): the bridge was
  configured with `slurmUser="k8s-bridge"`, not a real provisioned Slurm
  user; slurmrestd answered every job update with HTTP 422 error 2010
  "Invalid user id" via the `X-SLURM-USER-NAME` header. Fix: leave
  `slurmUser` empty (so the JWT's own embedded user is used) or configure a
  real provisioned user.
  `internal/slurm/client.go:805-819`
- slurmd needs writable cgroups and dies in a fully unprivileged container —
  an upstream (slurmd/slinky) constraint, not something the bridge controls;
  the slurm-operator runs its own nodeset pods privileged for the same
  reason. `internal/translate/translate.go:337-344`
- Live e2e bisection (2026-07-11, first runs of the unprivileged path
  against real slurmctld 26.05.1) found the **minimal capability set**
  needed when not running fully privileged:
  - Without `SETUID`/`SETGID`: slurmd dies at startup with `fatal: Failed to
    drop supplementary groups, setgroups: Operation not permitted` before it
    even contacts the conf server.
  - Without `CHOWN`: slurmd starts and registers, but **every** batch launch
    dies instantly (`slurmstepd return code -1`) and the job is requeued
    forever — slurmstepd chowns the job's spool/output files to the job
    owner before dropping privileges.
  - `KILL` and `DAC_OVERRIDE` were bisected as **not** required (slurmd runs
    as container root, which can signal/read its own trees without them).
  - `AllowPrivilegeEscalation: true` is required because slurmd's re-exec
    needs it under the added capabilities.
  `internal/translate/translate.go:351-379`
- The cgroup-free e2e test config avoids privileges entirely on the control
  plane side by combining `ProctrackType=proctrack/pgid` with
  `TaskPlugin=task/none`, so the cgroup plugins are never loaded and nothing
  needs kernel privileges a default container lacks.
  `test/e2e/slurm/slurm.conf:2-11`, `:109-111`

## GRES / GPU (count-only gres.conf, the /dev/nvidia0 pitfall, autodetect vs pod-sizing)

- **Do not bind-mount a `gres.conf` into the slurmd pod.** An earlier version
  (a live run) bind-mounted a count-only `gres.conf` at
  `/var/spool/slurmd/conf-cache/gres.conf` on the belief that configless
  slurmd would not otherwise receive it from the controller. On Slurm 26.05
  this **broke dynamic-node registration entirely**: configless slurmd
  fetches its whole config from slurmctld and writes it under `conf-cache/`,
  and renaming its freshly-written `gres.conf.new` over the bind-mounted,
  read-only `gres.conf` fails with `_write_conf: ... Device or resource
  busy`, aborting slurmd before it ever registers (found live, documented in
  `docs/VALIDATION.md`). The controller already
  distributes the count-only `gres.conf` (`Name=gpu Count=1`) to configless
  slurmd via the Slurm chart's `configFiles`; the per-pod mount was both
  redundant and the direct cause of failure — removed.
  `internal/translate/translate.go:179-193`
- The `gpus` value computed from the Slurm job's GRES request still drives
  the extended-resource **request on the Kubernetes pod** even though no
  `gres.conf` is mounted — GPU count travels via `--conf ... Gres=gpu:N` on
  the slurmd command line instead.
  `internal/translate/translate.go:193`, `:396-397`
- GPU requests are parsed from `tres_per_node` (e.g. `gres/gpu:2`,
  `gres/gpu=2`, `gres/gpu:a100:2`, bare `gres/gpu`) with a fallback to
  `tres_per_job` (what `sbatch --gpus=N` populates), distributing the
  job-level GPU count across requested nodes and rounding up.
  `internal/slurm/client.go:228-280`
- An explicit `gres/gpu:0` must return 0 GPUs, not be mistaken for "no GRES
  field present, default to 1" — a job that explicitly asked for zero GPUs
  must not be translated as if it asked for one (audit finding).
  `internal/slurm/client.go:228-231`, `:260-264`
- When GPUs are requested, the pod needs a toleration for the
  `nvidia.com/gpu:NoSchedule` taint plus explicit `PATH`, `LD_LIBRARY_PATH`,
  `NVIDIA_VISIBLE_DEVICES`, and `NVIDIA_DRIVER_CAPABILITIES` environment
  variables for the NVIDIA device plugin / container toolkit integration.
  `internal/translate/translate.go:206-218`

## Networking / address (kind MASQUERADE, defunct NoAddrCache / cloud_reg_addrs in 26.05)

- `CommunicationParameters=NoAddrCache` is **defunct in Slurm 26.05** —
  slurmctld logs an error and ignores it, so the originally planned
  stale-address defense does not exist anymore (observed in testing, 2026-07-11).
  Address staleness for dynamic nodes now relies solely on
  `ReturnToService=2` plus an `/etc/hosts` re-injection step in the e2e
  runner script; if later scale suites flake on address caching, this needs
  revisiting against whatever 26.05 replacement mechanism exists.
  `test/e2e/slurm/slurm.conf:94-99`
- `cloud_reg_addrs` is likewise defunct in 26.05 (same fate as
  `NoAddrCache`) — an attempted fix for the masqueraded registration address
  was silently ignored (observed in testing). The real fix for the `kind`
  networking case lives in the runner script instead: a `nat POSTROUTING
  ACCEPT` rule inside the kind node exempts pod→docker-network traffic from
  kind's `MASQUERADE`, so slurmd registers from its real pod IP and
  slurmctld's return path uses the pod-CIDR route directly.
  `test/e2e/slurm/slurm.conf:67-72`
- `SlurmctldHost` is set by name, never a hard-coded address, because every
  party (slurmctld container, slurmrestd container, bridge-created slurmd
  pods inside kind) resolves it through a different mechanism — Docker's
  embedded DNS on one side, a selectorless Service + manually created
  EndpointSlice on the other. One static config file only works on both
  sides of the kind boundary because of this indirection.
  `test/e2e/slurm/slurm.conf:26-38`

## Node identity & job-ID reuse (deterministic pod hostnames, A3 submit-time anchor)

- Slurm **reuses job IDs** once slurmctld's counter wraps at `MaxJobId`. The
  JobSet name is derived from the job ID alone
  (`slurm-job-<id>`), so a brand-new job can collide with a stale, still-live
  JobSet from a previous incarnation of that ID and silently inherit
  capacity shaped for a different job (wrong pod count, CPUs, memory, GPUs).
  `internal/translate/translate.go:40-50`
- The fix (labeled "A3" throughout) is a submit-time identity anchor: the
  Slurm job's `submit_time` (unix seconds) is stored as an annotation
  (`SlurmSubmitTimeAnnotation`) on the JobSet, not a label — the value is
  never selected on, and a raw unix timestamp is a poor label value anyway.
  A job cannot be resubmitted at the same ID with the same submit second
  without the previous incarnation having left the system first, so
  ID+submit-time is unique in practice.
  `internal/translate/translate.go:40-50`,
  `internal/slurm/client.go:106-115`
- The identity check is wired at **two** points so a reused ID is caught
  regardless of cache timing: (1) create-time, on `AlreadyExists` from
  `kube.Create`; (2) the per-tick snapshot path, before a cached stale
  JobSet is treated as this job's capacity.
  `internal/bridge/reconciler.go:639-674`, `:524-534`
- On a mismatch, the bridge deliberately does **not** adopt (wrong-shaped
  capacity) and does **not** delete (the stale JobSet may still be running a
  previous job's live pods) the conflicting JobSet — it logs, emits a
  Warning Event, increments a conflict metric, and skips the job for that
  tick, leaving resolution to the operator.
  `internal/bridge/reconciler.go:653-661`, `:701-719`
- An existing JobSet **without** the submit-time annotation is treated as
  matching (not a conflict): it predates the A3 migration, or came from a
  job whose slurmrestd exposed no `submit_time`. Failing every such JobSet
  on upgrade would turn a routine rollout into a mass identity-conflict
  event; the wrap window is years long while the upgrade window is one
  reconcile-loop restart, so tolerating the ambiguity is the safer trade.
  `internal/bridge/reconciler.go:663-668`,
  `:721-735`
- Node names the bridge deletes on cleanup are recomputed deterministically
  (`<jobset>-workers-0-<i>`) from the same job fields `ToJobSet` used, rather
  than read off the JobSet — necessary because by cleanup time the JobSet
  (and its `Spec.Parallelism`) may already be gone.
  `internal/translate/translate.go:107-114`,
  `internal/bridge/reconciler.go:1002-1005`

## Scale & performance (O(n^2) API traffic, comment-rewrite throttle, parallel creates, watch-nudge damping)

- Before a once-per-tick snapshot of owned JobSets/Workloads existed, the
  loop issued a LIST per job and a blind CREATE per already-processed job —
  O(n²) API traffic that a 5000-job backlog test would have turned into a
  self-inflicted DoS (a performance fix found in testing).
  `internal/bridge/reconciler.go:402-406`
- `GET /jobs` on the v0.0.44 slurmrestd data parser has **no server-side
  paging** (no page/limit/cursor parameter, no "next page" link) — at 3000+
  jobs, buffering the whole response body plus a fully materialized `[]Job`
  slice peaked at ~23MB of heap. The fix streams the `jobs` array
  element-by-element via `json.Decoder` instead of `io.ReadAll` +
  `json.Unmarshal` (backlog "P4").
  `internal/slurm/client.go:547-563`,
  `:565-679`
- The total Slurm queue depth (not just the held backlog) is tracked as a
  metric specifically because it is the size of the payload the bridge (and
  slurmrestd) must produce and parse every tick — the quantity that
  exhausted both components' memory at 20k queued jobs (suite E, 2026-07).
  `internal/bridge/reconciler.go:397-401`
- Trying to pin (set-features + release) thousands of not-yet-admitted jobs
  every tick meant 2 REST mutations per job per tick and ~3-minute ticks at
  a 3000-job backlog. Fix: an admission gate (`isAdmitted`) that skips
  per-tick REST attempts entirely until Kueue has actually admitted the
  Workload (a scale finding observed in testing).
  `internal/bridge/kueue.go:120-124`
- `SetJobFeatures` and `ReleaseJob` were briefly merged into one POST to
  halve per-admission REST mutations (P2, a performance fix found in
  testing) — **this
  regressed correctness** and was reverted: live testing showed slurmrestd
  can apply `"hold": false` from a merged body without having applied
  `"constraints"` from the same body, releasing the job onto arbitrary nodes
  ("runaway job") before it was pinned to its dynamic nodes. `SetJobFeatures`
  must complete before `ReleaseJob` as two separate calls; do not re-merge.
  `internal/slurm/client.go:766-781`
- A 2500-comment burst (one `SetJobComment` per unadmitted backlog job, every
  tick, because the quota-reason message's numbers moved) ballooned a single
  tick to 4m02s (observed in testing). Fix ("P3"): a comment-rewrite throttle
  that (a) strips volatile digits before comparing so only the *stable* part
  of the message is compared, and (b) throttles same-class rewrites to at
  most once per 60s — but a status-class *transition* (e.g.
  waiting-for-quota → admitted) always rewrites immediately regardless of
  the throttle window.
  `internal/bridge/kueue.go:182-270`, `:272-309`
- Sequential `ensureJobSet` calls dominated the first tick of a burst (one
  Create per held job, one at a time). Fix ("P5"): JobSet creation is
  parallelized across a bounded worker pool (`CreateWorkers`, default 8)
  before the rest of per-job processing (status comment, admission gate,
  release) continues sequentially in the original oldest-first order.
  `internal/bridge/reconciler.go:454-462`,
  `internal/config/config.go:188-194`
- slurmrestd — not the Kubernetes API server, which already had an explicit
  client-go QPS/Burst ceiling — turned out to be the fragile dependency
  under scale testing (suite E, 2026-07): it shares slurmctld's lock and has
  no server-side throttling of its own, so a bridge burst (parallel creates,
  watch-nudge storms) can starve the very scheduler the bridge exists to
  feed. Fix: an optional client-side rate limiter (`slurmRequestsPerSecond`)
  applied at the single choke point (`newRequest`) both request paths must
  pass through.
  `internal/slurm/client.go:287-297`, `:411-426`
- The Kueue `Workload` watch is deliberately **unfiltered** by owner: Kueue
  stamps no class label on `Workload` objects the bridge could select on
  server- or client-side, so any Workload event anywhere in the namespace
  nudges an extra tick. This is bounded by a damping floor
  (`minNudgeInterval`, 1s) between watch-triggered ticks so sustained
  Workload churn cannot turn into continuous slurmrestd polling — deferred,
  never dropped, since the pending nudge already sits in a size-1 coalescing
  channel.
  `internal/bridge/kueue.go:38-48`,
  `internal/bridge/reconciler.go:80-91`, `:225-248`

## Security context (privileged slurmd, minimal-capability path)

- Default posture is `Privileged: true` for the slurmd container — the
  upstream slurmd/slinky constraint (writable cgroups) that k8s-bridge
  cannot lift on its own; reducing it is tracked as an upstream dependency in
  the threat model, not something fixable purely in this controller.
  `internal/translate/translate.go:337-353`
- An explicit minimal-capability alternative exists
  (`slurmd.privileged=false`) for operators with an unprivileged-capable
  slurmd image — still incompatible with GKE Autopilot, but closer to the
  restricted Pod Security Standard. See the "Auth & credentials" section
  above for the exact capability set (`SETUID`, `SETGID`, `CHOWN`, plus
  `SYS_ADMIN`/`SYS_NICE`/`NET_ADMIN`) bisected as necessary.
  `internal/translate/translate.go:344-379`
- The e2e test control plane proves a **fully cgroup-free, fully
  unprivileged** path is possible on both sides (slurmctld/slurmrestd
  outside kind, slurmd pods inside kind) by combining
  `ProctrackType=proctrack/pgid` with `TaskPlugin=task/none` so the cgroup
  plugins are never loaded — this is what lets the whole e2e lifecycle run
  on a free CI runner.
  `test/e2e/slurm/slurm.conf:2-11`, `:109-111`

## Lifecycle & cleanup (activeDeadlineSeconds leak guard, orphan cancellation guards, release-after-registration)

- Every JobSet's Job gets `ActiveDeadlineSeconds` capped at the Slurm
  wall-clock time limit plus a buffer of `max(10%, 5 min)` — a resource-leak
  guard (MVD requirement) so slurmd pods that outlive their Slurm job are
  force-completed by Kubernetes rather than leaking forever.
  `internal/translate/translate.go:245-255`
- A JobSet reaching a `Failed` condition (e.g. `activeDeadlineSeconds` fired
  before the job's dynamic nodes ever registered — a live defect found in
  testing) previously fell into the "leave it alone" branch forever, since
  the Slurm job itself never transitions out of pending on its own ("D1").
  Fix: detect `Failed` explicitly and actively fail/cancel the Slurm job
  (write an explanatory comment, call `CancelJob`) before falling through to
  the shared cleanup path.
  `internal/bridge/reconciler.go:738-783`,
  `:818-838`
- If `CancelJob` itself fails (e.g. slurmrestd unreachable), the JobSet must
  **not** be deleted anyway: the JobSet is the only record carrying the
  Slurm job ID (`SlurmJobIDLabel`), and deleting it would strand the Slurm
  job pending forever with nothing left to retry the cancel against. The
  JobSet is kept and the whole failure path (comment + cancel) is retried
  next tick. `internal/bridge/reconciler.go:751-762`,
  `:826-836`
- Orphan cancellation (a released Slurm job whose JobSet has vanished from
  Kubernetes — "D2") is guarded by **four** independent layers before an
  irreversible `scancel`-equivalent runs, because the trigger (absence of a
  Kubernetes object) is indistinguishable from a bridge-side read fault:
  1. off by default (`cfg.CancelOrphanedJobs`), opt-in only;
  2. an **empty** owned-JobSet list is treated as "we cannot see our
     JobSets" (mis-scoped informer cache, lost RBAC), never as "every job is
     orphaned";
  3. `cfg.OrphanGraceTicks` consecutive observations required, since the
     informer cache may simply not have caught up with a just-created
     JobSet yet;
  4. ("A9") a candidate fraction at or above 50% of bridge-managed jobs seen
     this tick refuses the *whole pass* for that tick — guard 2 alone cannot
     catch a *partially* visible cache (one surviving JobSet in an otherwise
     nearly-empty read); genuine orphans accrue one at a time, so half the
     fleet orphaned at once is treated as a read fault until a human says
     otherwise. This guard is stateless per tick and does not consume grace
     periods, so it self-heals the moment the cache recovers.
  `internal/bridge/reconciler.go:888-1021`
- Node-record cleanup after an orphan cancellation recomputes the pod count
  from the same job fields `ToJobSet` used (`translate.PodCount`), since the
  JobSet — and its `Spec.Parallelism` — is already gone by then; without
  this, every node record past the first would be left behind as a ghost
  `down` node. `internal/bridge/reconciler.go:1002-1006`
- A managed-labeled JobSet with zero `ReplicatedJobs` (e.g. hand-edited, or
  mutated by something else) must not panic the reconcile loop by indexing
  `ReplicatedJobs[0]` blindly — falls back to a default `nTasks=1` and
  continues cleanup on a best-effort basis (audit "D5(a)").
  `internal/bridge/reconciler.go:852-862`
- Release happens immediately after JobSet creation by design, not after
  some separate "wait" step: the Slurm job cannot start anyway until its
  Feature-matched nodes register, so there is no separate readiness state to
  wait through beyond what the `SetJobFeatures` retry loop already provides.
  `internal/bridge/reconciler.go:1-9`

## Priority & status propagation (secondary but notable findings)

- Priority sync between Slurm and Kueue uses a three-way "last synced value"
  annotation stored on the JobSet, so either side changing its priority is
  detectable and mirrored to the other, without the two sides fighting in a
  loop. `internal/bridge/kueue.go:311-359`
- User-originated priority values (from either the Slurm
  `admin_comment`/`scontrol` directive or a direct Workload patch) are
  clamped via `MaxUserPriority` so a job owner cannot jump the whole mixed
  queue ahead of other tenants by requesting an arbitrary priority ("H1").
  `internal/bridge/kueue.go:130-144`, `:339-343`
- The raw priority-field mirror (as opposed to the `admin_comment` lua
  directive channel) stays flag-gated and off by default after it re-held a
  running job live — the lua/`admin_comment` channel (ADR-0009) is
  considered the safe mutation path.
  `internal/bridge/reconciler.go:839-843`
- Kueue condition messages fed into the Slurm comment field can contain
  multi-byte UTF-8 and control characters; truncation must operate on runes
  (not bytes) to avoid corrupting the tail of the string, and control
  characters are normalized to spaces before writing.
  `internal/bridge/kueue.go:235-245`,
  `internal/slurm/client.go:825-837`
