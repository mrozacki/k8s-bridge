# ADR-0015: One controller, many WorkloadMixing objects (namespace-scoped first)

- **Status:** Accepted — Phase A ratified 2026-07-13; Phase B
  (cluster-wide watch) remains explicitly deferred until a concrete customer
  need appears. Rationale the ratification confirmed: Phase A satisfies the
  known need (many Slurm clusters, one install) without trading away the H4
  namespaced-RBAC secure default that a cluster-wide watch would force back
  to ClusterRoles.
- **Context source:** production-readiness review 2026-07-11
  (`docs/production-readiness-plan.md` §3 phase 1, item "multi-CR support");
  depends on ADR-0014 (typed API) whose PR 1 is merged and PR 2 in flight.
- **Decision owner:** engineering team; this ADR frames the decision for
  ratification.

## Context

The k8s-bridge process is bound to exactly one WorkloadMixing object via the
`--workloadmixing <namespace>/<name>` flag (`cmd/k8s-bridge/main.go`). A
customer with three Slurm clusters — or one platform team serving several
tenant namespaces — must run three Helm releases, three Deployments, three
leader-election leases. Costs of the current shape:

1. **Operational**: N releases to upgrade, monitor, and alert on; N sets of
   RBAC objects; chart values duplicated per instance.
2. **Product**: creating a WorkloadMixing CR does nothing until an operator
   also deploys a controller pointed at it — the CR is not self-serve, which
   contradicts what a CRD-shaped API promises its users.
3. **Anti-idiomatic**: a controller that reconciles only one named instance
   of its own CRD surprises anyone who has operated Kubernetes controllers.

Constraints worth respecting (they shaped the recommendation):

- **The namespaced-RBAC secure default must survive.** The chart's
  `rbac.namespaced=true` (security audit H4) grants Roles only in the
  release namespace, and the Manager cache is deliberately scoped to it
  (`cacheScopedToNamespace`, live finding B8). Watching CRs cluster-wide
  would force ClusterRoles back in — a security regression shipped as a
  convenience feature.
- **One CR = one Slurm universe.** Each WorkloadMixing carries its own
  slurmrestd URL, token, partitions, and poll cadence. The tick loop,
  backoff state, health state, comment throttle and orphan-grace maps in
  `internal/bridge.Bridge` are all per-Slurm-cluster state — the natural
  unit is one `Bridge` instance per CR, not one shared loop.

## Decision (proposed)

Phase A — **all WorkloadMixing objects in the controller's own namespace**:

1. `--workloadmixing` becomes optional. When unset (the new default in CR
   mode), the controller watches WorkloadMixing objects in `POD_NAMESPACE`
   and runs ONE `bridge.Bridge` (its own poll loop, health state, and Slurm
   client) per CR, started/stopped as CRs come and go. When set, behavior is
   exactly today's single-CR binding (escape hatch and upgrade path).
2. **Lifecycle**: CR added → construct Bridge, start its Run loop under the
   Manager's leader-elected context; CR deleted → cancel the loop, leave
   Slurm-side state per the documented orphan/cleanup semantics (finalizer
   strategy stays a separate roadmap item); CR spec changed → existing
   hot-reload path per Bridge (unchanged).
3. **Identity & conflicts**: two CRs naming the same slurmRestURL AND an
   overlapping partition would double-manage jobs. The reconciler refuses to
   start the second Bridge and reports it via the Ready condition
   (`Ready=False, Reason=ConflictingSpec`) — same fail-loud philosophy as
   the orphan guards. Uniqueness is judged on (slurmRestURL, partitionName)
   pairs.
4. **Observability**: `k8s_bridge_*` metrics gain a `workloadmixing`
   (object name) label — cardinality bounded by CR count in one namespace;
   per-CR readiness folds into `/readyz` as "every started Bridge is ready"
   with per-CR detail in the body (operators alert per CR via the metric
   label, not the probe).
5. **Leader election**: unchanged — one lease for the whole controller; all
   Bridges run on the leader. Per-CR sharding across replicas is explicitly
   out of scope (revisit only if a real deployment saturates one process,
   which suite-E data says is far away).
6. **Chart**: drops the `workloadmixing.name` single-binding in CR mode;
   RBAC unchanged (still namespaced).

Phase B — **cluster-wide watch** (opt-in `rbac.namespaced=false` +
`--all-namespaces`), only if a customer actually needs CRs outside the
release namespace. Ships nothing until then; the Phase A code must simply
not preclude it (the Bridge-per-CR map is already keyed by namespace/name).

## Alternatives considered

- **Keep one-CR-per-Deployment, improve the chart to stamp N instances.**
  Rejected: multiplies leases/metrics endpoints/upgrades, and leaves the
  CRD non-self-serve (context point 2).
- **Cluster-wide from day one.** Rejected: trades the H4 namespaced-RBAC
  default for convenience nobody has asked for; Phase B covers it when
  someone does.
- **One shared tick loop multiplexing all CRs.** Rejected: entangles
  per-Slurm-cluster backoff/health/throttle state that is cleanly isolated
  per Bridge today; independent loops also isolate a wedged slurmrestd to
  its own CR's cadence.

## Consequences

- `internal/bridge` needs no semantic changes — the unit of work stays one
  Bridge; new code is a small supervisor (map[namespacedName]→running
  Bridge + start/stop on watch events) plus the conflict check.
- Metrics dashboards/alerts gain a `workloadmixing` label dimension;
  operations.md and the operator dashboard need a one-line update each.
- The `--workloadmixing` flag stays for one release as the compatibility
  path, documented in the upgrade guide, then can be deprecated.
- Multi-CR e2e coverage: extend the real-Slurm e2e with a second CR against
  the same control plane but a disjoint partition (cheap — same containers,
  one more partition in slurm.conf).

## Implementation notes (Phase A landed 2026-07-13)

Two findings from implementation worth recording for the production team:

1. **Metrics labeling (Decision point 4) is deferred to a follow-up.** The
   `workloadmixing` label re-shapes every package-level `k8s_bridge_*`
   metric into a labeled vector and ripples into the operator dashboards
   and alert rules, so it ships separately. Until then the series are
   process-global AGGREGATES across CRs, and
   `k8s_bridge_last_successful_tick_timestamp` reflects the most recent
   success of ANY Bridge — per-CR health lives on the CRs' Ready
   conditions and in the `/readyz` body (which DID land as specified).
2. **"internal/bridge needs no semantic changes" was wrong in one spot.**
   All supervised CRs share one JobSet namespace (a CR's namespace is its
   JobSet namespace, and every CR lives in POD_NAMESPACE), so
   "managed-by=k8s-bridge" stopped answering "is this JobSet mine?" —
   bridge A's cleanup would look bridge B's JobSets up in bridge A's Slurm
   cluster, find nothing, and delete B's live JobSets as finished. Fix:
   supervisor-started Bridges stamp and select a per-CR ownership label
   (`k8s-bridge.x-k8s.io/workloadmixing`); single-CR/file modes are
   byte-identical to before. Consequence: JobSets created before a
   deployment switches to supervisor mode lack the label and are invisible
   to the new loops (documented in installation.md §4.2). Deterministic
   `slurm-job-<id>` names can still collide ACROSS Slurm clusters; the A3
   submit-time identity check turns that into a loud per-job conflict
   rather than silent adoption.
3. **Restart rule for spec changes** (refines Decision point 2's
   "hot-reload unchanged"): endpoint/client construction-time fields
   (`slurmRestURL`, `slurmUser`, token/CA files, TLS-skip, request
   timeout, rate limit) cannot be hot-reloaded into a live `slurm.Client`,
   so a change to any of them restarts that CR's Bridge; everything else
   reuses the existing `setCfg` hot-reload. Single-CR mode already had
   exactly this boundary (those fields required a controller restart).

## Validation plan

1. Unit: supervisor lifecycle (add/update/delete/conflict), conflict
   detection table.
2. Envtest: two CRs → two Ready conditions; delete one → its Bridge stops,
   the other unaffected; conflicting second CR → Ready=False with reason.
3. e2e: the two-partition scenario above as a new soft stage before it
   hardens.
