# Upgrade guide

Audience: platform operators upgrading an existing k8s-bridge / ray-bridge
install. Both charts are pre-1.0 (chart `version: 0.2.0`, `appVersion:
"0.1.0"` in `deploy/chart/k8s-bridge/Chart.yaml` and
`deploy/chart/ray-bridge/Chart.yaml` at the time of writing) — read the
compatibility-policy section before assuming anything about backward
compatibility.

## 1. CRDs are not upgraded by `helm upgrade` — do this manually first

Helm's documented CRD lifecycle policy (linked from
`deploy/chart/k8s-bridge/README.md`) is: CRDs in a chart's `crds/` directory
are installed on the **first** `helm install` and never touched again by
`helm upgrade` or `helm uninstall`. This chart ships the `WorkloadMixing`
CRD at `deploy/chart/k8s-bridge/crds/workloadmixing-crd.yaml` (a manually
kept-in-sync copy of the canonical `deploy/crd/workloadmixing-crd.yaml`).

If a new chart version changes the CRD's schema (e.g. a new field under
`spec`), `helm upgrade` alone will **not** apply that change — the cluster
keeps running the old CRD schema, and any new field the chart's templates
or the controller now expect will simply not validate or will be silently
dropped by the apiserver's schema pruning.

**Always apply the CRD manually before `helm upgrade` when a release note
mentions a CRD change:**

```bash
kubectl apply --server-side -f deploy/crd/workloadmixing-crd.yaml
# then
helm upgrade k8s-bridge deploy/chart/k8s-bridge -n <release-namespace> -f my-values.yaml
```

`--server-side` (or an equivalent server-side apply) avoids the
"metadata.annotations: Too long" failure that a client-side
`kubectl apply` on a CRD with a large embedded schema can hit. If you are
not using `configSource: cr`, you can skip this CRD step for changes that
only touch chart templates/values, but it is always safe to run.

## 2. Expect a brief downtime on every upgrade — by design

Both `Deployment`s (`deploy/chart/k8s-bridge/templates/deployment.yaml`,
`deploy/chart/ray-bridge/templates/deployment.yaml`) use
`strategy: { type: Recreate }`, not the Kubernetes default `RollingUpdate`.
From the chart's own comment:

> Recreate (L9 fix): with leader election on a single replica, RollingUpdate
> deadlocks — the old pod keeps renewing its Lease while Running, so the new
> pod never wins leadership, never becomes Ready, and the rollout stalls
> (found live). Recreate stops the old pod first; a brief reconcile
> gap is safe (new Slurm jobs stay held until the bridge returns — MVD §6.1).

This was a live-discovered deadlock, not a theoretical concern: leader
election (`leaderElect: true` by default) means a `RollingUpdate`'s "start
the new pod before killing the old one" behavior leaves two pods where the
old one keeps the Lease and the new one can never take over, so the
Deployment's rollout never completes. `Recreate` avoids this by tearing down
the old pod first. The gap this creates is safe: k8s-bridge is stateless
(state lives in Slurm + Kueue + JobSet objects, reconstructed on restart by
idempotent naming), so newly-held Slurm jobs simply wait a little longer,
and running jobs are unaffected. Running `replicas > 1` with
`podDisruptionBudget.enabled: true` does **not** avoid this gap for a
`helm upgrade` — the PDB only protects against *voluntary* disruptions
(node drain), and the Recreate strategy still tears down every replica
before starting new ones on an upgrade.

## 3. Config compatibility policy (current reality)

**Pre-1.0, there are no compatibility promises.** Neither chart has reached
a `1.0.0` version; both `README.md`s state this explicitly ("this chart is
experimental... expect breaking changes between minor versions until a 1.0
is cut"). Concretely:

- Config field names, defaults, and validation rules in
  `internal/config/config.go` / `internal/rayconfig/config.go` may change
  between any two versions without a deprecation period.
- The `WorkloadMixing` CRD's schema may add required fields or change
  validation (`x-kubernetes-validations` CEL rules) between versions.
- There is currently **no `CHANGELOG.md`** in this repository. Breaking
  changes are, today, recorded in ADRs (`docs/adr/`) and the validation
  summary (`docs/VALIDATION.md`) rather than a dedicated release-notes
  document.
- A tag-triggered release pipeline now exists
  (`.github/workflows/release.yaml`, triggered on `v*` tags): it re-runs a
  lean build+test gate on the tagged commit, then builds/pushes both
  controller images and both Helm charts to `ghcr.io`, and cuts a GitHub
  Release. Two things this pipeline does **not** do, per its own header
  comment: it does not bump `Chart.yaml`'s `version`/`appVersion` for you
  (a release PR must do that and get merged before the tag is pushed), and
  it does not verify image tag / chart `appVersion` agreement — that
  agreement is a *process* guarantee (bump-then-tag), not an automated one.
  Until GitHub Releases from this pipeline accumulate real history, ADRs and
  `docs/VALIDATION.md` remain the practical place to check for breaking
  changes around any given tag.

**Until release notes are a mature, populated artifact, treat every upgrade
as potentially breaking:** diff your values file against the new chart's
`values.yaml`/`values.schema.json`, and diff your `WorkloadMixing` CR (if
using `configSource: cr`) against the new `deploy/crd/workloadmixing-crd.yaml`
before upgrading a production install.

## 3a. Upgrading to chart 0.3.0: supervisor mode in `configSource: cr` (ADR-0015)

Chart 0.3.0 makes `workloadmixing.name` optional. Nothing changes for
existing installs: with `workloadmixing.name` set, the rendered flags and
controller behavior are identical to before (the single-CR binding via
`--workloadmixing`, kept as the compatibility path — plan for at least one
release of overlap before any deprecation). Leaving `workloadmixing.name`
empty selects the new supervisor mode: one reconcile loop per WorkloadMixing
CR in the release namespace.

Before SWITCHING an existing single-CR install to supervisor mode, note that
JobSets created by the old mode lack the per-CR ownership label
(`k8s-bridge.x-k8s.io/workloadmixing`) and are invisible to supervisor-mode
loops — drain running jobs first or clean up leftover `slurm-job-*` JobSets
manually (details: `docs/installation.md` §4.2).

## 4. Recommended upgrade ordering

1. Read the diff between your current chart version and the target
   version's `values.yaml` / `values.schema.json` / CRD.
2. Apply the CRD manually (§1), if changed.
3. `helm upgrade` the chart with your (possibly updated) values file.
4. Watch the rollout: `kubectl rollout status deployment/<release> -n <namespace>`
   — expect the brief Recreate gap from §2.
5. Verify readiness: `curl localhost:8080/readyz` (k8s-bridge) or
   `curl localhost:8081/readyz` (ray-bridge) after port-forwarding the
   metrics/health Service, per `docs/installation.md` §6. A non-Ready pod
   after roughly a minute usually means slurmrestd is unreachable or the
   Slurm token is stale (`docs/operations.md`).
6. If `configSource: cr`, confirm the `WorkloadMixing` CR's
   `status.conditions[Ready]` flips back to `True`.

## 5. Rollback notes

- `helm rollback <release> <revision> -n <namespace>` reverts the
  `Deployment`/`ConfigMap`/etc. to a prior chart render, but — per Helm's
  CRD policy (§1) — does **not** revert a CRD schema change. If you applied
  a new CRD manually as part of the upgrade, a `helm rollback` alone leaves
  the newer CRD schema in place; you may need to reapply the older CRD
  manually too if the rollback target genuinely depends on the old schema.
- Both controllers are stateless: a rollback (like an upgrade) is safe from
  a data-loss perspective — everything reconstructs from Slurm + Kueue +
  JobSet object state on the next reconcile tick, and idempotent JobSet
  naming means there is nothing to "undo" beyond the Deployment's own spec.
- The Recreate strategy applies to rollbacks too — expect the same brief
  gap described in §2.

_Related reading: `docs/installation.md` (fresh install), `docs/compatibility-matrix.md`
(validated versions), `docs/operations.md` (runbooks referenced above)._
