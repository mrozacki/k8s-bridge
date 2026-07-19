# Production-readiness plan

Date: 2026-07-11. Scope: everything except security (a separate workstream
covers auth, RBAC tightening, TLS, and the 2026-07-05 audit follow-ups).

This document synthesizes a full-repo review (code, charts, CI, docs) plus a
quality review of all changes landed 2026-07-04 → 2026-07-11. It records
(1) the assessment, (2) what was implemented in the accompanying change set,
and (3) the remaining roadmap for the engineering team.

## 1. Assessment summary

**Overall:** the prototype is unusually disciplined for its stage — layered
tests (unit / envtest / kind e2e / fuzz / benchmark regression gates), a
coverage ratchet, live-validated runbooks, and rationale-dense godoc. The
production gap is concentrated in four places, none of which is "the code is
bad":

1. **Distribution is nonexistent.** No release pipeline, no published images
   (charts point at placeholder `ghcr.io/owner/*`), no chart publishing, no
   tags, no changelog. A customer cannot install the product without building
   from source.
2. **ray-bridge is one design generation behind k8s-bridge**: no domain
   metrics, vacuous probes, no failed-worker-JobSet handling, no CR/status
   surface, and the pin-gate's only enforcement point (the mutating webhook)
   is off by default.
3. **Operational packaging is incomplete**: bridge metrics were unscrapable
   (no Service/ServiceMonitor), alerts existed only as prose, charts lacked
   standard scheduling knobs, CRD lived in three hand-synced copies (one had
   already drifted — the envtest suite was validating a stale schema).
4. **One functional question is still open**: hold detection (`IsHeld()`)
   failed on a live cluster during TC-C1; regression tests exist but are
   skipped pending a payload capture (TC-B7). **This should gate any customer
   deployment** — the entire admission model depends on it.

The week's changes themselves trended upward in quality: the hardening
commits (orphan-cancellation guards, constraints-before-release invariant,
auto-merge gate rework) are strong; the debt came from fast-moving early
commits that landed broken and were repaired the next day, plus repo hygiene
(test-driver scripts in the root, debug files committed then removed).

## 2. Implemented in the accompanying change set

All items below are code/manifests/docs in the working tree.

**Verification status (2026-07-11, local):** `go mod tidy` (one-line diff),
full unit suite (`-race`), full envtest integration suite, and
`golangci-lint` all pass; both charts pass `helm lint` and render in default,
all-options, gke-test-overlay and webhook-enabled configurations; the three
CRD copies are body-identical; all workflow YAML parses. Three defects were
found and fixed during verification: (1) the new `ensureJobSet` identity
check treated the AlreadyExists→NotFound race as an error — it now preserves
the historic tolerate-AlreadyExists behavior; (2) `chart-ci.yaml`'s
gke-overlay render step needed throwaway `--set image.repository/tag` values
because the overlay deliberately blanks them and the schema now requires
them non-empty; (3) five golangci-lint findings (two justified test
`nolint:gosec` annotations, a used-now logger in the metrics shutdown path, a
documented `nolint:unparam` on the ray reconcile signature, and a dead
eventType parameter removed from ray-bridge's `eventf`). Still pending:
`actionlint` over the workflows (not installed locally; workflows are
YAML-valid), the two new action SHA pins, and the first live firing of the
new workflows on a PR.

### Go — k8s-bridge (Slurm side)

| Item | What changed |
|---|---|
| Rate limiting toward slurmrestd | Token-bucket limiter in the Slurm client; new config/CRD field `slurmRequestsPerSecond` (0 = unlimited, ≤ 10000). slurmrestd was the fragile dependency in scale testing; the K8s client already had a QPS ceiling, Slurm did not. |
| Cleanup error isolation | `cleanupFinishedJobs` no longer aborts the whole pass on the first `GetJob` error; errors are joined and cleanup continues per-job. |
| Slurm job-ID reuse guard | JobSets are stamped with a submit-time annotation; on `AlreadyExists` the bridge verifies identity instead of silently adopting a stale JobSet (metric `k8s_bridge_jobset_identity_conflicts_total`, Warning event, no auto-delete). |
| Orphan-cancellation guard 4 | If >50% of bridge-managed jobs look orphaned in one tick, cancellation is refused (suspected partial informer cache) — closes the gap where one visible JobSet defeated the empty-list guard. |
| Watch-tick damping | Minimum 1s interval between watch-triggered ticks, so Workload churn cannot turn the bridge into a continuous `GET /jobs` poller. |
| Config validation | Rejects negative `orphanGraceTicks`, duplicate/empty partition mappings, and the `maxUserPriority` int32/CRD mismatch (CRD maximum lowered to 2147483647 in all copies). |
| Metrics server hardening | Bind failure now terminates the process (previously the controller ran on with dead probe endpoints); graceful shutdown added. |
| Observability | New `k8s_bridge_slurm_request_duration_seconds` histogram; orphan node-delete failures now log at Warn; the missing `TestIsBridgeOwnedJobSetMatchesTranslateConstants` drift guard was written. |
| QPS/burst flag fix | Float comparison no longer truncates (`--kube-api-qps=20.5 --kube-api-burst=20` is now rejected). |

### Go — ray-bridge

| Item | What changed |
|---|---|
| Failed worker JobSet handling | D1 analog: Warning event + metric + bounded retry (delete/recreate, max 3, tracked in a RayJob annotation); after exhaustion the failed JobSet is left for inspection. Previously a failed worker set left the RayJob waiting forever, silently. |
| Real readiness | `/readyz` now reflects informer sync (and webhook-server start when enabled) instead of `healthz.Ping`. |
| Domain metrics | `ray_bridge_worker_jobsets_created_total`, `..._failed_total`, `ray_bridge_reconcile_errors_total`, `ray_bridge_webhook_decisions_total{decision}`. |
| Config validation | Negative `DefaultWorkers`/`DefaultCPUs`/`DefaultMemoryMB` rejected. |
| Bypass warning | Startup Warn + flag/godoc caveat: with the webhook disabled (default), inner RayJobs without self-declared entrypointResources bypass Kueue admission. |

### Helm / deploy

- k8s-bridge chart: `replicas`, nodeSelector/tolerations/affinity/
  topologySpreadConstraints/podAnnotations/podLabels, PodDisruptionBudget,
  metrics Service + optional ServiceMonitor, optional PrometheusRule carrying
  the 7 operations.md alerts, NOTES.txt, schema/README corrections (memory
  default, `slurmUser`, missing rows). Chart 0.2.0.
- ray-bridge chart: brought to parity — declared `replicas`, scheduling
  knobs, full `values.schema.json`, README, NOTES.txt, and complete webhook
  wiring (Service + MutatingWebhookConfiguration + cert-manager Certificate/
  Issuer, namespace-parameterized). Chart 0.2.0.
- Plain manifests under `deploy/ray-bridge/` marked deprecated in favor of
  the chart.
- CRD copies re-synced (the `test/crd/` copy had drifted — missing
  `slurmRequestTimeout`).

### CI / release engineering

- `chart-ci.yaml`: helm lint + template render on chart PRs (previously
  chart-only PRs ran nothing but DCO) **plus a CRD three-copy `.spec` sync
  gate** so the drift found in this review cannot recur.
- `image-ci.yaml`: both Dockerfiles built on PRs (previously never built in
  CI at all).
- `release.yaml`: tag-triggered (`v*`) pipeline — test gate, ghcr.io image
  push, OCI chart push, GitHub Release with generated notes. Registry
  location is a placeholder decision.
- Dependabot auto-merge: DCO gate no longer fails open (missing/in-progress
  check-runs now block); nightly kind download gets `-f --retry` + checksum.
- Dependabot now also watches the Dockerfiles.

### Docs & hygiene

- New: `docs/installation.md` (full-stack), `docs/upgrade-guide.md`
  (including the manual CRD-apply step Helm never performs),
  `docs/compatibility-matrix.md` (validated-with versions, gaps marked),
  `docs/ray-bridge-reference.md` (config, annotation contract, webhook
  semantics).
- Fixed: wrong config key `slurmRestBaseURL` → `slurmRestURL` in
  operations.md (an operator following the runbook would have set a key that
  does nothing); metrics reference completed; stale dashboard comment.
- 15 `run-suite-*.sh` scripts moved from the repo root to `test/suites/`
  with a README; scale-test teardown script's `kubectl proxy` block made
  crash-safe; stale S3 banner corrected.

## 3. Remaining roadmap (excluding security)

### Phase 0 — gates before any customer conversation

1. **Resolve hold detection (TC-B7).** Capture the live payload, fill the two
   skipped tests in `internal/slurm/client_test.go`, remove the skips. Until
   then the core admission invariant is unproven on real clusters.
2. **Run the verification checklist (§4)** on this change set and merge it.
3. **First tagged release** (v0.2.0): decide the registry/org, replace the
   placeholder image repos, tag, and verify the release pipeline end-to-end.

### Phase 1 — architecture decisions (engineering team, ADRs required)

4. **Typed API migration**: move WorkloadMixing from unstructured JSON
   round-trip to a kubebuilder-generated typed `api/v1alpha1` package with
   controller-gen'd CRDs (kills the three-copy sync problem at the root) and
   deepcopy/clients. Prerequisite for conversion webhooks and multi-version.
5. **API group decision**: `k8s-bridge.x-k8s.io` implies a Kubernetes SIG
   subproject. Choose: pursue SIG sponsorship or move to an owned domain.
   Renaming after GA is a breaking migration — decide now.
6. **Multi-CR support**: one controller reconciling all WorkloadMixing
   objects (today: one Deployment per CR via `--workloadmixing` flag).
7. **ray-bridge API surface**: give it a CR (config + status) and decide
   whether the webhook becomes default-on; the annotation contract needs a
   discoverable schema. Also implement the promised Kueue eviction/
   preemption handling for worker JobSets.
8. **RayService admission** (under evaluation per ADR-0002) — decide in/out.

### Phase 2 — reliability engineering

9. Finalize HA story: document leader-failover semantics for in-memory state
   (orphan grace restarts, comment throttle), test failover in kind/e2e.
10. Finalizer strategy ADR: today deleting the bridge/CR leaves Slurm-side
    state (comments, node records) unreconciled — document or add finalizers.
11. E2e as a merge gate: today kind e2e is nightly-only and does not exercise
    hold→release (mock slurmrestd). Build a containerized slurmctld+slurmrestd
    fixture so the core lifecycle gets an automated gate.
12. Identity-check residual gap: `ensureJobSet` verifies identity on
    `AlreadyExists`, but a stale JobSet already in the cache snapshot bypasses
    creation entirely — extend the check to the snapshot path.
13. Rate-limit defaults: `slurmRequestsPerSecond` ships default-off; after
    field experience, pick a safe default and consider a per-tick mutation
    budget.

### Phase 3 — operations & docs

14. Operator/on-call dashboard (bridge-sourced panels — backlog A3) alongside
    the researcher dashboard; validate the shipped PrometheusRule against a
    live Prometheus (`promtool check rules`).
15. Ratify SLOs (operations.md still marks them "proposals").
16. Compatibility range testing: the matrix records validated-with versions
    only; test at least one version back/forward for Kueue and JobSet.
17. Sizing guide consolidation (single table by backlog depth, from the
    suite-E data).
18. CHANGELOG + release-notes discipline; fill CODEOWNERS placeholders
    (publication blocker).
19. Version the docs with the chart (docs referenced from NOTES.txt).

### Deferred / explicitly out of scope here

- Security workstream (2026-07-05 audit follow-ups, webhook default-on
  decision has a security dimension too).

## 4. Verification checklist for this change set

Executed locally on 2026-07-11 with the results recorded in §2 (all green
except the items marked "still pending" there). Kept for re-runs after
review changes:

```bash
# Go (from the repo root)
go mod tidy                 # x/time promoted to direct; expect no-op or minimal diff
make test                   # unit + race; then make test-integration (envtest)
golangci-lint run

# Charts
helm lint deploy/chart/k8s-bridge deploy/chart/ray-bridge
helm template deploy/chart/k8s-bridge   # plus: --set serviceMonitor.enabled=true \
                                        #   --set prometheusRule.enabled=true \
                                        #   --set podDisruptionBudget.enabled=true
helm template deploy/chart/ray-bridge --set webhook.enabled=true

# Workflows
actionlint .github/workflows/*.yaml .github/workflows/*.yml
# Verify the two newly-pinned action SHAs:
#   azure/setup-helm@9bc31f4e... (v5.0.1), docker/login-action@af1e73f9... (v4.4.0)
# and the kind v0.32.0 checksum in nightly.yaml.
```

Known open ends flagged by the implementation review:

- ray-bridge fake-client tests assume seeded JobSet status conditions survive
  `WithObjects` (consistent with existing tests) — the test run will confirm.
- Chart schema root is now `additionalProperties: false` — a deliberate
  tightening; `helm template` with existing values files will confirm nothing
  legitimate is rejected.
- `kubeVersion: ">= 1.29.0-0"` floor in both charts needs ratification.
- Whether Dependabot's docker ecosystem picks up `Dockerfile.ray-bridge`
  (non-canonical name) — verify after the first scheduled run.
- Helm CLI pinned at v3.21.2 in CI while Helm 4 is current — revisit.
- ray-bridge package doc still describes the pre-ADR-0013 suspend/unsuspend
  cycle in one place (reconciler.go top comment) — cosmetic follow-up.
