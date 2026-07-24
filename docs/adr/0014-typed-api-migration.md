# ADR-0014: Migrate WorkloadMixing to a typed, controller-gen'd API

- **Status:** Accepted — delivered in two PRs (PR 1: typed API package +
  generated manifests; PR 2: typed consumption + `metav1.Condition` status),
  2026-07-12. See "Delivery notes" below for the one deliberate deviation
  from the plan (status.conditions upgraded to `metav1.Condition`).
- **Context source:** production-readiness review 2026-07-11, which ranked
  the unstructured-API design as the highest-leverage production-readiness
  gap and root cause of several recurring costs.
- **Decision owner:** engineering team; this ADR frames the decision and the
  migration plan for ratification.

## Context

The WorkloadMixing CR is today consumed as `unstructured.Unstructured` and
JSON-round-tripped into the file-config struct (`internal/bridge/crdconfig.go`).
The CRD itself is hand-written YAML, maintained as **three manually synced
copies** (`deploy/crd/`, `deploy/chart/k8s-bridge/crds/`,
`test/crd/`). This was the right prototype shape: one
config struct served both the file mode and the CR mode, and no codegen
toolchain was needed.

The costs have now materialized:

1. **Copy drift is real, not theoretical.** The `test/crd` copy silently lost
   `slurmRequestTimeout`, so the envtest suite validated a stale schema until
   the 2026-07-11 review caught it. A CI gate (chart-ci's `crd-sync-gate`) now
   diffs the copies, but the gate treats the symptom — the root cause is that
   a human writes the same schema three times.
2. **Schema/Go drift is real too.** The CRD allowed `maxUserPriority` up to
   4294967294 while the Go field is `int32`; a schema-valid CR failed at
   decode time with a JSON error instead of a validation message.
3. **Evolution is blocked.** Conversion webhooks, multi-version serving
   (`v1alpha1` → `v1beta1`), defaulting webhooks and status subresource
   codegen all assume typed API packages. The JSON-round-trip style makes
   each of these a bespoke project instead of a `kubebuilder` idiom.
4. **Every new field costs five edits** (Go struct, three CRD copies, chart
   values.schema.json) — measured directly while adding
   `slurmRequestsPerSecond`.

## Decision (proposed)

Adopt the standard kubebuilder/controller-gen layout for the WorkloadMixing
API, WITHOUT adopting the full kubebuilder scaffold for the whole project
(the Manager wiring from ADR-0011 already covers the runtime side):

1. New package `api/v1alpha1` with a typed
   `WorkloadMixing` struct, kubebuilder validation markers mirroring the
   current CRD schema 1:1 (including the CEL https rule), and
   `zz_generated.deepcopy.go` from controller-gen.
2. CRD YAML becomes a **build artifact**: `make manifests` runs controller-gen
   and writes `deploy/crd/workloadmixing-crd.yaml`; the chart copy and the
   test copy are refreshed by the same target (copy step, not hand-sync).
   The chart-ci `crd-sync-gate` stays as the enforcement that nobody edits a
   copy by hand; CI additionally runs `make manifests && git diff --exit-code`
   so a marker/YAML mismatch cannot merge.
3. `internal/bridge/crdconfig.go` loads the typed object via the Manager's
   cached client and converts to `config.Config` with a plain, tested
   `func FromCR(*v1alpha1.WorkloadMixing) (*config.Config, error)` — the
   file mode keeps the existing YAML loader; the two paths converge on
   `config.Config` exactly as today.
4. Status writes move from unstructured patching to the typed
   `Status().Update` path with the same `Ready` condition semantics
   (`observedGeneration`, foreign-condition preservation — behavior pinned by
   the existing crdconfig tests, which must pass unchanged).
5. The API **group stays `k8s-bridge.x-k8s.io` for now**: renaming the group
   is a breaking migration and its own decision (deferred,
   2026-07-11); nothing in this migration makes that rename harder later —
   a typed package makes the eventual conversion webhook EASIER.

## Alternatives considered

- **Full kubebuilder scaffold (PROJECT file, api/, controllers/ layout).**
  Rejected for now: the Manager/controller wiring already exists (ADR-0011)
  and works; re-scaffolding would churn every import path for no functional
  gain. Only the API package + codegen is adopted.
- **Keep unstructured, add more CI gates.** Rejected: gates catch drift after
  the fact; they do not remove the five-edits-per-field cost, and none of the
  beta-blocking items (conversion, defaulting) become possible.
- **CRD-only source of truth (generate Go FROM the YAML).** No mature
  tooling; controller-gen's direction (Go → YAML) is the ecosystem standard.

## Consequences

- New build-time dependency: `sigs.k8s.io/controller-tools` (controller-gen),
  pinned in the Makefile like envtest already is.
- The three CRD copies stop being handwritten; the sync gate flips from
  "humans, don't diverge" to "regenerate, don't edit".
- `config.Config` remains the single internal config type — no behavior
  change intended anywhere in the reconcile loop; the migration is
  API-plumbing only and must be provable by the existing test suite passing
  unchanged (plus new tests for `FromCR`).
- Estimated size: the api package + codegen + Makefile/CI wiring in one PR
  (reviewable), the crdconfig.go switch in a second PR — each independently
  green.

## Validation plan

1. PR 1: typed package + controller-gen + `make manifests` producing
   byte-identical spec to today's canonical CRD (diff gate proves parity).
2. PR 2: crdconfig switch; full unit + envtest suites unchanged; the
   real-Slurm e2e (configSource=file and =cr variants) green.

## Delivery notes (2026-07-12)

- **PR 1** landed as planned: `api/v1alpha1`, controller-gen'd deepcopy and
  CRD manifests, the `manifests-drift` CI gate, and a semantic-parity gate
  (`make manifests-parity` against a frozen pre-migration snapshot) proving
  the generated schema changed nothing.
- **PR 2** landed the typed consumption: `LoadConfigFromCR` reads a
  `*v1alpha1.WorkloadMixing` through the Manager's cached client and converts
  via a plain, tested `FromCR` (no unstructured, no JSON round-trip); status
  writes go through the typed object (merge patch, preserving the
  pre-migration no-optimistic-locking semantics).
- **Deviation from decision item 4, deliberate:** `status.conditions` was
  upgraded from the reproduced hand-written shape to the ecosystem-standard
  `metav1.Condition`. PR 1 had documented that the old structural schema
  silently PRUNED the `observedGeneration` the controller has stamped since
  audit AUD2 — a live status bug (a watcher could not tell which spec
  generation a Ready reading reflected). The new schema persists it; envtest
  now asserts persistence against a real apiserver, plus upgrade safety for
  conditions written under the old shape. Consequences: `lastTransitionTime`
  is a validated date-time, `status` is enum'd, and the schema enforces the
  `metav1.Condition` contract (required type/status/reason/lastTransitionTime)
  on foreign conditions too.
- **Strictness shift, documented:** the pre-migration loader re-decoded the
  CR spec with `DisallowUnknownFields`; that guard could only ever fire under
  a fake client, because a real apiserver's structural schema prunes unknown
  fields before persistence. Pruning is now the explicit contract, pinned by
  an envtest.
- **Parity gate retired** with PR 2, exactly as planned when it was built:
  this PR changes the schema on purpose, so the frozen pre-migration
  reference stopped being a truth to compare against. The lasting gates are
  `manifests-drift` (ci.yaml) and `crd-sync-gate` (chart-ci.yaml).
- The real-Slurm e2e variants (configSource=file and =cr) need a live
  kind+docker environment and are validated by the nightly e2e workflow, not
  by the PR itself.
