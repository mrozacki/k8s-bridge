# k8s-bridge — prototype controller (phase 3)

- **Status:** **validated end to end against a live GKE cluster
  (2026-07-04)** — a held Slurm job was discovered, translated, admitted by
  Kueue, executed on dynamic slurmd nodes, and cleaned up (JobSet + Slurm
  node records) with zero manual steps. Live-run fixes are annotated
  "Live finding" in the code; see
  `docs/VALIDATION.md`.
- **2026-07-06 update:** the controller-runtime Manager refactor
  (ADR-0011: leader election, informer cache, watch-nudge) was deployed via
  the Helm chart in-cluster on GKE for the **first time** — previous
  sessions only ran the bare binary. That surfaced and fixed 8 deploy-time
  bugs (RBAC/Lease-namespace, cache scope, logging wiring — see
  `docs/VALIDATION.md`) and validated 520 held
  Slurm jobs → 520 JobSets with 0 tick errors.

## What it does

A controller-runtime `manager.Manager` (leader election, JobSet informer
cache, graceful shutdown) hosts one polling reconcile loop
(`internal/bridge`, `Bridge.Run`) implementing the MVD workflow, plus two
watches that nudge it to run sooner (ADR-0011):

1. **Discover** — list Slurm jobs via slurmrestd (streamed decode, P4); keep
   pending+held jobs in partitions listed in the config, oldest first.
2. **Translate** (`internal/translate`) — one JobSet per job
   (`slurm-job-<id>`): one pod per Slurm task, `cpus-per-task` as CPU
   request, partition mapped to a Kueue WorkloadPriorityClass (optionally to
   its own LocalQueue, A1b), pods run `slurmd -Z` with
   `Feature=nodes-for-<id>`.
3. **Admit** — create JobSets in parallel across a bounded worker pool (P5),
   idempotent by deterministic name; pin each Slurm job to its feature and
   release its hold in one merged REST call (P2) once Kueue has admitted it.
4. **Clean up** — delete owned JobSets whose Slurm job is terminal or gone.
   A JobSet that reaches a `Failed` condition before its Slurm job ever ran
   (e.g. `activeDeadlineSeconds` fired before nodes registered) now fails
   the Slurm job with a reason instead of leaving it pending forever (D1).
5. **Nudge** — JobSet events (label-filtered to bridge-owned) and Kueue
   Workload events (unfiltered) wake the poll loop immediately instead of
   waiting out `pollInterval`; the timer remains the unconditional floor
   since slurmrestd has no event API.

## Deliberate prototype simplifications

| Simplification | Production path |
|---|---|
| ~~YAML file config~~ **CRD config shipped** | `--workloadmixing <ns>/<name>` loads config from the `WorkloadMixing` CR via `LoadConfigFromCR` and reports health in `status.conditions` (Ready). File mode is retained for local/laptop runs (ADR-0004). CR mode now **hot-reloads** on spec change (ADR-0011/A1) — no restart needed. |
| ~~Polling both APIs~~ **hybrid poll + watch-nudge** (ADR-0011) | A controller-runtime Manager watches JobSets and Kueue Workloads and nudges the poll loop to tick immediately on a relevant event; the timer interval remains the floor (slurmrestd has no event API, so polling stays the correctness backbone by design, not tech debt). JobSet LISTs are cache-served via the Manager's informer cache; Kueue Workload LISTs remain live/uncached (an unstructured-cache attempt deadlocked the first live tick — backlog P8-for-Workloads tracks the pre-warmed-typed-informer fix). |
| ~~No dynamic-node GC~~ **implemented** | `cleanupFinishedJobs` (`internal/bridge/reconciler.go`) deletes owned JobSets whose Slurm job is terminal/gone and removes the matching Slurm node records via `slurm.Client.DeleteNode` (`internal/slurm/client.go`). |
| ~~Fixed 1Gi-per-CPU memory heuristic~~ **translated** | `--mem-per-cpu` is honored end to end (`internal/translate/translate.go` + tests): pod `RealMemory`/resource requests are derived from `MemPerCPUMB() * CPUs() * perPodTasks`. |
| ~~Translation failure only logged~~ **JobSet-death handling shipped (D1)**; translation failure itself is still logged/skipped, not retried as a Slurm-side failure | A JobSet that reaches `Failed` before its Slurm job ran is now failed with a reason via `CancelJob` (`failJobForDeadJobSet`, `reconciler.go`) — see `docs/operations.md`. A per-job *translation* error (e.g. unsupported flag) is still logged, counted, and skipped for the operator to fix; failing that Slurm job with a reason via REST remains a documented TODO, distinct from D1. |
| ~~No metrics~~ **Prometheus metrics shipped** (backlog AUD2) | `/metrics`, `/healthz` and `/readyz` all serve on `--metrics-addr` (see flags below), registered on controller-runtime's own metrics registry; `pprof` remains a separate opt-in listener. ~~Leader election and the controller-runtime Manager are still open~~ **shipped (ADR-0011)** — `--leader-elect` (default on), a `coordination.k8s.io` Lease, a namespace-scoped cached client, and Kubernetes Events (`Created`/`Released`/`JobSetFailed`/`TranslationFailed`) on JobSets all ship. Live-validated via the Helm chart on GKE: 520 held jobs → 520 JobSets, 0 tick errors (`docs/VALIDATION.md`). |

Field shapes of the slurmrestd v0.0.44 API are written from the spec and
marked `TODO(live)` where they must be verified against a real server.

## Flags (`cmd/k8s-bridge/main.go`)

| Flag | Default | Purpose |
|---|---|---|
| `--config` | `config/example-config.yaml` | Path to the bridge config file (file mode). |
| `--workloadmixing` | *(empty)* | `<namespace>/<name>` of a `WorkloadMixing` CR to load config from instead of a file (CR mode). Mutually exclusive in effect with `--config`: when set, the CR wins. |
| `--pprof-addr` | *(empty, disabled)* | pprof listen address, e.g. `127.0.0.1:6060`. Off by default: heap profiles contain the in-memory Slurm token, and an unauthenticated listener is a DoS/exfiltration surface — bind to localhost only. |
| `--allowed-slurmd-images` | *(empty, allows any)* | Comma-separated allowlist of permitted slurmd image prefixes. This is a controller-level trust anchor set by the platform admin and deliberately cannot come from the config/CR (whose author is exactly who the allowlist defends against). |
| `--metrics-addr` | `:8080` | Consolidated HTTP surface: serves `/metrics` (Prometheus, on controller-runtime's own registry — ADR-0011), `/healthz` and `/readyz` on one mux (empty disables). The Helm chart's Deployment probes and container port assume this default. |
| `--log-level` | `info` | Log level: `debug`, `info`, `warn`, or `error`. Logs are structured JSON on stdout. |
| `--leader-elect` | `true` | ADR-0011: gates the reconcile loop behind winning a `coordination.k8s.io` Lease (`k8s-bridge-leader`) in the config namespace, so a second replica sits idle instead of double-processing. Disable only for a single-replica local/dev run without Lease RBAC. |

## Config modes

Two ways to supply the bridge's `Config` (`internal/config/config.go`), sharing
one `ApplyDefaults`/`Validate` pair:

1. **File mode** (`--config path/to.yaml`, the default) — a YAML file whose
   schema mirrors the future `WorkloadMixing` CRD spec (ADR-0004). Intended
   for local/laptop runs and the experiment playground.
2. **CR mode** (`--workloadmixing <namespace>/<name>`) — reads a live
   `WorkloadMixing` custom resource (`internal/bridge/crdconfig.go`), the
   in-cluster production path. The bridge reports health back onto the CR's
   `status.conditions` (`Ready` flips to `False` on tick failures, `True` when
   the loop recovers) via `UpdateReadyCondition`.

## Build & test

```bash
make                 # fmt + vet + verify-surface + test + build
make test-integration  # envtest: real kube-apiserver + JobSet CRD (layer 2)
make verify-surface  # thin-surface guard (WAS-readiness check, ADR audit D1)
make run             # requires kubeconfig pointing at the playground cluster
```

See `TESTING.md` for the full test-layer strategy.
