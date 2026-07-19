# Compatibility matrix

**These are *validated-with* versions, not tested ranges.** Every row below
was actually exercised in this repository's experiments, CI, or chart
defaults — nothing here is a compatibility promise across a version range.
Range-testing (e.g. "does this work on Kueue v0.19?") is future work, not
yet attempted. Where a component's version is unpinned in this repo (picks
up "latest" at install time), that is called out explicitly rather than
guessed at.

| Component | Version validated against | Notes / source |
|---|---|---|
| Kubernetes (envtest, integration tests) | 1.33 | `Makefile` (`ENVTEST_K8S_VERSION`) |
| Kubernetes (kind, nightly reduced-scope e2e) | kind `v0.32.0` (kind's own default node image for that release; not separately pinned in the workflow) | `.github/workflows/nightly.yaml` |
| Kubernetes (GKE, live experiments) | GKE Standard, `--release-channel regular` (no specific minor version pinned) | `experiments/01-gke-playground/scripts/01-create-cluster.sh` |
| Go | 1.26.5 | `go.mod` (`go 1.26.5`); CI resolves the toolchain from this file via `go-version-file` |
| controller-runtime | v0.24.1 | `go.mod` |
| `setup-envtest` tool | v0.24.1 | `Makefile` (`ENVTEST_TOOL_VERSION`, pinned to match controller-runtime's minor) |
| Kueue | v0.18.2 | `deploy/monitoring/README.md`, `experiments/01-gke-playground/scripts/00-env.sh`, `experiments/10-ray-bridge/README.md` — installed via the upstream `manifests.yaml` release artifact |
| JobSet | v0.12.0 | `go.mod` (`sigs.k8s.io/jobset`), `experiments/01-gke-playground/scripts/00-env.sh`, `experiments/10-ray-bridge/README.md` |
| KubeRay operator | 1.6.2 | `experiments/01-gke-playground/scripts/00-env.sh`, `experiments/10-ray-bridge/README.md` (Helm chart `kuberay/kuberay-operator --version 1.6.2`) |
| Ray (worker/head image) | `rayproject/ray:2.9.0` | `deploy/chart/ray-bridge/values.yaml`, `experiments/10-ray-bridge/README.md` |
| Slurm / slurmd (Slinky) | 26.05 (`ghcr.io/slinkyproject/slurmd:26.05-ubuntu26.04`) | `deploy/chart/k8s-bridge/values.yaml`, `docs/VALIDATION.md` |
| slurmrestd API | v0.0.44 | `internal/config/config.go` and `internal/slurm` package comments (the client is written against this API version's quirks — no server-side paging on `/jobs`, 422 semantics on comment updates) |
| slurm-operator (Slinky) chart | **unpinned** — `SLURM_OPERATOR_CHART_VERSION` is empty, resolving to whatever the OCI registry serves as latest at install time | `experiments/01-gke-playground/scripts/00-env.sh` (comment: "slurm-operator publishes no GitHub releases; charts live in the OCI registry... Empty version = latest chart") |
| cert-manager | **unpinned** — installed via `oci://quay.io/jetstack/charts/cert-manager` with no `--version` flag | `experiments/01-gke-playground/scripts/02-install-components.sh` |
| k8s-bridge / ray-bridge Helm charts | chart `version` 0.2.0, `appVersion` 0.1.0 (both charts) | `deploy/chart/k8s-bridge/Chart.yaml`, `deploy/chart/ray-bridge/Chart.yaml` — pre-1.0, expect breaking changes between minor versions (see `docs/upgrade-guide.md`) |
| Kubernetes floor declared by both charts | `kubeVersion: ">= 1.29.0-0"` | `deploy/chart/k8s-bridge/Chart.yaml`, `deploy/chart/ray-bridge/Chart.yaml` — the chart's own comment says this floor is picked conservatively for Kueue TAS/JobSet and is **not independently verified** against a live cluster below 1.29 |

## Explicitly unverified / broader-range-unverified

- **Kubernetes minor-version range.** Only 1.33 (envtest) and whatever kind
  `v0.32.0` bundles by default have been exercised. No claim is made about
  older or newer Kubernetes minors.
- **GKE specific version.** The GKE experiments use the `regular` release
  channel and never pin a control-plane/node minor version explicitly, so
  the exact GKE Kubernetes version validated against varies run to run and
  is not recorded anywhere in the repo.
- **slurm-operator (Slinky) chart version.** Because the install script
  passes no `--version`, every live run picks up whatever the OCI registry
  currently serves as latest — there is no single pinned version this
  project has validated against repeatably.
- **cert-manager version.** Same situation — installed unpinned.
- **slurmrestd versions other than v0.0.44.** The client's request/response
  handling (streaming decode, warnings-as-errors, no server-side paging) is
  written against v0.0.44's behavior specifically; compatibility with other
  slurmrestd versions is unverified and at least one known drift exists
  (comment-update `422`, see `docs/VALIDATION.md`).
- **Internal Slurm-version comment inconsistency.** `internal/slurm/client.go`'s
  package doc currently describes the v0.0.44 data parser as "Slurm 25.11,
  as deployed by the Slinky charts", while the chart's own slurmd image
  (`deploy/chart/k8s-bridge/values.yaml`) and the live-deploy findings
  (`docs/VALIDATION.md`) both say Slurm **26.05**.
  This repo has not reconciled the two; treat 26.05 (the actually-deployed
  image tag) as authoritative until that comment is corrected.

_Related reading: `docs/installation.md` (how these components are installed
together), `docs/upgrade-guide.md` (upgrade ordering and CRD caveats)._
