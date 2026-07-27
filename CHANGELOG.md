# Changelog

All notable changes to this project are documented here. The format loosely
follows [Keep a Changelog](https://keepachangelog.com/); until 1.0 there are
NO compatibility promises between versions (see docs/upgrade-guide.md), and
breaking changes are called out per release.

## v0.3.0 — 2026-07-27

Everything below landed on `main` between 2026-07-13 and 2026-07-27, on top of
v0.2.0. **This release exists primarily to get security fixes into a published
artifact**: the v0.2.0 chart shipped the Slurm token over cleartext by default,
and the v0.2.0 image predates two dependency CVE fixes and the ADR-0017
deploy-time gating. Anyone running v0.2.0 images or charts should upgrade.

### Security
- **The chart's default sent the Slurm JWT in cleartext.** `slurmRestURL`
  defaulted to `http://`, and the token is bearer-equivalent, so a default
  install leaked credentials to anything on the pod network path. The default
  is `https://` again; plaintext now requires an explicit `allowInsecureHTTP`
  opt-in, which the CRD enforces with a CEL rule rather than trusting docs.
- **`allowedTokenPaths` shipped wide enough to defeat its own purpose**
  (ADR-0017): the default `/var/run/secrets/` covers the controller's *own*
  projected ServiceAccount token, so a `WorkloadMixing` CR could point the
  controller at that token and have it sent to an attacker-chosen
  `slurmRestURL` — precisely the attack the allowlist exists to stop. Narrowed,
  and `config.DefaultAllowedTokenPaths` is now the single asserted source of
  truth. The unit tests had passed only because they exercised a narrower list
  than the one that shipped; the lesson — *test the configuration that ships* —
  is recorded in ADR-0017.
- **Deploy-time gating for CR-supplied paths and TLS** (ADR-0017): in
  supervisor mode a CR may only name a token path the platform admin allowed,
  and skipping TLS verification requires the operator to pass
  `--allow-insecure-tls`. A tenant-authored CR can no longer widen the
  controller's own trust boundary.
- **demo-console**: closed a DOM XSS (a `?grafana=javascript:...` URI was
  accepted verbatim), a wildcard CORS header on the status API, and a
  path-traversal in the static file server.
- **Dependency CVEs**: `golang.org/x/net` → v0.56.0 (GO-2026-5942),
  `golang.org/x/text` → v0.39.0 (GO-2026-5970).
- **Supply chain**: every released image is now cosign-signed in keyless
  (OIDC) mode **by digest**, with a SLSA provenance attestation and an SPDX
  SBOM attached; base images are digest-pinned; `:latest` moves only for
  non-pre-release tags.

### Added
- **Hands-on tutorial** (`docs/tutorial.md`) and a `WorkloadMixing` design and
  field reference (`docs/custom-resource.md`).
- **Installation-guide e2e** (`test/e2e/run-installation-docs.sh`): asserts the
  *documented* install path works, statically (every `--set` checked against
  `values.yaml` and the JSON schema) and against a live cluster, with negative
  security assertions. It found four real defects on its first run, including
  the `allowedTokenPaths` default above.
- **Runnable-documentation guard** (`scripts/verify-doc-commands.sh`, wired
  into `make verify-docs` and CI): every command block in the runnable docs
  must be repo-root-relative, reference only paths that exist, and invoke only
  executable scripts.

### Fixed
- **Ghost Slurm jobs**: `slurmd` now starts with `-b`, so a recreated pod
  cannot strand a job whose node registration it silently replaced.
- **A JobSet completing out from under its Slurm job** no longer leaves that
  job pending forever — it is failed with the reason propagated.
- **Non-registration job-update failures are no longer masked**: only the
  expected "not yet registered" case is swallowed; everything else surfaces at
  Warn with the Slurm error attached.
- **Simulated GPUs on Slurm 26.05**: a count-only `gres.conf` makes slurmd
  report zero devices and slurmctld drain the freshly registered node
  (`INVALID_REG`, job stuck at `ReqNodeNotAvail` with nothing in the bridge
  logs). `Name=gpu File=/dev/null` in the Slurm cluster's own config is the
  fix; the bridge deliberately does not mount a `gres.conf` of its own.
- **The runnable documentation was, in places, not runnable.** Twelve defects
  across `experiments/DEMO.md` and `docs/tutorial.md`: command blocks whose
  relative paths escaped the repository once an earlier section changed
  directory; a config patch the CRD's CEL rule rejects because it omitted the
  cleartext opt-in; blocks silently interleaving commands for two different
  shells; a profiling step whose enabling flag was never mentioned; a
  simulated-GPU step observing a node that is deregistered ~7 seconds after it
  appears; a failure-handling section proposing two reproductions the
  apiserver refuses because `JobSet.spec.replicatedJobs` is immutable; and a
  mixing section whose two workloads landed in *different* `ClusterQueue`s
  while the surrounding text described them sharing one quota. Found by
  executing the documents, not reading them — see `docs/VALIDATION.md`.
- **Playground**: the node floor is pinnable (`MIN_NODES`), so the
  `optimize-utilization` autoscaler cannot shrink the pool out from under the
  topology sections; the team-b filler Job is split out of `cohort-queues.yaml`
  so it no longer starts during cluster setup and consume quota for a whole
  session.

### Changed
- Chart `appVersion` is `v0.3.0`; the `k8s-bridge` chart is `0.4.0` and
  `ray-bridge` is `0.3.0`. Chart `version` and `appVersion` intentionally
  differ: chart `0.3.0` was already published against `appVersion v0.2.0`, and
  a released chart version is never republished with different content.
- README trimmed to a scope table and external-reader framing; internal
  planning documents removed from the published tree.
- Dependency bumps carried into this release: `k8s.io/{api,apimachinery,
  client-go}` 0.36.2 → 0.36.3, `github.com/go-logr/logr` 1.4.3 → 1.4.4,
  `github.com/prometheus/client_golang` 1.23.2 → 1.24.1.

### Upgrading
Read `docs/upgrade-guide.md` first — CRDs are not upgraded by `helm upgrade`.
Two changes in this release can break an existing install **by design**:
- if you relied on the `http://` `slurmRestURL` default, the install now fails
  validation until you set `allowInsecureHTTP: true` deliberately;
- if a `WorkloadMixing` CR names a `slurmTokenFile` outside the narrowed
  `allowedTokenPaths`, the controller refuses it. Widen the allowlist
  explicitly via the controller flag if you genuinely need the old path.

## v0.2.0 — 2026-07-13

First tagged release. Everything below landed between 2026-07-11 and
2026-07-13 (PRs #19–#35) on top of the untagged prototype.

### Added
- **Real-Slurm e2e gate**: the full lifecycle (held job → translate → Kueue
  admission → dynamic-node registration → pin+release → COMPLETED → cleanup)
  runs nightly against a real slurmctld/slurmrestd 26.05 on kind, all stages
  hard (`.github/workflows/e2e-slurm.yaml`, `test/e2e/run-real-slurm.sh`).
- **Typed WorkloadMixing API** (`api/v1alpha1`, ADR-0014): controller-gen'd
  CRDs (the three copies are now build artifacts with CI drift gates),
  typed client in the bridge, `status.conditions` as `metav1.Condition` —
  `observedGeneration` now actually persists.
- Slurm-side hardening: client-side slurmrestd rate limiting
  (`slurmRequestsPerSecond`), job-ID-reuse identity guard, fourth
  orphan-cancellation guard (partial-cache fraction bound), watch-nudge
  damping, per-job cleanup error isolation, request-duration histogram,
  last-successful-tick timestamp gauge.
- ray-bridge parity: failed worker JobSet handling (bounded retry + events
  + metrics), informer-sync readiness, `ray_bridge_*` metric set.
- Charts: scheduling knobs, PodDisruptionBudget, metrics Service +
  ServiceMonitor + PrometheusRule (the 7 operations.md alerts), full
  ray-bridge webhook wiring; operator dashboard
  (`dashboards/operator-dashboard.json`).
- CI/release: chart lint/render + CRD sync gates, image build checks, this
  tag-triggered release pipeline, hardened Dependabot auto-merge.
- Docs: installation, upgrade guide, compatibility matrix, ray-bridge
  reference, HA/leader-failover semantics (validated in envtest).

### Changed
- **`slurm.Client.ListJobs` is now streaming** (`func(Job) error` callback)
  — suite-E finding: materializing 20k jobs OOM-killed the bridge at 2Gi.
- Suite-F GPU fixes: `sbatch --gpus` (`tres_per_job`) translates correctly;
  GPU jobs' pods tolerate `nvidia.com/gpu:NoSchedule` and carry the NVIDIA
  env vars NVML needs on GKE (PATH including the sbin dirs, per the live
  suite-B–F follow-up).
- Unprivileged slurmd (`slurmd.privileged=false`) now works: the capability
  set gained SETUID/SETGID/CHOWN (bisected against a live launch; without
  them the mode could never run a job).
- Chart images default to this repository's GHCR packages; `appVersion`
  0.2.0.

### Known gaps (tracked)
- Multi-CR support: designed (ADR-0015, Proposed), not implemented.
- Slurm auth-key Secret is not synced across namespaces by the bridge
  (suite-F finding 9; documented interim: pre-create it, as
  docs/installation.md does).
- API group `k8s-bridge.x-k8s.io` naming decision deferred.
