# Changelog

All notable changes to this project are documented here. The format loosely
follows [Keep a Changelog](https://keepachangelog.com/); until 1.0 there are
NO compatibility promises between versions (see docs/upgrade-guide.md), and
breaking changes are called out per release.

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
