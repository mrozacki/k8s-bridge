# k8s-bridge

**Make Kueue the single admission controller for mixed Slurm, Ray, and
Kubernetes workloads on one pool of resources.**

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
![Status: experimental](https://img.shields.io/badge/status-experimental-orange.svg)
![Go](https://img.shields.io/badge/go-1.26-00ADD8.svg)

k8s-bridge lets HPC-style **Slurm** jobs and **Ray** workloads share a single
pool of Kubernetes resources with **Kueue** as the one admission authority —
so quotas, priorities, preemption, fair sharing, autoscaling, and topology-aware
placement apply uniformly across all of them. It is a Kubernetes-native
alternative to [Slurm Bridge](https://github.com/SlinkyProject/slurm-bridge):
instead of making Slurm the cluster scheduler, it makes Kueue the master and
translates other workloads into Kueue-admitted capacity.

> **Status: experimental prototype.** This repository contains a working
> prototype, its design records, and reproducible experiments. It is
> live-validated (see below) but is not production-ready. Interfaces, layout,
> and scope may change.

## The problem

Organisations running both Slurm (HPC/AI training) and Kubernetes (Ray, batch,
inference) usually keep them on **separate, statically partitioned pools** —
one side sits idle while the other is starved. Unifying them means picking a
single scheduler that arbitrates every workload. k8s-bridge picks **Kueue**,
because it already provides cluster-wide quotas, priorities, preemption,
fair-sharing cohorts, and integrations with cluster autoscaling and
topology-aware scheduling — and keeps the whole cluster Kubernetes-native.

## How it works

The core idea is a **bridge pattern**: for each external workload, create a
Kueue-managed unit of dedicated capacity, let Kueue admit it against the shared
quota, then pin the workload to exactly that capacity and release it — cleaning
up when it finishes.

```
external workload  ──►  bridge creates a Kueue-labelled JobSet of dedicated pods
                        Kueue admits it (quota, priority, preemption, topology)
                        pods join the external system as workers
                        the workload is pinned to those workers and released
                        on completion: drain workers, delete the JobSet
```

Two controllers apply this pattern:

- **`k8s-bridge`** (Slurm): watches held Slurm jobs via the Slurm REST API,
  translates each into a JobSet of `slurmd` pods that register as *dynamic Slurm
  nodes*, and releases the job onto them once Kueue admits the JobSet.
- **`ray-bridge`** (Ray): watches inner workloads of a shared, long-lived
  `RayCluster` (`RayJob`s targeting it via `clusterSelector`), stands up a
  Kueue-admitted JobSet of dedicated Ray workers advertising a per-job custom
  resource, and gates the job on that resource so it cannot run until Kueue
  admits the workers (the *pin-gate* model,
  [ADR-0013](docs/adr/0013-ray-pin-gate-admission.md)).

Both are built on controller-runtime and share a common admission library, so a
Slurm JobSet and a Ray worker JobSet are just two Kueue workloads competing in
the same ClusterQueue.

## Workloads in scope

| Workload | How it participates | Status |
|----------|---------------------|--------|
| Slurm batch jobs | Translated to JobSets by `k8s-bridge` | Implemented, live-validated |
| Ray inner workloads (jobs in a shared `RayCluster`) | Dedicated Kueue-admitted workers per job, gated by a Ray custom resource (`ray-bridge`) | Implemented, live-validated ([ADR-0013](docs/adr/0013-ray-pin-gate-admission.md)) |
| Kubernetes batch (Job/JobSet) | Admitted by Kueue directly | Native |
| Standalone `RayJob` (own cluster) | Already Kueue-integrated natively; no bridge needed | Native / out of scope |
| Serving / inference | Two-tier: Kueue admits capacity at high priority; autoscalers own replica counts | Design ([ADR-0007](docs/adr/0007-two-tier-serving-admission.md)) |

## What's been validated

Live, on `kind` and small GKE clusters (torn down after each run):

- **Slurm:** the full discover → translate → admit → dynamic-node → release →
  cleanup cycle, and 520 concurrent held-job → JobSet translations with zero
  tick errors.
- **Ray:** worker join, per-job resource pinning, run-to-completion, and Kueue
  **preemption by priority** — including *cross-type* preemption (a high-priority
  Ray workload preempting a plain Kubernetes batch JobSet in one ClusterQueue)
  and **topology-aware co-location** (TAS).

See [`docs/VALIDATION.md`](docs/VALIDATION.md) for the per-run findings.

## Getting started

Build and test the controllers:

```bash
make test              # unit tests
make test-integration  # envtest against real JobSet / Kueue / RayJob CRDs
go build ./...         # both binaries: cmd/k8s-bridge and cmd/ray-bridge
```

Try it on a cluster:

- **Ray on `kind` (free):** [`experiments/10-ray-bridge/README.md`](experiments/10-ray-bridge/README.md) —
  a reproducible runbook that stands up KubeRay + Kueue + JobSet, runs the bridge,
  and submits an inner workload.
- **Full feature tour:** [`experiments/DEMO.md`](experiments/DEMO.md) — a narrated
  runbook exercising Slurm + Kubernetes + Ray + inference on one pool.
- **Deploy:** Helm charts under [`deploy/chart/`](deploy/chart/) for both bridges;
  see [`docs/installation.md`](docs/installation.md) for a consolidated,
  production-oriented install guide covering the full stack.
- **Upgrading an existing install:** [`docs/upgrade-guide.md`](docs/upgrade-guide.md) —
  CRD upgrade caveats, the `Recreate` rollout strategy, and compatibility policy.
- **Component versions:** [`docs/compatibility-matrix.md`](docs/compatibility-matrix.md) —
  which Kubernetes/Kueue/JobSet/KubeRay/Slurm versions this repo has actually
  validated against.
- **ray-bridge configuration reference:** [`docs/ray-bridge-reference.md`](docs/ray-bridge-reference.md) —
  every config field, the `ray-bridge.x-k8s.io/*` annotation contract, webhook
  decision semantics, and the admission-enforcement caveat.

## Repository layout

| Path | Purpose |
|------|---------|
| `cmd/`, `internal/`, `api/` | The Go controllers (`cmd/k8s-bridge`, `cmd/ray-bridge`) and their internal packages |
| `deploy/` | Helm charts, the `WorkloadMixing` CRD, and monitoring manifests |
| `docs/reference/` | Supporting reference documents (threat model) |
| `docs/adr/` | Architecture Decision Records |
| `docs/architecture.md` | System and code architecture |
| `docs/controller.md` | Controller reference: flags, config surface, deployment shapes |
| `docs/installation.md` | Consolidated production installation guide (full stack) |
| `docs/upgrade-guide.md` | Upgrading an existing install: CRDs, rollout strategy, compatibility policy |
| `docs/compatibility-matrix.md` | Component versions this repo has actually validated against |
| `docs/ray-bridge-reference.md` | ray-bridge configuration, annotation contract, webhook reference |
| `docs/operations.md` | Day-2 SLOs, alerts, metrics, and runbooks |
| `docs/VALIDATION.md` | Consolidated validation summary and findings |
| `experiments/` | Numbered, self-contained experiments (manifests, scripts, results) |
| `dashboards/` | Grafana dashboards-as-code |

## Design and decisions

The authoritative design starts with
[`docs/architecture.md`](docs/architecture.md);
significant decisions are recorded as ADRs in [`docs/adr/`](docs/adr/).
A good entry point for the *why* is
[ADR-0006](docs/adr/0006-ray-inner-workload-admission.md) (Ray admission
granularity) and [ADR-0013](docs/adr/0013-ray-pin-gate-admission.md) (the
pin-gate mechanism).

## Related projects

- [Kueue](https://kueue.sigs.k8s.io/) — job queueing and admission control for Kubernetes
- [JobSet](https://jobset.sigs.k8s.io/) — the grouped-job API the bridges emit
- [KubeRay](https://github.com/ray-project/kuberay) — Ray on Kubernetes (`RayCluster`, `RayJob`)
- [Slinky / slurm-operator](https://github.com/SlinkyProject/slurm-operator) — Slurm on Kubernetes
- [Slurm](https://slurm.schedmd.com/) — the HPC workload manager

## Contributing

Contributions are welcome. Please read [`CONTRIBUTING.md`](CONTRIBUTING.md) and
the [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md). Commits must be signed off
([DCO](https://developercertificate.org/)). Security issues: see
[`SECURITY.md`](SECURITY.md).

## License

Apache License 2.0 — see [`LICENSE`](LICENSE).
