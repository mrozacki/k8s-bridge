# Experiment 10 — ray-bridge: RayCluster inner-workload admission (small scale)

**Goal.** Validate, on a free local cluster, that ray-bridge brings the inner
workloads of a shared KubeRay `RayCluster` under Kueue admission — the Ray
mirror of how k8s-bridge queues Slurm jobs (ADR-0006/0012/0013).

**Setup.** Everything runs on `kind` (local). No GKE.

**Result (2026-07-07).** The dedicated-capacity-through-Kueue model is
validated live; the original `spec.suspend` mechanism was **falsified** by real
KubeRay and replaced with the **pin-gate** model (ADR-0013). Details:
`docs/VALIDATION.md`.

## Setup (validated commands)

```bash
kind create cluster --name raymix --wait 90s

# Controllers (versions match the prototype's go.mod / prior sessions).
kubectl apply --server-side -f https://github.com/kubernetes-sigs/jobset/releases/download/v0.12.0/manifests.yaml
kubectl apply --server-side -f https://github.com/kubernetes-sigs/kueue/releases/download/v0.18.2/manifests.yaml
kubectl -n kueue-system rollout status deploy/kueue-controller-manager --timeout=300s   # wait: its webhook gates the next step
helm repo add kuberay https://ray-project.github.io/kuberay-helm/ && helm repo update
helm install kuberay-operator kuberay/kuberay-operator --version 1.6.2 -n kuberay-system --create-namespace
kubectl -n kuberay-system rollout status deploy/kuberay-operator --timeout=180s

# The ray image is large (~2.7 GB); pre-pull + load so the head starts fast.
docker pull rayproject/ray:2.9.0 && kind load docker-image rayproject/ray:2.9.0 --name raymix

# Platform (Kueue quota + priorities) and the shared cluster.
kubectl apply -f manifests/ray-platform.yaml
kubectl apply -f manifests/raycluster.yaml
kubectl -n ray wait --for=condition=ready pod -l ray.io/node-type=head --timeout=180s
```

## Run the bridge and submit an inner workload

```bash
# Build + run ray-bridge against the kind cluster (leader election off for a
# single local process). Config: ray-bridge-config.yaml (managed cluster +
# pool→priority mapping + worker spec).
cd ../../. && go build -o /tmp/ray-bridge ./cmd/ray-bridge
POD_NAMESPACE=ray /tmp/ray-bridge --config=/path/to/ray-bridge-config.yaml \
    --leader-elect=false --allowed-worker-images=rayproject/ &

# Submit an inner RayJob (pin-gate model: NOT suspended; carries
# entrypointResources requiring wm-job-inner-c — the admission gate).
kubectl apply -f manifests/inner-rayjob.yaml
```

Expected: ray-bridge creates a Kueue-admitted worker `JobSet`
(`ray-workers-inner-c`) whose pods `ray start` into the shared cluster
advertising `wm-job-inner-c`. The RayJob's driver stays **pending** until Kueue
admits those workers (the pin resource appearing is the release).

## What was validated live

| Mechanic | Result | How to see it |
|---|---|---|
| Worker joins via bare `ray start` | ✅ | `kubectl -n ray exec <head> -- ray status` shows 2+ nodes |
| `wm-job-<id>` custom resource | ✅ | same `ray status`: `0.0/2.0 wm-job-<id>` |
| `ray health-check` readiness | ✅ exit 0 | worker readiness probe passes |
| Kueue admits the worker JobSet | ✅ | `kubectl -n ray get workloads` → ADMITTED=True |
| Pin gates the driver | ✅ | RayJob stays `Running` with no job status until a worker advertises the pin |
| `spec.suspend` + clusterSelector | ❌ **KubeRay forbids** | drove the ADR-0013 pivot |

**Known env limitation:** run-to-completion (`ray job submit`) is flaky in this
single-node kind setup (`No available agent to submit job`); the *gate* works,
the *completion* needs a sturdier Ray cluster. See the findings note.

## Teardown

```bash
kind delete cluster --name raymix
```
