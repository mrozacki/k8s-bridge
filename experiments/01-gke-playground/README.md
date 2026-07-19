# Experiment 01 — GKE playground: the mixing stack, side by side

- **Status:** executed 2026-07-04 — PASSED; see Results below and
  [docs/VALIDATION.md](../../docs/VALIDATION.md)
- **Setup:** zonal cluster (management fee covered by the GKE free tier),
  2× `e2-standard-4` spot nodes; **teardown after every session**

## Goal

Stand up all building blocks of the k8s-bridge architecture on a minimal GKE
cluster and run one workload of each kind **without any bridge yet**:

1. a plain Kubernetes batch Job admitted by **Kueue**,
2. a **RayCluster** (KubeRay) admitted by Kueue from the same quota,
3. a long-running **inference** Deployment outside Kueue (baseline per ADR-0002),
4. a classic **Slurm** job on the slurm-operator's static nodeset.

This validates our understanding of each component in isolation and produces
the shared environment that experiment 02 (manual bridge) builds on.

## What we want to learn

- How Kueue admission actually behaves: suspended Jobs, Workload objects,
  quota accounting in the ClusterQueue status.
- Whether Kueue's KubeRay integration works out of the box for RayCluster
  (which Kueue config changes are needed — record them).
- How the Slinky slurm chart is structured (slurmctld, slurmrestd, login,
  nodesets) and how to reach `sbatch` and the REST API.
- Whether everything fits on 2 spot nodes (record actual resource usage).
- What happens to each workload class when a **spot node is reclaimed** —
  free chaos engineering.

## Run order

```bash
source scripts/00-env.sh
./scripts/01-create-cluster.sh       # ~5 min
./scripts/02-install-components.sh   # ~10-15 min
./scripts/03-configure-queues.sh

# Scenario A: K8s batch through Kueue
kubectl create -f workloads/kueue-batch-job.yaml
kubectl get workloads,jobs -n default

# Scenario B: Ray through Kueue (same quota!)
kubectl apply -f workloads/ray-cluster.yaml
kubectl get workloads,rayclusters -n default

# Scenario C: inference baseline (outside Kueue)
kubectl apply -f workloads/inference-deployment.yaml

# Scenario D: classic Slurm job on the static nodeset
kubectl -n slurm exec -it deploy/slurm-login -- bash   # verify actual login entrypoint
sbatch /path/to/slurm-hello.sbatch                     # copy script in first

./scripts/99-teardown.sh             # ALWAYS
```

## Known unknowns & toolchain notes

- **VM Sizing:** A minimum of `e2-standard-4` (4 vCPU, 16GB) is recommended for both system/control-plane and worker node pools so that Slurm controllers, Kueue, cert-manager, and Ray operator fit comfortably alongside test workloads without CPU throttling.
- **Helm toolchain on managed workstations:** on some corporate/managed Linux images the packaged `helm` on `PATH` is not upstream Helm 3 and fails on standard charts or flags. Install upstream Helm 3 into `~/.local/bin/helm` and ensure it precedes any system binary on `PATH` (see `docs/operations.md`).
- Kueue's default config may need the `jobset` / `ray.io/raycluster`
  frameworks enabled explicitly (`kueue-manager-config` ConfigMap) —
  script 02 prints it for inspection.
- The slurm chart values schema (nodeset sizing, REST API exposure, login
  access) must be checked against `helm show values`; the install command is
  the upstream README default and may need tuning to fit small nodes.
- Exact name of the login Deployment/Service and how the auth key secret is
  organized — needed later by experiment 02.

## Results (executed 2026-07-04, overnight run)

**All four scenarios passed.** K8s Job and RayCluster were admitted by Kueue
from the same ClusterQueue (4 CPU / 6 Gi visibly reserved), the inference
Deployment ran alongside, and a classic Slurm job executed on the static
nodeset. The mixed load triggered a real Cluster Autoscaler scale-up (2→3
nodes). Kueue v0.18.2 needed no integration patching. One chart fix was
required (partition validation) — details and all findings in
[docs/VALIDATION.md](../../docs/VALIDATION.md).
Cluster lifetime ≈ 1.5 h. Environment fully torn down afterwards.
