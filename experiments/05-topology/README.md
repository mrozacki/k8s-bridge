# Experiment 05 — topology-aware scheduling with a simulated datacenter

- **Status:** prepared; live validation in progress (results below)
- **Setup:** same playground cluster, scaled to 4 spot nodes

## Goal

Prove that network-topology requirements flow end to end:
`sbatch --switches=1` → bridge translation (ADR-0008) → Kueue TAS →
slurmd pods co-located in one simulated rack → Slurm job runs rack-local.

## The simulated topology

Four nodes labeled as `block-{a,b}` × `rack-{1,2}` (script
`scripts/label-topology.sh`). Kueue TAS reads only node labels, so this is
behaviorally identical to a real GKE block/subblock topology.

```
block-a            block-b
├─ rack-1: node1   ├─ rack-1: node3
└─ rack-2: node2   └─ rack-2: node4
```

## Run order (after experiments/DEMO.md steps 1-2)

```bash
cd experiments/05-topology
./scripts/label-topology.sh
kubectl apply -f manifests/topology-tas.yaml

# Scenario A: K8s gang that MUST share a rack
kubectl create -f manifests/tas-job-required-rack.yaml   # first Job only
kubectl get pods -n default -o wide     # both pods on the SAME rack's node

# Scenario B: Slurm job with switch locality through the bridge
#   (bridge config must set topology.requiredLevel; see example-config.yaml)
kubectl -n slurm exec deploy/slurm-login-slinky -- \
  sbatch --hold --partition=mixing --ntasks=2 --switches=1 \
  --wrap='srun bash -c "echo TOPO task on $(hostname); sleep 30"'
# watch bridge-top: both slurmd pods land in one rack; job runs there

# Scenario C (negative): gang too big for any rack -> stays inadmissible
# (second Job in tas-job-required-rack.yaml)
kubectl get workloads -n default   # inadmissible with a TAS message
```

Observe everything in `tools/bridge-top.sh` — the TOPOLOGY panel groups
nodes by block/rack and shows pod placement live.

## Results (executed 2026-07-04)

**All three scenarios passed.**

- **A (required rack, K8s gang):** both pods admitted into `block-a/rack-1`
  and placed on its node — TAS picked one domain and pinned the podset.
- **B (Slurm `--switches=1` via bridge):** the bridge stamped
  `podset-required-topology: example.com/rack` on the JobSet, both slurmd
  pods landed in `block-a/rack-1`, and Slurm ran the job on its two
  rack-local dynamic nodes. Full chain: sbatch flag → REST
  (`required_switches`) → annotation → TAS placement → Slurm execution.
- **C (negative):** the 4-pod gang stayed inadmissible with a precise
  message: `topology "simulated-dc" doesn't allow to fit any of 4 pod(s)`.
- **UI:** `bridge-top` TOPOLOGY panel verified live — blocks/racks with
  per-node pod placement, appears automatically when labels exist.

Live findings:
1. `required_switches` in slurmrestd v0.0.44 is a PLAIN number, unlike the
   `{set,number}` wrappers elsewhere — bridge parsing fixed accordingly.
2. The warnings-as-errors client policy was too eager: an EMPTY queue
   answers `GET /jobs` with warning "Zero jobs to dump", which broke every
   tick on an idle cluster. Policy narrowed: warnings fail hard only on
   mutating requests.
3. **Autoscaler vs. simulated topology:** with `optimize-utilization` and
   `min-nodes=1`, idle "racks" get deleted (labels die with nodes). For
   this experiment pin the pool: `gcloud container clusters resize ... 4`
   and/or create with `--min-nodes 4`. Real deployments get topology
   labels from node provisioning, so this is a simulation artifact.
4. Kueue v1beta1 manifests print deprecation warnings (v1beta2 is current)
   — migration noted as follow-up.
