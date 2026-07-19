# Experiment 11: Large-Scale Validation (S1–S5) & DWS GPU Integration

This directory contains the reproduction scripts, configuration manifests, and raw empirical measurements for the large-scale validation scenarios (S1–S5) and Dynamic Workload Scheduler (DWS) GPU integration executed on GKE (`k8s-bridge-test` in `europe-west4-a`).

## Scenarios Covered

1. **S1: 5k Backlog & Paged Listing (`scripts/s1-backlog.sh`)**
   - Injects 5,000 held Slurm jobs and measures controller RSS and working set memory during batch translation.
   - Empirical outcome: ~3,448 jobs/min injection rate, peak steady-state RSS 307 MiB, 0 API errors.

2. **S2: 1,000-Pod Dynamic Churn (`scripts/s2-churn.sh`)**
   - Creates and deletes worker pods in batches of 250 across an autoscaled Spot node pool (`9→87` nodes).
   - Empirical outcome: Sustained churn of 8.33 pods/s (peaking at 21.65 pods/s during deletion), zero informer deadlocks or memory leaks.

3. **S3: Sustained Throughput (`scripts/s3-throughput.sh`)**
   - Submits steady batches of 25 jobs every 30s to achieve 50 jobs/min throughput.
   - Empirical outcome: ~17.6s reconcile duration per 25-JobSet batch.

4. **S4: Multi-Partition Fan-Out (`manifests/workloadmixing-fanout.yaml`)**
   - Deploys a single `WorkloadMixing` CR (`wm-fanout`) with 10 array entries in `spec.partitionMappings` (`fanout-1`..`10`), hot-reloaded via `--workloadmixing slurm-jobs/wm-fanout`.

5. **S5 & DWS GPU Integration (`manifests/dws-gpu-qp.yaml`)**
   - Demonstrates Kueue `ProvisioningRequestConfig` and `AdmissionCheck` generating GKE `ProvisioningRequest` objects (`Accepted: True / SuccessfullyQueued`).

## Results Directory
The `results/` folder contains raw log excerpts and measurement summaries tracing every empirical figure cited in `docs/VALIDATION.md`.
