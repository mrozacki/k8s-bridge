# k8s-bridge Test Plan Proposal

This document presents proposed integration and End-to-End (E2E) test scenarios for the Workload Mixing (`k8s-bridge`) project. It is based on doubts, edge cases, and risks identified in the "Implementation Details" and "Minimal Viable Design" documents, including feedback from the review process (specifically suggestions reported by co-authors and team reviewers).

## 1. `WorkloadMixing` CR Validation (Admission Webhook)
**Goal:** Ensure early rejection of misconfigured Custom Resources for WorkloadMixing.
* **Scenario 1.1:** Applying a CR with a missing or invalid `localQueue` field.
  * *Expected:* The Admission Webhook rejects the request with a clear error message before saving it to the cluster.
* **Scenario 1.2:** Applying a CR with missing partition definitions (`partitionMappings`) or referencing a non-existent Slurm controller configuration.
  * *Expected:* The Admission Webhook correctly intercepts and rejects the invalid configuration structure.

## 2. Advanced `PodSpec` and Sidecars Handling
**Goal:** Verify that created workspace pods (dynamic Slurm worker nodes) fully support extensive pod specifications in Kubernetes, specifically sidecars and volume mounts.
* **Scenario 2.1 (Pods with sidecar and volumes):** Deploying a CR mapping to a partition that requires a sidecar (e.g., for logging, network daemons) and custom mounted volumes.
  * *Expected:* `k8s-bridge` correctly launches the dynamic node in Kubernetes by creating an appropriate JobSet, which successfully spawns all defined containers with correct mounts.
* **Scenario 2.2 (Orphans / Zombie Pod Cleanup):** Completing a job under Slurm control using a partition featuring a sidecar.
  * *Expected:* Upon job completion or termination by Slurm, the JobSet in Kueue cleanly shuts down long-running sidecars, avoiding node deletion blocks and resource leaks in the K8s cluster.
* **Scenario 2.3 (Partition-specific manifests):** Concurrently submitting Slurm jobs to two different partitions (e.g., `cpu-default` with preemptible tolerations and `gpu-high-priority` with dedicated `nodeSelector` labels).
  * *Expected:* `k8s-bridge` creates separate `JobSet` objects with appropriate settings for both sets of jobs, accurately transferring the intent from the CR object.

## 3. Node Identity and Custom Compute Classes (CCC) Reliability
**Goal:** Safeguard against improper node allocation back to Slurm via dedicated Slurm features, and prevent mixing up identity bindings between job IDs and dedicated pods.
* **Scenario 3.1 (In-flight Pod Restart):** Manually killing a slurmd worker pod during a long-running computational process.
  * *Expected:* The Slurm pool correctly marks the node as failing (e.g., `DOWN` status) after `SlurmdTimeout`, and then requeues the job. Meanwhile, K8s rebuilds the node without identity mismatch upon re-registration via the Custom Compute Classes mechanism.
* **Scenario 3.2 (Resource Isolation with SCC):** Concurrently running multiple similar symmetric jobs within Slurm.
  * *Expected:* The resulting dynamic nodes have hermetic 'features' flags generated (`nodes-for-<slurm_id>`). No mixing occurs – job `A` never injects into or occupies pod-nodes generated for job `B`.

## 4. Kueue Preemption / Parallel Reclamation Issue
**Goal:** Ensure tight state synchronization for hardware-suspended or preempted jobs within Kueue Preemption and fallback mechanisms to Slurm.
* **Scenario 4.1 (Kueue Preemption with Higher Priority):** Submitting a high-priority native Kueue job to a cluster fully utilized by a lower-priority job controlled via `k8s-bridge`.
  * *Expected:* Kueue preempts the JobSet belonging to the lower-priority job from k8s-bridge (state returns to `Suspend`). The k8s-bridge system correctly captures the preemption on the JobSet side, and releases or shuts down the node allocation on the Slurm controller side. Slurm flawlessly returns the job to the pending queue to wait out the resource conflict.

## 5. Resilience to k8s-bridge Component Failure (Resource Leaks)
**Goal:** Verify cleanup mechanisms and avoid "orphan" issues in the Kubernetes space during control interruptions on the k8s-slurm bridge.
* **Scenario 5.1 (k8s-bridge Controller Disappearance):** Killing the `k8s-bridge` component in the cluster while the Slurm Epilog script hangs, or the job ends.
  * *Expected:* Even in the absence of `k8s-bridge`, resources cannot be leaked indefinitely due to built-in `activeDeadlineSeconds` limits or similar built-in methods on JobSet.
* **Scenario 5.2 (Startup Reconciliation):** Controller startup after an outage in the K8s control cluster with stale states in the Slurm controller and unreconciled JobSets.
  * *Expected:* The k8s-bridge startup reconciliation routine immediately diagnoses and cancels unfinished workload nodes after the lost binding without triggering system-wide failures.

## 6. Slurm REST API Compatibility
**Goal:** Flawless retrieval of objects from different versions of Slurm clusters, confirming correct operation of pollers.
* **Scenario 6.1 (Slurm REST API for Hold States):** Testing refresh on reservation statuses against an API version prior to Slurm 24 lacking list format status.
  * *Expected:* `k8s-bridge` resilience to minor JSON API differences depending on the target installation. The poll module does not crash on JSON objects.

## 7. Exclusive vs. Non-exclusive Nodes Job Requests
**Goal:** Verify the division of the mapped resource request into the native K8s environment for flags such as `--exclusive` or node allocation by Slurm.
* **Scenario 7.1:** A user job submitted with the exclusive request flag on the Slurm side (`--exclusive`).
  * *Expected:* `k8s-bridge` creates the correct pod breakdown and requests a full node at the Kubernetes level (or configures restrictive Anti-Affinity rules preventing the location of other jobs on the JobSet node).
