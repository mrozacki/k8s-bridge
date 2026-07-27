# The `WorkloadMixing` custom resource

`WorkloadMixing` (API group `k8s-bridge.x-k8s.io/v1alpha1`, short name `wm`) is the
configuration surface of the Slurm bridge: **one CR describes one bridge
instance** — one Slurm cluster whose jobs are translated into Kueue-admitted
JobSets. This document explains the design of the resource: why it exists,
how it is structured, how the controller consumes it, and what every field
means.

## Why a custom resource

The prototype originally read a mounted YAML file. Promoting that file to a
CRD-backed API keeps the schema identical (the CR spec mirrors the file config
1:1 — the `json` tags in `api/v1alpha1/workloadmixing_types.go` match the
internal config struct exactly) while adding what a file cannot provide:

- **Declarative lifecycle** — the bridge's configuration is a Kubernetes
  object: versioned, RBAC-guarded, auditable, and editable with `kubectl`
  like everything else in the cluster.
- **Server-side validation** — OpenAPI schema constraints and CEL rules
  reject invalid configuration *at admission time*, before the controller
  ever sees it, instead of failing at controller startup.
- **Hot reload** — the controller watches the CR and re-applies every spec
  change on the fly. A change that fails deeper validation is reported on
  `status.conditions` and the controller keeps running on its previous,
  still-valid configuration.
- **Status reporting** — the bridge writes its health back to the same
  object, so `kubectl get wm` answers "is the bridge healthy and which queue
  does it feed" in one line.
- **Multiple instances** — one CR per Slurm cluster. The controller runs a
  supervisor that starts one reconcile loop per CR in its own namespace; two
  CRs pointing at the same `slurmRestURL` with an overlapping partition are
  refused (`Ready=False`, reason `ConflictingSpec`) because they would
  double-manage the same jobs.

The file mode still exists for development; the CR is the deployment-grade
path, and the CRD YAML is generated from the Go types (a build artifact, not
hand-maintained).

## Anatomy of the spec

The spec groups into five concerns:

```yaml
apiVersion: k8s-bridge.x-k8s.io/v1alpha1
kind: WorkloadMixing
metadata:
  name: default            # one per Slurm cluster
  namespace: slurm-bridge  # JobSets are created in this namespace
spec:
  # 1. How to reach and authenticate to Slurm
  slurmRestURL: https://slurm-restapi.slurm:6820
  slurmTokenFile: /var/run/secrets/slurm/token
  # 2. What Kueue queue the translated JobSets compete in
  localQueue: team-a-queue
  # 3. Which Slurm partitions are bridged, and at what priority
  partitionMappings:
    - partitionName: batch
      workloadPriorityClass: standard
    - partitionName: urgent
      workloadPriorityClass: high
      localQueue: urgent-queue   # optional per-partition override
  # 4. What the dynamic Slurm node pods look like
  slurmd:
    image: ghcr.io/example/slurmd:24.05
    confServer: slurm-controller.slurm.svc.cluster.local:6817
    authSecretName: slurm-auth
  # 5. Optional behavior knobs (topology, pacing, safety valves)
  topology:
    preferredLevel: cloud.google.com/gce-topology-block
```

### 1. Slurm connection and authentication

| Field | Meaning |
|-------|---------|
| `slurmRestURL` | Base URL of `slurmrestd`. Must be `https://` unless `allowInsecureHTTP: true` — a CEL validation rule enforces this at admission, because the Slurm JWT is bearer-equivalent and must not travel in cleartext. |
| `allowInsecureHTTP` | Development-only opt-in for a plaintext endpoint. |
| `slurmCACertFile` | PEM CA bundle path (mounted Secret/ConfigMap) to verify the `slurmrestd` certificate; empty uses system roots. |
| `slurmInsecureSkipTLSVerify` | Development escape hatch: disable certificate verification. |
| `slurmTokenFile` | Path to a mounted JWT for `slurmrestd`. Token delivery is file-mount only by design: a `slurmTokenSecretRef` field is deliberately absent until code exists to consume it, so the schema never advertises capabilities the controller does not have. |
| `slurmUser` | Sent as `X-SLURM-USER-NAME` alongside the token. Empty (the safe default) omits the header so `slurmrestd` uses the user the JWT was minted for. |

### 2. Kueue queue binding

| Field | Meaning |
|-------|---------|
| `localQueue` | The Kueue `LocalQueue` translated JobSets are submitted to. This is the single point where Slurm work enters Kueue's quota/priority/preemption machinery. |

The CR's own `metadata.namespace` is authoritative for where JobSets are
created — there is no `namespace` field in the spec, so configuration cannot
disagree with reality.

### 3. Partition mappings

`partitionMappings` (min 1, max 256) enumerates the Slurm partitions under
bridge management. Each entry ties a partition to a Kueue
`WorkloadPriorityClass` — the unified priority model both schedulers agree
on — and may override `localQueue` per partition (multi-team setups: each
partition can feed a different team's queue). Jobs in unmapped partitions are
ignored by the bridge.

### 4. The `slurmd` pod template

The bridge does not run Slurm jobs itself — it creates JobSets of `slurmd`
pods that register with `slurmctld` as *dynamic nodes*, and the job is then
released onto them. The `slurmd` block describes those pods:

| Field | Meaning |
|-------|---------|
| `image` | The `slurmd` container image. |
| `confServer` | The `slurmctld` address `slurmd` registers against. |
| `authSecretName` | Secret (in the CR's namespace) holding `slurm.key` for node authentication. |
| `gpuResourceName` | Kubernetes extended resource requested when a Slurm job asks for GRES GPUs; defaults to `nvidia.com/gpu`. |
| `privileged` | Run `slurmd` privileged (default true — `slurmd` needs writable cgroups). |
| `sharedStorage` | Optional NFS mount (`nfsServer`, `nfsPath`, `mountPath`) into every `slurmd` pod so job scripts and data resolve like on a static cluster. |

### 5. Behavior knobs

| Field | Meaning |
|-------|---------|
| `pollInterval` | How often the bridge polls Slurm for held jobs (Go duration; 1s minimum enforced). |
| `slurmRequestTimeout` | Per-request HTTP timeout to `slurmrestd` (default 30s; 1s–10m). Raise for large backlogs — the `GET /jobs` payload grows with queue depth. Startup-only. |
| `slurmRequestsPerSecond` | Client-side rate limit on all bridge → `slurmrestd` traffic (0 = unlimited). Exists because `slurmrestd` shares `slurmctld`'s internal lock and has no throttling of its own — an unpaced bridge burst can starve the Slurm scheduler itself. Burst is 2× the rate. Startup-only. |
| `createWorkers` | Bounded worker-pool size for parallel JobSet creation (default 8). |
| `enablePrioritySync` | Experimental Slurm ↔ Kueue priority mirror; default off. |
| `maxUserPriority` | Cap on user-originated priority requests (0 disables the cap). |
| `cancelOrphanedJobs` | Cancel a released Slurm job whose JobSet has disappeared. **Off by default**: `scancel` is irreversible and the trigger is the *absence* of a JobSet — which a mis-scoped informer cache or lost RBAC looks exactly like. Opt in only once the deployment's cache scope is trusted. |
| `orphanGraceTicks` | Consecutive ticks a job must be seen without its JobSet before `cancelOrphanedJobs` acts (default 3). |
| `drainOnPreemption` | Proactively delete a preempted job's dynamic-node records on Kueue eviction instead of waiting out Slurm's `SlurmdTimeout`. Off by default; the timeout remains the backstop. |
| `failedJobSetRetention` | How long to keep a JobSet that outlived its Slurm job, so the failure stays inspectable with `kubectl`. Go duration (e.g. `1h`); empty or `0` — the default — deletes it in the same tick that fails the job, as before. Only JobSets that already reached a terminal condition are ever retained, so their pods are finished and their Kueue quota released: this holds an API object, not capacity. Capped at `7d`. |
| `topology.requiredLevel` | Node-label key applied as `podset-required-topology` when a Slurm job requests switch locality (`--switches=N`). Empty disables translation. |
| `topology.preferredLevel` | Node-label key applied as `podset-preferred-topology` to every other bridge job (best-effort gang locality). |

A recurring design rule is visible in the risky knobs: **anything that can
destroy work is opt-in and defaults off**, with a stated failure mode and a
recovery path.

## Validation model

Three layers, from cheapest to deepest:

1. **OpenAPI schema** (generated from kubebuilder markers): required fields,
   string length ceilings, numeric ranges, duration/path regexes. Rejected by
   the apiserver before anything runs.
2. **CEL rules on the spec**: cross-field invariants — today, the
   https-unless-opted-out rule for `slurmRestURL`.
3. **Controller validation on (re)load**: everything the schema cannot see —
   e.g. cross-CR conflict detection (same `slurmRestURL` + overlapping
   partition). Failures surface as `Ready=False` on status, and a running
   bridge keeps its last valid config.

## Status

`status.conditions` uses the ecosystem-standard `metav1.Condition`. The
bridge maintains exactly one condition of type `Ready` (with
`observedGeneration`, so a watcher can tell whether the reading is fresh for
the spec it sees) and leaves foreign condition types untouched for other
tooling. Printer columns surface the essentials:

```
$ kubectl get wm
NAME      READY   QUEUE
default   True    team-a-queue
```

## Deployment shapes

- **Supervisor mode (`--config-source=cr`, the CR-mode default):** the
  controller supervises every `WorkloadMixing` in its own namespace
  (`POD_NAMESPACE`) and runs one bridge loop per CR.
- **Single-CR mode (`--workloadmixing <namespace>/<name>`):** binds the
  controller to exactly one CR — the conservative shape for staged rollouts.
- **File mode (`--config-source=file`):** a mounted config file, development
  only.

## See also

- [`docs/tutorial.md`](tutorial.md) — hands-on walk-through, including
  applying a minimal `WorkloadMixing`.
- [`docs/installation.md`](installation.md) — production-oriented install.
- [`docs/controller.md`](controller.md) — controller flags and deployment
  shapes.
- `api/v1alpha1/workloadmixing_types.go` — the authoritative schema with
  field-by-field rationale in comments.
