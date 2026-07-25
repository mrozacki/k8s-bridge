# Installation guide

Audience: platform operators installing the full stack (k8s-bridge for
Slurm, ray-bridge for Ray, both admitted through Kueue) and the engineering
team taking this prototype toward production. Component versions referenced
below are the ones this repository has actually validated — see
`docs/compatibility-matrix.md` for the full table and what is explicitly
*not* validated.

This guide is written cluster-agnostic; a **GKE-specific subsection** at the
end calls out the handful of details (node pools, DWS/GPU, quota) that only
apply there. Everything else applies to any conformant Kubernetes cluster.

## 1. Prerequisites

Install these in order — later components assume the earlier ones exist.

### 1.1 Kubernetes cluster

Any cluster the project has exercised: kind (`v0.32.0` locally validated),
GKE Standard (`--release-channel regular`), or an envtest-backed apiserver
(1.33, integration tests only). See `docs/compatibility-matrix.md`.

### 1.2 cert-manager

Required if you install the Slinky slurm-operator (its webhooks need a
serving certificate) or want the ray-bridge admission webhook enabled.
Skip it if you are running ray-bridge only, with the webhook off.

```bash
helm upgrade --install cert-manager oci://quay.io/jetstack/charts/cert-manager \
  --namespace cert-manager --create-namespace \
  --set crds.enabled=true \
  --wait
```

(No version is pinned in this repo's own scripts — see the compatibility
matrix's "explicitly unverified" section.)

### 1.3 JobSet

Both bridges emit `jobset.x-k8s.io/v1alpha2` JobSets; the JobSet controller
must be installed before either bridge can create anything.

```bash
kubectl apply --server-side -f \
  https://github.com/kubernetes-sigs/jobset/releases/download/v0.12.0/manifests.yaml
```

### 1.4 Kueue

The master admission authority for the whole mixed pool.

```bash
kubectl apply --server-side -f \
  https://github.com/kubernetes-sigs/kueue/releases/download/v0.18.2/manifests.yaml
kubectl -n kueue-system rollout status deploy/kueue-controller-manager --timeout=300s
```

Kueue v0.18.2's default config already enables the `jobset.x-k8s.io/jobset`
and `ray.io/raycluster` integration frameworks — no `ConfigMap` patching
needed (verified live, `experiments/01-gke-playground`).

### 1.5 KubeRay (Ray side only)

Skip if you are not deploying ray-bridge.

```bash
helm repo add kuberay https://ray-project.github.io/kuberay-helm/ && helm repo update
helm install kuberay-operator kuberay/kuberay-operator --version 1.6.2 \
  -n kuberay-system --create-namespace
kubectl -n kuberay-system rollout status deploy/kuberay-operator --timeout=180s
```

### 1.6 Slinky slurm-operator (Slurm side only)

Skip if you are not deploying k8s-bridge. The chart is served from an OCI
registry with no GitHub-release versioning; `SLURM_OPERATOR_CHART_VERSION`
is left empty in this repo's scripts, meaning "latest at install time" — see
the compatibility matrix.

```bash
helm upgrade --install slurm-operator-crds oci://ghcr.io/slinkyproject/charts/slurm-operator-crds
helm upgrade --install slurm-operator oci://ghcr.io/slinkyproject/charts/slurm-operator \
  --namespace slinky --create-namespace --wait
helm upgrade --install slurm oci://ghcr.io/slinkyproject/charts/slurm \
  --namespace slurm --create-namespace \
  --values <your-slurm-values.yaml> \
  --wait --timeout 15m
```

Adapt `<your-slurm-values.yaml>` from
`experiments/01-gke-playground/manifests/slurm-values.yaml` (nodeset sizing,
REST API exposure, and — per `docs/architecture.md` §3 step 1 — the lua
JobSubmit plugin that auto-holds jobs submitted to mixing partitions).

## 2. Kueue queue objects

Every workload class (Slurm-via-k8s-bridge, Ray-via-ray-bridge, native
Kubernetes batch) is admitted from the **same** `ClusterQueue`/`LocalQueue`
topology. A minimal, working shape (merged from
`experiments/01-gke-playground/manifests/kueue-config.yaml` and
`experiments/10-ray-bridge/manifests/ray-platform.yaml`, both live-validated):

```yaml
apiVersion: kueue.x-k8s.io/v1beta1
kind: ResourceFlavor
metadata:
  name: default-flavor
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: ClusterQueue
metadata:
  name: main-queue
spec:
  namespaceSelector: {} # accept workloads from any namespace with a LocalQueue
  preemption:
    withinClusterQueue: LowerPriority
  resourceGroups:
    - coveredResources: ["cpu", "memory"]
      flavors:
        - name: default-flavor
          resources:
            - { name: cpu, nominalQuota: 12 }
            - { name: memory, nominalQuota: 48Gi }
---
apiVersion: v1
kind: Namespace
metadata:
  name: slurm-jobs # where k8s-bridge creates slurmd JobSets
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: LocalQueue
metadata:
  name: main
  namespace: slurm-jobs
spec:
  clusterQueue: main-queue
---
apiVersion: v1
kind: Namespace
metadata:
  name: ray # where ray-bridge watches inner RayJobs and creates worker JobSets
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: LocalQueue
metadata:
  name: ray-main
  namespace: ray
spec:
  clusterQueue: main-queue
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: WorkloadPriorityClass
metadata:
  name: high-priority
value: 1000
description: "latency-sensitive Slurm/Ray pools"
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: WorkloadPriorityClass
metadata:
  name: normal-priority
value: 100
description: "default for batch workloads"
```

Add a `LocalQueue` per team/namespace as needed (multi-team routing via
`partitionMappings[].localQueue` / `poolMappings[].localQueue` — see below);
Topology-Aware Scheduling objects (`Topology` CR, a `topologyName`-scoped
`ResourceFlavor`) are covered separately in `docs/architecture.md` §4a and
`experiments/05-topology/`.

## 3. Slurm token Secret

k8s-bridge authenticates to slurmrestd with a JWT; it does **not** create
this Secret itself (a credential is not a chart-managed value).

```bash
# Mint a token (interactively, from inside the Slurm cluster):
kubectl -n slurm exec slurm-controller-0 -c slurmctld -- \
  scontrol token username=root lifespan=14400 | sed 's/SLURM_JWT=//' > /tmp/wm-slurm-token

kubectl create secret generic slurm-rest-token \
  --from-file=token=/tmp/wm-slurm-token \
  -n <release-namespace>
```

`slurmTokenSecret` (chart value, default `slurm-rest-token`) names this
Secret; the chart mounts it at `/var/run/secrets/slurm/token` with
`defaultMode: 0440` and sets pod-level `fsGroup: 65532` so the
distroless-nonroot container (UID 65532) can read it — replicate both
settings if you mount a token Secret outside the chart's defaults (see
`docs/operations.md`, "Token Secret unreadable").

Tokens expire (the example above: 14400s / 4h). Rotate by minting a new
token and replacing the Secret; the bridge re-reads it per request, no
restart needed (`docs/operations.md`, "Token expiry").

## 3.1 Slurm auth-key Secret (prerequisite in the workload namespace)

The slurmd pods the bridge creates mount the Slurm cluster's auth key
(`config.slurmd.authSecretName`, default `slurm-auth-slurm`, key
`slurm.key`) to authenticate to slurmctld. That Secret is created by the
Slurm operator **in the Slurm cluster's namespace** — Kubernetes Secrets are
namespace-scoped, so the bridge's workload namespace does not see it, and a
GPU/any job's pod will hang in `ContainerCreating` with a `FailedMount`
event (suite-F finding 9, `docs/VALIDATION.md`).

Copy it once per workload namespace, after the Slurm cluster is up:

```bash
kubectl get secret slurm-auth-slurm -n slurm -o yaml \
  | grep -v -E 'namespace:|resourceVersion:|uid:|creationTimestamp:' \
  | kubectl apply -n slurm-jobs -f -
```

Re-copy after rotating the Slurm auth key. This is a DELIBERATE design
decision (2026-07-13), not a gap: the alternative — the bridge syncing
Secrets across namespaces itself — would require granting the controller
cross-namespace Secret read/write, widening exactly the trust boundary the
security audit's namespaced-RBAC default (H4) exists to keep narrow.
Revisit only if operators report real friction.

## 4. Installing k8s-bridge

Two `configSource` modes, selected by the chart's `configSource` value
(`deploy/chart/k8s-bridge/values.yaml`):

### 4.1 `configSource: file` (default — no CRD required)

```bash
helm install k8s-bridge deploy/chart/k8s-bridge \
  --namespace slurm-jobs --create-namespace \
  --set image.repository=<your-registry>/k8s-bridge \
  --set image.tag=<your-tag> \
  -f my-values.yaml
```

`config` in `values.yaml` is rendered into a `ConfigMap` and passed via
`--config`; the controller reads it once at startup (no hot-reload in this
mode — a config-only `helm upgrade` still rolls the pod via a
`checksum/config` annotation).

### 4.2 `configSource: cr` (WorkloadMixing CRs, hot-reloadable)

The default in CR mode is **supervisor mode** (ADR-0015 Phase A): one
controller runs one reconcile loop — its own Slurm client, poll loop, and
health state — per `WorkloadMixing` CR in the release namespace,
started/stopped as CRs come and go. Creating a CR is all a platform user
needs to do; no per-CR Helm release, Deployment, or leader-election Lease.

```bash
helm install k8s-bridge deploy/chart/k8s-bridge \
  --namespace slurm-jobs --create-namespace \
  --set configSource=cr

kubectl apply -f deploy/crd/workloadmixing-sample.yaml   # shape reference
```

The `WorkloadMixing` CRD ships in `deploy/chart/k8s-bridge/crds/` (Helm
installs it automatically on first install; it is **not** touched by
`helm upgrade` — see `docs/upgrade-guide.md`). The controller watches the
CRs and hot-reloads a live config on every spec change; a reload that fails
validation is reported on `status.conditions` and that CR's loop keeps
running on its last-good config. Changing the Slurm *endpoint* fields
(`slurmRestURL`, `slurmUser`, `slurmTokenFile`, `slurmCACertFile`,
`slurmInsecureSkipTLSVerify`, `slurmRequestTimeout`,
`slurmRequestsPerSecond`) restarts that CR's loop instead — those are baked
into the Slurm client at construction, exactly the fields that already
required a controller restart before.

Rules worth knowing in supervisor mode:

- **Conflicts**: two CRs naming the same `slurmRestURL` AND an overlapping
  `partitionName` would double-manage those jobs. The second CR is refused
  — `Ready=False`, reason `ConflictingSpec`, message naming the CR it lost
  to, plus a Warning event — and retried periodically, so deleting or
  fixing either CR resolves it without operator ceremony.
- **Deletion**: deleting a CR stops its loop and nothing else — no
  Slurm-side cleanup (jobs stay as they are; a finalizer-based teardown
  story is a separate roadmap item).
- **Scope**: the watch covers the release namespace only (`POD_NAMESPACE`),
  preserving the chart's namespaced-RBAC secure default. Cluster-wide watch
  is ADR-0015 Phase B and is not shipped.
- **Token/CA paths**: `slurmTokenFile` and `slurmCACertFile` must resolve
  under one of the chart's `allowedTokenPaths` prefixes (default
  `/var/run/secrets/`, `/etc/k8s-bridge/`) — a deploy-time trust anchor the
  CR itself cannot loosen (ADR-0017). The shipped
  `workloadmixing-sample.yaml` already satisfies it.
- **Migration from single-CR mode**: JobSets created before the switch lack
  the per-CR ownership label (`k8s-bridge.x-k8s.io/workloadmixing`) and are
  invisible to supervisor-mode loops — they will not be cleaned up. Drain
  first (let running jobs finish under the old mode) or delete leftover
  `slurm-job-*` JobSets manually after switching.

**Single-CR binding (compatibility path)**: set `workloadmixing.name` (and
optionally `.namespace`) to reproduce exactly the pre-ADR-0015 behavior —
the controller binds to that one CR via `--workloadmixing <ns>/<name>` and
ignores all others:

```bash
helm install k8s-bridge deploy/chart/k8s-bridge \
  --namespace slurm-jobs --create-namespace \
  --set configSource=cr \
  --set workloadmixing.namespace=slurm-jobs \
  --set workloadmixing.name=playground
```

### 4.3 Fields worth setting explicitly

From `internal/config/config.go` / `deploy/chart/k8s-bridge/values.yaml`:

| Field | Notes |
|---|---|
| `config.slurmRestURL` | Chart default is `https://`; plaintext requires the explicit opt-in `config.allowInsecureHTTP: true` — the JWT is bearer-equivalent. |
| `config.slurmUser` | Leave empty (default) unless you provisioned a dedicated low-privilege Slurm REST user; an unknown user 422s every job update. |
| `config.namespace` / `config.localQueue` | Must match a namespace/LocalQueue created in step 2. |
| `config.partitionMappings` | At least one entry required; maps a Slurm partition to a `WorkloadPriorityClass`, optionally overriding `localQueue` per partition (multi-team). |
| `config.slurmd.image` | Must match an `allowedSlurmdImages` prefix (chart value, controller-level trust anchor — deliberately not settable via the CR). |
| `allowedTokenPaths` | Directory prefixes a `WorkloadMixing` CR's `slurmTokenFile` / `slurmCACertFile` may resolve under in supervisor mode (chart value, controller-level trust anchor — deliberately not settable via the CR). Default `/var/run/secrets/`, `/etc/k8s-bridge/` (ADR-0017). |
| `allowInsecureTLS` | Must be `true` before a CR's `slurmInsecureSkipTLSVerify` is honored in supervisor mode (chart value, controller-level trust anchor — deliberately not settable via the CR). Default `false` (ADR-0017). |
| `config.slurmRequestTimeout` | Per-request slurmrestd HTTP timeout, default 30s, bounded 1s–10m; raise for large backlogs (`GET /jobs` has no server-side paging in slurmrestd v0.0.44). |
| `config.slurmRequestsPerSecond` | Client-side token-bucket rate limit toward slurmrestd itself (distinct from `kubeClient.qps/burst`, which limits calls to kube-apiserver). `0` (default) is unlimited; valid range is `[0, 10000]`. Burst is derived automatically as 2x the configured rate (floored at 1). Set this on shared/rate-limited slurmrestd deployments — scale testing found slurmrestd itself, not the Kubernetes API, is the fragile dependency under a bridge burst. |
| `config.maxUserPriority` | Caps user-originated priority requests so no job owner can jump the whole mixed queue. |
| `replicas` / `leaderElect` | `leaderElect: true` (default) makes `replicas > 1` safe — extra replicas sit idle until they win the `coordination.k8s.io` Lease. |
| `podDisruptionBudget.enabled` | Off by default; turn on once you run `replicas > 1`, otherwise a PDB on a single replica just blocks voluntary node drains for no availability benefit. |
| `serviceMonitor.enabled` / `prometheusRule.enabled` | Off by default — both require prometheus-operator CRDs installed first; `helm install` fails outright if the CRD is missing and the flag is on. See `docs/operations.md`. |

## 5. Installing ray-bridge

Two deployment paths exist in this repo: a plain-manifests path
(`deploy/ray-bridge/manifests.yaml` — ServiceAccount, RBAC, ConfigMap,
Deployment, kept legible as the reference shape) and a Helm chart
(`deploy/chart/ray-bridge/`). Both render the same controller.

```bash
# Plain manifests (edit the ConfigMap's config.yaml first — namespace,
# managedClusters, poolMappings, worker image must match your cluster):
kubectl apply -f deploy/ray-bridge/manifests.yaml

# Or via Helm:
helm install ray-bridge deploy/chart/ray-bridge \
  --namespace ray --create-namespace \
  --set image.repository=<your-registry>/ray-bridge \
  --set image.tag=<your-tag> \
  -f my-ray-values.yaml
```

Config fields mirror `internal/rayconfig.Config` — full reference in
`docs/ray-bridge-reference.md`.

### 5.1 The admission webhook — read this before deciding to skip it

`webhook.enabled` is **off by default** in both deployment paths.

**With the webhook disabled, ray-bridge does not enforce admission by
itself.** The pin-gate mechanism (ADR-0013) only gates a RayJob's driver if
its `spec.entrypointResources` requests the pin resource
(`wm-job-<name>`). Without the webhook, that field must be self-declared by
whoever submits the RayJob (see `deploy/ray-bridge/README.md`, section (a)).
**An inner RayJob that omits `spec.entrypointResources` runs immediately —
its driver never waits for a Kueue-admitted worker JobSet, bypassing Kueue
admission entirely**, even though ray-bridge still creates dedicated worker
capacity for it in the background. The webhook is what turns this from "an
honor system" into an enforced gate: it denies unmapped/malformed
submissions outright and auto-injects the pin so a submitter cannot omit it.
This is exactly the caveat the controller itself logs loudly at startup when
the webhook is off (`cmd/ray-bridge/main.go`'s "ADMISSION WEBHOOK DISABLED"
warning).

Enable it via the Helm chart's `webhook.enabled: true`
(`deploy/chart/ray-bridge/values.yaml`), which — unlike the plain-manifests
path — can provision the serving certificate for you:

```bash
helm upgrade --install ray-bridge deploy/chart/ray-bridge \
  --namespace ray --create-namespace \
  --set webhook.enabled=true \
  -f my-ray-values.yaml
```

With `webhook.certManager.enabled: true` (the default whenever
`webhook.enabled` is true), the chart renders a self-signed cert-manager
`Issuer` + `Certificate` for the webhook's serving cert alongside the
`Service` and `MutatingWebhookConfiguration` — no manual cert steps needed,
as long as cert-manager (§1.2) is installed. Set `webhook.certManager.enabled:
false` if you provision the `<release>-webhook-tls` Secret yourself instead.
`webhook.failurePolicy` defaults to `Ignore` (a webhook outage must not
block RayJob submission cluster-wide) — tighten to `Fail` only once the
webhook's own availability is proven.

Whichever path you use, the target namespace must carry the
`ray-bridge.x-k8s.io/managed: "true"` label for the webhook's
`namespaceSelector` to intercept RayJobs in it (`deploy/ray-bridge/webhook.yaml`,
`deploy/chart/ray-bridge/templates/mutatingwebhookconfiguration.yaml`).

## 6. Post-install verification

For k8s-bridge, the chart's own post-install NOTES (rendered by
`helm install`/`helm upgrade`) walk through this; summarized:

```bash
# 1. Confirm the token Secret exists (the chart does not create it).
kubectl get secret slurm-rest-token -n <release-namespace>

# 2. Watch the rollout (Recreate strategy — brief gap is expected).
kubectl rollout status deployment/k8s-bridge -n <release-namespace>

# 3. Confirm readiness (the first /readyz pass needs a completed slurmrestd
#    tick, so allow ~20-35s after the pod starts).
kubectl get pods -n <release-namespace> -l app.kubernetes.io/instance=k8s-bridge
kubectl port-forward -n <release-namespace> svc/k8s-bridge-metrics 8080:8080
curl -s localhost:8080/readyz

# 4. Confirm metrics are served.
curl -s localhost:8080/metrics | grep k8s_bridge_
```

A non-Ready pod after roughly a minute usually means slurmrestd is
unreachable or the token is stale — see `docs/operations.md`'s "Token
expiry" and "slurmrestd TLS-vs-plain-HTTP mismatch" runbooks.

For ray-bridge, the equivalent checks: `kubectl rollout status
deployment/ray-bridge -n ray`, then `curl localhost:8081/readyz` and
`curl localhost:8080/metrics` against the health/metrics ports
(`--health-addr :8081`, `--metrics-addr :8080` by default). `/readyz` gates
on real startup milestones, not a bare ping: it waits for the controller's
RayJob/JobSet informer caches to finish their initial sync, and — when
`--enable-webhook` is set — also waits for the webhook server to actually be
listening (`cmd/ray-bridge/main.go`'s `informer-sync` and `webhook-server`
readyz checks). A ray-bridge pod that never reaches Ready usually means the
apiserver/RBAC is unreachable for the informer LIST+WATCH, or — with the
webhook enabled — the serving certificate is missing/invalid.

Then exercise one workload of each kind end to end: `sbatch --hold` on a
mixing partition (k8s-bridge should discover, translate, and release it —
`docs/architecture.md` §3), and an inner `RayJob` targeting a managed
`RayCluster` (`kubectl -n ray get workloads` should show `ADMITTED=True` for
the dedicated worker JobSet). `experiments/DEMO.md` is the full narrated
walkthrough if you want a scripted end-to-end validation.

## 7. GKE-specific notes

Everything above is cluster-agnostic. On GKE specifically (validated in
`experiments/01-gke-playground`, `experiments/09-scale-gpu-churn`,
`experiments/11-scale-s1-s5`):

- Use a **zonal** cluster with `--release-channel regular` for the smallest
  management footprint; `e2-standard-4` spot nodes are the smallest shape
  this repo has validated the full stack on comfortably.
- **DWS (Dynamic Workload Scheduler) with GPU node pools**: GKE rejects
  `ProvisioningRequest` objects for CPU-only node pools. Node pools must be
  provisioned with `--enable-queued-provisioning` and
  `--reservation-affinity=none`; workloads targeting GKE GPU pools must
  include an explicit toleration for `nvidia.com/gpu:NoSchedule` (GKE's
  default automated GPU node taint). See `docs/operations.md`, "Large-scale
  & DWS production runbook."
- **Memory limits at scale**: at backlogs above ~5,000 held jobs,
  `kueue-controller-manager` and `k8s-bridge` can exceed a `512Mi` limit
  during preemption/reconciliation; configure at least `1Gi` for backlogs up
  to 10k jobs (`resources.limits.memory: 1Gi`, the chart's own default).
- **Teardown discipline**: always run the experiment's `99-teardown.sh`; it
  sweeps orphaned `pvc-*` disks scoped by
  `labels.goog-k8s-cluster-name`/`--zones` (cluster deletion does not remove
  these disks on its own).

_Related reading: `docs/compatibility-matrix.md` (versions), `docs/upgrade-guide.md`
(upgrading an existing install), `docs/operations.md` (day-2 runbooks),
`docs/ray-bridge-reference.md` (full ray-bridge config/annotation reference)._
