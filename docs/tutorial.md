# Tutorial: mixing Slurm and Kubernetes workloads with k8s-bridge

This tutorial walks you through standing up k8s-bridge on a small GKE
cluster and exercising it end to end: submitting a plain Slurm job and
watching it get admitted through Kueue, sharing quota between Slurm and
native Kubernetes workloads, and seeing priority and preemption in action.
It follows the same technical path as
[`experiments/DEMO.md`](../experiments/DEMO.md) (the project's narrated
validation runbook) but is written to be worked through step by step by
someone new to the project, with explanations of the Kubernetes and Kueue
concepts as they come up.

> **Ray is deliberately not covered here.** `RayCluster` admission through
> Kueue is in scope for the project ([ADR-0002](adr/0002-ray-and-inference-scope.md)),
> but the component that automates it — `ray-bridge` — is **experimental**:
> validated only at small scale on `kind`, not on a real multi-node cluster,
> and not exercised by this tutorial's live validation. Walking a newcomer
> through a mechanism at that maturity teaches the wrong thing about how
> settled it is. If you want to explore it anyway, it lives in
> [`experiments/10-ray-bridge/`](../experiments/10-ray-bridge/README.md) with
> its own runnable `kind` setup, and the manual version of the mechanism is
> in [`experiments/03-open-items/`](../experiments/03-open-items/README.md).
> KubeRay is still installed by the bring-up script below, because the rest
> of the stack expects it and other experiments use it.

## What you'll build

One small Kubernetes cluster hosting Slurm (via the Slinky slurm-operator),
Kueue, JobSet, KubeRay, and the k8s-bridge controller — all on the same pool
of nodes. By the end you will have:

- submitted an ordinary `sbatch` job and watched k8s-bridge translate it
  into a Kueue-admitted JobSet of `slurmd` pods, run it, and clean up;
- seen memory, simulated-GPU, and topology-aware (rack/block) placement
  requests flow from Slurm all the way through to Kueue;
- changed a running job's priority and watched Kueue re-rank preemption;
- shared one quota pool between two teams, with idle capacity borrowed and
  reclaimed automatically;
- submitted a plain Kubernetes `Job` and a Slurm job into the **same**
  `ClusterQueue` and watched Kueue treat them identically;
- seen a latency-sensitive serving deployment preempt batch capacity;
- deployed the bridge into the cluster with Helm, the way a real site runs
  it, rather than only as a binary on your laptop; and
- read and understood the `WorkloadMixing` custom resource that configures
  a whole bridge instance.

## Prerequisites

Tools, on your PATH:

- `gcloud`, authenticated against your own GCP project. Every script below
  reads `PROJECT_ID` from your environment and has no default — set it once,
  in the shell you'll be working in:
  ```bash
  export PROJECT_ID=<your-gcp-project>
  ```
- `kubectl`, `helm`, `jq`, `nc`, Go 1.26. (`jq` is needed in section 5; `nc`
  is used in section 2 to check the port-forward actually came up.)

**Where each command runs.** Every command block runs **from the repo root**
in your own shell — `cd` there once and stay put — unless its first line is
`# [slurm-pod]`, which means the shell inside the Slurm login pod you open
in section 4. Several sections interleave the two, so keep both open:
`kubectl` and `gcloud` don't exist inside the Slurm pod, and
`sbatch`/`squeue`/`scontrol`/`sinfo` don't exist on your machine.

Cluster: this tutorial **runs on GKE**, not `kind`. Slinky's Slurm stack
needs real nodes to register dynamic `slurmd` workers against, and the
topology and autoscaling sections exercise real GKE behavior. There is
currently no `kind` path for this full mixed-workload walkthrough — the
experimental Ray side is the one part that does have one, in
[`experiments/10-ray-bridge/README.md`](../experiments/10-ray-bridge/README.md).
A full run here uses small `e2-standard-4` spot nodes.

Before you start, it's worth skimming:

- [`README.md`](../README.md) — the project's framing and vocabulary.
- [`docs/architecture.md`](architecture.md) — the system and code
  architecture, including the lifecycle diagram this tutorial walks through
  step by step.
- [`docs/operations.md`](operations.md) — SLOs, alerts, and day-2 runbooks,
  useful background for the troubleshooting notes below.

Optional but genuinely helpful: open a second terminal and run
`./tools/bridge-top.sh` from the repo root once your cluster exists. It's a
live dashboard of cluster nodes, Kueue queue/quota state, the bridge's
JobSets, and the Slurm queue, refreshed every few seconds — handy for
watching admission happen in near-real time instead of polling `kubectl get`
by hand.

**A note on cost discipline.** Everything here runs on spot nodes, and the
last section of this tutorial is teardown — run it whenever you stop, not
just at the very end, so you don't leave a cluster and its disks running
unattended.

---

## 1. Bring up the stack

We start with one small GKE cluster that hosts Slurm, Kueue, JobSet,
KubeRay, and the bridge controller together — there is no separate Slurm
cluster; everything shares the same pool of nodes.

First, the one variable everything below depends on. The scripts have no
default project and stop immediately without it:

```bash
export PROJECT_ID=<your-gcp-project>
```

Now the bring-up. **The `&&` between these steps is load-bearing, not
style.** Pasting a multi-line block into a shell runs every line regardless
of whether the ones above it failed, and each step here depends on the
previous one having succeeded. Chained, the run stops at the real cause.
Unchained — the way this block used to read — a failed first step surfaced
three commands later as `no matches for kind "ResourceFlavor"`, which looks
like a broken manifest and is really "the cluster was never created".

```bash
MIN_NODES=3 NUM_NODES=3 MAX_NODES=3 ./experiments/01-gke-playground/scripts/01-create-cluster.sh && \
./experiments/01-gke-playground/scripts/02-install-components.sh && \
./experiments/01-gke-playground/scripts/03-configure-queues.sh && \
kubectl apply -f experiments/05-topology/manifests/topology-tas.yaml && \
kubectl apply -f experiments/06-multitenant/manifests/cohort-queues.yaml && \
kubectl apply -f deploy/crd/workloadmixing-crd.yaml
```

Then the simulated topology. This labels **every** node rather than the
first three: if you later resize the pool (section 8 wants a 4th node) or
add a second node pool, an unlabelled node is invisible to Kueue's
Topology-Aware Scheduling, which silently shrinks the topology the later
sections assume.

```bash
i=0; for N in $(kubectl get nodes -o name | cut -d/ -f2); do \
  kubectl label node "$N" example.com/topology-managed=true \
  example.com/block=block-$([ $((i % 4)) -lt 2 ] && echo a || echo b) \
  example.com/rack=rack-$((i % 2 + 1)) --overwrite; i=$((i+1)); done
kubectl get nodes -L example.com/block,example.com/rack
```

**Expected result:** 3 nodes `Ready`, Kueue/JobSet/KubeRay/Slurm pods
`Running` in their namespaces, and the `workloadmixings.k8s-bridge.x-k8s.io`
CRD installed (`kubectl get crd | grep workloadmixing`).

`MIN_NODES=3` matters: the cluster uses the `optimize-utilization`
autoscaling profile, which aggressively removes idle nodes — without a
floor of 3, the autoscaler can shrink the pool before you label it, and
the topology sections later assume all three labeled nodes exist.

**What just happened:** the install script laid down the whole shared
substrate — Kueue as the admission authority, JobSet as the grouped-pod API
both bridges emit, KubeRay for Ray, and the Slinky Slurm operator (which
needs cert-manager for its webhooks). The queue-configuration script created
the `ClusterQueue`/`LocalQueue` objects Kueue uses to track quota, and the
topology manifest registered a `Topology` object so Kueue's
Topology-Aware Scheduling (TAS) knows about racks and blocks. The node
labels at the end (`example.com/block`, `example.com/rack`) *are* the
topology as far as Kueue TAS is concerned — in a real cluster these would
come from the cloud provider's own topology labels; here we set them by
hand to simulate a two-rack layout across three nodes.

If you want to watch this settle live, start `./tools/bridge-top.sh` in a
second terminal now and leave it running for the rest of the tutorial.

Further reading: `experiments/01-gke-playground/README.md`.

---

## 2. Run the bridge in CRD mode

k8s-bridge can read its configuration from a flat file or from a
`WorkloadMixing` custom resource. CRD mode is the in-cluster production
path, and the one this tutorial uses throughout.

> **This section is the development workflow, not the architecture.** The
> binary below runs on *your machine* and reaches the cluster through a
> `kubectl port-forward`. That's convenient for learning and iterating, but
> k8s-bridge is a Kubernetes controller and in a real deployment it runs
> *inside* the cluster. Run it locally and your terminal becomes part of the
> system: close the shell — or let a Cloud Shell session time out — and
> translation stops, leaving submitted Slurm jobs sitting in `JobHeldUser`
> with nothing visibly wrong on the Kubernetes side. Section 2b shows the
> in-cluster deployment. Do one or the other, not both at once.

```bash
# The sed also strips the server-managed metadata: a Secret piped straight
# from `get -o yaml` back into `apply` carries its source resourceVersion, and
# re-running this (§2b repeats it) fails with "the object has been modified".
# Reproduced live 2026-07-27.
kubectl -n slurm get secret slurm-auth-slurm -o yaml \
  | sed -e 's/namespace: slurm/namespace: slurm-jobs/' -e '/resourceVersion:/d' \
        -e '/^  uid:/d' -e '/creationTimestamp:/d' \
  | kubectl apply -f -

# The port-forward runs in the background, which means its failure is silent:
# if slurm-restapi isn't up yet, it exits immediately and the only symptom is
# the bridge below printing "connection refused" forever. Wait for the port to
# actually answer before continuing.
kubectl -n slurm rollout status deploy/slurm-restapi --timeout=180s
kubectl port-forward -n slurm svc/slurm-restapi 6820:6820 &
for i in $(seq 30); do
  nc -z 127.0.0.1 6820 2>/dev/null && { echo "port-forward up"; break; }
  [ "$i" = 30 ] && echo "ERROR: port-forward to slurm-restapi never came up" >&2
  sleep 1
done

kubectl -n slurm exec slurm-controller-0 -c slurmctld -- scontrol token username=root lifespan=14400 | sed 's/SLURM_JWT=//' > /tmp/wm-slurm-token
kubectl apply -f deploy/crd/workloadmixing-sample.yaml
# The sample CR is the IN-CLUSTER shape: an https service DNS name that does
# not resolve from your workstation, and a token path under the cluster's
# Secret mount. Point both at your local setup instead.
# Do this BEFORE starting the binary: endpoint fields (slurmRestURL, token,
# TLS) are baked into the Slurm client at construction time, so in
# single-CR mode changing them later requires restarting the controller.
# Note allowInsecureHTTP: the CRD refuses a plaintext URL without it, by
# design — the Slurm token is bearer-equivalent, so sending it over http is
# something you have to say out loud. It is acceptable here because the
# traffic never leaves your machine (127.0.0.1 via the port-forward).
kubectl -n slurm-jobs patch workloadmixing playground --type=merge \
  -p '{"spec":{"slurmRestURL":"http://127.0.0.1:6820","allowInsecureHTTP":true,"slurmTokenFile":"/tmp/wm-slurm-token"}}'
make build
# --pprof-addr is off by default (heap profiles contain the Slurm token).
# Pass it now if you plan to run the optional scale drill in section 14.
./bin/k8s-bridge --workloadmixing slurm-jobs/playground --pprof-addr 127.0.0.1:6060 &
kubectl get workloadmixing -n slurm-jobs playground -o yaml | grep -A3 conditions   # Ready=True
```

**Expected result:** `status.conditions[type=Ready].status == "True"` on
the `playground` CR.

**What just happened:** the first command copies the Slurm cluster's
auth-key Secret into the bridge's workload namespace (`slurm-jobs`) —
Kubernetes Secrets are namespace-scoped, so the `slurmd` pods the bridge
creates there need their own copy to authenticate back to `slurmctld`. The
`scontrol token` command mints a short-lived JWT the bridge uses to
authenticate to `slurmrestd`, the Slurm REST API. Applying the sample CR
and starting the binary brings the bridge up in CRD mode, pointed at that
one `WorkloadMixing` object.

The `Ready: "True"` condition on the CR is the bridge reporting its own
health back onto the object it's configured from — if the Slurm REST
endpoint or the token goes bad, this condition flips to `False` before
anything else visibly breaks, which is the first thing worth checking if a
later step seems stuck. If you have `:8080/healthz` and `:8080/metrics`
port-forwarded, those carry the same signal (and are what Prometheus would
scrape in a real deployment).

---

## 2b. The same bridge, running inside the cluster

Everything you just did works identically with the bridge deployed as a
Kubernetes `Deployment` — no port-forward, no laptop in the data path, and
the controller comes back on its own if its node dies. That is the shape a
real site runs, and it's worth doing once so the local binary above doesn't
leave the wrong mental model behind.

Do this **instead of** section 2's binary, or stop that one first:

```bash
pkill -f 'bin/k8s-bridge' || true
```

The chart deliberately does not create credentials for you — each of these
is a namespace-scoped decision an operator should make explicitly rather
than a default a chart can guess:

```bash
# 1. slurmd's auth key, in the bridge's own namespace. Kubernetes Secrets are
#    namespace-scoped and the bridge does not copy credentials between
#    namespaces. Skip this and the slurmd pods it creates hang in
#    ContainerCreating with FailedMount — and the bridge's own logs say
#    nothing about it, because the failure is the kubelet's, not the bridge's.
kubectl create namespace slurm-jobs --dry-run=client -o yaml | kubectl apply -f -
kubectl -n slurm get secret slurm-auth-slurm -o yaml \
  | sed -e 's/namespace: slurm/namespace: slurm-jobs/' -e '/resourceVersion:/d' \
        -e '/^  uid:/d' -e '/creationTimestamp:/d' \
  | kubectl apply -f -

# 2. the Slurm REST token, as the Secret the chart mounts (values.yaml:
#    slurmTokenSecret, default "slurm-rest-token"). lifespan is in seconds;
#    14400 = 4 hours, fine for a tutorial and not for production — see
#    docs/operations.md for rotation.
kubectl -n slurm exec slurm-controller-0 -c slurmctld -- \
  scontrol token username=root lifespan=14400 | sed 's/SLURM_JWT=//' > /tmp/wm-slurm-token
kubectl -n slurm-jobs create secret generic slurm-rest-token \
  --from-file=token=/tmp/wm-slurm-token --dry-run=client -o yaml | kubectl apply -f -

# 3. re-aim the CR at the in-cluster endpoint. This is the exact reverse of
#    the patch in section 2: 127.0.0.1 means the pod itself once the bridge
#    runs inside the cluster. allowInsecureHTTP is still needed because the
#    Slinky slurmrestd Service does not terminate TLS by default; that is
#    acceptable only because the traffic stays inside the cluster network.
kubectl -n slurm-jobs patch workloadmixing playground --type=merge -p '{"spec":{
  "slurmRestURL":"http://slurm-restapi.slurm.svc.cluster.local:6820",
  "allowInsecureHTTP":true,
  "slurmTokenFile":"/var/run/secrets/slurm/token"}}'

# 4. install the chart in CR mode. configSource defaults to `file`; left at
#    that default the controller ignores every WorkloadMixing CR and you get
#    a pod that is Running, healthy, and does nothing at all.
helm upgrade --install k8s-bridge deploy/chart/k8s-bridge \
  --namespace slurm-jobs --set configSource=cr

kubectl -n slurm-jobs rollout status deploy/k8s-bridge --timeout=180s
kubectl get workloadmixing -n slurm-jobs playground -o yaml | grep -A3 conditions
```

**Expected result:** `deploy/k8s-bridge` reaches 1/1 `Ready`, and the CR's
`Ready` condition is `True` — the same signal as section 2, now reported by
a pod instead of your terminal.

**What just happened:** nothing about the bridge's behaviour changed; only
where it runs. Every later section of this tutorial works unmodified, with
one exception — the optional `pprof` step in the scale drill targets
`127.0.0.1:6060`, which now needs a `kubectl port-forward` to the pod.

One consequence worth knowing: leader election is on by default in *both*
modes, using the same `k8s-bridge-leader` Lease in the same namespace. If
you accidentally leave the local binary running and also install the chart,
they won't corrupt each other — but only one of them is actually working,
and it's whichever won the Lease. `kubectl get lease -n slurm-jobs
k8s-bridge-leader -o jsonpath='{.spec.holderIdentity}'` says which.

With `configSource=cr` and no `workloadmixing.name` set, the controller runs
in supervisor mode: it watches *every* `WorkloadMixing` in its namespace and
runs one independent loop per CR. Onboarding a second team is then a
`kubectl apply`, not a second Helm release.

Further reading: [`deploy/chart/k8s-bridge/README.md`](../deploy/chart/k8s-bridge/README.md),
[`docs/installation.md`](installation.md).

---

## The WorkloadMixing custom resource

Before going further, it's worth understanding the object you just applied.
**One `WorkloadMixing` CR configures one entire bridge instance for one
Slurm cluster** — everything the bridge needs to talk to Slurm, decide how
jobs map onto Kueue, and shape the pods it creates lives in this single
object. There's no separate config file, ConfigMap, or Helm values layer to
reconcile against it in CRD mode: this CR *is* the live configuration, and
editing it hot-reloads the bridge without a pod restart (more on that in
the next section).

Here's the sample CR this tutorial applies in section 2
(`deploy/crd/workloadmixing-sample.yaml`):

```yaml
apiVersion: k8s-bridge.x-k8s.io/v1alpha1
kind: WorkloadMixing
metadata:
  name: playground
  namespace: slurm-jobs
spec:
  # Local dev used a plaintext localhost endpoint; that now requires an explicit
  # opt-in (the CRD's CEL rule rejects http:// otherwise). In-cluster, use https.
  slurmRestURL: "http://slurm-restapi.slurm.svc.cluster.local:6820"
  allowInsecureHTTP: true
  slurmTokenFile: "/tmp/wm-slurm-token"
  localQueue: "team-a"
  pollInterval: "10s"
  maxUserPriority: 10000 # cap user-originated priority requests (security audit H1)
  partitionMappings:
    - partitionName: "mixing"
      workloadPriorityClass: "normal-priority"
    - partitionName: "mixing-high"
      workloadPriorityClass: "high-priority"
    - partitionName: "mixing-gpu"
      workloadPriorityClass: "gpu-priority"
  slurmd:
    image: "ghcr.io/slinkyproject/slurmd:26.05-ubuntu26.04"
    confServer: "slurm-controller.slurm.svc.cluster.local:6817"
    authSecretName: "slurm-auth-slurm"
  topology:
    preferredLevel: "example.com/block"
```

Reading it top to bottom, by concern:

- **Talking to Slurm.** `slurmRestURL` and `slurmTokenFile` tell the bridge
  where `slurmrestd` lives and how to authenticate to it. The optional
  `slurmUser` field is left unset on purpose: empty means slurmrestd acts
  as the user the JWT was minted for. Only set it to a user that actually
  exists in your Slurm cluster — a non-existent user makes every job
  update fail with "Invalid user id", and the bridge can then never
  release held jobs.
  `allowInsecureHTTP` is a deliberate, explicit opt-in for plaintext
  `http://` endpoints (useful for a local playground; production should use
  `https://`, and the CRD rejects `http://` otherwise via a validation
  rule). `pollInterval` is how often the bridge ticks against `slurmrestd`
  in the absence of a watch-driven nudge (Slurm's REST API has no watch
  primitive, so polling is the floor).
- **Partition → priority mapping.** `partitionMappings` is the routing
  table: each Slurm partition maps to a Kueue `WorkloadPriorityClass`, so a
  job's partition determines how it's ranked for admission and preemption.
  An entry can also override which Kueue `localQueue` that partition's jobs
  land in (useful for routing different partitions to different teams —
  section 9 below exercises this). `maxUserPriority` is a safety cap so no
  job owner can request a priority high enough to jump the whole shared
  queue.
- **Kueue placement.** `localQueue` is the default Kueue `LocalQueue` this
  bridge instance's JobSets are submitted into (in the namespace the CR
  lives in), unless a partition mapping overrides it.
- **The slurmd pod template.** The `slurmd` block controls what the pods
  the bridge creates actually look like: which container image runs
  `slurmd` (must match an operator-configured allow-list of trusted
  images), which `slurmctld` endpoint they register against
  (`confServer`), and which Secret carries the cluster's auth key
  (`authSecretName` — the one you copied into the workload namespace in
  section 2).
- **Topology translation.** `topology.preferredLevel` tells the bridge
  which node-label key represents the topology domain Slurm's
  `--switches` locality hint should map onto for Kueue's Topology-Aware
  Scheduling (see section 6). This is what lets a Slurm concept
  (network-switch locality) drive a Kubernetes-native scheduling
  mechanism without Slurm ever knowing Kubernetes topology exists.

This sample is deliberately minimal — enough fields to run the rest of this
tutorial, not a tour of every option. For the complete, field-by-field
reference (every optional field, validation rule, and default), see
[`docs/custom-resource.md`](custom-resource.md).

---

## 3. Control-plane basics: leader election, watches, events, hot-reload

Before running jobs, it's worth understanding a few things about how the
bridge itself behaves as a controller, since they explain what you'll see
(and not see) in the sections that follow.

### 3a. Leader election

```bash
# works whether the bridge is the local binary from section 2 or the Helm chart —
# either way it creates the same Lease object in its config namespace
kubectl get lease -n slurm-jobs k8s-bridge-leader -o yaml | grep -A2 holderIdentity
```

That `Lease` is a real `coordination.k8s.io` object. If the bridge were
scaled to two replicas (only possible with the Helm chart, not the local
binary from section 2), only the Lease holder would tick; the other
replica sits idle. This is what makes running a hot standby safe. Leader
election can be disabled with `--leader-elect=false`, but only makes sense
for a single-replica local/dev run without the Lease RBAC in place.

### 3b. Watch-driven latency

```bash
kubectl -n slurm exec deploy/slurm-login-slinky -- \
  sbatch --partition=mixing --ntasks=1 --wrap='sleep 10'
curl -s http://127.0.0.1:8080/metrics | grep k8s_bridge_tick_trigger_total
```

**What to look for:** `k8s_bridge_tick_trigger_total{source="watch"}`
incrementing right after the JobSet or Workload changes — faster than the
next `source="timer"` tick would have fired. A JobSet-ready or
Workload-admitted event nudges the reconcile loop immediately; the timer
stays as the unconditional floor if watches ever lag or disconnect. In
practice this means the job gets admitted and released noticeably faster
than one full `pollInterval`.

### 3c. Kubernetes Events

```bash
kubectl describe jobset -n slurm-jobs $(kubectl get jobset -n slurm-jobs -o jsonpath='{.items[0].metadata.name}') | tail -15
```

**What to look for:** the `Events:` section at the bottom —
`Created`/`Released` Normal events, or `JobSetFailed`/`TranslationFailed`
Warning events if something went wrong. This works the same way whether
the bridge is running as the local binary or the Helm chart, since the
Recorder posts Events to the API server either way. The practical benefit:
someone who only knows `kubectl describe` gets the same story a
Kubernetes-native operator would, without needing to read the bridge's own
logs.

### 3d. Config hot-reload, no restart

```bash
kubectl get workloadmixing -n slurm-jobs playground -o yaml > /tmp/wm-before.yaml
kubectl patch workloadmixing -n slurm-jobs playground --type merge \
  -p '{"spec":{"maxUserPriority":5000}}'
# local binary from section 2: watch its stdout in that terminal for the reload log;
# Helm-chart deploy: kubectl -n slurm-jobs logs deploy/k8s-bridge | grep -i "config reload\|spec change"
```

**What to look for:** the bridge picks up the new `maxUserPriority` without
a pod restart — no rolling update, no dropped ticks. This only works in CRD
mode: file-based config loads once at startup by design, so a config-only
`helm upgrade` in file mode needs the chart's `checksum/config` annotation
to force a restart instead.

**Expected result:** the reconcile loop snapshots its config once per tick,
so a reload landing mid-tick never mixes fields from two config
generations — the next tick after the patch uses the new value.

---

## 4. Your first Slurm job through Kueue

This is the core loop: a researcher submits a completely ordinary Slurm
job, with no special flags, and the whole Kubernetes admission chain
happens invisibly underneath.

Open the Slurm pane now and **leave it open** — sections 5, 7 and 13 all
come back to it:

```bash
kubectl -n slurm exec -it deploy/slurm-login-slinky -- bash
```

```bash
# [slurm-pod]
sbatch --partition=mixing --ntasks=2 --wrap='srun hostname'   # NO --hold needed
squeue -o "%i %T %k"    # watch it move: held -> quota -> provisioning
```

There's nothing bridge-specific about this `sbatch` call — no `--hold`,
nothing bridge-aware. A JobSubmit plugin auto-holds the job the moment it
hits the `mixing` partition, which is what gives the bridge a window to
translate it before Slurm would otherwise try to schedule it.

**What to watch, in order** (in `bridge-top.sh`, or with plain `kubectl
get -w`):

1. A new Kueue `Workload` appears, `ADMITTED: false`.
2. A `slurm-job-<id>` JobSet appears with the right pod count.
3. If quota required a new node, the autoscaler brings one up.
4. The `Workload` flips to `ADMITTED: true`.
5. Back in the Slurm pane, `squeue` shows the job leave `Hold` and move to
   `RUNNING`, then disappear as it completes.

**What just happened:** that JobSet's pods run `slurmd` — they register as
dynamic Slurm nodes. The bridge saw that registration, lifted the hold,
and Slurm scheduled the job onto its own dedicated nodes exactly as it
would on bare metal. When the job finishes, the bridge deregisters the
nodes and deletes the JobSet, returning the capacity to the shared pool.

**Try the negative case too**, in the same session:

```bash
# [slurm-pod]
sbatch --partition=mixing --array=1-5 --wrap=hostname         # clean rejection
```

**Expected result:** immediate `sbatch` rejection with a clear message —
array jobs are rejected at submit time by the lua plugin, not silently
dropped later.

Further reading: [`docs/architecture.md`](architecture.md) section 3 (the
lifecycle, step by step), `experiments/01-gke-playground/manifests/slurm-values.yaml`
(the lua plugin).

---

## 5. Resource requests: memory, simulated GPUs, wall-clock limits

Resource requests translate too — memory per CPU, and even GPUs, without
needing real GPU hardware.

```bash
# [slurm-pod]
# memory: pods sized to --mem-per-cpu, node advertises RealMemory to match
sbatch --partition=mixing --ntasks=1 --mem-per-cpu=2G --wrap='sleep 20'

# GPU simulation (no hardware, full chain). THREE prerequisites, all required —
# skipping any one leaves the job pending forever with a misleading reason.
#
# 1. fake the extended resource on EVERY node you want to be eligible. Kueue's
#    topology-aware scheduling is all-or-nothing per workload, so a single
#    faked node is usually not enough once the rest of the stack is running.
for N in $(kubectl get nodes -o name | cut -d/ -f2); do
  kubectl patch node "$N" --subresource=status --type=merge \
    -p '{"status":{"capacity":{"nvidia.com/gpu":"2"},"allocatable":{"nvidia.com/gpu":"2"}}}'
done

# 2. give the ClusterQueue nvidia.com/gpu quota — without it Kueue reports
#    "resource nvidia.com/gpu unavailable in ClusterQueue" and never admits.
#    (Adjust the queue name to the one your CR's localQueue points at.)
kubectl get clusterqueue team-a -o json \
  | jq '.spec.resourceGroups[0].coveredResources += ["nvidia.com/gpu"]
        | .spec.resourceGroups[0].flavors[].resources += [{"name":"nvidia.com/gpu","nominalQuota":"2"}]' \
  | kubectl apply -f -

# 3. the SLURM CLUSTER's gres.conf needs a device-file entry. On Slurm 26.05 a
#    count-only "Name=gpu" is NOT enough: slurmd verifies the actual device
#    count, reports 0, and slurmctld puts the freshly registered dynamic node
#    into INVALID_REG + DRAIN with "gres/gpu count reported lower than
#    configured (0 < 1)". The job then sits at ReqNodeNotAvail forever. Set
#    this in the Slurm chart's configFiles (see
#    experiments/01-gke-playground/manifests/slurm-values.yaml) — NOT in a
#    bridge-side ConfigMap: the bridge deliberately no longer mounts one
#    (see the NOTE in internal/translate/translate.go).
#      gres.conf: |
#        Name=gpu File=/dev/null
```

```bash
# [slurm-pod]
# NOTE the trailing sleep. With a bare `srun echo` the whole lifecycle —
# JobSet created, node registered, job run, JobSet cleaned up — takes about
# 7 seconds, and the dynamic node is gone before you can type sinfo.
sbatch --partition=mixing --gres=gpu:1 --wrap='srun echo GPU job; sleep 90'
sinfo -N -o "%N %T %G"  # dynamic node advertises gpu:1 (idle, then allocated)

# --nodes / --ntasks-per-node and wall-clock leak guard:
sbatch --partition=mixing --nodes=2 --ntasks-per-node=2 --time=10 --wrap='sleep 30'
```

**On the GPU step specifically:** this is a fully simulated GPU — no
hardware, no real accelerator involved. We patch a fake `nvidia.com/gpu`
resource onto a node; Kueue quota and the scheduler treat it exactly like
a real device. On the Slurm side, GRES verification needs a real device-file
entry, which is why the Slurm cluster's own `gres.conf` points at
`/dev/null` — the bridge deliberately does not mount one. Every step of
the chain except the actual CUDA workload is real, which makes this a
useful way to exercise GPU-shaped scheduling logic without provisioning
GPU nodes.

**Expected result:** `sinfo -N -o "%N %G"` shows `gpu:1` on the dynamic
node; the job runs and completes.

---

## 6. Topology-aware placement

Slurm's `--switches` flag — rack/network locality — flows all the way
through to Kueue's Topology-Aware Scheduling (TAS). Slurm never has to know
Kubernetes topology exists; its dynamic nodes just end up co-located.

```bash
# [slurm-pod]
sbatch --partition=mixing --ntasks=2 --switches=1 --wrap='sleep 30'
# both slurmd pods land in ONE rack (dashboard TOPOLOGY panel);
# jobs without --switches get best-effort block locality
```

**What to look for:** in `bridge-top.sh`'s `TOPOLOGY` panel, which groups
nodes by block/rack and shows live pod placement — both pods for this job
land under the same rack.

**Expected result:** both `slurmd` pods scheduled onto nodes sharing one
`example.com/rack` label. A job requesting more capacity than any single
rack holds stays inadmissible, with a topology message like `doesn't allow
to fit any of N pod(s)` — worth trying deliberately as a negative case (see
`experiments/05-topology/README.md`, scenario C).

Further reading: [`docs/architecture.md`](architecture.md) section 4a,
`experiments/05-topology/README.md`.

---

## 7. Priority is mutable, even while a job is running

Priorities aren't fixed at submit time. A researcher — or an admin — can
raise or lower a job's priority after submission, even while it's running,
and Kueue immediately re-ranks who gets preempted first.

```bash
# [slurm-pod]
# --parsable makes sbatch print the bare job id, so it can be captured rather
# than copied by hand. This matters: the older form of this block spelled the
# placeholder <ID>, and pasting that into a shell makes bash treat < and > as
# redirections — you get "No such file or directory" for a file named ID,
# which reads like a Slurm error and isn't one.
JOBID=$(sbatch --parsable --partition=mixing --ntasks=1 --time=10 --wrap='sleep 180')
echo "submitted ${JOBID}"
scontrol update job "${JOBID}" priority=700   # lua turns this into a directive
scontrol show job "${JOBID}" | grep AdminComment   # wm:prio-applied=700
```

```bash
# host: the same number, now on the Kubernetes side
kubectl get workload -n slurm-jobs -o jsonpath='{.items[0].spec.priority}'  # 700
```

**What to look for:** the `AdminComment` field acknowledging the applied
priority, then the `Workload.spec.priority` value on the Kubernetes side
matching it.

**Expected result:** `wm:prio-applied=700` in the Slurm comment; `700`
reflected on the `Workload` object.

**Why this needs a workaround:** Slurm's own `priority` field can't be the
data channel here — it's scheduler-owned and gets reset — so the lua
plugin intercepts the update and writes a directive (the `AdminComment`)
instead, which the bridge reads and applies to the `Workload`.

Further reading: [`docs/architecture.md`](architecture.md) section 4b.

---

## 8. Multi-team quota sharing: cohorts, borrowing, reclaim

Two teams can share a quota pool. Idle capacity gets lent out
automatically, and reclaimed the moment the owning team actually needs
it — and this composes with topology-aware scheduling too.

> **This section needs a 4th node.** The filler is a 4-pod gang at 2 CPU each
> (`parallelism: 4`, `requests.cpu: "2"`), Kueue's topology-aware scheduling
> admits a workload all-or-nothing, and an `e2-standard-4` has room for exactly
> one such pod once the shared stack is running. On the 3-node cluster section 1
> pins, Kueue reports `topology "simulated-dc" allows to fit only 2 out of 4
> pod(s)` and the filler never borrows. Either bring up a 4th node for this
> section or shrink the filler's `parallelism` to match the room you have.
> Verified live 2026-07-25.
>
> ```bash
> source ./experiments/01-gke-playground/scripts/00-env.sh   # exports ZONE, CLUSTER_NAME
> gcloud container clusters resize "${CLUSTER_NAME}" --num-nodes=4 --zone "${ZONE}" --quiet
> # then re-run section 1's labelling loop so the new node joins the topology
> ```

```bash
kubectl create -f experiments/06-multitenant/manifests/teamb-filler-job.yaml
kubectl get clusterqueue team-b -o jsonpath='{.status.flavorsUsage[0].resources[0]}'  # borrowed>0
```

```bash
# [slurm-pod]
sbatch --partition=mixing --ntasks=6 --wrap='sleep 120'   # team-a reclaims
```

```bash
kubectl get events -n default | grep -i "reclamation within the cohort"
```

**What to look for:** the `clusterqueue team-b` borrowed-quota value going
above zero, then the eviction event once team-a reclaims — the event text
names the preemptor/preemptee paths explicitly, worth reading in full.

**Expected result:** team-b's filler job borrows team-a's idle CPU;
team-a's submission evicts team-b's borrowing workload wholesale — reclaim
is all-or-nothing per workload, not partial.

Further reading: `experiments/06-multitenant/README.md`.

---

## 9. Per-partition queues: routing partitions to different teams

Every Slurm partition can target its own Kueue queue instead of sharing one
global one — this is the config knob a real multi-team HPC site would use,
mapping each partition to the team that owns it (via the
`partitionMappings` field you saw in the CR section above).

```bash
# the bridge's config maps partitionName -> localQueue per entry
# (config.PartitionMapping.LocalQueue overrides the global LocalQueue)
kubectl get workloadmixing -n slurm-jobs playground -o jsonpath='{.spec.partitionMappings}' | jq .

# a job on a partition with its own localQueue override lands in THAT queue,
# not the global one, with no per-job flag needed
kubectl -n slurm exec deploy/slurm-login-slinky -- \
  sbatch --partition=mixing --ntasks=1 --wrap='sleep 20'
kubectl get workload -n slurm-jobs -o jsonpath='{.items[-1:].spec.queueName}'
```

**What to look for:** the `Workload.spec.queueName` value matching the
partition's configured `localQueue` (or falling back to the CR's global
`localQueue` for partitions without an override).

**Expected result:** a partition with a `localQueue` override routes its
JobSets to that queue; a partition without one falls back to the global
queue — both observable from the same `Workload.spec.queueName` field, no
separate mechanism to reason about.

Further reading: [`docs/architecture.md`](architecture.md) section 5
(config surface), `internal/config/config.go`
(`PartitionMapping.LocalQueue`, `Config.LocalQueueFor`).

---

## 10. Mixing Kubernetes-native and Slurm workloads in one queue

This is the scenario that makes the whole point of the project concrete:
two completely different workload systems, same admission authority, same
numbers.

> **The queue label has to match the bridge's `localQueue`, or the two
> workloads never actually compete.** `kueue-batch-job.yaml` ships targeting
> LocalQueue `main` (→ ClusterQueue `main-queue`) because its own experiment
> README uses it standalone, while the sample `WorkloadMixing` CR routes Slurm
> jobs to `team-a`. Applied unmodified, you get two workloads in two separate
> ClusterQueues with separate quota — which does not demonstrate anything this
> section claims. The `sed` below retargets it. Verified live 2026-07-26.

```bash
# a native Kubernetes batch Job, retargeted at the SAME LocalQueue the bridge
# uses (LocalQueue team-a already exists in `default` — cohort-queues.yaml
# creates it there as well as in slurm-jobs)
sed 's|kueue.x-k8s.io/queue-name: main$|kueue.x-k8s.io/queue-name: team-a|' \
  experiments/01-gke-playground/workloads/kueue-batch-job.yaml | kubectl create -f -
kubectl get jobs -n default   # note the generated name, e.g. sample-batch-xxxxx

# a Slurm job contending for the SAME quota, submitted the ordinary way
kubectl -n slurm exec deploy/slurm-login-slinky -- \
  sbatch --partition=mixing --ntasks=2 --wrap='sleep 90'
```

One plain Kubernetes `Job`, one Slurm job, same `ClusterQueue`, same quota.
Kueue doesn't care which system submitted the workload — first up gets the
resources; if both want more than the pool has, whichever has priority
wins, exactly as it would if both were native Kubernetes Jobs.

**What to look for:** in `bridge-top.sh`'s `WORKLOADS` panel, both objects
show up as `Workload` resources in the same `ClusterQueue` — one backed by
a plain `batch/v1.Job`, the other by the bridge's JobSet. If quota is
tight, `kubectl describe workload <name>` shows the pending condition's
message explaining which one is waiting and why.

**Expected result:** both workloads reserve quota from the same
`ClusterQueue`; whichever fits first (or has priority) admits first. Both
the Slurm job's completion and the Kubernetes Job's completion free
capacity back to the same pool, visible in the same panel.

Further reading: `experiments/01-gke-playground/workloads/kueue-batch-job.yaml`,
[`docs/architecture.md`](architecture.md) section 2 (the system overview
diagram — this is literally the picture in that diagram).

---

## 11. Serving and batch: two-tier admission

Inference serving has different rules from batch work — it's
latency-sensitive, so instead of queueing and waiting, a high-priority
serving scale-up preempts batch outright to get capacity immediately.

```bash
kubectl apply -f experiments/04-serving-admission/manifests/serving-queued.yaml
kubectl scale deployment queued-inference --replicas=3
# serving preempts batch; batch re-admits on borrowed capacity (events tell the story)
```

**What to look for:** eviction events (`kubectl get events -n default |
grep -i preempt`), or the equivalent Grafana panel if you have one wired
up — batch pods get evicted the moment the serving replica needs the CPU.

**Expected result:** `queued-inference` scales to 3 Ready pods quickly; a
concurrently running batch Job/JobSet gets suspended (evicted) to make
room, then re-admits once capacity frees up.

Further reading: `experiments/04-serving-admission/README.md`.

---

## 12. Optional: DWS queued provisioning

Queued provisioning inverts the usual failure mode: instead of a workload
failing because capacity is absent, Kueue asks GKE *for* capacity and holds
the workload inadmissible until a `ProvisioningRequest` comes back. That's
the mechanism you want for scarce accelerators — jobs can queue for hardware
that does not exist yet.

> **This is the one section of the tutorial that costs real money.**
> `g2-standard-4` carries an NVIDIA L4, billed as GPU. Everything else in
> this tutorial runs on spot CPU nodes with *simulated* accelerators
> (section 5). Skip this on a first pass, and if you do run it, make sure
> section 16's teardown happens even if you stop halfway.

**13a. The node pool.** `--enable-queued-provisioning` is the load-bearing
flag — `--flex-start` alone is not enough for the Cluster Autoscaler to
service Kueue's `ProvisioningRequest` objects. `--reservation-affinity=none`
is mandatory or the create call is rejected outright.

```bash
source ./experiments/01-gke-playground/scripts/00-env.sh   # exports ZONE, CLUSTER_NAME

# ZONE is parameterised deliberately: L4 availability is zone-specific, so a
# hardcoded zone fails for anyone running elsewhere. Override it before
# sourcing if needed, e.g. export ZONE=us-central1-a.
gcloud container node-pools create dws-gpu-qp --cluster "${CLUSTER_NAME}" --zone "${ZONE}" \
  --machine-type g2-standard-4 --accelerator type=nvidia-l4,count=1 \
  --num-nodes 0 --enable-autoscaling --min-nodes 0 --max-nodes 2 \
  --enable-queued-provisioning --reservation-affinity=none
```

**13b. The Kueue admission chain.** This ships as one file — the
`ProvisioningRequestConfig` → `AdmissionCheck` → `ClusterQueue` stack that
the previous version of this section left as an exercise:

```bash
kubectl apply -f experiments/11-scale-s1-s5/manifests/dws-gpu-qp.yaml
kubectl get provisioningrequestconfig,admissioncheck
kubectl get clusterqueue dws-gpu-cq
```

**13c. Point a Slurm partition at it.** This is what makes DWS a bridge
story rather than a Kubernetes one: the researcher keeps typing `sbatch`,
and GKE queues up accelerator capacity underneath them.

```bash
kubectl -n slurm-jobs patch workloadmixing playground --type=merge -p '{"spec":{"partitionMappings":[
  {"partitionName":"mixing","workloadPriorityClass":"normal-priority"},
  {"partitionName":"mixing-gpu","workloadPriorityClass":"gpu-priority","localQueue":"dws-gpu-lq"}]}}'
```

```bash
# [slurm-pod]
sbatch --partition=mixing-gpu --gres=gpu:1 --wrap='srun nvidia-smi'
```

```bash
kubectl get provisioningrequests -A
kubectl get workload -n slurm-jobs -o 'custom-columns=NAME:.metadata.name,QUEUE:.spec.queueName,ADMITTED:.status.conditions[?(@.type=="Admitted")].status'
```

**What to look for:** a `ProvisioningRequest` created in response to an
ordinary `sbatch`, and the resulting `Workload`'s `queueName` reading
`dws-gpu-lq` — the Slurm user never named a Kubernetes queue.

**Expected result:** the `ProvisioningRequest` reaches `Accepted: True`
(`Reason: SuccessfullyQueued`) and the workload stays inadmissible until GKE
reports capacity. This chain was validated to that point on 2026-07-08;
physical L4 boot and job completion were **not** scoped in that run, so
treat anything past `Accepted` as unverified.

> **`PROVISIONED` does not mean "pods are running".** Once the request flips
> to `PROVISIONED` the VM has joined the cluster, but GPU pods can stay
> `Pending` for several more minutes: GKE does not advertise
> `nvidia.com/gpu` to the scheduler until the NVIDIA driver installer
> DaemonSet finishes on the new node. This gap looks like a stuck bridge and
> isn't one. Watch the capacity appear with:
> `kubectl get nodes -o custom-columns=NAME:.metadata.name,GPU:.status.allocatable.nvidia\.com/gpu`

**A known limit worth stating plainly:** point this same chain at a CPU-only
node pool and the `ProvisioningRequest` fails with `Resize requests without
accelerators are not supported`. That's GKE policy, not a bridge bug — and
it's why 13a provisions an accelerator pool rather than the cheaper
`e2-standard-2` this section used to suggest.

**A working alternative** that doesn't need GPU hardware — Custom Compute
Classes (`experiments/08-ccc-dws/`): declarative machine-family preference
lists (spot E2 → on-demand E2, or on-demand N2) with GKE auto-creating
matching node pools on demand.

```bash
kubectl apply -f experiments/08-ccc-dws/manifests/compute-classes.yaml
kubectl get pods -o wide -l job-name=ccc-probe-econo   # spot E2 node
kubectl get nodes -L machine-family,cloud.google.com/gke-spot,compute-class
```

Further reading: `experiments/08-ccc-dws/README.md`.

---

## 13. Failure handling: what happens when a JobSet dies

Failure handling matters as much as the happy path. If a JobSet dies — it
blows a deadline, or can never pull its image before the Slurm nodes
register — the bridge detects the `Failed` condition and fails the
corresponding Slurm job with a clear reason, instead of leaving it pending
forever.

> **This one cannot be provoked on demand.** A JobSet's
> `spec.replicatedJobs` is immutable, so neither patching
> `activeDeadlineSeconds` nor swapping in an unpullable image is accepted by
> the apiserver, and force-deleting the pod just makes JobSet restart it. The
> mechanism is validated; treat the commands below as "what you would see"
> rather than a reproduction. Verified live 2026-07-26.

**First, turn on retention — otherwise there is nothing left to look at.**
By default the bridge deletes the JobSet in the same tick that it fails the
Slurm job, which is correct behaviour (the capacity should go back to the
pool immediately) and terrible for post-mortems: both commands below return
`No resources found` moments after a real failure, which reads as "failure
handling is broken" rather than "failure handling worked, then tidied up".
`failedJobSetRetention` keeps the corpse around for exactly this purpose.

```bash
kubectl -n slurm-jobs patch workloadmixing playground --type=merge \
  -p '{"spec":{"failedJobSetRetention":"1h"}}'
```

This hot-reloads — no restart. Retaining costs nothing but an API object: a
JobSet is only ever retained *after* it reached a terminal condition, so its
pods are finished and Kueue has already released its quota.

```bash
# now the failure leaves evidence behind
kubectl get jobset -n slurm-jobs          # Failed / DeadlineExceeded, retained
kubectl get jobset -n slurm-jobs -o jsonpath='{.items[*].metadata.annotations.k8s-bridge\.x-k8s\.io/retain-until}'
kubectl get events -n slurm-jobs \
  --field-selector reason=JobSetFailed    # "Slurm job <id> failed: JobSet reported Failed (<reason>)"
kubectl get events -n slurm-jobs \
  --field-selector reason=RetainedForInspection
```

```bash
# [slurm-pod]
squeue -o "%i %T %r"                       # the job leaves PENDING — cancelled, not hanging
# take the id from the line above; quoted, so no shell redirection happens
JOBID=12345
scontrol show job "${JOBID}" | grep Comment   # Comment=wm: JobSet failed: <reason>
```

**What to look for:** the JobSet's `Failed` status and, right beside it,
the Slurm job leaving `PENDING` with the failure reason propagated as its
comment — the bridge closes the loop end to end, with no human in the
middle.

**Expected result:** on the next tick the bridge copies the JobSet's
failure message onto the Slurm job (`wm: JobSet failed: <reason>`), cancels
the job, increments the `k8s_bridge_jobs_failed_total` metric, and emits a
`JobSetFailed` Warning Event — no manual `scancel` needed. A related path
covers a JobSet that *disappears* entirely rather than failing: its Slurm
job is cancelled as orphaned after a grace period.

With retention on, the JobSet additionally carries a
`k8s-bridge.x-k8s.io/retain-until` annotation and a `RetainedForInspection`
event, and is collected by ordinary cleanup once that deadline passes. The
Slurm-side node records are **not** retained — they are deleted immediately
either way, because a lingering node record would leave `slurmctld`
advertising capacity that no longer exists. Retention preserves the
evidence, not the resources.

Turn it back off when you're done poking at it:

```bash
kubectl -n slurm-jobs patch workloadmixing playground --type=merge \
  -p '{"spec":{"failedJobSetRetention":"0s"}}'
```

Further reading: [`docs/operations.md`](operations.md), the runbook titled
"JobSet dead, Slurm job pending forever";
[`docs/custom-resource.md`](custom-resource.md) for the field itself.

---

## 14. Optional: a scale drill

Worth trying once you're comfortable with the basics, to see what the
bridge looks like under load rather than a toy queue of one or two jobs.

```bash
./experiments/07-scale/scripts/backlog-slurm.sh 500     # throughput run
./experiments/07-scale/scripts/backlog-slurm.sh 2500    # backlog (stays pending)
./experiments/07-scale/scripts/backlog-k8s.sh 2000      # mixed queues/priorities

# profiling only works if you started the bridge with --pprof-addr in
# section 2 — it is off by default because heap profiles contain the token
curl -s http://127.0.0.1:6060/debug/pprof/profile?seconds=25 -o cpu.pprof
```

**What to look for:** the bridge's own Prometheus metrics on
`:8080/metrics` (`k8s_bridge_tick_duration_seconds`,
`k8s_bridge_held_jobs`) if you have Grafana wired up; otherwise
`bridge-top.sh`'s queue panel filling up.

**Expected result:** the bridge stays I/O-bound and lightweight even at
around 5000 objects (observed roughly 88 MB RSS, 1.6% CPU in earlier runs);
throughput is bound by the poll interval rather than CPU or memory — an
honest, named limitation of the current polling-based design rather than a
hidden one.

Further reading: `experiments/07-scale/README.md`.

---

## 15. Resetting between runs

To re-run from section 4 onward without tearing down the whole cluster:

```bash
# clear any Slurm jobs left over from a previous run
kubectl -n slurm exec deploy/slurm-login-slinky -- bash -c 'squeue -h -o "%i" | xargs -r -n1 scancel'

# clear bridge-managed JobSets and their pods (the bridge will not
# resurrect jobs that no longer exist in Slurm — safe to delete directly)
kubectl delete jobsets -n slurm-jobs --all --ignore-not-found

# clear ad-hoc Kubernetes workloads created during the mixing/serving/CCC sections
kubectl delete jobs -n default -l 'kueue.x-k8s.io/queue-name' --ignore-not-found
kubectl delete deployment queued-inference sample-inference -n default --ignore-not-found
# only needed if you went through experiments/10-ray-bridge or 03-open-items;
# this tutorial creates no Ray objects
kubectl delete raycluster,rayservice -n default --all --ignore-not-found
kubectl delete pod pinned-worker -n default --ignore-not-found

# restart the bridge binary / port-forward if left running from a previous section
pkill -f 'bin/k8s-bridge' 2>/dev/null || true
kill %1 2>/dev/null || true   # the port-forward from section 2, if still open
```

Confirm a clean slate before re-starting from section 4:

```bash
kubectl get workloads -A            # should be empty (or only long-lived infra)
kubectl get jobsets -n slurm-jobs   # should be empty
```

If quota looks stuck (a `ClusterQueue` shows usage but no matching
`Workload`), check for orphaned pods directly:
`kubectl get pods -A --field-selector=status.phase=Failed`.

---

## 16. Tearing down

**Run this whenever you're done, whether that's at the very end or just
pausing for the day.** Leftover clusters and disks are easy to forget
about otherwise.

```bash
./experiments/01-gke-playground/scripts/99-teardown.sh
```

This deletes the cluster, then explicitly sweeps orphaned `pvc-*` disks
(cluster deletion does **not** remove dynamically provisioned disks — the
Slurm controller's state volume is the usual culprit) and prints a final
inventory. Verify the printed inventory is empty:

```bash
gcloud compute instances list --format="table(name,zone,status)"
gcloud compute disks list --format="table(name,zone,sizeGb)"
gcloud compute forwarding-rules list
gcloud container clusters list   # should be empty
```

If section 12 created a standalone node pool (`dws-gpu-qp` — **GPU nodes,
the one genuinely expensive resource in this tutorial**) or Custom
Compute Class node pools, confirm they were deleted along with the cluster
(`ComputeClass`-managed pools are GKE-managed and go with the cluster;
`gcloud container node-pools list --cluster k8s-bridge-playground` before
the cluster delete completes is a belt-and-suspenders check).

If any of the four `gcloud`/`gcloud container` list commands above returns
a non-empty table, **don't walk away yet** — investigate and delete the
leftover resource first.

---

## 17. Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `no matches for kind "ResourceFlavor"` during bring-up | steps 01/02 never ran — almost always `PROJECT_ID` unset. Section 1 now chains with `&&` so the run stops at the real cause, and `03-configure-queues.sh` preflights the Kueue CRDs itself |
| the bridge prints `connection refused` on :6820 forever | the backgrounded `port-forward` died silently. Re-run it and wait for `port-forward up` (section 2) |
| `bash: ID: No such file or directory`, or a stray `syntax error` | you pasted an `<ID>` placeholder — `<` and `>` are shell redirections. Sections 7 and 13 capture the id instead |
| Helm-deployed bridge is Running and healthy but nothing happens | `configSource` is still `file` (the chart default), so `WorkloadMixing` CRs are ignored — reinstall with `--set configSource=cr` (section 2b) |
| bridge's `slurmd` pods stuck in `ContainerCreating` / `FailedMount` | `slurm-auth-slurm` is missing from the bridge's namespace; Secrets are namespace-scoped and the bridge does not copy them (section 2b, step 1) |
| jobs stuck in `JobHeldUser`, bridge logs gone, terminal was closed | the section-2 binary died with your shell. That's the development workflow — section 2b runs it in the cluster |
| two bridges, only one doing anything | the local binary and the chart are both running and share the `k8s-bridge-leader` Lease; check `holderIdentity` and stop one |
| every job Pending on a hand-built cluster | node allocatable CPU is below the 1000m the tutorial requests per task (`e2-medium` is ~940m) — use `e2-standard-4` |
| DWS request says `PROVISIONED` but GPU pods stay `Pending` | the NVIDIA driver DaemonSet hasn't finished on the new node, so `nvidia.com/gpu` isn't advertised yet — wait rather than debug the bridge (section 12) |
| everything pending, "no topology domains" | you forgot the node labels (section 1) |
| jobs un-releasable, "priority request forwarded" on release | old lua plugin: `hold(0)`/`release(INFINITE)` must pass through |
| GPU node drained, "count reported lower" | `gres.conf` must be `Name=gpu File=/dev/null` (count-only alone is not enough for the gpu type) and mounted into the slurmd conf-cache |
| autoscaler deletes "racks" | pin the pool: set `min-nodes` = `num-nodes` |
| flex-start create fails on reservations | add `--reservation-affinity=none` |
| DWS provisioning request `Failed` | GKE rejects CPU-only DWS Flex today — accelerators required; use Custom Compute Classes (section 12 alternative) instead |
| JobSet dead but Slurm job still pending | the bridge fails the job on a JobSet `Failed` condition (section 13) — if it doesn't, check the bridge is ticking (section 2) and watch the `k8s_bridge_jobs_failed_total` metric |
| `sbatch` hangs / no comment update | check the bridge process is running and its `Ready` condition (section 2) — the bridge being down is safe, but nothing progresses until it's back |
| bridge stuck, no ticks at all, no obvious error | check leader election (section 3a) — a second replica or a stuck Lease holder means this pod never wins and never starts running |
| config change (`WorkloadMixing` patch) has no visible effect | confirm CRD mode is active (`--workloadmixing` set); file mode never hot-reloads (section 3d) — it needs a restart, or a chart `checksum/config`-triggered rollout instead |
| Grafana panels blank | dashboards only render once Grafana and Prometheus are actually deployed and scraping the relevant controllers — `bridge-top.sh` or `tools/demo-console/` degrade gracefully without them |

---

_Related reading: [`docs/architecture.md`](architecture.md) (system
architecture, the lifecycle this tutorial walks), [`docs/operations.md`](operations.md)
(SLOs, alerts, runbooks referenced throughout), [`docs/custom-resource.md`](custom-resource.md)
(full `WorkloadMixing` field reference), [`docs/installation.md`](installation.md)
(installing each component individually rather than via the experiment
scripts used here), and [`experiments/DEMO.md`](../experiments/DEMO.md)
(the narrated version of this same walkthrough, exercised live during
validation)._
