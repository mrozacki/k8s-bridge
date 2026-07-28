# Full-stack demo runbook — narrated showcase (Slurm + K8s + Ray + inference)

This is the copy-pasteable script for **showing** k8s-bridge end to end:
Slurm jobs mixed with native Kubernetes batch, Ray, and inference workloads,
all admitted through one Kueue quota. It doubles as the validation runbook —
everything below was exercised live on GKE unless marked otherwise.

**Two windows.** Terminal A: the commands in this file. Terminal B:
`./tools/bridge-top.sh` (live cluster/queue/bridge/Slurm state) — or, if you
want a single screen instead of two windows, use `tools/demo-console/`
(embeds a typed terminal plus the dashboards side by side; see its README).

**Where each command runs.** Every fenced block runs **from the repo root**
in your host shell — `cd` there once and stay put — unless its first line is
`# [slurm-pod]`, which means the shell inside the Slurm login pod opened in
§4. Several sections interleave the two, so keep both panes open: `kubectl`
and `gcloud` never work inside the Slurm pod, and `sbatch`/`squeue`/
`scontrol`/`sinfo` never work on the host.

**Runs on GKE**, not `kind` — Slinky's Slurm stack needs real nodes to
register dynamic slurmd workers against, and the CCC/DWS/topology sections
exercise real GKE autoscaling behavior. Every section below is marked
**[GKE]** or **[GKE — optional add-on]**; nothing in this runbook has a
`kind` path today (the playground never grew one — Slinky's
controller/accounting stack and dynamic-node registration assume a real
cluster network).

**Teardown discipline.** A full run uses `e2-standard-4` spot nodes.
Section 17 (teardown) is **not optional** — run it at the end of every
session, live demo or not.

## Contents

0. Prereqs
1. Cluster + full stack bring-up **[GKE]**
2. Bridge in CRD mode — local binary, development workflow **[GKE]**
2b. The same bridge, deployed in-cluster by Helm — production shape **[GKE]**
3. Control plane: leader election, watch-nudge, Events, config hot-reload (ADR-0011) **[GKE]**
4. Core loop: plain Slurm job, auto-hold → admit → run → cleanup **[GKE]**
5. Resources: memory, GPU simulation, wall-clock guard **[GKE]**
6. Topology-aware placement **[GKE]**
7. Priority — mutable, even while running (ADR-0009) **[GKE]**
8. Multi-team: cohorts, borrowing, reclaim **[GKE]**
9. Per-partition queues: routing different Slurm partitions to different Kueue LocalQueues **[GKE]**
10. Mixing scenario: Kueue-native batch vs. Slurm job contending for one quota **[GKE]**
11. Serving + batch, two-tier admission (ADR-0007) **[GKE]**
12. Ray — deliberately not demonstrated (ray-bridge is experimental)
13. DWS — queued provisioning, incl. routing a Slurm partition at it **[GKE — optional add-on]**
14. Failure path: JobSet dies → bridge fails the Slurm job with a reason (D1) **[GKE]**
15. Scale drill **[GKE — optional add-on]**
16. Reset between runs
17. Teardown + verify zero billing
18. Troubleshooting

---

## 0. Prereqs

- `gcloud` authenticated against your own GCP project:
  `export PROJECT_ID=<your-sandbox-project>`
- `kubectl`, `helm`, `jq`, `nc`, Go 1.26 on PATH. (`jq` is not optional — §5
  and §9 pipe `kubectl -o json` through it; `nc` backs §2's port-forward
  readiness check.)
- Optional, for the single-screen presenter console: `ttyd`
  (`brew install ttyd`) — see `tools/demo-console/README.md`.
- Read first if you haven't: `README.md` (project framing),
  `docs/architecture.md` (system + code architecture, the lifecycle
  diagram this whole demo walks), `docs/operations.md` (SLOs, alerts,
  runbooks — useful for answering "what happens if..." questions live).

---

## 1. Cluster + full stack bring-up **[GKE]**

**Say:** "We're standing up one small GKE cluster that hosts Slurm, Kueue,
JobSet, KubeRay, and the bridge controller — everything lives on the same
pool of nodes; there is no separate Slurm cluster."

```bash
export PROJECT_ID=<your-sandbox-project>   # no default; every script below bails without it
```

**The `&&` chaining below is load-bearing, not style.** Each step depends on
the previous one having succeeded, and pasting a multi-line block into a shell
runs every line regardless of what failed above it. Chained, the run stops at
the real cause; unchained, a failed step 1 surfaces three commands later as
`no matches for kind "ResourceFlavor"` — a missing-CRD error that reads like a
broken manifest. `03-configure-queues.sh` now also preflights this itself.
Measured live 2026-07-27.

```bash
MIN_NODES=3 NUM_NODES=3 MAX_NODES=3 ./experiments/01-gke-playground/scripts/01-create-cluster.sh && \
./experiments/01-gke-playground/scripts/02-install-components.sh && \
./experiments/01-gke-playground/scripts/03-configure-queues.sh && \
kubectl apply -f experiments/05-topology/manifests/topology-tas.yaml && \
kubectl apply -f experiments/06-multitenant/manifests/cohort-queues.yaml && \
kubectl apply -f deploy/crd/workloadmixing-crd.yaml
```

Simulated topology — the labels **are** the topology as far as Kueue TAS is
concerned. This labels **every** node, not the first three: if you resize the
pool (§8 needs a 4th node) or add a second node pool, an unlabelled node is
invisible to TAS and silently shrinks the topology the later sections assume.

```bash
i=0; for N in $(kubectl get nodes -o name | cut -d/ -f2); do \
  kubectl label node "$N" example.com/topology-managed=true \
  example.com/block=block-$([ $((i % 4)) -lt 2 ] && echo a || echo b) \
  example.com/rack=rack-$((i % 2 + 1)) --overwrite; i=$((i+1)); done
kubectl get nodes -L example.com/block,example.com/rack   # every node labelled
```

**Point at:** node count settling (`kubectl get nodes`), then start
`./tools/bridge-top.sh` in the second window/pane — leave it running for
the rest of the demo.

**Expected:** 3 nodes Ready, Kueue/JobSet/KubeRay/Slurm pods Running in
their namespaces, `workloadmixings.k8s-bridge.x-k8s.io` CRD installed.

`MIN_NODES=3` is not decoration: the cluster runs the
`optimize-utilization` autoscaling profile, which removes idle nodes
aggressively. Without a floor of 3 the pool can shrink before you label it
and §6/§8 lose the topology they assume.

**Budget ~25–30 minutes for this section** (`02-install-components.sh` alone
waits up to 15 minutes on the Slurm chart). Bring the cluster up *before*
your audience joins and start the live narration at §3 or §4.

Cross-reference: `experiments/01-gke-playground/README.md`,
`docs/adr/0003-gke-first-playground.md`.

---

## 2. Bridge in CRD mode **[GKE]**

**Say:** "The bridge itself is a small Go controller. It can read its
config from a file or from this `WorkloadMixing` custom resource — we'll
run it in CR mode, which is the in-cluster production path."

> **§2 is the DEVELOPMENT workflow, not the architecture.** The binary below
> runs on your laptop and talks to the cluster through a `kubectl
> port-forward`. That is a convenience for demoing and iterating — the bridge
> is a Kubernetes controller and belongs *in* the cluster. Run it locally and
> your terminal becomes a single point of failure: close the shell or let a
> Cloud Shell session time out and translation stops, leaving submitted Slurm
> jobs sitting in `JobHeldUser` with nothing wrong on the Kubernetes side to
> point at. **§2b is the production shape** — same controller, deployed by
> Helm, no port-forward. Run §2 *or* §2b, not both: leader election is on by
> default in *both* (same `k8s-bridge-leader` Lease, same namespace), so they
> will not corrupt each other — but exactly one of them ticks, and it is
> whichever grabbed the Lease first. Debugging the silent one is a waste of
> demo time. `kubectl get lease -n slurm-jobs k8s-bridge-leader -o
> jsonpath='{.spec.holderIdentity}'` tells you which is live (§3a).
> Reported by the owner's tutorial run, 2026-07-27.

```bash
# The sed also strips the server-managed metadata: a Secret piped straight
# from `get -o yaml` back into `apply` carries its source resourceVersion, and
# re-running this (§2b repeats it) fails with "the object has been modified".
# Reproduced live 2026-07-27.
kubectl -n slurm get secret slurm-auth-slurm -o yaml \
  | sed -e 's/namespace: slurm/namespace: slurm-jobs/' -e '/resourceVersion:/d' \
        -e '/^  uid:/d' -e '/creationTimestamp:/d' \
  | kubectl apply -f -

# The port-forward is backgrounded, so its failure is SILENT: if the slurm
# namespace or the service is not up yet, it exits immediately and the only
# symptom is the bridge binary below flooding the terminal with "connection
# refused". Wait for the port to actually answer before going on.
# Measured live 2026-07-27.
kubectl -n slurm rollout status deploy/slurm-restapi --timeout=180s
kubectl port-forward -n slurm svc/slurm-restapi 6820:6820 &
for i in $(seq 30); do
  nc -z 127.0.0.1 6820 2>/dev/null && { echo "port-forward up"; break; }
  [ "$i" = 30 ] && { echo "ERROR: port-forward to slurm-restapi never came up" >&2; }
  sleep 1
done

kubectl -n slurm exec slurm-controller-0 -c slurmctld -- scontrol token username=root lifespan=14400 | sed 's/SLURM_JWT=//' > /tmp/wm-slurm-token
kubectl apply -f deploy/crd/workloadmixing-sample.yaml

# The sample CR is the IN-CLUSTER shape: an https service DNS name that does
# not resolve from the demo machine, and a token path under the cluster's
# Secret mount. Aim all three fields at your local setup BEFORE starting the
# binary — endpoint fields are baked into the Slurm client at construction,
# so single-CR mode needs a restart to change them afterwards.
# allowInsecureHTTP is mandatory here, not optional: the CRD's CEL rule
# REJECTS a plaintext http:// URL without it (the Slurm token is
# bearer-equivalent). It is acceptable only because this traffic never
# leaves the machine — it goes over the 127.0.0.1 port-forward above.
kubectl -n slurm-jobs patch workloadmixing playground --type=merge \
  -p '{"spec":{"slurmRestURL":"http://127.0.0.1:6820","allowInsecureHTTP":true,"slurmTokenFile":"/tmp/wm-slurm-token"}}'

make build
# --pprof-addr is off by default (heap profiles contain the Slurm token).
# Pass it now if you intend to run the §15 scale drill; drop it otherwise.
./bin/k8s-bridge --workloadmixing slurm-jobs/playground \
  --pprof-addr 127.0.0.1:6060 &
kubectl get workloadmixing -n slurm-jobs playground -o yaml | grep -A3 conditions   # Ready=True
```

**Point at:** the `Ready: "True"` condition on the CR — "the bridge reports
its own health back onto this object; if the Slurm REST endpoint or token
goes bad, this flips to False and you'll see it here before anything else
breaks." If Grafana/`demo-console` is up, also point at `:8080/healthz` and
`/metrics` — "these are the same signals Prometheus scrapes."

**Expected:** `status.conditions[type=Ready].status == "True"`.

Cross-reference: `docs/architecture.md` §5 (config surface),
`docs/adr/0004-prototype-file-config-over-crd.md`.

---

## 2b. The same bridge, deployed in-cluster by Helm **[GKE]**

**Say:** "Everything you just saw runs identically as a Deployment — that's
the shape a real site would run. No port-forward, no laptop in the data
path, and the controller restarts on its own if the node it lands on dies."

Do this **instead of** §2's local binary (see the box above), or after
stopping it with `pkill -f 'bin/k8s-bridge'`. Four prerequisites the chart
deliberately does not do for you — each is a namespace-scoped credential
decision, not a default the chart can safely guess:

```bash
# 1. slurmd's auth key. Secrets are namespace-scoped and the bridge does NOT
#    sync credentials across namespaces (decision 2026-07-13, docs/backlog.md).
#    Without this the slurmd pods the bridge creates hang in ContainerCreating
#    with FailedMount — and nothing in the bridge's own logs says why.
#    (§2's first command already did this; it is repeated here so §2b stands
#    alone.)
kubectl create namespace slurm-jobs --dry-run=client -o yaml | kubectl apply -f -
kubectl -n slurm get secret slurm-auth-slurm -o yaml \
  | sed -e 's/namespace: slurm/namespace: slurm-jobs/' -e '/resourceVersion:/d' \
        -e '/^  uid:/d' -e '/creationTimestamp:/d' \
  | kubectl apply -f -

# 2. a Slurm REST token, mounted as the Secret the chart expects
#    (values.yaml: slurmTokenSecret, default "slurm-rest-token"). lifespan is
#    in seconds — 14400 = 4h, long enough for a demo, NOT for production
#    (see docs/operations.md for the rotation runbook).
kubectl -n slurm exec slurm-controller-0 -c slurmctld -- \
  scontrol token username=root lifespan=14400 | sed 's/SLURM_JWT=//' > /tmp/wm-slurm-token
kubectl -n slurm-jobs create secret generic slurm-rest-token \
  --from-file=token=/tmp/wm-slurm-token --dry-run=client -o yaml | kubectl apply -f -

# 3. the CR, aimed at the IN-CLUSTER endpoint. This is the reverse of the §2
#    patch: 127.0.0.1 is meaningless inside a pod. allowInsecureHTTP is still
#    required because the Slinky slurmrestd service does not terminate TLS by
#    default — acceptable here only because the traffic stays inside the
#    cluster network; a production deployment should front it with TLS and
#    drop this flag (docs/operations.md).
kubectl apply -f deploy/crd/workloadmixing-sample.yaml
kubectl -n slurm-jobs patch workloadmixing playground --type=merge -p '{"spec":{
  "slurmRestURL":"http://slurm-restapi.slurm.svc.cluster.local:6820",
  "allowInsecureHTTP":true,
  "slurmTokenFile":"/var/run/secrets/slurm/token"}}'

# 4. install the chart in CR mode. configSource defaults to `file`; left at the
#    default the controller ignores every WorkloadMixing CR and you get a
#    running, healthy-looking pod that does nothing.
helm upgrade --install k8s-bridge deploy/chart/k8s-bridge \
  --namespace slurm-jobs \
  --set configSource=cr \
  --set image.tag="${BRIDGE_TAG:-$(helm show chart deploy/chart/k8s-bridge | awk '/^appVersion:/{print $2}' | tr -d '"')}"

kubectl -n slurm-jobs rollout status deploy/k8s-bridge --timeout=180s
kubectl -n slurm-jobs logs deploy/k8s-bridge | grep -i "leader\|reconcil" | head
kubectl get workloadmixing -n slurm-jobs playground -o yaml | grep -A3 conditions   # Ready=True
```

**Point at:** the same `Ready: "True"` condition as §2, now reported by a pod
instead of a laptop — "the object is the interface; who runs the controller is
an operational detail."

**Expected:** `deploy/k8s-bridge` reaches 1/1 Ready, wins the
`k8s-bridge-leader` Lease, and the CR's `Ready` condition flips to `True`.
Every later section of this runbook works unchanged — except §15's `pprof`
step, which targets `127.0.0.1:6060` and needs a `kubectl port-forward` to
the pod instead.

With `configSource=cr` and `workloadmixing.name` left empty, the controller
runs in **supervisor mode** (ADR-0015 Phase A): it watches *every*
`WorkloadMixing` in `slurm-jobs` and runs one independent loop per CR. Adding
a second team's mixing config is then `kubectl apply`, not a second Helm
release — that is the multi-tenancy story, and §9 is its per-partition
counterpart.

Cross-reference: `deploy/chart/k8s-bridge/README.md`,
`docs/installation.md` §3.1 (the credential prerequisites above, in full),
`docs/adr/0015-multi-cr-support.md`.

---

## 3. Control plane: leader election, watch-nudge, Events, config hot-reload (ADR-0011) **[GKE]**

**Say:** "Before we run any jobs, worth showing what changed under the hood
this batch: the bridge adopted a controller-runtime Manager. It still polls
Slurm — that never goes away, slurmrestd has no watch API — but it now has
leader election, an informer cache, Kubernetes Events, and it wakes up
immediately on cluster changes instead of waiting out the poll interval."

**3a. Leader election.**

```bash
# works whether the bridge is the local binary from §2 or the Helm chart —
# either way it creates the same Lease object in its config namespace
kubectl get lease -n slurm-jobs k8s-bridge-leader -o yaml | grep -A2 holderIdentity
```

**Say:** "That Lease is a real `coordination.k8s.io` object — if we scaled
the bridge to two replicas (only possible with the Helm chart, not the local
binary from §2), only the Lease holder would tick; the other sits idle.
That's what makes running a hot standby safe." Disable with
`--leader-elect=false` only for a single-replica local/dev run without the
Lease RBAC. **If demoing the Helm-chart deploy instead of the §2 local
binary** (`helm upgrade --install k8s-bridge deploy/chart/k8s-bridge
...`, same pattern as `experiments/09-scale-gpu-churn`), also show
`kubectl -n slurm-jobs logs deploy/k8s-bridge | grep -i leader`.

**3b. Watch-nudge (latency).**

```bash
kubectl -n slurm exec deploy/slurm-login-slinky -- \
  sbatch --partition=mixing --ntasks=1 --wrap='sleep 10'
curl -s http://127.0.0.1:8080/metrics | grep k8s_bridge_tick_trigger_total
```

**Point at:** `k8s_bridge_tick_trigger_total{source="watch"}` incrementing
right after the JobSet/Workload changes, faster than the next
`source="timer"` tick would have fired.

**Expected:** the job is admitted and released noticeably faster than one
full `pollInterval` — a JobSet-ready or Workload-admitted event nudges the
loop immediately; the timer stays the unconditional floor if watches ever
lag or disconnect.

**3c. Kubernetes Events.**

```bash
kubectl describe jobset -n slurm-jobs $(kubectl get jobset -n slurm-jobs -o jsonpath='{.items[0].metadata.name}') | tail -15
```

**Point at:** the `Events:` section at the bottom — `Created`/`Released`
Normal events, or `JobSetFailed`/`TranslationFailed` Warning events if
something went wrong. This works regardless of whether the bridge is
running as the §2 local binary or the Helm chart — the Recorder posts
Events to the apiserver either way. **Say:** "A Slurm-adjacent operator who
only knows `kubectl describe` now gets the same story a Kubernetes-native
operator would, without needing to read the bridge's own logs."

**3d. Config hot-reload (WorkloadMixing CR, no restart).**

```bash
kubectl get workloadmixing -n slurm-jobs playground -o yaml > /tmp/wm-before.yaml
kubectl patch workloadmixing -n slurm-jobs playground --type merge \
  -p '{"spec":{"maxUserPriority":5000}}'
# local binary from §2: watch its stdout in that terminal for the reload log;
# Helm-chart deploy: kubectl -n slurm-jobs logs deploy/k8s-bridge | grep -i "config reload\|spec change"
```

**Point at:** the bridge picks up the new `maxUserPriority` without a pod
restart — no rolling update, no dropped ticks. **Say:** "This only works in
CRD mode — file mode loads once at startup by design, so a config-only
`helm upgrade` in file mode needs the chart's `checksum/config` annotation
to force a restart instead (see the B5 finding, `docs/notes/
2026-07-06-batch2-live-deploy.md`)."

**Expected:** `tick()` snapshots the config once per tick
(`cfgSnapshot()`), so a reload landing mid-tick never mixes fields from two
config generations; the next tick after the patch uses the new value.

Cross-reference: `docs/adr/0011-controller-runtime-manager-and-watch-nudge.md`,
`docs/notes/2026-07-06-batch2-live-deploy.md` (the eight deploy bugs this
Manager adoption surfaced, all fixed).

---

## 4. Core loop: plain Slurm job → admit → run → cleanup **[GKE]**

This is the headline demo: a researcher submits a completely ordinary
Slurm job, with no special flags, and the whole Kubernetes admission chain
happens invisibly underneath.

**Say:** "Watch — this is a plain `sbatch`, nothing special. No `--hold`,
nothing bridge-aware. A JobSubmit plugin auto-holds it the moment it hits
the mixing partition."

Open the Slurm pane now and **leave it open** — §5, §7 and §14 all come
back to it:

```bash
kubectl -n slurm exec -it deploy/slurm-login-slinky -- bash
```

```bash
# [slurm-pod]
sbatch --partition=mixing --ntasks=2 --wrap='srun hostname'   # NO --hold needed
squeue -o "%i %T %k"    # comment narrates: held -> quota -> provisioning
```

**Point at, in order, on the dashboard (bridge-top or demo-console):**
1. `WORKLOADS` panel — a new Kueue `Workload` appears, `ADMITTED: false`.
2. `BRIDGE JOBSETS` panel — a `slurm-job-<id>` JobSet appears with the
   right pod count.
3. `NODES`/autoscaler — if quota required a new node, watch it join.
4. `WORKLOADS` panel flips `ADMITTED: true`.
5. Back in the Slurm pane: `squeue` shows the job leave Hold and move to
   `RUNNING`, then disappear as it completes.

**Say while it lands:** "That JobSet's pods are running `slurmd` — they
just registered as dynamic Slurm nodes. The bridge saw that, lifted the
hold, and Slurm scheduled the job onto its own dedicated nodes exactly like
it would on bare metal. When the job finishes, the bridge deregisters the
nodes and deletes the JobSet — the capacity goes right back into the shared
pool."

**Negative case, same breath:**

```bash
# [slurm-pod]
sbatch --partition=mixing --array=1-5 --wrap=hostname         # clean rejection
```

**Expected:** immediate `sbatch` rejection with a clear message (array
jobs are rejected at submit time by the lua plugin, not silently dropped
later).

Cross-reference: `docs/architecture.md` §3 (the lifecycle, step by step),
`experiments/01-gke-playground/manifests/slurm-values.yaml` (the lua
plugin), `docs/adr/0005-release-after-node-registration.md`.

---

## 5. Resources: memory, GPU simulation, wall-clock guard **[GKE]**

**Say:** "Resource requests translate too — memory per CPU, and even GPUs,
without needing real GPU hardware."

```bash
# [slurm-pod]
# memory: pods sized to --mem-per-cpu, node advertises RealMemory to match
sbatch --partition=mixing --ntasks=1 --mem-per-cpu=2G --wrap='sleep 20'
```

**GPU simulation has three prerequisites, all on the host side, all
required.** Skipping any one leaves the job pending forever behind a
misleading reason — do them *before* the `--gres` submit, not after
(that mistake cost a live session an hour):

```bash
# [host]  <- NOT the Slurm pod: kubectl does not exist in there
# 1. fake the extended resource on EVERY node you want eligible. Kueue's
#    topology-aware scheduling is all-or-nothing per workload, so one faked
#    node is not enough once the rest of the stack occupies the others.
for N in $(kubectl get nodes -o name | cut -d/ -f2); do
  kubectl patch node "$N" --subresource=status --type=merge \
    -p '{"status":{"capacity":{"nvidia.com/gpu":"2"},"allocatable":{"nvidia.com/gpu":"2"}}}'
done

# 2. give the ClusterQueue nvidia.com/gpu quota. Without it Kueue reports
#    "resource nvidia.com/gpu unavailable in ClusterQueue" and never admits.
#    (team-a is the queue the sample CR's localQueue points at.)
kubectl get clusterqueue team-a -o json \
  | jq '.spec.resourceGroups[0].coveredResources += ["nvidia.com/gpu"]
        | .spec.resourceGroups[0].flavors[].resources += [{"name":"nvidia.com/gpu","nominalQuota":"2"}]' \
  | kubectl apply -f -

# 3. the SLURM CLUSTER's gres.conf needs a device-file entry — already
#    shipped as `Name=gpu File=/dev/null` in
#    experiments/01-gke-playground/manifests/slurm-values.yaml. Verify it
#    survived any local edit; do NOT recreate it as a bridge-side ConfigMap,
#    the bridge deliberately no longer mounts one.
grep -A1 'Name=gpu' experiments/01-gke-playground/manifests/slurm-values.yaml
```

```bash
# [slurm-pod]
# NOTE the trailing sleep. With a bare `srun echo`, the whole lifecycle —
# JobSet created, node registered, job run, JobSet cleaned up — takes about
# 7 seconds, and the dynamic node is gone before you can type sinfo. The
# sleep keeps it on screen long enough to point at. Measured live 2026-07-26.
sbatch --partition=mixing --gres=gpu:1 --wrap='srun echo GPU job; sleep 90'
sinfo -N -o "%N %T %G"  # dynamic node advertises gpu:1 (idle, then allocated)

# --nodes / --ntasks-per-node and wall-clock leak guard:
sbatch --partition=mixing --nodes=2 --ntasks-per-node=2 --time=10 --wrap='sleep 30'
```

**Say on the GPU step specifically:** "This is a fully simulated GPU — no
hardware, no cost. We patch a fake `nvidia.com/gpu` resource onto the nodes;
Kueue quota and the scheduler treat it exactly like a real device. On the
Slurm side, GRES verification needs a real device-file entry — on Slurm
26.05 a count-only `Name=gpu` makes slurmd report zero devices and
slurmctld drains the freshly registered node — so the Slurm cluster's own
`gres.conf` points at `/dev/null`. Every step of the chain except CUDA
itself is real." (ADR-0010.)

**Expected:** the dynamic node's `sinfo -N -o "%N %G"` line shows `gpu:1`;
the job runs and completes. If it sits at `ReqNodeNotAvail` instead, you
skipped prerequisite 3 — check for `INVALID_REG` in the slurmctld log.

Cross-reference: `docs/adr/0010-simulated-accelerators.md`.

---

## 6. Topology-aware placement **[GKE]**

**Say:** "Slurm's `--switches` flag — rack/network locality — flows all the
way through to Kueue's Topology-Aware Scheduling. Slurm never has to know
Kubernetes topology exists; its dynamic nodes just end up co-located."

```bash
# [slurm-pod]
sbatch --partition=mixing --ntasks=2 --switches=1 --wrap='sleep 30'
# both slurmd pods land in ONE rack (dashboard TOPOLOGY panel);
# jobs without --switches get best-effort block locality
```

**Point at:** the `TOPOLOGY` panel in `bridge-top.sh` — it groups nodes by
block/rack and shows live pod placement; both pods for this job land under
the same rack.

**Expected:** both slurmd pods scheduled onto nodes sharing one
`example.com/rack` label; a job requesting more capacity than any single
rack holds stays inadmissible with a topology message
(`doesn't allow to fit any of N pod(s)`) — worth demonstrating as the
negative case if time allows (see `experiments/05-topology/README.md`
scenario C).

Cross-reference: `docs/architecture.md` §4a,
`docs/adr/0008-topology-translation.md`,
`experiments/05-topology/README.md`.

---

## 7. Priority — mutable, even while running (ADR-0009) **[GKE]**

**Say:** "Priorities aren't fixed at submit time. A researcher — or an
admin — can bump a job's priority after submission, even while it's
running, and Kueue immediately re-ranks who gets preempted first."

```bash
# [slurm-pod]
# --parsable makes sbatch print the bare job id, so the id is captured instead
# of hand-copied. This is deliberate: the older form spelled the placeholder
# `<ID>`, and pasting that into a shell makes bash read `<` and `>` as
# redirections — the command fails with "No such file or directory" on a file
# literally named ID, which looks like a Slurm error and is not one.
# Reported by the owner's tutorial run, 2026-07-27.
JOBID=$(sbatch --parsable --partition=mixing --ntasks=1 --time=10 --wrap='sleep 180')
echo "submitted ${JOBID}"
scontrol update job "${JOBID}" priority=700   # lua turns this into a directive
scontrol show job "${JOBID}" | grep AdminComment   # wm:prio-applied=700
```

```bash
# [host]  <- back out of the Slurm pod; the same number, now on the K8s side
kubectl get workload -n slurm-jobs -o jsonpath='{.items[0].spec.priority}'  # 700
```

**Point at:** the `AdminComment` field acknowledging the applied priority,
then the `Workload.spec.priority` value on the Kubernetes side matching it.

**Expected:** `wm:prio-applied=700` in the Slurm comment; `700` reflected
on the `Workload` object. Explain the plumbing if asked: Slurm's own
`priority` field can't be the data channel (it's scheduler-owned and
resets), so the lua plugin intercepts the update and writes a directive
instead — see ADR-0009 for the live failure that led to this design.

Cross-reference: `docs/architecture.md` §4b,
`docs/adr/0009-priority-mutation-channel.md`.

---

## 8. Multi-team: cohorts, borrowing, reclaim **[GKE]**

**Say:** "Two teams can share a quota pool. Idle capacity gets lent out
automatically, and reclaimed the moment the owning team actually needs
it — this composes with topology-aware scheduling too."

> **This section needs a 4th node.** The filler is a 4-pod gang at 2 CPU
> each, Kueue's topology-aware scheduling admits all-or-nothing, and an
> `e2-standard-4` has room for exactly one such pod once the shared stack is
> running. On the 3-node cluster §1 pins, Kueue reports `topology
> "simulated-dc" allows to fit only 2 out of 4 pod(s)` and the borrow never
> happens. Either resize first or shrink the filler's `parallelism`.
> Measured live 2026-07-25 — skip this section rather than debug it in front
> of an audience.
>
> ```bash
> source ./experiments/01-gke-playground/scripts/00-env.sh   # exports ZONE
> gcloud container clusters resize "${CLUSTER_NAME}" --num-nodes=4 --zone "${ZONE}" --quiet
> # a new node needs the topology labels too — re-run §1's labelling loop
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

**Point at:** the `clusterqueue team-b` borrowed-quota value going above
zero, then the eviction event once team-a reclaims — read the event text
aloud, it names preemptor/preemptee paths explicitly.

**Expected:** team-b's filler job borrows team-a's idle CPU; team-a's
submission evicts team-b's borrowing workload wholesale (reclaim is
all-or-nothing per workload, not partial).

Cross-reference: `experiments/06-multitenant/README.md`.

---

## 9. Per-partition queues: routing Slurm partitions to different Kueue LocalQueues **[GKE]**

**Say:** "Every partition can target its own queue instead of sharing one
global one — this is the config knob a real multi-team HPC site would use:
each Slurm partition maps to the team that owns it."

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

**Point at:** the `Workload.spec.queueName` value matching the partition's
configured `localQueue` (or falling back to the global `LocalQueue` for
partitions without an override) — `config.LocalQueueFor()` implements this
fallback.

**Expected:** a partition with a `localQueue` override routes its JobSets to
that queue; a partition without one falls back to the global queue — both
observable from the same `Workload.spec.queueName` field, no separate
mechanism to explain.

Cross-reference: `docs/architecture.md` §5 (config surface, backlog A1b),
`internal/config/config.go` (`PartitionMapping.LocalQueue`,
`Config.LocalQueueFor`).

---

## 10. Mixing scenario: Kueue-native batch vs. Slurm job, one quota **[GKE]**

This is the scenario that makes the whole pitch concrete: two completely
different workload systems, same admission authority, same numbers.

**Say:** "Here's the actual point of this project. One plain Kubernetes
Job, one Slurm job, same ClusterQueue, same quota. Kueue doesn't care which
one submitted — first up gets the resources; if both want more than the
pool has, whichever has priority wins, exactly like it would if they were
both native Kubernetes Jobs."

> **The queue label must match the bridge's `localQueue`, or nothing
> contends.** `kueue-batch-job.yaml` ships targeting LocalQueue `main`
> (→ ClusterQueue `main-queue`), because its own experiment README uses it
> standalone. The sample `WorkloadMixing` CR routes Slurm jobs to `team-a`.
> Applied unmodified, the two workloads land in **two different
> ClusterQueues** with separate quota — they never compete, and the claim
> above is visibly false on screen. The override below is what makes this
> section demonstrate what it says. Measured live 2026-07-26.

```bash
# a native Kubernetes batch Job, retargeted at the SAME LocalQueue the
# bridge's CR uses (LocalQueue team-a exists in `default` already —
# cohort-queues.yaml creates it there and in slurm-jobs)
sed 's|kueue.x-k8s.io/queue-name: main$|kueue.x-k8s.io/queue-name: team-a|' \
  experiments/01-gke-playground/workloads/kueue-batch-job.yaml | kubectl create -f -
kubectl get jobs -n default   # note the generated name, e.g. sample-batch-xxxxx

# a Slurm job contending for the SAME quota, submitted the ordinary way
kubectl -n slurm exec deploy/slurm-login-slinky -- \
  sbatch --partition=mixing --ntasks=2 --wrap='sleep 90'

# the money shot: both, plus anything from §11, in ONE ClusterQueue
kubectl get workload -A -o 'custom-columns=NS:.metadata.namespace,NAME:.metadata.name,QUEUE:.spec.queueName,ADMITTED:.status.conditions[?(@.type=="Admitted")].status'
kubectl get clusterqueue team-a -o jsonpath='{.status.flavorsUsage}'
```

**Point at:** the `WORKLOADS` panel in `bridge-top.sh` — both objects show
up as `Workload` resources in the same `ClusterQueue`, one backed by a
plain `batch/v1.Job`, the other by the bridge's JobSet. If quota is tight,
narrate whichever one waits and why (`kubectl describe workload <name>` for
the pending condition's message).

**Expected:** both workloads reserve quota from the same `ClusterQueue`;
whichever fits first (or has priority) admits first; the Slurm job's
completion and the Kubernetes Job's completion both free capacity back to
the same pool, visible in the same panel.

Cross-reference: `experiments/01-gke-playground/workloads/kueue-batch-job.yaml`,
`docs/architecture.md` §2 (system overview diagram — this is literally the
picture in that diagram).

---

## 11. Serving + batch, two-tier admission (ADR-0007) **[GKE]**

**Say:** "Inference serving has different rules — it's latency-sensitive,
so instead of queueing and waiting, a high-priority serving scale-up
preempts batch outright to get capacity immediately."

```bash
kubectl apply -f experiments/04-serving-admission/manifests/serving-queued.yaml
kubectl scale deployment queued-inference --replicas=3
# serving preempts batch; batch re-admits on borrowed capacity (events tell the story)
```

**Point at:** the `Serving admissions and batch displacements` panel on
the Grafana dashboard (or the eviction events: `kubectl get events -n
default | grep -i preempt`) — batch pods get evicted the moment the
serving replica needs the CPU.

**Expected:** `queued-inference` scales to 3 Ready pods quickly; a
concurrently running batch Job/JobSet gets suspended (evicted) to make
room, then re-admits once capacity frees up.

Cross-reference: `docs/adr/0007-two-tier-serving-admission.md`,
`experiments/04-serving-admission/README.md`.

---

## 12. Ray — deliberately not demonstrated **[not in this runbook]**

**Say, if asked:** "Ray is in scope for the project — a shared `RayCluster`
admitted through Kueue is exactly the mixing story, and ADR-0002 covers it.
What we do *not* demo is the controller that automates it. `ray-bridge` is
experimental: validated at small scale on `kind`, never on a real multi-node
cluster. Showing it next to nine sections that were walked live on GKE would
imply a maturity it doesn't have."

The mechanism, the manual pin-gate walkthrough, and the automated
`kind`-runnable version all still exist — they are just not part of the
narrated demo:

- `experiments/10-ray-bridge/README.md` — the automated ray-bridge run.
- `experiments/03-open-items/README.md` — the shared cluster, the inner-job
  admission gap, and the pinned worker, by hand.
- `docs/adr/0002-ray-and-inference-scope.md`, `docs/adr/0013-ray-pin-gate-admission.md`.

Owner decision 2026-07-27: keep Ray out of the reader- and audience-facing
documents until ray-bridge has been validated on a real cluster. KubeRay
stays in the §1 bring-up because the rest of the stack expects it.

## 13. DWS — queued provisioning **[GKE — optional add-on]**

Skip this section on a tight time budget; it demonstrates GKE's queued node
provisioning integration with Kueue's `AdmissionCheck`, not a bridge feature
per se.

> **This section creates real GPU nodes and costs real money** (`g2-standard-4`
> = 1× NVIDIA L4 each). It is the only part of this runbook that does. Every
> other section runs on spot CPU nodes with simulated accelerators (§5,
> ADR-0010). Decide deliberately, and note that §17's teardown must run even
> if you abort halfway.

**Say:** "This is queued provisioning: instead of failing when capacity is
absent, Kueue asks GKE *for* capacity and holds the workload inadmissible
until a `ProvisioningRequest` comes back. It's the mechanism you want for
scarce accelerators — jobs queue for hardware that doesn't exist yet."

**13a. The node pool.** `--enable-queued-provisioning` is the load-bearing
flag; `--flex-start` alone is not sufficient for the Cluster Autoscaler to
service Kueue's `ProvisioningRequest` objects (S1–S5 lesson 4).
`--reservation-affinity=none` is mandatory or the create call fails.

```bash
# Re-export PROJECT_ID in a fresh shell: 00-env.sh has no default and stops on
# the unset variable, leaving ZONE and CLUSTER_NAME EMPTY — the gcloud call then
# runs with `--cluster "" --zone ""`. Measured live 2026-07-28.
export PROJECT_ID=<your-sandbox-project>
source ./experiments/01-gke-playground/scripts/00-env.sh
echo "cluster=${CLUSTER_NAME:?} zone=${ZONE:?}"

# Zone is parameterised on purpose: L4 availability is zone-specific, and a
# hardcoded europe-* zone silently fails for anyone running elsewhere.
# --node-labels is equally load-bearing: the bridge annotates every JobSet with
# a podset topology, so this pool's nodes need the matching level labels or a
# provisioned GPU node is invisible to placement.
gcloud container node-pools create dws-gpu-qp --cluster "${CLUSTER_NAME}" --zone "${ZONE}" \
  --machine-type g2-standard-4 --accelerator type=nvidia-l4,count=1 \
  --num-nodes 0 --enable-autoscaling --min-nodes 0 --max-nodes 2 \
  --enable-queued-provisioning --reservation-affinity=none \
  --node-labels=example.com/topology-managed=true,example.com/block=block-a,example.com/rack=rack-1
```

**13b. The Kueue admission chain**, shipped as one file — this is the
`ProvisioningRequestConfig` → `AdmissionCheck` → `ClusterQueue` chain the
older version of this section told you to build yourself:

```bash
kubectl apply -f experiments/11-scale-s1-s5/manifests/dws-gpu-qp.yaml
kubectl get provisioningrequestconfig,admissioncheck,clusterqueue dws-gpu-cq
```

**13c. Route a *Slurm* partition at that queue.** This is the part that makes
DWS a bridge feature rather than a Kubernetes demo: a researcher keeps using
`sbatch`, and the capacity is queued out of GKE underneath them. The chart
ships this mapping commented out (`deploy/chart/k8s-bridge/values.yaml`)
precisely because `dws-gpu-lq` only exists once 13b has run.

```bash
kubectl -n slurm-jobs patch workloadmixing playground --type=merge -p '{"spec":{"partitionMappings":[
  {"partitionName":"mixing","workloadPriorityClass":"normal-priority"},
  {"partitionName":"mixing-gpu","workloadPriorityClass":"normal-priority","localQueue":"dws-gpu-lq"}]}}'
kubectl get workloadmixing -n slurm-jobs playground -o jsonpath='{.spec.partitionMappings}' | jq .
```

```bash
# [slurm-pod]
sbatch --partition=mixing-gpu --gres=gpu:1 --wrap='srun nvidia-smi'
```

```bash
kubectl get provisioningrequests -A
kubectl get workload -n slurm-jobs -o 'custom-columns=NAME:.metadata.name,QUEUE:.spec.queueName,ADMITTED:.status.conditions[?(@.type=="Admitted")].status'
```

**Point at:** the `ProvisioningRequest` appearing in response to an ordinary
`sbatch`, and the `Workload`'s `queueName` reading `dws-gpu-lq` — the Slurm
user never named a Kubernetes queue.

**Expected** (walked live 2026-07-28 on an account with NO GPU quota, which
is a useful way to see the queueing behaviour):

```
Workload             QuotaReserved True  in ClusterQueue dws-gpu-cq
ProvisioningRequest  Accepted      True  SuccessfullyQueued
                     Provisioned   False ResourcePoolExhausted: Waiting for resources
```

`Provisioned: False` is DWS working, not failing — the request QUEUES for
capacity instead of erroring. With quota and available L4s it goes on to
`Provisioned: True`. Physical L4 boot and job completion remain unvalidated.

> **Two prerequisites that fail silently.** (1) `gpu-priority` must exist —
> without it Kueue never creates a Workload, the JobSet sits Suspended
> forever, the Slurm job hangs, and the bridge still reports `Ready=True`;
> `kueue-config.yaml` now ships it. (2) The flavor must set `topologyName` —
> the bridge annotates every JobSet with a podset topology, and Kueue refuses
> a non-TAS flavor, so no `ProvisioningRequest` is ever created. Both found
> live 2026-07-28.

> **`PROVISIONED` does not mean "pods running".** Once the
> `ProvisioningRequest` flips to `PROVISIONED` the VM has joined the cluster,
> but GPU pods can stay `Pending` for several more minutes: GKE does not
> advertise `nvidia.com/gpu` capacity to the scheduler until the NVIDIA driver
> installer DaemonSet finishes on that node. This gap reads like a broken
> bridge and is not one — watch it with
> `kubectl get nodes -o custom-columns=NAME:.metadata.name,GPU:.status.allocatable.nvidia\.com/gpu`.
> Reported by the owner's tutorial run, 2026-07-27.

**Known limit, worth narrating: DWS rejects CPU-only requests.** If you point
this chain at a CPU node pool instead, the `ProvisioningRequest` reaches
`Failed` with `Resize requests without accelerators are not supported` — a
GKE policy, not a bug in the bridge (backlog A5). That is why 13a provisions
an accelerator pool rather than the cheaper `e2-standard-2` this section used
to suggest.

**Alternative that works cleanly** — Custom Compute Classes (session 6,
`experiments/08-ccc-dws/`): declarative machine-family preference lists
(spot E2 → on-demand E2, or on-demand N2) with GKE auto-creating matching
node pools on demand.

```bash
kubectl apply -f experiments/08-ccc-dws/manifests/compute-classes.yaml
kubectl get pods -o wide -l job-name=ccc-probe-econo   # spot E2 node
kubectl get nodes -L machine-family,cloud.google.com/gke-spot,compute-class
```

Cross-reference: `experiments/08-ccc-dws/README.md`,
`docs/notes/2026-07-05-session6-ccc-dws.md`.

---

## 14. Failure path: JobSet dies → bridge fails the Slurm job with a reason (D1) **[GKE]**

**Say:** "Failure handling matters as much as the happy path. If a JobSet
dies — it blows a deadline, or can never pull its image before the Slurm
nodes register — the bridge detects the `Failed` condition and fails the
corresponding Slurm job with a clear reason, instead of leaving it pending
forever. This is what closed the old D1 gap."

Reproduce (D1 was first observed as the pre-fix pending-forever case in
`docs/notes/2026-07-05-session5-findings.md`; the fix is documented in
`docs/operations.md`):

> **Do not try to trigger this live.** Both reproductions this section used
> to suggest are impossible on a running JobSet: `spec.replicatedJobs` is
> **immutable**, so neither patching `activeDeadlineSeconds` nor swapping in
> an unpullable image is accepted by the apiserver, and force-deleting the
> pod just makes JobSet restart it. Narrate this section from the runbook and
> the metric instead of performing it — the mechanism itself is validated,
> it simply cannot be provoked on demand. Measured live 2026-07-26.

```bash
# read-only: what you would see if a JobSet had died
kubectl get jobset -n slurm-jobs          # would reach Failed / DeadlineExceeded
kubectl get events -n slurm-jobs \
  --field-selector reason=JobSetFailed    # "Slurm job <id> failed: JobSet reported Failed (<reason>)"
```

```bash
# [slurm-pod]
squeue -o "%i %T %r"                       # the job leaves PENDING — cancelled, not hanging
# substitute the id from the line above; quoted so no shell redirection happens
JOBID=12345
scontrol show job "${JOBID}" | grep Comment   # Comment=wm: JobSet failed: <reason>
```

**Point at:** the JobSet's `Failed` status and, right beside it, the Slurm
job leaving `PENDING` with the failure reason propagated as its comment — the
bridge closed the loop end to end, no human in the middle.

**Expected:** on the next tick the bridge (`jobSetFailed` →
`failJobForDeadJobSet`) copies the JobSet's failure message onto the Slurm job
(`wm: JobSet failed: <reason>`), cancels the job, increments
`k8s_bridge_jobs_failed_total`, and emits a `JobSetFailed` Warning Event — no
manual `scancel` needed. (A related path, **D2**, covers a JobSet that
*disappears* entirely: its Slurm job is cancelled as orphaned after a grace
period.)

Cross-reference: `docs/operations.md` runbook "JobSet dead, Slurm job pending
forever (backlog D1 — fixed 2026-07-06)".

---

## 15. Scale drill **[GKE — optional add-on]**

**Say:** "And this is what it looks like under load — not a toy queue of
one or two jobs."

```bash
./experiments/07-scale/scripts/backlog-slurm.sh 500     # throughput run
./experiments/07-scale/scripts/backlog-slurm.sh 2500    # backlog (stays pending)
./experiments/07-scale/scripts/backlog-k8s.sh 2000      # mixed queues/priorities

# profiling only works if the bridge was started with --pprof-addr (§2) —
# it is off by default because heap profiles contain the Slurm token
curl -s http://127.0.0.1:6060/debug/pprof/profile?seconds=25 -o cpu.pprof
```

**Point at:** the bridge's own Prometheus metrics on `:8080/metrics`
(`k8s_bridge_tick_duration_seconds`, `k8s_bridge_held_jobs`) if Grafana is
wired up; otherwise `bridge-top.sh`'s queue panel filling up.

**Expected:** bridge stays I/O-bound and lightweight even at ~5000 objects
(observed ~88 MB RSS, 1.6% CPU); throughput is poll-interval-bound
(~3-4 jobs/min under full backlog pressure) — an honest, named limitation,
not a hidden one (backlog P1: watch-driven reconciliation).

Cross-reference: `experiments/07-scale/README.md`,
`docs/notes/2026-07-05-session5-findings.md`.

---

## 16. Reset between runs

To re-run the demo from section 4 onward without tearing down the whole
cluster:

```bash
# clear any Slurm jobs left over from a previous run
kubectl -n slurm exec deploy/slurm-login-slinky -- bash -c 'squeue -h -o "%i" | xargs -r -n1 scancel'

# clear bridge-managed JobSets and their pods (the bridge will not
# resurrect jobs that no longer exist in Slurm — safe to delete directly)
kubectl delete jobsets -n slurm-jobs --all --ignore-not-found

# clear ad-hoc Kubernetes workloads created during the mixing/serving/CCC sections
kubectl delete jobs -n default -l 'kueue.x-k8s.io/queue-name' --ignore-not-found
kubectl delete deployment queued-inference sample-inference -n default --ignore-not-found
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

## 17. Teardown + verify zero billing

**Always run this at the end of a session — live demo or not.** An idle
cluster still bills for its nodes.

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

If section 13 created a standalone node pool (`dws-gpu-qp` — **GPU nodes,
the one genuinely expensive resource in this runbook**) or Custom Compute
Class node pools, confirm they were deleted along with the cluster
(`ComputeClass`-managed pools are GKE-managed and go with the cluster;
`gcloud container node-pools list --cluster "${CLUSTER_NAME}" --zone "${ZONE}"`
before the cluster delete completes is a belt-and-suspenders check). GPU
quota is released only once the pool is actually gone:

```bash
gcloud compute accelerator-types list --filter="zone:${ZONE}" >/dev/null   # sanity: zone reachable
gcloud container node-pools list --cluster "${CLUSTER_NAME}" --zone "${ZONE}" 2>/dev/null || echo "cluster already deleted — nothing to check"
```

If any of the four `gcloud`/`gcloud container` list commands above returns
a non-empty table, **do not end the session** — investigate and delete the
leftover resource before walking away.

---

## 18. Troubleshooting (all learned live)

| Symptom | Fix |
|---|---|
| `no matches for kind "ResourceFlavor"` (or `ClusterQueue`) on step 1 | steps 01/02 never ran — almost always `PROJECT_ID` unset. The §1 block chains with `&&` so it now stops at the real cause; `03-configure-queues.sh` also preflights the Kueue CRDs itself |
| bridge floods the terminal with `connection refused` on :6820 | the backgrounded `port-forward` died silently (§2). Re-run it and wait for the readiness loop to print `port-forward up` |
| `bash: ID: No such file or directory`, or `syntax error near unexpected token` | you pasted a `<ID>` placeholder — `<`/`>` are shell redirections. §7/§14 now capture the id with `sbatch --parsable` instead |
| Helm-deployed bridge is Running and healthy but does nothing | `configSource` is still `file` (the chart default), so every `WorkloadMixing` CR is ignored — reinstall with `--set configSource=cr` (§2b) |
| bridge's slurmd pods stuck `ContainerCreating` / `FailedMount` | `slurm-auth-slurm` is missing from the bridge's namespace. Secrets are namespace-scoped and the bridge deliberately does not copy credentials across namespaces — copy it per §2b step 1 |
| jobs held forever, nothing in the bridge logs, terminal was closed | the §2 local binary died with your shell. That is the development workflow; §2b is the in-cluster deployment |
| two bridges, only one ticking | both §2 and §2b are running and contending for the `k8s-bridge-leader` Lease. Check `holderIdentity` (§3a) and stop the other one |
| every job Pending on a hand-built cluster | node allocatable CPU below the 1000m the demo requests per task (`e2-medium` is ~940m). Use `e2-standard-4`; `03-configure-queues.sh` warns about this |
| DWS `ProvisioningRequest` says `PROVISIONED` but GPU pods stay `Pending` | the NVIDIA driver DaemonSet has not finished on the new node, so `nvidia.com/gpu` is not advertised yet — wait, do not debug the bridge (§13) |
| everything pending, "no topology domains" | you forgot the node labels (step 1) |
| jobs un-releasable, "priority request forwarded" on release | old lua: hold(0)/release(INFINITE) must pass through |
| GPU node drained "count reported lower" | the SLURM CLUSTER's gres.conf must be `Name=gpu File=/dev/null` — count-only is not enough on 26.05. It belongs in the Slurm chart's `configFiles` (`experiments/01-gke-playground/manifests/slurm-values.yaml`), NOT in a bridge-side ConfigMap; the bridge deliberately no longer mounts one (§5 prerequisite 3) |
| GPU job pending, "resource nvidia.com/gpu unavailable in ClusterQueue" | you skipped §5 prerequisite 2 — the ClusterQueue has no `nvidia.com/gpu` quota, so Kueue can never admit |
| `kubectl`/`gcloud` "command not found" mid-demo | you are in the Slurm login pod; those blocks are host-side. Blocks marked `# [slurm-pod]` are the only ones that run inside it |
| `curl :6060/debug/pprof` connection refused | pprof is off unless the bridge was started with `--pprof-addr 127.0.0.1:6060` (§2) |
| `patch workloadmixing` rejected, "must use https unless allowInsecureHTTP is set" | the CRD's CEL rule refuses a plaintext URL on its own — patch `allowInsecureHTTP: true` in the same merge (§2) |
| autoscaler deletes "racks" | pin the pool: min-nodes = num-nodes |
| flex-start create fails on reservations | add `--reservation-affinity=none` |
| JobSet stuck `Suspended`, NO Workload object at all, bridge says `Ready=True` | the partition's `workloadPriorityClass` does not exist — Kueue's reconciler fails with `WorkloadPriorityClass "<name>" not found` and never creates the Workload. Nothing bridge-side looks wrong. `kubectl get workloadpriorityclass`, then the kueue-controller-manager log (§13) |
| Workload `Pending`, `does not support TopologyAwareScheduling`, no ProvisioningRequest | the ResourceFlavor lacks `topologyName`. The bridge annotates EVERY JobSet with a podset topology (per-CR setting), so every flavor it routes to must be TAS-enabled, with matching node labels (§13) |
| `gcloud` runs with `--cluster "" --zone ""` | `00-env.sh` was sourced with `PROJECT_ID` unset: it stops there and never exports `ZONE`/`CLUSTER_NAME`. Re-export `PROJECT_ID` first (§13) |
| DWS provisioning request `Failed` | GKE rejects CPU-only DWS Flex today — accelerators required (backlog A5); use Custom Compute Classes (§13 alternative) instead for a working demo of queued/preferred node classes |
| JobSet dead but Slurm job still pending | the bridge fails the job on a JobSet `Failed` condition (D1, §14) — if it doesn't, check the bridge is ticking (§2) and watch `k8s_bridge_jobs_failed_total` |
| `sbatch` hangs / no comment update | check the bridge process is running and its `Ready` condition (§2) — bridge-down is safe but nothing progresses until it's back |
| bridge stuck, no ticks at all, no obvious error | check leader election (§3a) — a second replica or a stuck Lease holder means this pod never wins and never starts `Run` |
| config change (WorkloadMixing patch) has no visible effect | confirm CRD mode (`--workloadmixing` set); file mode never hot-reloads (§3d) — needs a restart / chart `checksum/config`-triggered rollout instead |
| Grafana panels blank | dashboard JSON is metric-name-validated but only render-validated once Grafana + Prometheus are actually deployed and scraping `kueue-controller-manager`/`kuberay-operator`/the bridge (backlog A3) — `bridge-top.sh` or `tools/demo-console/` degrade gracefully without it |

---

_Related reading: `docs/architecture.md` (system architecture, the
lifecycle this whole demo walks), `docs/operations.md` (SLOs, alerts,
runbooks referenced throughout), `docs/backlog.md` (every "known gap"
mentioned above is tracked there by ID), `docs/adr/` (the decisions behind
each mechanism), `tools/demo-console/README.md` (single-screen presenter
console: live terminal + dashboards, no window switching)._
