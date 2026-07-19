#!/usr/bin/env bash
# Nightly kind e2e smoke test (backlog AUD5, project audit CNCF-gap item 4).
#
# SCOPE SHIPPED: REDUCED, not the full DEMO.md Slurm+Kueue+JobSet story.
#
# Why reduced: the audit's recommendation ("Quality gates" §4: "nightly kind
# e2e (~80% of DEMO.md runs on kind at zero cost)") assumed the Slinky
# slurm-operator chart (experiments/01-gke-playground
# /scripts/02-install-components.sh) can run on a kind cluster. It cannot,
# reliably, in an unattended nightly CI job:
#   - slurmd nodesets want writable cgroups and run privileged (see
#     internal/config/config.go's Slurmd.PrivilegedOrDefault comment, and the
#     security audit's threat-model T7) — kind's nodes are themselves
#     containers, and privileged-in-privileged is exactly the kind of flaky,
#     host-dependent setup a nightly gate must not depend on.
#   - the full stack (cert-manager + JobSet + Kueue + KubeRay + slurm-operator
#     + a Slurm cluster) takes ~15-20 minutes even on real nodes (DEMO.md
#     step 1) — workable for a manual/paid GKE session, marginal for a free
#     nightly GitHub Actions runner budget.
#
# What this script does (two phases, both against the same kind cluster):
#
#   PHASE 1 — bare chart-equivalent manifests (original scope, kept as-is).
#   Stands up JobSet + Kueue, then applies hand-written manifests that mirror
#   (not invoke) the chart's Deployment/RBAC/ConfigMap shape in
#   `configSource=file` mode against a mock slurmrestd, and asserts the bridge
#   Deployment reaches Ready, /metrics and /readyz serve. This phase predates
#   phase 2 and is kept because it is a useful, minimal, helm-free sanity
#   check that the underlying manifests/args are consistent — but it does NOT
#   exercise the chart itself, so it structurally cannot catch a chart bug.
#
#   PHASE 2 — CHART-DEPLOY SMOKE (backlog SEC3 / this task). `helm install`s
#   the ACTUAL Helm chart (deploy/chart/k8s-bridge) — real securityContext
#   (runAsNonRoot + fsGroup), real namespaced RBAC (Role/RoleBinding, not a
#   ClusterRole), real leader election (--leader-elect=true), configSource=file
#   pointed at the same mock slurmrestd, a pre-created token Secret, and the
#   wm-gres-conf ConfigMap + a copied slurm-auth-slurm Secret (the two objects
#   experiments/09-scale-gpu-churn's namespace-prereqs script showed a live
#   deploy expects to exist in the bridge's namespace, even though this
#   reduced scope has no slurmd JobSet member that actually mounts them).
#   This is the phase that matters most: FIVE of the eight bugs found
#   on the first live chart deploy (B1 leader-election-namespace, B2
#   token-fsGroup, B3 controller-runtime logging, B5 checksum/config, B8
#   cluster-scoped-cache-vs-namespaced-RBAC) are only reachable by running the
#   real chart with its real securityContext and real namespaced RBAC — no
#   unit test or envtest suite exercises `helm install`, a distroless
#   non-root UID, or the chart's actual Role scope. Phase 1's hand-copied
#   manifests happen to already encode the post-fix shape (fsGroup, Role not
#   ClusterRole, --leader-elect=true), so phase 1 passing again is NOT
#   evidence phase 2 would also pass — the two phases test genuinely
#   different things and are asserted independently below.
#
# What phase 2 does NOT exercise: hold->translate->admit->release->cleanup for
# a real job (there is no real Slurm, so there is no job to hold/release),
# JobSet creation (the mock has an empty job list, so the bridge's tick has
# nothing to translate), topology/GPU/priority features, or the Slinky chart
# at all. Promoting to the full DEMO.md scope on kind (or a small ephemeral
# GKE cluster) remains open — see backlog AUD5/A5.
#
# Validated: phase 1 ran successfully end-to-end on 2026-07-06 (kind v0.32.0,
# Docker Desktop, macOS/arm64) — kind cluster up, JobSet v0.12.0 + Kueue
# v0.18.2 installed, bridge image built and loaded, mock slurmrestd + bridge
# deployed, all three assertions passed, cluster torn down. Phase 2 (this
# task's addition) — see this change's PR/session notes for whether it was
# run locally (kind/docker/helm availability) or is CI-validated-only
# (`bash -n` + review); if the latter, that is recorded there, not here, so
# this header does not go stale independently of the actual run.
#
# Local run: `make e2e-kind` from ., or
# `./test/e2e/run.sh` directly. Requires: kind, docker, kubectl, helm on PATH
# (helm only for phase 2 — phase 1 stays helm-free by design, see its own
# comment above).
#
# Location rationale: this lives under test/e2e (not a
# repo-root test/e2e) because it needs the Go module (to build the bridge
# image) and config as its working context; it reaches
# up to deploy/chart to actually install the chart in phase 2, and does not
# need experiments/ manifests at all (no GKE, no Slinky, no DWS in this
# scope).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.." # -> repo root
REPO_ROOT="$(pwd)"
CHART_DIR="${REPO_ROOT}/deploy/chart/k8s-bridge"

CLUSTER_NAME="${CLUSTER_NAME:-k8s-bridge-e2e}"
NAMESPACE="${NAMESPACE:-slurm-jobs}"
IMAGE_TAG="${IMAGE_TAG:-k8s-bridge:e2e}"

# Phase 2 installs into a separate namespace/release from phase 1 so the two
# phases cannot mask each other's failures (e.g. phase 1 leaving a Ready
# bridge behind would make phase 2's Deployment-not-Ready bug invisible if
# they shared a Service/label selector).
CHART_NAMESPACE="${CHART_NAMESPACE:-slurm-jobs-chart}"
CHART_RELEASE="${CHART_RELEASE:-k8s-bridge-e2e}"

# Pinned versions match experiments/01-gke-playground/scripts/00-env.sh
# (verified 2026-07-03 against upstream release pages) so this e2e installs
# the SAME component versions the manual playground validates, not a
# drifting "latest".
JOBSET_VERSION="${JOBSET_VERSION:-v0.12.0}"
KUEUE_VERSION="${KUEUE_VERSION:-v0.18.2}"

KEEP_CLUSTER="${KEEP_CLUSTER:-false}" # set true to skip teardown for debugging

log() { echo "==> $*"; }

cleanup() {
  if [ "${KEEP_CLUSTER}" != "true" ]; then
    log "tearing down kind cluster ${CLUSTER_NAME}"
    kind delete cluster --name "${CLUSTER_NAME}" >/dev/null 2>&1 || true
  else
    log "KEEP_CLUSTER=true: leaving cluster ${CLUSTER_NAME} up for inspection"
  fi
}
trap cleanup EXIT

for bin in kind docker kubectl helm; do
  command -v "$bin" >/dev/null 2>&1 || { echo "missing required tool: $bin" >&2; exit 1; }
done

log "creating kind cluster ${CLUSTER_NAME}"
kind create cluster --name "${CLUSTER_NAME}" --wait 120s

log "installing JobSet ${JOBSET_VERSION}"
kubectl apply --server-side -f \
  "https://github.com/kubernetes-sigs/jobset/releases/download/${JOBSET_VERSION}/manifests.yaml"

log "installing Kueue ${KUEUE_VERSION}"
kubectl apply --server-side -f \
  "https://github.com/kubernetes-sigs/kueue/releases/download/${KUEUE_VERSION}/manifests.yaml"
kubectl -n kueue-system rollout status deployment/kueue-controller-manager --timeout=180s

log "building bridge image ${IMAGE_TAG}"
docker build -t "${IMAGE_TAG}" .
kind load docker-image "${IMAGE_TAG}" --name "${CLUSTER_NAME}"

# =============================================================================
# PHASE 1 — chart-equivalent plain manifests (helm-free, original scope).
# =============================================================================
log "PHASE 1/2: chart-equivalent plain-manifest smoke (namespace ${NAMESPACE})"

kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

# --- mock slurmrestd: one static JSON response, no real Slurm anywhere ---
# Serves exactly what internal/slurm.Client.ListJobs needs: a 200 OK with an
# empty jobs/errors/warnings envelope. That's enough for the bridge's poll
# loop to complete a tick successfully (proving config+client+loop wiring)
# without needing a job to hold/release/translate (out of this reduced
# scope; see the header comment). Phase 2 reuses this same mock shape in its
# own namespace.
log "deploying mock slurmrestd (static JSON, no real Slurm)"
kubectl -n "${NAMESPACE}" create configmap mock-slurmrestd-response \
  --from-literal=jobs.json='{"jobs": [], "errors": [], "warnings": []}' \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "${NAMESPACE}" create configmap slurm-token --from-literal=token=e2e-fake-token \
  --dry-run=client -o yaml | kubectl apply -f -

cat <<'EOF' | kubectl -n "${NAMESPACE}" apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mock-slurmrestd
  labels: { app: mock-slurmrestd }
spec:
  replicas: 1
  selector: { matchLabels: { app: mock-slurmrestd } }
  template:
    metadata:
      labels: { app: mock-slurmrestd }
    spec:
      containers:
        - name: mock-slurmrestd
          image: busybox:1.36
          command: ["sh", "-c"]
          # httpd -f: foreground static file server. Any request for any
          # path under /slurm/v0.0.44/ gets the same canned jobs.json —
          # good enough for ListJobs polling; ReleaseJob/SetJob* (POST) are
          # never exercised in this reduced scope, so no routing is needed.
          args:
            - |
              mkdir -p /www/slurm/v0.0.44
              cp /cfg/jobs.json /www/slurm/v0.0.44/jobs
              httpd -f -p 6820 -h /www
          volumeMounts:
            - { name: cfg, mountPath: /cfg }
          ports:
            - { containerPort: 6820 }
      volumes:
        - name: cfg
          configMap: { name: mock-slurmrestd-response }
---
apiVersion: v1
kind: Service
metadata:
  name: mock-slurmrestd
spec:
  selector: { app: mock-slurmrestd }
  ports:
    - { port: 6820, targetPort: 6820 }
EOF
kubectl -n "${NAMESPACE}" rollout status deployment/mock-slurmrestd --timeout=120s

# --- bridge, deployed with chart-equivalent plain manifests ---
# Not `helm install` here: this phase intentionally renders the same shape as
# deploy/chart/k8s-bridge/templates (ServiceAccount, Role/RoleBinding for
# jobsets+workloads, ConfigMap-mounted file config, container args
# --config/--metrics-addr) directly, matching configSource=file mode. Phase 2
# below is what actually installs the chart.
log "deploying k8s-bridge (configSource=file, mock slurmrestd, http allowed for the in-cluster mock)"
kubectl -n "${NAMESPACE}" create configmap k8s-bridge-config --from-literal=config.yaml="$(cat <<CFG
slurmRestURL: "http://mock-slurmrestd.${NAMESPACE}.svc.cluster.local:6820"
allowInsecureHTTP: true
slurmTokenFile: "/var/run/secrets/slurm/token"
slurmUser: "e2e"
namespace: "${NAMESPACE}"
localQueue: "main"
pollInterval: 5s
partitionMappings:
  - partitionName: "mixing"
    workloadPriorityClass: "normal-priority"
slurmd:
  image: "ghcr.io/slinkyproject/slurmd:26.05-ubuntu26.04"
  confServer: "slurm-controller.slurm.svc.cluster.local:6817"
  authSecretName: "slurm-auth-slurm"
CFG
)" --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "${NAMESPACE}" create secret generic slurm-rest-token \
  --from-literal=token=e2e-fake-token --dry-run=client -o yaml | kubectl apply -f -

cat <<EOF | kubectl -n "${NAMESPACE}" apply -f -
apiVersion: v1
kind: ServiceAccount
metadata:
  name: k8s-bridge-e2e
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: k8s-bridge-e2e
rules:
  - apiGroups: ["jobset.x-k8s.io"]
    resources: ["jobsets"]
    verbs: ["get", "list", "watch", "create", "update", "delete"]
  - apiGroups: ["kueue.x-k8s.io"]
    resources: ["workloads"]
    verbs: ["get", "list", "watch", "patch"]
  # Mirror the chart's policyRules: --leader-elect defaults to true, so the
  # controller needs its coordination.k8s.io Lease (in this namespace, once
  # POD_NAMESPACE is injected above), and it emits Events on JobSets during a
  # tick. Without these, leader election is forbidden (reconcile never starts)
  # and a tick that records an Event fails — either way /readyz stays 503.
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["create", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: k8s-bridge-e2e
roleRef: { apiGroup: rbac.authorization.k8s.io, kind: Role, name: k8s-bridge-e2e }
subjects:
  - kind: ServiceAccount
    name: k8s-bridge-e2e
    namespace: ${NAMESPACE}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: k8s-bridge
  labels: { app: k8s-bridge }
spec:
  replicas: 1
  selector: { matchLabels: { app: k8s-bridge } }
  template:
    metadata:
      labels: { app: k8s-bridge }
    spec:
      serviceAccountName: k8s-bridge-e2e
      containers:
        - name: bridge
          image: ${IMAGE_TAG}
          imagePullPolicy: Never # kind-loaded, never pulled from a registry
          args: ["--config", "/etc/k8s-bridge/config.yaml", "--metrics-addr", ":8080"]
          # Inject POD_NAMESPACE via the downward API exactly as the chart's
          # Deployment does. --leader-elect defaults to true, and
          # leaderElectionNamespace() (cmd/k8s-bridge/main.go) falls back to
          # "default" when POD_NAMESPACE is unset in configSource=file mode.
          # This phase's Role grants coordination.k8s.io/leases only in its own
          # namespace, so without this env the Lease lock lands in "default"
          # and is forbidden — leader election never resolves, the reconcile
          # loop (a LeaderElectionRunnable) never starts, and /readyz stays 503.
          env:
            - name: POD_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
          ports: [{ containerPort: 8080, name: metrics }]
          volumeMounts:
            - { name: config, mountPath: /etc/k8s-bridge, readOnly: true }
            - { name: token, mountPath: /var/run/secrets/slurm, readOnly: true }
      volumes:
        - name: config
          configMap: { name: k8s-bridge-config }
        - name: token
          secret: { secretName: slurm-rest-token }
---
apiVersion: v1
kind: Service
metadata:
  name: k8s-bridge
spec:
  selector: { app: k8s-bridge }
  ports: [{ port: 8080, targetPort: 8080 }]
EOF

log "waiting for k8s-bridge rollout"
kubectl -n "${NAMESPACE}" rollout status deployment/k8s-bridge --timeout=120s

# --- assertions ---
log "phase 1 assertion 1/3: bridge pod Ready"
kubectl -n "${NAMESPACE}" wait --for=condition=Ready pod -l app=k8s-bridge --timeout=60s

log "phase 1 assertion 2/3: /metrics serves Prometheus text"
metrics_probe_pod="metrics-probe"
kubectl -n "${NAMESPACE}" delete pod "${metrics_probe_pod}" --ignore-not-found --wait=true >/dev/null 2>&1 || true
kubectl -n "${NAMESPACE}" run "${metrics_probe_pod}" --restart=Never --image=curlimages/curl:8.11.0 \
  --command -- sh -c "curl -sf http://k8s-bridge:8080/metrics | head -1" >/dev/null
kubectl -n "${NAMESPACE}" wait --for=condition=Ready pod/"${metrics_probe_pod}" --timeout=30s
metrics_output=$(kubectl -n "${NAMESPACE}" logs "${metrics_probe_pod}")
kubectl -n "${NAMESPACE}" delete pod "${metrics_probe_pod}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
if [ -z "${metrics_output}" ]; then
  echo "FAIL: /metrics returned no output" >&2
  exit 1
fi
log "  /metrics first line: ${metrics_output}"

log "phase 1 assertion 3/3: /readyz returns 200 (proves a successful tick against the mock slurmrestd)"
readyz_probe_pod="readyz-probe"
# Poll readyz for up to ~30s: pollInterval is 5s, readiness needs one
# successful tick to complete first.
ready_ok="false"
for _ in $(seq 1 10); do
  kubectl -n "${NAMESPACE}" delete pod "${readyz_probe_pod}" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  kubectl -n "${NAMESPACE}" run "${readyz_probe_pod}" --restart=Never --image=curlimages/curl:8.11.0 \
    --command -- sh -c "curl -sf -o /dev/null -w '%{http_code}' http://k8s-bridge:8080/readyz" >/dev/null
  kubectl -n "${NAMESPACE}" wait --for=jsonpath='{.status.phase}'=Succeeded pod/"${readyz_probe_pod}" --timeout=15s >/dev/null 2>&1 || true
  code=$(kubectl -n "${NAMESPACE}" logs "${readyz_probe_pod}" 2>/dev/null || true)
  if [ "${code}" = "200" ]; then
    ready_ok="true"
    break
  fi
  sleep 3
done
kubectl -n "${NAMESPACE}" delete pod "${readyz_probe_pod}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
if [ "${ready_ok}" != "true" ]; then
  echo "FAIL: /readyz never returned 200 (last code: ${code:-<none>})" >&2
  kubectl -n "${NAMESPACE}" logs deployment/k8s-bridge --tail=50 || true
  exit 1
fi
log "  /readyz returned 200"
log "PHASE 1 PASSED (plain manifests: bridge Ready + /metrics + /readyz)"

# =============================================================================
# PHASE 2 — CHART-DEPLOY SMOKE (this task). Installs the real Helm chart.
# =============================================================================
# Exists specifically to catch chart-deploy regressions that unit tests and
# the envtest integration suite structurally cannot, because neither of those
# ever runs `helm install`, applies the chart's securityContext, or is bound
# by the chart's namespaced Role.
# Concretely this phase exercises, end to end, against the REAL chart:
#   - B1 (leader-election Lease namespace): --leader-elect=true here, and the
#     chart's POD_NAMESPACE downward-API wiring must resolve the Lease to
#     ${CHART_NAMESPACE}, matching the Role's scope, or the pod can never win
#     the election and never becomes Ready.
#   - B2 (token Secret unreadable by nonroot container): the real
#     securityContext (runAsNonRoot + fsGroup 65532) plus the real Secret
#     defaultMode 0440 the chart's Deployment template sets — a distroless
#     nonroot UID that cannot read the token surfaces as a Ready timeout here,
#     exactly as it did live.
#   - B3 (controller-runtime logging wired) and B8 (cache scoped to the
#     namespace the chart's Role actually grants, not cluster scope): both
#     manifest as the same failure mode from the outside — the Deployment
#     never reaches Ready because the informer cache never syncs against the
#     namespaced-only RBAC this chart renders (rbac.namespaced=true, the
#     chart default) — so the Ready assertion below is this phase's proof
#     these still work, not just B1/B2.
#   - B5 (checksum/config annotation): not directly exercised by a fresh
#     install (checksum/config only forces a *re*-roll on `helm upgrade`),
#     but the annotation's presence is asserted directly below so a future
#     removal of it is still caught here rather than only on the next live
#     `helm upgrade`.
log "PHASE 2/2: chart-deploy smoke (namespace ${CHART_NAMESPACE}, release ${CHART_RELEASE})"

kubectl create namespace "${CHART_NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

log "deploying mock slurmrestd for the chart phase (static JSON, no real Slurm)"
kubectl -n "${CHART_NAMESPACE}" create configmap mock-slurmrestd-response \
  --from-literal=jobs.json='{"jobs": [], "errors": [], "warnings": []}' \
  --dry-run=client -o yaml | kubectl apply -f -

cat <<EOF | kubectl -n "${CHART_NAMESPACE}" apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mock-slurmrestd
  labels: { app: mock-slurmrestd }
spec:
  replicas: 1
  selector: { matchLabels: { app: mock-slurmrestd } }
  template:
    metadata:
      labels: { app: mock-slurmrestd }
    spec:
      containers:
        - name: mock-slurmrestd
          image: busybox:1.36
          command: ["sh", "-c"]
          args:
            - |
              mkdir -p /www/slurm/v0.0.44
              cp /cfg/jobs.json /www/slurm/v0.0.44/jobs
              httpd -f -p 6820 -h /www
          volumeMounts:
            - { name: cfg, mountPath: /cfg }
          ports:
            - { containerPort: 6820 }
      volumes:
        - name: cfg
          configMap: { name: mock-slurmrestd-response }
---
apiVersion: v1
kind: Service
metadata:
  name: mock-slurmrestd
spec:
  selector: { app: mock-slurmrestd }
  ports:
    - { port: 6820, targetPort: 6820 }
EOF
kubectl -n "${CHART_NAMESPACE}" rollout status deployment/mock-slurmrestd --timeout=120s

# --- pre-create the objects the chart itself does not template ---
# The chart mounts a Secret (slurmTokenSecret, chart-created ConfigMap/Role/
# Deployment aside) and expects it to already exist — same division of
# responsibility as a real install, where the token is provisioned
# out-of-band (e.g. by a platform team's secret-management flow), not by this
# chart. Mode/ownership are irrelevant here: the chart's Deployment sets
# defaultMode: 0440 on the volume mount regardless of how the Secret itself
# was created, so the fsGroup fix (B2) is exercised either way.
log "pre-creating slurm-rest-token Secret (chart expects this to exist, does not template it)"
kubectl -n "${CHART_NAMESPACE}" create secret generic slurm-rest-token \
  --from-literal=token=e2e-fake-token --dry-run=client -o yaml | kubectl apply -f -

# wm-gres-conf and a copied slurm-auth-slurm Secret: these are what
# experiments/09-scale-gpu-churn/scripts/05-namespace-prereqs.sh pre-creates
# in the bridge's namespace before a live run, for the slurmd JobSet members
# the bridge would translate real held jobs into. This reduced scope has no
# real Slurm job to translate (mock slurmrestd returns an empty job list), so
# no JobSet/slurmd pod ever mounts them — but pre-creating them here matches
# what a real namespace looks like before this chart is installed into it,
# per this task's brief, and costs nothing to assert are present.
log "pre-creating wm-gres-conf ConfigMap and a fake slurm-auth-slurm Secret (parity with a real namespace, not consumed by this reduced scope)"
kubectl -n "${CHART_NAMESPACE}" create configmap wm-gres-conf \
  --from-literal=gres.conf="Name=gpu Count=1
" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "${CHART_NAMESPACE}" create secret generic slurm-auth-slurm \
  --from-literal=slurm.key=e2e-fake-munge-key --dry-run=client -o yaml | kubectl apply -f -

# --- install the actual chart ---
# rbac.namespaced defaults to true in values.yaml (least privilege, security
# audit H4) — left at its default here so this smoke exercises the SAME RBAC
# shape a real namespaced install gets, which is exactly the shape B8's
# cluster-scoped cache collided with. leaderElect is also left at its
# values.yaml default (true, ADR-0011) for the same reason: this smoke must
# run with the chart's real defaults, not a permissive test-only override,
# or it stops being able to catch chart-default regressions.
log "helm install ${CHART_RELEASE} (chart deploy/chart/k8s-bridge, configSource=file, real securityContext + namespaced RBAC + leader election)"
helm install "${CHART_RELEASE}" "${CHART_DIR}" \
  --namespace "${CHART_NAMESPACE}" \
  --set image.repository="${IMAGE_TAG%%:*}" \
  --set image.tag="${IMAGE_TAG##*:}" \
  --set image.pullPolicy=Never \
  --set slurmTokenSecret=slurm-rest-token \
  --set config.namespace="${CHART_NAMESPACE}" \
  --set config.slurmRestURL="http://mock-slurmrestd.${CHART_NAMESPACE}.svc.cluster.local:6820" \
  --set config.allowInsecureHTTP=true \
  --set config.slurmUser=e2e \
  --set config.pollInterval=5s \
  --set config.slurmTokenFile=/var/run/secrets/slurm/token \
  --set config.slurmd.image=ghcr.io/slinkyproject/slurmd:26.05-ubuntu26.04 \
  --set config.slurmd.confServer=slurm-controller.slurm.svc.cluster.local:6817 \
  --set config.slurmd.authSecretName=slurm-auth-slurm \
  --wait --timeout=180s

log "waiting for the chart's Deployment rollout explicitly (helm --wait already gated on this, kept as a named checkpoint)"
kubectl -n "${CHART_NAMESPACE}" rollout status "deployment/${CHART_RELEASE}" --timeout=60s

log "phase 2 assertion (a): bridge Deployment reached Ready — this alone proves B1 (leader-election namespace), B2 (token fsGroup), B8 (cache scope vs namespaced RBAC): if any of those regressed, the pod would never reach Ready and the rollout-status call above would already have failed"
kubectl -n "${CHART_NAMESPACE}" wait --for=condition=Ready \
  pod -l "app.kubernetes.io/instance=${CHART_RELEASE}" --timeout=60s

log "phase 2 assertion (a-checksum): Deployment carries a checksum/config annotation (B5) — its presence is what forces a rolling restart on a config-only helm upgrade"
checksum_annotation=$(kubectl -n "${CHART_NAMESPACE}" get "deployment/${CHART_RELEASE}" \
  -o jsonpath='{.spec.template.metadata.annotations.checksum/config}')
if [ -z "${checksum_annotation}" ]; then
  echo "FAIL: chart Deployment has no checksum/config annotation (B5 regression — config-only upgrades would silently not roll the pod)" >&2
  exit 1
fi
log "  checksum/config: ${checksum_annotation}"

BRIDGE_SVC_URL="http://${CHART_RELEASE}:8080"
# The chart does not template a Service — kubectl port-forward or an in-cluster
# probe pod both need a stable DNS name, so create a minimal one here that
# just selects the chart's pod by its standard labels. This is scaffolding for
# this test, not something the chart is expected to provide.
cat <<EOF | kubectl -n "${CHART_NAMESPACE}" apply -f -
apiVersion: v1
kind: Service
metadata:
  name: ${CHART_RELEASE}
spec:
  selector:
    app.kubernetes.io/instance: ${CHART_RELEASE}
  ports: [{ port: 8080, targetPort: 8080, name: metrics }]
EOF

log "phase 2 assertion (b): /metrics serves Prometheus text through the chart's real Deployment"
metrics_probe_pod="chart-metrics-probe"
# A --restart=Never curl pod runs to completion, so it reports phase=Succeeded,
# not condition=Ready — wait on the phase and retry, mirroring assertion (d)
# below. The previous single-shot `--for=condition=Ready` was racy: it could
# read empty logs before the metrics server accepted the connection.
chart_metrics_output=""
for _ in $(seq 1 10); do
  kubectl -n "${CHART_NAMESPACE}" delete pod "${metrics_probe_pod}" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  kubectl -n "${CHART_NAMESPACE}" run "${metrics_probe_pod}" --restart=Never --image=curlimages/curl:8.11.0 \
    --command -- sh -c "curl -sf ${BRIDGE_SVC_URL}/metrics" >/dev/null
  kubectl -n "${CHART_NAMESPACE}" wait --for=jsonpath='{.status.phase}'=Succeeded pod/"${metrics_probe_pod}" --timeout=15s >/dev/null 2>&1 || true
  chart_metrics_output=$(kubectl -n "${CHART_NAMESPACE}" logs "${metrics_probe_pod}" 2>/dev/null || true)
  if [ -n "${chart_metrics_output}" ]; then
    break
  fi
  sleep 3
done
kubectl -n "${CHART_NAMESPACE}" delete pod "${metrics_probe_pod}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
if [ -z "${chart_metrics_output}" ]; then
  echo "FAIL: chart's /metrics returned no output" >&2
  exit 1
fi

log "phase 2 assertion (c): at least one successful tick completed (k8s_bridge_ticks_total > 0), proving the poll loop + slurmrestd client + config loading work together under the chart's real config"
ticks_total=$(echo "${chart_metrics_output}" | grep -E '^k8s_bridge_ticks_total ' | awk '{print $2}' || true)
ticks_total=${ticks_total%.*} # metrics text format prints counters as floats ("3" -> "3", "3.0" -> "3")
if [ -z "${ticks_total}" ] || [ "${ticks_total}" -lt 1 ] 2>/dev/null; then
  echo "FAIL: k8s_bridge_ticks_total is '${ticks_total:-<absent>}', expected >= 1" >&2
  echo "${chart_metrics_output}" | grep -E '^k8s_bridge_' || true
  kubectl -n "${CHART_NAMESPACE}" logs "deployment/${CHART_RELEASE}" --tail=80 || true
  exit 1
fi
log "  k8s_bridge_ticks_total=${ticks_total}"

log "phase 2 assertion (d): /readyz returns 200 (readiness gate agrees a tick succeeded)"
readyz_probe_pod="chart-readyz-probe"
ready_ok="false"
for _ in $(seq 1 10); do
  kubectl -n "${CHART_NAMESPACE}" delete pod "${readyz_probe_pod}" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  kubectl -n "${CHART_NAMESPACE}" run "${readyz_probe_pod}" --restart=Never --image=curlimages/curl:8.11.0 \
    --command -- sh -c "curl -sf -o /dev/null -w '%{http_code}' ${BRIDGE_SVC_URL}/readyz" >/dev/null
  kubectl -n "${CHART_NAMESPACE}" wait --for=jsonpath='{.status.phase}'=Succeeded pod/"${readyz_probe_pod}" --timeout=15s >/dev/null 2>&1 || true
  code=$(kubectl -n "${CHART_NAMESPACE}" logs "${readyz_probe_pod}" 2>/dev/null || true)
  if [ "${code}" = "200" ]; then
    ready_ok="true"
    break
  fi
  sleep 3
done
kubectl -n "${CHART_NAMESPACE}" delete pod "${readyz_probe_pod}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
if [ "${ready_ok}" != "true" ]; then
  echo "FAIL: chart's /readyz never returned 200 (last code: ${code:-<none>})" >&2
  kubectl -n "${CHART_NAMESPACE}" logs "deployment/${CHART_RELEASE}" --tail=80 || true
  exit 1
fi
log "  /readyz returned 200"

log "PHASE 2 PASSED (real Helm chart: Deployment Ready under real securityContext+RBAC+leader-election, checksum/config present, /metrics + /readyz serve, ticks_total >= 1)"

log "ALL ASSERTIONS PASSED (phase 1: plain manifests; phase 2: real Helm chart deploy smoke — see script header for exactly which live-deploy bugs, B1/B2/B3/B5/B8, this run guards against). No real Slurm job lifecycle is exercised in either phase — see script header."
