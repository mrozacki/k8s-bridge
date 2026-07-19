#!/usr/bin/env bash
# REAL-Slurm kind e2e: the full hold -> translate -> admit -> register ->
# release -> run -> cleanup lifecycle against a REAL slurmctld + slurmrestd.
#
# This is the promotion run.sh's header defers ("Promoting to the full
# DEMO.md scope on kind ... remains open — see backlog AUD5/A5"): run.sh
# proves the bridge deploys and ticks against a MOCK slurmrestd with an empty
# job list, so it structurally cannot catch a regression in anything the
# bridge actually exists for — hold detection, translation, the
# admission-gated release, node pinning, or cleanup. This script exercises
# exactly that lifecycle (mirroring the manual test plan's TC-B3 golden path
# and TC-B7's hold-detection assertion — see docs/VALIDATION.md) with
# no GKE bill attached.
#
# WHY THIS DOES NOT HIT THE PRIVILEGED-SLURMD TRAP (run.sh header; security
# audit threat-model T7): the reason Slinky-on-kind was rejected for the
# nightly is that the slurm-operator's slurmd nodesets run privileged and
# want writable cgroups — privileged-in-privileged on kind's
# container-nodes is flaky. This design sidesteps both halves of that:
#
#   1. The Slurm CONTROL PLANE (slurmctld + slurmrestd) runs OUTSIDE kind,
#      as two plain docker containers attached to the `kind` docker network.
#      Neither daemon needs cgroups, privileges, or the Slinky operator —
#      they get a hand-written, cgroup-free slurm.conf (see
#      test/e2e/slurm/slurm.conf) and per-run generated auth keys.
#   2. The slurmd pods the BRIDGE creates inside kind run UNPRIVILEGED
#      (chart --set config.slurmd.privileged=false): the slurm.conf they
#      fetch via configless sets ProctrackType=proctrack/pgid and
#      TaskPlugin=task/none, so slurmd never loads a cgroup plugin and has
#      nothing to ask privileges for. The chart's privileged=true DEFAULT is
#      an upstream slurmd/cgroup constraint (config.go Slurmd.Privileged) —
#      constraint removed, default overridden, and this run doubles as the
#      first exercise of the minimal-capability securityContext path.
#
# TOPOLOGY AND REACHABILITY (the honest part — read before debugging):
#
#     docker network "kind" (172.18.0.0/16 by default)
#     +----------------+   +-----------------+   +---------------------+
#     | slurmctld      |   | slurmrestd      |   | kind node(s)        |
#     | :6817          |   | :6820           |   |  pods 10.244.0.0/16 |
#     +----------------+   +-----------------+   +---------------------+
#
#   - bridge pod -> slurmrestd, slurmd pod -> slurmctld: pods reach docker-
#     network IPs through their node (kind's CNI masquerades to the node
#     IP). Stable DNS names are provided by selectorless Services + manual
#     EndpointSlices ("slurmctld", "slurm-restapi") pointing at the
#     containers' docker IPs; the bare name "slurmctld" in slurm.conf's
#     SlurmctldHost resolves on BOTH sides (docker's embedded DNS between
#     containers, the Service via the pods' DNS search path inside kind).
#     This direction is solid.
#   - slurmctld -> slurmd pod (the batch-launch RPC after release): NOT
#     given by default, and NAT breaks the two documented Slurm answers:
#     `cloud_reg_addrs` would record the REGISTRATION source address, but
#     the pod's registration arrives masqueraded as the NODE's IP, so
#     slurmctld would call back to the wrong place; plain hostname
#     resolution fails because docker containers don't use cluster DNS.
#     This script therefore does two things instead:
#       (a) adds a route for the pod CIDR via the kind node into the
#           slurmctld container's netns (nsenter from the host, falling
#           back to `docker exec ip route add` if the image ships
#           iproute2), so pod IPs are directly reachable, and
#       (b) injects "podIP podHostname" lines into the slurmctld
#           container's /etc/hosts as soon as the slurmd pods get IPs, so
#           slurmctld's resolver finds the deterministic node names
#           (translate.NodeNames == pod hostnames).
#     `CommunicationParameters=NoAddrCache` keeps slurmctld from caching a
#     stale answer. This is the one genuinely novel piece of this setup and
#     it can only be PROVEN on the runner — hence the soft stages below.
#
# STAGED ASSERTIONS S1..S7 (each logged as a hard gate):
#   S1  slurmrestd answers an authenticated GET /slurm/v0.0.44/jobs.
#   S2  `sbatch --hold --partition=mixing` lands and the API reports it held
#       in exactly the shape internal/slurm.IsHeld accepts (TC-B7's check).
#   S3  the bridge creates JobSet slurm-job-<id> (translate + tick loop).
#   S4  Kueue admits the Workload (Admitted=True) under the test quota.
#   S5  the slurmd pod starts and registers as a dynamic Slurm node.
#   S6  the bridge pins + releases the job; JobState reaches RUNNING then
#       COMPLETED (SetJobFeatures succeeding is the registration signal, so
#       a release proves the whole chain).
#   S7  after completion the bridge deletes the JobSet and the node record.
# S1-S4 always fail the run. S5-S7 are SOFT by default (logged loudly,
# diagnostics collected, exit 0) because they depend on the
# dynamic-node-behind-NAT wiring above, which no local environment available
# to this change could execute; set HARD_ALL=true to make them hard. The
# workflow (.github/workflows/e2e-slurm.yaml) flips to HARD_ALL=true once
# the first dispatch run validates them — see its header.
#
# VALIDATION LOG:
#   - 2026-07-11 (authored): NOT yet executed; bash -n only.
#   - 2026-07-11 (validated locally): 17 iterative runs on macOS/Docker
#     Desktop (arm64 + Rosetta for the amd64 Slurm images); run 17 = ALL
#     STAGES S1-S7 GREEN with zero manual intervention. Every fix the runs
#     forced is documented at its site (grep "run-N finding"); highlights:
#     SLURM_JWT=daemon for slurmrestd (run 1/2), local single-platform
#     image rebuild for kind load (runs 3/4), image-allowlist-compliant tag
#     (run 6 — the chart's secure default correctly refused a foreign name),
#     slurmd caps SETUID/SETGID/CHOWN in translate.go (runs 7-16, bisected
#     live — a PRODUCT fix), CredType=cred/slurm + CgroupPlugin=disabled
#     (runs 9/14), masquerade exemption + routes in both control containers
#     (runs 10-13), admin (SlurmUser) REST identity for node-record
#     deletion (run 16 — explains the 2026-07-06 live 422 "Invalid user
#     id"). Slinky-image assumptions (entrypoint override, shipped
#     binaries, FUTURE-node submit, generated auth keys) all confirmed.
#   - 2026-07-12 (first runner dispatch, run 29181061822): S1-S4 PASSED on
#     ubuntu-latest; S5 soft-failed for a capacity reason, not a networking
#     one — the worker pod sat Pending with "Insufficient cpu" because the
#     single kind node's 4-vCPU allocatable was consumed by the control
#     plane + Kueue + JobSet + bridge. Fixed by the two-node topology below
#     plus per-node podCIDR routes (a blanket route via the control plane
#     broke cross-node replies — run 18) and per-node masquerade exemptions.
#     Two-node flow re-validated locally: ALL S1-S7 GREEN (run 19).
#   - 2026-07-12 (runner round 2, run 29182310159): still S5, but a NEW
#     capacity shape — multi-node kind KEEPS the control-plane NoSchedule
#     taint (single-node kind removes it), so every infra pod piled onto
#     the one schedulable worker and the job was squeezed out again. Fixed
#     by untainting after cluster create; re-validated locally ALL S1-S7
#     GREEN (run 20).
#   - 2026-07-12 (runner round 3, run 29183294715): ALL STAGES S1-S7
#     GREEN on ubuntu-latest — the full lifecycle now validates on stock CI
#     infrastructure. HARD_ALL flipped to "true" in the workflow and the
#     scheduled run's continue-on-error grace removed the same day; every
#     stage is a hard nightly gate from here on.
#
# Local run: `make e2e-kind-real-slurm` from .. Requires:
# kind, docker, kubectl, helm, jq, openssl on PATH, and a docker host whose
# containers can be nsenter'd (Linux; on Docker Desktop/macOS the route
# fallback path applies and S5-S7 may soft-fail — the CI runner is the
# reference environment).
#
# Location rationale: same as run.sh — needs the Go module to build the
# bridge image and reaches up to deploy/chart for the real chart install.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.." # -> repo root
REPO_ROOT="$(pwd)"
CHART_DIR="${REPO_ROOT}/deploy/chart/k8s-bridge"
SLURM_ASSETS_DIR="$(pwd)/test/e2e/slurm"

# Distinct names from run.sh throughout, so the two e2e variants can coexist
# on one docker host without clobbering each other's clusters/containers.
CLUSTER_NAME="${CLUSTER_NAME:-k8s-bridge-e2e-slurm}"
NAMESPACE="${NAMESPACE:-slurm-jobs}"
CHART_RELEASE="${CHART_RELEASE:-k8s-bridge-e2e}"
IMAGE_TAG="${IMAGE_TAG:-k8s-bridge:e2e-slurm}"

# Pinned versions: JobSet/Kueue match run.sh (which matches
# experiments/01-gke-playground/scripts/00-env.sh); the Slurm images match
# the live Slinky deployment (experiments/01-gke-playground) and the chart's
# own config.slurmd.image default, so the CONTROL PLANE and the slurmd pods
# run the same Slurm 26.05 build — a version-skewed control plane would test
# a mix no deployment runs.
JOBSET_VERSION="${JOBSET_VERSION:-v0.12.0}"
KUEUE_VERSION="${KUEUE_VERSION:-v0.18.2}"
SLURMCTLD_IMAGE="${SLURMCTLD_IMAGE:-ghcr.io/slinkyproject/slurmctld:26.05-ubuntu26.04}"
# NEEDS-RUNNER-VALIDATION: the slurmrestd image tag is assumed to follow the
# same scheme as slurmctld/slurmd (both confirmed live in experiments 01/02).
SLURMRESTD_IMAGE="${SLURMRESTD_IMAGE:-ghcr.io/slinkyproject/slurmrestd:26.05-ubuntu26.04}"
SLURMD_IMAGE="${SLURMD_IMAGE:-ghcr.io/slinkyproject/slurmd:26.05-ubuntu26.04}"
CURL_IMAGE="${CURL_IMAGE:-curlimages/curl:8.11.0}" # same pin as run.sh's probe pods

# kind's default cluster pod CIDR; only override if the cluster is created
# with a non-default networking.podSubnet.
POD_SUBNET="${POD_SUBNET:-10.244.0.0/16}"

# HARD_ALL=true promotes the soft stages S5-S7 to hard failures (see header).
HARD_ALL="${HARD_ALL:-false}"
KEEP_CLUSTER="${KEEP_CLUSTER:-false}" # true: keep cluster AND slurm containers
# Diagnostics land here (workflow uploads this directory as an artifact).
DIAG_DIR="${DIAG_DIR:-$(pwd)/e2e-slurm-diagnostics}"

CTLD_CONTAINER="e2e-slurmctld"
RESTD_CONTAINER="e2e-slurmrestd"

log() { echo "==> $*"; }

WORKDIR="$(mktemp -d)"

cleanup() {
  if [ "${KEEP_CLUSTER}" != "true" ]; then
    log "tearing down kind cluster ${CLUSTER_NAME} and slurm containers"
    kind delete cluster --name "${CLUSTER_NAME}" >/dev/null 2>&1 || true
    docker rm -f "${CTLD_CONTAINER}" "${RESTD_CONTAINER}" >/dev/null 2>&1 || true
  else
    log "KEEP_CLUSTER=true: leaving cluster ${CLUSTER_NAME} and containers ${CTLD_CONTAINER}/${RESTD_CONTAINER} up for inspection"
  fi
  rm -rf "${WORKDIR}"
}
# Run-5 finding: a bare command failing under `set -e` (e.g. a helm --wait
# timeout) used to exit through the trap WITHOUT diagnostics — stage_fail
# was the only collection point, so exactly the least-expected failures
# left nothing behind. Collect on every non-zero exit instead.
on_exit() {
  rc=$?
  if [ "${rc}" -ne 0 ]; then
    collect_diagnostics || true
  fi
  cleanup
}
trap on_exit EXIT

for bin in kind docker kubectl helm jq openssl; do
  command -v "$bin" >/dev/null 2>&1 || { echo "missing required tool: $bin" >&2; exit 1; }
done

# --- diagnostics: collected on ANY stage failure (and uploadable by CI) ---
DIAG_COLLECTED="false"
collect_diagnostics() {
  [ "${DIAG_COLLECTED}" = "true" ] && return 0
  DIAG_COLLECTED="true"
  log "collecting diagnostics into ${DIAG_DIR}"
  mkdir -p "${DIAG_DIR}"
  docker logs --tail 500 "${CTLD_CONTAINER}" >"${DIAG_DIR}/slurmctld.log" 2>&1 || true
  docker logs --tail 500 "${RESTD_CONTAINER}" >"${DIAG_DIR}/slurmrestd.log" 2>&1 || true
  docker exec "${CTLD_CONTAINER}" scontrol show jobs >"${DIAG_DIR}/scontrol-jobs.txt" 2>&1 || true
  docker exec "${CTLD_CONTAINER}" scontrol show nodes >"${DIAG_DIR}/scontrol-nodes.txt" 2>&1 || true
  docker exec "${CTLD_CONTAINER}" cat /etc/hosts >"${DIAG_DIR}/slurmctld-etc-hosts.txt" 2>&1 || true
  kubectl get pods -A -o wide >"${DIAG_DIR}/pods.txt" 2>&1 || true
  kubectl -n "${NAMESPACE}" get jobsets,workloads,events -o wide >"${DIAG_DIR}/namespace-objects.txt" 2>&1 || true
  kubectl -n "${NAMESPACE}" get jobsets -o yaml >"${DIAG_DIR}/jobsets.yaml" 2>&1 || true
  kubectl -n "${NAMESPACE}" get workloads -o yaml >"${DIAG_DIR}/workloads.yaml" 2>&1 || true
  kubectl -n "${NAMESPACE}" describe pods >"${DIAG_DIR}/describe-pods.txt" 2>&1 || true
  kubectl -n "${NAMESPACE}" logs "deployment/${CHART_RELEASE}" --tail=300 >"${DIAG_DIR}/bridge.log" 2>&1 || true
  # Run-7 gap: S5 failed with a Running-but-unregistered slurmd pod and no
  # slurmd logs anywhere in the artifact — the JobSet worker pods are the
  # protagonists of S5/S6 and must be captured individually.
  for pod in $(kubectl -n "${NAMESPACE}" get pods -o name 2>/dev/null); do
    kubectl -n "${NAMESPACE}" logs "${pod}" --all-containers --tail=200 \
      >"${DIAG_DIR}/pod-$(basename "${pod}").log" 2>&1 || true
  done
  kubectl -n "${NAMESPACE}" get endpointslices -o yaml >"${DIAG_DIR}/endpointslices.yaml" 2>&1 || true
  kubectl -n kueue-system logs deployment/kueue-controller-manager --tail=200 >"${DIAG_DIR}/kueue.log" 2>&1 || true
  # Echo the two most decision-relevant files into the job log so a CI
  # failure is triageable without downloading the artifact.
  echo "----- bridge log (tail) -----";       tail -n 60 "${DIAG_DIR}/bridge.log" 2>/dev/null || true
  echo "----- slurmctld log (tail) -----";    tail -n 60 "${DIAG_DIR}/slurmctld.log" 2>/dev/null || true
}

# stage_fail STAGE MESSAGE — the single exit point for a failed assertion.
# S1-S4 (and everything with HARD_ALL=true) fail the run; S5-S7 soft-fail:
# loud banner, diagnostics, exit 0 — because those stages assert the
# dynamic-node-behind-NAT wiring that only the runner can prove (header).
stage_fail() {
  local stage="$1" msg="$2"
  echo "!! STAGE ${stage} FAILED: ${msg}" >&2
  collect_diagnostics || true
  case "${stage}" in
    S5|S6|S7)
      if [ "${HARD_ALL}" != "true" ]; then
        echo "=======================================================================" >&2
        echo "SOFT-FAIL: stage ${stage} failed but is soft by default (HARD_ALL!=true)." >&2
        echo "S1-S4 PASSED: hold detection, translation, JobSet creation and Kueue" >&2
        echo "admission all work against the real Slurm control plane. What failed" >&2
        echo "is the dynamic-node stage that needs runner validation — see the" >&2
        echo "script header ('TOPOLOGY AND REACHABILITY') and ${DIAG_DIR}." >&2
        echo "Rerun with HARD_ALL=true to make this a hard failure." >&2
        echo "=======================================================================" >&2
        exit 0
      fi
      ;;
  esac
  exit 1
}

stage_pass() { log "STAGE $1 PASSED: $2"; }

# =============================================================================
# SETUP 1/5 — kind cluster + JobSet + Kueue (same pins as run.sh).
# =============================================================================
log "creating kind cluster ${CLUSTER_NAME}"
# Two nodes, not kind's single-node default: first runner validation
# (2026-07-12, run 29181061822) had the worker pod Pending its whole life
# with "0/1 nodes are available: 1 Insufficient cpu" — on a 4-vCPU GitHub
# runner the lone node's allocatable is ~consumed by the control plane +
# Kueue + JobSet + the bridge, leaving less than the 1 full CPU the
# translated job requests. A dedicated worker node carries only kindnet/
# kube-proxy (~200m), so the job's request always fits, and the topology is
# closer to production anyway (workloads do not run on control-plane
# nodes). CPU is oversubscribed against the same host — irrelevant for a
# sleep job.
kind create cluster --name "${CLUSTER_NAME}" --wait 120s --config /dev/stdin <<'KINDCFG'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
KINDCFG
# Untaint the control plane: single-node kind removes the
# node-role.kubernetes.io/control-plane:NoSchedule taint itself, but
# MULTI-node kind keeps it — so on the 4-vCPU runner every workload (Kueue,
# JobSet, the bridge, and the job's slurmd pod) piled onto the one
# schedulable worker and S5 was back to "Insufficient cpu" (runner round 2,
# run 29182310159; round 1's fix added capacity that the taint then fenced
# off). Untainting restores single-node kind's scheduling behavior while
# keeping the second node's allocatable — the infra pods spread across both
# nodes and the job's 1-CPU request always fits. `|| true`: the taint may
# legitimately be absent (future kind versions), and untainting twice errors.
kubectl taint nodes --all node-role.kubernetes.io/control-plane:NoSchedule- 2>/dev/null || true

log "installing JobSet ${JOBSET_VERSION}"
kubectl apply --server-side -f \
  "https://github.com/kubernetes-sigs/jobset/releases/download/${JOBSET_VERSION}/manifests.yaml"
# Wait for the JobSet controller (and thus its mutating webhook) explicitly:
# run-8 showed the bridge's first JobSet create bouncing off "failed calling
# webhook mjobset.kb.io ... connection refused" while the controller was
# still starting. The bridge retries next tick, so it self-heals — but a
# slow-starting webhook eats into S3's timeout budget for no reason.
kubectl -n jobset-system rollout status deployment/jobset-controller-manager --timeout=180s

log "installing Kueue ${KUEUE_VERSION}"
kubectl apply --server-side -f \
  "https://github.com/kubernetes-sigs/kueue/releases/download/${KUEUE_VERSION}/manifests.yaml"
kubectl -n kueue-system rollout status deployment/kueue-controller-manager --timeout=180s

# =============================================================================
# SETUP 2/5 — Slurm control plane OUTSIDE kind, on the kind docker network.
# =============================================================================
# Per-run key material. slurm.key is the auth/slurm cluster key (arbitrary
# random bytes, munge-key-sized); jwt_hs256.key is the HS256 signing key for
# auth/jwt REST tokens. Generated fresh each run: nothing to rotate, nothing
# secret to commit, and a leaked key from one run unlocks nothing.
log "generating per-run slurm.key and jwt_hs256.key"
cp "${SLURM_ASSETS_DIR}/slurm.conf" "${WORKDIR}/slurm.conf"
cp "${SLURM_ASSETS_DIR}/cgroup.conf" "${WORKDIR}/cgroup.conf"
cp "${SLURM_ASSETS_DIR}/slurmctld-init.sh" "${WORKDIR}/slurmctld-init.sh"
cp "${SLURM_ASSETS_DIR}/slurmrestd-init.sh" "${WORKDIR}/slurmrestd-init.sh"
chmod +x "${WORKDIR}/slurmctld-init.sh" "${WORKDIR}/slurmrestd-init.sh"
openssl rand -out "${WORKDIR}/slurm.key" 1024
openssl rand -out "${WORKDIR}/jwt_hs256.key" 32

# The `kind` docker network exists once the cluster above is up. --hostname
# slurmctld makes slurm.conf's SlurmctldHost=slurmctld self-consistent, and
# docker's embedded DNS resolves the container name for slurmrestd.
log "starting slurmctld (${SLURMCTLD_IMAGE}) on the kind docker network"
docker run -d --name "${CTLD_CONTAINER}" --hostname slurmctld --network kind \
  --network-alias slurmctld \
  -v "${WORKDIR}:/bootstrap:ro" \
  --entrypoint /bin/bash \
  "${SLURMCTLD_IMAGE}" /bootstrap/slurmctld-init.sh

log "starting slurmrestd (${SLURMRESTD_IMAGE}) on the kind docker network"
docker run -d --name "${RESTD_CONTAINER}" --hostname slurmrestd --network kind \
  --network-alias slurmrestd \
  -v "${WORKDIR}:/bootstrap:ro" \
  --entrypoint /bin/bash \
  "${SLURMRESTD_IMAGE}" /bootstrap/slurmrestd-init.sh

CTLD_IP="$(docker inspect -f '{{.NetworkSettings.Networks.kind.IPAddress}}' "${CTLD_CONTAINER}")"
RESTD_IP="$(docker inspect -f '{{.NetworkSettings.Networks.kind.IPAddress}}' "${RESTD_CONTAINER}")"
NODE_IP="$(docker inspect -f '{{.NetworkSettings.Networks.kind.IPAddress}}' "${CLUSTER_NAME}-control-plane")"
log "container IPs: slurmctld=${CTLD_IP} slurmrestd=${RESTD_IP} kind-node=${NODE_IP}"

# Control-plane -> pod routes (header, reachability point (a)). BOTH Slurm
# containers need the pod-CIDR route: slurmctld for its RPCs to registered
# slurmd nodes, and slurmrestd because — once the masquerade exemption below
# is active — the BRIDGE pod's REST traffic arrives from its real pod IP and
# the replies need a way back (run-12 finding: with the exemption in place
# and only slurmctld routed, every bridge tick timed out against
# slurm-restapi and the chart never reached Ready).
#
# Routes are PER NODE, not one blanket ${POD_SUBNET} route: with the
# two-node topology (runner-capacity fix), a single route via the
# control-plane node only reaches control-plane-local pods — traffic for a
# worker-node pod would need the control plane to forward across nodes, and
# run 18 showed those replies never arrive (the bridge, scheduled on the
# worker, timed out every tick against slurm-restapi). Per-node routes to
# each node's ACTUAL podCIDR keep every path symmetric and one hop.
#
# Primary path: nsenter into the container's netns from the host — works on
# any Linux docker host (the CI runner) regardless of what the image ships.
# Fallbacks: `docker exec ip route add` (needs iproute2 in the image), then
# a throwaway busybox sharing the container's netns with NET_ADMIN (run-1
# finding: the Slinky images ship no iproute2 and the macOS docker host has
# no nsenter path into the VM; the busybox helper works on both Linux CI
# and Docker Desktop).
add_route() {
  container="$1" cidr="$2" via="$3"
  cont_pid="$(docker inspect -f '{{.State.Pid}}' "${container}")"
  if sudo -n nsenter -t "${cont_pid}" -n ip route add "${cidr}" via "${via}" 2>/dev/null; then
    log "  ${container}: ${cidr} via ${via} (nsenter)"
  elif docker exec "${container}" ip route add "${cidr}" via "${via}" 2>/dev/null; then
    log "  ${container}: ${cidr} via ${via} (docker exec, image ships iproute2)"
  elif docker run --rm --net "container:${container}" --cap-add NET_ADMIN \
      busybox:1.36 ip route add "${cidr}" via "${via}" >/dev/null 2>&1; then
    log "  ${container}: ${cidr} via ${via} (busybox netns helper)"
  else
    log "  WARNING: ${container}: could not add route ${cidr} via ${via};"
    log "  pod->${container} replies will fail (bridge readiness and/or stages S5-S7)"
  fi
}
log "routing each node's podCIDR to that node inside both Slurm control-plane containers"
for node in $(kind get nodes --name "${CLUSTER_NAME}"); do
  node_pod_cidr="$(kubectl get node "${node}" -o jsonpath='{.spec.podCIDR}')"
  node_ip="$(docker inspect -f '{{.NetworkSettings.Networks.kind.IPAddress}}' "${node}")"
  [ -n "${node_pod_cidr}" ] && [ -n "${node_ip}" ] || { echo "FAIL: could not resolve podCIDR/IP for node ${node}" >&2; exit 1; }
  add_route "${CTLD_CONTAINER}" "${node_pod_cidr}" "${node_ip}"
  add_route "${RESTD_CONTAINER}" "${node_pod_cidr}" "${node_ip}"
done

# Masquerade exemption (run-10 finding, the other half of the return path):
# kind's CNI MASQUERADEs pod->docker-network traffic, so slurmd registered
# with NodeAddr=<kind node IP> and slurmctld's pings to port 6818 there went
# dark -> node DOWN -> the running job was requeued. Slurm-side fixes are a
# dead end (cloud_reg_addrs is defunct in 26.05, and NodeAddr from
# registration follows the TCP source). Instead, exempt pod->docker-network
# traffic from masquerade INSIDE the kind node: an ACCEPT placed first in
# nat POSTROUTING is terminal for the nat table, so kind's later MASQUERADE
# never fires for these flows, slurmd registers from its REAL pod IP, and
# the pod-CIDR route added above carries the return traffic. Chain-name
# agnostic on purpose (kind's masq chain naming is an implementation
# detail).
# The kind network is dual-stack; take the IPv4 subnet specifically (run-11
# finding: index 0 of IPAM.Config was the IPv6 ULA range, which iptables -4
# rejects).
DOCKER_NET_SUBNET="$(docker network inspect kind -f '{{range .IPAM.Config}}{{.Subnet}}{{"\n"}}{{end}}' | grep -E '^[0-9]+\.' | head -1)"
[ -n "${DOCKER_NET_SUBNET}" ] || { echo "FAIL: could not determine the kind network's IPv4 subnet" >&2; exit 1; }
log "exempting pod->docker-network traffic (${POD_SUBNET} -> ${DOCKER_NET_SUBNET}) from kind masquerade"
# Applied on EVERY kind node: masquerading happens on the node a pod
# egresses from, and with the two-node topology the slurmd pod lives on the
# worker node, not the control plane.
for node in $(kind get nodes --name "${CLUSTER_NAME}"); do
  docker exec "${node}" \
    iptables -t nat -I POSTROUTING 1 -s "${POD_SUBNET}" -d "${DOCKER_NET_SUBNET}" -j ACCEPT
  log "  masquerade exemption installed on ${node}"
done

log "waiting for slurmctld to answer scontrol ping"
ctld_ok="false"
for _ in $(seq 1 30); do
  if docker exec "${CTLD_CONTAINER}" scontrol ping >/dev/null 2>&1; then ctld_ok="true"; break; fi
  sleep 3
done
[ "${ctld_ok}" = "true" ] || { collect_diagnostics; echo "FAIL: slurmctld never answered scontrol ping" >&2; exit 1; }

# The bridge's REST identity. Run-1 finding (2026-07-11, first execution):
# the original plan — token for root, no X-SLURM-USER-NAME header — is
# rejected by rest_auth/jwt ("Rejecting thread config token for user
# (null)"). Rather than hardcode the next guess, S1 MEASURES the identity:
# tokens are minted for a dedicated non-root user (`bridge`, created by both
# init scripts with a pinned uid) and for root, and the probe tries
# name-header/token-only combinations in preference order, locking in the
# first one slurmrestd accepts. The winner then feeds the token Secret, the
# chart's config.slurmUser, AND the sbatch submission identity — job owner
# and API identity must match, because without accounting-backed AdminLevel
# a non-root user may only update its own jobs.
log "minting candidate slurmrestd JWTs (users: slurm, bridge, root)"
JWT_SLURM="$(docker exec "${CTLD_CONTAINER}" scontrol token username=slurm lifespan=3600 | sed 's/^SLURM_JWT=//' || true)"
JWT_BRIDGE="$(docker exec "${CTLD_CONTAINER}" scontrol token username=bridge lifespan=3600 | sed 's/^SLURM_JWT=//' || true)"
JWT_ROOT="$(docker exec "${CTLD_CONTAINER}" scontrol token username=root lifespan=3600 | sed 's/^SLURM_JWT=//' || true)"
[ -n "${JWT_SLURM}${JWT_BRIDGE}${JWT_ROOT}" ] || { collect_diagnostics; echo "FAIL: scontrol token produced no JWT for any candidate user" >&2; exit 1; }

# Locked in by the S1 probe below; empty until then.
AUTH_USER=""
AUTH_HEADER="" # "name" = send X-SLURM-USER-NAME alongside the token; "token-only" = token alone
SLURM_JWT=""

# curl helper that runs INSIDE the kind docker network, so container names
# resolve and the check works identically on Linux CI and Docker Desktop
# (where host->container IPs are not routable). Sends whatever identity the
# S1 probe locked in.
curl_rest() {
  if [ "${AUTH_HEADER}" = "name" ]; then
    docker run --rm --network kind "${CURL_IMAGE}" \
      -sf -H "X-SLURM-USER-NAME: ${AUTH_USER}" -H "X-SLURM-USER-TOKEN: ${SLURM_JWT}" "$@"
  else
    docker run --rm --network kind "${CURL_IMAGE}" \
      -sf -H "X-SLURM-USER-TOKEN: ${SLURM_JWT}" "$@"
  fi
}

# =============================================================================
# S1 — slurmrestd answers an authenticated /jobs list.
# =============================================================================
# Runs BEFORE the chart install on purpose: the bridge only reaches Ready
# after a successful tick against slurmrestd, so a broken REST endpoint
# would otherwise surface as an unattributed `helm --wait` timeout instead
# of this named stage. Polled rather than one-shot: slurmrestd may still be
# settling against slurmctld right after start. -f in curl_rest makes an
# auth failure (401) or a bad route a non-zero exit, so this asserts
# AUTHENTICATED access, not mere TCP reachability.
log "S1: probe which identity slurmrestd accepts, then GET /slurm/v0.0.44/jobs"
s1_ok="false"
for _ in $(seq 1 10); do
  # Preference order: SlurmUser first — run-16 finding: node-record deletion
  # (the S7 cleanup half) is an ADMIN operation; a plain user's token gets
  # 422 "Invalid user id" for it (the same error signature as the 2026-07-06
  # live-deploy finding), so the bridge's REST identity must be an admin,
  # exactly as in the live deployment. Jobs are submitted as the separate
  # plain user `bridge` (S2), mirroring production where the bridge manages
  # OTHER users' jobs. Name header before token-only (run-2: token-only is
  # rejected outright).
  for candidate in \
    "slurm:name:${JWT_SLURM}" \
    "bridge:name:${JWT_BRIDGE}" \
    "root:name:${JWT_ROOT}" \
    "slurm:token-only:${JWT_SLURM}" \
    "bridge:token-only:${JWT_BRIDGE}" \
    "root:token-only:${JWT_ROOT}"; do
    user="${candidate%%:*}"
    rest="${candidate#*:}"
    header="${rest%%:*}"
    token="${rest#*:}" # JWTs are base64url + dots, never contain ':'
    [ -n "${token}" ] || continue
    AUTH_USER="${user}"
    AUTH_HEADER="${header}"
    SLURM_JWT="${token}"
    if body="$(curl_rest "http://slurmrestd:6820/slurm/v0.0.44/jobs" 2>/dev/null)" \
       && echo "${body}" | jq -e 'has("jobs")' >/dev/null 2>&1; then
      s1_ok="true"
      break 2
    fi
  done
  sleep 3
done
[ "${s1_ok}" = "true" ] || stage_fail S1 "slurmrestd rejected every identity combination (bridge/root x name-header/token-only)"
stage_pass S1 "slurmrestd serves authenticated v0.0.44 /jobs as user=${AUTH_USER} (${AUTH_HEADER})"

# =============================================================================
# SETUP 3/5 — bridge + slurmd images into kind.
# =============================================================================
log "building bridge image ${IMAGE_TAG}"
docker build -t "${IMAGE_TAG}" .
kind load docker-image "${IMAGE_TAG}" --name "${CLUSTER_NAME}"

# Pre-load the slurmd image so S5's pod start is not at the mercy of a ghcr
# pull racing the stage timeout.
log "pre-loading slurmd image ${SLURMD_IMAGE} into kind (via local single-platform rebuild)"
docker pull "${SLURMD_IMAGE}"
# Run-3/run-4 finding: loading a REGISTRY-PULLED multi-arch image into kind
# fails on Docker Desktop with "ctr: content digest ... not found" — BOTH
# `kind load docker-image` and `kind load image-archive` funnel into
# `ctr images import --all-platforms`, which chases the manifest index's
# other-platform blobs that the local docker never pulled (`docker save`
# preserves the index under the containerd image store, so the archive
# route fails identically). What demonstrably DOES load on every run is a
# LOCALLY BUILT image (the bridge image rides that path), so re-package the
# pulled image as a local single-platform build — a one-line FROM, zero
# layer duplication — and point the chart at that name.
# Run-6 finding: the local tag must keep the ghcr.io/slinkyproject/ prefix —
# the chart's SECURE DEFAULT passes --allowed-slurmd-images with exactly that
# prefix, and the bridge (correctly!) refused a `slurmd-e2e-local:` name.
# Deliberately NOT overridden with a looser allowlist: this e2e runs the
# chart's real defaults, and the refusal in run 6 doubled as live proof the
# allowlist guard works. The tag is one no registry serves, so the image can
# only ever come from the kind pre-load below (pullPolicy IfNotPresent).
SLURMD_KIND_IMAGE="ghcr.io/slinkyproject/slurmd:e2e-local-single-platform"
# The `bridge` user (uid 1500, matching the control-plane containers) must
# exist on the NODE too: run-13 finding — with comms fully working, every
# allocation died in 13ms with `Launching batch JobId=1 ... for UID 1500`
# followed by `slurmstepd return code -1` and a requeue, because slurmstepd
# cannot switch to a job owner the node cannot resolve. Production Slinky
# nodes resolve users via sssd; this environment intentionally runs no
# identity backend, so the rebuild bakes the one test user in statically.
printf 'FROM %s\nRUN useradd --uid 1500 --create-home --shell /usr/sbin/nologin bridge\n' "${SLURMD_IMAGE}" \
  | docker build -t "${SLURMD_KIND_IMAGE}" -
kind load docker-image "${SLURMD_KIND_IMAGE}" --name "${CLUSTER_NAME}"

# =============================================================================
# SETUP 4/5 — namespace, DNS for the control plane, secrets, Kueue objects.
# =============================================================================
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

# Selectorless Services + manual EndpointSlices: stable in-cluster DNS names
# for the two out-of-cluster containers. The Service names are load-bearing:
# "slurmctld" must match slurm.conf's SlurmctldHost (bare name, expanded by
# the pods' DNS search path), and both feed the chart config below.
log "creating selectorless Services + EndpointSlices for slurmctld/slurmrestd"
cat <<EOF | kubectl -n "${NAMESPACE}" apply -f -
apiVersion: v1
kind: Service
metadata:
  name: slurmctld
spec:
  ports: [{ port: 6817, targetPort: 6817, protocol: TCP }]
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: slurmctld-1
  labels:
    kubernetes.io/service-name: slurmctld
addressType: IPv4
ports: [{ port: 6817, protocol: TCP }]
endpoints:
  - addresses: ["${CTLD_IP}"]
    conditions: { ready: true }
---
apiVersion: v1
kind: Service
metadata:
  name: slurm-restapi
spec:
  ports: [{ port: 6820, targetPort: 6820, protocol: TCP }]
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: slurm-restapi-1
  labels:
    kubernetes.io/service-name: slurm-restapi
addressType: IPv4
ports: [{ port: 6820, protocol: TCP }]
endpoints:
  - addresses: ["${RESTD_IP}"]
    conditions: { ready: true }
EOF

# The two secrets a live namespace carries (run.sh phase 2 fakes both; here
# both are REAL): the bridge's REST token, and the auth/slurm key the
# bridge-created slurmd pods mount (translate.ToJobSet mounts Secret
# slurm-auth-slurm key slurm.key at /etc/slurm/slurm.key) — the SAME key the
# control-plane containers were started with, or registration is rejected.
log "creating slurm-rest-token and slurm-auth-slurm Secrets"
kubectl -n "${NAMESPACE}" create secret generic slurm-rest-token \
  --from-literal=token="${SLURM_JWT}" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "${NAMESPACE}" create secret generic slurm-auth-slurm \
  --from-file=slurm.key="${WORKDIR}/slurm.key" --dry-run=client -o yaml | kubectl apply -f -

# Kueue admission objects, mirroring experiments/01-gke-playground/manifests/
# kueue-config.yaml shrunk to this test's needs: one flavor, one ClusterQueue
# with quota comfortably above the single 1-CPU/1Gi test job, the LocalQueue
# named "main" (the chart config's localQueue default) in the workload
# namespace, and the WorkloadPriorityClass "normal-priority" that the chart's
# default partitionMappings entry for partition "mixing" references — Kueue's
# webhook rejects a Workload naming a class that does not exist.
log "creating Kueue objects (ResourceFlavor/ClusterQueue/LocalQueue/WorkloadPriorityClass)"
cat <<EOF | kubectl apply -f -
apiVersion: kueue.x-k8s.io/v1beta1
kind: ResourceFlavor
metadata:
  name: default-flavor
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: ClusterQueue
metadata:
  name: e2e-main-queue
spec:
  namespaceSelector: {}
  resourceGroups:
    - coveredResources: ["cpu", "memory"]
      flavors:
        - name: default-flavor
          resources:
            - name: cpu
              nominalQuota: 8
            - name: memory
              nominalQuota: 16Gi
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: LocalQueue
metadata:
  name: main
  namespace: ${NAMESPACE}
spec:
  clusterQueue: e2e-main-queue
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: WorkloadPriorityClass
metadata:
  name: normal-priority
value: 100
description: "e2e: Slurm 'mixing' partition mapping target"
EOF

# =============================================================================
# SETUP 5/5 — install the REAL chart, pointed at the REAL slurmrestd.
# =============================================================================
# Same discipline as run.sh phase 2 (real securityContext, namespaced RBAC,
# leader election — all chart defaults), with three deliberate differences:
#   - slurmRestURL/confServer point at the Services above (real Slurm);
#   - config.slurmd.privileged=false: the cgroup-free slurm.conf is exactly
#     the setup that knob exists for (config.go Slurmd.Privileged) — and a
#     privileged pod on a kind node is the flakiness this design avoids;
#   - config.slurmUser is set from the S1-probed identity: when the probe
#     locked in the name-header variant the bridge client must send
#     X-SLURM-USER-NAME too (internal/slurm sends it iff slurmUser != ""),
#     otherwise it stays "" (token-only identity).
if [ "${AUTH_HEADER}" = "name" ]; then CHART_SLURM_USER="${AUTH_USER}"; else CHART_SLURM_USER=""; fi
log "helm install ${CHART_RELEASE} (real chart, real slurmrestd at slurm-restapi:6820, slurmUser='${CHART_SLURM_USER}')"
helm install "${CHART_RELEASE}" "${CHART_DIR}" \
  --namespace "${NAMESPACE}" \
  --set image.repository="${IMAGE_TAG%%:*}" \
  --set image.tag="${IMAGE_TAG##*:}" \
  --set image.pullPolicy=Never \
  --set slurmTokenSecret=slurm-rest-token \
  --set config.namespace="${NAMESPACE}" \
  --set config.slurmRestURL="http://slurm-restapi.${NAMESPACE}.svc.cluster.local:6820" \
  --set config.allowInsecureHTTP=true \
  --set config.pollInterval=5s \
  --set config.slurmTokenFile=/var/run/secrets/slurm/token \
  --set config.slurmd.image="${SLURMD_KIND_IMAGE}" \
  --set config.slurmd.confServer="slurmctld.${NAMESPACE}.svc.cluster.local:6817" \
  --set config.slurmd.authSecretName=slurm-auth-slurm \
  --set config.slurmd.privileged=false \
  --set config.slurmUser="${CHART_SLURM_USER}" \
  --wait --timeout=180s

# =============================================================================
# S2 — a held job is submitted and REPORTED held through the API.
# =============================================================================
# --hold at submit (this scope has no lua JobSubmit plugin; auto-hold is the
# live deployment's ergonomics, not part of the lifecycle under test).
# The jq predicate is EXACTLY internal/slurm.Job.IsHeld's logic: hold==true,
# OR priority set-and-zero with a "JobHeld*" state reason — TC-B7's
# hold-representation assertion, automated.
log "S2: sbatch --hold --partition=mixing as bridge, then assert the API reports it held"
# Submitted as the plain user `bridge`, deliberately DIFFERENT from the
# bridge's (admin) API identity — production-shaped: the bridge mutates
# other users' jobs, which is exactly what requires the admin identity the
# S1 probe prefers. (If the probe fell back to the non-admin `bridge`
# identity, submitter == API identity and job mutations still work on own
# jobs; only the S7 node-record deletion soft-fails then.)
sbatch_out="$(docker exec -u bridge "${CTLD_CONTAINER}" sbatch --hold --partition=mixing \
  --ntasks=1 --cpus-per-task=1 --time=5 --chdir=/tmp --wrap 'sleep 20' || true)"
log "  ${sbatch_out}"
JOBID="$(echo "${sbatch_out}" | grep -oE '[0-9]+' | tail -1 || true)"
[ -n "${JOBID}" ] || stage_fail S2 "could not parse a job ID out of sbatch output: ${sbatch_out}"

s2_ok="false"
for _ in $(seq 1 10); do
  if payload="$(curl_rest "http://slurmrestd:6820/slurm/v0.0.44/job/${JOBID}" 2>/dev/null)" \
     && echo "${payload}" | jq -e '
          .jobs[0] as $j
          | ($j.hold == true)
            or (($j.priority.set == true) and ($j.priority.number == 0)
                and (($j.state_reason // "") | startswith("JobHeld")))' >/dev/null 2>&1; then
    s2_ok="true"
    break
  fi
  sleep 3
done
[ "${s2_ok}" = "true" ] || stage_fail S2 "job ${JOBID} is not reported held in any shape IsHeld() accepts"
stage_pass S2 "job ${JOBID} submitted and reported held via the API"

JOBSET_NAME="slurm-job-${JOBID}"
# Deterministic per translate.NodeNames: <jobset>-workers-0-<podIndex>, one
# pod for this 1-task job. This is BOTH the Slurm node name and the pod
# hostname — the identity the /etc/hosts injection below relies on.
SLURM_NODE_NAME="${JOBSET_NAME}-workers-0-0"

# =============================================================================
# S3 — the bridge translates the held job into JobSet slurm-job-<id>.
# =============================================================================
log "S3: waiting for the bridge to create JobSet ${JOBSET_NAME} (timeout 120s)"
s3_ok="false"
for _ in $(seq 1 24); do
  if kubectl -n "${NAMESPACE}" get jobset "${JOBSET_NAME}" >/dev/null 2>&1; then s3_ok="true"; break; fi
  sleep 5
done
[ "${s3_ok}" = "true" ] || stage_fail S3 "bridge never created JobSet ${JOBSET_NAME}"
stage_pass S3 "JobSet ${JOBSET_NAME} created by the bridge"

# =============================================================================
# S4 — Kueue admits the Workload (condition Admitted=True).
# =============================================================================
# Kueue derives the Workload name from the JobSet ("jobset-<name>-<hash>"),
# so it is located by owner reference rather than by name.
log "S4: waiting for the Workload of ${JOBSET_NAME} to be Admitted (timeout 120s)"
s4_ok="false"
for _ in $(seq 1 24); do
  admitted="$(kubectl -n "${NAMESPACE}" get workloads -o json 2>/dev/null | jq -r --arg js "${JOBSET_NAME}" '
    .items[]
    | select(.metadata.ownerReferences[]? | (.kind == "JobSet" and .name == $js))
    | .status.conditions[]? | select(.type == "Admitted") | .status' || true)"
  if [ "${admitted}" = "True" ]; then s4_ok="true"; break; fi
  sleep 5
done
[ "${s4_ok}" = "true" ] || stage_fail S4 "Workload for ${JOBSET_NAME} never reached Admitted=True"
stage_pass S4 "Workload admitted by Kueue"

# --- /etc/hosts injection (header, reachability point (b)) ---
# As soon as the admitted JobSet's pod has an IP, teach the slurmctld
# container to resolve the pod's deterministic hostname. Done BEFORE the S5
# registration assertion to narrow the race between slurmd registering and
# slurmctld first trying to resolve the node name; the residual race (ctld
# resolving during registration itself, before this loop lands the entry)
# is a documented needs-runner-validation risk, tolerated by
# ReturnToService=2 (NoAddrCache is defunct in 26.05 — run-1 finding).
log "injecting slurmd pod IP/hostname mappings into the slurmctld container's /etc/hosts"
hosts_injected="false"
for _ in $(seq 1 36); do
  mappings="$(kubectl -n "${NAMESPACE}" get pods \
    -l "k8s-bridge.x-k8s.io/slurm-job-id=${JOBID}" \
    -o jsonpath='{range .items[*]}{.status.podIP} {.spec.hostname}{"\n"}{end}' 2>/dev/null \
    | awk 'NF == 2' || true)"
  if [ -n "${mappings}" ]; then
    while IFS= read -r line; do
      docker exec "${CTLD_CONTAINER}" sh -c "grep -qF '${line}' /etc/hosts || echo '${line}' >> /etc/hosts" || true
      log "  /etc/hosts += ${line}"
    done <<<"${mappings}"
    hosts_injected="true"
    break
  fi
  sleep 5
done
[ "${hosts_injected}" = "true" ] || log "  WARNING: no pod IPs appeared within 180s; S5 will tell the real story"

# =============================================================================
# S5 — the slurmd pod registers as a dynamic Slurm node.        [SOFT stage]
# =============================================================================
# `scontrol show node <name>` exits non-zero for an unknown node, so its
# success IS the registration signal; the state grep then rejects a node
# that registered but was marked invalid (INVAL: mismatched config/key,
# exactly the historic dynamic-node failure shapes).
log "S5: waiting for dynamic node ${SLURM_NODE_NAME} to register (timeout 180s)"
s5_ok="false"
for _ in $(seq 1 36); do
  if node_out="$(docker exec "${CTLD_CONTAINER}" scontrol show node "${SLURM_NODE_NAME}" 2>/dev/null)"; then
    if echo "${node_out}" | grep -q "State=" && ! echo "${node_out}" | grep -qE "State=[A-Z_+]*INVAL"; then
      s5_ok="true"
      break
    fi
  fi
  sleep 5
done
[ "${s5_ok}" = "true" ] || stage_fail S5 "dynamic node ${SLURM_NODE_NAME} never registered (or registered INVAL)"
stage_pass S5 "slurmd pod registered as dynamic node ${SLURM_NODE_NAME}"

# =============================================================================
# S6 — the bridge releases the job; it RUNs and COMPLETEs.      [SOFT stage]
# =============================================================================
# No manual `scontrol release` here — the RELEASE IS THE ASSERTION: the
# bridge must SetJobFeatures (which only succeeds once the dynamic node
# advertises nodes-for-<id>) and then ReleaseJob. RUNNING additionally
# proves slurmctld could reach the pod's slurmd to launch the batch script
# (the NAT-sensitive direction). COMPLETED alone also passes: a 20s job can
# race past a 5s poll. Terminal failure states abort immediately.
log "S6: waiting for the bridge to release job ${JOBID} and for RUNNING -> COMPLETED (timeout 300s)"
s6_ok="false"
saw_running="false"
for _ in $(seq 1 60); do
  state="$(docker exec "${CTLD_CONTAINER}" scontrol show job "${JOBID}" 2>/dev/null \
    | grep -oE 'JobState=[A-Z_]+' | head -1 | cut -d= -f2 || true)"
  case "${state}" in
    RUNNING) saw_running="true" ;;
    COMPLETED) s6_ok="true"; break ;;
    FAILED|CANCELLED|TIMEOUT|NODE_FAIL|BOOT_FAIL|OUT_OF_MEMORY|DEADLINE)
      stage_fail S6 "job ${JOBID} ended ${state} instead of COMPLETED" ;;
  esac
  sleep 5
done
[ "${s6_ok}" = "true" ] || stage_fail S6 "job ${JOBID} never reached COMPLETED (last state: ${state:-<none>}, sawRunning=${saw_running})"
stage_pass S6 "job ${JOBID} released by the bridge and COMPLETED (sawRunning=${saw_running})"

# =============================================================================
# S7 — cleanup: JobSet deleted, node record gone.               [SOFT stage]
# =============================================================================
# cleanupFinishedJobs (reconciler.go) sees the terminal job in its ListJobs
# snapshot, deletes the deterministic node records via DELETE /node/<name>,
# then deletes the JobSet. Both disappearances are asserted; either lingering
# means the leak the MVD's cleanup exists to prevent.
log "S7: waiting for the bridge to delete JobSet ${JOBSET_NAME} and node record ${SLURM_NODE_NAME} (timeout 180s)"
s7_ok="false"
for _ in $(seq 1 36); do
  jobset_gone="false"; node_gone="false"
  kubectl -n "${NAMESPACE}" get jobset "${JOBSET_NAME}" >/dev/null 2>&1 || jobset_gone="true"
  docker exec "${CTLD_CONTAINER}" scontrol show node "${SLURM_NODE_NAME}" >/dev/null 2>&1 || node_gone="true"
  if [ "${jobset_gone}" = "true" ] && [ "${node_gone}" = "true" ]; then s7_ok="true"; break; fi
  sleep 5
done
[ "${s7_ok}" = "true" ] || stage_fail S7 "cleanup incomplete: jobsetGone=${jobset_gone} nodeRecordGone=${node_gone}"
stage_pass S7 "bridge cleaned up the JobSet and the dynamic node record"

log "ALL STAGES PASSED (S1-S7): real-Slurm lifecycle verified — held job detected, translated, admitted by Kueue, dynamic node registered, pinned+released by the bridge, ran to COMPLETED, and cleaned up. Update the VALIDATION log in this script's header after the first run that reaches this line."
