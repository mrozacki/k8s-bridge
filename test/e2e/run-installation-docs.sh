#!/usr/bin/env bash
# Installation-DOCS e2e: does `docs/installation.md` actually work?
#
# test/e2e/run.sh proves the chart deploys; run-real-slurm.sh proves the
# lifecycle works. Neither reads the documentation, so neither can catch the
# failure mode that hurts a first-time operator most: an install guide whose
# `--set` flags, defaults, or sample manifests have drifted away from the
# chart they describe. This script closes that gap — it treats
# `docs/installation.md` as the spec and the chart as the implementation, and
# fails when they disagree.
#
# THREE PHASES. Set PHASE=static to run only the first (no cluster, seconds,
# cheap enough for a per-PR gate); PHASE=all (the default) runs everything.
#
#   PHASE A — STATIC. No cluster. Extracts every `--set key=value` from the
#   k8s-bridge `helm install`/`helm upgrade` commands in the guide and proves
#   each key is a real, settable chart value (values.yaml + values.schema.json),
#   renders each documented command as written, and asserts the SECURE
#   DEFAULTS the guide advertises (§4.3) are the chart's actual defaults.
#   That last part is a regression guard for a bug that really shipped: the
#   two lines in values.yaml had drifted to `http://` + `allowInsecureHTTP:
#   true`, so a default `helm install` sent the bearer-equivalent Slurm JWT in
#   cleartext. The failure messages below say so, loudly, on purpose.
#
#   PHASE B — LIVE. Stands up kind + the guide's own JobSet/Kueue versions and
#   queue objects, then walks the THREE documented install shapes (§4.1 file
#   mode, §4.2 supervisor mode, §4.2 single-CR binding), each in its own
#   namespace, asserting each reaches a working state. The assertion that
#   matters most is B2's: `kubectl apply -f deploy/crd/workloadmixing-sample.yaml`
#   into a supervisor-mode install must reach Ready=True, which proves the
#   SHIPPED SAMPLE is compatible with the chart's own `allowedTokenPaths`
#   default — a conflict that existed and was fixed, and one that only a live
#   supervisor can detect (the sample is what operators copy into real
#   clusters, and a CR naming a token path outside the allowlist is refused).
#
#   PHASE C — LIVE NEGATIVE. The ADR-0017 trust anchors, asserted in a running
#   cluster rather than only in unit tests: the token-path allowlist, the TLS
#   escape-hatch gate (refused, then honored after an opt-in `helm upgrade` —
#   proving it is a gate, not a wall), and the CRD's CEL cleartext rule
#   (rejected by the apiserver itself). Every negative assertion prints the
#   condition/error it actually observed so a CI log alone is enough to triage.
#
# WHAT THIS DOES *NOT* COVER — read before trusting a green run:
#   - NO REAL SLURM. slurmrestd is the same mock run.sh uses (busybox httpd
#     serving one static JSON envelope), so "Ready" here means "the bridge
#     completed a tick against a well-formed endpoint", not "jobs flow".
#     Hold -> translate -> admit -> release is run-real-slurm.sh's job.
#   - §1.2 cert-manager, §1.5 KubeRay and §1.6 Slinky slurm-operator are OUT
#     OF SCOPE: cert-manager and KubeRay serve the ray-bridge/webhook paths
#     this script does not install, and the slurm-operator only earns its keep
#     with a real Slurm cluster behind it — which is exactly what run.sh's
#     header explains cannot run unattended on kind (privileged slurmd,
#     writable cgroups). Their `helm` commands are therefore skipped by the
#     PHASE A extractor too (it only looks at commands targeting
#     deploy/chart/k8s-bridge).
#   - §5 ray-bridge and §7 GKE notes: different chart, different cluster.
#
# Local run:
#   PHASE=static ./test/e2e/run-installation-docs.sh   # seconds, no cluster
#   ./test/e2e/run-installation-docs.sh                # full, needs kind+docker
# Requires: helm (always); kind, docker, kubectl (PHASE=all only).
# KEEP_CLUSTER=true leaves the cluster up for debugging, same as run.sh.
#
# Location rationale: same as run.sh — it needs the Go module (to build the
# controller image), the chart, and docs/ as its working context, all of which
# are resolved relative to this script's own path, never absolute.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.." # -> repo root
REPO_ROOT="$(pwd)"
CHART_DIR="${REPO_ROOT}/deploy/chart/k8s-bridge"
DOC="${REPO_ROOT}/docs/installation.md"
SAMPLE_CR="${REPO_ROOT}/deploy/crd/workloadmixing-sample.yaml"

PHASE="${PHASE:-all}" # static | all

CLUSTER_NAME="${CLUSTER_NAME:-k8s-bridge-docs-e2e}"
IMAGE_TAG="${IMAGE_TAG:-k8s-bridge:docs-e2e}"

# One namespace per documented install shape so a leftover Ready controller
# from an earlier shape can never mask a later one's failure (the same
# reasoning run.sh gives for splitting its two phases).
NS_FILE="${NS_FILE:-docs-file-mode}"      # B1, §4.1 configSource=file
NS_SUP="${NS_SUP:-slurm-jobs}"            # B2, §4.2 supervisor mode (the guide's own namespace)
NS_BIND="${NS_BIND:-docs-single-cr}"      # B3, §4.2 single-CR binding
NS_SLURM="${NS_SLURM:-slurm}"             # stand-in for the Slurm operator's namespace (§3.1 source)

RELEASE="${RELEASE:-k8s-bridge}"          # the release name the guide uses
METRICS_SVC="${RELEASE}-metrics"          # what §6 tells operators to port-forward

KEEP_CLUSTER="${KEEP_CLUSTER:-false}" # set true to skip teardown for debugging

log() { echo "==> $*"; }
pass() { echo "PASS: $*"; }

# Assertions accumulate instead of exiting: one run should report every
# disagreement between the guide and the chart, not just the first. Hard
# setup failures (kind/helm/kubectl) still abort via set -e.
FAIL_COUNT=0
FAIL_LOG=""
fail() {
  echo "FAIL: $*" >&2
  FAIL_COUNT=$((FAIL_COUNT + 1))
  FAIL_LOG="${FAIL_LOG}
  - $*"
}

WORKDIR="$(mktemp -d)"

cleanup() {
  if [ "${PHASE}" != "static" ]; then
    if [ "${KEEP_CLUSTER}" != "true" ]; then
      log "tearing down kind cluster ${CLUSTER_NAME}"
      kind delete cluster --name "${CLUSTER_NAME}" >/dev/null 2>&1 || true
    else
      log "KEEP_CLUSTER=true: leaving cluster ${CLUSTER_NAME} up for inspection"
    fi
  fi
  rm -rf "${WORKDIR}"
}
trap cleanup EXIT

summarize_and_exit() {
  if [ "${FAIL_COUNT}" -gt 0 ]; then
    echo "" >&2
    echo "=======================================================================" >&2
    echo "${FAIL_COUNT} assertion(s) FAILED:${FAIL_LOG}" >&2
    echo "=======================================================================" >&2
    exit 1
  fi
  log "ALL ASSERTIONS PASSED"
  exit 0
}

command -v helm >/dev/null 2>&1 || { echo "missing required tool: helm" >&2; exit 1; }
[ -f "${DOC}" ] || { echo "missing ${DOC}" >&2; exit 1; }
[ -f "${SAMPLE_CR}" ] || { echo "missing ${SAMPLE_CR}" >&2; exit 1; }

# =============================================================================
# Shared helpers (used by both phases)
# =============================================================================

# chart_key_exists <dotted.path> — is this a key the chart's values.yaml
# actually declares? Walks values.yaml by indentation (the file is uniformly
# 2-space indented) instead of shelling out to a YAML tool this repo does not
# already require. A COMMENTED-OUT key ("# allowInsecureHTTP: true") counts as
# declared: those are the chart's documented-but-unset knobs, and the schema
# check below is what decides whether a key is actually settable.
chart_key_exists() {
  awk -v want="$1" '
    {
      raw = $0
      match(raw, /^[ \t]*/); indent = RLENGTH
      s = substr(raw, indent + 1)
      sub(/^#[ ]?/, "", s)
      if (s !~ /^[A-Za-z0-9_.-]+:/) next
      key = s; sub(/:.*/, "", key)
      lvl = int(indent / 2)
      stack[lvl] = key
      path = stack[0]
      for (i = 1; i <= lvl; i++) path = path "." stack[i]
      if (path == want) { found = 1; exit }
    }
    END { exit(found ? 0 : 1) }
  ' "${CHART_DIR}/values.yaml"
}

# path_allowed <path> <prefix>... — the bash mirror of
# config.ValidateFilePathAllowed: boundary-aware, so /var/run/secretsomething
# does not pass on /var/run/secrets. Kept in sync with that function by hand;
# it exists here so PHASE A can answer "would the controller accept this path?"
# without a cluster.
path_allowed() {
  local p="$1" prefix
  shift
  for prefix in "$@"; do
    [ -n "${prefix}" ] || continue
    prefix="${prefix%/}"
    [ "${p}" = "${prefix}" ] && return 0
    case "${p}" in
      "${prefix}"/*) return 0 ;;
    esac
  done
  return 1
}

# chart_default_token_paths — the chart's default allowedTokenPaths list, one
# prefix per line, read straight out of values.yaml.
chart_default_token_paths() {
  awk '
    /^allowedTokenPaths:/ { inlist = 1; next }
    inlist && /^[ \t]*-[ \t]*/ {
      v = $0
      sub(/^[ \t]*-[ \t]*/, "", v)
      gsub(/^["'"'"']|["'"'"']$/, "", v)
      print v
      next
    }
    inlist && /^[^ \t#]/ { exit }
  ' "${CHART_DIR}/values.yaml"
}

# yaml_scalar <file> <key> — first uncommented "key: value" scalar at any
# indentation, quotes stripped. Enough for the flat sample-CR fields this
# script asserts on; deliberately not a YAML parser.
yaml_scalar() {
  awk -v key="$2" '
    {
      raw = $0
      match(raw, /^[ \t]*/); indent = RLENGTH
      s = substr(raw, indent + 1)
      if (s ~ /^#/) next
      if (s !~ "^" key ":") next
      v = s; sub("^" key ":[ \t]*", "", v)
      sub(/[ \t]*#.*$/, "", v)
      gsub(/^["'"'"']|["'"'"']$/, "", v)
      print v
      exit
    }
  ' "$1"
}

# =============================================================================
# PHASE A — STATIC VALIDATION (no cluster)
# =============================================================================
log "PHASE A: static validation of ${DOC#"${REPO_ROOT}"/} against the chart"

# --- A0: extract the guide's k8s-bridge helm commands ------------------------
# Only commands whose chart argument is deploy/chart/k8s-bridge are collected:
# the cert-manager (§1.2), KubeRay (§1.5), slurm-operator (§1.6) and ray-bridge
# (§5) commands install other charts this script deliberately does not cover,
# and asserting their flags against THIS chart's schema would be nonsense.
# Emits two record kinds, both tab-separated, both carrying the doc line
# number so a failure can name the exact line an operator would copy:
#   CMD <start-line> <namespace> <full command, continuations joined>
#   SET <line> <key=value>
awk '
  { lines[NR] = $0 }
  END {
    for (i = 1; i <= NR; i++) {
      if (lines[i] !~ /helm[ \t]+(install|upgrade)/) continue
      j = i; cmd = ""
      while (1) {
        l = lines[j]
        sub(/\\[ \t]*$/, "", l)
        cmd = cmd " " l
        if (lines[j] ~ /\\[ \t]*$/) { j++ } else break
      }
      if (cmd !~ /deploy\/chart\/k8s-bridge/) { i = j; continue }
      ns = ""
      n = split(cmd, t, /[ \t]+/)
      for (m = 1; m <= n; m++) if ((t[m] == "--namespace" || t[m] == "-n") && m < n) ns = t[m+1]
      printf "CMD\t%d\t%s\t%s\n", i, (ns == "" ? "default" : ns), cmd
      for (k = i; k <= j; k++) {
        nf = split(lines[k], u, /[ \t]+/)
        for (m = 1; m <= nf; m++) if (u[m] == "--set" && m < nf) printf "SET\t%d\t%s\n", k, u[m+1]
      }
      i = j
    }
  }
' "${DOC}" >"${WORKDIR}/helm-commands.txt"

cmd_count=$(grep -c '^CMD' "${WORKDIR}/helm-commands.txt" || true)
set_count=$(grep -c '^SET' "${WORKDIR}/helm-commands.txt" || true)
if [ "${cmd_count}" -lt 1 ] || [ "${set_count}" -lt 1 ]; then
  # A guide with no k8s-bridge helm command at all means the extractor broke
  # (or the guide was restructured) — either way, silently asserting nothing
  # would be the worst outcome, so this is a hard failure.
  fail "A0: found ${cmd_count} k8s-bridge helm command(s) and ${set_count} --set flag(s) in ${DOC} — expected at least one of each; the extractor or the guide's structure changed"
else
  pass "A0: extracted ${cmd_count} k8s-bridge helm command(s), ${set_count} --set flag(s) from ${DOC}"
fi

# --- A1a: every documented --set key is a real chart value -------------------
# Placeholders like <your-registry>/k8s-bridge are substituted with a benign
# token; the point is whether the KEY resolves, not whether the operator's
# registry exists.
sanitize_value() { echo "$1" | sed -e 's/<[^>]*>/docs-e2e-placeholder/g'; }

while IFS=$'\t' read -r kind lineno kv; do
  [ "${kind}" = "SET" ] || continue
  key="${kv%%=*}"
  value="${kv#*=}"
  value="$(sanitize_value "${value}")"

  if chart_key_exists "${key}"; then
    pass "A1a: --set ${key} (${DOC#"${REPO_ROOT}"/}:${lineno}) resolves against values.yaml"
  else
    fail "A1a: --set ${key} at ${DOC#"${REPO_ROOT}"/}:${lineno} names a value the chart does not declare in values.yaml — the documented command would silently do nothing"
  fi

  # Schema check: render with this one flag and look specifically for a SCHEMA
  # rejection. A template-level `fail` (e.g. workloadmixing.namespace without
  # workloadmixing.name, which the chart refuses on purpose) is NOT a schema
  # problem and is covered by the per-command render in A1b instead.
  if ! out=$(helm template docs-check "${CHART_DIR}" --namespace slurm-jobs \
      --set "${key}=${value}" 2>&1); then
    case "${out}" in
      *"don't meet the specifications of the schema"*|*"values don't meet"*|*schema*)
        fail "A1b: --set ${key}=${value} at ${DOC#"${REPO_ROOT}"/}:${lineno} violates values.schema.json: ${out}"
        ;;
      *)
        : # template-level failure; A1c renders the whole documented command
        ;;
    esac
  else
    pass "A1b: --set ${key}=${value} passes values.schema.json"
  fi
done <"${WORKDIR}/helm-commands.txt"

# --- A1c: each documented command renders as written -------------------------
# All of one command's --set flags applied together, because some of them only
# make sense as a set (§4.2's single-CR binding sets workloadmixing.namespace
# AND .name; namespace alone is rejected at template time by design).
while IFS=$'\t' read -r kind lineno ns cmd; do
  [ "${kind}" = "CMD" ] || continue
  # shellcheck disable=SC2086 # deliberate word splitting of the doc's command
  set -- ${cmd}
  render_ok=true
  render_out=""
  # Rebuild the --set flags into a positional list (bash 3.2 friendly: no
  # associative arrays, no mapfile).
  sets=""
  while [ $# -gt 0 ]; do
    if [ "$1" = "--set" ] && [ $# -gt 1 ]; then
      k="${2%%=*}"
      v="$(sanitize_value "${2#*=}")"
      sets="${sets} --set ${k}=${v}"
      shift
    fi
    shift
  done
  # shellcheck disable=SC2086 # sets is a deliberately built flag list
  if ! render_out=$(helm template "${RELEASE}" "${CHART_DIR}" --namespace "${ns}" ${sets} 2>&1); then
    render_ok=false
  fi
  if [ "${render_ok}" = "true" ]; then
    pass "A1c: documented helm command at ${DOC#"${REPO_ROOT}"/}:${lineno} renders (namespace ${ns},${sets:-" no --set flags"})"
  else
    fail "A1c: documented helm command at ${DOC#"${REPO_ROOT}"/}:${lineno} does NOT render: ${render_out}"
  fi
done <"${WORKDIR}/helm-commands.txt"

# --- A2: the guide's advertised secure defaults are the chart's defaults -----
# Rendered through the chart (not grepped out of values.yaml) so this asserts
# what an operator's `helm install` with no overrides would actually deploy.
default_config=$(helm template defaults "${CHART_DIR}" --namespace slurm-jobs \
  --show-only templates/configmap.yaml 2>/dev/null || true)
default_url=$(echo "${default_config}" | awk '/^[ \t]*slurmRestURL:/ { v=$2; gsub(/^"|"$/, "", v); print v; exit }')
case "${default_url}" in
  https://*)
    pass "A2a: chart default config.slurmRestURL is https (${default_url}), matching docs/installation.md §4.3 and the chart README"
    ;;
  *)
    fail "A2a: chart default config.slurmRestURL is '${default_url:-<unset>}', not https:// — SECURITY REGRESSION. docs/installation.md §4.3 and deploy/chart/k8s-bridge/README.md both promise https by default, with plaintext gated behind an explicit config.allowInsecureHTTP opt-in. This exact drift shipped once: values.yaml said http:// + allowInsecureHTTP: true, so a DEFAULT \`helm install\` sent the bearer-equivalent Slurm JWT over the wire in cleartext, to anyone on the path. Restore https:// in values.yaml; test/dev overrides belong in values-gke-test.yaml or an explicit --set"
    ;;
esac

if echo "${default_config}" | grep -Eq '^[ \t]*allowInsecureHTTP:[ \t]*true'; then
  fail "A2b: chart default config.allowInsecureHTTP is TRUE — SECURITY REGRESSION. The plaintext opt-in must stay unset by default (docs/installation.md §4.3: 'Must be https:// unless config.allowInsecureHTTP: true'; chart README lists the default as 'unset'). With it on by default, nothing stops an http:// slurmRestURL from shipping the Slurm JWT in cleartext — this is precisely the pair of lines that drifted once already"
else
  pass "A2b: chart default config.allowInsecureHTTP is unset/false — plaintext stays an explicit opt-in"
fi

# --- A3: the shipped sample CR is secure-by-default --------------------------
# Rationale: deploy/crd/workloadmixing-sample.yaml is what operators copy into
# real clusters (§4.2 tells them to apply it verbatim). In supervisor mode a CR
# naming a token path outside the chart's allowedTokenPaths is REFUSED
# (Ready=False/InvalidSpec, ADR-0017) — so a sample that violates its own
# chart's default would be broken on arrival, and a sample carrying http:// +
# allowInsecureHTTP would teach cleartext as the normal shape. B2 proves the
# same thing live; this catches it in seconds.
sample_url=$(yaml_scalar "${SAMPLE_CR}" "slurmRestURL")
case "${sample_url}" in
  https://*) pass "A3a: sample CR slurmRestURL is https (${sample_url})" ;;
  *) fail "A3a: sample CR slurmRestURL is '${sample_url:-<unset>}' — the sample operators copy into real clusters must default to https (a plaintext endpoint is a per-site opt-in, patched in, per docs/tutorial.md)" ;;
esac

if grep -Eq '^[ \t]*allowInsecureHTTP:[ \t]*true' "${SAMPLE_CR}"; then
  fail "A3b: sample CR sets allowInsecureHTTP: true — the sample must not ship the cleartext combination; the CRD's CEL rule exists to make that an explicit, per-cluster decision"
else
  pass "A3b: sample CR does not enable allowInsecureHTTP"
fi

sample_token=$(yaml_scalar "${SAMPLE_CR}" "slurmTokenFile")
token_prefixes=""
while IFS= read -r prefix; do
  [ -n "${prefix}" ] || continue
  token_prefixes="${token_prefixes} ${prefix}"
done <<EOF
$(chart_default_token_paths)
EOF
# shellcheck disable=SC2086 # token_prefixes is a deliberately built list
if [ -n "${sample_token}" ] && path_allowed "${sample_token}" ${token_prefixes}; then
  pass "A3c: sample CR slurmTokenFile (${sample_token}) is inside the chart's default allowedTokenPaths (${token_prefixes# })"
else
  fail "A3c: sample CR slurmTokenFile '${sample_token:-<unset>}' is NOT inside the chart's default allowedTokenPaths (${token_prefixes# }) — in supervisor mode the controller would refuse the shipped sample with Ready=False/InvalidSpec, i.e. the guide's own §4.2 walkthrough is broken on arrival"
fi

# --- A4: §4.2's supervisor/single-CR keys exist ------------------------------
# Named explicitly (not just swept up by A1a) because these three are the
# entire public surface of ADR-0015's two CR modes: if any of them is renamed,
# §4.2's copy-paste blocks become silent no-ops rather than errors.
for key in configSource workloadmixing.namespace workloadmixing.name; do
  if chart_key_exists "${key}"; then
    pass "A4: §4.2 key '${key}' exists in values.yaml"
  else
    fail "A4: §4.2 documents --set ${key}, but the chart's values.yaml no longer declares it"
  fi
  if ! grep -q -- "--set ${key}=" "${DOC}"; then
    fail "A4: values.yaml declares '${key}' but no documented command in ${DOC#"${REPO_ROOT}"/} sets it — §4.2's examples drifted"
  fi
done

# --- A5: the token-path anchor must actually bound the named attack ----------
# ADR-0017's stated target is the controller's OWN ServiceAccount token at
# /var/run/secrets/kubernetes.io/serviceaccount/token, exfiltrated to a URL the
# same CR author chooses. The chart default has to be narrow enough to refuse
# that exact path, otherwise the anchor documents a defense it does not
# provide. C1 asserts the same thing end to end in a live cluster.
SA_TOKEN_PATH="/var/run/secrets/kubernetes.io/serviceaccount/token"
# shellcheck disable=SC2086 # token_prefixes is a deliberately built list
if path_allowed "${SA_TOKEN_PATH}" ${token_prefixes}; then
  fail "A5: the chart's DEFAULT allowedTokenPaths (${token_prefixes# }) admits ${SA_TOKEN_PATH} — the controller's own ServiceAccount token, which is the exact attack ADR-0017 says this anchor closes. In supervisor mode any WorkloadMixing CR in the release namespace can name that path and an https:// slurmRestURL of its choosing, and the controller will read the token and send it there as a request header (neither the allowInsecureHTTP gate nor the CRD's CEL rule sees anything wrong). Narrow the default to the chart's own mount point (/var/run/secrets/slurm/) — the sample CR and the chart's Deployment already use exactly that path, so the fix costs nothing"
else
  pass "A5: chart default allowedTokenPaths refuses ${SA_TOKEN_PATH} (the ADR-0017 attack path)"
fi

# --- A6: every chart value named in §4.3's fields table still exists ---------
# The table is the guide's reference surface: an operator sets these by hand in
# their own values file, so a renamed chart value turns a documented knob into
# a typo Helm silently ignores (nothing in `helm install` rejects an unknown
# key in a -f values file the way values.schema.json rejects a --set on a
# closed object). Only the table's first column is read, and only tokens that
# look like dotted value paths.
awk '
  /^### 4\.3/ { insection = 1; next }
  insection && /^#{2,3} / { exit }
  !insection { next }
  /^\|/ {
    n = split($0, cells, "|")
    if (n < 2) next
    first = cells[2]
    if (first ~ /^[ \t]*(Field|-+)[ \t]*$/) next
    while (match(first, /`[^`]+`/)) {
      tok = substr(first, RSTART + 1, RLENGTH - 2)
      first = substr(first, RSTART + RLENGTH)
      sub(/:.*$/, "", tok)
      if (tok ~ /^[A-Za-z][A-Za-z0-9]*(\.[A-Za-z][A-Za-z0-9]*)*$/) print NR "\t" tok
    }
  }
' "${DOC}" >"${WORKDIR}/fields-table.txt"

table_keys=$(wc -l <"${WORKDIR}/fields-table.txt" | tr -d ' ')
if [ "${table_keys}" -lt 1 ]; then
  fail "A6: could not read any chart value out of §4.3's fields table in ${DOC#"${REPO_ROOT}"/} — the table or its heading changed shape"
else
  a6_bad=0
  while IFS=$'\t' read -r lineno key; do
    [ -n "${key}" ] || continue
    if ! chart_key_exists "${key}"; then
      fail "A6: §4.3 documents chart value '${key}' (${DOC#"${REPO_ROOT}"/}:${lineno}), but values.yaml no longer declares it"
      a6_bad=$((a6_bad + 1))
    fi
  done <"${WORKDIR}/fields-table.txt"
  [ "${a6_bad}" -eq 0 ] && pass "A6: all ${table_keys} chart values named in §4.3's fields table exist in values.yaml"
fi

# --- A7: CR mode must not render RBAC outside the release namespace ----------
# §4.2 says `config` is ignored in CR mode, and the chart's whole RBAC stance
# (rbac.namespaced=true, security audit H4) is "least privilege, scoped to the
# namespaces you asked for". Rendering CR mode into an arbitrary namespace and
# checking every rendered object's namespace catches the case where a
# file-mode-only value still leaks a Role somewhere the operator never named —
# which is both a privilege leak and a collision: two CR-mode releases in
# different namespaces would render the SAME Role name into that third
# namespace, and the second `helm install` fails with a Helm ownership error.
a7_ns="docs-e2e-rbac-probe"
a7_stray=$(helm template "${RELEASE}" "${CHART_DIR}" --namespace "${a7_ns}" --set configSource=cr 2>/dev/null \
  | awk -v want="${a7_ns}" '/^kind:/ { k = $2 } /^  namespace:/ { if ($2 != want) print k "/" $2 }' | sort -u | tr '\n' ' ')
if [ -z "${a7_stray}" ]; then
  pass "A7: configSource=cr renders every object into the release namespace only"
else
  fail "A7: configSource=cr into namespace '${a7_ns}' also renders objects into other namespaces: ${a7_stray}— in CR mode the guide (§4.2) states that \`config\` is ignored, but templates/rbac.yaml still seeds its namespace list from config.namespace (default 'slurm-jobs'). Two consequences, both real: the controller's ServiceAccount silently gets a Role in a namespace the operator never named (contrary to the chart's least-privilege default, audit H4), and a SECOND CR-mode release in another namespace fails to install at all — 'Role \"${RELEASE}\" in namespace \"...\" exists and cannot be imported into the current release'. Fix: build the namespace list from config.namespace only when configSource=file"
fi

log "PHASE A complete (${FAIL_COUNT} failure(s) so far)"

if [ "${PHASE}" = "static" ]; then
  summarize_and_exit
fi

# =============================================================================
# PHASE B — LIVE VALIDATION ON KIND
# =============================================================================
for bin in kind docker kubectl; do
  command -v "$bin" >/dev/null 2>&1 || { echo "missing required tool: $bin" >&2; exit 1; }
done

# Component versions come from the guide itself (§1.3/§1.4) rather than a
# second hardcoded copy here: the point of this script is that following the
# guide works, so it installs exactly what the guide's own command lines say.
# Falls back to run.sh's pins if the extraction finds nothing.
JOBSET_URL=$(grep -o 'https://github.com/kubernetes-sigs/jobset/releases/download/[^ ]*manifests.yaml' "${DOC}" | head -1 || true)
KUEUE_URL=$(grep -o 'https://github.com/kubernetes-sigs/kueue/releases/download/[^ ]*manifests.yaml' "${DOC}" | head -1 || true)
[ -n "${JOBSET_URL}" ] || JOBSET_URL="https://github.com/kubernetes-sigs/jobset/releases/download/v0.12.0/manifests.yaml"
[ -n "${KUEUE_URL}" ] || KUEUE_URL="https://github.com/kubernetes-sigs/kueue/releases/download/v0.18.2/manifests.yaml"

log "PHASE B: live validation on kind (JobSet from ${JOBSET_URL##*/download/}, Kueue from ${KUEUE_URL##*/download/} — both read from ${DOC#"${REPO_ROOT}"/})"

log "creating kind cluster ${CLUSTER_NAME}"
kind create cluster --name "${CLUSTER_NAME}" --wait 120s

log "installing JobSet (docs/installation.md §1.3)"
kubectl apply --server-side -f "${JOBSET_URL}"

log "installing Kueue (docs/installation.md §1.4)"
kubectl apply --server-side -f "${KUEUE_URL}"
kubectl -n kueue-system rollout status deploy/kueue-controller-manager --timeout=300s

log "building bridge image ${IMAGE_TAG}"
docker build -t "${IMAGE_TAG}" .
kind load docker-image "${IMAGE_TAG}" --name "${CLUSTER_NAME}"

# --- §2 queue objects, applied verbatim from the guide -----------------------
# The whole YAML block under "## 2. Kueue queue objects" is extracted and
# applied as-is: if that block ever stops being valid against the installed
# Kueue version, an operator following the guide hits it here first.
awk '
  /^## 2\. Kueue queue objects/ { insection = 1 }
  insection && /^```yaml/ { inblock = 1; next }
  inblock && /^```/ { exit }
  inblock { print }
' "${DOC}" >"${WORKDIR}/kueue-objects.yaml"
if [ ! -s "${WORKDIR}/kueue-objects.yaml" ]; then
  fail "B0: could not extract the §2 Kueue queue-object manifest from ${DOC#"${REPO_ROOT}"/}"
else
  log "applying the guide's §2 queue objects (ResourceFlavor/ClusterQueue/LocalQueue/WorkloadPriorityClass)"
  # Kueue's webhooks can be briefly unavailable right after rollout; retry a
  # bounded number of times rather than failing the whole run on a race.
  applied=false
  for _ in 1 2 3 4 5 6; do
    if kubectl apply -f "${WORKDIR}/kueue-objects.yaml" >/dev/null 2>&1; then
      applied=true
      break
    fi
    sleep 5
  done
  if [ "${applied}" = "true" ]; then
    pass "B0: the guide's §2 queue objects apply cleanly against Kueue ${KUEUE_URL##*/download/}"
  else
    kubectl apply -f "${WORKDIR}/kueue-objects.yaml" || true
    fail "B0: the guide's §2 queue objects do not apply — see the error above"
  fi
fi

# --- helpers that stand up one documented install shape ----------------------

# deploy_mock <ns> — the same static-JSON mock slurmrestd run.sh uses: a 200 OK
# with an empty jobs/errors/warnings envelope, which is all
# internal/slurm.Client.ListJobs needs for a tick to complete. No real Slurm,
# no job to hold/release.
deploy_mock() {
  local ns="$1"
  kubectl -n "${ns}" create configmap mock-slurmrestd-response \
    --from-literal=jobs.json='{"jobs": [], "errors": [], "warnings": []}' \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  cat <<'EOF' | kubectl -n "${ns}" apply -f - >/dev/null
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
  kubectl -n "${ns}" rollout status deployment/mock-slurmrestd --timeout=180s
}

mock_url() { echo "http://mock-slurmrestd.$1.svc.cluster.local:6820"; }

# namespace_prereqs <ns> — everything the guide says must exist in the release
# namespace BEFORE `helm install`:
#   §3   the slurm-rest-token Secret (the chart mounts it, never creates it)
#   §3.1 the slurm-auth-slurm Secret, copied from the Slurm cluster's namespace
#        with the guide's own one-liner — Secrets are namespace-scoped and the
#        bridge deliberately does not sync them. The copy pipeline is run
#        VERBATIM (only the target namespace is parameterized) so a broken
#        `grep -v` filter in the guide fails here instead of in production.
# A LocalQueue mirroring §2 is created too, so config.localQueue names a queue
# that actually exists (§4.3's "must match a namespace/LocalQueue from step 2").
namespace_prereqs() {
  local ns="$1"
  kubectl create namespace "${ns}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl -n "${ns}" create secret generic slurm-rest-token \
    --from-literal=token=docs-e2e-fake-token --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl get secret slurm-auth-slurm -n "${NS_SLURM}" -o yaml \
    | grep -v -E 'namespace:|resourceVersion:|uid:|creationTimestamp:' \
    | kubectl apply -n "${ns}" -f - >/dev/null
  if kubectl -n "${ns}" get secret slurm-auth-slurm >/dev/null 2>&1; then
    pass "B-prereq(${ns}): the guide's §3.1 auth-key copy one-liner works (slurm-auth-slurm present in ${ns})"
  else
    fail "B-prereq(${ns}): the guide's §3.1 copy pipeline did not produce slurm-auth-slurm in ${ns} — an operator following it verbatim would hit FailedMount on every slurmd pod"
  fi
  cat <<EOF | kubectl apply -f - >/dev/null
apiVersion: kueue.x-k8s.io/v1beta1
kind: LocalQueue
metadata:
  name: main
  namespace: ${ns}
spec:
  clusterQueue: main-queue
EOF
}

# assert_deployment_ready <ns> <label>
assert_deployment_ready() {
  local ns="$1" label="$2"
  if kubectl -n "${ns}" rollout status "deployment/${RELEASE}" --timeout=180s >/dev/null 2>&1 \
    && kubectl -n "${ns}" wait --for=condition=Ready pod \
      -l "app.kubernetes.io/instance=${RELEASE}" --timeout=120s >/dev/null 2>&1; then
    pass "${label}: Deployment ${RELEASE} reached Ready in ${ns}"
    return 0
  fi
  fail "${label}: Deployment ${RELEASE} never became Ready in ${ns}"
  kubectl -n "${ns}" get pods -o wide || true
  kubectl -n "${ns}" logs "deployment/${RELEASE}" --tail=60 || true
  return 1
}

# assert_readyz <ns> <label> — an in-cluster curl pod against the chart's own
# metrics Service (the one §6 tells operators to port-forward). /readyz only
# returns 200 after a tick has actually completed against the mock, so this is
# the "the documented install really works" assertion, not a liveness ping.
assert_readyz() {
  local ns="$1" label="$2" pod="readyz-probe" code="" ok="false"
  local url="http://${METRICS_SVC}:8080/readyz"
  local i
  for i in $(seq 1 12); do
    kubectl -n "${ns}" delete pod "${pod}" --ignore-not-found --wait=true >/dev/null 2>&1 || true
    kubectl -n "${ns}" run "${pod}" --restart=Never --image=curlimages/curl:8.11.0 \
      --command -- sh -c "curl -sf -o /dev/null -w '%{http_code}' ${url}" >/dev/null
    kubectl -n "${ns}" wait --for=jsonpath='{.status.phase}'=Succeeded pod/"${pod}" --timeout=20s >/dev/null 2>&1 || true
    code=$(kubectl -n "${ns}" logs "${pod}" 2>/dev/null || true)
    if [ "${code}" = "200" ]; then
      ok="true"
      break
    fi
    sleep 5
  done
  kubectl -n "${ns}" delete pod "${pod}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  if [ "${ok}" = "true" ]; then
    pass "${label}: /readyz returned 200 through svc/${METRICS_SVC} (a slurmrestd tick completed)"
    return 0
  fi
  fail "${label}: /readyz never returned 200 (last code: ${code:-<none>})"
  kubectl -n "${ns}" logs "deployment/${RELEASE}" --tail=80 || true
  return 1
}

# cr_condition <ns> <name> <field> — one field of the CR's Ready condition.
# jsonpath with a filter expression is used for READING only; `kubectl wait
# --for=condition=Ready` does not work on a custom resource's arbitrary
# condition list across kubectl versions, and `--for=jsonpath=` with a filter
# expression is likewise version-dependent — so every wait below is an
# explicit bounded poll instead (never an unbounded sleep).
cr_condition() {
  kubectl -n "$1" get workloadmixing "$2" \
    -o "jsonpath={.status.conditions[?(@.type=='Ready')].$3}" 2>/dev/null || true
}

cr_condition_dump() {
  echo "    observed Ready condition for ${2} in ${1}: status='$(cr_condition "$1" "$2" status)' reason='$(cr_condition "$1" "$2" reason)' message='$(cr_condition "$1" "$2" message)'"
}

# wait_cr_ready <ns> <name> <label> — bounded poll for Ready=True.
wait_cr_ready() {
  local ns="$1" name="$2" label="$3" i status=""
  for i in $(seq 1 30); do
    status=$(cr_condition "${ns}" "${name}" status)
    [ "${status}" = "True" ] && break
    sleep 5
  done
  if [ "${status}" = "True" ]; then
    pass "${label}: WorkloadMixing ${ns}/${name} reached Ready=True"
    return 0
  fi
  fail "${label}: WorkloadMixing ${ns}/${name} never reached Ready=True (last status '${status:-<none>}')"
  cr_condition_dump "${ns}" "${name}"
  kubectl -n "${ns}" logs "deployment/${RELEASE}" --tail=80 || true
  return 1
}

# wait_cr_refused <ns> <name> <reason> <message-substring> <label> — bounded
# poll for a REFUSAL: Ready=False with the expected reason and a message that
# names the trust anchor responsible, so an operator can act on it.
wait_cr_refused() {
  local ns="$1" name="$2" want_reason="$3" want_msg="$4" label="$5"
  local i status="" reason="" message=""
  for i in $(seq 1 24); do
    status=$(cr_condition "${ns}" "${name}" status)
    reason=$(cr_condition "${ns}" "${name}" reason)
    if [ "${status}" = "False" ] && [ "${reason}" = "${want_reason}" ]; then
      break
    fi
    sleep 5
  done
  message=$(cr_condition "${ns}" "${name}" message)
  if [ "${status}" = "False" ] && [ "${reason}" = "${want_reason}" ] && case "${message}" in *"${want_msg}"*) true ;; *) false ;; esac; then
    pass "${label}: ${ns}/${name} refused with Ready=False/${want_reason}, message names '${want_msg}'"
    echo "    message: ${message}"
    return 0
  fi
  fail "${label}: ${ns}/${name} was NOT refused as expected (wanted Ready=False/${want_reason} naming '${want_msg}')"
  cr_condition_dump "${ns}" "${name}"
  return 1
}

# The §3.1 source Secret: in a real install the Slurm operator creates this in
# the Slurm cluster's namespace. There is no Slurm operator here (see the
# header), so a stand-in is created — what is under test is the guide's COPY
# step, not who minted the key.
kubectl create namespace "${NS_SLURM}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "${NS_SLURM}" create secret generic slurm-auth-slurm \
  --from-literal=slurm.key=docs-e2e-fake-munge-key --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# -----------------------------------------------------------------------------
# B1 — §4.1 `configSource: file` (the chart default)
# -----------------------------------------------------------------------------
log "B1: §4.1 configSource=file install into ${NS_FILE}"
namespace_prereqs "${NS_FILE}"
deploy_mock "${NS_FILE}"

# NOTE on --set config.allowInsecureHTTP=true: the chart default is https and
# this mock speaks plaintext http, so the install MUST opt in explicitly.
# Needing this flag IS the secure default working as intended (A2 above asserts
# the default; here the opt-in is exercised) — if this install ever starts
# working without the flag, A2 has regressed.
helm install "${RELEASE}" "${CHART_DIR}" \
  --namespace "${NS_FILE}" --create-namespace \
  --set image.repository="${IMAGE_TAG%%:*}" \
  --set image.tag="${IMAGE_TAG##*:}" \
  --set image.pullPolicy=Never \
  --set config.namespace="${NS_FILE}" \
  --set config.localQueue=main \
  --set config.slurmRestURL="$(mock_url "${NS_FILE}")" \
  --set config.allowInsecureHTTP=true \
  --set config.pollInterval=5s \
  --wait --timeout=300s

assert_deployment_ready "${NS_FILE}" "B1" || true
# §6 tells operators to port-forward svc/k8s-bridge-metrics; assert that name
# exists, since a renamed Service silently breaks the documented verification.
if kubectl -n "${NS_FILE}" get "svc/${METRICS_SVC}" >/dev/null 2>&1; then
  pass "B1: the Service §6 names (svc/${METRICS_SVC}) exists"
else
  fail "B1: §6 tells operators to port-forward svc/${METRICS_SVC}, but no such Service exists in ${NS_FILE}"
fi
assert_readyz "${NS_FILE}" "B1" || true

# -----------------------------------------------------------------------------
# B2 — §4.2 supervisor mode + the SHIPPED SAMPLE CR
# -----------------------------------------------------------------------------
# The assertion that matters most in this script: `helm install --set
# configSource=cr` followed by `kubectl apply -f
# deploy/crd/workloadmixing-sample.yaml`, exactly as §4.2 prints it, must end
# with that CR Ready=True. That proves the sample's slurmTokenFile is inside
# the chart's own allowedTokenPaths default — a real conflict that existed and
# was fixed, and one that no unit test can catch because it is a disagreement
# between two shipped FILES (values.yaml and the sample), resolved only by a
# running supervisor.
# The sample CR carries its own metadata.namespace, and `kubectl apply -f`
# honors that over any -n flag — so B2 must run in exactly that namespace or it
# would be asserting on a CR that landed somewhere else entirely.
sample_ns=$(yaml_scalar "${SAMPLE_CR}" "namespace")
if [ -n "${sample_ns}" ] && [ "${sample_ns}" != "${NS_SUP}" ]; then
  log "B2: using the sample CR's own metadata.namespace (${sample_ns}) instead of NS_SUP=${NS_SUP} — kubectl apply -f honors the file's namespace"
  NS_SUP="${sample_ns}"
fi

log "B2: §4.2 supervisor mode into ${NS_SUP} (the guide's own namespace) + the shipped sample CR"
namespace_prereqs "${NS_SUP}"
deploy_mock "${NS_SUP}"

helm install "${RELEASE}" "${CHART_DIR}" \
  --namespace "${NS_SUP}" --create-namespace \
  --set image.repository="${IMAGE_TAG%%:*}" \
  --set image.tag="${IMAGE_TAG##*:}" \
  --set image.pullPolicy=Never \
  --set configSource=cr \
  --wait --timeout=300s

assert_deployment_ready "${NS_SUP}" "B2 (supervisor, no CRs yet)" || true

log "B2: kubectl apply -f deploy/crd/workloadmixing-sample.yaml (verbatim, as §4.2 instructs)"
kubectl apply -f "${SAMPLE_CR}"

# Only the ENDPOINT is patched, mirroring what docs/tutorial.md tells a user to
# do when their slurmrestd is not the in-cluster https one the sample assumes
# (same --type=merge shape, same two fields). The sample's own slurmTokenFile
# is deliberately left untouched: it is the subject of the assertion.
kubectl -n "${NS_SUP}" patch workloadmixing playground --type=merge \
  -p "{\"spec\":{\"slurmRestURL\":\"$(mock_url "${NS_SUP}")\",\"allowInsecureHTTP\":true}}" >/dev/null

sample_token_live=$(kubectl -n "${NS_SUP}" get workloadmixing playground -o jsonpath='{.spec.slurmTokenFile}')
log "B2: sample CR's slurmTokenFile is unchanged at '${sample_token_live}' (the chart's allowedTokenPaths default must accept it)"
wait_cr_ready "${NS_SUP}" playground "B2 (shipped sample CR under supervisor mode)" || true
assert_readyz "${NS_SUP}" "B2" || true

# -----------------------------------------------------------------------------
# B3 — §4.2 single-CR binding
# -----------------------------------------------------------------------------
# ORDERING NOTE (worth fixing in the guide): unlike supervisor mode, single-CR
# mode loads its config from the named CR AT STARTUP and exits if it is missing
# (cmd/k8s-bridge/main.go, modeSingleCR -> LoadConfigFromCR -> os.Exit(1)). The
# CR must therefore exist BEFORE `helm install`, or the pod CrashLoopBackOffs;
# §4.2's single-CR snippet does not say so. This script creates the CR first.
log "B3: §4.2 single-CR binding into ${NS_BIND}"
namespace_prereqs "${NS_BIND}"
deploy_mock "${NS_BIND}"

cat <<EOF | kubectl apply -f - >/dev/null
apiVersion: k8s-bridge.x-k8s.io/v1alpha1
kind: WorkloadMixing
metadata:
  name: playground
  namespace: ${NS_BIND}
spec:
  slurmRestURL: "$(mock_url "${NS_BIND}")"
  allowInsecureHTTP: true
  slurmTokenFile: "/var/run/secrets/slurm/token"
  localQueue: "main"
  pollInterval: "5s"
  partitionMappings:
    - partitionName: "mixing"
      workloadPriorityClass: "normal-priority"
  slurmd:
    image: "ghcr.io/slinkyproject/slurmd:26.05-ubuntu26.04"
    confServer: "slurm-controller.slurm.svc.cluster.local:6817"
    authSecretName: "slurm-auth-slurm"
EOF

# The documented command, verbatim except for the test image. It is run
# tolerantly (|| true) because it is itself under test: A7 predicts that a
# SECOND CR-mode install into a different namespace collides on the Role the
# chart still renders into config.namespace (default slurm-jobs, which B2's
# release already owns). If that happens, record it as the finding it is and
# retry with the workaround so the rest of B3 — and phase C — still run.
b3_out=""
b3_rc=0
set +e
b3_out=$(helm install "${RELEASE}" "${CHART_DIR}" \
  --namespace "${NS_BIND}" --create-namespace \
  --set image.repository="${IMAGE_TAG%%:*}" \
  --set image.tag="${IMAGE_TAG##*:}" \
  --set image.pullPolicy=Never \
  --set configSource=cr \
  --set workloadmixing.namespace="${NS_BIND}" \
  --set workloadmixing.name=playground \
  --wait --timeout=300s 2>&1)
b3_rc=$?
set -e
if [ "${b3_rc}" -ne 0 ]; then
  fail "B3: the §4.2 single-CR command as documented FAILED when a k8s-bridge release already exists in another namespace: $(echo "${b3_out}" | tr '\n' ' ' | cut -c1-400)"
  log "B3: retrying with --set config.namespace=${NS_BIND} (the workaround A7 names) so phase C still runs"
  helm install "${RELEASE}" "${CHART_DIR}" \
    --namespace "${NS_BIND}" --create-namespace \
    --set image.repository="${IMAGE_TAG%%:*}" \
    --set image.tag="${IMAGE_TAG##*:}" \
    --set image.pullPolicy=Never \
    --set configSource=cr \
    --set config.namespace="${NS_BIND}" \
    --set workloadmixing.namespace="${NS_BIND}" \
    --set workloadmixing.name=playground \
    --wait --timeout=300s
else
  pass "B3: the §4.2 single-CR install command works as documented"
fi

assert_deployment_ready "${NS_BIND}" "B3" || true
assert_readyz "${NS_BIND}" "B3" || true

log "PHASE B complete (${FAIL_COUNT} failure(s) so far)"

# =============================================================================
# PHASE C — LIVE SECURITY ASSERTIONS (negative tests)
# =============================================================================
# All three run in B2's supervisor-mode namespace, because supervisor mode is
# the only mode where a CR is untrusted input (ADR-0017: in file/single-CR mode
# the platform admin named the config source explicitly, so there is nothing to
# gate). Each new CR uses its OWN partitionName so the conflict rule
# (same slurmRestURL + overlapping partition => ConflictingSpec) cannot mask
# the reason under test.
log "PHASE C: live security assertions in ${NS_SUP}"

apply_cr() { # apply_cr <name> <spec-body>; returns kubectl's exit code, output on stdout
  cat <<EOF | kubectl apply -f - 2>&1
apiVersion: k8s-bridge.x-k8s.io/v1alpha1
kind: WorkloadMixing
metadata:
  name: $1
  namespace: ${NS_SUP}
spec:
$2
EOF
}

# --- C1: token-path allowlist ------------------------------------------------
log "C1: token-path allowlist (supervisor mode)"

# C1a — a path plainly outside every allowed prefix. This is the assertion that
# the anchor is wired at all in a live controller.
apply_cr evil-token-path "  slurmRestURL: \"$(mock_url "${NS_SUP}")\"
  allowInsecureHTTP: true
  slurmTokenFile: \"/etc/passwd\"
  localQueue: \"main\"
  pollInterval: \"5s\"
  partitionMappings:
    - partitionName: \"mixing-c1a\"
      workloadPriorityClass: \"normal-priority\"
  slurmd:
    image: \"ghcr.io/slinkyproject/slurmd:26.05-ubuntu26.04\"
    confServer: \"slurm-controller.slurm.svc.cluster.local:6817\"
    authSecretName: \"slurm-auth-slurm\""
wait_cr_refused "${NS_SUP}" evil-token-path InvalidSpec "allowed-token-paths" "C1a (token path outside the allowlist)" || true

# C1b — THE ACTUAL ATTACK ADR-0017 names: the controller's own ServiceAccount
# token, which the chart's Deployment leaves mounted, read by the controller
# and sent as a request header to whatever slurmRestURL this same CR chooses.
# A5 predicts this statically; this is the end-to-end proof.
apply_cr evil-sa-token "  slurmRestURL: \"$(mock_url "${NS_SUP}")\"
  allowInsecureHTTP: true
  slurmTokenFile: \"${SA_TOKEN_PATH}\"
  localQueue: \"main\"
  pollInterval: \"5s\"
  partitionMappings:
    - partitionName: \"mixing-c1b\"
      workloadPriorityClass: \"normal-priority\"
  slurmd:
    image: \"ghcr.io/slinkyproject/slurmd:26.05-ubuntu26.04\"
    confServer: \"slurm-controller.slurm.svc.cluster.local:6817\"
    authSecretName: \"slurm-auth-slurm\""
wait_cr_refused "${NS_SUP}" evil-sa-token InvalidSpec "allowed-token-paths" \
  "C1b (controller's own ServiceAccount token — the ADR-0017 attack; see A5 for the fix)" || true

# C1c — a bad CR must refuse ITSELF, not take down the shared controller: the
# healthy sample CR keeps running and the Deployment stays Ready.
if [ "$(cr_condition "${NS_SUP}" playground status)" = "True" ]; then
  pass "C1c: the healthy sample CR is still Ready — a refused CR does not take the shared supervisor down with it"
else
  fail "C1c: after the refused CRs, the healthy sample CR ${NS_SUP}/playground is no longer Ready — a bad CR must isolate itself, not break its neighbours"
  cr_condition_dump "${NS_SUP}" playground
fi
if kubectl -n "${NS_SUP}" wait --for=condition=Ready pod \
    -l "app.kubernetes.io/instance=${RELEASE}" --timeout=60s >/dev/null 2>&1; then
  pass "C1c: the controller Deployment is still Ready after two refused CRs"
else
  fail "C1c: the controller Deployment stopped being Ready after the refused CRs — a refused CR must not crash the shared process"
  kubectl -n "${NS_SUP}" logs "deployment/${RELEASE}" --tail=80 || true
fi

# --- C2: TLS escape-hatch gate ----------------------------------------------
# slurmInsecureSkipTLSVerify is a platform-admin decision, not a CR-author one
# (ADR-0017 L8): refused by default, honored after the admin opts in. Both
# halves are asserted, because a gate that can never open is a wall, and the
# guide documents it as an opt-in.
log "C2: TLS escape-hatch gate (allowInsecureTLS)"
apply_cr insecure-tls "  slurmRestURL: \"$(mock_url "${NS_SUP}")\"
  allowInsecureHTTP: true
  slurmInsecureSkipTLSVerify: true
  slurmTokenFile: \"/var/run/secrets/slurm/token\"
  localQueue: \"main\"
  pollInterval: \"5s\"
  partitionMappings:
    - partitionName: \"mixing-c2\"
      workloadPriorityClass: \"normal-priority\"
  slurmd:
    image: \"ghcr.io/slinkyproject/slurmd:26.05-ubuntu26.04\"
    confServer: \"slurm-controller.slurm.svc.cluster.local:6817\"
    authSecretName: \"slurm-auth-slurm\""
wait_cr_refused "${NS_SUP}" insecure-tls InvalidSpec "allow-insecure-tls" "C2a (slurmInsecureSkipTLSVerify without the admin opt-in)" || true

log "C2: helm upgrade --reuse-values --set allowInsecureTLS=true (the documented admin opt-in)"
helm upgrade "${RELEASE}" "${CHART_DIR}" \
  --namespace "${NS_SUP}" --reuse-values \
  --set allowInsecureTLS=true \
  --wait --timeout=300s
kubectl -n "${NS_SUP}" rollout status "deployment/${RELEASE}" --timeout=180s

wait_cr_ready "${NS_SUP}" insecure-tls "C2b (same CR after the admin opt-in)" || true

# --- C3: the CRD's CEL cleartext rule ---------------------------------------
# Not a controller check: the apiserver itself must reject an http:// CR that
# does not carry allowInsecureHTTP, so the cleartext-token mistake cannot even
# be persisted. This is what makes B1/B2 needing the explicit opt-in meaningful.
log "C3: CRD CEL rule rejects a cleartext CR at the apiserver"
set +e
c3_out=$(apply_cr cleartext-no-optin "  slurmRestURL: \"$(mock_url "${NS_SUP}")\"
  slurmTokenFile: \"/var/run/secrets/slurm/token\"
  localQueue: \"main\"
  pollInterval: \"5s\"
  partitionMappings:
    - partitionName: \"mixing-c3\"
      workloadPriorityClass: \"normal-priority\"
  slurmd:
    image: \"ghcr.io/slinkyproject/slurmd:26.05-ubuntu26.04\"
    confServer: \"slurm-controller.slurm.svc.cluster.local:6817\"
    authSecretName: \"slurm-auth-slurm\"")
c3_rc=$?
set -e
if [ "${c3_rc}" -ne 0 ] && echo "${c3_out}" | grep -qi "https"; then
  pass "C3: the apiserver rejected an http:// CR with no allowInsecureHTTP (exit ${c3_rc})"
  echo "    apiserver said: $(echo "${c3_out}" | tr '\n' ' ' | cut -c1-300)"
else
  fail "C3: an http:// WorkloadMixing with no allowInsecureHTTP was ACCEPTED (kubectl exit ${c3_rc}) — the CRD's CEL rule is not enforcing; the Slurm token could be configured into cleartext with no opt-in anywhere"
  echo "    kubectl output: ${c3_out}"
  kubectl -n "${NS_SUP}" delete workloadmixing cleartext-no-optin --ignore-not-found >/dev/null 2>&1 || true
fi

log "PHASE C complete"
log "Coverage recap: §1.3/§1.4 prerequisites, §2 queue objects, §3 token Secret, §3.1 auth-key copy, §4.1 file mode, §4.2 supervisor + single-CR binding, §4.3 secure defaults, §6's metrics Service. NOT covered: real Slurm, §1.2/§1.5/§1.6 prerequisites, §5 ray-bridge, §7 GKE."
summarize_and_exit
