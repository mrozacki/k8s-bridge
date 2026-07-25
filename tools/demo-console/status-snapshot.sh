#!/usr/bin/env bash
# status-snapshot.sh — emits ONE JSON snapshot of cluster/bridge/Slurm state,
# reusing the exact kubectl queries from ../bridge-top.sh (kept in sync by
# hand; both are thin wrappers around the same handful of `kubectl get`
# calls). Used by demo-console's status API when no Grafana URL is
# configured, so the presenter still sees live state on one screen.
#
# Usage: ./status-snapshot.sh   -> prints one JSON object to stdout
#
# Requires: kubectl pointed at the playground cluster. Degrades gracefully
# (empty arrays / null fields) when the cluster or Slurm login pod is
# unreachable — same philosophy as bridge-top.sh.
set -uo pipefail

jesc() { # JSON-escape a string for embedding in a hand-built JSON blob
  local s="${1:-}"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  s="${s//$'\n'/\\n}"
  printf '%s' "$s"
}

# json_objects KEY... — reads rows on stdin and prints them as a comma-separated
# list of JSON objects, mapping the Nth field of each row to the Nth KEY.
# Fields are separated by \037 (ASCII unit separator): it cannot occur in
# kubectl/squeue output and, unlike a tab, bash's read does not collapse runs of
# it, so empty fields survive.
#
# Why not interpolate awk output straight into the JSON like this used to?
# Because these values are free-form text. squeue's %r reason field in
# particular is a human-readable string that can contain a double quote, which
# produced a malformed JSON body the page then failed to parse. Every value
# goes through jesc() here instead, so no field can break out of its string.
# (Deliberately not jq: the whole demo console is dependency-free — node core
# modules and bash only — and jq is optional everywhere else in this repo.)
json_objects() {
  local keys=("$@") out="" obj i
  local -a f
  while IFS=$'\037' read -r -a f; do
    obj=""
    for i in "${!keys[@]}"; do
      [ -n "$obj" ] && obj="${obj},"
      obj="${obj}\"${keys[$i]}\":\"$(jesc "${f[$i]:-}")\""
    done
    [ -n "$out" ] && out="${out},"
    out="${out}{${obj}}"
  done
  printf '%s' "$out"
}

context="$(kubectl config current-context 2>/dev/null || true)"

nodes_json="$(kubectl get nodes --no-headers 2>/dev/null | awk '{printf "%s\037%s\n", $1, $2}' | json_objects name status)"

queues_json="$(kubectl get clusterqueues --no-headers 2>/dev/null | awk '{printf "%s\037%s\n", $1, $NF}' | json_objects name pending)"

workloads_json="$(kubectl get workloads -A --no-headers 2>/dev/null | awk '{printf "%s\037%s\037%s\037%s\037%s\n", $1, $2, $3, $5, $NF}' | json_objects namespace name queue admitted age)"

jobsets_json="$(kubectl get jobsets -n slurm-jobs --no-headers 2>/dev/null | awk '{printf "%s\037%s\037%s\n", $1, $3, $5}' | json_objects name restarts suspended)"

squeue_json="$(kubectl -n slurm exec deploy/slurm-login-slinky -- squeue -h -o "%i|%P|%T|%r" 2>/dev/null \
  | awk -F'|' '{printf "%s\037%s\037%s\037%s\n", $1, $2, $3, $4}' | json_objects id partition state reason)"

sinfo_json="$(kubectl -n slurm exec deploy/slurm-login-slinky -- sinfo -h -N -o "%N|%P|%t" 2>/dev/null \
  | awk -F'|' '{printf "%s\037%s\037%s\n", $1, $2, $3}' | json_objects node partition state)"

bridge_ready="$(kubectl get workloadmixing -A -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' 2>/dev/null | head -1)"
[ -z "$bridge_ready" ] && bridge_ready="unknown"

cat <<EOF
{
  "generatedAt": "$(date -u '+%Y-%m-%dT%H:%M:%SZ')",
  "context": "$(jesc "$context")",
  "bridgeReady": "$(jesc "$bridge_ready")",
  "nodes": [${nodes_json}],
  "clusterQueues": [${queues_json}],
  "workloads": [${workloads_json}],
  "jobsets": [${jobsets_json}],
  "slurmQueue": [${squeue_json}],
  "slurmNodes": [${sinfo_json}]
}
EOF
