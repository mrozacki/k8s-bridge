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

context="$(kubectl config current-context 2>/dev/null || true)"

nodes_json="$(kubectl get nodes --no-headers 2>/dev/null | awk '{printf "{\"name\":\"%s\",\"status\":\"%s\"},", $1, $2}' | sed 's/,$//')"

queues_json="$(kubectl get clusterqueues --no-headers 2>/dev/null | awk '{printf "{\"name\":\"%s\",\"pending\":\"%s\"},", $1, $NF}' | sed 's/,$//')"

workloads_json="$(kubectl get workloads -A --no-headers 2>/dev/null | awk '{printf "{\"namespace\":\"%s\",\"name\":\"%s\",\"queue\":\"%s\",\"admitted\":\"%s\",\"age\":\"%s\"},", $1, $2, $3, $5, $NF}' | sed 's/,$//')"

jobsets_json="$(kubectl get jobsets -n slurm-jobs --no-headers 2>/dev/null | awk '{printf "{\"name\":\"%s\",\"restarts\":\"%s\",\"suspended\":\"%s\"},", $1, $3, $5}' | sed 's/,$//')"

squeue_json="$(kubectl -n slurm exec deploy/slurm-login-slinky -- squeue -h -o "%i|%P|%T|%r" 2>/dev/null \
  | awk -F'|' '{printf "{\"id\":\"%s\",\"partition\":\"%s\",\"state\":\"%s\",\"reason\":\"%s\"},", $1, $2, $3, $4}' | sed 's/,$//')"

sinfo_json="$(kubectl -n slurm exec deploy/slurm-login-slinky -- sinfo -h -N -o "%N|%P|%t" 2>/dev/null \
  | awk -F'|' '{printf "{\"node\":\"%s\",\"partition\":\"%s\",\"state\":\"%s\"},", $1, $2, $3}' | sed 's/,$//')"

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
