#!/usr/bin/env bash
# bridge-top — terminal dashboard for the k8s-bridge playground.
#
# Shows, refreshed every N seconds (default 5):
#   * cluster nodes
#   * Kueue ClusterQueue quota usage and pending/admitted workloads
#   * JobSets and slurmd pods managed by the bridge
#   * Slurm queue (squeue) and node list (sinfo)
#
# Usage:  ./tools/bridge-top.sh [refresh-seconds]
# Quit:   Ctrl+C
#
# Requires: kubectl pointed at the playground cluster. The Slurm panel needs
# the login pod (slurm-login-slinky); it degrades gracefully when absent.
set -uo pipefail

INTERVAL="${1:-5}"
BOLD=$(tput bold 2>/dev/null || true)
DIM=$(tput dim 2>/dev/null || true)
RST=$(tput sgr0 2>/dev/null || true)

hdr() { printf "%s── %s %s\n" "${BOLD}" "$1" "${RST}"; }

panel_nodes() {
  hdr "NODES"
  kubectl get nodes --no-headers 2>/dev/null \
    | awk '{printf "  %-55s %s\n", $1, $2}' || echo "  (cluster unreachable)"
}

panel_queues() {
  hdr "KUEUE / CLUSTERQUEUE"
  kubectl get clusterqueues --no-headers 2>/dev/null | awk '{printf "  %-20s pending:%-4s\n", $1, $NF}'
  kubectl get clusterqueue main-queue -o jsonpath='{range .status.flavorsUsage[*].resources[*]}  used {.name}: {.total}{"\n"}{end}' 2>/dev/null
  echo
  hdr "WORKLOADS (all namespaces)"
  printf "  %-12s %-38s %-10s %-9s %s\n" "NAMESPACE" "NAME" "QUEUE" "ADMITTED" "AGE"
  kubectl get workloads -A --no-headers 2>/dev/null \
    | awk '{printf "  %-12s %-38s %-10s %-9s %s\n", $1, substr($2,1,38), $3, $5, $NF}' \
    || echo "  (none)"
}

panel_bridge() {
  hdr "BRIDGE JOBSETS (namespace: slurm-jobs)"
  kubectl get jobsets -n slurm-jobs --no-headers 2>/dev/null \
    | awk '{printf "  %-30s restarts:%-3s suspended:%s\n", $1, $3, $5}' \
    || true
  kubectl get pods -n slurm-jobs --no-headers 2>/dev/null \
    | awk '{printf "  pod %-42s %-18s ready:%s\n", $1, $3, $2}' \
    || true
  [ -z "$(kubectl get jobsets -n slurm-jobs --no-headers 2>/dev/null)" ] && echo "  (no bridge-managed JobSets)"
}

panel_topology() {
  # Shown only when the simulated topology labels exist (experiment 05).
  local nodes
  nodes=$(kubectl get nodes -l example.com/topology-managed=true \
    -L example.com/block,example.com/rack --no-headers 2>/dev/null) || return 0
  [ -z "$nodes" ] && return 0
  hdr "TOPOLOGY (simulated blocks/racks + pod placement)"
  while read -r name _ _ _ _ block rack; do
    local pods
    pods=$(kubectl get pods -A --field-selector "spec.nodeName=${name}" --no-headers 2>/dev/null \
      | awk '$1=="slurm-jobs" || $1=="default" {printf "%s ", $2}')
    printf "  %-9s %-8s %-50s %s\n" "${block}" "${rack}" "${name}" "${pods:--}"
  done <<< "$nodes" | sort
  echo
}

panel_slurm() {
  hdr "SLURM QUEUE (squeue)"
  kubectl -n slurm exec deploy/slurm-login-slinky -- squeue -o "%.6i %.9P %.10T %.14r %.18f %N" 2>/dev/null \
    | sed 's/^/  /' || echo "  (login pod unavailable)"
  echo
  hdr "SLURM NODES (sinfo)"
  kubectl -n slurm exec deploy/slurm-login-slinky -- sinfo -N -o "%.28N %.10P %.8t %f" 2>/dev/null \
    | sed 's/^/  /' || echo "  (login pod unavailable)"
}

while true; do
  OUT="$(
    printf "%sk8s-bridge playground%s   %s   ctx: %s\n\n" \
      "${BOLD}" "${RST}" "$(date '+%H:%M:%S')" "$(kubectl config current-context 2>/dev/null | cut -c1-60)"
    panel_nodes; echo
    panel_queues; echo
    panel_bridge; echo
    panel_topology
    panel_slurm
    printf "\n%srefresh: %ss — Ctrl+C to quit%s\n" "${DIM}" "${INTERVAL}" "${RST}"
  )"
  clear 2>/dev/null || printf "\n\n"
  printf "%s\n" "$OUT"
  sleep "${INTERVAL}"
done
