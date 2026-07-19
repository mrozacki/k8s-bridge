#!/usr/bin/env bash
# WAS-readiness guard (owner-approved P5): the Kueue API surface must stay
# confined to the shared internal/kueue package. Labels/annotations
# (declarative pass-throughs like kueue.x-k8s.io/queue-name) are allowed
# anywhere; API-object access (constructing the Workload GVK, naming a
# kueue.x-k8s.io API version) is not.
#
# ADR-0012: the confinement point moved from internal/bridge/kueue.go into the
# shared internal/kueue/kueue.go, which both bridges (Slurm k8s-bridge and
# ray-bridge) now depend on. internal/bridge/kueue.go delegates to it.
set -euo pipefail
cd "$(dirname "$0")/.."
violations=$(grep -rnE '"kueue\.x-k8s\.io", Version:|kueue\.x-k8s\.io/v1beta' \
  --include="*.go" cmd/ internal/ \
  | grep -v "internal/kueue/kueue.go" \
  | grep -v "_test.go" || true)
if [ -n "$violations" ]; then
  echo "Kueue API surface leaked outside internal/kueue/kueue.go:"
  echo "$violations"
  exit 1
fi
echo "thin-surface check OK (Kueue API confined to internal/kueue/kueue.go)"
