#!/usr/bin/env bash
# Installs the four building blocks of the mixing stack:
#   cert-manager (slurm-operator prerequisite), JobSet, Kueue, KubeRay,
#   slurm-operator + a minimal Slurm cluster.
#
# Order matters: framework CRDs (JobSet, RayCluster) must exist before Kueue
# is (re)configured to integrate with them.
set -euo pipefail
source "$(dirname "$0")/00-env.sh"

echo "==> cert-manager (required by slurm-operator webhooks)"
helm upgrade --install cert-manager oci://quay.io/jetstack/charts/cert-manager \
  --namespace cert-manager --create-namespace \
  --set crds.enabled=true \
  --wait

echo "==> JobSet ${JOBSET_VERSION}"
kubectl apply --server-side -f \
  "https://github.com/kubernetes-sigs/jobset/releases/download/${JOBSET_VERSION}/manifests.yaml"

echo "==> KubeRay operator ${KUBERAY_VERSION}"
helm repo add kuberay https://ray-project.github.io/kuberay-helm/ --force-update
helm upgrade --install kuberay-operator kuberay/kuberay-operator \
  --version "${KUBERAY_VERSION}" \
  --namespace kuberay --create-namespace \
  --wait

echo "==> Kueue ${KUEUE_VERSION}"
kubectl apply --server-side -f \
  "https://github.com/kubernetes-sigs/kueue/releases/download/${KUEUE_VERSION}/manifests.yaml"
kubectl -n kueue-system set resources deployment kueue-controller-manager --limits=memory=1Gi
kubectl -n kueue-system rollout status deployment/kueue-controller-manager --timeout=180s

# Verified 2026-07-04: Kueue v0.18.2 default config already enables all
# frameworks we need (batch/job, jobset.x-k8s.io/jobset, ray.io/raycluster,
# ray.io/rayjob, ray.io/rayservice) — no ConfigMap patching required.

echo "==> slurm-operator (Slinky)"
CHART_VERSION_FLAG=""
[ -n "${SLURM_OPERATOR_CHART_VERSION}" ] && CHART_VERSION_FLAG="--version ${SLURM_OPERATOR_CHART_VERSION}"
helm upgrade --install slurm-operator-crds oci://ghcr.io/slinkyproject/charts/slurm-operator-crds ${CHART_VERSION_FLAG}
helm upgrade --install slurm-operator oci://ghcr.io/slinkyproject/charts/slurm-operator \
  --namespace slinky --create-namespace ${CHART_VERSION_FLAG} \
  --wait

echo "==> Minimal Slurm cluster (1 small nodeset, REST API enabled)"
helm upgrade --install slurm oci://ghcr.io/slinkyproject/charts/slurm \
  --namespace slurm --create-namespace ${CHART_VERSION_FLAG} \
  --values "$(dirname "$0")/../manifests/slurm-values.yaml" \
  --wait --timeout 15m

echo
echo "All components installed. Verify with:"
echo "  kubectl get pods -A | grep -E 'cert-manager|jobset|kueue|kuberay|slinky|slurm'"
