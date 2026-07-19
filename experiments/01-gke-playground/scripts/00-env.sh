#!/usr/bin/env bash
# Shared environment for experiment 01. Source this file, do not execute it:
#   source scripts/00-env.sh

# Set PROJECT_ID in your shell before sourcing (kept out of the repo so no
# personal project id is published): export PROJECT_ID=my-sandbox-project
export PROJECT_ID="${PROJECT_ID:?set PROJECT_ID to your GCP project, e.g. export PROJECT_ID=my-sandbox-project}"
export ZONE="${ZONE:-europe-west1-b}" # zonal cluster: management fee covered by GKE free tier
export CLUSTER_NAME="k8s-bridge-playground"

# Sizing levers — keep these small. ${VAR:-default} lets a caller override,
# e.g. `NUM_NODES=4 ./scripts/01-create-cluster.sh` for experiment 05.
export MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}" # 4 vCPU/16Gi, spot
export NUM_NODES="${NUM_NODES:-2}"
export MAX_NODES="${MAX_NODES:-4}"

# Component versions (verified 2026-07-03 against upstream release pages).
export KUEUE_VERSION="v0.18.2"
export JOBSET_VERSION="v0.12.0"
export KUBERAY_VERSION="1.6.2"
# slurm-operator publishes no GitHub releases; charts live in the OCI registry
# ghcr.io/slinkyproject/charts. Empty version = latest chart.
export SLURM_OPERATOR_CHART_VERSION=""

gcloud config set project "${PROJECT_ID}" >/dev/null
echo "Environment loaded: project=${PROJECT_ID} zone=${ZONE} cluster=${CLUSTER_NAME}"
