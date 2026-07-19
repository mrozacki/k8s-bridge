# ADR-0003: Run the phase-1 playground directly on GKE (skip local kind)

- **Status:** Accepted
- **Date:** 2026-07-03

## Context

One option was a zero-cost local `kind` cluster for phase 1 before touching
GKE. We chose to go straight to GKE, accepting a small per-session cloud cost.

## Decision

Phase 1 (component playground) runs on a minimal GKE Standard cluster from the
start. No local kind environment is maintained.

## Alternatives considered

- **kind first, GKE later** — rejected: the
  value of the playground is higher on the real target platform, and features
  central to the value proposition (Cluster Autoscaler, DWS Flex, Custom
  Compute Classes, spot capacity) simply do not exist in kind. Two
  environments also mean duplicated setup work.

## Consequences

- Strict teardown discipline: the teardown script is part of every experiment
  run, so a cluster is never left running after a session ends.
- Cluster choices are cost-optimized: zonal cluster (management fee covered by
  the GKE free tier for one cluster), spot node pool, scale-to-zero secondary
  pools, smallest machine types that fit the components.
- We can exercise autoscaling and obtainability features in phase 1 already,
  which a kind-based plan would have deferred to phase 4.
