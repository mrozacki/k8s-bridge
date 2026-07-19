# Experiment 08 — Custom Compute Classes + DWS queued provisioning

- **Status:** executed 2026-07-05; details in
  [docs/VALIDATION.md](../../docs/VALIDATION.md)
- **Setup:** minimal cluster (Kueue only + node auto-provisioning), ~40 min run

**CCC — PASSED.** `manifests/compute-classes.yaml`: class `econo`
(spot E2 → on-demand E2) and `perf` (on-demand N2); node
auto-provisioning created matching pools in ~4 min; probes verified via
node labels (`machine-family`, `gke-spot`, `compute-class`). Scale-tier
CCC test (fallback storms, E2/N2/C3 under quota pressure) = backlog S5.

**DWS — chain proven; CPU-only rejected by GKE policy.** Flag matrix that
works: `--enable-queued-provisioning` + `--reservation-affinity=none`,
no spot. Kueue AdmissionCheck → ProvisioningRequest created; GKE's final
answer: `Failed: Resize requests without accelerators are not supported`
→ DWS E2E requires an accelerator, merged with the GPU-quota session
(backlog A5).
