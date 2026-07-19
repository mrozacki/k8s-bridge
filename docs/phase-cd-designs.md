# Phase C/D design notes (delivered as designs; implementation pending)

Written early in the project. Each section is scoped so an
engineer can pick it up without this conversation.

## C1. Array jobs → indexed completions (design)

Mapping: one Slurm array job of N elements → ONE JobSet whose Job uses
`completionMode: Indexed`, `completions=N`, per-element env
`SLURM_ARRAY_TASK_ID` = completion index; each pod is a single-task dynamic
node pinned by the same `nodes-for-<arrayJobId>` feature. Throttling
(`--array=0-999%50`) maps to `parallelism`. Open issues: (a) Slurm array
elements get individual job IDs on start — the bridge must translate
element states back (REST exposes `array_job_id`/`array_task_id`); (b)
Kueue sees ONE workload — per-element preemption granularity is lost
(document as semantic narrowing, mirroring TAS/switches). Keep the lua
rejection until this lands.

## C2. TPU support (design sketch, blocked on hardware)

Translation target: GKE TPU podslices (`google.com/tpu` resource +
`cloud.google.com/gke-tpu-topology` selectors). Slurm has no TPU GRES
convention — proposal: `--gres=tpu:<type>:<count>` accepted by the lua
plugin and rendered to TPU nodeSelector + resource requests; TAS levels map
to TPU topology labels natively (same mechanism as experiment 05). Needs
TPU quota + a v5e/v6e sandbox session.

## C3. DWS Flex / CCC validation plan

Kueue side is ready (AdmissionChecks + ProvisioningRequest for queued
provisioning). Plan: nodepool with `--enable-queued-provisioning` + Kueue
ProvisioningRequestConfig; bridge jobs need NO changes (they are plain
JobSets). CCC: ComputeClass CR with a priority list (spot e2 → on-demand
e2) referenced by the TAS/main flavor's nodeLabels; validate autoscaler
picks classes per priority. Both are configuration exercises on the
playground — deferred for cost, not complexity.

## D1. WAS readiness audit (thin-surface check)

Bridge's entire Kueue coupling, enumerated: (1) `queue-name` label,
(2) `priority-class` label, (3) `podset-*-topology` annotations,
(4) Workload read for status/priority (one GET/list + one PATCH),
(5) suspend semantics on JobSet honored by any admission controller.
Items 1-3 are declarative pass-throughs; item 4 is isolated in
`internal/bridge/kueue.go` (one file to swap); item 5 is upstream JobSet,
not Kueue. Verdict: the WAS migration surface is ~200 lines. Keep it that
way — CI could enforce an import allowlist (no kueue imports outside
kueue.go).

## D2. Independence from the Slurm Operator (design)

Current couplings: auth key secret name/format, conf-server address,
slurmrestd deployment, login pod conventions. Proposal: `WorkloadMixing`
CR grows an `externalSlurm:` block (restURL + secretRefs for key/JWT) and
the operator-specific discovery becomes one adapter behind an interface.
Prereq: OpenAPI-generated clients (version matrix) since external clusters
won't track Slinky's Slurm version.

## D3. Upstream: duration-aware backfill for Kueue (proposal sketch)

Problem: BestEffortFIFO admits past a stuck head-of-line workload but
cannot bound the head's delay (no time model). Proposal: optional
`workload.spec.expectedDuration`; scheduler computes a projected start for
the head workload from running workloads' durations and admits a smaller
workload only if it finishes before that projection (classic conservative
backfill). Sources: Slurm `--time` (bridge sets it — already parsed),
Ray/K8s via annotation. Positioning: this is the single biggest UX gap vs
Slurm (finding #4) and a natural Google-led contribution per the OSS
strategy.

## D4. SchedMD/NVIDIA cooperation posture

Unchanged from the strategy doc (their "Slurm Bridge + Kueue" counter-
proposal pending evaluation). Technical ammunition from this workspace:
the validated bridge cycle, the reclaim/fair-sharing story, and the UX
delta list — bring these to the comparison table.
