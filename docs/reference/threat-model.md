# k8s-bridge threat model / security self-assessment

Status: initial (2026-07-05). Lightweight, CNCF TAG-Security-style self-assessment.
Revisit on any change to the trust boundaries below.

k8s-bridge is a controller that discovers held Slurm jobs and materializes each
as a Kubernetes JobSet admitted through Kueue, so Kueue arbitrates a mixed
Slurm + Kubernetes resource pool. This document states what it protects, who can
attack it, and how the design and configuration defend each boundary.

## 1. Assets

| Asset | Why it matters |
|-------|----------------|
| Kubernetes node integrity | slurmd pods run **privileged**; whoever controls their image gets root on the node. |
| Slurm REST credential (JWT) | Bearer-equivalent; controls the whole Slurm side (submit/cancel/reprioritize). |
| Fair admission / priority ordering | The product's whole point — Kueue arbitrating a shared pool. Priority escalation defeats it. |
| Tenant isolation | Multiple teams submit into the same pool; one must not read/preempt another's work or storage. |
| Controller availability | It is the admission path for all Slurm work; if it stops, Slurm jobs never admit. |

## 2. Actors / trust boundaries

| Actor | Trust | Can do |
|-------|-------|--------|
| Platform admin | Trusted | Installs the chart, sets controller flags (image allowlist), writes `WorkloadMixing`. |
| Controller ServiceAccount | Trusted-but-contained | Creates JobSets, patches Kueue Workloads, reads/updates the CR in managed namespaces. |
| Slurm job owner (tenant) | **Untrusted** | Submits jobs; sets job name, resource requests, comment, `--switches`; runs `scontrol update priority`. |
| Namespace tenant (K8s) | **Untrusted** | May have `edit` in their own namespace. Must NOT be able to edit `WorkloadMixing`. |
| On-path pod | **Untrusted** | Another workload on the cluster network. |

Primary boundary: **untrusted Slurm/K8s tenants → trusted controller → node root.**
The controller must not let tenant-controlled input cross into node compromise,
credential theft, priority escalation, or cross-tenant access.

## 3. Data flows

1. Controller → slurmrestd (HTTPS + `X-SLURM-USER-TOKEN`): list/hold/release/priority.
2. Controller → kube-apiserver (SA token): create JobSets, patch Workloads, update CR status.
3. Tenant → Slurm → controller: job fields and the `admin_comment` priority directive (ADR-0009).
4. Controller → JobSet → slurmd pod: privileged pod running the configured image, mounting the Slurm auth secret and optional NFS storage.

## 4. Threats and mitigations

| # | Threat | Mitigation | Residual risk |
|---|--------|-----------|---------------|
| T1 | Attacker-chosen image runs privileged → node root (audit C1) | Deploy-time `--allowed-slurmd-images` allowlist (trust anchor NOT taken from the CR); CRD CEL/patterns; `WorkloadMixing` documented as platform-admin-only; RBAC namespaced by default | slurmd is still privileged — an allowed-but-compromised image is root. Upstream dependency: minimal-capability slurmd (see T7). |
| T2 | Tenant escalates priority, jumping the mixed queue (audit H1) | `maxUserPriority` cap enforced in the controller (`capUserPriority`) on both the directive and Slurm-side-deviation paths; defense-in-depth clamp in the lua plugin | A tenant can still reach the cap; ordering within the cap is by submission. |
| T3 | Slurm JWT stolen in transit (audit H3) | `https` required unless `allowInsecureHTTP` explicitly set; client verifies TLS (optional CA bundle); token mounted read-only, `defaultMode 0400`, re-read per request, never logged; default `slurmUser` is a low-privilege user, not root | Cleartext still possible if an operator opts in for dev. |
| T4 | Cross-tenant config abuse via `WorkloadMixing` (audit H4) | Namespaced CR; documented as platform-admin resource; excluded from default `edit`/`admin` aggregated roles; controller validates config (shared `Validate`) | Requires correct RBAC at install time — operator responsibility. |
| T5 | DoS: hot-loop poll, unbounded lists, pprof exhaustion | Minimum `pollInterval` enforced; per-tick single snapshot (O(n) fix); CRD `maxItems`/`maxLength` caps; pprof off by default and bound to localhost with timeouts; controller resource limits in the chart | Large legitimate backlogs still cost API traffic. |
| T6 | Integer overflow from untrusted Slurm fields (audit M4) | Saturating conversions (`clampUint64ToInt32`, `safeUint32`) on task/node counts and priorities; `gosec` G115 in CI | — |
| T7 | slurmd requires privileged (upstream constraint) | `slurmd.privileged` switch to trial a minimal-capability context; documented as an upstream (slurmd/slinky) dependency, not fixable in k8s-bridge alone | Open until upstream supports unprivileged slurmd. |
| T8 | Supply-chain compromise of controller image / deps | Dependabot (gomod + actions); `govulncheck` in CI; image digest-pinning supported in the chart; least-privilege `GITHUB_TOKEN` | SHA-pinning of GitHub Actions is a documented follow-up (see backlog). |
| T9 | Controller unavailability halts admission | Single replica today; resource limits + probes; leader-election RBAC planned before raising replicas | No HA yet (documented). |

## 5. Non-goals (current)

- High availability / leader election (single replica).
- Unprivileged slurmd (blocked on upstream).
- Network-level isolation is opt-in (`networkPolicy.enabled`), not default.
- Admission webhooks / policy enforcement beyond CRD schema + controller validation.

## 6. Assumptions

- The platform admin installs the chart correctly, sets a real
  `--allowed-slurmd-images`, and does not expose `WorkloadMixing` to tenants.
- The target namespace enforces the `restricted` Pod Security Standard for
  everything except the intentionally-privileged slurmd pods.
- slurmrestd is reachable only over TLS in production.
