# ADR-0017: CR-supplied file paths and TLS verification are deploy-time trust anchors

- **Status:** Accepted
- **Date:** 2026-07-25
- **Supersedes / amends:** none (extends the C1 trust-anchor pattern from the
  security audit and the supervisor model of ADR-0015)

## Context

Reviewing ADR-0015 Phase A against the audit's C1 finding (see
`docs/reference/threat-model.md`) surfaced a trust boundary that multi-CR
supervisor mode widens and single-CR mode never had.

Supervisor mode adopts **every** `WorkloadMixing` CR in `POD_NAMESPACE` and
builds a Slurm client per CR. Three CR fields are attacker-relevant together:

| Field | What the controller does with it |
|---|---|
| `slurmTokenFile` | reads that path **on the controller's own filesystem**, per request |
| `slurmCACertFile` | same, read at client construction |
| `slurmRestURL` | the destination the token's contents are sent to, as `X-SLURM-USER-TOKEN` |

Because one CR author supplies all three, an unrestricted `slurmTokenFile` is
an arbitrary-file read wired to a sink the same author chooses. The obvious
target is the controller's own ServiceAccount token at
`/var/run/secrets/kubernetes.io/serviceaccount/token`, exfiltrated over a
perfectly valid `https://` connection — so neither the `allowInsecureHTTP`
gate nor the CRD's CEL rule sees anything wrong.

A fourth field, `slurmInsecureSkipTLSVerify`, let a CR author silently turn
off slurmrestd certificate verification for their own loop.

Pre-ADR-0015 this was latent: only the one CR named by
`--workloadmixing <ns>/<name>` was ever read. Today it is bounded by RBAC —
`WorkloadMixing` is namespaced, granted only to the controller by the chart,
and deliberately **not** aggregated into the default `edit`/`admin` cluster
roles — so creating one requires a grant a platform admin controls. But
ADR-0015's stated purpose is running many, potentially tenant-owned CRs, and
Phase B (cluster-wide watch) is on the roadmap. At that step the finding stops
being latent.

## Decision

Treat all four fields the way the audit's C1 finding taught us to treat the
privileged slurmd image: **the CR author is exactly who the control defends
against, so the control must live at deploy time, where the platform admin
owns it.**

1. **`--allowed-token-paths`** (chart: `allowedTokenPaths`, default
   `/var/run/secrets/`, `/etc/k8s-bridge/`) bounds which directory prefixes a
   CR may name in `slurmTokenFile` / `slurmCACertFile`.
2. **`--allow-insecure-tls`** (chart: `allowInsecureTLS`, default `false`)
   must be set before a CR's `slurmInsecureSkipTLSVerify` is honored.
3. Both are enforced in `Supervisor.buildConfig` — the existing per-CR vetting
   hook, alongside the slurmd image allowlist. A refused CR gets
   `Ready=False` / `InvalidSpec` naming the flag, plus a Warning event, and
   does **not** kill the shared process.

### Why supervisor-mode only

Enforcement is deliberately **not** applied in file mode or explicit single-CR
mode (`--workloadmixing <ns>/<name>`). There the config comes from a source
the platform admin named explicitly; a CR is untrusted input only when it is
picked up **automatically**. Restricting the admin's own chosen config would
add friction with no attacker removed — and would break the documented
workstation tutorial, which legitimately reads a token from `/tmp`.

### Path matching is boundary-aware, not a string prefix

Two bypasses a naive `strings.HasPrefix` would allow are closed in
`config.ValidateFilePathAllowed`:

- **Traversal inside an allowed prefix.** The path is `filepath.Clean`ed
  *before* matching, so `/var/run/secrets/../../var/lib/kubelet/token` is
  rejected rather than accepted on its harmless-looking first segment.
  Relative paths are rejected outright — they resolve against the
  controller's working directory, which is not a stable trust base.
- **Sibling sharing the prefix.** Allowing `/var/run/secrets` must not also
  allow `/var/run/secretsomething`. Matching requires the candidate to equal
  the prefix or sit beneath it at a separator boundary. Prefixes are accepted
  with or without a trailing separator.

An unset (empty) field reads no file at all, so it is skipped — this check can
only reject a path the controller was actually going to open, which keeps the
change strictly additive.

## Alternatives considered

- **Resolve the token from a Secret in the CR's own namespace instead of a
  filesystem path** (a `slurmTokenSecretRef`-style field). Strictly better
  long-term: a CR could only ever reference its own tenant's credentials, and
  the whole class of "read the controller's filesystem" disappears. Not done
  here because no loader consumes such a field yet, and adding an API field
  is a bigger change than this fix warrants. **This ADR does not implement
  that fix — it buys time for it** (see Follow-up below).
- **A CRD CEL rule for `slurmInsecureSkipTLSVerify`.** This was the fix
  originally sketched when the finding was reported, and it was rejected on
  implementation: CEL validates what the CR author writes, and the CR author
  is the adversary in this threat model. A rule could only enforce internal
  consistency (e.g. "don't set a CA bundle and skip verification at once"),
  which is misconfiguration hygiene, not a security boundary. Deploy-time
  gating is what actually removes the capability. The consistency rule remains
  available as a separate usability improvement.
- **Hardcoding the allowlist** to the chart's mount point. Rejected: operators
  legitimately mount tokens elsewhere, and a control people must patch out to
  work gets patched out entirely.
- **Defaulting the allowlist to empty** (like `allowedSlurmdImages`).
  Rejected for this field: unlike container registries, where any registry may
  be legitimate, the token path in supervisor mode is always a mounted Secret
  and the chart controls the mount point, so a secure default is achievable
  without guessing. `allowedSlurmdImages` keeps its empty default for
  backward compatibility; this flag does not need to repeat that compromise.

## Consequences

**Positive.** The supervisor now has one uniform per-CR vetting stage
covering image, file paths, and TLS posture, all deploy-time owned.
Regression tests pin both the refusal and the allowed path
(`TestSupervisorTokenPathAllowlist`, `TestSupervisorInsecureTLSGate`), plus
the two bypass classes (`TestValidateFilePathAllowed`).

**Negative / breaking.** A supervisor-mode deployment whose CRs read tokens
from a path outside `/var/run/secrets/` or `/etc/k8s-bridge/` will see those
CRs refused with `Ready=False` / `InvalidSpec` after upgrading. This is
intentional and visible (condition + event + log), not silent.

**Rollback path.** Per-install, no rebuild required:

```bash
# widen to an extra mount point
helm upgrade k8s-bridge deploy/chart/k8s-bridge --reuse-values \
  --set 'allowedTokenPaths={/var/run/secrets/,/etc/k8s-bridge/,/srv/slurm/}'

# or disable the check entirely (restores pre-ADR-0017 behavior; the
# controller then logs a warning in supervisor mode)
helm upgrade k8s-bridge deploy/chart/k8s-bridge --reuse-values \
  --set 'allowedTokenPaths=null'

# re-enable the TLS escape hatch for a dev cluster
helm upgrade k8s-bridge deploy/chart/k8s-bridge --reuse-values \
  --set allowInsecureTLS=true
```

**Follow-up.** Revisit the Secret-reference alternative above before
ADR-0015 Phase B ships cluster-wide watching; at that point the path
allowlist becomes the weaker of the two controls, because a cluster-wide
supervisor adopts CRs from namespaces whose authors the platform admin may
not know at all.
