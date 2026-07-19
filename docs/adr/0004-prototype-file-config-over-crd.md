# ADR-0004: Prototype uses file-based config instead of the WorkloadMixing CRD

- **Status:** Accepted (prototype scope only)
- **Date:** 2026-07-03
- **Update:** CRD promotion landed 2026-07-05; file mode
  retained for local runs.
- **Update 2026-07-06 (ADR-0011):** the WorkloadMixing CR is now a watched,
  reconciled object — the controller-runtime Manager watches it and
  hot-reloads the bridge's live config on every spec change, no restart
  required. File mode is unaffected (no CR to watch, so it stays a one-shot
  load at startup).

## Context

The MVD specifies a `WorkloadMixing` CustomResource as the configuration
surface for k8s-bridge, including status conditions reporting system health.
Building a CRD properly means schema generation, webhooks or CEL validation,
RBAC, and a reconciler for the resource itself — significant scaffolding
before the core hypothesis (Slurm→JobSet translation) is validated.

## Decision

The phase-3 prototype reads a YAML file whose schema deliberately mirrors the
future CR spec (same field names and structure). CRD promotion is deferred
until the mechanism is validated end to end.

## Alternatives considered

- **kubebuilder scaffolding with the CRD from day one** — rejected for the
  prototype: most of the generated surface would be dead weight while the
  translation logic is still hypothetical.
- **Command-line flags** — rejected: the config is structured (partition
  mappings, pod spec details); flags would diverge from the CR schema instead
  of converging on it.

## Consequences

- Faster iteration; the prototype stays a few hundred lines.
- No status reporting surface yet — health is visible only in logs; the CR's
  `status.conditions` model remains a production requirement.
- Migration path is mechanical: the config struct becomes the CRD spec.
