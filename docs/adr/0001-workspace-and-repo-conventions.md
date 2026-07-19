# ADR-0001: Workspace and repository conventions

- **Status:** Accepted
- **Date:** 2026-07-03

## Context

k8s-bridge is an experimental prototype: a working controller together with its
design records and reproducible experiments. The repository needs to stay easy
to navigate and to produce artifacts — documentation, experiment results, and
prototype code — that an engineering team can pick up and build a production
version from.

## Decision

1. **Docs-first structure**: `docs/` holds the architecture overview
   (`architecture.md`), an Architecture Decision Record log (`docs/adr/`), a
   security threat model (`docs/reference/threat-model.md`), a consolidated
   validation summary (`docs/VALIDATION.md`), and day-2 operational guides.
   `experiments/` holds numbered, self-contained experiments, each with its
   own README describing goal, setup, and result.
2. **Apache-2.0 with CNCF-style project hygiene from the start**: `LICENSE`,
   `SECURITY.md`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `MAINTAINERS.md`,
   `CODEOWNERS`, and DCO sign-off are all in place so the project is
   contributor-ready on day one.
3. **Trunk-based, PR-friendly workflow**: work happens on short-lived branches
   merged via pull requests, keeping `main` releasable and every change
   reviewable.

## Alternatives considered

- **Production-grade Go scaffolding before any experiments** — rejected as
  premature: empty scaffolding adds noise before the first experiments have
  validated the architecture.
- **Folding the experiments into the controller source tree** — rejected: the
  numbered, self-contained experiments double as reproducible validation and
  are easier to follow kept separate from the controller code.

## Consequences

- Engineering-handoff material accumulates naturally under `docs/`.
- Every experiment leaves a reproducible trace.
- The controller lives at the repository root in a standard Go /
  controller-runtime (kubebuilder) layout: `cmd/`, `internal/`, `api/`,
  `config/`.
