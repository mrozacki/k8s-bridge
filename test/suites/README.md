# Test-plan suite drivers

Driver scripts for the test suites summarized in the consolidated validation
summary ([docs/VALIDATION.md](../../docs/VALIDATION.md)).
Each `run-suite-<X>.sh` executes one suite (or one test case, for the
numbered variants like `run-suite-d1.sh`) against a live cluster that
already has the bridge, Slurm, Kueue and JobSet installed — see the
validation summary for the per-suite environment and teardown requirements.

These previously lived in the repository root; they were moved here to keep
the root free of campaign-specific tooling. Conventions:

- Scripts assume `kubectl` context points at the test cluster and must be
  run from the repository root (some call into `experiments/*/scripts/`).
- Evidence logs default to `$HOME/k8s-bridge-testrun/` (override via the
  environment variables documented in each script header).
- Nothing here is part of the shipped product; CI does not run these.
