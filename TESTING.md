# Testing strategy — the standard controller pyramid

This project follows the canonical test layering used by Kubernetes
controller projects (kubebuilder / CNCF convention): the lower the layer,
the more tests, the faster they run, the more often they execute.

```
        E2E          runbook on a live GKE cluster (per session, manual)
      ▲ integration  real kube-apiserver via envtest (seconds, CI)
    ▲ ▲ unit         fakes at every boundary (milliseconds, every save)
```

## Layer 1 — unit (`make test`)

- **What:** every package; table-driven tests; all boundaries replaced by
  test doubles: `SlurmAPI` is an in-memory fake (`fakeSlurm`), the
  Kubernetes API is controller-runtime's **fake client**, slurmrestd is an
  `httptest` server.
- **What they defend:** the reconcile decisions (create→pin→release order
  per ADR-0005, idempotency, cleanup triggers incl. "Slurm forgot the
  job"), the translation contract (CPU/mem/GRES/probe/labels), config
  parsing, and the REST client's **warnings-are-errors** rule — every rule
  here exists because a live run broke without it.
- Run with `-race` always; it is free at this scale.

## Layer 2 — integration (`make test-integration`)

- **What:** envtest starts a REAL `kube-apiserver` + etcd (no kubelet, no
  cloud) with the REAL JobSet CRD (vendored in `test/crd/`, conversion
  webhook stubbed to `None`). The full `tick()` lifecycle runs against it.
- **What it adds over layer 1:** schema validation, defaulting, field
  pruning, server-side idempotency — the class of bugs a fake client can
  never catch (e.g. a JobSet field that marshals fine but is rejected by
  the CRD schema).
- Binaries are fetched automatically by `setup-envtest` on first run;
  gated behind the `integration` build tag so `go test ./...` stays fast.

## Layer 3 — E2E (manual, per session)

- **What:** `experiments/DEMO.md` on a live GKE cluster — real Slurm, real
  Kueue, real autoscaling, real preemption.
- **Why not automated (yet):** ADR-0003 made GKE the only environment
  (kind cannot exercise autoscaling/DWS/spot); a paid cluster per CI run
  is not justified at prototype stage. The runbook doubles as the E2E
  script; converting it to an asserted script is a tracked follow-up.
- **Honest gaps at this layer:** physical-GPU path (blocked on
  `GPUS_ALL_REGIONS` quota), slurmd parsing of `Gres=` on a real GPU node,
  Slurm's post-MinJobAge job purge path.

## CI (`.github/workflows/ci.yaml`)

Parallel jobs on every PR and push to main touching `prototype/**`: `unit`
(gofmt gate, vet, thin-surface guard, benchmark smoke, build, `-race` tests +
coverage — the thin-surface guard and benchmark smoke joined this job in
a later iteration), `lint` (golangci-lint), `vuln` (govulncheck), `integration`
(envtest). Green CI is the merge gate — review discussions happen on PRs, CI
answers "does it still work".

## Conventions for contributors

1. A bug found live becomes a regression test at the LOWEST layer able to
   reproduce it (see `TestWaitsForNodeRegistration`,
   `TestWarningsAreHardErrors`, `TestReleaseJobSendsBareJobDescMsg` — all
   born from live incidents).
2. New boundary = new interface + fake. No test may talk to a real
   network service at layers 1-2.
3. Validate *placement and payloads*, not just "no error" (the --conf
   word-splitting bug survived a "job completed" check).
