# Contributing to k8s-bridge

Thanks for your interest in k8s-bridge — a Kueue-centric bridge that admits
Slurm jobs as Kubernetes JobSets. This is an experimental, pre-1.0 project; the
authoritative design lives in [`docs/reference/`](docs/reference/).

## Ground rules

- **Be respectful.** This project follows the
  [Code of Conduct](CODE_OF_CONDUCT.md).
- **Everything in the repo is English** — code, comments, commit messages,
  docs, manifests.
- **Sign your commits (DCO).** See
  [Developer Certificate of Origin](#developer-certificate-of-origin-dco)
  below.

## Developer Certificate of Origin (DCO)

Every commit in a pull request must carry a `Signed-off-by` trailer certifying
the [Developer Certificate of Origin](https://developercertificate.org/) — a
lightweight attestation that you wrote the change (or otherwise have the right
to submit it under the project's license), used instead of a separate CLA.

Add the trailer automatically:

```sh
git commit -s -m "your commit message"
```

which appends a line like:

```
Signed-off-by: Your Name <you@example.com>
```

using the name/email from your `git config user.name` / `user.email`. If you
forgot `-s` on a commit you already made:

```sh
git commit --amend -s          # most recent commit
git rebase --signoff main      # every commit on the branch, rebased onto main
```

then force-push the branch. A `.github/workflows/dco.yaml` CI check
(backlog SEC3, "DCO enforcement") runs on every pull request and fails if any
commit in the PR range is missing the trailer.

## Development setup

The controller lives at the repository root (Go, controller-runtime
layout). From the root:

```sh
make fmt vet test      # format, vet, unit tests (race)
make test-integration  # envtest suite (real kube-apiserver + JobSet CRD)
make lint              # golangci-lint (includes gosec); CI always runs it
```

Before opening a pull request, ensure:

- `gofmt` is clean and `go vet ./...` passes;
- unit and integration tests pass;
- `golangci-lint` (including `gosec`) is clean;
- `govulncheck ./...` reports no known-vulnerable dependencies;
- new behavior has tests, and security-relevant changes update the
  [threat model](docs/reference/threat-model.md).

## Design decisions

Significant decisions are recorded as ADRs in [`docs/adr/`](docs/adr/). If your
change alters an architectural choice, add or update an ADR in the same PR.

## Security

Do **not** report vulnerabilities in public issues or pull requests. Follow
[SECURITY.md](SECURITY.md).

## Pull request process

1. Fork and branch from `main` (never commit directly to `main`).
2. Keep PRs focused; describe the change and its rationale.
3. Link the issue or ADR the change addresses.
4. A maintainer (see [MAINTAINERS.md](MAINTAINERS.md)) will review; at least one
   maintainer approval is required to merge.
