# Security Policy

k8s-bridge is early-stage software. We take security seriously and appreciate
responsible disclosure.

## Supported versions

The project is pre-1.0 and experimental. Only the `main` branch and the latest
tagged release receive security fixes. Do not run it on production or
multi-tenant clusters without an independent security review.

## Reporting a vulnerability

**Please do not open a public issue for security problems.**

Report privately through GitHub's ["Report a vulnerability"][gh-advisory]
(Security → Advisories) on this repository. This routes the report privately to
the maintainers; please use it rather than email so disclosure stays coordinated.

Please include:

- affected component (controller, Helm chart, CRD, experiment manifests);
- version / commit;
- a description and, if possible, a minimal reproduction;
- impact assessment (what an attacker gains).

We aim to acknowledge a report within **3 business days** and to provide a
remediation timeline within **10 business days**. We will coordinate a
disclosure date with you and credit you unless you prefer to remain anonymous.

## Scope and known trust boundaries

k8s-bridge translates Slurm jobs into Kubernetes JobSets admitted through Kueue.
Its security posture depends on several trust boundaries documented in the
[threat model](docs/reference/threat-model.md). In particular:

- **Slurmd runs privileged.** This is an upstream Slurm/slinky requirement
  (writable cgroups). The controller gates which image may run privileged via a
  deploy-time allowlist (`--allowed-slurmd-images`).
- **`WorkloadMixing` is a platform-admin resource.** It selects the privileged
  image, mounted storage and priority mapping. It must not be exposed to
  tenants — see the RBAC guidance in the chart.
- **The Slurm REST token is bearer-equivalent.** Use TLS (`https`) for the
  slurmrestd endpoint; a plaintext endpoint requires an explicit opt-in.

Reports that amount to "the controller can create privileged pods" are expected
behavior within the documented trust model, but reports of *bypassing* the
allowlist, the priority cap, or the CR tenancy boundary are in scope.

[gh-advisory]: https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability
