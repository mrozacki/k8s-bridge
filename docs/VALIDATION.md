# Validation summary

k8s-bridge has gone through several rounds of testing, spanning unit and
controller-integration tests, functional and chaos validation on live
Kubernetes clusters, large-scale load testing, GPU and dynamic-provisioning
scenarios, and a security review ahead of publication. This document
describes, by class, what was exercised and what was learned. It is a
narrative summary rather than a raw test log: detailed run logs, machine
telemetry, and environment-specific artifacts are intentionally not
published here — the goal is to give readers (and prospective adopters or
contributors) an honest picture of validation coverage and known limits,
not a step-by-step reproduction trace.

## Unit and controller-integration tests (envtest)

The controller's core packages (Slurm client, translation logic, config
loading, the reconciler) carry unit tests with high coverage on the
reconciler package after a dedicated remediation pass, plus dedicated
coverage for the newest features (priority synchronization, CR-based
configuration, the `Run()` loop's error-tolerance guarantee). Tests use
table-driven, payload-exact assertions against captured Slurm REST
responses, run under `-race`, and are flake-free.

Above the unit layer, `envtest` starts a real Kubernetes API server and
runs the reconciler against the actual `JobSet` and Kueue `Workload` CRDs
(and, for the Ray controller, the actual KubeRay `RayJob` CRD). This
validates field paths and schema compatibility that a fake client cannot —
for example, confirming that `spec.suspend`, `spec.clusterSelector`, and
`spec.entrypointResources` are the correct, schema-valid paths on the
published RayJob type. A parser-and-config integration test pass also
walked through ten concrete defects surfaced by a large-scale run
(Slurm job-count ceilings, script portability bugs, teardown ordering,
and API burst behavior — see the Scale section below); each is recorded
with symptom, root cause, and fix, and re-verified with a dedicated
fix-verification test suite afterward.

A recurring, explicitly documented lesson from this project: **unit and
envtest coverage cannot catch deploy-time defects.** Neither harness runs
the actual Helm chart, applies its pod security context, or evaluates its
RBAC — so an entire class of bugs (leader-election namespace mismatches,
file-permission issues from non-root containers, cache-scope-vs-RBAC
mismatches) only ever surfaced on a real cluster. That finding motivated
adding a chart-deploy smoke test to the CI pipeline (installing the actual
chart into a disposable cluster and asserting the controller reaches
`Ready` and completes a tick) so this class of defect is caught
automatically going forward rather than rediscovered live each time.

## Live validation on a managed Kubernetes cluster

The core end-to-end path — a Slurm job submitted normally, auto-held by a
submit-time plugin, translated into a Kubernetes `JobSet`, admitted by
Kueue, registered as a dynamic Slurm node, released, run to completion,
and cleaned up — has been validated repeatedly on managed Kubernetes
clusters (never on a purely local/simulated environment), across several
independent sessions as the controller evolved. Functional and edge-case
coverage on live clusters included: CRD admission validation
(rejecting malformed configuration), multi-partition fan-out (routing many
Slurm partitions through one shared queue), feature-based node isolation
(no cross-job leakage between concurrently running jobs), sidecar/volume
injection, and `--exclusive` whole-node requests. Chaos and resilience
scenarios included killing the controller mid-cycle and confirming it
reconciles cleanly on restart with no duplicate or lost objects, forcing a
dead JobSet and confirming the corresponding Slurm job fails with a clear
reason rather than hanging pending forever, killing a running worker pod
and confirming Slurm correctly marks the node down and requeues the job,
and hot-reloading configuration without disturbing running jobs.

The first-ever in-cluster Helm chart deployment (as opposed to running the
controller binary directly) surfaced a chain of eight deploy-time defects
in one pass — spanning leader-election namespace scoping, a token secret
unreadable by the non-root container, missing controller-runtime log
wiring, a config-reload gap, and an informer cache/RBAC scope mismatch
that silently deadlocked the first reconcile tick. All eight were fixed in
the same pass, each backed by either a regression test or a subsequent
successful live redeploy. This result is the direct justification for
treating "deploys cleanly via the published chart, with default values, on
a real cluster" as a first-class, separate test class rather than treating
binary-level correctness as sufficient.

Two further defects found only through live reproduction, not design
review, were: an update payload contract mismatch against a specific Slurm
REST API version that silently broke every job status update (traced by
direct reproduction against the live API rather than by guessing from a
truncated client error); and a scheduler node-selection plugin requirement
for dynamic node registration that was never set at the Slurm configuration
layer, so worker pods started but never joined as usable nodes. Both are
now fixed and covered by regression tests and/or environment defaults.

## Scale tests (S1–S5 series)

A dedicated large-scale test series (backlog depth, pod churn, sustained
throughput, multi-partition routing, and quota-pressure fallback) validated
the controller's behavior well beyond functional-test volumes:

- **Backlog depth**: a backlog of several thousand held Slurm jobs was
  injected and translated into thousands of `JobSet` objects with zero API
  errors. The controller's steady-state memory stayed low, but the
  *unpaginated* nature of the Slurm REST job-listing endpoint caused a
  working-set memory spike during JSON decoding at this volume, which is
  why the controller's recommended memory limit was raised and,
  subsequently, the listing path was refactored to stream-decode the
  response rather than buffer it whole — closing the underlying memory-scaling
  issue rather than only working around it with a larger limit.
- **Mixed queues/priorities and churn**: a mixed backlog of Slurm and
  native Kubernetes-batch jobs across multiple priorities and queues was
  run concurrently, alongside a separate, sustained pod churn workload
  (thousands of create/delete cycles). The informer cache and watch-driven
  reconciliation loop showed no deadlocks, no memory leaks, and no
  controller restarts under either kind of pressure, even as the
  underlying cluster autoscaled up by an order of magnitude to absorb the
  churn.
- **Sustained throughput**: a steady stream of job submissions was
  maintained over multiple cycles to characterize reconcile-batch latency
  and admission-to-run latency under continuous (not just bursty) load.
- **Multi-partition fan-out**: a single configuration object mapping ten
  independent Slurm partitions onto one shared Kueue queue was validated
  for correct routing and for hot-reload behavior (no controller restart
  required on a mapping change).
- **Quota-pressure fallback**: simulated capacity starvation on one
  compute class successfully triggered fallback provisioning onto a
  different, more available compute class with no user intervention,
  and cohort-based quota borrowing/reclaiming between two simulated teams
  was confirmed to compose correctly with priority-based preemption.

A second, deeper pass at even larger backlog sizes stress-tested the
supporting components rather than the controller alone: the Slurm REST
API's own memory footprint and job-count ceiling, Kubernetes admission
webhook responsiveness under a burst of object creation, and the
practical limits of client-side bulk delete during teardown were all
identified as separate scaling boundaries from the controller's own
behavior, each now documented with either a fix or a recommended
mitigation (see ADR references below).

## GPU churn and simulated-accelerator tests

Because provisioning real accelerator hardware for every test run is not
practical, most GPU-shaped validation uses a **simulated-accelerator**
mechanism (ADR-0010): a lightweight in-cluster component patches a fake
extended resource onto node status, which the Kubernetes scheduler and
Kueue's quota accounting treat identically to a genuine device, while the
Slurm side is given a matching count-only GRES declaration. This let the
project validate the full GPU-shaped admission and scheduling chain —
including a combined scale-and-churn scenario targeting several hundred
concurrently running GPU-shaped jobs with a much larger pending backlog,
and independently sustained pod churn above the target rate — without
consuming any real accelerator quota. In practice, the concurrently
running-job target during one such run was capped by an account-level CPU
quota rather than by the controller, which is recorded as an environment
constraint, not a controller defect; the translation and admission path
itself handled the full submitted backlog with zero reconcile errors.

A small number of defects were unique to the GPU-simulation mechanism
itself (not the controller): a one-shot resource patch gets reverted by
the kubelet's periodic status refresh (fixed by moving to a continuously
reconciling component instead), and a naive mount of the simulated GPU
configuration collided with the path a config-less Slurm worker needs
write access to for its own registration — a lesson recorded as "a fix can
be necessary but not sufficient," since removing an earlier blocker
revealed this next, independent one.

## Dynamic Workload Scheduler (DWS) / capacity provisioning

The full admission chain for queued, on-demand capacity provisioning
(Kueue `AdmissionCheck` → cloud provisioning request → node pool scale-up)
was validated end to end against real infrastructure using a small GPU
node pool, and separately explored with CPU-only pools. The consistent,
repeated finding is that queued-provisioning capacity requests without an
attached accelerator are rejected by policy at the cloud-provider level —
this is a platform constraint, not a bug in the bridge or in Kueue, and is
documented as such rather than worked around. With a real accelerator
attached, the provisioning request chain was confirmed to reach an
accepted/queued state, demonstrating that the Kueue-to-provisioning-API
integration itself works; full completion through to a running,
GPU-executing container was validated in a later, larger run. A related,
declarative alternative — custom compute-class fallback (e.g., preferring
lower-cost capacity with automatic fallback to a different machine family
when unavailable) — was validated independently and works cleanly for
CPU-only scenarios where DWS's accelerator requirement is a blocker.

## Ray ecosystem — pin-gate admission, preemption/requeue

Kueue admits a shared, long-lived Ray cluster as a single unit; the
challenge this project addresses is bringing the *individual workloads
running inside* that shared cluster under the same per-workload admission,
priority, and preemption model the Slurm side already gets — otherwise an
admitted-but-mostly-idle cluster hoards quota and every job inside it is
invisible to fair-share scheduling. A second, purpose-built controller
handles this.

The initial design assumed the inner workload could be submitted
suspended and unsuspended once its dedicated capacity was admitted,
mirroring the Slurm approach. Live validation against a real Ray operator
falsified that assumption directly: the operator forbids suspending a job
targeting a shared cluster by that mechanism. This was pivoted, based on
direct evidence, to a **pin-gate model**: the inner job is *not* suspended;
instead it carries a resource requirement that only a Kueue-admitted,
dedicated worker can satisfy, so the job's driver simply cannot schedule
until that worker joins and advertises the resource. This mechanism —
worker join, resource advertisement, readiness signaling, and the gate
itself — was validated live end to end, including full run-to-completion,
on a genuine multi-node cluster.

Preemption was validated in the same environment: a higher-priority inner
Ray workload preempts a lower-priority one competing for the same quota,
and — importantly — Kueue was confirmed to preempt correctly **across
workload types** in a single shared queue (a plain Kubernetes batch job,
a Ray inner workload, and a Slurm-originated JobSet all compete on
priority from one admission point). The one genuine, now well-characterized
gap is around **requeue after preemption**: a plain Kubernetes-native job
requeues gracefully when capacity frees up, matching the Slurm behavior,
but a Ray inner workload targeting a shared cluster by the mechanism this
project uses currently has no automatic-retry path available to it at the
operator level, so a preempted inner workload ends in a failed state
rather than a paused one. This is documented as a known limitation with
several candidate solutions (bridge-driven resubmission, an alternative
scheduling-gate mechanism, or moving the pin to per-task rather than
per-driver granularity), rather than silently worked around.

Topology-aware placement (co-locating a shared cluster's dedicated workers
within one simulated network domain) was also validated on this mechanism,
confirming it composes with the same topology feature validated on the
Slurm side.

## High-availability / leader-election failover

The controller adopted a standard Kubernetes leader-election mechanism so
that only one replica is ever active at a time, with informer-cache-driven
reconciliation and an explicit watch-triggered "nudge" so the control loop
reacts to relevant cluster events immediately rather than only on a fixed
poll interval. This was validated on a live cluster: the leader-election
lease is a real, observable object; killing the active controller pod
mid-cycle results in a clean restart with no duplicate or lost work
(verified directly by submitting several jobs, killing the controller
pod as soon as the first result of its work appeared, and confirming
exactly one output object existed per input afterward, with no errors in
the reconcile log). A secondary, minor finding was that a rolling upgrade
can, in specific timing conditions, leave the outgoing pod holding its
lease long enough to delay the incoming pod's promotion — recorded as a
known, low-severity operational quirk with candidate mitigations (shorter
lease/renew intervals, or an explicit release-on-shutdown hook), not a
correctness defect.

## Multi-tenant / namespaced RBAC

Two related but distinct multi-tenancy concerns were validated. First,
**quota-sharing behavior**: two simulated teams sharing a quota pool
(cohort) were confirmed to correctly borrow idle capacity from one another
and have it reclaimed, at whole-workload granularity, the moment the
owning team's own priority demand exceeds its nominal share — and this was
confirmed to compose correctly with topology-aware placement constraints
active on the same queues. Second, **RBAC and namespace scoping** for the
controller's own permissions: the chart was hardened to grant only
namespace-scoped roles by default (with cluster-scope as an explicit
opt-in), and a live deployment confirmed that the controller's informer
cache must be scoped to match exactly what its RBAC grants — a mismatch
between a cluster-scoped cache and namespace-scoped permissions was one of
the deploy-time defects found and fixed (see the live-validation section
above). The `WorkloadMixing` configuration object itself was explicitly
documented as a platform-administrator-level resource (it can influence
which container image runs privileged, which storage gets mounted, and
which priority classes are available), and is deliberately kept out of the
default tenant-facing Kubernetes roles.

## Security validation

A dedicated security review was carried out ahead of any public-facing
release, covering the controller code, the Helm chart and CRD, deployment
manifests, CI configuration, and repository hygiene. Findings spanned
severities from critical to low; the most significant were: worker pods
running in a fully privileged security context by default (now made
opt-in, with the required capability set narrowed and the container image
constrained to an operator-controlled allowlist rather than an arbitrary
user-supplied value); an unauthenticated priority-escalation path where a
Slurm job owner could request an unbounded priority value that flowed
through to the Kubernetes admission layer (now capped, with enforcement on
both the submission-time hook and the controller itself); and the
controller's own pod running with no security context, resource limits, or
health probes (now hardened to a restricted profile with liveness/readiness
probes). Additional medium/low findings covered CI supply-chain posture,
unchecked integer conversions from untrusted external input, an
always-on debug/profiling endpoint (now off by default, on a dedicated
server with timeouts), loose CRD field validation, and a missing default
network policy.

All identified code- and configuration-level findings were remediated in
the same pass and verified with the project's standard build, unit, and
integration test gates, plus static analysis (vulnerability scanning and a
security-focused linter) integrated into continuous integration going
forward. A small number of items are explicitly deferred as
pre-publication process steps rather than code changes (for example,
supply-chain attestation and governance-document steps that only make
sense once the repository is actually public), and are tracked separately
from the completed remediation.

## Documentation as an executable artifact

The tutorial and the demo runbook are not prose about the system — they are
sequences of commands a reader is expected to run, which makes them code that
can rot like any other. A 2026-07-26 audit read both documents from the
executor's point of view rather than the reader's and found nine defects that
would have stopped a live run: command blocks whose relative paths resolved
outside the repository once an earlier section had changed directory; a
configuration patch the CRD's own validation rule would reject, because it
omitted the explicit opt-in required for a plaintext endpoint; a simulated-GPU
section that predated a live-validated rewrite of the same material elsewhere;
a profiling step that could never produce a profile, because the flag enabling
it is off by default and was never mentioned; and blocks that silently
interleaved commands meant for two different shells (the reader's machine and
the Slurm login pod).

The instructive part was the pattern. The one section that was correct was the
one that had actually been executed during an earlier live session; sections
that had only been reviewed still carried their defects. Review does not find
this class of problem.

All nine are fixed, and a check now runs in continuous integration that parses
every command block in the runnable documents and enforces that each block is
repo-root-relative, that every path it references exists, and that every script
it invokes is executable. It was verified in both directions — passing on the
corrected documents, and correctly naming file and line when the original
defects were reintroduced deliberately.

The corrected runbook was then walked end to end on a live cluster, and that
pass found three further defects which no static check could have caught,
because each one concerns what the reader *sees* rather than whether a command
runs. The mixing section — the one the runbook itself calls the point of the
project — shipped a workload manifest targeting a different queue from the one
the bridge routes Slurm jobs to, so the two workloads landed in separate quota
pools and never contended, while the accompanying narration described them
sharing one. The simulated-GPU section asked the reader to observe a property
of a node that, with the job as written, is deregistered about seven seconds
after it appears. And the failure-handling section proposed two ways to kill a
JobSet on demand, both of which the apiserver rejects because the relevant
field is immutable. All three are corrected; the lesson recorded alongside them
is that static verification covers the commands, not the claims made about
their output.

## Traceability

Design rationale for the mechanisms referenced above is recorded in the
project's Architecture Decision Records, notably: ADR-0005 (release only
after dynamic node registration), ADR-0006 (Ray inner-workload admission
gap), ADR-0008 (topology translation), ADR-0009 (priority mutation
channel), ADR-0010 (simulated accelerators), ADR-0011 (controller-runtime
manager and watch-driven reconciliation), ADR-0012 (Ray bridge topology and
shared library), ADR-0013 (Ray pin-gate admission), ADR-0015 (multi-CR
support), and ADR-0016 (proactive drain on Kueue preemption). Detailed run
logs, telemetry captures, and environment-specific
reproduction artifacts underlying this summary are kept out of the public
record by design.
