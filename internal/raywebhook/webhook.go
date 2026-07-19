// Package raywebhook is the admission-webhook analog of the Slurm side's
// JobSubmit lua plugin: it holds inner RayJobs the moment they are created so
// they cannot run on the shared cluster before ray-bridge admits them through
// Kueue, and it rejects shapes the bridge cannot support — surfacing the error
// at submit time instead of silently ignoring the job later (ADR-0006/0012).
//
// The decision logic (Decide) is a pure function so it is fully unit-testable
// without the admission machinery; Handler is the thin controller-runtime
// wrapper around it. Wiring the webhook into the manager needs a serving
// certificate (cert-manager or manual), so it is deployed separately from the
// core controller — see deploy/ray-bridge/webhook.yaml.
//
// IMPORTANT: this webhook is the ONLY enforcement point of the pin-gate
// invariant. ray-bridge runs with it disabled by default (--enable-webhook,
// cmd/ray-bridge); in that mode an inner RayJob that does not self-declare
// entrypointResources bypasses Kueue admission entirely while the bridge
// still builds dedicated workers for it. See the SECURITY CAVEAT in the
// cmd/ray-bridge package doc.
package raywebhook

import (
	"github.com/mrozacki/k8s-bridge/internal/ray"
	"github.com/mrozacki/k8s-bridge/internal/rayconfig"
	"github.com/mrozacki/k8s-bridge/internal/raytranslate"
)

// Decision is the outcome of evaluating a RayJob at admission time.
type Decision struct {
	// InjectPin is true when the webhook should add the pin resource to
	// spec.entrypointResources (PinName). This is the admission gate in the
	// pin-gate model (ADR-0013): the RayJob's driver cannot be scheduled until a
	// Kueue-admitted dedicated worker advertises the pin resource. It replaces
	// the earlier spec.suspend approach, which KubeRay FORBIDS in clusterSelector
	// mode (live finding 2026-07-07).
	InjectPin bool
	PinName   string
	// Deny is true when the RayJob must be rejected; Reason explains why.
	Deny   bool
	Reason string
}

// Decide evaluates a RayJob (already parsed from the admission request) against
// the ray-bridge config. It never mutates its inputs.
//
// Rules, in order:
//   - Not an inner workload (no clusterSelector) → allow untouched: it is a
//     standalone RayJob handled by Kueue's native integration, out of scope.
//   - Inner workload targeting an UNmanaged cluster → allow untouched: not our
//     cluster; another operator (or none) owns it.
//   - Inner workload targeting a MANAGED cluster:
//   - pool annotation unmapped → DENY: the submitter must pick a mapped pool,
//     otherwise the job would be silently ignored by the reconciler.
//   - pool mapped → INJECT the pin into entrypointResources, so the job waits
//     for its Kueue-admitted dedicated workers before it can run.
func Decide(job *ray.RayJob, cfg *rayconfig.Config) Decision {
	if !job.IsInnerWorkload() {
		return Decision{}
	}
	if _, managed := cfg.HeadAddressFor(job.TargetCluster); !managed {
		return Decision{}
	}
	if _, mapped := cfg.PriorityClassFor(job.Pool); !mapped {
		return Decision{Deny: true, Reason: denyReason(job)}
	}
	return Decision{InjectPin: true, PinName: raytranslate.PinResource(job)}
}

// PinUnits is how many units of the pin resource the entrypoint requests. One
// unit steers the driver onto a dedicated worker; comprehensive per-task
// pinning (all tasks/actors, not just the entrypoint) is an open item.
const PinUnits = 1

func denyReason(job *ray.RayJob) string {
	if job.Pool == "" {
		return "inner RayJob targeting managed cluster " + job.TargetCluster +
			" must set the " + ray.PoolAnnotation + " annotation to a mapped pool"
	}
	return "inner RayJob pool " + job.Pool + " (annotation " + ray.PoolAnnotation +
		") is not mapped to a WorkloadPriorityClass by ray-bridge"
}

// ParseDefaults builds the worker-shape defaults the RayJob parser needs from
// the config (the webhook does not size workers, but FromUnstructured needs
// concrete defaults to validate the shape annotations, so a malformed one is
// rejected at submit time too).
func ParseDefaults(cfg *rayconfig.Config) ray.Defaults {
	return ray.Defaults{
		Workers:        cfg.Worker.DefaultWorkers,
		CPUsPerWorker:  cfg.Worker.DefaultCPUs,
		MemPerWorkerMB: cfg.Worker.DefaultMemoryMB,
	}
}
