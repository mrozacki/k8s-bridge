// Package ray is a thin accessor over KubeRay RayJob custom resources,
// read as unstructured objects. This mirrors how internal/bridge treats Kueue
// Workloads: the bridge needs only a handful of fields, so it does NOT import
// the full KubeRay Go module (thin-surface principle, ADR-0012) — it reads the
// RayJob's spec/status directly out of the unstructured map.
//
// A RayJob is an "inner workload" of a shared, long-lived RayCluster when it
// targets that cluster via clusterSelector (the ray.io/cluster label) instead
// of carrying an inline rayClusterSpec. Those inner workloads are exactly the
// ones ADR-0006 brings under Kueue admission — the RayJob-with-own-cluster
// case is already handled by Kueue's native integration and is out of scope.
package ray

import (
	"fmt"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GVK / GVR constants for the RayJob CRD (KubeRay ray.io/v1). Kept here so the
// entire KubeRay API surface is confined to this package, the same way
// internal/bridge confines the Kueue Workload GVK to kueue.go.
var (
	// RayJobGVK is the singular kind, used for watch sources.
	RayJobGVK = schema.GroupVersionKind{Group: "ray.io", Version: "v1", Kind: "RayJob"}
	// RayJobListGVK is the list kind, used for cache/API LISTs.
	RayJobListGVK = schema.GroupVersionKind{Group: "ray.io", Version: "v1", Kind: "RayJobList"}
)

const (
	// clusterSelectorKey is the label key KubeRay uses inside a RayJob's
	// clusterSelector to name the existing RayCluster the job attaches to.
	clusterSelectorKey = "ray.io/cluster"

	// Annotation contract (the sbatch-flags analog): an inner RayJob declares
	// the dedicated worker capacity ray-bridge should stand up for it. This is
	// the explicit, deterministic capacity request — the same role
	// --ntasks / --cpus-per-task / --gres play for a Slurm job. Absent
	// annotations fall back to the config defaults (see rayconfig.Worker).
	PoolAnnotation         = "ray-bridge.x-k8s.io/pool"
	WorkersAnnotation      = "ray-bridge.x-k8s.io/workers"
	WorkerCPUsAnnotation   = "ray-bridge.x-k8s.io/worker-cpus"
	WorkerGPUsAnnotation   = "ray-bridge.x-k8s.io/worker-gpus"
	WorkerMemoryAnnotation = "ray-bridge.x-k8s.io/worker-memory"
	LocalQueueAnnotation   = "ray-bridge.x-k8s.io/local-queue"

	// RequiredTopologyAnnotation names a node-label topology level (e.g.
	// "ray-bridge.x-k8s.io/rack") that the inner job's dedicated workers must
	// all share — translated to a Kueue TAS podset-required-topology on the
	// worker JobSet (R8, the analog of the Slurm side's --switches, ADR-0008).
	// Validated live on GKE (2026-07-07): a worker JobSet with this annotation
	// co-locates its pods in one rack.
	RequiredTopologyAnnotation = "ray-bridge.x-k8s.io/required-topology"
)

// RayJob is the subset of a KubeRay RayJob the bridge cares about. Worker-shape
// fields are resolved from annotations with config-supplied defaults already
// applied by the caller (translate), so this struct always carries concrete,
// usable numbers.
type RayJob struct {
	Name      string
	Namespace string
	UID       string

	// TargetCluster is the RayCluster named by clusterSelector[ray.io/cluster].
	// Empty means the RayJob is not attached to an existing cluster (own-cluster
	// mode) and is therefore out of scope for the bridge.
	TargetCluster string

	// Suspended reflects spec.suspend — the native hold analog.
	Suspended bool

	// Entrypoint is the Ray job command (spec.entrypoint), informational for
	// the bridge (the reconciler sets entrypointResources for pinning).
	Entrypoint string

	// Worker-shape request (annotations, defaulted). Workers is the pod count;
	// the per-worker fields size each pod so Kueue quota tracks real capacity.
	Workers        int32
	CPUsPerWorker  int64
	GPUsPerWorker  int64
	MemPerWorkerMB int64

	// Pool selects the WorkloadPriorityClass (PoolAnnotation), the partition
	// analog. Empty when the annotation is unset; the caller decides whether an
	// unset pool is managed.
	Pool string
	// LocalQueue optionally overrides the config's global LocalQueue
	// (LocalQueueAnnotation), the multi-team analog of PartitionMapping.LocalQueue.
	LocalQueue string
	// RequiredTopology is the node-label topology level the dedicated workers
	// must co-locate under (RequiredTopologyAnnotation). Empty = no hard
	// topology constraint (a config-level preferred level may still apply).
	RequiredTopology string

	// JobStatus / DeploymentStatus mirror status.jobStatus and
	// status.jobDeploymentStatus, used to detect terminal jobs for cleanup.
	JobStatus        string
	DeploymentStatus string
}

// Defaults carries the config-supplied fallbacks for worker-shape annotations
// that an inner RayJob leaves unset. Passed in so this package stays free of a
// dependency on rayconfig (translate wires the two together).
type Defaults struct {
	Workers        int32
	CPUsPerWorker  int64
	MemPerWorkerMB int64
}

// FromUnstructured parses the fields the bridge needs out of a RayJob object,
// applying def for any worker-shape annotation the RayJob omits. It fails only
// on a value that is present but malformed (e.g. a non-numeric worker count or
// an unparseable memory quantity) — a missing annotation is not an error, it
// takes the default.
func FromUnstructured(u *unstructured.Unstructured, def Defaults) (*RayJob, error) {
	if u == nil {
		return nil, fmt.Errorf("nil RayJob object")
	}
	j := &RayJob{
		Name:      u.GetName(),
		Namespace: u.GetNamespace(),
		UID:       string(u.GetUID()),
	}

	// spec.clusterSelector[ray.io/cluster]
	sel, _, _ := unstructured.NestedStringMap(u.Object, "spec", "clusterSelector")
	j.TargetCluster = sel[clusterSelectorKey]

	j.Suspended, _, _ = unstructured.NestedBool(u.Object, "spec", "suspend")
	j.Entrypoint, _, _ = unstructured.NestedString(u.Object, "spec", "entrypoint")
	j.JobStatus, _, _ = unstructured.NestedString(u.Object, "status", "jobStatus")
	j.DeploymentStatus, _, _ = unstructured.NestedString(u.Object, "status", "jobDeploymentStatus")

	ann := u.GetAnnotations()
	j.Pool = ann[PoolAnnotation]
	j.LocalQueue = ann[LocalQueueAnnotation]
	j.RequiredTopology = ann[RequiredTopologyAnnotation]

	// Worker shape: annotation overrides default; a malformed value is a hard
	// error so a typo surfaces instead of silently sizing capacity wrong.
	workers, err := intAnnotation(ann, WorkersAnnotation, int64(def.Workers))
	if err != nil {
		return nil, err
	}
	if workers < 1 {
		workers = 1
	}
	j.Workers = clampToInt32(workers)

	cpus, err := intAnnotation(ann, WorkerCPUsAnnotation, def.CPUsPerWorker)
	if err != nil {
		return nil, err
	}
	if cpus < 1 {
		cpus = 1
	}
	j.CPUsPerWorker = cpus

	gpus, err := intAnnotation(ann, WorkerGPUsAnnotation, 0)
	if err != nil {
		return nil, err
	}
	if gpus < 0 {
		gpus = 0
	}
	j.GPUsPerWorker = gpus

	j.MemPerWorkerMB = def.MemPerWorkerMB
	if raw, ok := ann[WorkerMemoryAnnotation]; ok {
		q, err := resource.ParseQuantity(raw)
		if err != nil {
			return nil, fmt.Errorf("annotation %s=%q: %w", WorkerMemoryAnnotation, raw, err)
		}
		mb := q.Value() / (1 << 20)
		if mb < 1 {
			mb = 1
		}
		j.MemPerWorkerMB = mb
	}

	return j, nil
}

// IsInnerWorkload reports whether this RayJob targets an existing shared
// cluster (the only case the bridge admits). A RayJob with its own inline
// cluster returns false.
func (j *RayJob) IsInnerWorkload() bool { return j.TargetCluster != "" }

// IsTerminal reports whether the RayJob has finished, so its dedicated workers
// can be drained and the worker JobSet deleted. Covers both the coarse
// jobDeploymentStatus lifecycle field and the fine-grained jobStatus.
func (j *RayJob) IsTerminal() bool {
	switch j.DeploymentStatus {
	case "Complete", "Failed", "ValidationFailed":
		// ValidationFailed = KubeRay rejected the RayJob (e.g. an unsupported
		// clusterSelector-mode spec). Treat it as terminal so the bridge cleans
		// up the dedicated workers it created rather than leaving them orphaned
		// (live finding, GKE 2026-07-07: a rejected RayJob left a running worker
		// JobSet behind).
		return true
	}
	switch j.JobStatus {
	case "SUCCEEDED", "FAILED", "STOPPED":
		return true
	}
	return false
}

// intAnnotation returns the int64 value of ann[key], or def when the key is
// absent. A present-but-non-integer value is an error. Uses strict base-10
// parsing over the WHOLE trimmed string: fmt.Sscan would silently accept a
// leading-integer-with-garbage ("3abc"->3, "3.9"->3) and C-style bases
// ("0x10"->16), mis-sizing capacity from a typo instead of surfacing it
// (review finding, 2026-07-06).
func intAnnotation(ann map[string]string, key string, def int64) (int64, error) {
	raw, ok := ann[key]
	if !ok {
		return def, nil
	}
	v, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("annotation %s=%q is not a base-10 integer: %w", key, raw, err)
	}
	return v, nil
}

func clampToInt32(v int64) int32 {
	const maxInt32 = 1<<31 - 1
	if v > maxInt32 {
		return maxInt32
	}
	return int32(v)
}
