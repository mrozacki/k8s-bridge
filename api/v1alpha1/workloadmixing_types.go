// WorkloadMixing typed API (ADR-0014). The spec mirrors the prototype's
// file config 1:1 — see internal/config/config.go for the field-by-field
// rationale; the json tags here match that struct exactly so the CR spec and
// the file config remain the same schema. PR 1 introduced the typed package
// and made the CRD YAML a build artifact; PR 2 switched
// internal/bridge/crdconfig.go to consume these types directly (FromCR) and
// upgraded status.conditions to metav1.Condition.
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PartitionMapping ties a Slurm partition to a Kueue WorkloadPriorityClass,
// the unified priority model both schedulers agree on. Mirrors
// config.PartitionMapping.

type PartitionMapping struct {
	// PartitionName is the Slurm partition under workload-mixing management.

	// +kubebuilder:validation:MaxLength=253
	PartitionName string `json:"partitionName"`

	// WorkloadPriorityClass is the Kueue WorkloadPriorityClass JobSets
	// translated from this partition are labeled with.

	// +kubebuilder:validation:MaxLength=253
	WorkloadPriorityClass string `json:"workloadPriorityClass"`

	// LocalQueue overrides the top-level spec.localQueue for JobSets
	// translated from this partition (backlog A1b: multi-team support).
	// Empty/omitted falls back to spec.localQueue.

	// +kubebuilder:validation:MaxLength=253
	LocalQueue string `json:"localQueue,omitempty"`
}

// Slurmd describes the pods that will register as dynamic Slurm nodes.
// Mirrors config.Slurmd.

type Slurmd struct {
	// Image is the slurmd container image.

	// +kubebuilder:validation:MaxLength=512
	Image string `json:"image"`

	// ConfServer is the slurmctld address slurmd registers against,
	// e.g. "slurm-controller.slurm.svc.cluster.local:6817".

	// +kubebuilder:validation:MaxLength=512
	ConfServer string `json:"confServer"`

	// AuthSecretName is the Secret (in the CR's namespace) holding slurm.key.

	// +kubebuilder:validation:MaxLength=253
	AuthSecretName string `json:"authSecretName"`

	// GPUResourceName is the Kubernetes extended resource requested when a
	// Slurm job asks for GRES GPUs. Defaults to "nvidia.com/gpu".

	// +kubebuilder:validation:MaxLength=253
	GPUResourceName string `json:"gpuResourceName,omitempty"`

	// run slurmd privileged (default true; slurmd needs writable cgroups)
	Privileged *bool `json:"privileged,omitempty"`

	// SharedStorage, when set, mounts a shared filesystem (e.g. /home) into
	// every slurmd pod so job scripts and data resolve like on a static
	// cluster (MVP requirement).

	SharedStorage *SharedStorage `json:"sharedStorage,omitempty"`
}

// SharedStorage describes an NFS export mounted into slurmd pods. Mirrors
// config.SharedStorage.

type SharedStorage struct {
	// NFSServer is the NFS server host serving the export.

	// +kubebuilder:validation:MaxLength=253
	NFSServer string `json:"nfsServer,omitempty"`

	// NFSPath is the exported path on the NFS server.

	// +kubebuilder:validation:MaxLength=1024
	// +kubebuilder:validation:Pattern=`^/`
	NFSPath string `json:"nfsPath,omitempty"`

	// MountPath is where the export is mounted inside slurmd pods.

	// +kubebuilder:validation:MaxLength=1024
	// +kubebuilder:validation:Pattern=`^/`
	MountPath string `json:"mountPath,omitempty"`
}

// Topology controls translation of Slurm topology requests into Kueue
// Topology-Aware Scheduling (TAS) annotations. Levels are node label keys
// registered in the cluster's Topology CR (e.g. "example.com/rack").
// Mirrors config.Topology. The 316 MaxLength is a Kubernetes label key's
// maximum length (253-char prefix + "/" + 63-char name).

type Topology struct {
	// RequiredLevel is applied as podset-required-topology when a Slurm job
	// requests switch locality (--switches=N). Empty disables translation.

	// +kubebuilder:validation:MaxLength=316
	RequiredLevel string `json:"requiredLevel,omitempty"`

	// PreferredLevel is applied as podset-preferred-topology to every other
	// bridge job (best-effort gang locality). Empty disables it.

	// +kubebuilder:validation:MaxLength=316
	PreferredLevel string `json:"preferredLevel,omitempty"`
}

// WorkloadMixingSpec is the CR-shaped twin of config.Config minus Namespace
// (the CR's own metadata.namespace is authoritative for where JobSets are
// created — see internal/bridge/crdconfig.go). The CEL rule below is the
// security audit M7/H3 guard: refuse a plaintext endpoint unless the operator
// explicitly opted in — the Slurm token is bearer-equivalent.

// +kubebuilder:validation:XValidation:rule="self.slurmRestURL.startsWith('https://') || (has(self.allowInsecureHTTP) && self.allowInsecureHTTP)",message="slurmRestURL must use https unless allowInsecureHTTP is set (the Slurm token would travel in cleartext)"
type WorkloadMixingSpec struct {
	// SlurmRestURL is the slurmrestd base URL, e.g.
	// "https://slurm-restapi.slurm:6820".

	// +kubebuilder:validation:MaxLength=2048
	// +kubebuilder:validation:Pattern=`^https?://`
	SlurmRestURL string `json:"slurmRestURL"`

	// permit a plaintext http:// slurmRestURL (development only)
	AllowInsecureHTTP bool `json:"allowInsecureHTTP,omitempty"`

	// SlurmCACertFile, when set, is the PEM CA bundle used to verify the
	// slurmrestd TLS certificate (mounted Secret/ConfigMap). Empty uses the
	// system roots.

	// +kubebuilder:validation:MaxLength=1024
	SlurmCACertFile string `json:"slurmCACertFile,omitempty"`

	// SlurmInsecureSkipTLSVerify disables slurmrestd certificate
	// verification. Development escape hatch only.

	SlurmInsecureSkipTLSVerify bool `json:"slurmInsecureSkipTLSVerify,omitempty"`

	// SlurmUser is sent as X-SLURM-USER-NAME alongside the token; it must be
	// a real Slurm user, and empty (the safe default) omits the header so
	// slurmrestd uses the user the JWT was minted for — see config.Config.
	//
	// NOTE: slurmTokenSecretRef (Secret-based token delivery) is deliberately
	// NOT a field — the code only reads slurmTokenFile from a mounted path
	// (internal/config/config.go). Tracked as backlog A2; do not add it here
	// until a loader consumes it.

	// +kubebuilder:validation:MaxLength=64
	SlurmUser string `json:"slurmUser,omitempty"`

	// path to a mounted JWT for slurmrestd
	// +kubebuilder:validation:MaxLength=1024
	SlurmTokenFile string `json:"slurmTokenFile,omitempty"`

	// LocalQueue is the Kueue queue JobSets are submitted to (253 = DNS
	// subdomain, the maximum for a Kubernetes object name).

	// +kubebuilder:validation:MaxLength=253
	LocalQueue string `json:"localQueue"`

	// Go duration, e.g. 10s; the controller also enforces a 1s minimum
	// +kubebuilder:validation:Pattern=`^[0-9]+(ns|us|µs|ms|s|m|h)$`
	PollInterval string `json:"pollInterval,omitempty"`

	// per-request slurmrestd HTTP timeout (Go duration, default 30s;
	// controller enforces 1s..10m). Raise for large backlogs: the
	// GET /jobs payload grows with total queue depth. Read at
	// startup only — a hot-reload change needs a controller restart.
	// +kubebuilder:validation:Pattern=`^[0-9]+(ns|us|µs|ms|s|m|h)$`
	SlurmRequestTimeout string `json:"slurmRequestTimeout,omitempty"`

	// client-side rate limit on ALL bridge requests to slurmrestd,
	// in requests per second (0 or omitted = unlimited, the
	// previous behavior). The limiter exists because slurmrestd
	// was the fragile dependency in scale testing: it shares
	// slurmctld's lock and has no throttling of its own, so an
	// unpaced bridge burst can starve the scheduler itself. Burst
	// is derived as 2x the rate (minimum 1). Read at startup only
	// — a hot-reload change needs a controller restart.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=10000
	SlurmRequestsPerSecond float64 `json:"slurmRequestsPerSecond,omitempty"`

	// EnablePrioritySync gates the experimental Slurm<->Workload priority
	// mirror; default OFF (ADR-0009).

	EnablePrioritySync bool `json:"enablePrioritySync,omitempty"`

	// cap on user-originated priority requests (0 disables the extra cap)
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=2147483647
	MaxUserPriority int32 `json:"maxUserPriority,omitempty"`

	// bounded worker pool size for parallel JobSet creation (P5); 0 (or omitted) defaults to 8
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=256
	CreateWorkers int `json:"createWorkers,omitempty"`

	// cancel a released Slurm job whose JobSet has disappeared (D2).
	// OFF by default: scancel is irreversible and keys off the ABSENCE
	// of a JobSet, which a mis-scoped informer cache or lost RBAC on the
	// workload namespace looks exactly like. Opt in once the deployment's
	// cache scope is trusted.
	CancelOrphanedJobs bool `json:"cancelOrphanedJobs,omitempty"`

	// consecutive ticks a job must be seen without its JobSet before cancelOrphanedJobs acts; 0 (or omitted) defaults to 3
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	OrphanGraceTicks int `json:"orphanGraceTicks,omitempty"`

	// proactively delete a preempted job's dynamic Slurm node records on Kueue
	// eviction (ADR-0016), instead of waiting out SlurmdTimeout. OFF by default:
	// it keys off a Kueue eviction signal and deletes node records, so a misread
	// would requeue a healthy job (recoverable). SlurmdTimeout stays the backstop.
	DrainOnPreemption bool `json:"drainOnPreemption,omitempty"`

	// PartitionMappings lists the Slurm partitions under workload-mixing
	// management and their Kueue priority mapping. The 256 ceiling is a
	// sanity rail, far above any real partition count.

	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=256
	PartitionMappings []PartitionMapping `json:"partitionMappings"`

	// Slurmd describes the pods that will register as dynamic Slurm nodes.

	Slurmd Slurmd `json:"slurmd"`

	// Topology controls Slurm-to-Kueue TAS translation; empty disables it.

	Topology Topology `json:"topology,omitempty"`
}

// WorkloadMixingStatus carries the bridge's health report. The bridge writes
// a single Ready condition on tick-health and hot-reload transitions,
// preserving foreign condition types (see internal/bridge/crdconfig.go).
//
// Conditions are the ecosystem-standard metav1.Condition since ADR-0014 PR 2.
// This is a DELIBERATE schema change over the pre-migration hand-written
// shape (which PR 1 reproduced verbatim as a local struct): the old
// structural schema published only type/status/reason/message/
// lastTransitionTime as plain strings, so the apiserver silently PRUNED the
// observedGeneration UpdateReadyCondition has stamped since audit AUD2 — a
// live status bug where a watcher could never tell whether a Ready reading
// was fresh for the spec generation it saw. The change is additive for the
// bridge's own writes (every field the old controller persisted is still
// accepted; observedGeneration and the date-time lastTransitionTime now
// survive), but the schema also enforces metav1.Condition's contract —
// required type/status/reason/lastTransitionTime, enum'd status, name-shaped
// reason — on ANY condition written to this CR, including foreign ones.

type WorkloadMixingStatus struct {
	// Conditions reports the bridge's observed health. The bridge itself
	// maintains exactly one condition of type "Ready"; other condition types
	// are left untouched for other controllers/tooling.

	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// WorkloadMixing is the bridge's configuration surface (ADR-0004 promotion):
// one CR per Slurm-cluster bridge instance. Since ADR-0015 Phase A the
// controller runs one reconcile loop per CR in its own namespace
// (supervisor mode, the CR-mode default); --workloadmixing namespace/name
// remains the explicit single-CR binding. The printer columns surface the
// two things an operator checks first: whether the bridge is healthy
// (Ready) and which Kueue LocalQueue it feeds (Queue).

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=wm
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Queue",type=string,JSONPath=`.spec.localQueue`
type WorkloadMixing struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Bridge configuration, mirroring the file-mode config 1:1. The
	// controller watches this CR (one reconcile loop per CR in its own
	// namespace since ADR-0015; --workloadmixing namespace/name binds to a
	// single CR instead) and hot-reloads on every spec change; a reload
	// that fails validation is reported on status.conditions and the
	// controller keeps running on its previous, still-valid config. Two CRs
	// naming the same slurmRestURL AND an overlapping partition are refused
	// (Ready=False, reason ConflictingSpec) — they would double-manage jobs.
	Spec WorkloadMixingSpec `json:"spec,omitempty"`

	Status WorkloadMixingStatus `json:"status,omitempty"`
}

// WorkloadMixingList contains a list of WorkloadMixing. Used by the
// supervisor's namespace-scoped watch (ADR-0015) and the scheme/list
// machinery.

// +kubebuilder:object:root=true
type WorkloadMixingList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WorkloadMixing `json:"items"`
}
