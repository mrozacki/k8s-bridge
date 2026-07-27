// WorkloadMixing CR as a configuration source (ADR-0004 promotion; typed
// consumption since ADR-0014 PR 2): the CR spec is field-for-field identical
// to the file config, so conversion is a plain, tested field copy (FromCR) —
// no unstructured traversal, no JSON round-trip. The bridge also reports
// health through status.conditions.
package bridge

import (
	"context"
	"fmt"
	"sync"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/mrozacki/k8s-bridge/api/v1alpha1"
	"github.com/mrozacki/k8s-bridge/internal/config"
)

// LoadConfigFromCR reads a WorkloadMixing CR through the given client (the
// Manager's cached client in the hot-reload path; a direct client during
// bootstrap — see cmd/k8s-bridge/main.go) and converts its spec into the
// bridge config. The CR's namespace doubles as the JobSet namespace.
func LoadConfigFromCR(ctx context.Context, kube client.Client, namespace, name string) (*config.Config, string, error) {
	wm := &v1alpha1.WorkloadMixing{}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, wm); err != nil {
		return nil, "", fmt.Errorf("fetching WorkloadMixing %s/%s: %w", namespace, name, err)
	}
	cfg, err := FromCR(wm)
	if err != nil {
		return nil, "", err
	}
	return cfg, wm.ResourceVersion, nil
}

// FromCR converts a typed WorkloadMixing into the bridge config: a plain
// field copy (the CR spec mirrors config.Config 1:1 by construction — see
// api/v1alpha1/workloadmixing_types.go), then the SAME ApplyDefaults and
// Validate the file loader runs, so both config sources keep one source of
// truth for defaults and invariants and a validation failure surfaces the
// identical message into the Ready condition.
//
// Unknown-field strictness moved to the apiserver with the typed switch
// (ADR-0014 PR 2): the pre-migration unstructured loader re-decoded the spec
// with DisallowUnknownFields, a guard that could only ever fire against a
// fake client — a real apiserver's structural schema prunes unknown fields
// before anything is persisted, so the strict decoder had nothing left to
// reject. Now pruning is explicitly the contract (pinned by the envtest
// integration test), and the schema itself — generated from the same Go
// types this function copies from — is what keeps CR and config in sync.
func FromCR(wm *v1alpha1.WorkloadMixing) (*config.Config, error) {
	spec := wm.Spec

	pollInterval, err := parseCRDuration("pollInterval", spec.PollInterval)
	if err != nil {
		return nil, err
	}
	slurmRequestTimeout, err := parseCRDuration("slurmRequestTimeout", spec.SlurmRequestTimeout)
	if err != nil {
		return nil, err
	}
	failedJobSetRetention, err := parseCRDuration("failedJobSetRetention", spec.FailedJobSetRetention)
	if err != nil {
		return nil, err
	}

	cfg := &config.Config{
		SlurmRestURL:               spec.SlurmRestURL,
		SlurmTokenFile:             spec.SlurmTokenFile,
		SlurmUser:                  spec.SlurmUser,
		AllowInsecureHTTP:          spec.AllowInsecureHTTP,
		SlurmCACertFile:            spec.SlurmCACertFile,
		SlurmInsecureSkipTLSVerify: spec.SlurmInsecureSkipTLSVerify,
		SlurmRequestTimeout:        slurmRequestTimeout,
		SlurmRequestsPerSecond:     spec.SlurmRequestsPerSecond,
		LocalQueue:                 spec.LocalQueue,
		PollInterval:               pollInterval,
		EnablePrioritySync:         spec.EnablePrioritySync,
		MaxUserPriority:            spec.MaxUserPriority,
		CancelOrphanedJobs:         spec.CancelOrphanedJobs,
		OrphanGraceTicks:           spec.OrphanGraceTicks,
		DrainOnPreemption:          spec.DrainOnPreemption,
		FailedJobSetRetention:      failedJobSetRetention,
		CreateWorkers:              spec.CreateWorkers,
		Slurmd: config.Slurmd{
			Image:           spec.Slurmd.Image,
			ConfServer:      spec.Slurmd.ConfServer,
			AuthSecretName:  spec.Slurmd.AuthSecretName,
			GPUResourceName: spec.Slurmd.GPUResourceName,
		},
		Topology: config.Topology{
			RequiredLevel:  spec.Topology.RequiredLevel,
			PreferredLevel: spec.Topology.PreferredLevel,
		},
	}
	// Pointer fields are copied by VALUE so the returned config never aliases
	// the (possibly cache-owned) CR: a later hot-reload mutating one must not
	// reach into the other.
	if spec.Slurmd.Privileged != nil {
		v := *spec.Slurmd.Privileged
		cfg.Slurmd.Privileged = &v
	}
	if spec.Slurmd.SharedStorage != nil {
		cfg.Slurmd.SharedStorage = &config.SharedStorage{
			NFSServer: spec.Slurmd.SharedStorage.NFSServer,
			NFSPath:   spec.Slurmd.SharedStorage.NFSPath,
			MountPath: spec.Slurmd.SharedStorage.MountPath,
		}
	}
	for _, m := range spec.PartitionMappings {
		cfg.PartitionMappings = append(cfg.PartitionMappings, config.PartitionMapping{
			PartitionName:         m.PartitionName,
			WorkloadPriorityClass: m.WorkloadPriorityClass,
			LocalQueue:            m.LocalQueue,
		})
	}
	// The CR's namespace is authoritative for where JobSets are created; set it
	// before Validate so the required-field check sees it.
	cfg.Namespace = wm.Namespace
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("WorkloadMixing %s/%s: %w", wm.Namespace, wm.Name, err)
	}
	return cfg, nil
}

// parseCRDuration parses a Go-duration spec string ("10s") into the config
// wrapper type. The CR carries durations as strings (the CRD pattern
// pre-filters gross typos); an empty string means "unset" and lets
// ApplyDefaults fill the default, mirroring the file loader. The error text
// keeps the pre-migration decoder's framing ("spec does not match config
// schema") so operators' log greps and the Ready-condition message survive
// the typed switch.
func parseCRDuration(field, value string) (config.Duration, error) {
	var d config.Duration
	if value == "" {
		return d, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return d, fmt.Errorf("spec does not match config schema: %s: invalid duration %q: %w", field, value, err)
	}
	d.Duration = parsed
	return d, nil
}

// readyConditionType is the single condition type this bridge writes onto a
// WorkloadMixing CR's status. Using meta.SetStatusCondition (below) rather
// than replacing status.conditions wholesale means any OTHER condition type
// another controller/tool adds to the same CR survives our patch (audit
// AUD2: the previous version clobbered the whole array).
const readyConditionType = "Ready"

// Reasons stamped on the Ready condition. TickSucceeded/TickFailed are the
// historic pair every mode uses for tick-health transitions; the others are
// supervisor-mode outcomes (ADR-0015 Phase A) explaining why no Bridge is
// running for a CR at all.
const (
	// ReasonTickSucceeded / ReasonTickFailed reflect the outcome of the
	// most recent reconcile tick of the CR's running Bridge.
	ReasonTickSucceeded = "TickSucceeded"
	ReasonTickFailed    = "TickFailed"
	// ReasonConflictingSpec: the CR names the same slurmRestURL AND an
	// overlapping partition as another, already-running WorkloadMixing —
	// starting it would double-manage those jobs (ADR-0015 §Decision 3).
	ReasonConflictingSpec = "ConflictingSpec"
	// ReasonInvalidSpec: the spec failed FromCR conversion, validation, or
	// the deploy-time slurmd image allowlist, and no Bridge is running to
	// keep a last-good config alive.
	ReasonInvalidSpec = "InvalidSpec"
	// ReasonStartFailed: the spec is valid but constructing the Bridge's
	// Slurm client failed (e.g. the token file is not mounted/readable yet);
	// retried on a timer.
	ReasonStartFailed = "StartFailed"
)

// UpdateReadyCondition reflects tick health in the CR status. Called only
// on transitions to keep API traffic negligible.
//
// It reads the CR first (for its current status.conditions and
// metadata.generation), merges the Ready condition in with
// meta.SetStatusCondition, and merge-patches the status back — preserving
// any other condition types and stamping observedGeneration so a watcher
// can tell whether this Ready reading is fresh for the spec it saw. Since
// ADR-0014 PR 2 the CRD schema is metav1.Condition, so the stamped
// observedGeneration is actually PERSISTED (the pre-migration structural
// schema silently pruned it — see api/v1alpha1/workloadmixing_types.go).
//
// Status().Patch with MergeFrom (not Status().Update) is deliberate: it
// keeps the pre-migration merge-patch semantics — no optimistic-locking
// conflict against a concurrent writer of OTHER status fields, and the
// request body carries only what changed.
func UpdateReadyCondition(ctx context.Context, kube client.Client, namespace, name string, ready bool, message string) error {
	status, reason := metav1.ConditionTrue, ReasonTickSucceeded
	if !ready {
		status, reason = metav1.ConditionFalse, ReasonTickFailed
	}
	return UpdateReadyConditionReason(ctx, kube, namespace, name, status, reason, message)
}

// UpdateReadyConditionReason is UpdateReadyCondition with an explicit
// status/reason pair, for supervisor-mode outcomes that are not tick health
// (ConflictingSpec, InvalidSpec, StartFailed — ADR-0015 Phase A). Same merge
// semantics as UpdateReadyCondition, which delegates here.
func UpdateReadyConditionReason(ctx context.Context, kube client.Client, namespace, name string, status metav1.ConditionStatus, reason, message string) error {
	if len(message) > 256 {
		message = message[:256]
	}

	wm := &v1alpha1.WorkloadMixing{}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, wm); err != nil {
		return fmt.Errorf("fetching WorkloadMixing %s/%s for status update: %w", namespace, name, err)
	}
	base := wm.DeepCopy()
	apimeta.SetStatusCondition(&wm.Status.Conditions, metav1.Condition{
		Type:               readyConditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: wm.Generation,
	})
	if err := kube.Status().Patch(ctx, wm, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("patching WorkloadMixing %s/%s status: %w", namespace, name, err)
	}
	return nil
}

// ReadyConditionWriter is the single serialization point for every write to
// ONE WorkloadMixing CR's Ready condition. Two writers feed that condition:
// the per-tick health reporter (Ready=True/False as ticks succeed/fail) and
// the spec-rejection path (hot-reload failure in single-CR mode;
// InvalidSpec/ConflictingSpec/StartFailed in supervisor mode). Uncoordinated
// they RACE: a CI run caught the tick reporter's True/TickSucceeded landing
// AFTER the supervisor's False/InvalidSpec write — the CR then claimed the
// INVALID generation was healthy (a tick write stamps whatever generation
// it reads, i.e. the latest, rejected one), which is doubly misleading.
//
// Chosen semantics: **a rejected latest spec overrides tick health.** While
// rejected, tick reports are SUPPRESSED entirely rather than folded into a
// per-tick composite write: the rejection message already states that the
// loop keeps running on its last-good config, so rewriting it every tick
// would add API traffic without information — and a tick FAILURE during the
// rejection window would overwrite WHY the spec is bad with a transient
// Slurm error, hiding the operator's actual to-do. The mutex is held ACROSS
// the status write on both paths, which is what makes the override
// race-free: a tick report can never interleave between "spec rejected" and
// its False write — whichever writer enters second blocks, and if the tick
// writer entered first the rejection's False still lands last.
//
// ClearRejection resets the tick debounce (lastTickState), so the FIRST
// tick report after a fixed spec re-publishes truthful health even without
// a success/failure transition — without that reset, a bridge that ticked
// "ok" before and after the rejection window would leave the stale False
// condition in place forever.
type ReadyConditionWriter struct {
	Kube      client.Client
	Namespace string
	Name      string

	// mu guards rejected/lastTickState AND stays held across the status
	// writes below — see the type doc for why that hold is the point.
	// Consequence: a wedged API server can pin a writer for up to
	// readyPatchTimeout; acceptable because only status writers take this
	// mutex (never the HTTP probe handlers or the tick loop itself).
	mu            sync.Mutex
	rejected      bool
	lastTickState string
}

// readyPatchTimeout bounds each Ready-condition write (audit AUD2: a status
// write must not outlive process shutdown or hang forever against a wedged
// API server). Applied inside the writer so every caller inherits it.
const readyPatchTimeout = 5 * time.Second

// ReportTick reflects one tick outcome (nil = success) onto the Ready
// condition, debounced to transitions. Suppressed while the CR's latest
// spec is rejected — see the type doc. Suppressed and failed writes do NOT
// update the debounce state, so the report retries on a later tick.
func (w *ReadyConditionWriter) ReportTick(ctx context.Context, tickErr error) {
	state, msg := "ok", "reconcile loop healthy"
	if tickErr != nil {
		state, msg = "fail", tickErr.Error()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.rejected || state == w.lastTickState {
		return
	}
	patchCtx, cancel := context.WithTimeout(ctx, readyPatchTimeout)
	defer cancel()
	if err := UpdateReadyCondition(patchCtx, w.Kube, w.Namespace, w.Name, tickErr == nil, msg); err == nil {
		w.lastTickState = state
	}
}

// ReportRejection marks the CR's latest spec as rejected and writes
// Ready=False with the given reason/message. Tick reports are suppressed
// from before the write until ClearRejection, so this False cannot be
// overwritten by a concurrent or later healthy tick. The rejected flag is
// set even when the write itself fails — suppression is the safe direction,
// and the caller's requeue (or the next spec event) retries the write.
func (w *ReadyConditionWriter) ReportRejection(ctx context.Context, reason, message string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.rejected = true
	patchCtx, cancel := context.WithTimeout(ctx, readyPatchTimeout)
	defer cancel()
	return UpdateReadyConditionReason(patchCtx, w.Kube, w.Namespace, w.Name, metav1.ConditionFalse, reason, message)
}

// ClearRejection lifts the suppression once the spec is acceptable again
// (successful hot-reload, restart, or start). It deliberately writes
// nothing itself — Ready=True must come from an actual tick outcome — but
// resets the debounce so the very next ReportTick writes even if the tick
// state never transitioned across the rejection window.
func (w *ReadyConditionWriter) ClearRejection() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.rejected = false
	w.lastTickState = ""
}

// ConfigReconciler implements A1's hot-reload: it watches ONE WorkloadMixing
// CR (the same namespace/name the bridge was started with via
// --workloadmixing) through the manager's cache and, on every spec change,
// re-reads it and atomically swaps the Bridge's live *config.Config via
// setCfg — no restart required. This is a manager-adopted reconciler
// (ADR-0011), not the tick loop itself: watching the CR is cheap and the
// manager's informer/workqueue machinery already does exactly the
// debouncing and retry-with-backoff this needs, so there is no reason to
// hand-rebuild that on top of the bridge's own Nudge mechanism (which
// exists for JobSet/Workload events instead, where thin-surface and
// coalescing-into-a-poll-tick considerations are different — see
// docs/adr/0011).
//
// File-config mode never constructs a ConfigReconciler at all (see
// cmd/k8s-bridge/main.go): the CR loader and this reconciler are only wired
// up when --workloadmixing is set, preserving one-shot file-mode behavior
// exactly as before.
type ConfigReconciler struct {
	Kube      client.Client
	Bridge    *Bridge
	Namespace string
	Name      string
	// Writer coordinates this reconciler's rejection writes with the
	// Bridge's per-tick health reports (main.go wires the SAME writer into
	// both) — single-CR mode has the identical two-writer race supervisor
	// mode had: a reload-failure False racing an in-flight tick True.
	// Optional for older call sites/tests; nil falls back to the historic
	// direct write, which is exactly the racy behavior, so production
	// wiring must always set it.
	Writer *ReadyConditionWriter
}

// Reconcile re-reads the WorkloadMixing CR and swaps it into the Bridge.
// Any request for a different namespace/name is ignored (defensive: the
// watch this reconciler is wired to in main.go is already restricted to
// exactly one object via a field/name predicate, so this should never
// trigger in practice). A reload failure (e.g. a hand-edited CR that no
// longer validates) is reported on the CR's Ready condition — the bridge
// keeps running on its LAST GOOD config rather than crashing or freezing on
// a bad one, and (via Writer) tick health stops writing until the spec is
// fixed, so the False condition cannot be raced back to True. The reason
// stays TickFailed (not InvalidSpec) on this path: it is the reason
// UpdateReadyCondition has always stamped in single-CR mode, and operators'
// alerts/greps on the historic reason/message pair must survive unchanged.
func (r *ConfigReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	if req.Namespace != r.Namespace || req.Name != r.Name {
		return reconcile.Result{}, nil
	}
	cfg, rv, err := LoadConfigFromCR(ctx, r.Kube, r.Namespace, r.Name)
	if err != nil {
		msg := fmt.Sprintf("hot-reload failed, keeping previous config: %v", err)
		var uerr error
		if r.Writer != nil {
			uerr = r.Writer.ReportRejection(ctx, ReasonTickFailed, msg)
		} else {
			uerr = UpdateReadyCondition(ctx, r.Kube, r.Namespace, r.Name, false, msg)
		}
		if uerr != nil {
			return reconcile.Result{}, fmt.Errorf("%s (also failed to report status: %w)", msg, uerr)
		}
		// Do not requeue: nothing changes until the CR's spec changes again,
		// which will deliver a fresh watch event on its own.
		return reconcile.Result{}, nil
	}
	r.Bridge.setCfg(cfg)
	if r.Writer != nil {
		// The latest spec is good again: let tick health own the condition
		// (the next tick republishes it even without a transition).
		r.Writer.ClearRejection()
	}
	r.Bridge.log.Info("config hot-reloaded from WorkloadMixing CR", "cr", r.Namespace+"/"+r.Name, "resourceVersion", rv)
	return reconcile.Result{}, nil
}
