// Package raybridge implements the ray-bridge reconcile logic: bring the inner
// workloads of a shared, long-lived RayCluster under Kueue admission at
// per-job granularity (ADR-0006/0012). It is the Ray counterpart of
// internal/bridge, but event-driven rather than polling — a RayJob is a native,
// watchable Kubernetes object, so there is no slurmrestd-style poll loop.
//
// Per inner RayJob the controller runs the canonical bridge cycle:
//
//	hold (spec.suspend, set by the submitter/webhook)
//	  -> stand up dedicated Ray-worker capacity as a Kueue-admitted JobSet
//	  -> once Kueue admits AND the workers join, unsuspend + pin the RayJob
//	  -> on completion, GC the workers (RayJob owns the JobSet)
//	  -> if Kueue evicts the workers (preemption), re-suspend to requeue
package raybridge

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/mrozacki/k8s-bridge/internal/ray"
	"github.com/mrozacki/k8s-bridge/internal/rayconfig"
	"github.com/mrozacki/k8s-bridge/internal/raytranslate"
)

// WorkerRetriesAnnotation is set by the bridge ON the inner RayJob to count
// how many times it has deleted a Failed worker JobSet to retry it. It lives
// on the RayJob rather than the JobSet because the JobSet is exactly what gets
// deleted on every retry — a counter there would reset itself each attempt.
// The value is a plain base-10 integer; a value above MaxWorkerRetries means
// the retry budget is spent and the final Failed JobSet has been left in place
// for operator inspection. Operators re-arm the loop by removing the
// annotation (and deleting the failed JobSet).
const WorkerRetriesAnnotation = "ray-bridge.x-k8s.io/worker-retries"

// MaxWorkerRetries bounds how many times the bridge deletes and recreates a
// Failed worker JobSet before giving up (B1, the D1 analog). The JobSet's own
// FailurePolicy.MaxRestarts (2, see raytranslate.ToWorkerJobSet) already
// absorbs transient pod-level flake, so a JobSet that still went Failed is
// usually broken for a systemic reason (bad worker image, unschedulable shape,
// unreachable Ray head). Three whole-JobSet retries is enough to ride out a
// node drain or a slow head restart without turning a permanent failure into
// an unbounded create/fail/delete loop against the API server.
const MaxWorkerRetries = 3

// Reconciler reconciles inner RayJobs into Kueue-admitted worker JobSets.
type Reconciler struct {
	Kube client.Client
	Cfg  *rayconfig.Config
	Log  *slog.Logger
	// Recorder emits Events on the RayJob; nil is valid (unit tests).
	Recorder record.EventRecorder
}

func (r *Reconciler) defaults() ray.Defaults {
	return ray.Defaults{
		Workers:        r.Cfg.Worker.DefaultWorkers,
		CPUsPerWorker:  r.Cfg.Worker.DefaultCPUs,
		MemPerWorkerMB: r.Cfg.Worker.DefaultMemoryMB,
	}
}

// Reconcile drives one inner RayJob through the bridge cycle. It is idempotent
// and safe to call repeatedly: every step is a create-if-missing / patch-if-
// needed, so a redundant reconcile is a no-op. This wrapper only adds the
// error counter (ray_bridge_reconcile_errors_total) around the real work in
// reconcile, so every returned-and-requeued error is counted exactly once no
// matter which step produced it.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	res, err := r.reconcile(ctx, req)
	if err != nil {
		ReconcileErrorsTotal.Inc()
	}
	return res, err
}

// reconcile keeps the (reconcile.Result, error) shape of the public
// Reconcile deliberately, even though every current path returns a zero
// Result: the wrapper stays a pure pass-through, and a future
// requeue-with-backoff only has to change a return site, not both
// signatures.
//
//nolint:unparam // see above — Result is intentionally always zero today
func (r *Reconciler) reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := r.Log.With("rayJob", req.String())

	raw, found, err := ray.Get(ctx, r.Kube, req.Namespace, req.Name)
	if err != nil {
		return reconcile.Result{}, err
	}
	if !found {
		// RayJob gone: the worker JobSet is owned by it, so Kubernetes garbage
		// collection removes the workers automatically. Nothing to do.
		return reconcile.Result{}, nil
	}

	job, err := ray.FromUnstructured(raw, r.defaults())
	if err != nil {
		// Malformed worker-shape annotation: a permanent error the operator
		// must fix. Surface it and stop — retrying will not help.
		log.Warn("RayJob has invalid worker-shape annotation, ignoring", "error", err)
		r.eventf(raw, "InvalidSpec", "%v", err)
		return reconcile.Result{}, nil
	}

	// Scope gates (analogs of the Slurm side's unmanaged-partition skip): only
	// inner workloads of a managed cluster with a mapped pool are ours.
	if !job.IsInnerWorkload() {
		return reconcile.Result{}, nil
	}
	if _, managed := r.Cfg.HeadAddressFor(job.TargetCluster); !managed {
		return reconcile.Result{}, nil
	}
	if _, mapped := r.Cfg.PriorityClassFor(job.Pool); !mapped {
		log.Info("RayJob pool not managed by ray-bridge, ignoring", "pool", job.Pool)
		r.eventf(raw, "UnmanagedPool",
			"pool %q has no priority-class mapping", job.Pool)
		return reconcile.Result{}, nil
	}

	jsName := raytranslate.WorkerJobSetName(job)

	// Terminal RayJob: drain its dedicated capacity by deleting the worker
	// JobSet (its pods leave the Ray cluster on delete).
	if job.IsTerminal() {
		return reconcile.Result{}, r.deleteWorkerJobSet(ctx, log, req.Namespace, jsName)
	}

	// B1 (the D1 analog): a worker JobSet that exhausted FailurePolicy.MaxRestarts
	// and went Failed=True will never advertise the pin resource, so the RayJob
	// would wait on it forever with no signal. Handle it (bounded retry or final
	// give-up) before the ensure step below, which would otherwise treat the
	// failed set as "already exists, nothing to do".
	handled, err := r.handleFailedWorkerJobSet(ctx, log, raw, job, jsName)
	if err != nil {
		return reconcile.Result{}, err
	}
	if handled {
		// Either the failed JobSet was just deleted (its deletion event maps
		// back to this RayJob and the next reconcile recreates it), or retries
		// are exhausted and the failed set is deliberately left in place — in
		// both cases do NOT fall through to ensure/create here.
		return reconcile.Result{}, nil
	}

	// Ensure the worker JobSet exists (create-if-missing, RayJob-owned for GC).
	desired, err := raytranslate.ToWorkerJobSet(job, r.Cfg)
	if err != nil {
		log.Warn("translation failed, ignoring RayJob", "error", err)
		r.eventf(raw, "TranslationFailed", "%v", err)
		return reconcile.Result{}, nil
	}
	setOwner(desired, raw)
	if err := r.ensureJobSet(ctx, log, desired); err != nil {
		return reconcile.Result{}, err
	}

	// Pin-gate model (ADR-0013, live finding 2026-07-07): KubeRay FORBIDS
	// spec.suspend in clusterSelector mode ("the ClusterSelector mode doesn't
	// support the suspend operation"), so the bridge does NOT hold/release the
	// RayJob. Instead the inner RayJob carries entrypointResources requiring
	// wm-job-<id> (injected by the admission webhook, internal/raywebhook, or
	// set by the submitter), and the worker JobSet created above advertises
	// exactly that resource. The RayJob's driver therefore cannot be scheduled
	// until Kueue admits the worker JobSet and the workers join — the pin
	// resource appearing IS the release, gated by Kueue quota. Once the worker
	// JobSet exists the bridge has nothing more to do until the RayJob turns
	// terminal (handled above), so this is a clean tail with no suspend dance.
	return reconcile.Result{}, nil
}

// ensureJobSet creates the worker JobSet if missing (idempotent upsert on the
// deterministic name).
//
// On AlreadyExists it verifies the existing JobSet actually belongs to THIS
// RayJob (by UID label) before treating it as success (review finding): the
// JobSet name is derived from the RayJob name only, so a delete+recreate of a
// RayJob with the same name can leave the previous (terminating) JobSet in
// place. Returning success then would leave the new RayJob's pin gate advertised
// by nobody. A UID mismatch is treated as a transient error so the reconcile
// retries once the stale JobSet finalizes.
func (r *Reconciler) ensureJobSet(ctx context.Context, log *slog.Logger, desired *jobsetv1alpha2.JobSet) error {
	err := r.Kube.Create(ctx, desired)
	if err == nil {
		WorkerJobSetsCreatedTotal.Inc()
		log.Info("created worker JobSet", "jobset", desired.Name)
		return nil
	}
	if apierrors.IsAlreadyExists(err) {
		existing := &jobsetv1alpha2.JobSet{}
		if getErr := r.Kube.Get(ctx, client.ObjectKeyFromObject(desired), existing); getErr != nil {
			return fmt.Errorf("checking existing worker JobSet %s: %w", desired.Name, getErr)
		}
		if existing.Labels[raytranslate.RayJobUIDLabel] == desired.Labels[raytranslate.RayJobUIDLabel] {
			return nil // already ours — idempotent
		}
		return fmt.Errorf("worker JobSet %s exists for a different RayJob UID (stale/terminating), retrying", desired.Name)
	}
	return fmt.Errorf("creating worker JobSet %s: %w", desired.Name, err)
}

// jobSetFailed reports whether js carries a Failed=True condition (B1/D1),
// e.g. because FailurePolicy.MaxRestarts was exhausted. Uses the real
// jobsetv1alpha2.JobSetFailed constant, not a hand-rolled string — the same
// helper the Slurm side's internal/bridge uses (not imported from there: that
// package drags in the whole slurmrestd client).
func jobSetFailed(js *jobsetv1alpha2.JobSet) bool {
	return apimeta.IsStatusConditionTrue(js.Status.Conditions, string(jobsetv1alpha2.JobSetFailed))
}

// handleFailedWorkerJobSet implements B1, the ray-side analog of the Slurm
// bridge's failJobForDeadJobSet (D1): a worker JobSet with a Failed=True
// condition will never advertise the pin resource, so without intervention the
// RayJob waits forever with no event and no metric. It reports handled=true
// when it took ownership of this reconcile (retry issued, or retries already
// exhausted) so the caller skips the ensure/create step.
//
// Protocol, per observed failure:
//   - retries so far < MaxWorkerRetries: emit a Warning event
//     (WorkerJobSetFailed) carrying the JobSet's failure message, count the
//     failure, durably bump WorkerRetriesAnnotation on the RayJob, THEN delete
//     the failed JobSet. The deletion event maps back to the RayJob and the
//     next reconcile recreates a fresh set. The annotation is written BEFORE
//     the delete on purpose: if the two can be split by a crash, losing a
//     delete keeps the count honest, while losing a count would make the
//     retry loop unbounded — the failure mode we exist to prevent.
//   - retries == MaxWorkerRetries: budget spent. Count the final failure, bump
//     the annotation once more (to MaxWorkerRetries+1, the "already signalled"
//     sentinel) and emit the final WorkerRetriesExhausted Warning. The Failed
//     JobSet is deliberately LEFT IN PLACE so the operator can inspect its
//     pods/conditions; deleting it would also erase the evidence.
//   - retries > MaxWorkerRetries: exhaustion was already signalled on a prior
//     pass; stay quiet (no duplicate events/metric increments) but still claim
//     the reconcile so the failed set is not resurrected by ensureJobSet.
//
// Identity is verified via the RayJob-UID label before anything is deleted: a
// stale JobSet left by a deleted-and-recreated RayJob of the same name belongs
// to ensureJobSet's retry path, not to this one.
func (r *Reconciler) handleFailedWorkerJobSet(ctx context.Context, log *slog.Logger, raw *unstructured.Unstructured, job *ray.RayJob, jsName string) (handled bool, err error) {
	js := &jobsetv1alpha2.JobSet{}
	if err := r.Kube.Get(ctx, client.ObjectKey{Namespace: job.Namespace, Name: jsName}, js); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil // no worker JobSet yet — the ensure step creates it
		}
		return false, fmt.Errorf("checking worker JobSet %s for a Failed condition: %w", jsName, err)
	}
	if js.Labels[raytranslate.RayJobUIDLabel] != job.UID {
		return false, nil // not ours (stale/terminating predecessor) — never delete it here
	}
	if !jobSetFailed(js) {
		return false, nil
	}

	retries := workerRetries(raw, log)
	if retries > MaxWorkerRetries {
		return true, nil // already signalled exhaustion; leave the failed set alone
	}

	reason := "unknown"
	if cond := apimeta.FindStatusCondition(js.Status.Conditions, string(jobsetv1alpha2.JobSetFailed)); cond != nil && cond.Message != "" {
		reason = cond.Message
	}
	WorkerJobSetsFailedTotal.Inc()

	if retries == MaxWorkerRetries {
		// Record the sentinel BEFORE emitting the event, so a failed update
		// retries the whole pass and a duplicate event is the worst outcome
		// (the recorder aggregates duplicates anyway).
		if err := r.setWorkerRetries(ctx, raw, retries+1); err != nil {
			return false, err
		}
		r.eventf(raw, "WorkerRetriesExhausted",
			"worker JobSet %s failed %d times (last: %s); giving up — the failed JobSet is left in place for inspection, remove the %s annotation (and the JobSet) to retry",
			js.Name, MaxWorkerRetries+1, reason, WorkerRetriesAnnotation)
		log.Warn("worker JobSet retries exhausted, leaving failed JobSet in place",
			"jobset", js.Name, "retries", MaxWorkerRetries, "reason", reason)
		return true, nil
	}

	// Durably record the attempt first (see the ordering rationale above).
	if err := r.setWorkerRetries(ctx, raw, retries+1); err != nil {
		return false, err
	}
	r.eventf(raw, "WorkerJobSetFailed",
		"worker JobSet %s failed (%s); deleting it to retry (attempt %d of %d)",
		js.Name, reason, retries+1, MaxWorkerRetries)
	if err := r.Kube.Delete(ctx, js); err != nil && !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("deleting failed worker JobSet %s for retry: %w", js.Name, err)
	}
	log.Warn("deleted failed worker JobSet, will recreate",
		"jobset", js.Name, "reason", reason, "attempt", retries+1, "maxRetries", MaxWorkerRetries)
	return true, nil
}

// workerRetries reads the retry counter off the RayJob. Absent means zero. A
// malformed value (only possible via hand-editing) is treated as EXHAUSTED,
// not zero: failing safe here means "stop deleting JobSets", since a parse bug
// misread as zero would re-arm an unbounded retry loop.
func workerRetries(raw *unstructured.Unstructured, log *slog.Logger) int {
	v, ok := raw.GetAnnotations()[WorkerRetriesAnnotation]
	if !ok {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || n < 0 {
		log.Warn("RayJob has malformed worker-retries annotation, treating retries as exhausted",
			"annotation", WorkerRetriesAnnotation, "value", v)
		return MaxWorkerRetries + 1
	}
	return n
}

// setWorkerRetries persists the retry counter on the RayJob (mutating raw
// in-place so the caller's view stays consistent).
func (r *Reconciler) setWorkerRetries(ctx context.Context, raw *unstructured.Unstructured, n int) error {
	ann := raw.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}
	ann[WorkerRetriesAnnotation] = strconv.Itoa(n)
	raw.SetAnnotations(ann)
	if err := r.Kube.Update(ctx, raw); err != nil {
		return fmt.Errorf("recording worker-retry count on RayJob %s/%s: %w", raw.GetNamespace(), raw.GetName(), err)
	}
	return nil
}

// deleteWorkerJobSet removes the worker JobSet for a terminal/gone RayJob. A
// missing JobSet is success (already cleaned up).
func (r *Reconciler) deleteWorkerJobSet(ctx context.Context, log *slog.Logger, namespace, name string) error {
	js := &jobsetv1alpha2.JobSet{}
	js.Name = name
	js.Namespace = namespace
	if err := r.Kube.Delete(ctx, js); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("deleting worker JobSet %s: %w", name, err)
	}
	log.Info("cleaned up worker JobSet", "jobset", name)
	return nil
}

// setOwner makes the RayJob the owner of its worker JobSet so Kubernetes GC
// deletes the workers when the RayJob is deleted. Non-controller ownerRef: GC
// still cascades, and nothing competes to "control" the JobSet.
func setOwner(js *jobsetv1alpha2.JobSet, rayJob *unstructured.Unstructured) {
	js.OwnerReferences = append(js.OwnerReferences, metav1.OwnerReference{
		APIVersion: ray.RayJobGVK.GroupVersion().String(),
		Kind:       ray.RayJobGVK.Kind,
		Name:       rayJob.GetName(),
		UID:        rayJob.GetUID(),
	})
}

// eventf emits a Warning event on the RayJob (nil-safe without a recorder).
// The type is hardcoded: everything ray-bridge has to say to a RayJob owner
// today is a problem report (invalid spec, unmanaged pool, translation or
// worker failure) — routine lifecycle is visible on the JobSet itself. Add
// an eventType parameter back if a Normal-type event ever becomes needed.
func (r *Reconciler) eventf(obj *unstructured.Unstructured, reason, msgFmt string, args ...any) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(obj, corev1.EventTypeWarning, reason, msgFmt, args...)
}

// SetupWithManager wires the controller: it reconciles on RayJob events and
// re-reconciles the owning RayJob on worker-JobSet events (so a failed/deleted
// worker JobSet is recreated), mapped back through the deterministic
// ray-workers-<name> naming. No Kueue Workload watch is needed in the pin-gate
// model: the bridge does not act on admission state (the pin resource, not a
// suspend flag, is what gates the RayJob), so a Workload event would trigger no
// new work.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	rayJob := &unstructured.Unstructured{}
	rayJob.SetGroupVersionKind(ray.RayJobGVK)
	return ctrl.NewControllerManagedBy(mgr).
		Named("ray-bridge").
		For(rayJob).
		Watches(&jobsetv1alpha2.JobSet{}, handler.EnqueueRequestsFromMapFunc(mapJobSetToRayJob)).
		Complete(r)
}

// mapJobSetToRayJob enqueues the RayJob owning a bridge-created worker JobSet.
func mapJobSetToRayJob(_ context.Context, obj client.Object) []reconcile.Request {
	if obj.GetLabels()[raytranslate.ManagedByLabel] != raytranslate.ManagedByValue {
		return nil
	}
	name, ok := raytranslate.RayJobNameFromJobSet(obj.GetName())
	if !ok {
		return nil
	}
	return []reconcile.Request{{NamespacedName: client.ObjectKey{Namespace: obj.GetNamespace(), Name: name}}}
}
