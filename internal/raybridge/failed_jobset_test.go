package raybridge

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/mrozacki/k8s-bridge/internal/raytranslate"
)

// These tests pin the B1 behavior (the D1 analog): a worker JobSet with a
// Failed=True condition is retried by delete+recreate, bounded by
// MaxWorkerRetries tracked in WorkerRetriesAnnotation on the RayJob, with a
// Warning event and a metric increment per handled failure and a final
// WorkerRetriesExhausted event once the budget is spent.

// failedWorkerJobSet builds a bridge-owned worker JobSet carrying a Failed=True
// condition with the given message, labeled for the test RayJob's UID.
func failedWorkerJobSet(msg string) *jobsetv1alpha2.JobSet {
	js := &jobsetv1alpha2.JobSet{}
	js.Name = jsName
	js.Namespace = ns
	js.Labels = map[string]string{
		raytranslate.ManagedByLabel: raytranslate.ManagedByValue,
		raytranslate.RayJobUIDLabel: "uid-123",
	}
	js.Status.Conditions = []metav1.Condition{{
		Type:               string(jobsetv1alpha2.JobSetFailed),
		Status:             metav1.ConditionTrue,
		Reason:             "FailedJobs",
		Message:            msg,
		LastTransitionTime: metav1.Now(),
	}}
	return js
}

// drainEvents empties a FakeRecorder's buffered channel into a slice.
func drainEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func containsEvent(events []string, substrings ...string) bool {
	for _, e := range events {
		ok := true
		for _, s := range substrings {
			if !strings.Contains(e, s) {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

func TestFailedWorkerJobSetIsRetriedWithEventMetricAndAnnotation(t *testing.T) {
	r := newReconciler(t, rayJob("shared", "batch"), failedWorkerJobSet("job workers failed: MaxRestarts exhausted"))
	rec := record.NewFakeRecorder(10)
	r.Recorder = rec

	failedBefore := testutil.ToFloat64(WorkerJobSetsFailedTotal)
	if _, err := r.Reconcile(context.Background(), req()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// (c) The failed JobSet must be deleted so the next reconcile recreates it.
	js := &jobsetv1alpha2.JobSet{}
	if err := r.Kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: jsName}, js); err == nil {
		t.Errorf("failed worker JobSet should have been deleted for retry")
	}
	// Retry bookkeeping: attempt 1 recorded on the RayJob.
	if got := getRayJob(t, r.Kube).GetAnnotations()[WorkerRetriesAnnotation]; got != "1" {
		t.Errorf("worker-retries annotation = %q, want \"1\"", got)
	}
	// (b) Metric counted exactly once.
	if got := testutil.ToFloat64(WorkerJobSetsFailedTotal); got != failedBefore+1 {
		t.Errorf("WorkerJobSetsFailedTotal = %v, want %v", got, failedBefore+1)
	}
	// (a) Warning event carries the JobSet's failure message.
	events := drainEvents(rec)
	if !containsEvent(events, "WorkerJobSetFailed", "MaxRestarts exhausted") {
		t.Errorf("expected a WorkerJobSetFailed event with the JobSet failure message, got %v", events)
	}

	// Recreation: the next reconcile (in production triggered by the deletion
	// event mapping back to the RayJob) recreates a fresh worker JobSet and
	// does NOT count another failure.
	if _, err := r.Reconcile(context.Background(), req()); err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if err := r.Kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: jsName}, js); err != nil {
		t.Fatalf("worker JobSet should have been recreated after the retry delete: %v", err)
	}
	if jobSetFailed(js) {
		t.Errorf("recreated JobSet must not carry the old Failed condition")
	}
	if got := getRayJob(t, r.Kube).GetAnnotations()[WorkerRetriesAnnotation]; got != "1" {
		t.Errorf("worker-retries annotation after recreation = %q, want unchanged \"1\"", got)
	}
	if got := testutil.ToFloat64(WorkerJobSetsFailedTotal); got != failedBefore+1 {
		t.Errorf("WorkerJobSetsFailedTotal after recreation = %v, want unchanged %v", got, failedBefore+1)
	}
}

func TestWorkerRetriesExhaustedLeavesFailedJobSetInPlace(t *testing.T) {
	rj := rayJob("shared", "batch")
	ann := rj.GetAnnotations()
	ann[WorkerRetriesAnnotation] = "3" // budget already spent (== MaxWorkerRetries)
	rj.SetAnnotations(ann)
	r := newReconciler(t, rj, failedWorkerJobSet("still failing"))
	rec := record.NewFakeRecorder(10)
	r.Recorder = rec

	failedBefore := testutil.ToFloat64(WorkerJobSetsFailedTotal)
	if _, err := r.Reconcile(context.Background(), req()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// No delete: the failed JobSet stays for operator inspection.
	js := &jobsetv1alpha2.JobSet{}
	if err := r.Kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: jsName}, js); err != nil {
		t.Fatalf("exhausted retries must leave the failed JobSet in place: %v", err)
	}
	if !jobSetFailed(js) {
		t.Errorf("the surviving JobSet should still carry its Failed condition")
	}
	// Sentinel recorded so the give-up is signalled exactly once.
	if got := getRayJob(t, r.Kube).GetAnnotations()[WorkerRetriesAnnotation]; got != "4" {
		t.Errorf("worker-retries annotation = %q, want sentinel \"4\"", got)
	}
	if got := testutil.ToFloat64(WorkerJobSetsFailedTotal); got != failedBefore+1 {
		t.Errorf("WorkerJobSetsFailedTotal = %v, want %v (final failure still counted)", got, failedBefore+1)
	}
	if events := drainEvents(rec); !containsEvent(events, "WorkerRetriesExhausted") {
		t.Errorf("expected a final WorkerRetriesExhausted event, got %v", events)
	}

	// A further reconcile of the same exhausted state stays quiet: no new
	// events, no metric movement, no resurrection of the JobSet by ensure.
	if _, err := r.Reconcile(context.Background(), req()); err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if events := drainEvents(rec); len(events) != 0 {
		t.Errorf("exhausted state must not re-emit events, got %v", events)
	}
	if got := testutil.ToFloat64(WorkerJobSetsFailedTotal); got != failedBefore+1 {
		t.Errorf("WorkerJobSetsFailedTotal after quiet pass = %v, want unchanged %v", got, failedBefore+1)
	}
}

func TestFailedJobSetWithForeignUIDIsNotTouched(t *testing.T) {
	// A Failed JobSet under our deterministic name but labeled with a DIFFERENT
	// RayJob UID is a stale predecessor: the failure handler must not delete or
	// count it (that conflict belongs to ensureJobSet's retry-on-stale path).
	stale := failedWorkerJobSet("predecessor failure")
	stale.Labels[raytranslate.RayJobUIDLabel] = "OLD-uid"
	r := newReconciler(t, rayJob("shared", "batch"), stale)

	failedBefore := testutil.ToFloat64(WorkerJobSetsFailedTotal)
	if _, err := r.Reconcile(context.Background(), req()); err == nil {
		t.Errorf("expected the stale-UID retry error from ensureJobSet")
	}
	js := &jobsetv1alpha2.JobSet{}
	if err := r.Kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: jsName}, js); err != nil {
		t.Fatalf("stale JobSet must not be deleted by the failure handler: %v", err)
	}
	if got := testutil.ToFloat64(WorkerJobSetsFailedTotal); got != failedBefore {
		t.Errorf("WorkerJobSetsFailedTotal = %v, want unchanged %v (foreign failure is not ours)", got, failedBefore)
	}
	if _, ok := getRayJob(t, r.Kube).GetAnnotations()[WorkerRetriesAnnotation]; ok {
		t.Errorf("no retry must be recorded for a foreign JobSet's failure")
	}
}

func TestMalformedRetriesAnnotationFailsSafe(t *testing.T) {
	// A hand-mangled counter must be read as EXHAUSTED (stop deleting), never
	// as zero (which would re-arm an unbounded delete loop).
	rj := rayJob("shared", "batch")
	ann := rj.GetAnnotations()
	ann[WorkerRetriesAnnotation] = "banana"
	rj.SetAnnotations(ann)
	r := newReconciler(t, rj, failedWorkerJobSet("boom"))

	failedBefore := testutil.ToFloat64(WorkerJobSetsFailedTotal)
	if _, err := r.Reconcile(context.Background(), req()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	js := &jobsetv1alpha2.JobSet{}
	if err := r.Kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: jsName}, js); err != nil {
		t.Fatalf("malformed counter must fail safe and keep the JobSet: %v", err)
	}
	if got := testutil.ToFloat64(WorkerJobSetsFailedTotal); got != failedBefore {
		t.Errorf("WorkerJobSetsFailedTotal = %v, want unchanged %v (past-exhaustion passes stay quiet)", got, failedBefore)
	}
}
