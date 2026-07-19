package raybridge

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/mrozacki/k8s-bridge/internal/raytranslate"
)

// These tests exercise the ray_bridge_* metric wiring in reconciler.go (B3).
// Like the Slurm side's internal/bridge/metrics_test.go, the counters are
// package-level and shared across the whole test binary, so assertions use
// BEFORE/AFTER deltas rather than absolute values.

// TestReconcileIncrementsWorkerJobSetsCreatedTotal pins that a create counts
// exactly once and the idempotent second pass (AlreadyExists, ours) does not.
func TestReconcileIncrementsWorkerJobSetsCreatedTotal(t *testing.T) {
	r := newReconciler(t, rayJob("shared", "batch"))
	createdBefore := testutil.ToFloat64(WorkerJobSetsCreatedTotal)

	if _, err := r.Reconcile(context.Background(), req()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := testutil.ToFloat64(WorkerJobSetsCreatedTotal); got != createdBefore+1 {
		t.Errorf("WorkerJobSetsCreatedTotal = %v, want %v", got, createdBefore+1)
	}

	// Second reconcile is a no-op on the existing JobSet: no double count.
	if _, err := r.Reconcile(context.Background(), req()); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if got := testutil.ToFloat64(WorkerJobSetsCreatedTotal); got != createdBefore+1 {
		t.Errorf("WorkerJobSetsCreatedTotal after idempotent pass = %v, want unchanged %v", got, createdBefore+1)
	}
}

// TestReconcileIncrementsReconcileErrorsTotal pins that a reconcile returning
// an error moves ray_bridge_reconcile_errors_total, using the stale-UID
// JobSet conflict (a real error path) rather than an artificial failure.
func TestReconcileIncrementsReconcileErrorsTotal(t *testing.T) {
	stale := failedWorkerJobSet("irrelevant")
	stale.Status.Conditions = nil // plain stale JobSet, no Failed condition
	stale.Labels[raytranslate.RayJobUIDLabel] = "OLD-uid"
	r := newReconciler(t, rayJob("shared", "batch"), stale)

	errorsBefore := testutil.ToFloat64(ReconcileErrorsTotal)
	if _, err := r.Reconcile(context.Background(), req()); err == nil {
		t.Fatalf("expected the stale-UID error")
	}
	if got := testutil.ToFloat64(ReconcileErrorsTotal); got != errorsBefore+1 {
		t.Errorf("ReconcileErrorsTotal = %v, want %v", got, errorsBefore+1)
	}

	// A clean reconcile (of a fresh, conflict-free setup) must not move it.
	r2 := newReconciler(t, rayJob("shared", "batch"))
	errorsBefore2 := testutil.ToFloat64(ReconcileErrorsTotal)
	if _, err := r2.Reconcile(context.Background(), req()); err != nil {
		t.Fatalf("clean reconcile: %v", err)
	}
	if got := testutil.ToFloat64(ReconcileErrorsTotal); got != errorsBefore2 {
		t.Errorf("ReconcileErrorsTotal after clean pass = %v, want unchanged %v", got, errorsBefore2)
	}
}
