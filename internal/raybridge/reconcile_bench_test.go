package raybridge

import (
	"context"
	"testing"
)

// BenchmarkReconcileSteadyState measures the hot path most reconciles take:
// the worker JobSet already exists, so the reconcile is get RayJob → parse →
// scope-check → ensure-JobSet (AlreadyExists, no-op). This is the per-event
// cost under a stream of RayJob/JobSet watch events once things are running.
// Baseline history lives in test/bench-baseline.txt (benchstat gate, AUD5).
func BenchmarkReconcileSteadyState(b *testing.B) {
	r := newReconciler(b, rayJob("shared", "batch"))
	ctx := context.Background()
	// Prime: create the worker JobSet once so the loop measures steady state.
	if _, err := r.Reconcile(ctx, req()); err != nil {
		b.Fatalf("prime reconcile: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.Reconcile(ctx, req()); err != nil {
			b.Fatalf("reconcile: %v", err)
		}
	}
}
