package raywebhook

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/mrozacki/k8s-bridge/internal/raybridge"
)

// TestHandleCountsDecisions pins the ray_bridge_webhook_decisions_total wiring
// (B3): each of the three admission outcomes moves exactly its own label and
// no other. Counters are shared across the test binary, so assertions use
// BEFORE/AFTER deltas (same convention as internal/bridge/metrics_test.go).
func TestHandleCountsDecisions(t *testing.T) {
	h := handler(t)
	pinned := func() float64 {
		return testutil.ToFloat64(raybridge.WebhookDecisionsTotal.WithLabelValues(raybridge.WebhookDecisionPinned))
	}
	denied := func() float64 {
		return testutil.ToFloat64(raybridge.WebhookDecisionsTotal.WithLabelValues(raybridge.WebhookDecisionDenied))
	}
	skipped := func() float64 {
		return testutil.ToFloat64(raybridge.WebhookDecisionsTotal.WithLabelValues(raybridge.WebhookDecisionSkipped))
	}

	pinnedBefore, deniedBefore, skippedBefore := pinned(), denied(), skipped()

	// Inner workload, managed cluster, mapped pool → pin injected → "pinned".
	if resp := h.Handle(context.Background(), request(rayJobRaw(t, "shared", "batch", nil))); !resp.Allowed {
		t.Fatalf("pinned case unexpectedly denied: %v", resp.Result)
	}
	// Inner workload, managed cluster, unmapped pool → "denied".
	if resp := h.Handle(context.Background(), request(rayJobRaw(t, "shared", "ghost", nil))); resp.Allowed {
		t.Fatalf("denied case unexpectedly allowed")
	}
	// Standalone RayJob (no clusterSelector) → out of scope → "skipped".
	if resp := h.Handle(context.Background(), request(rayJobRaw(t, "", "batch", nil))); !resp.Allowed {
		t.Fatalf("skipped case unexpectedly denied: %v", resp.Result)
	}

	if got := pinned(); got != pinnedBefore+1 {
		t.Errorf("pinned = %v, want %v", got, pinnedBefore+1)
	}
	if got := denied(); got != deniedBefore+1 {
		t.Errorf("denied = %v, want %v", got, deniedBefore+1)
	}
	if got := skipped(); got != skippedBefore+1 {
		t.Errorf("skipped = %v, want %v", got, skippedBefore+1)
	}
}
