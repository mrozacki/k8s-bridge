package bridge

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mrozacki/k8s-bridge/internal/config"
)

// newHealthTestBridge builds a bare Bridge with a short poll interval so
// grace-period/staleness math in health.go runs on a fast clock, without
// needing the fuller testBridge fixture (no kube/slurm calls happen here).
func newHealthTestBridge(t *testing.T, pollInterval time.Duration) *Bridge {
	t.Helper()
	b, _ := testBridge(t, &fakeSlurm{})
	b.setCfg(&config.Config{PollInterval: config.Duration{Duration: pollInterval}})
	return b
}

// TestHealthyBeforeFirstTick is the audit AUD2 grace-period case: a
// freshly-started bridge that has not completed a tick yet must report
// healthy (nothing has had a chance to fail).
func TestHealthyBeforeFirstTick(t *testing.T) {
	b := newHealthTestBridge(t, 10*time.Second)
	ok, msg := b.Healthy()
	if !ok {
		t.Errorf("Healthy() = false before first tick, want true; msg=%q", msg)
	}
}

// TestHealthyAfterFailingTick pins the documented liveness contract: Healthy
// only requires that the loop attempted a tick, not that it succeeded — a
// struggling slurmrestd should surface via readiness/alerts, not restarts.
func TestHealthyAfterFailingTick(t *testing.T) {
	b := newHealthTestBridge(t, 10*time.Second)
	b.recordTickOutcome(errUnavailable)
	ok, _ := b.Healthy()
	if !ok {
		t.Error("Healthy() = false after a failing tick, want true (loop is still turning)")
	}
}

// TestNotReadyBeforeFirstSuccessWithinGrace confirms the startup grace
// period: before readyGraceMultiple x poll interval has elapsed with no
// successful tick yet, readiness is false but for the "still starting"
// reason, not treated as a hard failure.
func TestNotReadyBeforeFirstSuccessWithinGrace(t *testing.T) {
	b := newHealthTestBridge(t, time.Hour) // grace period way longer than the test
	ok, msg := b.Ready()
	if ok {
		t.Fatal("Ready() = true before any tick has completed, want false")
	}
	if msg == "" {
		t.Error("expected a non-empty reason")
	}
}

// TestNotReadyPastGraceWithNoSuccess confirms readiness stays false once the
// startup grace period elapses with zero successful ticks (e.g. slurmrestd
// unreachable since boot).
func TestNotReadyPastGraceWithNoSuccess(t *testing.T) {
	b := newHealthTestBridge(t, time.Millisecond) // grace period ~3ms
	b.recordTickOutcome(errUnavailable)
	time.Sleep(20 * time.Millisecond) // safely past readyGraceMultiple x poll
	ok, _ := b.Ready()
	if ok {
		t.Error("Ready() = true past the grace period with no successful tick, want false")
	}
}

// TestReadyAfterFreshSuccess is the common-case happy path: a tick succeeded
// recently, well inside staleSuccessMultiple x poll interval.
func TestReadyAfterFreshSuccess(t *testing.T) {
	b := newHealthTestBridge(t, 10*time.Second)
	b.recordTickOutcome(nil)
	ok, _ := b.Ready()
	if !ok {
		t.Error("Ready() = false right after a successful tick, want true")
	}
}

// TestNotReadyWhenLastSuccessIsStale is the core freshness contract: a
// bridge that succeeded once but has failed every tick since, for longer
// than staleSuccessMultiple x poll interval, must go unready even though
// Healthy() stays true.
func TestNotReadyWhenLastSuccessIsStale(t *testing.T) {
	b := newHealthTestBridge(t, 5*time.Millisecond) // 3x = 15ms staleness window
	b.recordTickOutcome(nil)                        // one success, long enough ago
	time.Sleep(30 * time.Millisecond)
	b.recordTickOutcome(errUnavailable) // then it started failing
	ok, msg := b.Ready()
	if ok {
		t.Error("Ready() = true with a stale last success, want false")
	}
	if msg == "" {
		t.Error("expected a non-empty staleness reason")
	}
	if healthy, _ := b.Healthy(); !healthy {
		t.Error("Healthy() should remain true even while unready (loop is still attempting ticks)")
	}
}

// TestHealthzHandlerServesJSONOK exercises the actual HTTP handler wiring
// used by cmd/k8s-bridge/main.go's consolidated --metrics-addr mux.
func TestHealthzHandlerServesJSONOK(t *testing.T) {
	b := newHealthTestBridge(t, 10*time.Second)
	rr := httptest.NewRecorder()
	b.HealthzHandler()(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rr.Code != http.StatusOK {
		t.Errorf("/healthz status = %d, want 200", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("/healthz Content-Type = %q, want application/json", ct)
	}
}

// TestReadyzHandlerReflectsUnreadyAsServiceUnavailable is the fresh-vs-stale
// contract at the HTTP layer: readyz must answer 503 while unready so a
// Kubernetes readiness probe removes the pod from Service endpoints.
func TestReadyzHandlerReflectsUnreadyAsServiceUnavailable(t *testing.T) {
	b := newHealthTestBridge(t, time.Millisecond)
	b.recordTickOutcome(errUnavailable)
	time.Sleep(20 * time.Millisecond)

	rr := httptest.NewRecorder()
	b.ReadyzHandler()(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("/readyz status = %d, want 503 while unready", rr.Code)
	}
}

// TestReadyzHandlerServesOKWhenFresh confirms the 200 side of the same
// contract right after a successful tick.
func TestReadyzHandlerServesOKWhenFresh(t *testing.T) {
	b := newHealthTestBridge(t, 10*time.Second)
	b.recordTickOutcome(nil)

	rr := httptest.NewRecorder()
	b.ReadyzHandler()(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusOK {
		t.Errorf("/readyz status = %d, want 200 right after a successful tick", rr.Code)
	}
}

var errUnavailable = &fakeErr{"slurmrestd unavailable"}

type fakeErr struct{ s string }

func (e *fakeErr) Error() string { return e.s }
