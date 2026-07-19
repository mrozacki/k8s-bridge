// Health/readiness surface (backlog AUD2, observability floor). The bridge
// tracks its own tick outcomes and exposes them as plain functions the HTTP
// handlers in cmd/k8s-bridge/main.go wire onto /healthz and /readyz on the
// consolidated --metrics-addr mux.
package bridge

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/mrozacki/k8s-bridge/internal/metrics"
)

// readyGraceMultiple bounds how long a fresh process is treated as "not
// ready yet" (rather than "unhealthy") before its first tick completes,
// expressed as a multiple of the poll interval — long enough to survive a
// slow first slurmrestd round trip without kubelet restarting the pod.
const readyGraceMultiple = 3

// staleSuccessMultiple is how many poll intervals may pass since the last
// SUCCESSFUL tick before readiness is withdrawn — 3x per the observability
// backlog spec, generous enough to absorb one or two backed-off retries
// without flapping the readiness probe on every transient slurmrestd hiccup.
const staleSuccessMultiple = 3

// recordTickOutcome updates the health state after every completed tick
// attempt (success or failure). Called only from Run's single goroutine, but
// guarded by healthMu since it is read concurrently by the HTTP handlers.
func (b *Bridge) recordTickOutcome(err error) {
	b.healthMu.Lock()
	defer b.healthMu.Unlock()
	b.lastTickAt = time.Now()
	b.lastTickErr = err
	if err == nil {
		b.lastSuccessAt = b.lastTickAt
		// Mirrored into a gauge so dashboards/alerts can graph staleness as
		// time() - this metric — the operator-dashboard review found that
		// "time since last good tick" was not honestly derivable from the
		// counters alone (a stalled counter's scrape timestamp is not the
		// last success time).
		metrics.LastSuccessfulTickTimestamp.Set(float64(b.lastSuccessAt.Unix()))
	}
}

// Healthy reports liveness: the reconcile loop is alive and its most recent
// tick attempt completed (Run's goroutine did not panic or wedge). This is
// deliberately permissive — it does not require SUCCESS, only that the loop
// is still turning, since a struggling slurmrestd should trigger backoff and
// alerting (docs/operations.md), not a container restart that wouldn't fix
// an external outage anyway. Before the first tick completes, the process
// counts as healthy (nothing has had a chance to fail yet).
func (b *Bridge) Healthy() (bool, string) {
	b.healthMu.RLock()
	defer b.healthMu.RUnlock()
	if b.lastTickAt.IsZero() {
		return true, "waiting for first tick"
	}
	return true, "reconcile loop running, last tick at " + b.lastTickAt.Format(time.RFC3339)
}

// Ready reports whether the bridge has recent, successful contact with
// Slurm: a grace period before the first tick, then a hard requirement that
// SOME tick succeeded within staleSuccessMultiple x the poll interval. A
// bridge stuck failing every tick (bad token, unreachable slurmrestd) goes
// unready so the Service/probe layer can signal the operator, even though it
// stays "healthy" (Run keeps trying, per design).
func (b *Bridge) Ready() (bool, string) {
	b.healthMu.RLock()
	defer b.healthMu.RUnlock()

	poll := b.cfgSnapshot().PollInterval.Duration
	if poll <= 0 {
		poll = 10 * time.Second // defensive default; config.Validate should have caught this
	}

	if b.lastSuccessAt.IsZero() {
		grace := poll * readyGraceMultiple
		if time.Since(b.startedAt) <= grace {
			return false, "waiting for first successful tick (within startup grace period)"
		}
		return false, "no successful tick yet since startup"
	}

	maxAge := poll * staleSuccessMultiple
	age := time.Since(b.lastSuccessAt)
	if age > maxAge {
		reason := "last successful tick too stale"
		if b.lastTickErr != nil {
			reason += ": " + b.lastTickErr.Error()
		}
		return false, reason
	}
	return true, "last successful tick " + age.Round(time.Millisecond).String() + " ago"
}

// HealthzHandler serves liveness: 200 while the reconcile loop is alive.
func (b *Bridge) HealthzHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		ok, msg := b.Healthy()
		writeProbeResult(w, ok, msg)
	}
}

// ReadyzHandler serves readiness: 200 only while Slurm contact is fresh.
func (b *Bridge) ReadyzHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		ok, msg := b.Ready()
		writeProbeResult(w, ok, msg)
	}
}

func writeProbeResult(w http.ResponseWriter, ok bool, msg string) {
	status := http.StatusOK
	if !ok {
		status = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": boolToStatus(ok), "message": msg})
}

func boolToStatus(ok bool) string {
	if ok {
		return "ok"
	}
	return "unavailable"
}
