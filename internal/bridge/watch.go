// P1 watch-nudge: the manager's cache lets the bridge watch JobSets (and,
// via kueue.go's WorkloadNudgeSource, Kueue Workloads) so a relevant event
// wakes the poll loop immediately instead of waiting for the next timer
// interval. See docs/adr/0011 for why this is a LATENCY OPTIMIZATION layered
// on top of the polling loop, not a replacement for it: slurmrestd cannot be
// watched, so the timer floor in Bridge.Run always remains the correctness
// backbone.
package bridge

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// NudgeReconciler is a minimal manager-adopted reconciler whose only job is
// to call Bridge.Nudge() when its watched source fires. It deliberately
// ignores the reconcile.Request's content (namespace/name): the poll loop's
// next tick re-derives full state from a fresh ListJobs + snapshot exactly
// as it always has, so the nudge only needs to say "something changed",
// never "which specific object". This keeps the nudge path simple and
// keeps the SAME reconcile loop as the source of truth (ADR-0011) — no
// per-object reconcile logic to keep in sync with tick()'s.
type NudgeReconciler struct {
	// Bridge receives the nudge in single-CR and file mode.
	Bridge *Bridge
	// Supervisor, when set instead of Bridge (supervisor mode, ADR-0015
	// Phase A), fans the nudge out to EVERY running per-CR Bridge: all CRs'
	// JobSets share one namespace and a reconcile.Request carries only
	// namespace/name — not the labels that would identify the owning CR —
	// so per-instance routing would cost a cache lookup per event for no
	// correctness gain. An extra tick per Bridge is coalesced (size-1
	// channel) and damped (minNudgeInterval) exactly like any other nudge,
	// so the fan-out is cheap and safe by design.
	Supervisor *Supervisor
	// Source labels the k8s_bridge_tick_trigger_total metric is not
	// incremented here (Run() owns that, since it knows definitively
	// whether the tick it is about to run was timer- or watch-triggered);
	// this field exists only for logging so a hot-reloading operator can
	// tell JobSet-driven nudges from Workload-driven ones in the log
	// stream.
	Source string
}

func (r *NudgeReconciler) Reconcile(_ context.Context, req reconcile.Request) (reconcile.Result, error) {
	if r.Supervisor != nil {
		r.Supervisor.Log.Debug("watch event, nudging all poll loops", "source", r.Source, "object", req.NamespacedName)
		r.Supervisor.NudgeAll()
		return reconcile.Result{}, nil
	}
	if r.Bridge.log != nil {
		r.Bridge.log.Debug("watch event, nudging poll loop", "source", r.Source, "object", req.NamespacedName)
	}
	r.Bridge.Nudge()
	return reconcile.Result{}, nil
}
