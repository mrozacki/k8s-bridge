// Ray-bridge domain metrics — the observability floor the Slurm side already
// has in internal/metrics, applied to the pin-gate reconciler and webhook.
//
// Kept in this package (NOT internal/metrics) on purpose: the two bridges are
// separate binaries with separate metric namespaces (ray_bridge_* here,
// k8s_bridge_* there), and importing the Slurm metric set into the ray-bridge
// binary would register a dozen counters that can never move. internal/raywebhook
// imports the decision counter from here, keeping the whole ray_bridge_
// namespace defined in one file.
//
// Registration follows internal/metrics: everything goes on controller-runtime's
// own metrics.Registry, so the single /metrics endpoint (--metrics-addr) carries
// these counters alongside the CNCF-standard controller-runtime metrics
// (rest_client_*, workqueue_*, controller_runtime_reconcile_*) for free.
//
// Naming follows the same audit convention: ray_bridge_<noun>_<unit>{_total for
// counters}. Never label by RayJob or JobSet name — both are unbounded,
// per-job cardinality that would blow up the series count. The only labeled
// metric (webhook decisions) has exactly three possible label values.
package raybridge

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Webhook decision label values for WebhookDecisionsTotal. They mirror the
// three outcomes of raywebhook.Decide (plus the submit-time validation denial,
// which is folded into "denied"): the pin was injected, the RayJob was
// rejected, or the RayJob was out of scope and allowed untouched. Defined here
// so the label set is closed by construction — callers cannot invent new
// values without touching this file.
const (
	WebhookDecisionPinned  = "pinned"
	WebhookDecisionDenied  = "denied"
	WebhookDecisionSkipped = "skipped"
)

var (
	// WorkerJobSetsCreatedTotal counts worker JobSets the reconciler created —
	// including recreations after a bounded failure retry, so
	// created_total - failed_total drifting apart flags a create/fail loop.
	WorkerJobSetsCreatedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ray_bridge_worker_jobsets_created_total",
		Help: "Total number of dedicated Ray worker JobSets created for inner RayJobs (including retry recreations).",
	})

	// WorkerJobSetsFailedTotal counts every observation of a worker JobSet
	// reaching a Failed=True condition that the bridge acted on (retry delete
	// or final retries-exhausted signal). The D1 analog of the Slurm side's
	// k8s_bridge_jobs_failed_total: before this counter existed, a failed
	// worker set left its RayJob pinned forever with no metric to alert on.
	WorkerJobSetsFailedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ray_bridge_worker_jobsets_failed_total",
		Help: "Total number of worker JobSet Failed conditions the bridge handled (retried or declared exhausted).",
	})

	// ReconcileErrorsTotal counts reconcile passes that returned an error (and
	// were therefore requeued by controller-runtime). The tick_errors_total
	// analog for an event-driven controller: alert on a sustained rate, since a
	// steady error stream means some RayJob is wedged in retry.
	ReconcileErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ray_bridge_reconcile_errors_total",
		Help: "Total number of ray-bridge reconcile passes that returned an error.",
	})

	// WebhookDecisionsTotal counts admission decisions by outcome (see the
	// WebhookDecision* constants). A flat "pinned" rate with ongoing RayJob
	// submissions is the operational smoking gun for the webhook-disabled
	// bypass: inner jobs are reaching the cluster without the pin gate.
	WebhookDecisionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ray_bridge_webhook_decisions_total",
		Help: "Total number of RayJob admission decisions made by the pin-injecting webhook, by outcome.",
	}, []string{"decision"})
)

func init() {
	ctrlmetrics.Registry.MustRegister(
		WorkerJobSetsCreatedTotal,
		WorkerJobSetsFailedTotal,
		ReconcileErrorsTotal,
		WebhookDecisionsTotal,
	)
}
