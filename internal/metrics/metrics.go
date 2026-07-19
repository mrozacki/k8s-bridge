// Package metrics defines the bridge's Prometheus metric set (backlog AUD2,
// observability floor). Kept as its own small package rather than importing
// prometheus/client_golang into internal/slurm or internal/bridge directly:
//   - internal/slurm is deliberately stdlib-only (see its package doc); it
//     reports request outcomes via a plain callback instead.
//   - internal/bridge instruments the tick loop through the package-level
//     functions here, keeping the dependency in one place.
//
// Naming follows the convention the observability audit specified:
// k8s_bridge_<noun>_<unit>{_total for counters}. Never label by Slurm job ID
// or JobSet name — both are unbounded, per-job cardinality that would blow up
// the metric's series count.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Registry is the registry all bridge metrics register on. ADR-0011 (Manager
// adoption): this is now controller-runtime's own metrics.Registry rather
// than a bridge-private one, so a single /metrics endpoint carries both the
// bridge's counters AND the CNCF-standard controller-runtime metrics that
// come registered on it for free (rest_client_* request metrics,
// leader_election_master_status, workqueue_* once controllers use one). The
// bridge's own tick loop is not a workqueue-driven controller, so no
// workqueue metrics are emitted by it specifically, but the watch-nudge
// reconciler added alongside the Manager does register one.
var Registry = ctrlmetrics.Registry

var (
	// TickDuration observes wall-clock time for one full reconcile tick
	// (ListJobs + snapshot + admit + cleanup). Buckets span a healthy tick
	// (tens of ms) up to a badly stuck slurmrestd call bounded by the
	// client's 30s HTTP timeout, with headroom for outliers.
	TickDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "k8s_bridge_tick_duration_seconds",
		Help:    "Duration of one bridge reconcile tick.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60, 120},
	})

	// TicksTotal counts every completed tick, success or failure — the
	// denominator for the error-tick ratio SLO in docs/operations.md.
	TicksTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "k8s_bridge_ticks_total",
		Help: "Total number of reconcile ticks completed (success or failure).",
	})

	// TickErrorsTotal counts ticks that returned an error.
	TickErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "k8s_bridge_tick_errors_total",
		Help: "Total number of reconcile ticks that failed.",
	})

	// JobSetsCreatedTotal / JobSetsDeletedTotal count JobSet lifecycle
	// events. Deliberately NOT labeled by JobSet name/job ID (cardinality).
	JobSetsCreatedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "k8s_bridge_jobsets_created_total",
		Help: "Total number of JobSets created for held Slurm jobs.",
	})
	JobSetsDeletedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "k8s_bridge_jobsets_deleted_total",
		Help: "Total number of JobSets deleted after their Slurm job finished or vanished.",
	})

	// JobSetCreateErrorsTotal counts per-job JobSet creation failures during
	// the parallel create phase (P5). One failed create is isolated to its
	// own job (logged + counted here) and must not abort the rest of the
	// tick's creates or the cleanup phase; the tick still returns an
	// aggregate error so Run()'s failure/backoff accounting sees it.
	JobSetCreateErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "k8s_bridge_jobset_create_errors_total",
		Help: "Total number of per-job JobSet creation failures (isolated: one failure does not abort other jobs in the same tick).",
	})

	// JobsFailedTotal counts Slurm jobs the bridge actively failed because
	// their owned JobSet reached a Failed condition (D1: previously such a
	// job was left pending forever with no signal).
	JobsFailedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "k8s_bridge_jobs_failed_total",
		Help: "Total number of Slurm jobs failed by the bridge because their JobSet reported a Failed condition.",
	})

	// OrphanedJobsCancelledTotal counts Slurm jobs cancelled because their
	// JobSet disappeared (D2). Deliberately distinct from JobsFailedTotal:
	// that tracks jobs failed on a JobSet Failed CONDITION, these were
	// cancelled on a JobSet's ABSENCE. Alert on any sustained rate here — it
	// can also mean the bridge has lost sight of its own JobSets.
	OrphanedJobsCancelledTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "k8s_bridge_orphaned_jobs_cancelled_total",
		Help: "Total number of Slurm jobs cancelled by the bridge because their JobSet no longer exists.",
	})

	// PreemptionDrainsTotal counts ticks on which the bridge proactively
	// deleted a preempted job's dynamic Slurm node records (ADR-0016) after
	// Kueue evicted it, rather than waiting out SlurmdTimeout. Only ticks that
	// actually removed a record increment this; repeat ticks over an
	// already-drained eviction are silent.
	PreemptionDrainsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "k8s_bridge_preemption_drains_total",
		Help: "Total number of preempted Slurm jobs whose dynamic node records the bridge proactively drained after Kueue eviction (ADR-0016).",
	})

	// HeldJobs is a gauge set once per tick to the number of held+pending
	// Slurm jobs observed — the numerator operators watch to gauge backlog.
	HeldJobs = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "k8s_bridge_held_jobs",
		Help: "Number of held, pending Slurm jobs observed in the most recent tick.",
	})

	// LastSuccessfulTickTimestamp is a unix-seconds gauge set after every
	// successful tick, so "seconds since the bridge last did useful work" is
	// simply time() - this metric in PromQL. Added because the operator
	// dashboard could not derive staleness honestly from the existing
	// counters: a stalled ticks_total counter still gets fresh scrape
	// timestamps, so its timestamp() reflects Prometheus's clock, not the
	// bridge's last success. The _timestamp_seconds suffix follows the
	// Prometheus naming convention for unix-time gauges.
	LastSuccessfulTickTimestamp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "k8s_bridge_last_successful_tick_timestamp_seconds",
		Help: "Unix timestamp of the last tick that completed without error.",
	})

	// SlurmJobsListed is a gauge set once per tick to the TOTAL number of
	// jobs returned by the slurmrestd /jobs poll — running, pending, held,
	// everything. Distinct from HeldJobs (the bridge's own admission
	// backlog): this one tracks how much queue the bridge must parse each
	// tick, the quantity that drove slurm-restapi and bridge memory during
	// the 20k-job scale run (suite E, 2026-07). Alert on sustained growth
	// here before the payload grows past what the components' memory
	// limits can absorb.
	SlurmJobsListed = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "k8s_bridge_slurm_jobs_listed",
		Help: "Total number of Slurm jobs returned by the most recent /jobs poll (all states).",
	})

	// SlurmAPIRequestsTotal counts every slurmrestd call the bridge makes,
	// labeled by HTTP method and status code — bounded cardinality (a
	// handful of methods x a handful of codes), unlike labeling by job ID.
	SlurmAPIRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "k8s_bridge_slurm_api_requests_total",
		Help: "Total number of slurmrestd API requests made by the bridge, by method and response code.",
	}, []string{"method", "code"})

	// SlurmRequestDuration observes wall-clock time per slurmrestd request,
	// from starting the request to finishing the response body (A10).
	// slurmrestd was the fragile dependency in scale testing (suite E,
	// 2026-07: memory pressure at 20k queued jobs slowed /jobs to a crawl
	// before anything failed outright), and until now the only latency
	// signal was the whole-tick TickDuration histogram, which cannot say
	// WHICH call was slow. Labeled by HTTP method only — bounded cardinality
	// (GET/POST/DELETE); never by URL/path, which embeds unbounded job IDs.
	// Buckets suit an HTTP API on the same cluster network: 5ms (healthy
	// single-job call) through 30s (the default slurmRequestTimeout ceiling
	// a wedged large-backlog /jobs read runs into).
	SlurmRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "k8s_bridge_slurm_request_duration_seconds",
		Help:    "Duration of slurmrestd HTTP requests, including reading the full response body, by method.",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
	}, []string{"method"})

	// JobSetIdentityConflictsTotal counts AlreadyExists collisions where the
	// existing `slurm-job-<id>` JobSet belongs to a DIFFERENT incarnation of
	// that job ID (A3: Slurm reuses IDs once slurmctld wraps at MaxJobId, so
	// the deterministic JobSet name alone is not an identity). The bridge
	// refuses to adopt or delete on a conflict — see bridge.ensureJobSet —
	// so any non-zero rate here means a job is parked awaiting an operator.
	JobSetIdentityConflictsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "k8s_bridge_jobset_identity_conflicts_total",
		Help: "Total number of times a new Slurm job collided with an existing JobSet created for a different incarnation of the same (reused) job ID.",
	})

	// OrphanCancellationsRefusedTotal counts ticks on which the partial-cache
	// guard (A9, guard 4 in bridge.cancelOrphanedJobs) refused to cancel any
	// orphan candidates because too large a fraction of bridge-managed jobs
	// looked orphaned at once — the signature of a partially-visible informer
	// cache rather than genuine mass orphanhood. Alert on any sustained rate:
	// either the cache/RBAC is broken, or a real mass deletion needs a human.
	OrphanCancellationsRefusedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "k8s_bridge_orphan_cancellations_refused_total",
		Help: "Total number of ticks on which orphan cancellation was refused because the orphan-candidate fraction suggested a partially visible informer cache.",
	})

	// JobReleaseLatency observes the time between a job's FIRST observed
	// held+pending tick and the tick that releases it onto its dynamic nodes
	// (AUD2 remainder). Buckets span a fast admission (sub-second, quota
	// already free) up to a multi-minute wait behind a large backlog or
	// scarce quota, matching the same order of magnitude as TickDuration's
	// upper buckets since release always happens on a tick boundary.
	JobReleaseLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "k8s_bridge_job_release_latency_seconds",
		Help:    "Time from a Slurm job first being seen held+pending to being released onto its dynamic nodes.",
		Buckets: []float64{0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800, 3600},
	})

	// TickTriggerTotal counts what caused each tick to run (P1 watch-nudge
	// observability): "timer" is the periodic poll floor, "watch" is a
	// JobSet/Workload event nudging the loop to run immediately instead of
	// waiting for the next interval. Bounded cardinality: exactly two label
	// values ever exist.
	TickTriggerTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "k8s_bridge_tick_trigger_total",
		Help: "Total number of reconcile ticks, by what triggered them (timer or watch nudge).",
	}, []string{"source"})
)

func init() {
	Registry.MustRegister(
		TickDuration,
		TicksTotal,
		TickErrorsTotal,
		JobSetsCreatedTotal,
		JobSetsDeletedTotal,
		JobSetCreateErrorsTotal,
		JobsFailedTotal,
		OrphanedJobsCancelledTotal,
		PreemptionDrainsTotal,
		HeldJobs,
		LastSuccessfulTickTimestamp,
		SlurmJobsListed,
		SlurmAPIRequestsTotal,
		SlurmRequestDuration,
		JobSetIdentityConflictsTotal,
		OrphanCancellationsRefusedTotal,
		JobReleaseLatency,
		TickTriggerTotal,
	)
}
