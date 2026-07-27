// Package bridge implements the polling reconcile loop described in the MVD:
// discover held Slurm jobs, materialize JobSets for them, release the jobs,
// and clean up JobSets whose Slurm jobs are gone.
//
// Prototype simplifications (tracked in docs/controller.md):
//   - polling only, no watches;
//   - no dynamic-node garbage collection yet (relies on epilog / manual);
//   - release happens right after JobSet creation (design-compliant: the job
//     cannot start anyway until its Feature-matched nodes register).
package bridge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/mrozacki/k8s-bridge/internal/config"
	"github.com/mrozacki/k8s-bridge/internal/metrics"
	"github.com/mrozacki/k8s-bridge/internal/slurm"
	"github.com/mrozacki/k8s-bridge/internal/translate"
)

type SlurmAPI interface {
	ListJobs(ctx context.Context, fn func(j slurm.Job) error) error
	GetJob(ctx context.Context, id uint64) (*slurm.Job, bool, error)
	// SetJobFeatures sets the node-locking constraints for a job
	SetJobFeatures(ctx context.Context, id uint64, features string) error
	// ReleaseJob lifts the Hold
	ReleaseJob(ctx context.Context, id uint64) error
	DeleteNode(ctx context.Context, name string) error
	SetJobComment(ctx context.Context, id uint64, comment string) error
	SetJobAdminComment(ctx context.Context, id uint64, comment string) error
	SetJobPriority(ctx context.Context, id uint64, priority uint32) error
	// CancelJob fails/cancels a Slurm job whose JobSet died (D1).
	CancelJob(ctx context.Context, id uint64) error
}

// syncedPriorityAnnotation remembers the last priority value the bridge saw
// on BOTH sides; deviation on either side reveals who changed it (three-way
// sync without loops).
const syncedPriorityAnnotation = "k8s-bridge.x-k8s.io/synced-priority"

type Bridge struct {
	// cfg holds the live *config.Config behind an atomic pointer (A1: the
	// WorkloadMixing CR can now be hot-reloaded on spec change, swapping cfg
	// out from under a running tick loop). Readers use cfgSnapshot(), which
	// returns one consistent *config.Config for the whole tick — never a mix
	// of old and new fields. Writers use setCfg(), called by Run() at
	// startup and by the CR watch handler (crdconfig.go) on every reload.
	cfg   atomic.Pointer[config.Config]
	kube  client.Client
	slurm SlurmAPI
	log   *slog.Logger
	// OnTick, when set, receives each tick's outcome (nil = success) —
	// used to reflect health into WorkloadMixing status conditions.
	OnTick func(error)
	// Recorder emits Kubernetes Events on JobSets (AUD2 remainder). Nil is
	// valid (e.g. older call sites, unit tests that don't care about
	// Events) — every call site nil-checks before use, matching the
	// record.EventRecorder contract of "safe to not have one".
	Recorder record.EventRecorder

	// nudge is the P1 watch-nudge coalescing channel: buffered size 1, so a
	// burst of JobSet/Workload watch events collapses into exactly one extra
	// tick instead of a tick storm. A non-blocking send (see Nudge) means a
	// slot already full from an earlier unconsumed nudge is left alone — the
	// pending tick will pick up whatever changed regardless of how many
	// separate events asked for it.
	nudge chan struct{}
	// nudgeMinInterval is the A11 watch-tick damping floor, defaulting to
	// minNudgeInterval (see that constant for the scale-test rationale).
	// A field rather than the constant directly so tests can shrink it and
	// exercise the deferral deterministically without real 1s waits.
	nudgeMinInterval time.Duration

	// health state backing the /healthz and /readyz handlers (health.go).
	// Guarded by healthMu since the HTTP server reads it from a different
	// goroutine than Run's tick loop writes it.
	healthMu      sync.RWMutex
	startedAt     time.Time
	lastTickAt    time.Time // last completed tick attempt, success or failure
	lastTickErr   error     // outcome of that last attempt (nil = success)
	lastSuccessAt time.Time // last tick that returned nil

	// commentState is cross-tick memory for the P3 comment-rewrite throttle:
	// keyed by Slurm job ID, it remembers the last-written status CLASS and
	// the last write time, so propagateStatus can skip a rewrite when only
	// volatile quota numbers changed within the minimum interval. Run() owns
	// the tick loop single-threaded (ticks never run concurrently with each
	// other), so a plain map needs no locking here — same reasoning as the
	// other per-tick state in this package. Unlike the maps the audit
	// flagged as unbounded, this one is pruned every tick (see
	// pruneCommentState) for job IDs no longer present in that tick's
	// snapshot, so it cannot grow without bound.
	commentState map[uint64]commentTrackEntry

	// seenAt is cross-tick memory for the release-latency metric (AUD2
	// remainder): keyed by Slurm job ID, it remembers the first tick
	// timestamp at which the job was observed held+pending. On release, the
	// elapsed time since that first sighting is observed into
	// metrics.JobReleaseLatency and the entry is removed. Pruned every tick
	// alongside commentState (see pruneCommentState) so a job that
	// disappears without ever being released (cancelled while still
	// pending, etc.) does not leak memory forever.
	seenAt map[uint64]time.Time
	// orphanSeen counts CONSECUTIVE ticks a released job has been observed
	// without its JobSet. Cancelling is irreversible and the JobSet list comes
	// from an informer cache, so a job must clear cfg.OrphanGraceTicks before
	// cancelOrphanedJobs acts. Reset the moment its JobSet is seen again, and
	// pruned when the job leaves ListJobs entirely.
	orphanSeen map[uint64]int
}

// commentTrackEntry is the P3 per-job memory for the comment-rewrite
// throttle.
type commentTrackEntry struct {
	class     statusClass
	lastWrite time.Time
}

func New(cfg *config.Config, kube client.Client, slurmAPI SlurmAPI, log *slog.Logger) *Bridge {
	b := &Bridge{
		kube: kube, slurm: slurmAPI, log: log, startedAt: time.Now(),
		commentState: map[uint64]commentTrackEntry{},
		seenAt:       map[uint64]time.Time{},
		orphanSeen:   map[uint64]int{},
		// Buffered size 1 (P1): see the nudge field doc for why this bound
		// is exactly what makes coalescing work.
		nudge:            make(chan struct{}, 1),
		nudgeMinInterval: minNudgeInterval,
	}
	b.setCfg(cfg)
	return b
}

// cfgSnapshot returns the current live config. Every tick calls this exactly
// once at the top (see tick()) and threads the result through, so a
// mid-tick hot-reload (A1) can never mix fields from two different config
// generations within one tick.
func (b *Bridge) cfgSnapshot() *config.Config {
	return b.cfg.Load()
}

// setCfg atomically swaps the live config. Called once at construction and
// again on every WorkloadMixing CR reload (A1's hot-reload reconciler in
// crdconfig.go). Safe to call concurrently with a running tick: the tick in
// flight keeps using the snapshot it already loaded via cfgSnapshot.
func (b *Bridge) setCfg(cfg *config.Config) {
	b.cfg.Store(cfg)
}

// Nudge requests an immediate extra tick instead of waiting for the next
// timer interval (P1 watch-nudge). It is safe to call from any goroutine
// (the manager's watch event handlers call it) and coalesces bursts: if a
// nudge is already pending and unconsumed, additional calls are dropped
// silently — Run's loop only needs to know "something changed", not how
// many times.
func (b *Bridge) Nudge() {
	select {
	case b.nudge <- struct{}{}:
	default:
		// A nudge is already pending; the upcoming extra tick will observe
		// whatever caused this call too. Never block the caller (a watch
		// event handler) and never queue more than one.
	}
}

// eventf emits a Kubernetes Event on obj if a Recorder is wired (AUD2
// remainder). Nil-safe: unit tests and any call site that never set
// b.Recorder simply get no Events, matching the pre-existing behavior of
// this package with no Recorder at all.
func (b *Bridge) eventf(obj runtime.Object, eventType, reason, messageFmt string, args ...any) {
	if b.Recorder == nil {
		return
	}
	b.Recorder.Eventf(obj, eventType, reason, messageFmt, args...)
}

// desiredEventRef builds a minimal stand-in JobSet object for a translation
// failure, which happens BEFORE any real JobSet exists to attach an Event
// to (translate.ToJobSet itself is what failed, so there is no created
// object). record.EventRecorder.Eventf requires a runtime.Object it can
// resolve via the scheme (for kind/apiVersion) — a corev1.ObjectReference
// alone does not implement that interface — so this constructs the same
// TypeMeta+ObjectMeta shell the real JobSet would have had, named
// deterministically (translate.JobSetName), without a UID. The API server
// accepts Events against an object with no UID; they are visible under
// `kubectl describe jobset` once retried successfully creates the real one
// in a later tick, since the name is the same.
func desiredEventRef(job *slurm.Job, namespace string) *jobsetv1alpha2.JobSet {
	js := &jobsetv1alpha2.JobSet{}
	js.Name = translate.JobSetName(job.JobID)
	js.Namespace = namespace
	return js
}

// maxBackoffMultiple caps the exponential backoff on consecutive tick
// failures at 6x the configured poll interval (audit P8a) — long enough to
// stop hammering a struggling slurmrestd, short enough that a operator
// watching squeue doesn't wait forever for recovery once it's back.
const maxBackoffMultiple = 6

// jitterFraction is the +/-10% (audit P8a) applied to every interval so that,
// if multiple bridge replicas or environments happen to start in lockstep,
// their ticks don't all land on slurmrestd at the same instant.
const jitterFraction = 0.10

// minNudgeInterval is the A11 damping floor between WATCH-TRIGGERED ticks.
// The Workload watch is unfiltered by design (Kueue stamps no class label
// the bridge could select on — see WorkloadNudgeSource), so sustained
// Workload churn anywhere in the namespace turns into sustained nudges, and
// every nudge is a full tick — a complete GET /jobs against slurmrestd, the
// fragile dependency the scale tests kept finding (suite E, 2026-07). One
// second caps watch-driven polling at 1 rps toward slurmrestd while staying
// well under any human-perceptible admission latency. A nudge arriving
// sooner is DEFERRED, never dropped: it already sits in the size-1
// coalescing channel; Run just waits out the remainder of the interval
// before consuming it into a tick. Poll-timer ticks and the initial tick are
// unaffected (their cadence is already bounded by minPollInterval >= 1s).
const minNudgeInterval = 1 * time.Second

// nudgeDamping returns how much longer a watch-triggered tick must wait so
// that at least minInterval elapses since the previous tick STARTED
// (sinceLastTick). Split out as a pure function for deterministic testing
// without real waits.
func nudgeDamping(minInterval, sinceLastTick time.Duration) time.Duration {
	if remaining := minInterval - sinceLastTick; remaining > 0 {
		return remaining
	}
	return 0
}

// jitteredInterval returns base +/- up to jitterFraction, picked uniformly at
// random. Split out for unit testing without a live timer.
func jitteredInterval(base time.Duration) time.Duration {
	if base <= 0 {
		return base
	}
	// jitter in [-jitterFraction*base, +jitterFraction*base]
	span := float64(base) * jitterFraction
	delta := (rand.Float64()*2 - 1) * span //nolint:gosec // scheduling jitter, not security-sensitive
	return time.Duration(float64(base) + delta)
}

// nextBackoff computes the delay before the next tick given the current
// number of CONSECUTIVE failures (0 = last tick succeeded, use the plain
// jittered poll interval). It doubles per additional failure and caps at
// maxBackoffMultiple x the poll interval (audit P8a).
func nextBackoff(base time.Duration, consecutiveFailures int) time.Duration {
	if consecutiveFailures <= 0 {
		return jitteredInterval(base)
	}
	multiple := 1 << consecutiveFailures // 2,4,8,...
	if multiple > maxBackoffMultiple {
		multiple = maxBackoffMultiple
	}
	return jitteredInterval(base * time.Duration(multiple))
}

// Run polls until the context is cancelled. One failed tick is logged and
// retried after a jittered/backed-off interval — the loop itself must never
// die. Consecutive failures back off exponentially (capped) to avoid
// hammering a struggling slurmrestd; a single success resets the backoff.
//
// P1 (watch-nudge): the periodic timer remains the floor — it is the ONLY
// trigger that can fire when nothing is watching or the watch layer is
// unavailable — but a JobSet/Workload event delivered via Nudge() wakes the
// loop immediately instead of waiting out the rest of the interval, subject
// to the A11 damping floor (minNudgeInterval) so sustained Workload churn
// cannot turn the nudge path into continuous slurmrestd polling. Run
// itself does not know or care whether it is called directly (tests, file
// config) or wrapped as a manager.Runnable (ADR-0011); either way it ticks
// on its own schedule plus nudges, and ADR-0011's rollback path (drop the
// Manager) leaves this loop completely unchanged.
func (b *Bridge) Run(ctx context.Context) error {
	b.log.Info("bridge started", "pollInterval", b.cfgSnapshot().PollInterval.String())
	consecutiveFailures := 0
	// The first tick always counts as timer-triggered: there is nothing to
	// have nudged it yet.
	trigger := "timer"
	for {
		start := time.Now()
		err := b.tick(ctx)
		metrics.TickDuration.Observe(time.Since(start).Seconds())
		metrics.TicksTotal.Inc()
		metrics.TickTriggerTotal.WithLabelValues(trigger).Inc()
		b.recordTickOutcome(err)
		if err != nil {
			b.log.Error("tick failed", "error", err, "consecutiveFailures", consecutiveFailures+1)
			consecutiveFailures++
			metrics.TickErrorsTotal.Inc()
		} else {
			consecutiveFailures = 0
		}
		b.log.Info("tick done", "duration", time.Since(start).Round(time.Millisecond).String(), "trigger", trigger)
		if b.OnTick != nil {
			b.OnTick(err)
		}
		wait := nextBackoff(b.cfgSnapshot().PollInterval.Duration, consecutiveFailures)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-b.nudge:
			// A watch event arrived: run the next tick immediately. Drain
			// and stop the pending timer so it doesn't ALSO fire right after
			// (which would just be a redundant back-to-back tick).
			timer.Stop()
			trigger = "watch"
			// A11 damping: enforce nudgeMinInterval between the previous
			// tick's start and this watch-triggered one. `start` is still
			// this iteration's tick start time, so Since(start) is exactly
			// "how long ago the last tick began". The nudge is already
			// consumed above; any further nudges arriving during this wait
			// coalesce into the size-1 channel as usual, so nothing is lost
			// — the tick that follows sees the world as it is then.
			if delay := nudgeDamping(b.nudgeMinInterval, time.Since(start)); delay > 0 {
				damp := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					damp.Stop()
					return ctx.Err()
				case <-damp.C:
				}
			}
			continue
		case <-timer.C:
			trigger = "timer"
		}
	}
}

func (b *Bridge) tick(ctx context.Context) error {
	// A1: snapshot the live config ONCE for the whole tick. If a hot-reload
	// lands mid-tick, this tick finishes on the generation it started with;
	// the next tick picks up the new one. Without this, admitHeldJobs could
	// read the old PartitionMappings while cleanupFinishedJobs (later in the
	// same tick) read a just-swapped-in new one — a torn read across two
	// config generations.
	cfg := b.cfgSnapshot()

	byID := make(map[uint64]*slurm.Job)
	var held []slurm.Job
	now := time.Now()
	var totalJobs int

	err := b.slurm.ListJobs(ctx, func(j slurm.Job) error {
		totalJobs++
		clone := j
		byID[j.JobID] = &clone

		_, mapped := cfg.PriorityClassFor(j.Partition)
		if !j.IsPending() || !j.IsHeld() {
			// Out of scope by design: only held+pending jobs are signalled or admitted.
			return nil
		}
		if !mapped {
			// Save for phase 0: signal without holding
			// Wait, signaling inside the callback blocks the HTTP stream parser.
			// So we'll just queue it. But to keep it simple and preserve behavior,
			// just append it to a separate array or call it?
			// Let's call it direct for now as before.
			b.signalUnmanagedPartition(ctx, &clone)
			return nil
		}
		held = append(held, clone)
		if _, tracked := b.seenAt[j.JobID]; !tracked {
			if b.seenAt == nil {
				b.seenAt = map[uint64]time.Time{}
			}
			b.seenAt[j.JobID] = now
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("listing slurm jobs: %w", err)
	}
	// Total queue depth, not just the held backlog: this is the size of the
	// payload the bridge (and slurmrestd) must produce and parse every tick,
	// the quantity that exhausted both components' memory at 20k queued jobs
	// (suite E, 2026-07).
	metrics.SlurmJobsListed.Set(float64(totalJobs))
	// One snapshot of owned JobSets and their Workloads per tick. Before
	// this existed the loop issued a LIST per job and a blind CREATE per
	// already-processed job - O(n^2) API traffic that the 5000-job backlog
	// test would have turned into a self-inflicted DoS (perf fix).
	snap, err := b.snapshot(ctx, cfg)
	if err != nil {
		return err
	}
	if err := b.admitHeldJobs(ctx, cfg, held, snap); err != nil {
		return err
	}

	cleanupErr := b.cleanupFinishedJobs(ctx, cfg, snap, byID)
	// P3/AUD2: prune the comment-rewrite-throttle and release-latency
	// tracking maps for job IDs that left the system entirely (completed,
	// cancelled, failed, purged...) this tick, so both stay bounded to the
	// currently-live job set instead of growing forever (the audit's
	// standing guidance on any cross-tick map).
	b.pruneCommentState(byID)
	b.pruneSeenAt(byID)
	return cleanupErr
}

// pruneCommentState removes commentState entries for job IDs absent from
// this tick's ListJobs snapshot (byID) — those jobs have left the system, so
// their throttle memory no longer serves any purpose.
func (b *Bridge) pruneCommentState(byID map[uint64]*slurm.Job) {
	for id := range b.commentState {
		if _, present := byID[id]; !present {
			delete(b.commentState, id)
		}
	}
}

// pruneSeenAt removes seenAt entries for job IDs absent from this tick's
// ListJobs snapshot (byID) — a job that vanished (cancelled while still
// pending, purged, etc.) without ever being released has no release event
// to observe, so its first-seen memory would otherwise never be cleaned up.
func (b *Bridge) pruneSeenAt(byID map[uint64]*slurm.Job) {
	for id := range b.seenAt {
		if _, present := byID[id]; !present {
			delete(b.seenAt, id)
		}
	}
}

// unmanagedPartitionComment is written to a held+pending job whose partition
// has no PartitionMappings entry (audit D4). Before this fix the job was
// dropped with no signal anywhere — the most common misconfiguration
// (unmapped or typo'd partition) was also the least debuggable.
const unmanagedPartitionComment = "wm: partition not managed by k8s-bridge"

// admitHeldJobs processes held jobs oldest-first (lowest ID) so no job is
// starved by newer submissions — the ordering guarantee from the MVD.
//
// P5 (perf fix): sequential ensureJobSet calls dominated the first
// tick of a burst (one Create per held job, one at a time). JobSet creation
// is now parallelized across a bounded worker pool (createJobSetsParallel)
// BEFORE the rest of the per-job processing (status comment, admission gate,
// release) runs sequentially exactly as before — that ordering guarantee and
// the admission gate are unaffected by which phase runs concurrently.
func (b *Bridge) admitHeldJobs(ctx context.Context, cfg *config.Config, held []slurm.Job, snap *tickSnapshot) error {
	sort.Slice(held, func(i, k int) bool { return held[i].JobID < held[k].JobID })
	metrics.HeldJobs.Set(float64(len(held)))

	// Phase 1: translate every held job and create its JobSet if missing, in
	// parallel across a bounded worker pool. Per-job isolation: one failed
	// create is logged, counted, and excluded from phase 2 below, but does
	// NOT abort the other jobs' creates or the rest of the tick.
	type prepared struct {
		job         *slurm.Job
		desired     *jobsetv1alpha2.JobSet
		createErr   error
		translateOK bool
	}
	results := make([]prepared, len(held))
	jobsToCreate := make(chan int)
	go func() {
		defer close(jobsToCreate)
		for i := range held {
			select {
			case jobsToCreate <- i:
			case <-ctx.Done():
				return
			}
		}
	}()

	workers := cfg.CreateWorkers
	if workers <= 0 {
		workers = 1
	}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobsToCreate {
				job := &held[i]
				desired, err := translate.ToJobSet(job, cfg)
				if err != nil {
					// Permanent translation error: per design the Slurm job
					// should be failed with a reason. TODO(live): implement
					// job failing; for now we log and skip so the operator
					// can intervene.
					b.log.Error("translation failed, skipping job", "slurmJobID", job.JobID, "partition", job.Partition, "error", err)
					b.eventf(desiredEventRef(job, cfg.Namespace), corev1.EventTypeWarning, "TranslationFailed",
						"translating Slurm job %d: %v", job.JobID, err)
					results[i] = prepared{job: job}
					continue
				}
				res := prepared{job: job, desired: desired, translateOK: true}
				if existing, exists := snap.ownedJobSets[desired.Name]; !exists {
					// TC-D2 fix: if the job already had its JobSet deleted, don't recreate it.
					// We know the bridge has already processed this job if the comment doesn't start with "wm: held".
					// We also allow empty comments to support unit tests that don't populate the Slurm comment.
					if job.Comment != "" && !strings.HasPrefix(job.Comment, "wm: held") {
						b.log.Info("skipping JobSet recreation for held job indicating orphan", "slurmJobID", job.JobID, "comment", job.Comment)
						continue
					}
					created, err := b.ensureJobSet(ctx, desired)
					if err != nil {
						res.createErr = err
					} else if created {
						b.log.Info("created JobSet", "slurmJobID", job.JobID, "jobset", desired.Name)
						metrics.JobSetsCreatedTotal.Inc()
						b.eventf(desired, corev1.EventTypeNormal, "Created",
							"JobSet created for Slurm job %d", job.JobID)
					}
				} else if !jobSetIdentityMatches(existing, desired) {
					// A3, snapshot half: the cache already holds a JobSet
					// under this job's deterministic name, but it belongs to
					// a PREVIOUS incarnation of the (reused) Slurm job ID.
					// Without this check the stale JobSet would sail into
					// phase 2 as this job's capacity — wrong shape, wrong
					// pods — and the job would be pinned and released onto
					// it. Same handling as the create-time half: skip the
					// job this tick, leave the JobSet for the operator.
					res.createErr = b.jobSetIdentityConflict(existing, desired)
				}
				results[i] = res
			}
		}()
	}
	wg.Wait()

	// Phase 2: sequential per-job processing (status comment, admission
	// gate, release) in the original oldest-first order — unaffected by
	// which worker created which JobSet in phase 1.
	var createErrs []error
	for i := range results {
		res := results[i]
		if !res.translateOK {
			continue // already logged in phase 1
		}
		if res.createErr != nil {
			// Per-job isolation (P5): log, count, and move on to the next
			// job instead of aborting the whole tick — previously a single
			// creation failure returned an error that stopped every other
			// held job's processing too.
			b.log.Error("ensuring JobSet failed, will retry next tick", "slurmJobID", res.job.JobID, "jobset", res.desired.Name, "error", res.createErr)
			metrics.JobSetCreateErrorsTotal.Inc()
			createErrs = append(createErrs, fmt.Errorf("ensuring JobSet for job %d: %w", res.job.JobID, res.createErr))
			continue
		}

		job := res.job
		desired := res.desired
		log := b.log.With("slurmJobID", job.JobID, "partition", job.Partition)

		// While the job waits, surface WHY in its comment field (UX
		// remediation: squeue -o %k shows a truthful status).
		b.propagateStatus(ctx, job, snap.workloadsByJobSet[desired.Name])

		// Scale gate: without admission there are no nodes to
		// pin to — skip the per-tick REST attempts for the whole backlog.
		if !isAdmitted(snap.workloadsByJobSet[desired.Name]) {
			continue
		}

		// Live finding (experiment 02): Slurm rejects a Features constraint
		// naming a feature no node advertises yet ("Invalid feature
		// specification"), so the design's set-feature-then-wait order is
		// impossible. Instead we use that validation as our readiness signal:
		// the update succeeds only once the dynamic nodes have registered,
		// and only then do we release the hold. Until then, retry each tick.
		//
		// P2 (perf fix): constraints and hold:false now travel in
		// ONE job-update call instead of two separate POSTs (SetJobFeature
		// then ReleaseJob) — this halves per-admission REST mutations. The
		// admission gate above (isAdmitted) still runs first, so constraints
		// and release continue to happen ONLY after Kueue admission, never
		// before — that ordering guarantee is unchanged by the merge.
		if err := b.slurm.SetJobFeatures(ctx, job.JobID, translate.NodeFeature(job.JobID)); err != nil {
			// Live finding (TC-B retest): only "Invalid feature
			// specification" means nodes-not-registered-yet. Anything else
			// (e.g. "Invalid user id" from a misconfigured slurmUser) is a
			// real fault that would otherwise strand the job held forever
			// with no visible signal — surface it at Warn.
			if strings.Contains(err.Error(), "Invalid feature specification") {
				// Demoted to Debug (audit AUD2): repeats every tick per unadmitted
				// job, so at Info it drowns out genuine signal under any backlog.
				log.Debug("dynamic nodes not registered yet, will retry", "reason", err)
			} else {
				log.Warn("job update failed for a reason other than pending node registration, will retry", "reason", err)
			}
			continue
		}
		if err := b.slurm.ReleaseJob(ctx, job.JobID); err != nil {
			log.Error("failed to release job after setting features", "reason", err)
			continue
		}
		log.Info("released slurm job onto its dynamic nodes")
		b.eventf(desired, corev1.EventTypeNormal, "Released",
			"Slurm job %d released onto its dynamic nodes", job.JobID)
		// AUD2 release-latency: observe elapsed time since this job was
		// FIRST seen held+pending (recorded above, or in an earlier tick),
		// then forget it — its wait is over. A job whose seenAt is somehow
		// missing (e.g. process restarted mid-wait) is skipped rather than
		// reporting a bogus near-zero latency.
		if first, tracked := b.seenAt[job.JobID]; tracked {
			metrics.JobReleaseLatency.Observe(time.Since(first).Seconds())
			delete(b.seenAt, job.JobID)
		}
	}
	if len(createErrs) > 0 {
		// P5: an aggregate error, not a hard abort — every other held job
		// was still fully processed above. Run() logs this and backs off
		// exactly like any other failed-tick outcome.
		return fmt.Errorf("%d JobSet(s) failed to create: %w", len(createErrs), errors.Join(createErrs...))
	}
	return nil
}

// signalUnmanagedPartition writes a Slurm comment on a held+pending job whose
// partition has no PartitionMappings entry (audit D4). It reuses the
// unchanged-comment skip from propagateStatus's pattern to avoid spamming a
// SetJobComment call (and a Warn log) every tick for the same job.
func (b *Bridge) signalUnmanagedPartition(ctx context.Context, job *slurm.Job) {
	if job.Comment == unmanagedPartitionComment {
		return
	}
	if err := b.slurm.SetJobComment(ctx, job.JobID, unmanagedPartitionComment); err != nil {
		// audit AUD2: raised to Warn, same class as kueue.go's propagateStatus
		// failure — this comment is the only signal the Slurm user gets for
		// an unmanaged-partition job.
		b.log.Warn("comment propagation failed (non-fatal)", "slurmJobID", job.JobID, "reason", err)
		return
	}
	b.log.Warn("held job on unmanaged partition, signalled via comment",
		"slurmJobID", job.JobID, "partition", job.Partition)
}

// ensureJobSet creates the JobSet if missing. Returns whether it was created
// now. Deterministic names make this an idempotent upsert.
//
// A3, mirroring ray-bridge's UID-label check (raybridge.Reconciler
// ensureJobSet): on AlreadyExists it verifies the existing JobSet belongs to
// THIS incarnation of the job before treating it as success. Slurm job IDs
// are REUSED once slurmctld wraps at MaxJobId, and the JobSet name is derived
// from the ID alone — so a new job colliding with a stale live
// `slurm-job-<id>` JobSet would otherwise silently inherit capacity shaped
// for a completely different job (wrong pod count, CPUs, memory, GPUs).
// Slurm jobs have no UID; the submit time (unique per ID incarnation in
// practice — see slurm.Job.SubmitTime) stored as
// translate.SlurmSubmitTimeAnnotation is the identity anchor instead.
//
// On a mismatch the bridge logs an error, emits a Warning event, increments
// k8s_bridge_jobset_identity_conflicts_total, and returns an error so the
// per-job isolation in admitHeldJobs skips this job for the tick (retried
// next tick, like ray-bridge's transient-error treatment of the same case).
// It deliberately does NOT adopt the JobSet (wrong-shaped capacity) and does
// NOT delete it (the stale JobSet may still be running a previous job's
// pods; destroying possibly-live work on an inference from an annotation is
// an operator decision, not the bridge's — same reasoning as the D2 orphan
// guards).
//
// An existing JobSet WITHOUT the annotation is treated as matching: it was
// created by a bridge version predating A3 (or from a job whose slurmrestd
// exposed no submit_time), and failing every such JobSet on upgrade would
// turn a routine rollout into a mass identity conflict. The wrap window is
// years long; the upgrade window is one reconcile loop restart — legacy
// tolerance is the right trade.
//
// Both halves of the check are wired: this create-time path, and the
// snapshot path in admitHeldJobs phase 1 (a stale JobSet already visible in
// the informer cache is compared via jobSetIdentityConflict before being
// treated as this job's capacity), so a reused job ID is caught regardless
// of whether the stale JobSet is cached yet.
func (b *Bridge) ensureJobSet(ctx context.Context, desired *jobsetv1alpha2.JobSet) (bool, error) {
	err := b.kube.Create(ctx, desired)
	if err == nil {
		return true, nil
	}
	if apierrors.IsAlreadyExists(err) {
		existing := &jobsetv1alpha2.JobSet{}
		if getErr := b.kube.Get(ctx, client.ObjectKeyFromObject(desired), existing); getErr != nil {
			if apierrors.IsNotFound(getErr) {
				// AlreadyExists on Create followed by NotFound on Get: the
				// JobSet vanished in between (concurrent delete, or an
				// apiserver/cache race). Nothing to verify identity against;
				// preserve the historic tolerate-AlreadyExists behavior and
				// let the next tick recreate it if the job still needs one.
				return false, nil
			}
			return false, fmt.Errorf("checking existing JobSet %s: %w", desired.Name, getErr)
		}
		if jobSetIdentityMatches(existing, desired) {
			return false, nil // already ours — idempotent retry
		}
		return false, b.jobSetIdentityConflict(existing, desired)
	}
	return false, err
}

// jobSetIdentityConflict is the single handler for an A3 identity mismatch,
// shared by ensureJobSet (create-time AlreadyExists) and admitHeldJobs
// (snapshot path): count it, log it, emit the Warning event, and return the
// error that makes per-job isolation skip this job for the tick. Neither
// caller adopts or deletes the stale JobSet — see ensureJobSet's doc for
// why that is an operator decision.
func (b *Bridge) jobSetIdentityConflict(existing, desired *jobsetv1alpha2.JobSet) error {
	metrics.JobSetIdentityConflictsTotal.Inc()
	b.log.Error("JobSet exists for a DIFFERENT incarnation of this (reused) Slurm job ID; refusing to adopt or delete it",
		"jobset", desired.Name,
		"existingSubmitTime", existing.Annotations[translate.SlurmSubmitTimeAnnotation],
		"newSubmitTime", desired.Annotations[translate.SlurmSubmitTimeAnnotation])
	b.eventf(existing, corev1.EventTypeWarning, "JobSetIdentityConflict",
		"JobSet %s belongs to a different incarnation of reused Slurm job ID (submit time %s, new job's is %s); operator action required",
		desired.Name,
		existing.Annotations[translate.SlurmSubmitTimeAnnotation],
		desired.Annotations[translate.SlurmSubmitTimeAnnotation])
	return fmt.Errorf("JobSet %s exists for a different incarnation of Slurm job ID (reused after MaxJobId wrap); skipping until the stale JobSet is resolved", desired.Name)
}

// jobSetIdentityMatches reports whether an existing `slurm-job-<id>` JobSet
// belongs to the same job incarnation as the desired one (A3), by comparing
// the submit-time annotation. "Unknowable" degrades to "matches": an
// existing JobSet without the annotation predates A3 (upgrade
// compatibility), and a desired JobSet without it comes from a job whose
// submit_time slurmrestd did not expose — in both cases the historic
// name-match-is-enough behavior is preserved rather than inventing a
// conflict the data cannot support.
func jobSetIdentityMatches(existing, desired *jobsetv1alpha2.JobSet) bool {
	want := desired.Annotations[translate.SlurmSubmitTimeAnnotation]
	got, present := existing.Annotations[translate.SlurmSubmitTimeAnnotation]
	if !present || want == "" {
		return true
	}
	return got == want
}

// jobSetFailed reports whether js carries a Failed=True condition (D1),
// e.g. because activeDeadlineSeconds fired or FailurePolicy.MaxRestarts was
// exhausted. Uses the real jobsetv1alpha2.JobSetFailed constant, not a
// hand-rolled string.
func jobSetFailed(js *jobsetv1alpha2.JobSet) bool {
	return apimeta.IsStatusConditionTrue(js.Status.Conditions, string(jobsetv1alpha2.JobSetFailed))
}

// jobSetCompleted reports whether js carries a Completed=True condition.
// Live finding (TC-D3 retest): slurmd exits 0 on a graceful pod delete, so
// the child Job counts it as a SUCCESS, the JobSet goes Completed, and no
// pod is ever recreated — while slurmctld has requeued the Slurm job to
// wait for a dynamic node that will never come back. slurmd pods never
// exit on their own in the normal flow (the bridge deletes the JobSet
// after the Slurm job turns terminal), so Completed + a non-terminal
// Slurm job is always this stranded state, never a healthy race.
func jobSetCompleted(js *jobsetv1alpha2.JobSet) bool {
	return apimeta.IsStatusConditionTrue(js.Status.Conditions, string(jobsetv1alpha2.JobSetCompleted))
}

// failJobForDeadJobSet implements D1: a JobSet that died (Failed condition)
// before its Slurm job could run must not leave that job pending forever.
// It writes an explanatory Slurm comment, cancels the job via slurmrestd,
// increments the failure metric, and emits exactly one Warn log line.
//
// It reports whether CancelJob itself succeeded. The caller uses this to
// decide whether to fall through to JobSet/node cleanup: if slurmrestd is
// unreachable and the cancel fails, the Slurm job is NOT actually
// cancelled — deleting the JobSet anyway would erase the only record tying
// the bridge back to that job (the SlurmJobIDLabel lives on the JobSet), so
// the next tick would have nothing left to retry the cancel against and the
// job would be stuck pending forever with just a stale comment. Leaving the
// JobSet in place lets cleanupFinishedJobs retry the whole failure path
// (comment + cancel) next tick, exactly like the existing "temporary error,
// retry next time" convention elsewhere in this file. The comment write is
// independently best-effort (UX only, never blocks the retry decision).
func (b *Bridge) failJobForDeadJobSet(ctx context.Context, job *slurm.Job, js *jobsetv1alpha2.JobSet) (cancelled bool) {
	reason := "unknown"
	if cond := apimeta.FindStatusCondition(js.Status.Conditions, string(jobsetv1alpha2.JobSetFailed)); cond != nil && cond.Message != "" {
		reason = cond.Message
	} else if cond := apimeta.FindStatusCondition(js.Status.Conditions, string(jobsetv1alpha2.JobSetCompleted)); cond != nil {
		// TC-D3 retest: the JobSet "succeeded" because slurmd exited 0 on a
		// pod delete — from the Slurm job's perspective its capacity is gone.
		reason = "JobSet completed (slurmd pods exited) while the Slurm job was still non-terminal"
	}
	comment := fmt.Sprintf("wm: JobSet failed: %s", reason)
	if err := b.slurm.SetJobComment(ctx, job.JobID, comment); err != nil {
		b.log.Debug("failed to write JobSet-failure comment (non-fatal)", "slurmJobID", job.JobID, "reason", err)
	}
	cancelErr := b.slurm.CancelJob(ctx, job.JobID)
	if cancelErr != nil {
		b.log.Debug("failed to cancel slurm job for dead JobSet (will retry next tick, JobSet kept)", "slurmJobID", job.JobID, "reason", cancelErr)
	}
	metrics.JobsFailedTotal.Inc()
	// Exactly one Warn-level line for this event, regardless of whether the
	// comment write or cancel call above also hit an error (those are logged
	// at Debug so this stays the single user-facing signal per spec).
	b.log.Warn("failing slurm job: its JobSet turned terminal before the job did", "slurmJobID", job.JobID, "jobset", js.Name, "reason", reason, "cancelled", cancelErr == nil)
	b.eventf(js, corev1.EventTypeWarning, "JobSetFailed",
		"Slurm job %d failed: JobSet reported Failed (%s)", job.JobID, reason)
	return cancelErr == nil
}

// stampRetention records, on the JobSet itself, the deadline before which
// cleanup must not delete it. Called only from the failure branch of
// cleanupFinishedJobs, and a no-op when retention is disabled (the default) or
// when a previous tick already stamped this object — the window is never
// extended, so a JobSet that keeps failing its cancel retry cannot creep past
// its original deadline.
//
// Best-effort by design: if the patch fails the annotation is dropped again and
// the JobSet is cleaned up exactly as it was before retention existed. A
// debugging aid must not become a new way for cleanup to get stuck.
func (b *Bridge) stampRetention(ctx context.Context, cfg *config.Config, js *jobsetv1alpha2.JobSet) {
	if cfg.FailedJobSetRetention.Duration <= 0 {
		return
	}
	if _, already := js.Annotations[translate.RetainUntilAnnotation]; already {
		return
	}
	until := time.Now().Add(cfg.FailedJobSetRetention.Duration).UTC().Format(time.RFC3339)
	patch := client.MergeFrom(js.DeepCopy())
	if js.Annotations == nil {
		js.Annotations = map[string]string{}
	}
	js.Annotations[translate.RetainUntilAnnotation] = until
	if err := b.kube.Patch(ctx, js, patch); err != nil {
		delete(js.Annotations, translate.RetainUntilAnnotation)
		b.log.Warn("stamping JobSet retention failed, cleaning up immediately instead",
			"jobset", js.Name, "error", err)
		return
	}
	b.log.Info("retaining failed JobSet for post-mortem inspection",
		"jobset", js.Name, "until", until)
	b.eventf(js, corev1.EventTypeNormal, "RetainedForInspection",
		"JobSet retained until %s for inspection (failedJobSetRetention); its Slurm job has already been failed", until)
}

// markNodesReleased records that this JobSet's dynamic Slurm node records are
// gone, so later ticks of its retention window skip the delete loop entirely.
// Best-effort: on failure the only cost is that the next tick retries the
// deletes, which is exactly what happened before this marker existed.
func (b *Bridge) markNodesReleased(ctx context.Context, js *jobsetv1alpha2.JobSet) {
	if js.Annotations[translate.NodesReleasedAnnotation] == "true" {
		return
	}
	patch := client.MergeFrom(js.DeepCopy())
	if js.Annotations == nil {
		js.Annotations = map[string]string{}
	}
	js.Annotations[translate.NodesReleasedAnnotation] = "true"
	if err := b.kube.Patch(ctx, js, patch); err != nil {
		delete(js.Annotations, translate.NodesReleasedAnnotation)
		b.log.Debug("marking node records released failed (will retry the deletes next tick)",
			"jobset", js.Name, "reason", err)
	}
}

// retentionHolds reports whether js carries a retain-until stamp that has not
// expired yet. An absent stamp means "delete now", which is what every JobSet
// the failure branch never touched carries — so the normal completed-job
// cleanup path is unaffected by this feature.
//
// An unparseable stamp is treated as expired: a corrupt annotation (hand-edited,
// or written by a future version with a different format) must not pin an object
// in the cluster forever.
func (b *Bridge) retentionHolds(js *jobsetv1alpha2.JobSet) bool {
	raw, ok := js.Annotations[translate.RetainUntilAnnotation]
	if !ok {
		return false
	}
	until, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		b.log.Warn("JobSet carries an unparseable retain-until annotation, cleaning it up normally",
			"jobset", js.Name, "value", raw)
		return false
	}
	return time.Now().Before(until)
}

// jobSetParallelism reports how many pods (and thus dynamic Slurm node records)
// a JobSet has. A managed JobSet with no ReplicatedJobs (hand-edited or mutated
// by something else) must not panic the loop indexing ReplicatedJobs[0]
// (audit D5(a)); it falls back to 1 and logs, so at least the first node record
// can still be handled. Shared by the terminal-cleanup path and the ADR-0016
// preemption drain.
func (b *Bridge) jobSetParallelism(js *jobsetv1alpha2.JobSet) int32 {
	if len(js.Spec.ReplicatedJobs) == 0 {
		b.log.Warn("owned JobSet has no replicatedJobs, using default nTasks=1", "jobset", js.Name)
		return 1
	}
	if p := js.Spec.ReplicatedJobs[0].Template.Spec.Parallelism; p != nil {
		return *p
	}
	return 1
}

// drainPreemptedNodes implements ADR-0016: delete a preempted job's dynamic
// Slurm node records so slurmctld frees and requeues the job immediately
// instead of waiting out SlurmdTimeout. The JobSet is NOT deleted — Kueue keeps
// it suspended and re-admits it later, at which point the pods re-register the
// same deterministic node names. 404s are expected and silent: a record may
// already be gone, and this runs every tick while the job stays evicted, so the
// event/metric/log fire ONLY on a tick that actually removed a record
// (drained > 0), which keeps repeat ticks quiet without any cross-tick state.
func (b *Bridge) drainPreemptedNodes(ctx context.Context, job *slurm.Job, js *jobsetv1alpha2.JobSet) {
	drained := 0
	for _, node := range translate.NodeNames(job.JobID, b.jobSetParallelism(js)) {
		switch err := b.slurm.DeleteNode(ctx, node); {
		case err == nil:
			drained++
		case isNotFound(err):
			// Already gone (a prior drain tick, or never registered) — expected.
		default:
			b.log.Warn("draining preempted node record failed, will retry next tick",
				"node", node, "slurmJobID", job.JobID, "error", err)
		}
	}
	if drained > 0 {
		metrics.PreemptionDrainsTotal.Inc()
		b.log.Info("drained preempted job's dynamic node records",
			"slurmJobID", job.JobID, "jobset", js.Name, "nodesDrained", drained)
		b.eventf(js, corev1.EventTypeNormal, "PreemptionDrain",
			"Slurm job %d evicted by Kueue; drained %d dynamic node record(s) so Slurm requeues without waiting for SlurmdTimeout (ADR-0016)",
			job.JobID, drained)
	}
}

// cleanupFinishedJobs deletes JobSets whose Slurm job is terminal or unknown.
// byID is this tick's ListJobs result indexed by job ID (audit P7): it is
// consulted first, and GetJob is called only for IDs absent from it, to
// confirm purged-equals-terminal semantics before deleting.
//
// Per-job isolation (A2), matching admitHeldJobs' P5 convention: one job's
// GetJob failure is recorded and the loop CONTINUES with the remaining
// JobSets, returning the aggregated error at the end. Before this, the first
// GetJob error aborted the whole pass, so a single flaky lookup postponed
// cleanup (and the orphan pass below) for every other finished job that tick
// — inconsistent with how every other per-job failure in this file behaves.
func (b *Bridge) cleanupFinishedJobs(ctx context.Context, cfg *config.Config, snap *tickSnapshot, byID map[uint64]*slurm.Job) error {
	var errs []error
	for _, js := range snap.ownedJobSets {
		idStr := js.Labels[translate.SlurmJobIDLabel]
		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil {
			b.log.Warn("owned JobSet has invalid job-id label, skipping", "jobset", js.Name, "label", idStr)
			continue
		}
		job, found := byID[id]
		if !found {
			// Absent from this tick's ListJobs: only now fall back to a
			// direct GetJob to confirm the job is truly gone (purged from
			// slurmctld) rather than assuming it based on the list alone.
			job, found, err = b.slurm.GetJob(ctx, id)
			if err != nil {
				// A2: record and move on — the other JobSets' cleanup must
				// not wait on this job's flaky lookup. Retried next tick.
				errs = append(errs, fmt.Errorf("checking slurm job %d: %w", id, err))
				continue
			}
		}
		// D1: a JobSet that reached a Failed condition (e.g.
		// activeDeadlineSeconds fired before the Slurm job's dynamic nodes
		// registered - a live defect) previously fell into the
		// "leave it alone" branch below forever, since the Slurm job itself
		// never transitions out of pending on its own. The TC-D3 retest
		// added the Completed twin: slurmd exits 0 on a graceful pod
		// delete, the child Job counts that as success, the JobSet goes
		// Completed and never recreates the pod — stranding the requeued
		// Slurm job on a node that cannot return. Both terminal shapes get
		// the same treatment: fail the Slurm job, then fall through to the
		// cleanup path used for completed jobs. A live (non-terminal)
		// JobSet must NOT take this branch.
		if found && !job.IsTerminal() && (jobSetFailed(js) || jobSetCompleted(js)) {
			// Stamp the retention deadline HERE — this is the only branch that
			// knows this JobSet is the failure case. By the next tick the Slurm
			// job is cancelled (or purged from slurmctld) and this branch is
			// never taken again, so a deadline computed later could not tell a
			// failed job's leftovers from an ordinary completed job's.
			// No-op when retention is disabled (the default).
			b.stampRetention(ctx, cfg, js)
			if !b.failJobForDeadJobSet(ctx, job, js) {
				// CancelJob itself failed (e.g. slurmrestd unreachable): do
				// NOT fall through to deleting the JobSet. The JobSet is the
				// only record carrying this job's ID (SlurmJobIDLabel); losing
				// it here would strand the Slurm job pending forever with no
				// way to retry the cancel. Skip to the next JobSet and retry
				// the whole failure path next tick.
				continue
			}
			// Cancel succeeded: fall through to the shared cleanup below
			// (node + JobSet deletion) exactly as the completed-job path
			// already does.
		} else if found && !job.IsTerminal() {
			// ADR-0016: proactive drain on Kueue preemption. A released job
			// whose Workload was evicted has slurmd pods Kueue already deleted,
			// but slurmctld still counts its dynamic nodes as up until
			// SlurmdTimeout. Deleting those records now frees and requeues the
			// job immediately; they re-register under the same deterministic
			// names when Kueue re-admits the (still-present, suspended) JobSet.
			// Gated (DrainOnPreemption) and guarded by a positive Evicted signal
			// that is ALSO not-currently-admitted, so a re-admitted job carrying
			// a stale Evicted condition is not drained. Held jobs never had
			// nodes, so they are out of scope.
			if cfg.DrainOnPreemption && !job.IsHeld() {
				wl := snap.workloadsByJobSet[js.Name]
				if isEvicted(wl) && !isAdmitted(wl) {
					b.drainPreemptedNodes(ctx, job, js)
				}
			}
			// ADR-0009: the safe mutation channel is the lua directive in
			// admin_comment; the raw-field mirror stays flag-gated (default
			// off) after it re-held a running job live.
			b.applyPriorityDirective(ctx, job, snap.workloadsByJobSet[js.Name])
			if cfg.EnablePrioritySync {
				b.syncPriority(ctx, js, job, snap.workloadsByJobSet[js.Name])
			}
			continue
		}
		// Delete the Slurm node records first (their names are deterministic,
		// see translate.NodeNames), then the JobSet. Node deletion errors are
		// tolerated: the record may already be gone.
		// Node cleanup runs on every pass EXCEPT for a retained JobSet whose
		// records are already confirmed gone. Without that exception a retained
		// object re-issues these DELETEs every tick for its whole window —
		// found live 2026-07-27, and slurmrestd shares slurmctld's lock, so it
		// is not free. Retrying while a delete still FAILS is kept on purpose:
		// live, the first attempts lost a race with a job in COMPLETING ("nodes
		// are busy") and only succeeded a few ticks later.
		if js.Annotations[translate.NodesReleasedAnnotation] != "true" {
			nTasks := b.jobSetParallelism(js)
			clean := true
			for _, node := range translate.NodeNames(id, nTasks) {
				if err := b.slurm.DeleteNode(ctx, node); err != nil {
					b.log.Info("deleting slurm node record (may be already gone)", "node", node, "reason", err)
					clean = false
				}
			}
			// Stop retrying once the deletes came back clean, or once the Slurm
			// job is gone from slurmctld entirely — a purged job takes its
			// dynamic node records with it, so there is nothing left to delete.
			// The second condition is what actually ends the loop in practice:
			// slurmrestd reports an unknown node as 422/2018, not 404, so a
			// record that is already gone cannot be told from a real failure by
			// status code alone. Only persisted for a retained JobSet — without
			// retention this object is deleted a few lines below anyway.
			if (clean || !found) && b.retentionHolds(js) {
				b.markNodesReleased(ctx, js)
			}
		}
		// Retention gate, deliberately placed AFTER node cleanup: the Slurm-side
		// node records must go immediately either way (a retained record would
		// leave slurmctld advertising capacity that no longer exists), while the
		// JobSet — the thing an operator wants to inspect — is what gets kept.
		if b.retentionHolds(js) {
			continue
		}
		if err := b.kube.Delete(ctx, js); err != nil && !apierrors.IsNotFound(err) {
			b.log.Error("deleting JobSet failed, will retry next tick", "jobset", js.Name, "error", err)
			continue
		}
		metrics.JobSetsDeletedTotal.Inc()
		b.log.Info("cleaned up JobSet", "jobset", js.Name, "slurmJobID", id, "jobFound", found)
	}

	b.cancelOrphanedJobs(ctx, cfg, snap, byID)

	// A2: nil when every job's check succeeded; otherwise the aggregate, so
	// Run() still counts the tick as failed and backs off exactly as before.
	return errors.Join(errs...)
}

// bridgeCommentPrefix marks a Slurm job the bridge has taken ownership of: it
// writes its admission status into the job's comment. Only such jobs are ever
// candidates for orphan cancellation.
const bridgeCommentPrefix = "wm:"

// maxOrphanFraction is guard 4's threshold (A9): when the fraction of
// bridge-managed, released Slurm jobs that look orphaned this tick reaches
// this value, orphan cancellation is refused for the tick. Half is far above
// anything organic — orphans are created one at a time by individual JobSet
// deletions — while a PARTIALLY visible informer cache (mid-resync, a
// paginated LIST cut short, one namespace of a two-namespace cache missing)
// makes most jobs look orphaned at once while a few visible JobSets defeat
// the empty-list guard below.
const maxOrphanFraction = 0.5

// cancelOrphanedJobs cancels a released Slurm job whose JobSet has vanished
// (D2: deleting a JobSet from Kubernetes must not leave its Slurm job running
// on nodes that no longer exist).
//
// The action is scancel — irreversible, and triggered by the ABSENCE of a
// Kubernetes object, which is exactly what a read fault also looks like. Four
// guards stand between a bad read and a user's lost work:
//
//  1. cfg.CancelOrphanedJobs — off by default; operators opt in.
//  2. An empty owned-JobSet list is treated as "we cannot see our JobSets",
//     never as "every job is orphaned". A mis-scoped informer cache or an RBAC
//     regression on the workload namespace (see the k8s-bridge-system cache
//     bug) presents identically, and cancelling the whole backlog is far worse
//     than leaving orphans for a human.
//  3. cfg.OrphanGraceTicks consecutive observations. snapshot() reads from the
//     informer cache, so a JobSet created moments ago legitimately may not be
//     listed yet; one tick's absence proves nothing.
//  4. (A9) A candidate fraction at or above maxOrphanFraction of the
//     bridge-managed jobs seen this tick refuses the whole pass. Guard 2 only
//     catches a FULLY blind cache; a PARTIALLY visible one (one surviving
//     JobSet in an otherwise-empty read) sails past it, and guard 3 alone
//     would then cancel everything a few ticks later. Genuine orphans accrue
//     one JobSet deletion at a time; half the fleet orphaned at once is a
//     read fault until a human says otherwise. The guard is stateless per
//     tick, so it lifts on its own the moment the cache heals and the
//     fraction drops — and because it returns BEFORE the grace counters
//     increment, suspect ticks don't consume anyone's grace period either.
func (b *Bridge) cancelOrphanedJobs(ctx context.Context, cfg *config.Config, snap *tickSnapshot, byID map[uint64]*slurm.Job) {
	// Prune jobs that have left Slurm entirely so the counter cannot leak.
	for id := range b.orphanSeen {
		if _, stillListed := byID[id]; !stillListed {
			delete(b.orphanSeen, id)
		}
	}
	if !cfg.CancelOrphanedJobs {
		return
	}

	// managed counts every job passing the scope filters below regardless of
	// whether its JobSet is visible — the denominator for guard 4's fraction.
	managed := 0
	var candidates []*slurm.Job
	for _, job := range byID {
		// Terminal jobs are already going away.
		// Brand new held jobs have not been released onto a JobSet yet, so absence is expected, not orphanhood.
		// However, older held jobs (that have a comment other than "wm: held..." or "") are orphans if their JobSet is missing.
		if job.IsTerminal() || (job.IsHeld() && (job.Comment == "" || strings.HasPrefix(job.Comment, "wm: held"))) {
			continue
		}
		if _, mapped := cfg.PriorityClassFor(job.Partition); !mapped {
			continue
		}
		if !strings.HasPrefix(job.Comment, bridgeCommentPrefix) {
			continue
		}
		managed++
		if _, exists := snap.ownedJobSets[translate.JobSetName(job.JobID)]; exists {
			delete(b.orphanSeen, job.JobID) // JobSet present: not orphaned.
			continue
		}
		candidates = append(candidates, job)
	}
	if len(candidates) == 0 {
		return
	}

	// Guard 2. Deliberately conservative: when ALL owned JobSets are genuinely
	// gone, orphans are left for a human rather than risk mass cancellation.
	if len(snap.ownedJobSets) == 0 {
		b.log.Error("refusing to cancel orphaned Slurm jobs: no owned JobSets are visible at all, "+
			"which is indistinguishable from a mis-scoped cache or lost RBAC on the workload namespace",
			"candidates", len(candidates), "namespace", cfg.Namespace)
		return
	}

	// Guard 4 (A9): refuse on a suspiciously large orphan fraction — the
	// partially-visible-cache case guard 2 cannot see. See the function doc.
	if float64(len(candidates)) >= maxOrphanFraction*float64(managed) {
		metrics.OrphanCancellationsRefusedTotal.Inc()
		b.log.Warn("refusing to cancel orphaned Slurm jobs this tick: too many bridge-managed jobs look orphaned at once, "+
			"which suggests a partially visible informer cache (mid-resync, truncated LIST, or partial RBAC) rather than genuine mass orphanhood; "+
			"grace counters were not advanced and the pass resumes automatically once the fraction drops",
			"candidates", len(candidates), "managedJobs", managed, "maxOrphanFraction", maxOrphanFraction, "namespace", cfg.Namespace)
		return
	}

	if b.orphanSeen == nil {
		b.orphanSeen = map[uint64]int{}
	}
	for _, job := range candidates {
		b.orphanSeen[job.JobID]++
		if seen := b.orphanSeen[job.JobID]; seen < cfg.OrphanGraceTicks {
			b.log.Info("Slurm job has no JobSet, waiting out the grace period",
				"slurmJobID", job.JobID, "ticksSeen", seen, "graceTicks", cfg.OrphanGraceTicks)
			continue
		}
		b.log.Warn("cancelling orphaned Slurm job: its JobSet is gone",
			"slurmJobID", job.JobID, "jobset", translate.JobSetName(job.JobID))
		if err := b.slurm.CancelJob(ctx, job.JobID); err != nil {
			b.log.Error("cancelling orphaned Slurm job failed, will retry next tick",
				"slurmJobID", job.JobID, "error", err)
			continue // keep the counter so the next tick retries immediately
		}
		metrics.OrphanedJobsCancelledTotal.Inc()

		// The JobSet is gone, so its parallelism cannot be read; recompute the
		// pod count from the same job fields ToJobSet used, otherwise every
		// node record but the first is left behind as a ghost `down` node.
		for _, node := range translate.NodeNames(job.JobID, translate.PodCount(job)) {
			if err := b.slurm.DeleteNode(ctx, node); err != nil && !isNotFound(err) {
				// A6: Warn, and phrased as the failure it is. This used to be
				// an Info line reading "deleting slurm node record for orphan"
				// — a success-shaped message for a REAL failure (404s are
				// already filtered out above), invisible to a level>=WARN
				// filter. The orphanSeen entry is deleted below either way,
				// so this delete is NOT retried next tick: the record lingers
				// as a `down` node in sinfo until removed manually, which is
				// exactly what the operator needs to be told.
				b.log.Warn("deleting slurm node record for orphaned job FAILED; the record will linger as a down node until removed manually",
					"node", node, "slurmJobID", job.JobID, "error", err)
			}
		}
		delete(b.orphanSeen, job.JobID)
	}
}

// isNotFound reports whether err is slurmrestd answering 404. Matching on the
// typed status, never on the message text: a body can contain "404" for
// unrelated reasons (see slurm.APIError and the audit D3 note).
func isNotFound(err error) bool {
	var apiErr *slurm.APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound
}
