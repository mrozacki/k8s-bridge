package bridge

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/mrozacki/k8s-bridge/internal/config"
	"github.com/mrozacki/k8s-bridge/internal/metrics"
	"github.com/mrozacki/k8s-bridge/internal/slurm"
	"github.com/mrozacki/k8s-bridge/internal/translate"
)

// orphanCfg is a config with orphan cancellation ENABLED. Every test that
// wants the default (disabled) behavior says so explicitly, so the default can
// never silently flip without a test noticing.
func orphanCfg(graceTicks int) *config.Config {
	cfg := &config.Config{
		Namespace:  "slurm-jobs",
		LocalQueue: "main",
		PartitionMappings: []config.PartitionMapping{
			{PartitionName: "mixing", WorkloadPriorityClass: "normal-priority"},
		},
		CancelOrphanedJobs: true,
		OrphanGraceTicks:   graceTicks,
	}
	cfg.ApplyDefaults()
	return cfg
}

// releasedJob is a job the bridge has taken ownership of (wm: comment) and
// released (pending, not held) — the only shape that is ever a candidate.
func releasedJob(id uint64, partition string, tasks uint64) *slurm.Job {
	return &slurm.Job{
		JobID:     id,
		Partition: partition,
		JobState:  []string{"RUNNING"},
		Comment:   "wm: capacity admitted, provisioning nodes",
		Tasks:     slurm.Uint64NoVal{Set: true, Number: tasks},
	}
}

func orphanBridge(t *testing.T, cfg *config.Config, fs *fakeSlurm) *Bridge {
	t.Helper()
	return New(cfg, nil, fs, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// someOtherJobSet keeps snap.ownedJobSets non-empty so the mass-cancellation
// guard does not short-circuit tests that are about the other guards. The
// JobSet names match anchorJobs' IDs so those jobs count as healthy
// (JobSet-present) managed jobs.
func someOtherJobSet() *tickSnapshot {
	return &tickSnapshot{
		ownedJobSets: map[string]*jobsetv1alpha2.JobSet{
			"slurm-job-998": {},
			"slurm-job-999": {},
		},
	}
}

// anchorJobs returns two released, bridge-managed jobs whose JobSets ARE
// visible in someOtherJobSet's snapshot. Tests about guards 1-3 include them
// in byID so that a single orphan candidate stays a MINORITY (1/3) of the
// managed jobs and guard 4 (A9, maxOrphanFraction) does not trip — without
// them, one candidate out of one managed job is a 100% orphan fraction,
// which is exactly the partial-cache signature guard 4 exists to refuse.
func anchorJobs() []*slurm.Job {
	return []*slurm.Job{
		releasedJob(998, "mixing", 1),
		releasedJob(999, "mixing", 1),
	}
}

// withAnchors prepends anchorJobs to the given jobs, for byIDOf.
func withAnchors(jobs ...*slurm.Job) []*slurm.Job {
	return append(anchorJobs(), jobs...)
}

func byIDOf(jobs ...*slurm.Job) map[uint64]*slurm.Job {
	m := map[uint64]*slurm.Job{}
	for _, j := range jobs {
		m[j.JobID] = j
	}
	return m
}

// runTicks invokes the orphan pass n times, as n consecutive reconcile ticks
// would.
func runTicks(b *Bridge, cfg *config.Config, snap *tickSnapshot, byID map[uint64]*slurm.Job, n int) {
	for i := 0; i < n; i++ {
		b.cancelOrphanedJobs(context.Background(), cfg, snap, byID)
	}
}

// TestOrphanCancellationIsOffByDefault is the safety default: scancel is
// irreversible, so a config that never mentions the feature must never fire it,
// no matter how orphaned a job looks.
func TestOrphanCancellationIsOffByDefault(t *testing.T) {
	cfg := &config.Config{
		Namespace:         "slurm-jobs",
		LocalQueue:        "main",
		PartitionMappings: []config.PartitionMapping{{PartitionName: "mixing", WorkloadPriorityClass: "normal-priority"}},
	}
	cfg.ApplyDefaults()
	if cfg.CancelOrphanedJobs {
		t.Fatal("CancelOrphanedJobs must default to false")
	}
	if cfg.OrphanGraceTicks != 3 {
		t.Errorf("OrphanGraceTicks = %d, want default 3", cfg.OrphanGraceTicks)
	}

	job := releasedJob(1, "mixing", 1)
	fs := &fakeSlurm{}
	b := orphanBridge(t, cfg, fs)
	runTicks(b, cfg, someOtherJobSet(), byIDOf(job), 10)

	if len(fs.cancelled) != 0 {
		t.Errorf("cancelled %v with the feature disabled", fs.cancelled)
	}
}

// TestOrphanCancellationWaitsOutGraceTicks pins the informer-cache guard: the
// JobSet list is cached, so a single tick's absence proves nothing. The job
// must be seen orphaned graceTicks times in a row before scancel.
func TestOrphanCancellationWaitsOutGraceTicks(t *testing.T) {
	const grace = 3
	cfg := orphanCfg(grace)
	job := releasedJob(7, "mixing", 1)
	fs := &fakeSlurm{}
	b := orphanBridge(t, cfg, fs)
	snap, byID := someOtherJobSet(), byIDOf(withAnchors(job)...)

	runTicks(b, cfg, snap, byID, grace-1)
	if len(fs.cancelled) != 0 {
		t.Fatalf("cancelled after %d ticks, want nothing before %d", grace-1, grace)
	}

	runTicks(b, cfg, snap, byID, 1)
	if len(fs.cancelled) != 1 || fs.cancelled[0] != 7 {
		t.Errorf("cancelled = %v, want [7] on the %dth consecutive tick", fs.cancelled, grace)
	}
}

// TestOrphanGraceResetsWhenJobSetReappears covers the cache catching up: an
// absent JobSet that shows up again must clear the counter, so a job that
// flickers out of the cache is never cancelled.
func TestOrphanGraceResetsWhenJobSetReappears(t *testing.T) {
	const grace = 3
	cfg := orphanCfg(grace)
	job := releasedJob(7, "mixing", 1)
	fs := &fakeSlurm{}
	b := orphanBridge(t, cfg, fs)
	byID := byIDOf(withAnchors(job)...)

	runTicks(b, cfg, someOtherJobSet(), byID, grace-1) // 2 strikes

	// The JobSet appears in the cache: counter must reset.
	withJobSet := someOtherJobSet()
	withJobSet.ownedJobSets[translate.JobSetName(7)] = &jobsetv1alpha2.JobSet{}
	runTicks(b, cfg, withJobSet, byID, 1)
	if got := b.orphanSeen[7]; got != 0 {
		t.Errorf("orphanSeen[7] = %d after the JobSet reappeared, want 0", got)
	}

	// It vanishes again: the count restarts, so grace-1 more ticks is not enough.
	runTicks(b, cfg, someOtherJobSet(), byID, grace-1)
	if len(fs.cancelled) != 0 {
		t.Errorf("cancelled = %v, want none — the grace counter must have restarted", fs.cancelled)
	}
}

// TestOrphanCancellationRefusesOnEmptyJobSetList is the mass-cancellation
// guard. "No owned JobSets at all" is what a mis-scoped informer cache or an
// RBAC regression on the workload namespace looks like — identical to "every
// job is orphaned". The bridge must leave the jobs alone and shout.
func TestOrphanCancellationRefusesOnEmptyJobSetList(t *testing.T) {
	cfg := orphanCfg(1) // grace of 1: only the empty-list guard can stop it
	jobs := []*slurm.Job{releasedJob(1, "mixing", 1), releasedJob(2, "mixing", 1), releasedJob(3, "mixing", 1)}
	fs := &fakeSlurm{}
	b := orphanBridge(t, cfg, fs)
	empty := &tickSnapshot{ownedJobSets: map[string]*jobsetv1alpha2.JobSet{}}

	runTicks(b, cfg, empty, byIDOf(jobs...), 10)

	if len(fs.cancelled) != 0 {
		t.Errorf("cancelled %v — an empty JobSet list must never mean 'cancel the whole backlog'", fs.cancelled)
	}
}

// TestOrphanCancellationSkipsNonCandidates pins every filter that keeps a job
// out of scancel's way.
func TestOrphanCancellationSkipsNonCandidates(t *testing.T) {
	cfg := orphanCfg(1)

	held := releasedJob(10, "mixing", 1) // held: its JobSet legitimately may not exist yet
	held.JobState = []string{"PENDING"}
	held.Hold = true

	terminal := releasedJob(11, "mixing", 1)
	terminal.JobState = []string{"COMPLETED"}

	unmapped := releasedJob(12, "other-partition", 1)

	notOurs := releasedJob(13, "mixing", 1)
	notOurs.Comment = "a user comment"

	noComment := releasedJob(14, "mixing", 1)
	noComment.Comment = ""

	fs := &fakeSlurm{}
	b := orphanBridge(t, cfg, fs)
	runTicks(b, cfg, someOtherJobSet(), byIDOf(held, terminal, unmapped, notOurs, noComment), 5)

	if len(fs.cancelled) != 0 {
		t.Errorf("cancelled %v; held, terminal, unmapped-partition and non-bridge jobs must all be skipped", fs.cancelled)
	}
}

// TestOrphanCancellationDeletesEveryNodeRecord is the ghost-node regression:
// the JobSet is gone, so its parallelism cannot be read. Recomputing the pod
// count from the job is what stops nodes 1..N-1 being left `down` in sinfo —
// which TC-C1 counts as an outright failure.
func TestOrphanCancellationDeletesEveryNodeRecord(t *testing.T) {
	cfg := orphanCfg(1)
	job := releasedJob(42, "mixing", 4) // --ntasks=4 => 4 pods => 4 node records
	fs := &fakeSlurm{}
	b := orphanBridge(t, cfg, fs)

	runTicks(b, cfg, someOtherJobSet(), byIDOf(withAnchors(job)...), 1)

	if len(fs.cancelled) != 1 {
		t.Fatalf("cancelled = %v, want [42]", fs.cancelled)
	}
	want := translate.NodeNames(42, 4)
	if len(fs.deletedNodes) != len(want) {
		t.Fatalf("deleted %d node records %v, want %d %v (one per pod)",
			len(fs.deletedNodes), fs.deletedNodes, len(want), want)
	}
	for i, n := range want {
		if fs.deletedNodes[i] != n {
			t.Errorf("deletedNodes[%d] = %q, want %q", i, fs.deletedNodes[i], n)
		}
	}
}

// TestOrphanCancellationRetriesAfterCancelFailure: a failed scancel must not
// consume the job's grace counter, so the next tick retries immediately rather
// than starting the whole wait over.
func TestOrphanCancellationRetriesAfterCancelFailure(t *testing.T) {
	cfg := orphanCfg(1)
	job := releasedJob(5, "mixing", 1)
	fs := &fakeSlurm{cancelErr: context.DeadlineExceeded}
	b := orphanBridge(t, cfg, fs)
	snap, byID := someOtherJobSet(), byIDOf(withAnchors(job)...)

	runTicks(b, cfg, snap, byID, 1)
	if len(fs.cancelled) != 0 || len(fs.deletedNodes) != 0 {
		t.Fatalf("a failed CancelJob must not record a cancel or delete node records")
	}

	fs.cancelErr = nil
	runTicks(b, cfg, snap, byID, 1)
	if len(fs.cancelled) != 1 {
		t.Errorf("cancelled = %v, want the next tick to retry immediately", fs.cancelled)
	}
}

// TestOrphanSeenIsPrunedWhenJobLeavesSlurm keeps the per-job counter from
// leaking for jobs that vanish from ListJobs before ever being cancelled.
func TestOrphanSeenIsPrunedWhenJobLeavesSlurm(t *testing.T) {
	cfg := orphanCfg(5)
	job := releasedJob(8, "mixing", 1)
	fs := &fakeSlurm{}
	b := orphanBridge(t, cfg, fs)

	runTicks(b, cfg, someOtherJobSet(), byIDOf(withAnchors(job)...), 2)
	if b.orphanSeen[8] == 0 {
		t.Fatal("expected the job to be tracked after two orphaned ticks")
	}

	// Job disappears from Slurm entirely.
	runTicks(b, cfg, someOtherJobSet(), map[uint64]*slurm.Job{}, 1)
	if _, tracked := b.orphanSeen[8]; tracked {
		t.Error("orphanSeen must be pruned once the job leaves ListJobs")
	}
}

// TestOrphanGuardRefusesAtOrOverMaxFraction is the A9 partial-cache guard:
// one visible JobSet defeats the empty-list guard (guard 2), so a PARTIALLY
// blind informer cache — most JobSets missing, a few visible — previously
// sailed through and cancelled everything after the grace ticks. When the
// candidate fraction reaches maxOrphanFraction of the bridge-managed jobs
// seen this tick, the pass must refuse, count the refusal, and leave the
// grace counters untouched.
func TestOrphanGuardRefusesAtOrOverMaxFraction(t *testing.T) {
	cases := map[string]struct {
		candidates int // orphan-looking jobs (no JobSet in the snapshot)
		healthy    int // jobs whose JobSets are visible (from anchorJobs)
	}{
		"exactly at the bound (1 of 2)":                                 {candidates: 1, healthy: 1},
		"over the bound (2 of 3)":                                       {candidates: 2, healthy: 1},
		"everything but one (2 of 2 candidates, 1 lone JobSet visible)": {candidates: 2, healthy: 0},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := orphanCfg(1) // grace 1: only the fraction guard can stop it
			fs := &fakeSlurm{}
			b := orphanBridge(t, cfg, fs)

			jobs := anchorJobs()[:tc.healthy]
			for i := 0; i < tc.candidates; i++ {
				jobs = append(jobs, releasedJob(uint64(100+i), "mixing", 1))
			}
			// someOtherJobSet keeps slurm-job-998/999 visible; only the ones
			// with a matching anchor job in byID count as healthy managed
			// jobs, but a non-empty map always defeats guard 2 — exactly the
			// partial-cache shape this guard exists for.
			refusedBefore := testutil.ToFloat64(metrics.OrphanCancellationsRefusedTotal)
			runTicks(b, cfg, someOtherJobSet(), byIDOf(jobs...), 5)

			if len(fs.cancelled) != 0 {
				t.Errorf("cancelled %v — a candidate fraction >= %v must refuse the pass", fs.cancelled, maxOrphanFraction)
			}
			if got := testutil.ToFloat64(metrics.OrphanCancellationsRefusedTotal); got != refusedBefore+5 {
				t.Errorf("OrphanCancellationsRefusedTotal delta = %v, want 5 (one per refused tick)", got-refusedBefore)
			}
			for id, n := range b.orphanSeen {
				if n != 0 {
					t.Errorf("orphanSeen[%d] = %d, want 0: refused ticks must not consume grace", id, n)
				}
			}
		})
	}
}

// TestOrphanGuardJustUnderFractionCancels is guard 4's counterpart: a
// fraction just below maxOrphanFraction (1 candidate of 3 managed jobs) is
// organic orphan territory and must still cancel after the grace ticks.
func TestOrphanGuardJustUnderFractionCancels(t *testing.T) {
	cfg := orphanCfg(1)
	job := releasedJob(9, "mixing", 1)
	fs := &fakeSlurm{}
	b := orphanBridge(t, cfg, fs)

	runTicks(b, cfg, someOtherJobSet(), byIDOf(withAnchors(job)...), 1) // 1 of 3 managed
	if len(fs.cancelled) != 1 || fs.cancelled[0] != 9 {
		t.Errorf("cancelled = %v, want [9]: a minority orphan fraction must not be refused", fs.cancelled)
	}
}

// TestOrphanGuardLiftsWhenFractionDrops pins that guard 4 is stateless per
// tick: once the cache heals and the fraction drops below the bound, the
// pass resumes — but the grace clock starts from zero, because refused
// ticks advanced no counters.
func TestOrphanGuardLiftsWhenFractionDrops(t *testing.T) {
	const grace = 2
	cfg := orphanCfg(grace)
	job := releasedJob(11, "mixing", 1)
	fs := &fakeSlurm{}
	b := orphanBridge(t, cfg, fs)
	snap := someOtherJobSet()

	// Partially blind cache: 1 candidate of 2 managed (0.5) -> refused.
	runTicks(b, cfg, snap, byIDOf(anchorJobs()[0], job), 3)
	if len(fs.cancelled) != 0 {
		t.Fatalf("cancelled %v during the refused phase", fs.cancelled)
	}

	// Cache heals: 1 of 3 managed. The first healthy tick must only START
	// the grace count (refused ticks contributed nothing)...
	healthy := byIDOf(withAnchors(job)...)
	runTicks(b, cfg, snap, healthy, grace-1)
	if len(fs.cancelled) != 0 {
		t.Fatalf("cancelled %v before the grace ticks elapsed — refused ticks must not have advanced the counter", fs.cancelled)
	}
	// ...and the guard must have lifted on its own: grace ticks later the
	// genuine orphan is cancelled.
	runTicks(b, cfg, snap, healthy, 1)
	if len(fs.cancelled) != 1 || fs.cancelled[0] != 11 {
		t.Errorf("cancelled = %v, want [11] once the fraction dropped and grace elapsed", fs.cancelled)
	}
}

// levelCapturingHandler records every slog message at or above Warn, for
// asserting on log severity and phrasing (A6).
type levelCapturingHandler struct {
	slog.Handler
	warns *[]string
}

func (h levelCapturingHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Level >= slog.LevelWarn {
		*h.warns = append(*h.warns, r.Message)
	}
	return h.Handler.Handle(ctx, r)
}

// TestOrphanNodeRecordDeleteFailureIsWarnLogged is the A6 regression: a REAL
// node-record delete failure during orphan cleanup (non-404 — 404s are
// filtered as "already gone") was logged at Info with the success-shaped
// message "deleting slurm node record for orphan", invisible to a
// level>=WARN filter even though the record then lingers as a down node
// with no retry. It must be a Warn phrased as a failure.
func TestOrphanNodeRecordDeleteFailureIsWarnLogged(t *testing.T) {
	cfg := orphanCfg(1)
	job := releasedJob(44, "mixing", 1)
	fs := &fakeSlurm{deleteNodeErr: errors.New("slurmrestd 500: internal error")}
	var warns []string
	b := orphanBridge(t, cfg, fs)
	b.log = slog.New(levelCapturingHandler{
		Handler: slog.NewTextHandler(io.Discard, nil),
		warns:   &warns,
	})

	runTicks(b, cfg, someOtherJobSet(), byIDOf(withAnchors(job)...), 1)

	if len(fs.cancelled) != 1 {
		t.Fatalf("cancelled = %v, want [44] (the cancel itself succeeded)", fs.cancelled)
	}
	var found bool
	for _, msg := range warns {
		if strings.Contains(msg, "node record") && strings.Contains(strings.ToLower(msg), "failed") {
			found = true
		}
	}
	if !found {
		t.Errorf("warn-level messages = %q, want one describing the node-record delete FAILURE", warns)
	}
}
