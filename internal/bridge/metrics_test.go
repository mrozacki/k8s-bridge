package bridge

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/mrozacki/k8s-bridge/internal/config"
	"github.com/mrozacki/k8s-bridge/internal/metrics"
	"github.com/mrozacki/k8s-bridge/internal/slurm"
)

// These tests exercise the metrics wiring added in reconciler.go (audit
// AUD2). The metrics package uses a single package-level Registry/counters
// shared across the whole test binary, so assertions use BEFORE/AFTER
// deltas rather than absolute values — this is the same reason
// TicksTotal/TickErrorsTotal are counters, not gauges, in production too.

// TestRunIncrementsTicksAndHeldJobsMetrics drives the metric wiring through
// the real Run loop (not tick() directly), since TicksTotal/TickErrorsTotal
// are incremented in Run around the tick() call (see reconciler.go) while
// JobSetsCreatedTotal/HeldJobs are set inside tick()'s admitHeldJobs.
func TestRunIncrementsTicksAndHeldJobsMetrics(t *testing.T) {
	fs := &fakeSlurm{jobs: []slurm.Job{heldJob(701, "mixing")}}
	cfg := &config.Config{
		Namespace:  "slurm-jobs",
		LocalQueue: "main",
		PartitionMappings: []config.PartitionMapping{
			{PartitionName: "mixing", WorkloadPriorityClass: "normal-priority"},
		},
		Slurmd:       config.Slurmd{Image: "slurmd:test", ConfServer: "ctl:6817", AuthSecretName: "s"},
		PollInterval: config.Duration{Duration: 5 * time.Millisecond},
	}
	b, _ := testBridge(t, fs, admittedWorkload("slurm-job-701"))
	b.setCfg(cfg)

	ticksBefore := testutil.ToFloat64(metrics.TicksTotal)
	errorsBefore := testutil.ToFloat64(metrics.TickErrorsTotal)
	createdBefore := testutil.ToFloat64(metrics.JobSetsCreatedTotal)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := b.Run(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want context.DeadlineExceeded", err)
	}

	if got := testutil.ToFloat64(metrics.TicksTotal); got <= ticksBefore {
		t.Errorf("TicksTotal = %v, want > %v (at least one tick ran)", got, ticksBefore)
	}
	if got := testutil.ToFloat64(metrics.TickErrorsTotal); got != errorsBefore {
		t.Errorf("TickErrorsTotal = %v, want unchanged at %v (every tick succeeded)", got, errorsBefore)
	}
	if got := testutil.ToFloat64(metrics.JobSetsCreatedTotal); got != createdBefore+1 {
		t.Errorf("JobSetsCreatedTotal = %v, want %v (JobSet created once, then idempotent)", got, createdBefore+1)
	}
	if got := testutil.ToFloat64(metrics.HeldJobs); got != 1 {
		t.Errorf("HeldJobs = %v, want 1 (one held+pending job seen each tick)", got)
	}
}

// TestRunIncrementsTickErrorsOnFailure is the counterpart pinning that a
// FAILING tick (ListJobs error) counts against tick_errors_total, exercised
// through the real Run loop the way main.go drives it.
func TestRunIncrementsTickErrorsOnFailure(t *testing.T) {
	fs := &fakeSlurm{listJobsErr: errors.New("slurmrestd unavailable")}
	cfg := &config.Config{
		Namespace:  "slurm-jobs",
		LocalQueue: "main",
		PartitionMappings: []config.PartitionMapping{
			{PartitionName: "mixing", WorkloadPriorityClass: "normal-priority"},
		},
		Slurmd:       config.Slurmd{Image: "slurmd:test", ConfServer: "ctl:6817", AuthSecretName: "s"},
		PollInterval: config.Duration{Duration: 5 * time.Millisecond},
	}
	b, _ := testBridge(t, fs)
	b.setCfg(cfg)

	errorsBefore := testutil.ToFloat64(metrics.TickErrorsTotal)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := b.Run(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want context.DeadlineExceeded", err)
	}
	if got := testutil.ToFloat64(metrics.TickErrorsTotal); got <= errorsBefore {
		t.Errorf("TickErrorsTotal = %v, want > %v (every tick failed)", got, errorsBefore)
	}
}

// TestCleanupIncrementsJobSetsDeletedTotal pins the deletion-side counter.
func TestCleanupIncrementsJobSetsDeletedTotal(t *testing.T) {
	fs := &fakeSlurm{getJob: func(uint64) (*slurm.Job, bool, error) { return nil, false, nil }}
	b, _ := testBridge(t, fs, ownedJobSet(t, 702))

	deletedBefore := testutil.ToFloat64(metrics.JobSetsDeletedTotal)
	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if got := testutil.ToFloat64(metrics.JobSetsDeletedTotal); got != deletedBefore+1 {
		t.Errorf("JobSetsDeletedTotal = %v, want %v", got, deletedBefore+1)
	}
}

// TestLastSuccessfulTickTimestampGauge pins the staleness anchor: a
// successful tick stamps k8s_bridge_last_successful_tick_timestamp_seconds
// with (approximately) now, and a FAILING tick leaves the previous stamp in
// place — the whole point of the gauge is that it stops moving when the
// bridge stops doing useful work, unlike scrape timestamps on stalled
// counters.
func TestLastSuccessfulTickTimestampGauge(t *testing.T) {
	fs := &fakeSlurm{jobs: []slurm.Job{}}
	b, _ := testBridge(t, fs)

	before := time.Now().Unix()
	b.recordTickOutcome(nil)
	got := testutil.ToFloat64(metrics.LastSuccessfulTickTimestamp)
	if int64(got) < before || int64(got) > time.Now().Unix() {
		t.Fatalf("gauge = %v, want a unix timestamp between %d and now", got, before)
	}

	// A failing tick must NOT advance the stamp.
	b.recordTickOutcome(errors.New("slurmrestd down"))
	if after := testutil.ToFloat64(metrics.LastSuccessfulTickTimestamp); after != got {
		t.Errorf("gauge moved on a FAILED tick: %v -> %v", got, after)
	}
}
