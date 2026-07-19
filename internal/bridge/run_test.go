package bridge

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mrozacki/k8s-bridge/internal/config"
)

// TestJitteredIntervalStaysWithinTenPercent pins audit P8a: the jitter must
// never exceed +/-10% of the base interval, so the average polling cadence
// stays close to what the operator configured.
func TestJitteredIntervalStaysWithinTenPercent(t *testing.T) {
	base := 10 * time.Second
	lo := time.Duration(float64(base) * 0.9)
	hi := time.Duration(float64(base) * 1.1)
	for i := 0; i < 200; i++ {
		got := jitteredInterval(base)
		if got < lo || got > hi {
			t.Fatalf("jitteredInterval(%s) = %s, want within [%s,%s]", base, got, lo, hi)
		}
	}
}

// TestJitteredIntervalZeroBaseStaysZero guards against a misconfigured or
// unset poll interval producing a negative or nonsensical wait.
func TestJitteredIntervalZeroBaseStaysZero(t *testing.T) {
	if got := jitteredInterval(0); got != 0 {
		t.Errorf("jitteredInterval(0) = %s, want 0", got)
	}
}

// TestNextBackoffResetsOnSuccessAndCapsAtSixX is the audit P8a regression:
// backoff must double per consecutive failure and cap at 6x the poll
// interval, then reset to the plain jittered interval once a tick succeeds.
func TestNextBackoffResetsOnSuccessAndCapsAtSixX(t *testing.T) {
	base := 10 * time.Second
	cases := []struct {
		name                string
		consecutiveFailures int
		wantMultipleAtLeast float64
		wantMultipleAtMost  float64
	}{
		{"success resets to base", 0, 0.9, 1.1},
		{"first failure doubles", 1, 1.8, 2.2},
		{"second failure quadruples", 2, 3.6, 4.4},
		{"third failure would be 8x but caps at 6x", 3, 5.4, 6.6},
		{"many failures still capped at 6x", 10, 5.4, 6.6},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i < 50; i++ {
				got := nextBackoff(base, tc.consecutiveFailures)
				gotMultiple := float64(got) / float64(base)
				if gotMultiple < tc.wantMultipleAtLeast || gotMultiple > tc.wantMultipleAtMost {
					t.Fatalf("nextBackoff(%s, %d) = %s (%.2fx), want within [%.2fx,%.2fx]",
						base, tc.consecutiveFailures, got, gotMultiple, tc.wantMultipleAtLeast, tc.wantMultipleAtMost)
				}
			}
		})
	}
}

// TestRunSurvivesFailingTicks is the audit P8a / long-standing "Run() 0%
// coverage" gap: Run's one documented guarantee is that a failing tick
// never kills the loop. This drives a few ticks (some failing) through the
// real Run loop with a short poll interval and confirms it keeps going and
// returns ctx.Err() on cancellation, not the tick error.
func TestRunSurvivesFailingTicks(t *testing.T) {
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
	b.setCfg(cfg) // use the short poll interval instead of the default test config

	var ticks int
	b.OnTick = func(error) { ticks++ }

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := b.Run(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Run() error = %v, want context.DeadlineExceeded (a failing tick must not kill the loop)", err)
	}
	if ticks < 2 {
		t.Errorf("OnTick fired %d times, want at least 2 (loop must keep retrying failing ticks)", ticks)
	}
}

// TestRunResetsBackoffAfterSuccess confirms a tick that fails then succeeds
// returns to the short jittered interval rather than staying backed off,
// by observing that many ticks fit in the deadline once ListJobs recovers.
func TestRunResetsBackoffAfterSuccess(t *testing.T) {
	fs := &fakeSlurm{listJobsErr: errors.New("boom")}
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

	var ticks int
	b.OnTick = func(err error) {
		ticks++
		if ticks == 1 {
			// Recover after the first (failing) tick so backoff should reset.
			fs.listJobsErr = nil
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := b.Run(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want context.DeadlineExceeded", err)
	}
	// With backoff reset after the first success, several more ticks should
	// have fit in the remaining ~195ms at a ~5ms base interval; if backoff
	// had NOT reset (capped 6x => 30ms) far fewer would fit, but the real
	// signal we assert is simply "more than the two guaranteed by the first
	// failure+recovery pair", which would fail under a stuck/growing backoff.
	if ticks < 5 {
		t.Errorf("ticks = %d, want at least 5 (backoff must reset after a successful tick)", ticks)
	}
}

// TestNudgeTriggersImmediateTick is the P1 watch-nudge regression: a call to
// Nudge() must cause the NEXT tick to run right away rather than waiting out
// the (long) poll interval, and Run must record that tick's trigger as
// "watch" rather than "timer".
func TestNudgeTriggersImmediateTick(t *testing.T) {
	fs := &fakeSlurm{}
	cfg := &config.Config{
		Namespace:  "slurm-jobs",
		LocalQueue: "main",
		PartitionMappings: []config.PartitionMapping{
			{PartitionName: "mixing", WorkloadPriorityClass: "normal-priority"},
		},
		Slurmd: config.Slurmd{Image: "slurmd:test", ConfServer: "ctl:6817", AuthSecretName: "s"},
		// Deliberately long: if the nudge did not fire, the test's short
		// deadline below would only ever observe the first (always-timer)
		// tick.
		PollInterval: config.Duration{Duration: 10 * time.Second},
	}
	b, _ := testBridge(t, fs)
	b.setCfg(cfg)
	// This test is about the nudge waking the loop AT ALL; the A11 damping
	// interval between watch ticks has its own dedicated test
	// (TestWatchNudgeIsDampedToMinInterval), so disable it here.
	b.nudgeMinInterval = 0

	tickDone := make(chan struct{}, 4)
	b.OnTick = func(error) { tickDone <- struct{}{} }

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- b.Run(ctx) }()

	// Wait for the first (timer) tick to complete before nudging, so the
	// nudge is observed as a SECOND, immediate tick rather than racing the
	// first one.
	select {
	case <-tickDone:
	case <-time.After(1 * time.Second):
		t.Fatal("first tick did not complete in time")
	}

	b.Nudge()

	select {
	case <-tickDone:
		// Second tick arrived well before the 10s poll interval would have
		// elapsed — proof the nudge, not the timer, triggered it.
	case <-time.After(1 * time.Second):
		t.Fatal("nudge did not trigger an immediate second tick")
	}

	cancel()
	<-runErr
}

// TestNudgeCoalescesBurstIntoOneExtraTick is the coalescing regression
// required by the spec: several Nudge() calls in a row, faster than the
// loop can consume them, must collapse into exactly one extra tick — never
// a tick storm, and never a lost signal either (at least one extra tick
// still happens).
func TestNudgeCoalescesBurstIntoOneExtraTick(t *testing.T) {
	fs := &fakeSlurm{}
	cfg := &config.Config{
		Namespace:  "slurm-jobs",
		LocalQueue: "main",
		PartitionMappings: []config.PartitionMapping{
			{PartitionName: "mixing", WorkloadPriorityClass: "normal-priority"},
		},
		Slurmd:       config.Slurmd{Image: "slurmd:test", ConfServer: "ctl:6817", AuthSecretName: "s"},
		PollInterval: config.Duration{Duration: 10 * time.Second},
	}
	b, _ := testBridge(t, fs)
	b.setCfg(cfg)
	// Coalescing, not damping, is under test here (the damping has its own
	// dedicated test) — with the default 1s nudgeMinInterval the one extra
	// tick would not fit inside this test's 300ms deadline at all.
	b.nudgeMinInterval = 0

	// A burst of nudges BEFORE Run even starts consuming them: the buffered
	// size-1 channel means only one is retained, exactly matching what a
	// coalescing channel should do regardless of timing.
	for i := 0; i < 50; i++ {
		b.Nudge()
	}

	var ticks int32
	var mu sync.Mutex
	b.OnTick = func(error) {
		mu.Lock()
		ticks++
		mu.Unlock()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := b.Run(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want context.DeadlineExceeded", err)
	}

	mu.Lock()
	got := ticks
	mu.Unlock()
	// Exactly 2: one ordinary first tick, plus ONE extra tick from the
	// entire burst of 50 nudges — not 50, not 0. The 10s poll interval
	// guarantees no timer-triggered tick fits in the 300ms deadline, so
	// every tick beyond the first must have come from the (coalesced) nudge.
	if got != 2 {
		t.Errorf("ticks = %d, want exactly 2 (1 initial + 1 coalesced from the burst of 50 nudges)", got)
	}
}

// TestNudgeDamping unit-tests the A11 deferral computation (pure function,
// no timers): a nudge landing before minInterval has elapsed since the last
// tick started must wait exactly the remainder; anything at or past the
// interval — and a disabled (0) interval — proceeds immediately.
func TestNudgeDamping(t *testing.T) {
	cases := []struct {
		name          string
		minInterval   time.Duration
		sinceLastTick time.Duration
		want          time.Duration
	}{
		{"nudge right after a tick waits the full interval", time.Second, 0, time.Second},
		{"partial elapse waits the remainder", time.Second, 400 * time.Millisecond, 600 * time.Millisecond},
		{"exactly at the interval proceeds immediately", time.Second, time.Second, 0},
		{"past the interval proceeds immediately", time.Second, 2 * time.Second, 0},
		{"disabled interval never defers", 0, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := nudgeDamping(tc.minInterval, tc.sinceLastTick); got != tc.want {
				t.Errorf("nudgeDamping(%s, %s) = %s, want %s", tc.minInterval, tc.sinceLastTick, got, tc.want)
			}
		})
	}
}

// TestNewBridgeDefaultsNudgeMinInterval pins the production default: a
// Bridge built through New must carry the A11 damping floor, so only tests
// that explicitly zero it get the undamped behavior.
func TestNewBridgeDefaultsNudgeMinInterval(t *testing.T) {
	b, _ := testBridge(t, &fakeSlurm{})
	if b.nudgeMinInterval != minNudgeInterval {
		t.Errorf("nudgeMinInterval = %s, want %s (the A11 default)", b.nudgeMinInterval, minNudgeInterval)
	}
	if minNudgeInterval != time.Second {
		t.Errorf("minNudgeInterval = %s, want 1s per the A11 spec", minNudgeInterval)
	}
}

// TestWatchNudgeIsDampedToMinInterval drives the damping through the real
// Run loop (A11): a nudge arriving right after a tick must be DEFERRED until
// the (test-shrunk) interval has elapsed — and then still fire, proving the
// nudge was delayed, not dropped. The 100ms "must not have ticked yet" probe
// sits well inside the 300ms damping window, so scheduling jitter cannot
// flake it; the scale-test rationale is on minNudgeInterval.
func TestWatchNudgeIsDampedToMinInterval(t *testing.T) {
	fs := &fakeSlurm{}
	cfg := &config.Config{
		Namespace:  "slurm-jobs",
		LocalQueue: "main",
		PartitionMappings: []config.PartitionMapping{
			{PartitionName: "mixing", WorkloadPriorityClass: "normal-priority"},
		},
		Slurmd: config.Slurmd{Image: "slurmd:test", ConfServer: "ctl:6817", AuthSecretName: "s"},
		// Long enough that no timer tick can interfere with the observation.
		PollInterval: config.Duration{Duration: 10 * time.Second},
	}
	b, _ := testBridge(t, fs)
	b.setCfg(cfg)
	b.nudgeMinInterval = 300 * time.Millisecond

	tickDone := make(chan struct{}, 4)
	b.OnTick = func(error) { tickDone <- struct{}{} }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- b.Run(ctx) }()

	select {
	case <-tickDone:
	case <-time.After(2 * time.Second):
		t.Fatal("first tick did not complete in time")
	}

	// Nudge immediately after the first tick: far inside the damping window.
	b.Nudge()

	select {
	case <-tickDone:
		t.Fatal("watch-triggered tick ran before the damping interval elapsed — the nudge was not deferred")
	case <-time.After(100 * time.Millisecond):
		// Good: still deferred a third of the way into the window.
	}

	select {
	case <-tickDone:
		// The deferred tick arrived once the interval elapsed: deferred, not
		// dropped.
	case <-time.After(2 * time.Second):
		t.Fatal("deferred nudge tick never ran — the nudge was dropped instead of deferred")
	}

	cancel()
	<-runErr
}
