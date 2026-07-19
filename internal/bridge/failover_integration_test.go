//go:build integration

// Leader-failover regressions (production-readiness plan §3 phase 2, "finalize
// HA story"). Two Managers share ONE envtest kube-apiserver and compete for
// the SAME coordination.k8s.io Lease under DISTINCT identities, exactly like
// two k8s-bridge replicas of one Deployment. What must hold:
//
//  1. The standby's Bridge.Run never ticks while the other identity holds
//     the Lease (exactly-one-leader at tick granularity).
//  2. When the leader goes away, the standby takes over within a bounded,
//     lease-timing-derived window — via BOTH handover paths:
//     voluntary release (LeaderElectionReleaseOnCancel, graceful stop) and
//     natural lease expiry (crash-like: the holder vanishes without
//     releasing, which is what production does today since main.go does not
//     set ReleaseOnCancel).
//  3. Once the standby ticks, the old leader never ticks again (no
//     overlapping leadership windows).
//
// Identity control: by default controller-runtime derives each Manager's
// leader-election identity as os.Hostname()+"_"+uuid — already unique per
// Manager even inside one process, but random. These tests instead inject a
// resourcelock.Interface via Options.LeaderElectionResourceLockInterface
// (the supported mechanism; there is no identity field on Options) so the
// identities are the deterministic strings "manager-a"/"manager-b" and the
// Lease's holderIdentity transitions can be asserted directly. Note the
// Options doc comment claims LeaseDuration/RenewDeadline/RetryPeriod are
// ignored when a custom lock is set — that is true only of lock
// CONSTRUCTION; manager.New still threads all three into the LeaderElector
// (verified against controller-runtime v0.24.1 source), so the short
// timings below do apply.
package bridge

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/mrozacki/k8s-bridge/internal/config"
)

// Lease timings for both failover tests, deliberately much shorter than
// main.go's production 15s/10s/2s so the expiry path completes in test time.
// The client-go constraint LeaseDuration > RenewDeadline > RetryPeriod*1.2
// must hold or NewLeaderElector rejects the config.
//
// NOT shorter: the original 3s/2s timings flaked under load (first
// independent re-run, 2026-07-12) — a leader that misses its 2s
// RenewDeadline because the machine is busy running the rest of the suite
// loses leadership SPONTANEOUSLY mid-scenario, and every subsequent
// assertion (standby-must-not-tick, holder identity, tick ordering) is
// judging a takeover the test never asked for. 6s/5s gives the leader ten
// 500ms renewal attempts' worth of stall headroom while keeping the expiry
// path under the eventually-timeouts.
const (
	failoverLeaseDuration = 6 * time.Second
	failoverRenewDeadline = 5 * time.Second
	failoverRetryPeriod   = 500 * time.Millisecond
	failoverLeaseName     = "k8s-bridge-leader"
)

// tickRecorder collects every tick from every bridge into one time-ordered
// slice (appends happen under one mutex, so slice order IS wall-clock
// order). This is what makes the exactly-one-leader assertion possible:
// after the fact, no "A" tick may appear after the first "B" tick.
type tickRecorder struct {
	mu     sync.Mutex
	events []tickEvent
}

type tickEvent struct {
	who string
	at  time.Time
}

func (r *tickRecorder) hook(who string) func(error) {
	return func(error) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.events = append(r.events, tickEvent{who: who, at: time.Now()})
	}
}

func (r *tickRecorder) count(who string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, e := range r.events {
		if e.who == who {
			n++
		}
	}
	return n
}

func (r *tickRecorder) snapshot() []tickEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]tickEvent(nil), r.events...)
}

// firstTick returns the time of who's first tick, or zero if none yet.
func (r *tickRecorder) firstTick(who string) time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.events {
		if e.who == who {
			return e.at
		}
	}
	return time.Time{}
}

// newFailoverManager assembles one competitor: a Manager whose
// leader-election identity is exactly `identity`, gating a Bridge (fake
// Slurm, 200ms poll) registered as a plain RunnableFunc — the same wiring
// cmd/k8s-bridge/main.go uses.
func newFailoverManager(t *testing.T, restCfg *rest.Config, ns, identity string, releaseOnCancel bool, onTick func(error)) manager.Manager {
	t.Helper()

	lock, err := resourcelock.NewFromKubeconfig(
		resourcelock.LeasesResourceLock, ns, failoverLeaseName,
		resourcelock.ResourceLockConfig{Identity: identity},
		restCfg, failoverRenewDeadline,
	)
	if err != nil {
		t.Fatalf("building resource lock for %s: %v", identity, err)
	}

	lease, renew, retry := failoverLeaseDuration, failoverRenewDeadline, failoverRetryPeriod
	mgr, err := manager.New(restCfg, manager.Options{
		Scheme:                 testScheme(t),
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		LeaderElection:         true,
		// Only feeds the elector's log name; the lock above carries the real
		// lease name/namespace/identity.
		LeaderElectionID:                    failoverLeaseName,
		LeaderElectionResourceLockInterface: lock,
		LeaseDuration:                       &lease,
		RenewDeadline:                       &renew,
		RetryPeriod:                         &retry,
		LeaderElectionReleaseOnCancel:       releaseOnCancel,
	})
	if err != nil {
		t.Fatalf("manager.New(%s): %v", identity, err)
	}

	cfg := &config.Config{
		Namespace:  ns,
		LocalQueue: "main",
		PartitionMappings: []config.PartitionMapping{
			{PartitionName: "mixing", WorkloadPriorityClass: "normal-priority"},
		},
		Slurmd:       config.Slurmd{Image: "slurmd:test", ConfServer: "ctl:6817", AuthSecretName: "s"},
		PollInterval: config.Duration{Duration: 200 * time.Millisecond},
	}
	b := New(cfg, mgr.GetClient(), &fakeSlurm{}, slog.Default())
	b.OnTick = onTick
	if err := mgr.Add(manager.RunnableFunc(b.Run)); err != nil {
		t.Fatalf("registering bridge runnable (%s): %v", identity, err)
	}
	return mgr
}

// leaseHolder reads the shared Lease's holderIdentity ("" when unset).
func leaseHolder(t *testing.T, kube client.Client, ns string) string {
	t.Helper()
	var lease coordinationv1.Lease
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: failoverLeaseName}, &lease); err != nil {
		t.Fatalf("reading Lease: %v", err)
	}
	if lease.Spec.HolderIdentity == nil {
		return ""
	}
	return *lease.Spec.HolderIdentity
}

// waitFor polls cond every 100ms until it returns true or the deadline
// passes, in which case the test fails with msg.
func waitFor(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if cond() {
			return
		}
		select {
		case <-deadline:
			t.Fatal(msg)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// assertSingleLeadershipWindow enforces the exactly-one-leader invariant at
// tick granularity: once B's first tick is recorded, no further A tick may
// ever appear. (A tick that COMPLETED between A's cancel and B's takeover is
// legal — leadership had not transferred yet — which is why the assertion is
// about ordering relative to B's first tick, not about A's cancel time.)
func assertSingleLeadershipWindow(t *testing.T, events []tickEvent) {
	t.Helper()
	firstB := -1
	for i, e := range events {
		if e.who == "B" {
			firstB = i
			break
		}
	}
	if firstB == -1 {
		t.Fatal("no B tick recorded — takeover never happened")
	}
	for _, e := range events[firstB:] {
		if e.who == "A" {
			t.Fatalf("overlapping leadership: A ticked at %s AFTER B's first tick at %s", e.at.Format(time.RFC3339Nano), events[firstB].at.Format(time.RFC3339Nano))
		}
	}
}

// runFailoverScenario drives the shared timeline of both failover tests:
// A leads and ticks, B stays silent, A's context is cancelled, B must take
// over. The two tests differ only in releaseOnCancel and in the
// path-specific takeover-timing assertions they layer on top of the
// returned measurements.
type failoverResult struct {
	takeover     time.Duration // A's cancel -> B's first tick
	aTicksAtStop int           // A's tick count the moment its Start returned
	rec          *tickRecorder
}

func runFailoverScenario(t *testing.T, ns string, releaseOnCancel bool) failoverResult {
	t.Helper()
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{"../../test/crd"},
		ErrorIfCRDPathMissing: true,
	}
	restCfg, err := env.Start()
	if err != nil {
		t.Fatalf("starting envtest (is KUBEBUILDER_ASSETS set? run via make test-integration): %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	kube, err := client.New(restCfg, client.Options{Scheme: testScheme(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := kube.Create(context.Background(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
	}); err != nil {
		t.Fatal(err)
	}

	rec := &tickRecorder{}
	mgrA := newFailoverManager(t, restCfg, ns, "manager-a", releaseOnCancel, rec.hook("A"))
	mgrB := newFailoverManager(t, restCfg, ns, "manager-b", releaseOnCancel, rec.hook("B"))

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()

	// A starts alone against a fresh Lease: it must win and start ticking.
	errA := make(chan error, 1)
	go func() { errA <- mgrA.Start(ctxA) }()
	waitFor(t, 30*time.Second, "manager A never won leadership / never ticked", func() bool {
		return rec.count("A") > 0
	})
	if holder := leaseHolder(t, kube, ns); holder != "manager-a" {
		t.Fatalf("Lease holder = %q while A leads, want manager-a (identity injection via LeaderElectionResourceLockInterface failed)", holder)
	}

	// B starts as the standby. Give it several retry periods (500ms each)
	// and poll intervals (200ms) to misbehave: it must NOT tick while A
	// holds the Lease, no matter how long its Bridge has been registered.
	errB := make(chan error, 1)
	go func() { errB <- mgrB.Start(ctxB) }()
	time.Sleep(1500 * time.Millisecond)
	if got := rec.count("B"); got != 0 {
		t.Fatalf("standby ticked %d times while manager-a held the Lease, want 0", got)
	}

	// Kill A. With releaseOnCancel the stop is a graceful handover (holder
	// zeroed on the way out); without it this approximates a crash — the
	// Lease keeps naming manager-a until it expires.
	cancelledAt := time.Now()
	cancelA()
	select {
	case err := <-errA:
		// "leader election lost" is controller-runtime's normal way of
		// reporting that a leading manager's context was cancelled; only
		// genuinely unexpected errors should fail the test.
		if err != nil && err.Error() != "leader election lost" {
			t.Fatalf("manager A Start returned unexpected error: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("manager A did not stop after context cancel")
	}
	aTicksAtStop := rec.count("A")

	waitFor(t, 30*time.Second, "standby never took over after the leader stopped", func() bool {
		return rec.count("B") > 0
	})
	takeover := rec.firstTick("B").Sub(cancelledAt)
	t.Logf("takeover (A cancel -> B first tick, releaseOnCancel=%v): %s", releaseOnCancel, takeover)

	if holder := leaseHolder(t, kube, ns); holder != "manager-b" {
		t.Fatalf("Lease holder = %q after takeover, want manager-b", holder)
	}

	// Let B tick a few more times, then check A stayed frozen the whole
	// time and the leadership windows never interleaved.
	waitFor(t, 10*time.Second, "B stopped ticking after takeover", func() bool {
		return rec.count("B") >= 3
	})
	if got := rec.count("A"); got != aTicksAtStop {
		t.Fatalf("A's tick count advanced after its manager stopped: %d -> %d", aTicksAtStop, got)
	}
	assertSingleLeadershipWindow(t, rec.snapshot())

	return failoverResult{takeover: takeover, aTicksAtStop: aTicksAtStop, rec: rec}
}

// TestLeaderFailoverGracefulRelease covers the voluntary-release path:
// LeaderElectionReleaseOnCancel makes the outgoing leader zero the Lease's
// holderIdentity on its way out, so the standby acquires on its next retry
// (~RetryPeriod) instead of waiting out LeaseDuration. main.go does NOT
// enable this today (see docs/operations.md, "High availability & leader
// failover") — this test documents/validates what the option would buy if
// the expiry-based handover ever proves too slow, and pins the semantics so
// enabling it later is a config change, not a research project.
func TestLeaderFailoverGracefulRelease(t *testing.T) {
	res := runFailoverScenario(t, "failover-release", true)

	// The discriminator against the expiry path: with a voluntary release
	// the standby can win as soon as its next 500ms retry fires, strictly
	// before the earliest possible lease expiry. A had renewed at most
	// RetryPeriod before the cancel, so expiry-based takeover could never
	// happen before LeaseDuration-RetryPeriod = 5.5s after cancel; 3s keeps
	// 2.5s of discrimination margin while allowing generous scheduling noise
	// on top of the ~0.2-1.1s takeovers measured for this path.
	if maxReleaseTakeover := 3 * time.Second; res.takeover >= maxReleaseTakeover {
		t.Fatalf("takeover took %s, want < %s — looks like the Lease expired instead of being released on cancel", res.takeover, maxReleaseTakeover)
	}
}

// TestLeaderFailoverLeaseExpiry covers the crash-like path — and the ONLY
// path production has today, since cmd/k8s-bridge/main.go leaves
// LeaderElectionReleaseOnCancel unset: the leader disappears without
// touching the Lease, which keeps naming it until LeaseDuration elapses;
// only then may the standby acquire. Cancelling A's context without
// release-on-cancel is behaviorally identical to a crash from the Lease's
// point of view: no release is written either way.
func TestLeaderFailoverLeaseExpiry(t *testing.T) {
	res := runFailoverScenario(t, "failover-expiry", false)

	// The discriminator against the release path: takeover must have WAITED
	// for expiry. A renewed at most RetryPeriod(500ms) before the cancel and
	// the standby's own expiry clock starts when it OBSERVES a renewal, so
	// the earliest legal takeover is LeaseDuration-RetryPeriod = 5.5s after
	// the cancel; 3s leaves 2.5s of slack against clock skew and scheduling
	// noise while still cleanly separating the two paths (the release test
	// bounds its takeover at < 3s from the other side of the same line).
	if minExpiryTakeover := 3 * time.Second; res.takeover < minExpiryTakeover {
		t.Fatalf("takeover took only %s, want >= %s — the standby acquired before the Lease could have expired, so this did not exercise the expiry path", res.takeover, minExpiryTakeover)
	}
}
