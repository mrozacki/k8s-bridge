// Unit tests for the ADR-0015 Phase A supervisor state machine: per-CR
// lifecycle (start/hot-reload/restart/stop), the (slurmRestURL, partition)
// conflict rule, invalid-spec handling, and readiness aggregation. Envtest
// coverage of the same flows against a real apiserver + manager lives in
// supervisor_integration_test.go.
package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/mrozacki/k8s-bridge/api/v1alpha1"
	"github.com/mrozacki/k8s-bridge/internal/config"
)

func supervisorTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := testScheme(t)
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

// specWith builds a valid spec bound to a given Slurm URL and partitions.
func specWith(url string, partitions ...string) v1alpha1.WorkloadMixingSpec {
	spec := validSpec()
	spec.SlurmRestURL = url
	spec.PartitionMappings = nil
	for _, p := range partitions {
		spec.PartitionMappings = append(spec.PartitionMappings,
			v1alpha1.PartitionMapping{PartitionName: p, WorkloadPriorityClass: "normal-priority"})
	}
	return spec
}

// startedSupervisor builds a Supervisor over the given CRs and runs Start on
// a test-scoped context so Reconcile has a leader context to attach Bridge
// goroutines to. Each CR gets its own fakeSlurm from the factory.
func startedSupervisor(t *testing.T, crs ...*v1alpha1.WorkloadMixing) *Supervisor {
	t.Helper()
	objs := make([]runtime.Object, 0, len(crs))
	for _, cr := range crs {
		objs = append(objs, cr)
	}
	builder := fake.NewClientBuilder().WithScheme(supervisorTestScheme(t)).WithStatusSubresource(&v1alpha1.WorkloadMixing{})
	for _, o := range objs {
		builder = builder.WithRuntimeObjects(o)
	}
	s := &Supervisor{
		Kube:                  builder.Build(),
		Namespace:             "slurm-jobs",
		Log:                   slog.Default(),
		NewSlurmClient:        func(*config.Config) (SlurmAPI, error) { return &fakeSlurm{}, nil },
		ConflictRetryInterval: 25 * time.Millisecond,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = s.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	waitForCond(t, "supervisor started", func() bool { return s.isStarted() })
	return s
}

func waitForCond(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		if cond() {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func mustReconcile(t *testing.T, s *Supervisor, name string) reconcile.Result {
	t.Helper()
	res, err := s.Reconcile(context.Background(),
		reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "slurm-jobs", Name: name}})
	if err != nil {
		t.Fatalf("Reconcile(%s): %v", name, err)
	}
	return res
}

func readyCondition(t *testing.T, s *Supervisor, name string) *metav1.Condition {
	t.Helper()
	wm := &v1alpha1.WorkloadMixing{}
	if err := s.Kube.Get(context.Background(), types.NamespacedName{Namespace: "slurm-jobs", Name: name}, wm); err != nil {
		t.Fatalf("fetching %s: %v", name, err)
	}
	return apimeta.FindStatusCondition(wm.Status.Conditions, "Ready")
}

// TestBridgeIdentityConflicts is the table for ADR-0015 §Decision 3:
// uniqueness is judged on (slurmRestURL, partitionName) pairs.
func TestBridgeIdentityConflicts(t *testing.T) {
	id := func(url string, partitions ...string) bridgeIdentity {
		parts := map[string]struct{}{}
		for _, p := range partitions {
			parts[p] = struct{}{}
		}
		return bridgeIdentity{endpoint: endpointIdentity{url: url}, partitions: parts}
	}
	cases := []struct {
		name string
		a, b bridgeIdentity
		want bool
	}{
		{"same URL, same partition", id("https://x:6820", "mixing"), id("https://x:6820", "mixing"), true},
		{"same URL, disjoint partitions", id("https://x:6820", "mixing"), id("https://x:6820", "batch"), false},
		{"different URL, same partition", id("https://x:6820", "mixing"), id("https://y:6820", "mixing"), false},
		{"overlap within larger sets", id("https://x:6820", "a", "b", "c"), id("https://x:6820", "c", "d"), true},
		{"multiple partitions, no overlap", id("https://x:6820", "a", "b"), id("https://x:6820", "c", "d"), false},
		// Exact string comparison is the documented contract: a trailing
		// slash makes a "different" endpoint (no URL normalization).
		{"same endpoint spelled differently", id("https://x:6820", "mixing"), id("https://x:6820/", "mixing"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.conflictsWith(tc.b); got != tc.want {
				t.Errorf("conflictsWith = %v, want %v", got, tc.want)
			}
			if got := tc.b.conflictsWith(tc.a); got != tc.want {
				t.Errorf("conflictsWith (reversed) = %v, want %v (rule must be symmetric)", got, tc.want)
			}
		})
	}
}

// TestSupervisorReconcileBeforeStartRequeues: the WorkloadMixing watch
// controller and the supervisor Runnable start concurrently in the manager's
// leader-election group, so an early reconcile must requeue, not error or
// drop the CR.
func TestSupervisorReconcileBeforeStartRequeues(t *testing.T) {
	s := &Supervisor{
		Kube:      fake.NewClientBuilder().WithScheme(supervisorTestScheme(t)).Build(),
		Namespace: "slurm-jobs",
		Log:       slog.Default(),
	}
	res, err := s.Reconcile(context.Background(),
		reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "slurm-jobs", Name: "wm"}})
	if err != nil {
		t.Fatalf("Reconcile before Start: %v", err)
	}
	if res.RequeueAfter != startupRetryInterval {
		t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, startupRetryInterval)
	}
}

// TestSupervisorLifecyclePerCR covers add → running, delete → stopped, and
// that deletion of one CR leaves another CR's loop untouched.
func TestSupervisorLifecyclePerCR(t *testing.T) {
	wmA := newWorkloadMixing("wm-a", "slurm-jobs", specWith("https://a:6820", "mixing"))
	wmA.Generation = 1
	wmB := newWorkloadMixing("wm-b", "slurm-jobs", specWith("https://b:6820", "mixing"))
	wmB.Generation = 1
	s := startedSupervisor(t, wmA, wmB)

	mustReconcile(t, s, "wm-a")
	mustReconcile(t, s, "wm-b")
	entryA, okA := s.get(types.NamespacedName{Namespace: "slurm-jobs", Name: "wm-a"})
	entryB, okB := s.get(types.NamespacedName{Namespace: "slurm-jobs", Name: "wm-b"})
	if !okA || !okB {
		t.Fatalf("running = %v/%v, want both bridges running", okA, okB)
	}

	// Each Bridge ticks on its own loop; its first tick against the empty
	// fakeSlurm succeeds and must surface as Ready=True on ITS CR.
	waitForCond(t, "wm-a Ready=True", func() bool {
		c := readyCondition(t, s, "wm-a")
		return c != nil && c.Status == metav1.ConditionTrue && c.Reason == ReasonTickSucceeded
	})

	// Delete wm-a: its loop stops, wm-b's is unaffected.
	if err := s.Kube.Delete(context.Background(), wmA); err != nil {
		t.Fatal(err)
	}
	mustReconcile(t, s, "wm-a")
	if _, still := s.get(types.NamespacedName{Namespace: "slurm-jobs", Name: "wm-a"}); still {
		t.Error("wm-a still tracked as running after CR deletion")
	}
	select {
	case <-entryA.done:
	case <-time.After(5 * time.Second):
		t.Fatal("wm-a's Run loop did not stop after CR deletion")
	}
	select {
	case <-entryB.done:
		t.Fatal("wm-b's Run loop stopped when wm-a was deleted (no crash isolation)")
	default:
	}
}

// updateSpec mutates the named CR's spec and stamps a new generation,
// retrying on optimistic-concurrency conflicts. Once a bridge is running,
// its status writer updates the SAME object concurrently with the test's
// Get→mutate→Update, so a bare Update flakes with "object was modified"
// (seen live in CI). Each retry re-Gets a fresh copy before mutating.
func updateSpec(t *testing.T, s *Supervisor, key types.NamespacedName, generation int64, mutate func(*v1alpha1.WorkloadMixing)) {
	t.Helper()
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		wm := &v1alpha1.WorkloadMixing{}
		if err := s.Kube.Get(context.Background(), key, wm); err != nil {
			return err
		}
		mutate(wm)
		wm.Generation = generation // the fake client does not bump generation itself
		return s.Kube.Update(context.Background(), wm)
	}); err != nil {
		t.Fatal(err)
	}
}

// TestSupervisorHotReloadVsRestart pins the restart rule: a spec change that
// leaves the Slurm client's construction-time fields intact is applied to
// the SAME Bridge via setCfg; a change to any of those fields replaces the
// Bridge (new client, new loop).
func TestSupervisorHotReloadVsRestart(t *testing.T) {
	wm := newWorkloadMixing("wm", "slurm-jobs", specWith("https://a:6820", "mixing"))
	wm.Generation = 1
	s := startedSupervisor(t, wm)
	key := types.NamespacedName{Namespace: "slurm-jobs", Name: "wm"}

	mustReconcile(t, s, "wm")
	entry1, _ := s.get(key)
	cfg1 := entry1.bridge.cfgSnapshot()

	// Same generation reconcile (status-only update / resync): a no-op —
	// not even a redundant setCfg.
	mustReconcile(t, s, "wm")
	if got, _ := s.get(key); got.bridge != entry1.bridge || got.bridge.cfgSnapshot() != cfg1 {
		t.Fatal("same-generation reconcile must not touch the running bridge or its config")
	}

	// Hot-reload: localQueue changes, endpoint fields don't.
	updateSpec(t, s, key, 2, func(wm *v1alpha1.WorkloadMixing) {
		wm.Spec.LocalQueue = "other-queue"
	})
	mustReconcile(t, s, "wm")
	entry2, _ := s.get(key)
	if entry2.bridge != entry1.bridge {
		t.Fatal("endpoint-preserving spec change must hot-reload the SAME bridge, not restart it")
	}
	if got := entry2.bridge.cfgSnapshot().LocalQueue; got != "other-queue" {
		t.Fatalf("LocalQueue after hot-reload = %q, want other-queue", got)
	}

	// Restart: the slurmRestURL is baked into the client, so the bridge must
	// be replaced and the old loop stopped.
	updateSpec(t, s, key, 3, func(wm *v1alpha1.WorkloadMixing) {
		wm.Spec.SlurmRestURL = "https://b:6820"
	})
	mustReconcile(t, s, "wm")
	entry3, _ := s.get(key)
	if entry3.bridge == entry1.bridge {
		t.Fatal("slurmRestURL change must restart the bridge (stale slurm client otherwise)")
	}
	select {
	case <-entry1.done:
	case <-time.After(5 * time.Second):
		t.Fatal("old bridge loop still running after endpoint-change restart")
	}
	if got := entry3.bridge.cfgSnapshot().SlurmRestURL; got != "https://b:6820" {
		t.Fatalf("SlurmRestURL after restart = %q", got)
	}
}

// TestSupervisorConflictingSpecRefused is ADR-0015 §Decision 3 end to end:
// the second CR claiming the same (slurmRestURL, partition) is not started,
// reports Ready=False/ConflictingSpec naming the CR it lost to, and STARTS
// once the older CR goes away (via the timed requeue's re-reconcile).
func TestSupervisorConflictingSpecRefused(t *testing.T) {
	wmA := newWorkloadMixing("wm-a", "slurm-jobs", specWith("https://x:6820", "mixing", "batch"))
	wmA.Generation = 1
	wmB := newWorkloadMixing("wm-b", "slurm-jobs", specWith("https://x:6820", "batch"))
	wmB.Generation = 1
	s := startedSupervisor(t, wmA, wmB)
	keyB := types.NamespacedName{Namespace: "slurm-jobs", Name: "wm-b"}

	mustReconcile(t, s, "wm-a")
	res := mustReconcile(t, s, "wm-b")
	if _, running := s.get(keyB); running {
		t.Fatal("conflicting CR was started; it must be refused")
	}
	if res.RequeueAfter != s.ConflictRetryInterval {
		t.Errorf("RequeueAfter = %v, want %v (blocked CRs retry on a timer)", res.RequeueAfter, s.ConflictRetryInterval)
	}
	cond := readyCondition(t, s, "wm-b")
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != ReasonConflictingSpec {
		t.Fatalf("Ready condition = %+v, want False/ConflictingSpec", cond)
	}
	if !strings.Contains(cond.Message, "wm-a") {
		t.Errorf("condition message %q must name the conflicting CR wm-a", cond.Message)
	}

	// Deleting the older CR frees the claim; the next (timer-driven)
	// reconcile of wm-b must start it.
	if err := s.Kube.Delete(context.Background(), wmA); err != nil {
		t.Fatal(err)
	}
	mustReconcile(t, s, "wm-a")
	mustReconcile(t, s, "wm-b")
	if _, running := s.get(keyB); !running {
		t.Fatal("wm-b did not start after the conflicting CR was deleted")
	}
}

// TestSupervisorInvalidSpec covers both invalid-spec branches: with no
// running bridge there is nothing to run (Ready=False/InvalidSpec, no
// requeue — the next spec edit delivers its own event); with a running
// bridge the last-good config keeps running (single-CR hot-reload parity).
func TestSupervisorInvalidSpec(t *testing.T) {
	spec := specWith("https://a:6820", "mixing")
	spec.PartitionMappings = nil // fails Validate: at least one mapping required
	wm := newWorkloadMixing("wm", "slurm-jobs", spec)
	wm.Generation = 1
	s := startedSupervisor(t, wm)
	key := types.NamespacedName{Namespace: "slurm-jobs", Name: "wm"}

	res := mustReconcile(t, s, "wm")
	if res != (reconcile.Result{}) {
		t.Errorf("result = %+v, want zero (no requeue for invalid spec)", res)
	}
	if _, running := s.get(key); running {
		t.Fatal("bridge running despite invalid spec")
	}
	cond := readyCondition(t, s, "wm")
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != ReasonInvalidSpec {
		t.Fatalf("Ready condition = %+v, want False/InvalidSpec", cond)
	}

	// Fix the spec, start the bridge, then break it again: the bridge must
	// keep running on the last-good config.
	updateSpec(t, s, key, 2, func(wm *v1alpha1.WorkloadMixing) {
		wm.Spec = specWith("https://a:6820", "mixing")
	})
	mustReconcile(t, s, "wm")
	entry, running := s.get(key)
	if !running {
		t.Fatal("bridge not started after the spec was fixed")
	}

	updateSpec(t, s, key, 3, func(wm *v1alpha1.WorkloadMixing) {
		wm.Spec.PartitionMappings = nil
	})
	mustReconcile(t, s, "wm")
	got, stillRunning := s.get(key)
	if !stillRunning || got.bridge != entry.bridge {
		t.Fatal("a bad spec edit must keep the running bridge on its last-good config, not stop or restart it")
	}
	cond = readyCondition(t, s, "wm")
	if cond == nil || cond.Status != metav1.ConditionFalse || !strings.Contains(cond.Message, "keeping previous config") {
		t.Fatalf("Ready condition = %+v, want False mentioning last-good config retention", cond)
	}
	if cond.Reason != ReasonInvalidSpec {
		t.Errorf("Ready reason = %q, want InvalidSpec", cond.Reason)
	}
	if cond.ObservedGeneration != 3 {
		t.Errorf("ObservedGeneration = %d, want 3 (the rejection must speak for the LATEST, invalid generation)", cond.ObservedGeneration)
	}

	// The race CI caught, forced deterministically: the still-running
	// bridge keeps ticking healthily on its last-good config — several
	// tick reports AFTER the rejection must all be suppressed by the shared
	// ReadyConditionWriter, never flipping the condition back to
	// True/TickSucceeded (which would stamp the invalid generation as
	// healthy). OnTick is invoked directly so the reports are synchronous
	// and the assertion cannot pass by timing luck on a fast machine.
	for i := 0; i < 5; i++ {
		got.bridge.OnTick(nil)
		cond = readyCondition(t, s, "wm")
		if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != ReasonInvalidSpec {
			t.Fatalf("Ready = %+v after healthy tick %d during rejection, want False/InvalidSpec to stick", cond, i+1)
		}
	}

	// Fixing the spec hands the condition back to tick health: the very
	// next tick report must write True even though the bridge's tick state
	// was "ok" the whole time (the ClearRejection debounce reset).
	updateSpec(t, s, key, 4, func(wm *v1alpha1.WorkloadMixing) {
		wm.Spec = specWith("https://a:6820", "mixing")
	})
	mustReconcile(t, s, "wm")
	got, _ = s.get(key)
	got.bridge.OnTick(nil)
	cond = readyCondition(t, s, "wm")
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != ReasonTickSucceeded {
		t.Fatalf("Ready = %+v after fixed spec + healthy tick, want True/TickSucceeded (stale rejection must not stick)", cond)
	}
}

// TestSupervisorImageAllowlist: the deploy-time slurmd image trust anchor
// (C1) applies per CR in supervisor mode — a disallowed image is refused
// like any other invalid spec instead of killing the process the way the
// single-CR bootstrap does.
func TestSupervisorImageAllowlist(t *testing.T) {
	wm := newWorkloadMixing("wm", "slurm-jobs", specWith("https://a:6820", "mixing"))
	wm.Generation = 1
	s := startedSupervisor(t, wm)
	s.AllowedSlurmdImages = []string{"registry.example.com/slurmd"}

	mustReconcile(t, s, "wm")
	if _, running := s.get(types.NamespacedName{Namespace: "slurm-jobs", Name: "wm"}); running {
		t.Fatal("bridge started despite slurmd image outside the allowlist")
	}
	cond := readyCondition(t, s, "wm")
	if cond == nil || cond.Reason != ReasonInvalidSpec || !strings.Contains(cond.Message, "allowed-slurmd-images") {
		t.Fatalf("Ready condition = %+v, want InvalidSpec naming the allowlist", cond)
	}
}

// TestSupervisorTokenPathAllowlist: the deploy-time token-path trust anchor
// (H2) applies per CR in supervisor mode. The CR author picks both the file
// the controller reads and the URL its contents are sent to, so a path
// outside the allowlist must refuse THAT CR (Ready=False/InvalidSpec) rather
// than let the controller read, say, its own ServiceAccount token and post it
// to an endpoint the same CR names.
func TestSupervisorTokenPathAllowlist(t *testing.T) {
	spec := specWith("https://a:6820", "mixing")
	spec.SlurmTokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	wm := newWorkloadMixing("wm", "slurm-jobs", spec)
	wm.Generation = 1
	s := startedSupervisor(t, wm)
	s.AllowedTokenPaths = []string{"/var/run/secrets/slurm/", "/etc/k8s-bridge/"}

	mustReconcile(t, s, "wm")
	if _, running := s.get(types.NamespacedName{Namespace: "slurm-jobs", Name: "wm"}); running {
		t.Fatal("bridge started despite slurmTokenFile outside the allowlist")
	}
	cond := readyCondition(t, s, "wm")
	if cond == nil || cond.Reason != ReasonInvalidSpec || !strings.Contains(cond.Message, "allowed-token-paths") {
		t.Fatalf("Ready condition = %+v, want InvalidSpec naming the token-path allowlist", cond)
	}

	// An allowed path under the same anchor starts normally — the check must
	// gate the escape, not the feature.
	spec.SlurmTokenFile = "/var/run/secrets/slurm/token"
	ok := newWorkloadMixing("ok", "slurm-jobs", spec)
	ok.Generation = 1
	s2 := startedSupervisor(t, ok)
	s2.AllowedTokenPaths = []string{"/var/run/secrets/slurm/", "/etc/k8s-bridge/"}
	mustReconcile(t, s2, "ok")
	if _, running := s2.get(types.NamespacedName{Namespace: "slurm-jobs", Name: "ok"}); !running {
		t.Fatalf("bridge did not start for an allowed token path; Ready = %+v", readyCondition(t, s2, "ok"))
	}
}

// TestSupervisorInsecureTLSGate: slurmInsecureSkipTLSVerify (L8) is a
// platform-admin decision. A CR asking to skip certificate verification is
// refused unless the controller was started with --allow-insecure-tls; with
// the flag on, the same CR runs (the gate must not break the dev escape hatch,
// only move the acknowledgement to whoever deploys the controller).
func TestSupervisorInsecureTLSGate(t *testing.T) {
	spec := specWith("https://a:6820", "mixing")
	spec.SlurmInsecureSkipTLSVerify = true
	wm := newWorkloadMixing("wm", "slurm-jobs", spec)
	wm.Generation = 1
	s := startedSupervisor(t, wm)

	mustReconcile(t, s, "wm")
	if _, running := s.get(types.NamespacedName{Namespace: "slurm-jobs", Name: "wm"}); running {
		t.Fatal("bridge started despite slurmInsecureSkipTLSVerify without --allow-insecure-tls")
	}
	cond := readyCondition(t, s, "wm")
	if cond == nil || cond.Reason != ReasonInvalidSpec || !strings.Contains(cond.Message, "allow-insecure-tls") {
		t.Fatalf("Ready condition = %+v, want InvalidSpec naming the flag", cond)
	}

	ok := newWorkloadMixing("ok", "slurm-jobs", spec)
	ok.Generation = 1
	s2 := startedSupervisor(t, ok)
	s2.AllowInsecureTLS = true
	mustReconcile(t, s2, "ok")
	if _, running := s2.get(types.NamespacedName{Namespace: "slurm-jobs", Name: "ok"}); !running {
		t.Fatalf("bridge did not start with --allow-insecure-tls; Ready = %+v", readyCondition(t, s2, "ok"))
	}
}

// TestSupervisorStartFailedRetries: a slurm-client construction failure
// (typically the token Secret not mounted yet) reports StartFailed and
// retries on the timer; once the factory heals, the bridge starts.
func TestSupervisorStartFailedRetries(t *testing.T) {
	wm := newWorkloadMixing("wm", "slurm-jobs", specWith("https://a:6820", "mixing"))
	wm.Generation = 1
	s := startedSupervisor(t, wm)
	fail := true
	s.NewSlurmClient = func(*config.Config) (SlurmAPI, error) {
		if fail {
			return nil, errors.New("reading token file: no such file")
		}
		return &fakeSlurm{}, nil
	}

	res := mustReconcile(t, s, "wm")
	if res.RequeueAfter != s.ConflictRetryInterval {
		t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, s.ConflictRetryInterval)
	}
	cond := readyCondition(t, s, "wm")
	if cond == nil || cond.Reason != ReasonStartFailed {
		t.Fatalf("Ready condition = %+v, want StartFailed", cond)
	}

	fail = false
	mustReconcile(t, s, "wm")
	if _, running := s.get(types.NamespacedName{Namespace: "slurm-jobs", Name: "wm"}); !running {
		t.Fatal("bridge not started after the slurm client factory recovered")
	}
}

// TestSupervisorReadyzAggregation pins the /readyz contract of ADR-0015
// Phase A: 503 before leadership, 200 on an empty namespace, all-running-
// bridges-ready aggregation with per-CR detail in the body, and blocked CRs
// listed without failing the verdict.
func TestSupervisorReadyzAggregation(t *testing.T) {
	probe := func(s *Supervisor) (int, supervisorReadyz) {
		rec := httptest.NewRecorder()
		s.ReadyzHandler()(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		var body supervisorReadyz
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("readyz body is not JSON: %v (%s)", err, rec.Body.String())
		}
		return rec.Code, body
	}

	// Not the leader yet: 503 by design (single-CR standby parity).
	cold := &Supervisor{Namespace: "slurm-jobs", Log: slog.Default()}
	if code, body := probe(cold); code != http.StatusServiceUnavailable || !strings.Contains(body.Message, "leader") {
		t.Errorf("pre-leadership readyz = %d %q, want 503 naming leader election", code, body.Message)
	}

	// Leader with an empty namespace: ready, with an explanatory message.
	empty := startedSupervisor(t)
	if code, body := probe(empty); code != http.StatusOK || !strings.Contains(body.Message, "no WorkloadMixing") {
		t.Errorf("empty-namespace readyz = %d %q, want 200 explaining the empty namespace", code, body.Message)
	}

	// Two running bridges: ready only once BOTH have a fresh successful tick.
	wmA := newWorkloadMixing("wm-a", "slurm-jobs", specWith("https://a:6820", "mixing"))
	wmA.Generation = 1
	wmB := newWorkloadMixing("wm-b", "slurm-jobs", specWith("https://b:6820", "mixing"))
	wmB.Generation = 1
	s := startedSupervisor(t, wmA, wmB)
	mustReconcile(t, s, "wm-a")
	mustReconcile(t, s, "wm-b")

	entryA, _ := s.get(types.NamespacedName{Namespace: "slurm-jobs", Name: "wm-a"})
	entryB, _ := s.get(types.NamespacedName{Namespace: "slurm-jobs", Name: "wm-b"})
	entryA.bridge.recordTickOutcome(nil)
	// One bridge un-ready (no successful tick yet) must fail the aggregate and
	// be attributable in the body. Re-inject the failure INSIDE the poll:
	// startedSupervisor's bridges run REAL tick loops against a fakeSlurm that
	// SUCCEEDS, so wm-b's own background tick writes recordTickOutcome(nil) and
	// races the failure we want to observe. On a slow/-race CI runner that
	// background success could land last and this wait would time out (observed
	// flake 2026-07-13, job failed at the 5s deadline). Asserting the failure
	// each iteration makes it the authoritative last write and converges.
	waitForCond(t, "aggregate 503 while wm-b has no successful tick", func() bool {
		entryB.bridge.recordTickOutcome(errors.New("slurmrestd unreachable"))
		code, body := probe(s)
		br, ok := body.Bridges["slurm-jobs/wm-b"]
		return code == http.StatusServiceUnavailable && ok && !br.Ready
	})

	entryB.bridge.recordTickOutcome(nil)
	code, body := probe(s)
	if code != http.StatusOK {
		t.Fatalf("readyz = %d after both bridges ticked successfully, want 200 (%+v)", code, body)
	}
	if len(body.Bridges) != 2 || !body.Bridges["slurm-jobs/wm-a"].Ready || !body.Bridges["slurm-jobs/wm-b"].Ready {
		t.Errorf("body.Bridges = %+v, want per-CR readiness for both", body.Bridges)
	}

	// A blocked CR (conflict with wm-a's universe) shows up in the body but
	// does not flip the verdict.
	wmC := newWorkloadMixing("wm-c", "slurm-jobs", specWith("https://a:6820", "mixing"))
	wmC.Generation = 1
	if err := s.Kube.Create(context.Background(), wmC); err != nil {
		t.Fatal(err)
	}
	mustReconcile(t, s, "wm-c")
	code, body = probe(s)
	if code != http.StatusOK {
		t.Errorf("readyz = %d with a blocked CR, want 200 (blocked CRs alert via conditions, not the probe)", code)
	}
	if note, ok := body.Blocked["slurm-jobs/wm-c"]; !ok || !strings.Contains(note, ReasonConflictingSpec) {
		t.Errorf("body.Blocked = %+v, want wm-c listed with ConflictingSpec", body.Blocked)
	}
}
