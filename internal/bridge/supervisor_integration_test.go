//go:build integration

package bridge

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/mrozacki/k8s-bridge/api/v1alpha1"
	"github.com/mrozacki/k8s-bridge/internal/config"
	"github.com/mrozacki/k8s-bridge/internal/slurm"
)

// fakeSlurmrestd is an httptest slurmrestd answering the only call an
// idle bridge makes (GET .../jobs, empty queue) and counting requests, so
// the test can prove WHICH bridge is polling WHICH Slurm cluster and
// whether a loop is still alive.
type fakeSlurmrestd struct {
	srv      *httptest.Server
	requests atomic.Int64
}

func newFakeSlurmrestd(t *testing.T) *fakeSlurmrestd {
	t.Helper()
	f := &fakeSlurmrestd{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		f.requests.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"jobs": [], "errors": [], "warnings": []}`))
	}))
	t.Cleanup(f.srv.Close)
	return f
}

// supervisedSpec builds a WorkloadMixing spec bound to a mock slurmrestd,
// with a 1s poll so the test observes several ticks quickly.
func supervisedSpec(url, partition string) v1alpha1.WorkloadMixingSpec {
	spec := validSpec()
	spec.SlurmRestURL = url
	spec.AllowInsecureHTTP = true // httptest serves plain http on 127.0.0.1
	spec.PollInterval = "1s"
	spec.PartitionMappings = []v1alpha1.PartitionMapping{
		{PartitionName: partition, WorkloadPriorityClass: "normal-priority"},
	}
	return spec
}

// TestSupervisorMultiCRAgainstRealAPIServer is the ADR-0015 Phase A envtest:
// the REAL manager/cache/watch chain drives the Supervisor exactly as
// cmd/k8s-bridge/main.go wires it (builder For(WorkloadMixing) + mgr.Add),
// with each CR's Bridge running a REAL slurm.Client against its own httptest
// slurmrestd. It covers, in order:
//  1. two CRs → two Bridges tick independently (distinct mock counters rise,
//     both CRs reach Ready=True through their own tick loops);
//  2. deleting one CR stops ITS loop (its mock goes quiet) while the other
//     keeps ticking — per-CR cancel, crash/lifecycle isolation;
//  3. a CR conflicting with a running one is refused with
//     Ready=False/ConflictingSpec naming the winner, then STARTS once the
//     winner is deleted (the timed-requeue retry path, no cross-CR enqueue).
func TestSupervisorMultiCRAgainstRealAPIServer(t *testing.T) {
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{"../../test/crd"},
		ErrorIfCRDPathMissing: true,
	}
	restCfg, err := env.Start()
	if err != nil {
		t.Fatalf("starting envtest (is KUBEBUILDER_ASSETS set? run via make test-integration): %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	scheme := wmScheme(t)
	mgr, err := manager.New(restCfg, manager.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		// Leader election off: leadership gating of plain Runnables is
		// already pinned by TestBridgeRunGatedByLeaderElection; the
		// Supervisor is gated identically (it does not implement
		// LeaderElectionRunnable), so this test focuses on the multi-CR
		// lifecycle itself.
		LeaderElection: false,
	})
	if err != nil {
		t.Fatalf("manager.New: %v", err)
	}
	kube := mgr.GetClient()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const ns = "supervised"
	if err := kube.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatal(err)
	}

	// The production slurm-client factory shape (real slurm.NewClient per
	// CR), minus the metrics wiring main.go adds.
	sup := &Supervisor{
		Kube:      kube,
		Namespace: ns,
		Log:       slog.Default(),
		NewSlurmClient: func(crCfg *config.Config) (SlurmAPI, error) {
			return slurm.NewClient(slurm.Options{
				BaseURL:        crCfg.SlurmRestURL,
				RequestTimeout: crCfg.SlurmRequestTimeout.Duration,
			})
		},
		Recorder:              mgr.GetEventRecorderFor("k8s-bridge-test"), //nolint:staticcheck // classic record.EventRecorder API, matching main.go; migration is separate
		ConflictRetryInterval: 500 * time.Millisecond,
	}
	if err := builder.ControllerManagedBy(mgr).
		Named("workloadmixing-supervisor").
		For(&v1alpha1.WorkloadMixing{}).
		Complete(sup); err != nil {
		t.Fatalf("wiring supervisor controller: %v", err)
	}
	if err := mgr.Add(sup); err != nil {
		t.Fatalf("registering supervisor runnable: %v", err)
	}
	mgrErr := make(chan error, 1)
	go func() { mgrErr <- mgr.Start(ctx) }()
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		t.Fatal("cache never synced")
	}

	waitCond := func(what string, timeout time.Duration, cond func() bool) {
		t.Helper()
		deadline := time.After(timeout)
		for {
			if cond() {
				return
			}
			select {
			case err := <-mgrErr:
				t.Fatalf("manager exited while waiting for %s: %v", what, err)
			case <-deadline:
				t.Fatalf("timed out waiting for %s", what)
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	readyCond := func(name string) *metav1.Condition {
		wm := &v1alpha1.WorkloadMixing{}
		if err := kube.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, wm); err != nil {
			return nil
		}
		return apimeta.FindStatusCondition(wm.Status.Conditions, "Ready")
	}

	// Phase 1: two CRs, two Slurm universes, two independent loops.
	mockA, mockB := newFakeSlurmrestd(t), newFakeSlurmrestd(t)
	wmA := newWorkloadMixing("wm-a", ns, supervisedSpec(mockA.srv.URL, "mixing"))
	wmB := newWorkloadMixing("wm-b", ns, supervisedSpec(mockB.srv.URL, "batch"))
	if err := kube.Create(ctx, wmA); err != nil {
		t.Fatal(err)
	}
	if err := kube.Create(ctx, wmB); err != nil {
		t.Fatal(err)
	}

	waitCond("both bridges polling their own slurmrestd", 30*time.Second, func() bool {
		return mockA.requests.Load() >= 2 && mockB.requests.Load() >= 2
	})
	waitCond("both CRs Ready=True via their own tick loops", 30*time.Second, func() bool {
		a, b := readyCond("wm-a"), readyCond("wm-b")
		return a != nil && a.Status == metav1.ConditionTrue && a.Reason == ReasonTickSucceeded &&
			b != nil && b.Status == metav1.ConditionTrue && b.Reason == ReasonTickSucceeded
	})

	// Phase 2: delete wm-b → its loop stops (mock B goes quiet), wm-a's
	// loop is untouched.
	if err := kube.Delete(ctx, wmB); err != nil {
		t.Fatal(err)
	}
	keyB := types.NamespacedName{Namespace: ns, Name: "wm-b"}
	waitCond("wm-b's bridge forgotten by the supervisor", 30*time.Second, func() bool {
		_, running := sup.get(keyB)
		return !running
	})
	// The loop is gone: its request counter must stay flat across several
	// would-be poll intervals, while wm-a's keeps climbing.
	quiesced := mockB.requests.Load()
	beforeA := mockA.requests.Load()
	time.Sleep(3 * time.Second) // 3x the 1s poll interval
	if got := mockB.requests.Load(); got != quiesced {
		t.Fatalf("deleted CR's slurmrestd still receiving requests (%d -> %d); its loop was not cancelled", quiesced, got)
	}
	if got := mockA.requests.Load(); got <= beforeA {
		t.Fatalf("surviving CR's bridge stopped polling (%d -> %d) when the other CR was deleted", beforeA, got)
	}

	// Phase 3: wm-c claims wm-a's (slurmRestURL, partition) → refused with
	// ConflictingSpec naming wm-a; deleting wm-a lets the timed requeue
	// start wm-c against mock A.
	wmC := newWorkloadMixing("wm-c", ns, supervisedSpec(mockA.srv.URL, "mixing"))
	if err := kube.Create(ctx, wmC); err != nil {
		t.Fatal(err)
	}
	waitCond("wm-c refused with ConflictingSpec naming wm-a", 30*time.Second, func() bool {
		c := readyCond("wm-c")
		return c != nil && c.Status == metav1.ConditionFalse &&
			c.Reason == ReasonConflictingSpec && strings.Contains(c.Message, "wm-a")
	})
	if _, running := sup.get(types.NamespacedName{Namespace: ns, Name: "wm-c"}); running {
		t.Fatal("conflicting CR has a running bridge; it must be refused")
	}

	if err := kube.Delete(ctx, wmA); err != nil {
		t.Fatal(err)
	}
	waitCond("wm-c started once wm-a was deleted (timed-requeue retry)", 30*time.Second, func() bool {
		c := readyCond("wm-c")
		return c != nil && c.Status == metav1.ConditionTrue && c.Reason == ReasonTickSucceeded
	})
	if _, running := sup.get(types.NamespacedName{Namespace: ns, Name: "wm-c"}); !running {
		t.Fatal("wm-c Ready=True but not tracked as running")
	}
}
