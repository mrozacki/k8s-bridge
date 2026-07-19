package bridge

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/mrozacki/k8s-bridge/internal/config"
	"github.com/mrozacki/k8s-bridge/internal/slurm"
)

// benchScheme mirrors testScheme but accepts testing.TB so it can be shared
// between benchmarks (*testing.B) and the API-call-budget test (*testing.T)
// in this file, without changing testScheme's existing *testing.T-only
// signature used throughout reconciler_test.go.
func benchScheme(tb testing.TB) *runtime.Scheme {
	tb.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		tb.Fatal(err)
	}
	if err := jobsetv1alpha2.AddToScheme(scheme); err != nil {
		tb.Fatal(err)
	}
	// L6: kueue.go now issues LIST/watch against v1beta2 (API graduation from
	// v1beta1) — the fake client's scheme must register the same version the
	// production code queries, or the snapshot LIST would find nothing.
	wlGVK := schema.GroupVersionKind{Group: "kueue.x-k8s.io", Version: "v1beta2", Kind: "Workload"}
	scheme.AddKnownTypeWithName(wlGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(wlGVK.GroupVersion().WithKind("WorkloadList"), &unstructured.UnstructuredList{})
	return scheme
}

// nHeldJobsBridge builds a Bridge and its backing fake client preloaded with
// n held+pending jobs, each with an already-admitted Workload, so a tick
// exercises the full create/pin/release path for every job (the worst case
// for API-call volume per audit P7/P8's O(n) fix). It returns the Bridge,
// the fakeSlurm double (to inspect call counts), and the fake client.
//
// P5: CreateWorkers is left at its zero value here, which admitHeldJobs
// treats the same as 1 (sequential) — this is deliberate. Both
// TestTickAPICallBudgetForHeldJobs and BenchmarkTick assert exact call
// counts/orderings; keeping the pool at effective size 1 keeps them
// deterministic. TestAdmitHeldJobsCreatesAllJobSetsWithRealParallelism below
// is the one test that deliberately sets CreateWorkers > 1 to exercise real
// concurrency.
func nHeldJobsBridge(tb testing.TB, n int) (*Bridge, *fakeSlurm, client.Client) {
	tb.Helper()
	cfg := &config.Config{
		SlurmRestURL: "http://fake",
		Namespace:    "slurm-jobs",
		LocalQueue:   "main",
		PartitionMappings: []config.PartitionMapping{
			{PartitionName: "mixing", WorkloadPriorityClass: "normal-priority"},
		},
		Slurmd: config.Slurmd{
			Image:          "slurmd:test",
			ConfServer:     "ctl:6817",
			AuthSecretName: "slurm-auth-slurm",
		},
	}

	jobs := make([]slurm.Job, n)
	objs := make([]client.Object, 0, n)
	for i := 0; i < n; i++ {
		id := uint64(10000 + i)
		jobs[i] = slurm.Job{
			JobID:       id,
			Partition:   "mixing",
			JobState:    []string{"PENDING"},
			StateReason: "JobHeldUser",
			Hold:        true,
			Tasks:       slurm.Uint64NoVal{Set: true, Number: 2},
			CPUsPerTask: slurm.Uint64NoVal{Set: true, Number: 1},
			Comment:     "wm: held for admission by k8s-bridge",
		}
		jobsetName := fmt.Sprintf("slurm-job-%d", id)
		objs = append(objs, admittedWorkload(jobsetName))
	}
	fs := &fakeSlurm{jobs: jobs}
	kube := fake.NewClientBuilder().WithScheme(benchScheme(tb)).WithObjects(objs...).Build()
	return New(cfg, kube, fs, slog.New(slog.NewTextHandler(discardWriter{}, nil))), fs, kube
}

// discardWriter is an io.Writer that throws everything away, so the
// benchmark measures tick() cost, not stderr formatting/syscalls.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// BenchmarkTick is the audit AUD5/P7 regression vehicle: a scale run found
// (and fixed) an O(n^2) API-traffic bug that only a benchmark measuring a
// realistic backlog size could catch mechanically — unit tests alone proved
// behavior, not call volume. 1000 held jobs is the same order of magnitude
// as the scale runs.
func BenchmarkTick(b *testing.B) {
	b.ReportAllocs()
	bridge, _, _ := nHeldJobsBridge(b, 1000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := bridge.tick(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// TestTickAPICallBudgetForHeldJobs is the audit P7/AUD5 regression as a
// mechanical, always-run assertion (not just a benchmark a human has to
// remember to eyeball): one tick over N held jobs, all already present in
// ListJobs, must perform:
//   - exactly 1 ListJobs call (the once-per-tick snapshot, not once per job);
//   - 0 GetJob calls, since every owned job's JobSet appears in this tick's
//     ListJobs result (the audit P7 fix: GetJob is the fallback ONLY for IDs
//     absent from the list);
//   - exactly 2 slurmrestd mutations per job's held->released transition, in
//     that order: SetJobFeatures (pin to the dynamic nodes) then ReleaseJob
//     (lift the hold). P2 had merged these into one call to halve the budget
//     to 1*n; that merge is deliberately reverted — slurmrestd could apply
//     "hold": false without the "constraints" from the same body, releasing a
//     job before it was pinned (runaway job). 2*n is the correct budget now,
//     and the ORDER is the load-bearing part.
func TestTickAPICallBudgetForHeldJobs(t *testing.T) {
	const n = 1000
	bridge, fs, _ := nHeldJobsBridge(t, n)
	listCalls := 0
	fs2 := &fakeSlurmCallBudget{fakeSlurm: fs, onListJobs: func() { listCalls++ }}
	bridge.slurm = fs2

	if err := bridge.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}

	if listCalls != 1 {
		t.Errorf("ListJobs called %d times, want exactly 1 per tick regardless of backlog size", listCalls)
	}
	if fs2.getJobCalls != 0 {
		t.Errorf("GetJob called %d times, want 0 (every owned job present in this tick's ListJobs result)", fs2.getJobCalls)
	}
	if fs2.setJobFeaturesCalls != n {
		t.Errorf("SetJobFeatures called %d times, want exactly %d (one pin per job)", fs2.setJobFeaturesCalls, n)
	}
	if fs2.releaseJobCalls != n {
		t.Errorf("ReleaseJob called %d times, want exactly %d (one release per job)", fs2.releaseJobCalls, n)
	}
	// The runaway-job guard: a job must never be released before its
	// node-locking constraints have been accepted by slurmrestd.
	if fs2.releasedBeforeFeatures != 0 {
		t.Errorf("%d job(s) released before SetJobFeatures — runaway-job regression", fs2.releasedBeforeFeatures)
	}
}

// fakeSlurmCallBudget wraps fakeSlurm to count calls per method (rather than
// relying on map sizes, which same-value overwrites would under-count) for
// the API-call-budget assertion above.
//
// P5: ensureJobSet calls (and, transitively, the tick loop) may now run
// concurrently across a worker pool, so every counter mutation here goes
// through mu — the fake must be safe for concurrent use exactly like a real
// HTTP client would be under concurrent requests.
type fakeSlurmCallBudget struct {
	*fakeSlurm
	onListJobs  func()
	mu          sync.Mutex
	getJobCalls int
	// setJobFeaturesCalls and releaseJobCalls are the two per-job mutations of
	// the admission path. P2 once merged them into one; the merge is reverted
	// (see slurm.SetJobFeatures), so both are counted separately again and are
	// expected to be equal.
	setJobFeaturesCalls int
	releaseJobCalls     int
	// featuresSet records which jobs had their constraints accepted, so
	// ReleaseJob can detect a release that overtook its pin.
	featuresSet map[uint64]bool
	// releasedBeforeFeatures counts releases issued for a job whose
	// constraints were never accepted — the runaway-job regression.
	releasedBeforeFeatures int
}

func (f *fakeSlurmCallBudget) ListJobs(ctx context.Context, fn func(j slurm.Job) error) error {
	if f.onListJobs != nil {
		f.onListJobs()
	}
	return f.fakeSlurm.ListJobs(ctx, fn)
}

func (f *fakeSlurmCallBudget) GetJob(ctx context.Context, id uint64) (*slurm.Job, bool, error) {
	f.mu.Lock()
	f.getJobCalls++
	f.mu.Unlock()
	return f.fakeSlurm.GetJob(ctx, id)
}

func (f *fakeSlurmCallBudget) SetJobFeatures(ctx context.Context, id uint64, features string) error {
	err := f.fakeSlurm.SetJobFeatures(ctx, id, features)
	f.mu.Lock()
	f.setJobFeaturesCalls++
	if err == nil {
		if f.featuresSet == nil {
			f.featuresSet = map[uint64]bool{}
		}
		f.featuresSet[id] = true
	}
	f.mu.Unlock()
	return err
}

func (f *fakeSlurmCallBudget) ReleaseJob(ctx context.Context, id uint64) error {
	f.mu.Lock()
	f.releaseJobCalls++
	if !f.featuresSet[id] {
		f.releasedBeforeFeatures++
	}
	f.mu.Unlock()
	return f.fakeSlurm.ReleaseJob(ctx, id)
}

// TestAdmitHeldJobsCreatesAllJobSetsWithRealParallelism is the P5 regression
// exercising genuine concurrency (CreateWorkers > 1), unlike every other
// test in this package which deliberately keeps the pool at effective size 1
// for determinism. It only asserts END STATE (every job's JobSet exists and
// was released) — never call ORDER, which is inherently non-deterministic
// once more than one worker is involved. Run under `go test -race` (part of
// the standard verification suite) this also serves as the race-safety
// check for the parallel ensureJobSet path and the fake client it drives.
func TestAdmitHeldJobsCreatesAllJobSetsWithRealParallelism(t *testing.T) {
	const n = 200
	bridge, fs, kube := nHeldJobsBridge(t, n)
	cfg := *bridge.cfgSnapshot()
	cfg.CreateWorkers = 8 // real parallelism, not the deterministic default
	bridge.setCfg(&cfg)

	if err := bridge.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}

	for i := 0; i < n; i++ {
		id := uint64(10000 + i)
		name := fmt.Sprintf("slurm-job-%d", id)
		js := &jobsetv1alpha2.JobSet{}
		if err := kube.Get(context.Background(), client.ObjectKey{Namespace: "slurm-jobs", Name: name}, js); err != nil {
			t.Errorf("JobSet %s not created: %v", name, err)
		}
	}
	if len(fs.released) != n {
		t.Errorf("released %d jobs, want %d (every job admitted despite concurrent creation)", len(fs.released), n)
	}
}
