package bridge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/mrozacki/k8s-bridge/internal/config"
	"github.com/mrozacki/k8s-bridge/internal/metrics"
	"github.com/mrozacki/k8s-bridge/internal/slurm"
	"github.com/mrozacki/k8s-bridge/internal/translate"
)

// fakeSlurm is an in-memory SlurmAPI double. Tests mutate its fields to
// steer each scenario and inspect the recorded calls afterwards.
type fakeSlurm struct {
	jobs          []slurm.Job
	featureErr    error
	listJobsErr   error
	cancelErr     error
	deleteNodeErr error
	features      map[uint64]string
	released      []uint64
	cancelled     []uint64
	deletedNodes  []string
	comments      map[uint64]string
	adminComments map[uint64]string
	priorities    map[uint64]uint32
	getJob        func(id uint64) (*slurm.Job, bool, error)
	// getJobCalls counts GetJob invocations (audit P7 regression: cleanup
	// must consult the tick's ListJobs result first and call GetJob only for
	// job IDs absent from it).
	getJobCalls int
}

func (f *fakeSlurm) ListJobs(_ context.Context, fn func(j slurm.Job) error) error {
	if f.listJobsErr != nil {
		return f.listJobsErr
	}
	for _, j := range f.jobs {
		if err := fn(j); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeSlurm) GetJob(_ context.Context, id uint64) (*slurm.Job, bool, error) {
	f.getJobCalls++
	if f.getJob != nil {
		return f.getJob(id)
	}
	for i := range f.jobs {
		if f.jobs[i].JobID == id {
			return &f.jobs[i], true, nil
		}
	}
	return nil, false, nil
}

// SetJobFeatures records the node-locking constraints update. featureErr
// simulates the "nodes not registered yet" retry path, in which case nothing is
// recorded and the caller must NOT go on to release the job.
func (f *fakeSlurm) SetJobFeatures(_ context.Context, id uint64, features string) error {
	if f.featureErr != nil {
		return f.featureErr
	}
	if f.features == nil {
		f.features = map[uint64]string{}
	}
	f.features[id] = features
	return nil
}

// ReleaseJob records the hold being lifted. P2 once merged this with the
// constraints update; the calls are deliberately separate now (see
// slurm.SetJobFeatures on runaway jobs), so a release recorded for a job with
// no features entry is the regression this fake makes visible.
func (f *fakeSlurm) ReleaseJob(_ context.Context, id uint64) error {
	f.released = append(f.released, id)
	return nil
}

func (f *fakeSlurm) CancelJob(_ context.Context, id uint64) error {
	if f.cancelErr != nil {
		return f.cancelErr
	}
	f.cancelled = append(f.cancelled, id)
	return nil
}

func (f *fakeSlurm) DeleteNode(_ context.Context, name string) error {
	if f.deleteNodeErr != nil {
		return f.deleteNodeErr
	}
	f.deletedNodes = append(f.deletedNodes, name)
	return nil
}

func (f *fakeSlurm) SetJobComment(_ context.Context, id uint64, comment string) error {
	if f.comments == nil {
		f.comments = map[uint64]string{}
	}
	f.comments[id] = comment
	return nil
}

func (f *fakeSlurm) SetJobAdminComment(_ context.Context, id uint64, comment string) error {
	if f.adminComments == nil {
		f.adminComments = map[uint64]string{}
	}
	f.adminComments[id] = comment
	return nil
}

func (f *fakeSlurm) SetJobPriority(_ context.Context, id uint64, priority uint32) error {
	if f.priorities == nil {
		f.priorities = map[uint64]uint32{}
	}
	f.priorities[id] = priority
	return nil
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := jobsetv1alpha2.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	// Register Kueue Workload as unstructured so the fake client can serve
	// the snapshot's Workload LIST. L6: v1beta2 (API graduation from
	// v1beta1 — a live Kueue deployment logged a deprecation warning for
	// v1beta1); this must track kueue.go's workloadGVK/workloadItemGVK.
	wlGVK := schema.GroupVersionKind{Group: "kueue.x-k8s.io", Version: "v1beta2", Kind: "Workload"}
	scheme.AddKnownTypeWithName(wlGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(wlGVK.GroupVersion().WithKind("WorkloadList"), &unstructured.UnstructuredList{})
	return scheme
}

func testBridge(t *testing.T, fs *fakeSlurm, objs ...client.Object) (*Bridge, client.Client) {
	t.Helper()
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
	kube := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).Build()
	return New(cfg, kube, fs, slog.Default()), kube
}

// admittedWorkload fabricates the Kueue Workload the snapshot would find
// for a JobSet, with Admitted=True so the pin/release path is exercised.
// L6: apiVersion is v1beta2, matching kueue.go's workloadGVK/workloadItemGVK.
func admittedWorkload(jobsetName string) *unstructured.Unstructured {
	wl := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kueue.x-k8s.io/v1beta2",
		"kind":       "Workload",
		"metadata": map[string]any{
			"name":      "wl-" + jobsetName,
			"namespace": "slurm-jobs",
			"ownerReferences": []any{map[string]any{
				"apiVersion": "jobset.x-k8s.io/v1alpha2",
				"kind":       "JobSet",
				"name":       jobsetName,
				"uid":        "test-uid-" + jobsetName,
			}},
		},
		"spec": map[string]any{
			"priority":  int64(100),
			"queueName": "main",
			"podSets": []any{map[string]any{
				"name":  "main",
				"count": int64(1),
				"template": map[string]any{
					"spec": map[string]any{
						"containers": []any{map[string]any{
							"name":  "w",
							"image": "busybox",
						}},
						"restartPolicy": "Never",
					},
				},
			}},
		},
		"status": map[string]any{"conditions": []any{
			map[string]any{"type": "Admitted", "status": "True"},
		}},
	}}
	return wl
}

func heldJob(id uint64, partition string) slurm.Job {
	return slurm.Job{
		JobID:       id,
		Partition:   partition,
		JobState:    []string{"PENDING"},
		StateReason: "JobHeldUser",
		Hold:        true,
		Tasks:       slurm.Uint64NoVal{Set: true, Number: 2},
		CPUsPerTask: slurm.Uint64NoVal{Set: true, Number: 1},
	}
}

func getJobSet(t *testing.T, kube client.Client, name string) (*jobsetv1alpha2.JobSet, bool) {
	t.Helper()
	js := &jobsetv1alpha2.JobSet{}
	err := kube.Get(context.Background(), types.NamespacedName{Namespace: "slurm-jobs", Name: name}, js)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return js, true
}

func TestTickCreatesPinsAndReleases(t *testing.T) {
	fs := &fakeSlurm{jobs: []slurm.Job{heldJob(7, "mixing")}}
	b, kube := testBridge(t, fs, admittedWorkload("slurm-job-7"))

	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}

	js, found := getJobSet(t, kube, "slurm-job-7")
	if !found {
		t.Fatal("expected JobSet slurm-job-7 to be created")
	}
	if js.Labels["kueue.x-k8s.io/queue-name"] != "main" {
		t.Errorf("queue-name label = %q, want main", js.Labels["kueue.x-k8s.io/queue-name"])
	}
	if got := fs.features[7]; got != "nodes-for-7" {
		t.Errorf("feature = %q, want nodes-for-7", got)
	}
	if len(fs.released) != 1 || fs.released[0] != 7 {
		t.Errorf("released = %v, want [7]", fs.released)
	}
}

// TestTickEmitsCreatedAndReleasedEvents is the AUD2 regression: creating a
// JobSet and releasing its Slurm job must each emit a Kubernetes Event of
// the expected reason, when a Recorder is wired.
func TestTickEmitsCreatedAndReleasedEvents(t *testing.T) {
	fs := &fakeSlurm{jobs: []slurm.Job{heldJob(703, "mixing")}}
	b, _ := testBridge(t, fs, admittedWorkload("slurm-job-703"))
	rec := record.NewFakeRecorder(10)
	b.Recorder = rec

	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	close(rec.Events)
	var reasons []string
	for e := range rec.Events {
		reasons = append(reasons, e)
	}
	if !containsSubstring(reasons, "Created") {
		t.Errorf("events = %v, want one containing Created", reasons)
	}
	if !containsSubstring(reasons, "Released") {
		t.Errorf("events = %v, want one containing Released", reasons)
	}
}

// TestTickEmitsNoEventsWithoutRecorder is the nil-safety regression: every
// call site in this package must tolerate an unset Recorder (the default
// for any Bridge built without main.go's manager wiring, including every
// other test in this file) without panicking.
func TestTickEmitsNoEventsWithoutRecorder(t *testing.T) {
	fs := &fakeSlurm{jobs: []slurm.Job{heldJob(704, "mixing")}}
	b, _ := testBridge(t, fs, admittedWorkload("slurm-job-704")) // b.Recorder left nil
	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
}

// TestCleanupEmitsJobSetFailedEvent pins the Warning event on the D1 path.
func TestCleanupEmitsJobSetFailedEvent(t *testing.T) {
	fs := &fakeSlurm{jobs: []slurm.Job{
		{JobID: 705, JobState: []string{"PENDING"}, Hold: false, StateReason: "BadConstraints"},
	}}
	b, _ := testBridge(t, fs, failedJobSet(t, 705, "test failure"))
	rec := record.NewFakeRecorder(10)
	b.Recorder = rec

	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	close(rec.Events)
	var found bool
	for e := range rec.Events {
		if containsSubstring([]string{e}, "JobSetFailed") {
			found = true
		}
	}
	if !found {
		t.Error("expected a JobSetFailed Warning event on the D1 path")
	}
}

// TestEventfEmitsTranslationFailedViaDesiredEventRef unit-tests the
// TranslationFailed wiring directly (eventf + desiredEventRef), since
// admitHeldJobs' own admission filter (PriorityClassFor) already excludes
// every unmapped-partition job BEFORE calling translate.ToJobSet — the only
// error translate.ToJobSet currently returns — making the event
// practically unreachable through tick() alone today (defense in depth for
// a future translate.ToJobSet error path). This confirms the plumbing
// itself is correct: a Warning event with reason TranslationFailed lands on
// a stand-in JobSet object named exactly like the real one would be.
func TestEventfEmitsTranslationFailedViaDesiredEventRef(t *testing.T) {
	b := &Bridge{log: slog.Default()}
	rec := record.NewFakeRecorder(10)
	b.Recorder = rec

	job := &slurm.Job{JobID: 707}
	ref := desiredEventRef(job, "slurm-jobs")
	if ref.Name != "slurm-job-707" {
		t.Errorf("desiredEventRef name = %q, want slurm-job-707 (matches translate.JobSetName)", ref.Name)
	}
	b.eventf(ref, corev1.EventTypeWarning, "TranslationFailed", "translating Slurm job %d: %v", job.JobID, errors.New("boom"))

	close(rec.Events)
	var found bool
	for e := range rec.Events {
		if strings.Contains(e, "TranslationFailed") {
			found = true
		}
	}
	if !found {
		t.Error("expected a TranslationFailed Warning event")
	}
}

// containsSubstring reports whether any string in ss contains substr.
func containsSubstring(ss []string, substr string) bool {
	for _, s := range ss {
		if strings.Contains(s, substr) {
			return true
		}
	}
	return false
}

// TestSetCfgIsAtomicUnderConcurrentReadersAndWriters is the A1 hot-reload
// regression: cfgSnapshot() must never observe a config value from BETWEEN
// two different setCfg() generations (a torn read), even when a hot-reload
// races with an in-flight tick reading fields off the same Bridge from
// another goroutine. Run under `go test -race`, which is what actually
// proves the atomic.Pointer swap (not a plain field write) is doing its job
// here — a data race on a plain *config.Config field would otherwise only
// show up flakily.
func TestSetCfgIsAtomicUnderConcurrentReadersAndWriters(t *testing.T) {
	b := &Bridge{}
	b.setCfg(&config.Config{Namespace: "gen-0", LocalQueue: "gen-0"})

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writer: swaps in a new, internally-consistent config generation
	// repeatedly.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			gen := fmt.Sprintf("gen-%d", i)
			b.setCfg(&config.Config{Namespace: gen, LocalQueue: gen})
		}
	}()

	// Readers: cfgSnapshot() must always return a config whose Namespace and
	// LocalQueue AGREE (same generation) — never a mix of an old Namespace
	// with a new LocalQueue, which would be impossible if writes always
	// swap a whole new *config.Config rather than mutating fields in place.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			cfg := b.cfgSnapshot()
			if cfg.Namespace != cfg.LocalQueue {
				t.Errorf("torn read: Namespace=%q LocalQueue=%q (must always match, same generation)", cfg.Namespace, cfg.LocalQueue)
				close(stop)
				return
			}
		}
		close(stop)
	}()

	wg.Wait()
}

// TestReleaseObservesLatencyAndPrunesSeenAt is the AUD2 regression: a job
// first seen held+pending, then released once admitted, must have its wait
// observed into k8s_bridge_job_release_latency_seconds exactly once, and its
// seenAt bookkeeping entry removed immediately on release (not merely at
// prune time), since the wait it was tracking is over.
func TestReleaseObservesLatencyAndPrunesSeenAt(t *testing.T) {
	fs := &fakeSlurm{jobs: []slurm.Job{heldJob(701, "mixing")}}
	b, _ := testBridge(t, fs) // not yet admitted: Workload absent

	countBefore := histogramSampleCount(t, metrics.JobReleaseLatency)

	// Tick 1: job is held+pending but NOT admitted yet -> seenAt records the
	// first sighting, but no release happens.
	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick 1: %v", err)
	}
	if _, tracked := b.seenAt[701]; !tracked {
		t.Fatal("seenAt must record the job's first held+pending sighting")
	}
	if len(fs.released) != 0 {
		t.Fatal("job must not be released before admission")
	}

	// Now admit it (fresh Bridge/kube would lose the seenAt state, so
	// mutate this same Bridge's kube client instead of building a new one).
	admitted := admittedWorkload("slurm-job-701")
	if err := b.kube.Create(context.Background(), admitted); err != nil {
		t.Fatalf("creating admitted workload: %v", err)
	}

	// Tick 2: admitted now -> release happens, latency must be observed,
	// and the seenAt entry must be gone right away.
	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick 2: %v", err)
	}
	if len(fs.released) != 1 || fs.released[0] != 701 {
		t.Fatalf("released = %v, want [701]", fs.released)
	}
	if _, tracked := b.seenAt[701]; tracked {
		t.Error("seenAt entry must be removed immediately on release")
	}
	if got := histogramSampleCount(t, metrics.JobReleaseLatency); got != countBefore+1 {
		t.Errorf("JobReleaseLatency sample count = %d, want %d (exactly one observation)", got, countBefore+1)
	}
}

// histogramSampleCount returns the total number of observations recorded by
// a Prometheus histogram (its "_count"), used because
// testutil.CollectAndCount reports the number of metric SERIES (always 1
// for an unlabeled histogram), not the number of Observe() calls.
func histogramSampleCount(t *testing.T, h prometheus.Histogram) uint64 {
	t.Helper()
	m := &dto.Metric{}
	if err := h.Write(m); err != nil {
		t.Fatalf("writing histogram metric: %v", err)
	}
	return m.GetHistogram().GetSampleCount()
}

// TestSeenAtPrunedWhenJobDisappearsWithoutRelease pins the pruning
// requirement explicitly called out in the spec: a job that vanishes from
// Slurm's job list while still held (e.g. cancelled before ever being
// admitted) must not leak its seenAt entry forever.
func TestSeenAtPrunedWhenJobDisappearsWithoutRelease(t *testing.T) {
	fs := &fakeSlurm{jobs: []slurm.Job{heldJob(702, "mixing")}}
	b, _ := testBridge(t, fs)

	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick 1: %v", err)
	}
	if _, tracked := b.seenAt[702]; !tracked {
		t.Fatal("seenAt must record the job's first held+pending sighting")
	}

	// The job vanishes entirely (purged from slurmctld, GetJob also returns
	// not-found) before ever being admitted/released.
	fs.jobs = nil
	fs.getJob = func(uint64) (*slurm.Job, bool, error) { return nil, false, nil }

	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick 2: %v", err)
	}
	if _, tracked := b.seenAt[702]; tracked {
		t.Error("seenAt entry for a vanished job must be pruned, not kept forever")
	}
}

func TestTickWaitsForNodeRegistration(t *testing.T) {
	// ADR-0005: while the constraint update fails (nodes not registered),
	// the job must NOT be released; the next tick retries and succeeds.
	fs := &fakeSlurm{
		jobs:       []slurm.Job{heldJob(8, "mixing")},
		featureErr: errors.New("422 UNPROCESSABLE: invalid feature"),
	}
	b, kube := testBridge(t, fs, admittedWorkload("slurm-job-8"))

	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick 1: %v", err)
	}
	if len(fs.released) != 0 {
		t.Fatalf("job released before nodes registered: %v", fs.released)
	}
	if _, found := getJobSet(t, kube, "slurm-job-8"); !found {
		t.Fatal("JobSet must exist even while waiting for registration")
	}

	fs.featureErr = nil // nodes "registered"
	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick 2: %v", err)
	}
	if len(fs.released) != 1 || fs.released[0] != 8 {
		t.Errorf("released = %v, want [8]", fs.released)
	}
}

// TestTickWarnsOnNonRegistrationUpdateFailure is the live regression from
// the tutorial validation run: a job-update failure that is NOT the
// "Invalid feature specification" registration signal (here: the "Invalid
// user id" a misconfigured slurmUser produces) stranded the job held
// forever with only a Debug log. Such a failure must still not release the
// job, but must surface at Warn; the true registration signal must stay
// quiet (Debug only).
func TestTickWarnsOnNonRegistrationUpdateFailure(t *testing.T) {
	fs := &fakeSlurm{
		jobs:       []slurm.Job{heldJob(11, "mixing")},
		featureErr: errors.New("422 Unprocessable Entity: Invalid user id"),
	}
	b, _, warnCount := testBridgeCountingWarns(t, fs, admittedWorkload("slurm-job-11"))
	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if len(fs.released) != 0 {
		t.Fatalf("job released despite failed constraint update: %v", fs.released)
	}
	if *warnCount != 1 {
		t.Errorf("Warn log lines = %d, want exactly 1 for a non-registration failure", *warnCount)
	}

	fs2 := &fakeSlurm{
		jobs:       []slurm.Job{heldJob(12, "mixing")},
		featureErr: errors.New("422 Unprocessable Entity: Invalid feature specification"),
	}
	b2, _, warnCount2 := testBridgeCountingWarns(t, fs2, admittedWorkload("slurm-job-12"))
	if err := b2.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if len(fs2.released) != 0 {
		t.Fatalf("job released before nodes registered: %v", fs2.released)
	}
	if *warnCount2 != 0 {
		t.Errorf("Warn log lines = %d, want 0 for the expected registration-wait path", *warnCount2)
	}
}

func TestTickIsIdempotent(t *testing.T) {
	fs := &fakeSlurm{jobs: []slurm.Job{heldJob(9, "mixing")}}
	b, _ := testBridge(t, fs, admittedWorkload("slurm-job-9"))

	for i := 0; i < 3; i++ {
		if err := b.tick(context.Background()); err != nil {
			t.Fatalf("tick %d: %v", i, err)
		}
		// After the first release the job is no longer held.
		fs.jobs[0].Hold = false
		fs.jobs[0].StateReason = "None"
	}
	if len(fs.released) != 1 {
		t.Errorf("released %d times, want exactly 1 (idempotency)", len(fs.released))
	}
}

func TestTickIgnoresUnmappedPartitions(t *testing.T) {
	fs := &fakeSlurm{jobs: []slurm.Job{heldJob(10, "gpu-team")}}
	b, kube := testBridge(t, fs)

	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if _, found := getJobSet(t, kube, "slurm-job-10"); found {
		t.Error("JobSet must not be created for a partition outside the config")
	}
	if len(fs.released) != 0 {
		t.Errorf("job in unmapped partition must not be released: %v", fs.released)
	}
}

func ownedJobSet(t *testing.T, id uint64) *jobsetv1alpha2.JobSet {
	t.Helper()
	job := heldJob(id, "mixing")
	cfg := &config.Config{
		Namespace:  "slurm-jobs",
		LocalQueue: "main",
		PartitionMappings: []config.PartitionMapping{
			{PartitionName: "mixing", WorkloadPriorityClass: "normal-priority"},
		},
		Slurmd: config.Slurmd{Image: "slurmd:test", ConfServer: "ctl:6817", AuthSecretName: "s"},
	}
	js, err := translate.ToJobSet(&job, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return js
}

func TestCleanupDeletesJobSetAndNodesForTerminalJob(t *testing.T) {
	states := []string{"COMPLETED", "FAILED", "CANCELLED"}
	for _, state := range states {
		t.Run(state, func(t *testing.T) {
			fs := &fakeSlurm{getJob: func(id uint64) (*slurm.Job, bool, error) {
				return &slurm.Job{JobID: id, JobState: []string{state}}, true, nil
			}}
			b, kube := testBridge(t, fs, ownedJobSet(t, 20))

			if err := b.tick(context.Background()); err != nil {
				t.Fatalf("tick: %v", err)
			}
			if _, found := getJobSet(t, kube, "slurm-job-20"); found {
				t.Error("JobSet should be deleted for terminal job")
			}
			want := []string{"slurm-job-20-workers-0-0", "slurm-job-20-workers-0-1"}
			if fmt.Sprint(fs.deletedNodes) != fmt.Sprint(want) {
				t.Errorf("deleted nodes = %v, want %v", fs.deletedNodes, want)
			}
		})
	}
}

func TestCleanupDeletesJobSetWhenSlurmForgotTheJob(t *testing.T) {
	// Jobs vanish from slurmctld after MinJobAge; treat unknown as terminal.
	fs := &fakeSlurm{getJob: func(uint64) (*slurm.Job, bool, error) { return nil, false, nil }}
	b, kube := testBridge(t, fs, ownedJobSet(t, 21))

	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if _, found := getJobSet(t, kube, "slurm-job-21"); found {
		t.Error("JobSet should be deleted when Slurm no longer knows the job")
	}
}

func TestCleanupLeavesRunningJobAlone(t *testing.T) {
	fs := &fakeSlurm{getJob: func(id uint64) (*slurm.Job, bool, error) {
		return &slurm.Job{JobID: id, JobState: []string{"RUNNING"}}, true, nil
	}}
	b, kube := testBridge(t, fs, ownedJobSet(t, 22))

	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if _, found := getJobSet(t, kube, "slurm-job-22"); !found {
		t.Error("JobSet of a running job must not be touched")
	}
	if len(fs.deletedNodes) != 0 {
		t.Errorf("no nodes should be deleted for a running job: %v", fs.deletedNodes)
	}
}

// TestCleanupUsesTickListJobsNotPerJobGetJob is the audit P7 regression:
// cleanupFinishedJobs must consult the map built from tick()'s ListJobs
// result instead of issuing GetJob per owned JobSet. When every owned job is
// still present in ListJobs, steady-state GetJob calls must drop to zero.
func TestCleanupUsesTickListJobsNotPerJobGetJob(t *testing.T) {
	fs := &fakeSlurm{jobs: []slurm.Job{
		{JobID: 30, JobState: []string{"RUNNING"}},
		{JobID: 31, JobState: []string{"COMPLETED"}},
	}}
	b, kube := testBridge(t, fs, ownedJobSet(t, 30), ownedJobSet(t, 31))

	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if fs.getJobCalls != 0 {
		t.Errorf("GetJob calls = %d, want 0 (both jobs present in ListJobs result)", fs.getJobCalls)
	}
	if _, found := getJobSet(t, kube, "slurm-job-30"); !found {
		t.Error("running job's JobSet must survive")
	}
	if _, found := getJobSet(t, kube, "slurm-job-31"); found {
		t.Error("completed job's JobSet must be deleted")
	}
}

// TestCleanupFallsBackToGetJobWhenAbsentFromList pins the fallback half of
// P7: a JobSet whose job ID is NOT in the tick's ListJobs result (purged from
// slurmctld) must still be resolved via GetJob before deleting, to confirm
// purged-equals-terminal semantics.
func TestCleanupFallsBackToGetJobWhenAbsentFromList(t *testing.T) {
	fs := &fakeSlurm{
		jobs:   nil, // job 32 absent from ListJobs entirely
		getJob: func(uint64) (*slurm.Job, bool, error) { return nil, false, nil },
	}
	b, kube := testBridge(t, fs, ownedJobSet(t, 32))

	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if fs.getJobCalls != 1 {
		t.Errorf("GetJob calls = %d, want 1 (job absent from ListJobs)", fs.getJobCalls)
	}
	if _, found := getJobSet(t, kube, "slurm-job-32"); found {
		t.Error("JobSet should be deleted once GetJob confirms the job is gone")
	}
}

// TestCleanupSurvivesEmptyReplicatedJobs is the audit D5(a) regression: a
// managed-labeled JobSet with an empty ReplicatedJobs slice (hand-edited or
// mutated externally) must not panic cleanupFinishedJobs when it indexes
// ReplicatedJobs[0] to size the node-name list.
func TestCleanupSurvivesEmptyReplicatedJobs(t *testing.T) {
	js := ownedJobSet(t, 33)
	js.Spec.ReplicatedJobs = nil
	fs := &fakeSlurm{jobs: nil, getJob: func(uint64) (*slurm.Job, bool, error) { return nil, false, nil }}
	b, kube := testBridge(t, fs, js)

	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick panicked or errored: %v", err)
	}
	if _, found := getJobSet(t, kube, "slurm-job-33"); found {
		t.Error("JobSet should still be cleaned up despite the missing replicatedJobs")
	}
}

// TestTickSignalsUnmanagedPartitionViaComment is the audit D4 regression: a
// held+pending job whose partition has no PartitionMappings entry must get a
// Slurm comment explaining why, instead of being silently dropped.
func TestTickSignalsUnmanagedPartitionViaComment(t *testing.T) {
	fs := &fakeSlurm{jobs: []slurm.Job{heldJob(40, "gpu-team")}}
	b, _ := testBridge(t, fs)

	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if got := fs.comments[40]; got != unmanagedPartitionComment {
		t.Errorf("comment = %q, want %q", got, unmanagedPartitionComment)
	}
}

// TestTickDoesNotRewriteUnchangedUnmanagedPartitionComment pins the
// dedup half of D4: once the comment is written, the second tick must not
// call SetJobComment again (avoids per-tick spam and the associated Warn log
// storm for a persistently unmapped partition).
func TestTickDoesNotRewriteUnchangedUnmanagedPartitionComment(t *testing.T) {
	job := heldJob(41, "gpu-team")
	job.Comment = unmanagedPartitionComment // already signalled on a prior tick
	fs := &fakeSlurm{jobs: []slurm.Job{job}}
	b, _ := testBridge(t, fs)

	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if _, wrote := fs.comments[41]; wrote {
		t.Error("SetJobComment must not be called again once the comment is unchanged")
	}
}

// TestTickDoesNotSignalNonHeldOrNonPendingUnmappedJobs confirms the D4 fix
// stays scoped to held+pending jobs, per the audit instructions: jobs that
// are merely not-held or not-pending are out of scope by design and must not
// be signalled.
func TestTickDoesNotSignalNonHeldOrNonPendingUnmappedJobs(t *testing.T) {
	notHeld := slurm.Job{JobID: 42, Partition: "gpu-team", JobState: []string{"PENDING"}, StateReason: "None"}
	notPending := slurm.Job{JobID: 43, Partition: "gpu-team", JobState: []string{"RUNNING"}, Hold: true}
	fs := &fakeSlurm{jobs: []slurm.Job{notHeld, notPending}}
	b, _ := testBridge(t, fs)

	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if len(fs.comments) != 0 {
		t.Errorf("comments = %v, want none written for non-held/non-pending jobs", fs.comments)
	}
}

// failedJobSet builds an owned JobSet (via ownedJobSet) with a Failed=True
// condition, simulating activeDeadlineSeconds firing before the Slurm job's
// dynamic nodes registered (the live defect that motivated D1).
func failedJobSet(t *testing.T, id uint64, reason string) *jobsetv1alpha2.JobSet {
	t.Helper()
	js := ownedJobSet(t, id)
	js.Status.Conditions = []metav1.Condition{{
		Type:               string(jobsetv1alpha2.JobSetFailed),
		Status:             metav1.ConditionTrue,
		Reason:             "DeadlineExceeded",
		Message:            reason,
		LastTransitionTime: metav1.Now(),
	}}
	return js
}

// captureWarnLogs returns a *slog.Logger backed by a handler that counts
// records at slog.LevelWarn, for the D1 "exactly one Warn log line" spec.
type warnCountingHandler struct {
	slog.Handler
	count *int
}

func (h warnCountingHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Level == slog.LevelWarn {
		*h.count++
	}
	return h.Handler.Handle(ctx, r)
}

// WithAttrs/WithGroup must re-wrap: the embedded handler's versions return
// the INNER handler type, silently dropping the counter for any logger
// derived via log.With(...) — which is how tick's per-job logger is built.
func (h warnCountingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return warnCountingHandler{Handler: h.Handler.WithAttrs(attrs), count: h.count}
}

func (h warnCountingHandler) WithGroup(name string) slog.Handler {
	return warnCountingHandler{Handler: h.Handler.WithGroup(name), count: h.count}
}

func testBridgeCountingWarns(t *testing.T, fs *fakeSlurm, objs ...client.Object) (*Bridge, client.Client, *int) {
	t.Helper()
	b, kube := testBridge(t, fs, objs...)
	count := 0
	base := slog.NewTextHandler(discardWriter{}, nil)
	b.log = slog.New(warnCountingHandler{Handler: base, count: &count})
	return b, kube, &count
}

// TestCleanupFailsSlurmJobForFailedJobSet is the D1 regression: a JobSet
// whose Slurm job never became terminal but which itself reports Failed=True
// must have its Slurm job actively failed (comment + cancel), be cleaned up
// like a completed job, increment k8s_bridge_jobs_failed_total, and emit
// exactly one Warn log line.
func TestCleanupFailsSlurmJobForFailedJobSet(t *testing.T) {
	fs := &fakeSlurm{jobs: []slurm.Job{
		{JobID: 50, JobState: []string{"PENDING"}, Hold: false, StateReason: "BadConstraints"},
	}}
	js := failedJobSet(t, 50, "DeadlineExceeded")
	b, kube, warnCount := testBridgeCountingWarns(t, fs, js)

	failedBefore := testutil.ToFloat64(metrics.JobsFailedTotal)
	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}

	if got := fs.comments[50]; got == "" || got == unmanagedPartitionComment {
		t.Errorf("comment = %q, want a JobSet-failure explanation", got)
	}
	if len(fs.cancelled) != 1 || fs.cancelled[0] != 50 {
		t.Errorf("cancelled = %v, want [50]", fs.cancelled)
	}
	if _, found := getJobSet(t, kube, "slurm-job-50"); found {
		t.Error("JobSet should be cleaned up after its Slurm job was failed")
	}
	if got := testutil.ToFloat64(metrics.JobsFailedTotal); got != failedBefore+1 {
		t.Errorf("JobsFailedTotal = %v, want %v", got, failedBefore+1)
	}
	if *warnCount != 1 {
		t.Errorf("Warn log lines = %d, want exactly 1", *warnCount)
	}
}

// TestCleanupRetriesFailedJobSetWhenCancelJobErrors is the D1 regression
// covering a defect in the initial fix: if slurmrestd is unreachable and
// CancelJob itself errors, the Slurm job is NOT actually cancelled. Deleting
// the JobSet anyway would erase the only record (SlurmJobIDLabel lives on the
// JobSet) the bridge has to retry the cancel from — the job would be stuck
// pending forever with just a stale comment and no further attempts. The
// JobSet must survive so the whole failure path (comment + cancel) is retried
// next tick, exactly like every other "temporary error" in this file.
func TestCleanupRetriesFailedJobSetWhenCancelJobErrors(t *testing.T) {
	fs := &fakeSlurm{
		jobs: []slurm.Job{
			{JobID: 53, JobState: []string{"PENDING"}, Hold: false, StateReason: "BadConstraints"},
		},
		cancelErr: errors.New("slurmrestd unreachable"),
	}
	js := failedJobSet(t, 53, "DeadlineExceeded")
	b, kube := testBridge(t, fs, js)

	failedBefore := testutil.ToFloat64(metrics.JobsFailedTotal)
	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}

	if _, found := getJobSet(t, kube, "slurm-job-53"); !found {
		t.Error("JobSet must survive a failed CancelJob so the next tick can retry")
	}
	if len(fs.cancelled) != 0 {
		t.Errorf("cancelled = %v, want none recorded (CancelJob errored)", fs.cancelled)
	}
	if len(fs.deletedNodes) != 0 {
		t.Errorf("deletedNodes = %v, want none: node cleanup must not run until the cancel succeeds", fs.deletedNodes)
	}
	// The failure metric and comment are still recorded on every attempt —
	// only the destructive cleanup (JobSet/node deletion) is gated on success.
	if got := testutil.ToFloat64(metrics.JobsFailedTotal); got != failedBefore+1 {
		t.Errorf("JobsFailedTotal = %v, want %v (metric records the attempt)", got, failedBefore+1)
	}

	// Next tick: slurmrestd recovers, CancelJob now succeeds, and cleanup
	// proceeds normally.
	fs.cancelErr = nil
	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick 2: %v", err)
	}
	if len(fs.cancelled) != 1 || fs.cancelled[0] != 53 {
		t.Errorf("cancelled = %v, want [53] after recovery", fs.cancelled)
	}
	if _, found := getJobSet(t, kube, "slurm-job-53"); found {
		t.Error("JobSet should be cleaned up once CancelJob succeeds")
	}
}

// TestCleanupLeavesHealthyJobSetAlone confirms a JobSet with no Failed
// condition (still running normally) never triggers the D1 failure path.
func TestCleanupLeavesHealthyJobSetAlone(t *testing.T) {
	fs := &fakeSlurm{jobs: []slurm.Job{
		{JobID: 51, JobState: []string{"RUNNING"}},
	}}
	js := ownedJobSet(t, 51) // no Failed condition set
	b, kube := testBridge(t, fs, js)

	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if len(fs.cancelled) != 0 {
		t.Errorf("cancelled = %v, want none for a healthy JobSet", fs.cancelled)
	}
	if _, found := getJobSet(t, kube, "slurm-job-51"); !found {
		t.Error("healthy JobSet must not be touched")
	}
}

// TestCleanupCompletedJobSetUnchangedByD1 confirms the D1 addition does not
// alter the pre-existing completed-job cleanup path: a JobSet with a
// Completed=True condition (not Failed) whose Slurm job is already terminal
// must be cleaned up exactly as before, with NO cancel/comment/failure-metric
// side effects from the new D1 branch.
func TestCleanupCompletedJobSetUnchangedByD1(t *testing.T) {
	fs := &fakeSlurm{jobs: []slurm.Job{
		{JobID: 52, JobState: []string{"COMPLETED"}},
	}}
	js := ownedJobSet(t, 52)
	js.Status.Conditions = []metav1.Condition{{
		Type:               string(jobsetv1alpha2.JobSetCompleted),
		Status:             metav1.ConditionTrue,
		Reason:             "AllJobsCompleted",
		Message:            "all replicated jobs completed",
		LastTransitionTime: metav1.Now(),
	}}
	failedBefore := testutil.ToFloat64(metrics.JobsFailedTotal)
	b, kube := testBridge(t, fs, js)

	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if len(fs.cancelled) != 0 {
		t.Errorf("cancelled = %v, want none for a completed (not failed) JobSet", fs.cancelled)
	}
	if _, found := getJobSet(t, kube, "slurm-job-52"); found {
		t.Error("completed JobSet should still be cleaned up (existing behavior)")
	}
	if got := testutil.ToFloat64(metrics.JobsFailedTotal); got != failedBefore {
		t.Errorf("JobsFailedTotal = %v, want unchanged at %v (completed path, not D1)", got, failedBefore)
	}
}

// TestCleanupIsolatesPerJobGetJobErrors is the A2 regression: one job's
// GetJob failure during the cleanup pass previously ABORTED the loop,
// postponing cleanup for every remaining JobSet that tick (and the orphan
// pass after it) — inconsistent with the per-job isolation used everywhere
// else in reconciler.go. The failing job must be recorded (tick still
// returns an error so Run's backoff accounting sees it) while the other
// JobSets' cleanup proceeds.
func TestCleanupIsolatesPerJobGetJobErrors(t *testing.T) {
	fs := &fakeSlurm{
		jobs: nil, // both jobs absent from ListJobs -> both fall back to GetJob
		getJob: func(id uint64) (*slurm.Job, bool, error) {
			if id == 60 {
				return nil, false, errors.New("slurmrestd hiccup")
			}
			return nil, false, nil // 61: confirmed purged -> cleanable
		},
	}
	b, kube := testBridge(t, fs, ownedJobSet(t, 60), ownedJobSet(t, 61))

	err := b.tick(context.Background())
	if err == nil {
		t.Fatal("tick must still return the aggregated GetJob error")
	}
	if !strings.Contains(err.Error(), "60") {
		t.Errorf("aggregated error %q should identify the failing job 60", err)
	}
	if _, found := getJobSet(t, kube, "slurm-job-61"); found {
		t.Error("JobSet 61 must still be cleaned up despite job 60's GetJob failure (per-job isolation)")
	}
	if _, found := getJobSet(t, kube, "slurm-job-60"); !found {
		t.Error("JobSet 60 must survive so the next tick retries its check")
	}
}

// submitTimeJobSet builds an owned JobSet for a held job carrying the given
// submit time, i.e. with the A3 identity annotation stamped by
// translate.ToJobSet.
func submitTimeJobSet(t *testing.T, id, submit uint64) *jobsetv1alpha2.JobSet {
	t.Helper()
	job := heldJob(id, "mixing")
	job.SubmitTime = slurm.Uint64NoVal{Set: true, Number: submit}
	cfg := &config.Config{
		Namespace:  "slurm-jobs",
		LocalQueue: "main",
		PartitionMappings: []config.PartitionMapping{
			{PartitionName: "mixing", WorkloadPriorityClass: "normal-priority"},
		},
		Slurmd: config.Slurmd{Image: "slurmd:test", ConfServer: "ctl:6817", AuthSecretName: "s"},
	}
	js, err := translate.ToJobSet(&job, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return js
}

// TestEnsureJobSetIdempotentOnMatchingIdentity pins the happy AlreadyExists
// path after A3: the existing JobSet carries the SAME submit-time annotation,
// so this is the ordinary idempotent retry — no error, no conflict metric.
func TestEnsureJobSetIdempotentOnMatchingIdentity(t *testing.T) {
	existing := submitTimeJobSet(t, 80, 1111)
	b, _ := testBridge(t, &fakeSlurm{}, existing)

	conflictsBefore := testutil.ToFloat64(metrics.JobSetIdentityConflictsTotal)
	created, err := b.ensureJobSet(context.Background(), submitTimeJobSet(t, 80, 1111))
	if err != nil {
		t.Fatalf("ensureJobSet on matching identity: %v", err)
	}
	if created {
		t.Error("created = true, want false (JobSet already existed)")
	}
	if got := testutil.ToFloat64(metrics.JobSetIdentityConflictsTotal); got != conflictsBefore {
		t.Errorf("JobSetIdentityConflictsTotal = %v, want unchanged at %v", got, conflictsBefore)
	}
}

// TestEnsureJobSetRefusesMismatchedIdentity is the core A3 regression: a NEW
// job whose (reused) ID collides with a live JobSet from a DIFFERENT
// incarnation must not silently inherit that JobSet's wrong-shaped capacity.
// ensureJobSet must return an error (per-job skip, retried next tick),
// increment the conflict counter, emit a Warning event — and leave the
// existing JobSet strictly alone (no adopt, no delete: deletion is an
// operator decision).
func TestEnsureJobSetRefusesMismatchedIdentity(t *testing.T) {
	existing := submitTimeJobSet(t, 81, 1111)
	b, kube := testBridge(t, &fakeSlurm{}, existing)
	rec := record.NewFakeRecorder(10)
	b.Recorder = rec

	conflictsBefore := testutil.ToFloat64(metrics.JobSetIdentityConflictsTotal)
	created, err := b.ensureJobSet(context.Background(), submitTimeJobSet(t, 81, 2222))
	if err == nil {
		t.Fatal("ensureJobSet must return an error on an identity mismatch")
	}
	if created {
		t.Error("created = true, want false")
	}
	if got := testutil.ToFloat64(metrics.JobSetIdentityConflictsTotal); got != conflictsBefore+1 {
		t.Errorf("JobSetIdentityConflictsTotal = %v, want %v", got, conflictsBefore+1)
	}
	close(rec.Events)
	var eventSeen bool
	for e := range rec.Events {
		if strings.Contains(e, "JobSetIdentityConflict") {
			eventSeen = true
		}
	}
	if !eventSeen {
		t.Error("expected a JobSetIdentityConflict Warning event")
	}
	// The stale JobSet must survive untouched, still carrying ITS submit time.
	js, found := getJobSet(t, kube, "slurm-job-81")
	if !found {
		t.Fatal("existing JobSet must NOT be deleted on a conflict (operator decision)")
	}
	if got := js.Annotations[translate.SlurmSubmitTimeAnnotation]; got != "1111" {
		t.Errorf("existing JobSet's submit-time annotation = %q, want the original 1111 (no adoption)", got)
	}
}

// TestEnsureJobSetToleratesUnknowableIdentity pins the A3 compatibility
// contract: when either side has no submit-time annotation the check
// degrades to the historic name-match-is-enough behavior — an existing
// JobSet from a pre-A3 bridge version (upgrade path) and a desired JobSet
// whose job exposed no submit_time must both be treated as matching, never
// as conflicts.
func TestEnsureJobSetToleratesUnknowableIdentity(t *testing.T) {
	t.Run("legacy existing JobSet without annotation", func(t *testing.T) {
		legacy := ownedJobSet(t, 82) // heldJob has no SubmitTime -> no annotation
		b, _ := testBridge(t, &fakeSlurm{}, legacy)
		conflictsBefore := testutil.ToFloat64(metrics.JobSetIdentityConflictsTotal)
		created, err := b.ensureJobSet(context.Background(), submitTimeJobSet(t, 82, 3333))
		if err != nil || created {
			t.Fatalf("ensureJobSet = (%v, %v), want (false, nil) for a legacy JobSet", created, err)
		}
		if got := testutil.ToFloat64(metrics.JobSetIdentityConflictsTotal); got != conflictsBefore {
			t.Errorf("JobSetIdentityConflictsTotal = %v, want unchanged at %v", got, conflictsBefore)
		}
	})
	t.Run("desired JobSet without annotation", func(t *testing.T) {
		existing := submitTimeJobSet(t, 83, 4444)
		b, _ := testBridge(t, &fakeSlurm{}, existing)
		created, err := b.ensureJobSet(context.Background(), ownedJobSet(t, 83))
		if err != nil || created {
			t.Fatalf("ensureJobSet = (%v, %v), want (false, nil) when the new job's submit time is unknowable", created, err)
		}
	})
}

// TestTickRefusesSnapshotJobSetWithMismatchedIdentity pins the SNAPSHOT half
// of the A3 identity check: a stale JobSet already in the informer cache
// (here: pre-seeded into the fake client) under this job's deterministic
// name, carrying a DIFFERENT submit-time, must not be treated as the job's
// capacity — no features set, no release, conflict counted and evented —
// even though the Workload is admitted. Before this check, only the
// create-time AlreadyExists path was guarded; a cached stale JobSet sailed
// straight into phase 2 and the new job was pinned onto wrong-shaped
// capacity.
func TestTickRefusesSnapshotJobSetWithMismatchedIdentity(t *testing.T) {
	staleJS := submitTimeJobSet(t, 91, 1111)
	newJob := heldJob(91, "mixing")
	newJob.SubmitTime = slurm.Uint64NoVal{Set: true, Number: 2222}
	fs := &fakeSlurm{jobs: []slurm.Job{newJob}}
	b, kube := testBridge(t, fs, staleJS, admittedWorkload("slurm-job-91"))
	rec := record.NewFakeRecorder(10)
	b.Recorder = rec

	conflictsBefore := testutil.ToFloat64(metrics.JobSetIdentityConflictsTotal)
	if err := b.tick(context.Background()); err == nil {
		t.Fatal("tick must surface the identity conflict as an error")
	}
	if got := testutil.ToFloat64(metrics.JobSetIdentityConflictsTotal); got != conflictsBefore+1 {
		t.Errorf("JobSetIdentityConflictsTotal = %v, want %v", got, conflictsBefore+1)
	}
	if got := fs.features[91]; got != "" {
		t.Errorf("features = %q, want none (job must be skipped, not pinned onto the stale JobSet)", got)
	}
	if len(fs.released) != 0 {
		t.Errorf("released = %v, want none", fs.released)
	}
	close(rec.Events)
	var eventSeen bool
	for e := range rec.Events {
		if strings.Contains(e, "JobSetIdentityConflict") {
			eventSeen = true
		}
	}
	if !eventSeen {
		t.Error("expected a JobSetIdentityConflict Warning event")
	}
	// The stale JobSet must survive untouched with ITS submit time.
	js, found := getJobSet(t, kube, "slurm-job-91")
	if !found {
		t.Fatal("stale JobSet must NOT be deleted on a conflict (operator decision)")
	}
	if got := js.Annotations[translate.SlurmSubmitTimeAnnotation]; got != "1111" {
		t.Errorf("stale JobSet's submit-time annotation = %q, want the original 1111", got)
	}
}

// TestTickAcceptsSnapshotJobSetWithMatchingIdentity is the happy twin: the
// cached JobSet carries the SAME submit-time, so the snapshot path treats it
// as this job's capacity and the normal pin+release flow proceeds.
func TestTickAcceptsSnapshotJobSetWithMatchingIdentity(t *testing.T) {
	ourJS := submitTimeJobSet(t, 92, 4242)
	job := heldJob(92, "mixing")
	job.SubmitTime = slurm.Uint64NoVal{Set: true, Number: 4242}
	fs := &fakeSlurm{jobs: []slurm.Job{job}}
	b, _ := testBridge(t, fs, ourJS, admittedWorkload("slurm-job-92"))

	conflictsBefore := testutil.ToFloat64(metrics.JobSetIdentityConflictsTotal)
	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if got := testutil.ToFloat64(metrics.JobSetIdentityConflictsTotal); got != conflictsBefore {
		t.Errorf("JobSetIdentityConflictsTotal = %v, want unchanged %v", got, conflictsBefore)
	}
	if len(fs.released) != 1 || fs.released[0] != 92 {
		t.Errorf("released = %v, want [92]", fs.released)
	}
}
