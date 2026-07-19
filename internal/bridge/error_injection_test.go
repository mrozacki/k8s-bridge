package bridge

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/mrozacki/k8s-bridge/internal/config"
	"github.com/mrozacki/k8s-bridge/internal/kueue"
	"github.com/mrozacki/k8s-bridge/internal/metrics"
	"github.com/mrozacki/k8s-bridge/internal/slurm"
)

// bridgeWithInterceptor builds a Bridge identical to testBridge but with the
// fake client wrapped by an interceptor.Funcs, for injecting Kubernetes API
// errors that the fake client alone cannot produce (create/delete/list
// failures on an otherwise-healthy object store).
func bridgeWithInterceptor(t *testing.T, fs *fakeSlurm, funcs interceptor.Funcs, objs ...client.Object) (*Bridge, client.Client) {
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
	kube := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).
		WithInterceptorFuncs(funcs).Build()
	return New(cfg, kube, fs, slog.Default()), kube
}

// TestEnsureJobSetCreateFailureIsIsolatedNotFatal is the P5 update of the
// audit AUD1 error-injection regression. Before P5's parallel-create pool, a
// single JobSet creation failure returned immediately and aborted the WHOLE
// tick, stopping every other held job's processing too. P5's semantics are
// per-job isolation instead: the failing job is logged, counted
// (k8s_bridge_jobset_create_errors_total), and skipped (its JobSet still
// doesn't exist, so it isn't released this tick either) — but the tick
// itself still surfaces the failure by returning a non-nil aggregate error
// so Run()'s failure/backoff accounting reacts to it.
func TestEnsureJobSetCreateFailureIsIsolatedNotFatal(t *testing.T) {
	wantErr := errors.New("etcd unavailable")
	funcs := interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*jobsetv1alpha2.JobSet); ok {
				return wantErr
			}
			return c.Create(ctx, obj, opts...)
		},
	}
	fs := &fakeSlurm{jobs: []slurm.Job{heldJob(200, "mixing")}}
	b, kube := bridgeWithInterceptor(t, fs, funcs)

	failedBefore := testutil.ToFloat64(metrics.JobSetCreateErrorsTotal)
	err := b.tick(context.Background())
	if err == nil {
		t.Fatal("expected tick to report an error when a JobSet creation fails")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("tick error = %v, want it to wrap %v", err, wantErr)
	}
	if _, found := getJobSet(t, kube, "slurm-job-200"); found {
		t.Error("JobSet must not exist after a failed create")
	}
	if len(fs.released) != 0 {
		t.Error("job must not be released when JobSet creation failed")
	}
	if got := testutil.ToFloat64(metrics.JobSetCreateErrorsTotal); got != failedBefore+1 {
		t.Errorf("JobSetCreateErrorsTotal = %v, want %v", got, failedBefore+1)
	}
}

// TestEnsureJobSetCreateFailureDoesNotBlockOtherHeldJobs is the P5 isolation
// regression proper: with TWO held jobs in the same tick, a Create failure
// injected for only ONE of them must not prevent the OTHER from being
// created, admitted, and released. Pool size 1 (via cfg.CreateWorkers) makes
// the worker pool process jobs one at a time in a deterministic order, so
// this test isn't racing the fake client.
func TestEnsureJobSetCreateFailureDoesNotBlockOtherHeldJobs(t *testing.T) {
	wantErr := errors.New("etcd unavailable")
	funcs := interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if js, ok := obj.(*jobsetv1alpha2.JobSet); ok && js.GetName() == "slurm-job-201" {
				return wantErr
			}
			return c.Create(ctx, obj, opts...)
		},
	}
	fs := &fakeSlurm{jobs: []slurm.Job{heldJob(201, "mixing"), heldJob(202, "mixing")}}
	b, kube := bridgeWithInterceptor(t, fs, funcs, admittedWorkload("slurm-job-202"))
	cfg := *b.cfgSnapshot()
	cfg.CreateWorkers = 1 // deterministic: one job processed at a time
	b.setCfg(&cfg)

	if err := b.tick(context.Background()); err == nil {
		t.Fatal("expected tick to report an error for job 201's failed create")
	}
	if _, found := getJobSet(t, kube, "slurm-job-201"); found {
		t.Error("job 201's JobSet must not exist (its create failed)")
	}
	if _, found := getJobSet(t, kube, "slurm-job-202"); !found {
		t.Error("job 202's JobSet must still be created despite job 201's failure")
	}
	if len(fs.released) != 1 || fs.released[0] != 202 {
		t.Errorf("released = %v, want [202]: job 201's failure must not block job 202's release", fs.released)
	}
}

// TestEnsureJobSetAlreadyExistsIsNotAnError confirms the idempotent-upsert
// path: a Create failing with AlreadyExists (a JobSet created by a previous
// tick that the snapshot LIST missed, e.g. due to caching) must be treated as
// success, not abort the tick.
func TestEnsureJobSetAlreadyExistsIsNotAnError(t *testing.T) {
	funcs := interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*jobsetv1alpha2.JobSet); ok {
				return apierrors.NewAlreadyExists(schema.GroupResource{Group: "jobset.x-k8s.io", Resource: "jobsets"}, obj.GetName())
			}
			return c.Create(ctx, obj, opts...)
		},
	}
	fs := &fakeSlurm{jobs: []slurm.Job{heldJob(201, "mixing")}}
	b, _ := bridgeWithInterceptor(t, fs, funcs, admittedWorkload("slurm-job-201"))

	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick should tolerate AlreadyExists on create: %v", err)
	}
}

// TestCleanupToleratesJobSetDeleteFailure is the audit AUD1 error-injection
// regression: a JobSet delete failure during cleanup must be logged and
// tolerated (retried next tick), not propagate up and kill the tick.
func TestCleanupToleratesJobSetDeleteFailure(t *testing.T) {
	wantErr := errors.New("conflict: object has been modified")
	funcs := interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if js, ok := obj.(*jobsetv1alpha2.JobSet); ok && js.GetName() == "slurm-job-210" {
				return wantErr
			}
			return c.Delete(ctx, obj, opts...)
		},
	}
	fs := &fakeSlurm{getJob: func(uint64) (*slurm.Job, bool, error) { return nil, false, nil }}
	b, kube := bridgeWithInterceptor(t, fs, funcs, ownedJobSet(t, 210))

	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick must not fail when a JobSet delete fails (tolerated/logged): %v", err)
	}
	if _, found := getJobSet(t, kube, "slurm-job-210"); !found {
		t.Error("JobSet should still exist (delete failed) so it can be retried next tick")
	}
}

// TestCleanupDeleteFailureDoesNotBlockOtherJobSets confirms a delete failure
// on one owned JobSet does not stop cleanup from processing the others in
// the same tick.
func TestCleanupDeleteFailureDoesNotBlockOtherJobSets(t *testing.T) {
	wantErr := errors.New("conflict")
	funcs := interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if js, ok := obj.(*jobsetv1alpha2.JobSet); ok && js.GetName() == "slurm-job-211" {
				return wantErr
			}
			return c.Delete(ctx, obj, opts...)
		},
	}
	fs := &fakeSlurm{getJob: func(uint64) (*slurm.Job, bool, error) { return nil, false, nil }}
	b, kube := bridgeWithInterceptor(t, fs, funcs, ownedJobSet(t, 211), ownedJobSet(t, 212))

	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if _, found := getJobSet(t, kube, "slurm-job-211"); !found {
		t.Error("JobSet 211 delete failed and should still exist")
	}
	if _, found := getJobSet(t, kube, "slurm-job-212"); found {
		t.Error("JobSet 212 had no delete failure injected and should be cleaned up")
	}
}

// TestSnapshotJobSetListFailureFailsTick is the audit AUD1 error-injection
// regression: a failure listing owned JobSets must fail the whole tick (no
// snapshot means no safe decision can be made) rather than proceeding with a
// partial/empty view.
func TestSnapshotJobSetListFailureFailsTick(t *testing.T) {
	wantErr := errors.New("apiserver timeout")
	funcs := interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*jobsetv1alpha2.JobSetList); ok {
				return wantErr
			}
			return c.List(ctx, list, opts...)
		},
	}
	fs := &fakeSlurm{jobs: []slurm.Job{heldJob(220, "mixing")}}
	b, _ := bridgeWithInterceptor(t, fs, funcs)

	err := b.tick(context.Background())
	if err == nil {
		t.Fatal("expected tick to fail when the owned-JobSet snapshot LIST fails")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("tick error = %v, want it to wrap %v", err, wantErr)
	}
}

// TestSnapshotWorkloadListFailureFailsTick is the Workload-LIST half of the
// same regression: the snapshot also lists Workloads (as an
// UnstructuredList), and that failure must equally fail the tick.
func TestSnapshotWorkloadListFailureFailsTick(t *testing.T) {
	wantErr := errors.New("apiserver timeout")
	funcs := interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if ul, ok := list.(*unstructured.UnstructuredList); ok && ul.GroupVersionKind() == kueue.WorkloadListGVK {
				return wantErr
			}
			return c.List(ctx, list, opts...)
		},
	}
	fs := &fakeSlurm{jobs: []slurm.Job{heldJob(221, "mixing")}}
	b, _ := bridgeWithInterceptor(t, fs, funcs)

	err := b.tick(context.Background())
	if err == nil {
		t.Fatal("expected tick to fail when the Workload snapshot LIST fails")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("tick error = %v, want it to wrap %v", err, wantErr)
	}
}
