//go:build integration

package bridge

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/mrozacki/k8s-bridge/internal/config"
	"github.com/mrozacki/k8s-bridge/internal/translate"
)

// batchJobTemplate builds the minimal valid batchv1.JobTemplateSpec needed
// to satisfy the real JobSet CRD's schema (envtest enforces it, unlike the
// fake client elsewhere in this package) without pulling in the full
// translate.ToJobSet machinery this test does not need.
func batchJobTemplate() batchv1.JobTemplateSpec {
	one := int32(1)
	return batchv1.JobTemplateSpec{
		Spec: batchv1.JobSpec{
			Parallelism: &one,
			Completions: &one,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:  "slurmd",
						Image: "busybox",
					}},
				},
			},
		},
	}
}

// TestManagerWatchNudgeAgainstRealAPIServer is the ADR-0011 regression that
// needs a REAL kube-apiserver + informer cache (envtest), not the fake
// client: it proves the full Manager -> cache -> watch -> NudgeReconciler ->
// Bridge.Nudge() -> Run() chain actually wakes the poll loop when a
// bridge-owned JobSet changes, using the exact wiring cmd/k8s-bridge/main.go
// assembles (builder.ControllerManagedBy + a label predicate), not a
// hand-rolled substitute.
//
// Run with: make test-integration
func TestManagerWatchNudgeAgainstRealAPIServer(t *testing.T) {
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{"../../test/crd"},
		ErrorIfCRDPathMissing: true,
	}
	restCfg, err := env.Start()
	if err != nil {
		t.Fatalf("starting envtest (is KUBEBUILDER_ASSETS set? run via make test-integration): %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	scheme := testScheme(t)
	mgr, err := manager.New(restCfg, manager.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		// Leader election off: this test only needs one instance and a real
		// Lease round trip is exercised separately in unit-level intent
		// (RunnableFunc grouping is controller-runtime's own tested
		// behavior, verified by reading its source — see ADR-0011); what
		// THIS test must prove is the watch -> nudge wiring.
		LeaderElection: false,
	})
	if err != nil {
		t.Fatalf("manager.New: %v", err)
	}

	kube := mgr.GetClient()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := kube.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "slurm-jobs"},
	}); err != nil {
		t.Fatal(err)
	}

	fs := &fakeSlurm{}
	cfg := &config.Config{
		Namespace:  "slurm-jobs",
		LocalQueue: "main",
		PartitionMappings: []config.PartitionMapping{
			{PartitionName: "mixing", WorkloadPriorityClass: "normal-priority"},
		},
		Slurmd: config.Slurmd{Image: "slurmd:test", ConfServer: "ctl:6817", AuthSecretName: "s"},
		// Deliberately long: if the watch->nudge chain did not fire, the
		// test's short wait below would only ever see the initial tick.
		PollInterval: config.Duration{Duration: 10 * time.Second},
	}
	b := New(cfg, kube, fs, slog.Default())

	tickDone := make(chan struct{}, 8)
	b.OnTick = func(error) { tickDone <- struct{}{} }

	// Exactly the wiring main.go assembles for the JobSet nudge: a
	// label-predicated watch driving bridge.NudgeReconciler.
	if err := builder.ControllerManagedBy(mgr).
		Named("jobset-nudge").
		For(&jobsetv1alpha2.JobSet{}, builder.WithPredicates(predicate.NewPredicateFuncs(func(obj client.Object) bool {
			js, ok := obj.(*jobsetv1alpha2.JobSet)
			return ok && js.Labels[translate.ManagedByLabel] == translate.ManagedByValue
		}))).
		Complete(&NudgeReconciler{Bridge: b, Source: "jobset"}); err != nil {
		t.Fatalf("wiring JobSet watch-nudge: %v", err)
	}

	if err := mgr.Add(manager.RunnableFunc(b.Run)); err != nil {
		t.Fatalf("registering bridge runnable: %v", err)
	}

	mgrErr := make(chan error, 1)
	go func() { mgrErr <- mgr.Start(ctx) }()

	if !mgr.GetCache().WaitForCacheSync(ctx) {
		t.Fatal("cache never synced")
	}

	// Wait for the initial (timer) tick before creating the JobSet, so the
	// nudge-triggered tick is observed as a clearly SECOND event.
	select {
	case <-tickDone:
	case <-time.After(5 * time.Second):
		t.Fatal("initial tick did not complete in time")
	}

	// Creating a bridge-owned JobSet is exactly the kind of event the
	// nudge exists for (a JobSet becoming Ready/Failed/Completed starts
	// with a Create/Update the watch observes).
	js := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "slurm-job-999",
			Namespace: "slurm-jobs",
			Labels: map[string]string{
				translate.ManagedByLabel:  translate.ManagedByValue,
				translate.SlurmJobIDLabel: "999",
			},
		},
		Spec: jobsetv1alpha2.JobSetSpec{
			ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{{
				Name:     "workers",
				Replicas: 1,
				Template: batchJobTemplate(),
			}},
		},
	}
	if err := kube.Create(ctx, js); err != nil {
		t.Fatalf("creating JobSet: %v", err)
	}

	select {
	case <-tickDone:
		// A tick ran well before the 10s poll interval would have elapsed —
		// proof the real watch -> Nudge() -> Run() path fired end to end.
	case <-time.After(5 * time.Second):
		t.Fatal("JobSet create did not trigger a nudge tick via the real Manager/cache/watch chain")
	}
}

// TestBridgeRunGatedByLeaderElection is the ADR-0011 regression proving
// Bridge.Run, registered as a plain manager.RunnableFunc, does not tick at
// all while ANOTHER identity holds the coordination.k8s.io Lease — only
// once that lease is freed does this manager win the election and start
// ticking. This is what makes a second bridge replica safe: it sits idle
// instead of double-processing/double-creating JobSets. Uses a real
// kube-apiserver (envtest) because leader election is fundamentally a
// Lease object round trip that a fake client cannot exercise meaningfully.
func TestBridgeRunGatedByLeaderElection(t *testing.T) {
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{"../../test/crd"},
		ErrorIfCRDPathMissing: true,
	}
	restCfg, err := env.Start()
	if err != nil {
		t.Fatalf("starting envtest (is KUBEBUILDER_ASSETS set? run via make test-integration): %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	scheme := testScheme(t)
	kube, err := client.New(restCfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := kube.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "leader-elect-test"},
	}); err != nil {
		t.Fatal(err)
	}

	// Pre-hold the Lease as a competing identity, well past "now", so this
	// test's Manager cannot possibly win the election immediately no
	// matter how fast it starts up.
	holder := "someone-else"
	now := metav1.NewMicroTime(time.Now())
	leaseDurationSeconds := int32(3600) // long: must NOT expire during this test
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "k8s-bridge-leader", Namespace: "leader-elect-test"},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &holder,
			LeaseDurationSeconds: &leaseDurationSeconds,
			RenewTime:            &now,
		},
	}
	if err := kube.Create(ctx, lease); err != nil {
		t.Fatalf("creating competing Lease: %v", err)
	}

	mgr, err := manager.New(restCfg, manager.Options{
		Scheme:                  scheme,
		Metrics:                 metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress:  "0",
		LeaderElection:          true,
		LeaderElectionID:        "k8s-bridge-leader",
		LeaderElectionNamespace: "leader-elect-test",
	})
	if err != nil {
		t.Fatalf("manager.New: %v", err)
	}

	fs := &fakeSlurm{}
	cfg := &config.Config{
		Namespace:  "leader-elect-test",
		LocalQueue: "main",
		PartitionMappings: []config.PartitionMapping{
			{PartitionName: "mixing", WorkloadPriorityClass: "normal-priority"},
		},
		Slurmd:       config.Slurmd{Image: "slurmd:test", ConfServer: "ctl:6817", AuthSecretName: "s"},
		PollInterval: config.Duration{Duration: 200 * time.Millisecond},
	}
	b := New(cfg, mgr.GetClient(), fs, slog.Default())
	var ticks atomic.Int32
	b.OnTick = func(error) { ticks.Add(1) }

	if err := mgr.Add(manager.RunnableFunc(b.Run)); err != nil {
		t.Fatalf("registering bridge runnable: %v", err)
	}

	mgrErr := make(chan error, 1)
	go func() { mgrErr <- mgr.Start(ctx) }()

	// While the competing Lease holder's term has 3600s left, this Manager
	// must NOT be elected, and Run() must NOT tick, no matter how long we
	// wait relative to the 200ms poll interval.
	time.Sleep(1500 * time.Millisecond)
	if got := ticks.Load(); got != 0 {
		t.Fatalf("ticks = %d while a competing Lease is held, want 0 (Run must not start before winning leader election)", got)
	}

	// Free the lease: delete it so this manager's next leader-election
	// retry can acquire it uncontested.
	if err := kube.Delete(ctx, lease); err != nil {
		t.Fatalf("deleting competing Lease: %v", err)
	}

	deadline := time.After(30 * time.Second)
	for {
		if ticks.Load() > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("Run never ticked after the competing Lease was freed (leader election never won)")
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// TestPrewarmedWorkloadCacheServesFirstTick is the L4 end-to-end regression
