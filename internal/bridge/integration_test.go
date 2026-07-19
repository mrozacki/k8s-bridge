//go:build integration

package bridge

import (
	"context"
	"log/slog"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/mrozacki/k8s-bridge/api/v1alpha1"
	"github.com/mrozacki/k8s-bridge/internal/config"
	"github.com/mrozacki/k8s-bridge/internal/slurm"
)

// workloadMixingGVK is only needed where these tests deliberately talk to the
// API server WITHOUT the typed API (simulating pre-migration writers and
// schema-level behavior the typed client would paper over). The controller
// itself has no unstructured WorkloadMixing path left (ADR-0014 PR 2).
var workloadMixingGVK = schema.GroupVersionKind{
	Group: "k8s-bridge.x-k8s.io", Version: "v1alpha1", Kind: "WorkloadMixing",
}

// wmScheme returns testScheme extended with the typed WorkloadMixing API,
// which is how cmd/k8s-bridge/main.go composes the real scheme.
func wmScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := testScheme(t)
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

// TestFullLifecycleAgainstRealAPIServer runs the reconcile loop against a
// REAL kube-apiserver (envtest) with the real JobSet CRD — everything the
// fake client cannot verify: schema validation, defaulting, field pruning.
// Slurm stays faked; its live behavior is covered by the E2E runbook.
//
// L6: admittedWorkload (reconciler_test.go) now builds its fixture at
// kueue.x-k8s.io/v1beta2, matching kueue.go's workloadGVK/workloadItemGVK
// (bumped from v1beta1 — a live Kueue deployment logged a deprecation
// warning for v1beta1). This test's Create + Status().Update round trip
// against the real envtest CRD (test/crd/kueue-workload-crd.yaml, which
// serves both versions) is what actually proves the bridge's v1beta2 LIST,
// the Admitted-condition read, and the spec.priority patch path all work
// against the real, schema-validated v1beta2 shape — not just a fake client
// that never checks the schema. See TestWorkloadV1Beta2AgainstRealAPIServer
// for a test that names v1beta2 directly rather than through the shared
// fixture helper.
//
// Run with: make test-integration
func TestFullLifecycleAgainstRealAPIServer(t *testing.T) {
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
	ctx := context.Background()
	if err := kube.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "slurm-jobs"},
	}); err != nil {
		t.Fatal(err)
	}

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
	job := heldJob(30, "mixing")
	job.MemoryPerCPU = slurm.Uint64NoVal{Set: true, Number: 2048}
	fs := &fakeSlurm{jobs: []slurm.Job{job}}
	b := New(cfg, kube, fs, slog.Default())

	// Tick 1: the JobSet must pass real schema validation on creation.
	// No Kueue controller runs in envtest, so no Workload exists yet and
	// the admission gate (scale fix) must HOLD the release.
	if err := b.tick(ctx); err != nil {
		t.Fatalf("tick 1: %v", err)
	}
	js, found := getJobSet(t, kube, "slurm-job-30")
	if !found {
		t.Fatal("JobSet not created on real API server")
	}
	if len(fs.released) != 0 {
		t.Fatal("job must not be released before Kueue admission")
	}

	// Simulate Kueue: create an admitted Workload owned by the JobSet
	// (real CRD schema enforced by envtest), then tick again.
	wl := admittedWorkload("slurm-job-30")
	if err := kube.Create(ctx, wl); err != nil {
		t.Fatalf("creating workload: %v", err)
	}
	wl.Object["status"] = map[string]any{"conditions": []any{
		map[string]any{"type": "Admitted", "status": "True", "reason": "Admitted",
			"message": "admitted", "lastTransitionTime": "2026-07-05T00:00:00Z"},
	}}
	if err := kube.Status().Update(ctx, wl); err != nil {
		t.Fatalf("updating workload status: %v", err)
	}
	if err := b.tick(ctx); err != nil {
		t.Fatalf("tick 1b: %v", err)
	}
	if *js.Spec.ReplicatedJobs[0].Template.Spec.Parallelism != 2 {
		t.Errorf("parallelism survived server round-trip wrong: %d",
			*js.Spec.ReplicatedJobs[0].Template.Spec.Parallelism)
	}
	if len(fs.released) != 1 {
		t.Fatalf("job not released: %v", fs.released)
	}

	// Tick 2: job turns terminal -> cleanup must delete the JobSet and the
	// deterministic node records.
	fs.jobs[0].JobState = []string{"COMPLETED"}
	fs.jobs[0].Hold = false
	if err := b.tick(ctx); err != nil {
		t.Fatalf("tick 2: %v", err)
	}
	if _, found := getJobSet(t, kube, "slurm-job-30"); found {
		t.Error("JobSet not cleaned up after terminal job")
	}
	if len(fs.deletedNodes) != 2 {
		t.Errorf("node records deleted = %v, want 2", fs.deletedNodes)
	}
}

// TestLoadConfigFromCRAgainstRealAPIServer is the audit AUD1 schema-drift
// guard: it creates a WorkloadMixing CR with a valid spec against a REAL
// kube-apiserver enforcing the vendored CRD schema
// (test/crd/workloadmixing-crd.yaml, which must track deploy/crd/ — see the
// comment at the top of that file) and runs the real LoadConfigFromCR
// against it. A fake client's Get/Unstructured round-trip cannot catch drift
// between the CRD's openAPIV3Schema and the Config struct (e.g. a field the
// schema requires/rejects that the Go struct does not, or vice versa); only
// a real API server enforcing the schema on Create can.
func TestLoadConfigFromCRAgainstRealAPIServer(t *testing.T) {
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{"../../test/crd"},
		ErrorIfCRDPathMissing: true,
	}
	restCfg, err := env.Start()
	if err != nil {
		t.Fatalf("starting envtest (is KUBEBUILDER_ASSETS set? run via make test-integration): %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	kube, err := client.New(restCfg, client.Options{Scheme: wmScheme(t)})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := kube.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "wm-config-test"},
	}); err != nil {
		t.Fatal(err)
	}

	cr := newWorkloadMixing("wm", "wm-config-test", validSpec())
	if err := kube.Create(ctx, cr); err != nil {
		t.Fatalf("creating WorkloadMixing CR: real API server rejected a spec the schema should accept: %v", err)
	}

	cfg, rv, err := LoadConfigFromCR(ctx, kube, "wm-config-test", "wm")
	if err != nil {
		t.Fatalf("LoadConfigFromCR against real API server: %v", err)
	}
	if rv == "" {
		t.Error("expected a non-empty resourceVersion from the real API server")
	}
	if cfg.Namespace != "wm-config-test" {
		t.Errorf("cfg.Namespace = %q, want wm-config-test", cfg.Namespace)
	}
	if cfg.LocalQueue != "main" {
		t.Errorf("cfg.LocalQueue = %q, want main (from the CR spec)", cfg.LocalQueue)
	}
	if len(cfg.PartitionMappings) != 1 || cfg.PartitionMappings[0].PartitionName != "mixing" {
		t.Errorf("cfg.PartitionMappings = %+v, want one mapping for partition 'mixing'", cfg.PartitionMappings)
	}
	// ApplyDefaults must have run against the real round-tripped spec too.
	if cfg.PollInterval.Duration.String() != "10s" {
		t.Errorf("cfg.PollInterval = %s, want 10s default", cfg.PollInterval.Duration)
	}
	// A spec that never mentions orphan cancellation must leave it off.
	if cfg.CancelOrphanedJobs {
		t.Error("cancelOrphanedJobs must default to false when the CR omits it")
	}
	if cfg.OrphanGraceTicks != 3 {
		t.Errorf("cfg.OrphanGraceTicks = %d, want 3 default", cfg.OrphanGraceTicks)
	}
}

// TestOrphanCancellationFieldsAcceptedByRealCRDSchema closes the gap between
// the Go config struct and the CRD: LoadConfigFromCR decodes with
// DisallowUnknownFields, and the API server validates against the CRD's
// openAPIV3Schema. A field added to one but not the other fails HERE — either
// the server rejects the CR, or the decoder rejects the field — rather than in
// production the first time an operator tries to switch the feature on.
func TestOrphanCancellationFieldsAcceptedByRealCRDSchema(t *testing.T) {
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{"../../test/crd"},
		ErrorIfCRDPathMissing: true,
	}
	restCfg, err := env.Start()
	if err != nil {
		t.Fatalf("starting envtest (is KUBEBUILDER_ASSETS set? run via make test-integration): %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	kube, err := client.New(restCfg, client.Options{Scheme: wmScheme(t)})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := kube.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "wm-orphan-test"},
	}); err != nil {
		t.Fatal(err)
	}

	spec := validSpec()
	spec.CancelOrphanedJobs = true
	spec.OrphanGraceTicks = 5
	// A1 rides the same struct-vs-CRD drift guard: slurmRequestsPerSecond is
	// `type: number` in the schema and a float64 in config.Config; a gap on
	// either side fails here rather than on an operator's first attempt to
	// turn the limiter on.
	spec.SlurmRequestsPerSecond = 12.5

	cr := newWorkloadMixing("wm", "wm-orphan-test", spec)
	if err := kube.Create(ctx, cr); err != nil {
		t.Fatalf("real API server rejected the orphan-cancellation/rate-limit fields — CRD schema is out of sync with config.Config: %v", err)
	}

	cfg, _, err := LoadConfigFromCR(ctx, kube, "wm-orphan-test", "wm")
	if err != nil {
		t.Fatalf("LoadConfigFromCR: %v", err)
	}
	if !cfg.CancelOrphanedJobs {
		t.Error("cfg.CancelOrphanedJobs = false, want true from the CR spec")
	}
	if cfg.OrphanGraceTicks != 5 {
		t.Errorf("cfg.OrphanGraceTicks = %d, want 5 from the CR spec (not the default)", cfg.OrphanGraceTicks)
	}
	if cfg.SlurmRequestsPerSecond != 12.5 {
		t.Errorf("cfg.SlurmRequestsPerSecond = %v, want 12.5 from the CR spec", cfg.SlurmRequestsPerSecond)
	}
}

// TestWorkloadV1Beta2AgainstRealAPIServer is the L6 regression: it names
// kueue.x-k8s.io/v1beta2 directly (rather than through the shared
// admittedWorkload fixture other tests in this package reuse), so this test
// keeps proving the bridge reads v1beta2 Workloads even if that shared
// fixture is ever changed for an unrelated reason. Exercises exactly the
// fields kueue.go actually reads: status.conditions (Admitted, QuotaReserved)
// and spec.priority, against the real envtest-enforced v1beta2 schema in
// test/crd/kueue-workload-crd.yaml (served: true, storage: true there).
//
// Run with: make test-integration
func TestWorkloadV1Beta2AgainstRealAPIServer(t *testing.T) {
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
	ctx := context.Background()
	if err := kube.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "slurm-jobs"},
	}); err != nil {
		t.Fatal(err)
	}

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
	job := heldJob(77, "mixing")
	job.MemoryPerCPU = slurm.Uint64NoVal{Set: true, Number: 2048}
	fs := &fakeSlurm{jobs: []slurm.Job{job}}
	b := New(cfg, kube, fs, slog.Default())

	// Tick 1 creates the JobSet; no Workload exists yet, so release must
	// stay gated (same admission-gate invariant as the sibling v1beta1-era
	// test, now proven at v1beta2 too).
	if err := b.tick(ctx); err != nil {
		t.Fatalf("tick 1: %v", err)
	}
	if _, found := getJobSet(t, kube, "slurm-job-77"); !found {
		t.Fatal("JobSet not created on real API server")
	}
	if len(fs.released) != 0 {
		t.Fatal("job must not be released before Kueue admission")
	}

	// Build the Workload directly at kueue.x-k8s.io/v1beta2 — deliberately
	// not reusing admittedWorkload, so this test's coverage of v1beta2
	// does not depend on that helper staying on v1beta2.
	wl := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kueue.x-k8s.io/v1beta2",
		"kind":       "Workload",
		"metadata": map[string]any{
			"name":      "wl-slurm-job-77-v1beta2",
			"namespace": "slurm-jobs",
			"ownerReferences": []any{map[string]any{
				"apiVersion": "jobset.x-k8s.io/v1alpha2",
				"kind":       "JobSet",
				"name":       "slurm-job-77",
				"uid":        "test-uid-slurm-job-77",
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
	}}
	if err := kube.Create(ctx, wl); err != nil {
		t.Fatalf("creating v1beta2 workload: real API server rejected the v1beta2 schema: %v", err)
	}
	wl.Object["status"] = map[string]any{"conditions": []any{
		map[string]any{"type": "Admitted", "status": "True", "reason": "Admitted",
			"message": "admitted", "lastTransitionTime": "2026-07-06T00:00:00Z"},
	}}
	if err := kube.Status().Update(ctx, wl); err != nil {
		t.Fatalf("updating v1beta2 workload status: %v", err)
	}

	// Tick 2: the bridge's v1beta2 LIST (workloadGVK in kueue.go) must find
	// this Workload, isAdmitted must read its Admitted=True condition, and
	// the job must be released onto its dynamic nodes.
	if err := b.tick(ctx); err != nil {
		t.Fatalf("tick 2: %v", err)
	}
	if len(fs.released) != 1 {
		t.Fatalf("job not released after v1beta2 Workload admitted: %v", fs.released)
	}
}

// TestReadyConditionObservedGenerationPersistsAgainstRealAPIServer is the
// ADR-0014 PR 2 acceptance proof for the conditions schema change, and it can
// ONLY be an envtest: the pre-migration structural schema silently PRUNED
// observedGeneration on the apiserver — the controller stamped it, every
// fake-client test saw it, and a real cluster dropped it. Three things are
// pinned against the real, schema-enforcing apiserver:
//  1. an OLD-SHAPE condition (written exactly as the pre-migration controller
//     persisted it: five fields, no observedGeneration) is still accepted by
//     the upgraded CRD and decodes through the typed client — upgrade safety
//     for CRs whose status predates the schema change;
//  2. UpdateReadyCondition's observedGeneration now actually PERSISTS, and
//     tracks metadata.generation across a spec change;
//  3. foreign condition types survive the Ready merge on the real server,
//     not just under the fake client.
func TestReadyConditionObservedGenerationPersistsAgainstRealAPIServer(t *testing.T) {
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{"../../test/crd"},
		ErrorIfCRDPathMissing: true,
	}
	restCfg, err := env.Start()
	if err != nil {
		t.Fatalf("starting envtest (is KUBEBUILDER_ASSETS set? run via make test-integration): %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	kube, err := client.New(restCfg, client.Options{Scheme: wmScheme(t)})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := kube.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "wm-status-test"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := kube.Create(ctx, newWorkloadMixing("wm", "wm-status-test", validSpec())); err != nil {
		t.Fatalf("creating WorkloadMixing CR: %v", err)
	}

	// (1) Seed status EXACTLY as the pre-migration controller left it in
	// etcd: an unstructured merge-patch-shaped write of plain-string
	// conditions, no observedGeneration anywhere. Deliberately not the typed
	// client — the point is simulating data written before the typed API
	// existed. A foreign condition rides along for (3).
	oldShape := &unstructured.Unstructured{}
	oldShape.SetGroupVersionKind(workloadMixingGVK)
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "wm-status-test", Name: "wm"}, oldShape); err != nil {
		t.Fatal(err)
	}
	oldShape.Object["status"] = map[string]any{"conditions": []any{
		map[string]any{"type": "Ready", "status": "True", "reason": "TickSucceeded",
			"message": "reconcile loop healthy", "lastTransitionTime": "2026-01-01T00:00:00Z"},
		map[string]any{"type": "SomeOtherCondition", "status": "True", "reason": "Preexisting",
			"message": "set by something else", "lastTransitionTime": "2026-01-01T00:00:00Z"},
	}}
	if err := kube.Status().Update(ctx, oldShape); err != nil {
		t.Fatalf("upgraded CRD rejected a pre-migration-shaped condition write (upgrade safety broken): %v", err)
	}

	got := &v1alpha1.WorkloadMixing{}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "wm-status-test", Name: "wm"}, got); err != nil {
		t.Fatal(err)
	}
	ready := apimeta.FindStatusCondition(got.Status.Conditions, "Ready")
	if ready == nil || ready.Status != metav1.ConditionTrue || ready.Reason != "TickSucceeded" {
		t.Fatalf("old-shape Ready condition did not round-trip through the typed client: %+v", got.Status.Conditions)
	}
	if ready.ObservedGeneration != 0 {
		t.Fatalf("old-shape condition has observedGeneration = %d, want 0 (field was never persisted pre-migration)", ready.ObservedGeneration)
	}

	// (2) The controller's own write path: observedGeneration must persist
	// and equal the CR's current metadata.generation.
	if err := UpdateReadyCondition(ctx, kube, "wm-status-test", "wm", false, "listing slurm jobs: connection refused"); err != nil {
		t.Fatalf("UpdateReadyCondition: %v", err)
	}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "wm-status-test", Name: "wm"}, got); err != nil {
		t.Fatal(err)
	}
	ready = apimeta.FindStatusCondition(got.Status.Conditions, "Ready")
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "TickFailed" {
		t.Fatalf("Ready condition not updated: %+v", got.Status.Conditions)
	}
	if ready.ObservedGeneration != got.Generation || ready.ObservedGeneration == 0 {
		t.Errorf("observedGeneration = %d, want the CR's generation %d — the real apiserver used to prune this field",
			ready.ObservedGeneration, got.Generation)
	}

	// Bump the spec (generation increments) and flip Ready back: the stamp
	// must FOLLOW the generation, which is the whole point of the field.
	got.Spec.PollInterval = "20s"
	if err := kube.Update(ctx, got); err != nil {
		t.Fatal(err)
	}
	if got.Generation < 2 {
		t.Fatalf("generation = %d after a spec update, want >= 2", got.Generation)
	}
	if err := UpdateReadyCondition(ctx, kube, "wm-status-test", "wm", true, "reconcile loop healthy"); err != nil {
		t.Fatalf("UpdateReadyCondition after spec change: %v", err)
	}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "wm-status-test", Name: "wm"}, got); err != nil {
		t.Fatal(err)
	}
	ready = apimeta.FindStatusCondition(got.Status.Conditions, "Ready")
	if ready == nil || ready.ObservedGeneration != got.Generation {
		t.Errorf("after spec change, observedGeneration = %v, want generation %d", ready, got.Generation)
	}

	// (3) The foreign condition must have survived both Ready merges.
	if apimeta.FindStatusCondition(got.Status.Conditions, "SomeOtherCondition") == nil {
		t.Errorf("foreign condition dropped by the Ready merge on the real apiserver: %+v", got.Status.Conditions)
	}
}

// TestUnknownSpecFieldPrunedByRealAPIServer pins where unknown-field
// strictness lives after the typed switch (ADR-0014 PR 2): the pre-migration
// loader re-decoded the CR spec with DisallowUnknownFields, a guard that only
// ever fired under the fake client — against a real apiserver the structural
// schema prunes unknown fields BEFORE persistence, so the strict decoder had
// nothing left to reject. This test makes that contract explicit: a phantom
// field is silently dropped on Create (not an error — that would need CRD
// x-kubernetes-preserve-unknown-fields tricks or a validating webhook, both
// out of scope), and the load succeeds on the pruned spec.
func TestUnknownSpecFieldPrunedByRealAPIServer(t *testing.T) {
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{"../../test/crd"},
		ErrorIfCRDPathMissing: true,
	}
	restCfg, err := env.Start()
	if err != nil {
		t.Fatalf("starting envtest (is KUBEBUILDER_ASSETS set? run via make test-integration): %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	kube, err := client.New(restCfg, client.Options{Scheme: wmScheme(t)})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := kube.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "wm-prune-test"},
	}); err != nil {
		t.Fatal(err)
	}

	// Unstructured on purpose: the typed client cannot even express an
	// unknown field, which is exactly the migration's point.
	cr := &unstructured.Unstructured{}
	cr.SetGroupVersionKind(workloadMixingGVK)
	cr.SetName("wm")
	cr.SetNamespace("wm-prune-test")
	cr.Object["spec"] = map[string]any{
		"slurmRestURL": "https://slurm-restapi.slurm:6820",
		"localQueue":   "main",
		"partitionMappings": []any{
			map[string]any{"partitionName": "mixing", "workloadPriorityClass": "normal-priority"},
		},
		"slurmd": map[string]any{
			"image":          "slurmd:test",
			"confServer":     "ctl:6817",
			"authSecretName": "slurm-auth-slurm",
		},
		// Phantom field, not in the schema (deliberately named after the
		// backlog-A2 non-field the API comments warn about).
		"slurmTokenSecretRef": "some-secret",
	}
	if err := kube.Create(ctx, cr); err != nil {
		t.Fatalf("creating WorkloadMixing CR with an unknown field: %v", err)
	}

	stored := &unstructured.Unstructured{}
	stored.SetGroupVersionKind(workloadMixingGVK)
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "wm-prune-test", Name: "wm"}, stored); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := unstructured.NestedString(stored.Object, "spec", "slurmTokenSecretRef"); found {
		t.Fatal("unknown spec field survived persistence — the CRD's structural schema should have pruned it")
	}

	cfg, _, err := LoadConfigFromCR(ctx, kube, "wm-prune-test", "wm")
	if err != nil {
		t.Fatalf("LoadConfigFromCR after pruning: %v", err)
	}
	if cfg.LocalQueue != "main" {
		t.Errorf("cfg.LocalQueue = %q, want main (the known fields must load normally)", cfg.LocalQueue)
	}
}
