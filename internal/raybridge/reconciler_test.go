package raybridge

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/mrozacki/k8s-bridge/internal/ray"
	"github.com/mrozacki/k8s-bridge/internal/rayconfig"
	"github.com/mrozacki/k8s-bridge/internal/raytranslate"
)

const (
	ns      = "ray"
	jobName = "job-a"
	jsName  = "ray-workers-job-a"
)

func testScheme(tb testing.TB) *runtime.Scheme {
	tb.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		tb.Fatal(err)
	}
	if err := jobsetv1alpha2.AddToScheme(s); err != nil {
		tb.Fatal(err)
	}
	// The bridge reads RayJobs as unstructured (thin-surface), so register the
	// GVKs the fake client tracks.
	s.AddKnownTypeWithName(ray.RayJobGVK, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(ray.RayJobListGVK, &unstructured.UnstructuredList{})
	return s
}

func testCfg() *rayconfig.Config {
	c := &rayconfig.Config{
		Namespace:       ns,
		LocalQueue:      "main",
		ManagedClusters: []rayconfig.ManagedCluster{{Name: "shared", HeadAddress: "shared-head.ray:6379"}},
		PoolMappings:    []rayconfig.PoolMapping{{Pool: "batch", WorkloadPriorityClass: "normal-priority"}},
		Worker:          rayconfig.Worker{Image: "rayproject/ray:2.9.0"},
	}
	c.ApplyDefaults()
	return c
}

func rayJob(cluster, pool string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(ray.RayJobGVK)
	u.SetNamespace(ns)
	u.SetName(jobName)
	u.SetUID("uid-123")
	u.SetAnnotations(map[string]string{"ray-bridge.x-k8s.io/pool": pool})
	if cluster != "" {
		_ = unstructured.SetNestedStringMap(u.Object, map[string]string{"ray.io/cluster": cluster}, "spec", "clusterSelector")
	}
	return u
}

func newReconciler(tb testing.TB, objs ...client.Object) *Reconciler {
	tb.Helper()
	return &Reconciler{
		Kube: fake.NewClientBuilder().WithScheme(testScheme(tb)).WithObjects(objs...).Build(),
		Cfg:  testCfg(),
		Log:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func req() reconcile.Request {
	return reconcile.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: jobName}}
}

func TestReconcileCreatesWorkerJobSet(t *testing.T) {
	r := newReconciler(t, rayJob("shared", "batch"))
	if _, err := r.Reconcile(context.Background(), req()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	js := &jobsetv1alpha2.JobSet{}
	if err := r.Kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: jsName}, js); err != nil {
		t.Fatalf("worker JobSet not created: %v", err)
	}
	if js.Labels[raytranslate.RayJobUIDLabel] != "uid-123" {
		t.Errorf("owner uid label = %q", js.Labels[raytranslate.RayJobUIDLabel])
	}
	if len(js.OwnerReferences) != 1 || js.OwnerReferences[0].Kind != "RayJob" {
		t.Errorf("expected RayJob owner ref, got %v", js.OwnerReferences)
	}
	// Reconcile is idempotent: a second pass must not error on the existing set.
	if _, err := r.Reconcile(context.Background(), req()); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
}

func TestReconcileIgnoresOutOfScope(t *testing.T) {
	// Not an inner workload (no clusterSelector) → no JobSet.
	r := newReconciler(t, rayJob("", "batch"))
	if _, err := r.Reconcile(context.Background(), req()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	js := &jobsetv1alpha2.JobSet{}
	if err := r.Kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: jsName}, js); err == nil {
		t.Errorf("JobSet should not be created for a non-inner workload")
	}
}

func TestReconcileIgnoresUnmanagedPool(t *testing.T) {
	r := newReconciler(t, rayJob("shared", "ghost"))
	if _, err := r.Reconcile(context.Background(), req()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	js := &jobsetv1alpha2.JobSet{}
	if err := r.Kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: jsName}, js); err == nil {
		t.Errorf("no JobSet should be created for an unmanaged pool")
	}
}

func TestReconcileIgnoresInvalidAnnotation(t *testing.T) {
	j := rayJob("shared", "batch")
	ann := j.GetAnnotations()
	ann["ray-bridge.x-k8s.io/workers"] = "three"
	j.SetAnnotations(ann)
	r := newReconciler(t, j)
	if _, err := r.Reconcile(context.Background(), req()); err != nil {
		t.Fatalf("reconcile should swallow a permanent spec error, got: %v", err)
	}
	js := &jobsetv1alpha2.JobSet{}
	if err := r.Kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: jsName}, js); err == nil {
		t.Errorf("no JobSet should be created for an invalid worker-shape annotation")
	}
}

func TestReconcileRetriesOnStaleWorkerJobSet(t *testing.T) {
	// A worker JobSet with our name but a DIFFERENT RayJob UID (a prior,
	// terminating RayJob of the same name) must not be mistaken for ours: the
	// reconcile returns an error so it retries once the stale one finalizes
	// (review finding).
	stale := &jobsetv1alpha2.JobSet{}
	stale.Name = jsName
	stale.Namespace = ns
	stale.Labels = map[string]string{
		raytranslate.ManagedByLabel: raytranslate.ManagedByValue,
		raytranslate.RayJobUIDLabel: "OLD-uid",
	}
	r := newReconciler(t, rayJob("shared", "batch"), stale)
	if _, err := r.Reconcile(context.Background(), req()); err == nil {
		t.Errorf("expected a retry error when a stale worker JobSet with a mismatched UID exists")
	}
}

func TestReconcileMissingRayJobIsNoop(t *testing.T) {
	r := newReconciler(t) // empty client
	if _, err := r.Reconcile(context.Background(), req()); err != nil {
		t.Fatalf("reconcile of a gone RayJob should be a no-op, got: %v", err)
	}
}

func TestReconcileCleansUpTerminalJob(t *testing.T) {
	r := newReconciler(t, rayJob("shared", "batch"))
	if _, err := r.Reconcile(context.Background(), req()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	// Mark the RayJob terminal.
	j := getRayJob(t, r.Kube)
	_ = unstructured.SetNestedField(j.Object, "Complete", "status", "jobDeploymentStatus")
	if err := r.Kube.Update(context.Background(), j); err != nil {
		t.Fatalf("update status: %v", err)
	}
	if _, err := r.Reconcile(context.Background(), req()); err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	js := &jobsetv1alpha2.JobSet{}
	if err := r.Kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: jsName}, js); err == nil {
		t.Errorf("worker JobSet should be deleted for a terminal RayJob")
	}
}

func TestMapJobSetToRayJob(t *testing.T) {
	js := &jobsetv1alpha2.JobSet{}
	js.Namespace = ns
	js.Name = jsName
	js.Labels = map[string]string{raytranslate.ManagedByLabel: raytranslate.ManagedByValue}
	got := mapJobSetToRayJob(context.Background(), js)
	if len(got) != 1 || got[0].Name != jobName || got[0].Namespace != ns {
		t.Fatalf("mapJobSetToRayJob = %v", got)
	}
	// Not ours (no managed-by label) → nothing.
	js.Labels = nil
	if got := mapJobSetToRayJob(context.Background(), js); got != nil {
		t.Errorf("unlabeled JobSet should map to nothing, got %v", got)
	}
	// Wrong prefix → nothing.
	js.Labels = map[string]string{raytranslate.ManagedByLabel: raytranslate.ManagedByValue}
	js.Name = "something-else"
	if got := mapJobSetToRayJob(context.Background(), js); got != nil {
		t.Errorf("non-ours JobSet name should map to nothing, got %v", got)
	}
}

func getRayJob(t *testing.T, c client.Client) *unstructured.Unstructured {
	t.Helper()
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(ray.RayJobGVK)
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: jobName}, u); err != nil {
		t.Fatalf("get RayJob: %v", err)
	}
	return u
}
