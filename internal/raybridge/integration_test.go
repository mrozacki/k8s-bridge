//go:build integration

package raybridge

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/mrozacki/k8s-bridge/internal/ray"
	"github.com/mrozacki/k8s-bridge/internal/rayconfig"
)

// TestReconcileAgainstRealAPIServer runs the ray-bridge reconciler against a
// REAL kube-apiserver (envtest) with the REAL RayJob, JobSet and Kueue Workload
// CRDs. This is what the fake-client unit tests cannot prove: that the RayJob
// field paths the bridge reads and patches (spec.suspend, spec.clusterSelector,
// spec.entrypointResources) and the worker JobSet it emits actually match the
// published schemas and survive a schema-validated server round-trip. No Kueue
// or JobSet controller runs in envtest, so admission and worker readiness are
// simulated exactly as the Slurm-side integration test does.
//
// Run with: make test-integration
func TestReconcileAgainstRealAPIServer(t *testing.T) {
	env := &envtest.Environment{
		// test/crd carries the JobSet + Kueue Workload CRDs (shared with the
		// Slurm integration test); test/crd-ray adds the KubeRay RayJob CRD.
		CRDDirectoryPaths:     []string{"../../test/crd", "../../test/crd-ray"},
		ErrorIfCRDPathMissing: true,
	}
	restCfg, err := env.Start()
	if err != nil {
		t.Fatalf("starting envtest (is KUBEBUILDER_ASSETS set? run via make test-integration): %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := jobsetv1alpha2.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube, err := client.New(restCfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := kube.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ray"}}); err != nil {
		t.Fatal(err)
	}

	cfg := &rayconfig.Config{
		Namespace:       "ray",
		LocalQueue:      "ray-main",
		ManagedClusters: []rayconfig.ManagedCluster{{Name: "shared", HeadAddress: "shared-head.ray:6379"}},
		PoolMappings:    []rayconfig.PoolMapping{{Pool: "batch", WorkloadPriorityClass: "normal-priority"}},
		Worker:          rayconfig.Worker{Image: "rayproject/ray:2.9.0"},
	}
	cfg.ApplyDefaults()
	r := &Reconciler{Kube: kube, Cfg: cfg, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	// An inner RayJob created through the REAL RayJob schema (pin-gate model,
	// ADR-0013: no spec.suspend — KubeRay forbids it in clusterSelector mode).
	rj := &unstructured.Unstructured{}
	rj.SetGroupVersionKind(ray.RayJobGVK)
	rj.SetNamespace("ray")
	rj.SetName("job-a")
	rj.SetAnnotations(map[string]string{
		ray.PoolAnnotation:    "batch",
		ray.WorkersAnnotation: "2",
	})
	_ = unstructured.SetNestedField(rj.Object, "python train.py", "spec", "entrypoint")
	_ = unstructured.SetNestedStringMap(rj.Object, map[string]string{"ray.io/cluster": "shared"}, "spec", "clusterSelector")
	if err := kube.Create(ctx, rj); err != nil {
		t.Fatalf("creating RayJob against real schema: %v", err)
	}

	req := reconcile.Request{NamespacedName: client.ObjectKey{Namespace: "ray", Name: "job-a"}}

	// Reconcile 1: the worker JobSet must pass real JobSet schema validation,
	// carry the RayJob owner ref, and advertise the pin resource so the RayJob's
	// driver is gated on Kueue admission.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile 1: %v", err)
	}
	js := &jobsetv1alpha2.JobSet{}
	if err := kube.Get(ctx, client.ObjectKey{Namespace: "ray", Name: "ray-workers-job-a"}, js); err != nil {
		t.Fatalf("worker JobSet not created on real API server: %v", err)
	}
	if got := js.Spec.ReplicatedJobs[0].Replicas; got != 2 {
		t.Errorf("replicas survived round-trip wrong: %d, want 2", got)
	}
	if len(js.OwnerReferences) != 1 || js.OwnerReferences[0].Kind != "RayJob" {
		t.Errorf("RayJob owner ref missing after round-trip: %v", js.OwnerReferences)
	}
	cmd := js.Spec.ReplicatedJobs[0].Template.Spec.Template.Spec.Containers[0].Command
	if len(cmd) != 3 || !strings.Contains(cmd[2], `wm-job-job-a`) {
		t.Errorf("worker command missing pin resource wm-job-job-a: %v", cmd)
	}

	// Reconcile 2: RayJob turns terminal → the worker JobSet must be deleted.
	got := getRayJobReal(t, kube)
	_ = unstructured.SetNestedField(got.Object, "Complete", "status", "jobDeploymentStatus")
	if err := kube.Status().Update(ctx, got); err != nil {
		t.Fatalf("updating rayjob status: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if err := kube.Get(ctx, client.ObjectKey{Namespace: "ray", Name: "ray-workers-job-a"}, &jobsetv1alpha2.JobSet{}); err == nil {
		t.Error("worker JobSet should be deleted after the RayJob turned terminal")
	}
}

func getRayJobReal(t *testing.T, c client.Client) *unstructured.Unstructured {
	t.Helper()
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(ray.RayJobGVK)
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "ray", Name: "job-a"}, u); err != nil {
		t.Fatalf("get RayJob: %v", err)
	}
	return u
}
