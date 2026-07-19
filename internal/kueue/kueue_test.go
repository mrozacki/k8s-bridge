package kueue

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func workload(conds []any, owner string) *unstructured.Unstructured {
	wl := &unstructured.Unstructured{}
	wl.SetGroupVersionKind(WorkloadGVK)
	wl.SetNamespace("ray")
	wl.SetName("wl-1")
	if owner != "" {
		wl.SetOwnerReferences([]metav1.OwnerReference{{Kind: "JobSet", Name: owner}})
	}
	if conds != nil {
		_ = unstructured.SetNestedSlice(wl.Object, conds, "status", "conditions")
	}
	return wl
}

func TestIsAdmitted(t *testing.T) {
	if IsAdmitted(nil) {
		t.Errorf("nil workload should not be admitted")
	}
	admitted := workload([]any{map[string]any{"type": "Admitted", "status": "True"}}, "")
	if !IsAdmitted(admitted) {
		t.Errorf("admitted workload not detected")
	}
	pending := workload([]any{map[string]any{"type": "Admitted", "status": "False"}}, "")
	if IsAdmitted(pending) {
		t.Errorf("pending workload reported admitted")
	}
	none := workload(nil, "")
	if IsAdmitted(none) {
		t.Errorf("workload with no conditions reported admitted")
	}
}

func TestQuotaReason(t *testing.T) {
	if QuotaReason(nil) != "" {
		t.Errorf("nil workload should have no reason")
	}
	wl := workload([]any{
		map[string]any{"type": "QuotaReserved", "status": "False", "message": "insufficient cpu"},
	}, "")
	if got := QuotaReason(wl); got != "insufficient cpu" {
		t.Errorf("QuotaReason = %q", got)
	}
	reserved := workload([]any{map[string]any{"type": "QuotaReserved", "status": "True"}}, "")
	if got := QuotaReason(reserved); got != "" {
		t.Errorf("reserved quota should have no reason, got %q", got)
	}
}

func TestWorkloadForOwner(t *testing.T) {
	s := runtime.NewScheme()
	s.AddKnownTypeWithName(WorkloadGVK, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(WorkloadListGVK, &unstructured.UnstructuredList{})
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(workload(nil, "ray-workers-job-a")).Build()

	got, err := WorkloadForOwner(context.Background(), c, "ray", "ray-workers-job-a")
	if err != nil {
		t.Fatalf("WorkloadForOwner: %v", err)
	}
	if got == nil {
		t.Fatalf("expected to find the workload owned by the JobSet")
	}
	miss, err := WorkloadForOwner(context.Background(), c, "ray", "ray-workers-other")
	if err != nil {
		t.Fatalf("WorkloadForOwner (miss): %v", err)
	}
	if miss != nil {
		t.Errorf("expected no workload for an unowned JobSet")
	}
}
