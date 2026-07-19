package v1alpha1

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// TestAddToSchemeRegistersBothKinds pins the scheme wiring: a client built
// on a scheme that ran AddToScheme must recognize WorkloadMixing and its
// List kind under the k8s-bridge.x-k8s.io/v1alpha1 group-version — this is
// exactly what PR 2's typed-client switch will depend on.
func TestAddToSchemeRegistersBothKinds(t *testing.T) {
	s := runtime.NewScheme()
	if err := AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	for _, kind := range []string{"WorkloadMixing", "WorkloadMixingList"} {
		if !s.Recognizes(GroupVersion.WithKind(kind)) {
			t.Errorf("scheme does not recognize %s in %s", kind, GroupVersion)
		}
	}
}

// TestDeepCopyIsActuallyDeep guards the generated deepcopy against marker or
// regeneration regressions on the reference-holding fields: mutating the
// original's slice-backed spec and status after DeepCopy must not leak into
// the copy. (The zz_generated file is tool-owned and excluded from the
// coverage ratchet; this test asserts its BEHAVIOR, not its lines.)
func TestDeepCopyIsActuallyDeep(t *testing.T) {
	orig := &WorkloadMixing{
		ObjectMeta: metav1.ObjectMeta{Name: "wm", Namespace: "slurm-jobs"},
		Spec: WorkloadMixingSpec{
			PartitionMappings: []PartitionMapping{
				{PartitionName: "mixing", WorkloadPriorityClass: "normal-priority"},
			},
		},
		Status: WorkloadMixingStatus{
			Conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "TickSucceeded"},
			},
		},
	}
	cp := orig.DeepCopy()
	orig.Spec.PartitionMappings[0].PartitionName = "mutated"
	orig.Status.Conditions[0].Reason = "Mutated"
	if cp.Spec.PartitionMappings[0].PartitionName != "mixing" {
		t.Error("DeepCopy shares the PartitionMappings backing array with the original")
	}
	if cp.Status.Conditions[0].Reason != "TickSucceeded" {
		t.Error("DeepCopy shares the Conditions backing array with the original")
	}
}
