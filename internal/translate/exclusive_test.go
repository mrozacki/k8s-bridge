package translate

import (
	"testing"
)

// TestToJobSetExclusiveAddsAntiAffinity is the TC-B6 regression. It asserts the
// translation MECHANISM, not the architecture's incidental one-pod-per-node
// property: before this, --exclusive was silently ignored and a coarser test
// could still pass because each slurmd pod is a dedicated Slurm node anyway.
//
//   - a non-exclusive job must carry NO affinity (unchanged behaviour);
//   - an --exclusive job must carry a REQUIRED pod anti-affinity keyed on
//     ManagedByLabel over kubernetes.io/hostname, and the pod must actually
//     carry that label (or the anti-affinity would select nothing).
func TestToJobSetExclusiveAddsAntiAffinity(t *testing.T) {
	// Non-exclusive: no affinity at all.
	js, err := ToJobSet(heldJob(7, "mixing", 2, 1), testConfig())
	if err != nil {
		t.Fatalf("ToJobSet: %v", err)
	}
	if aff := js.Spec.ReplicatedJobs[0].Template.Spec.Template.Spec.Affinity; aff != nil {
		t.Errorf("non-exclusive job must have no Affinity, got %+v", aff)
	}

	// Exclusive: required pod anti-affinity, one bridge slurmd pod per node.
	exj := heldJob(7, "mixing", 2, 1)
	exj.Exclusive = []string{"true"} // []string is assignable to the unexported field type
	js, err = ToJobSet(exj, testConfig())
	if err != nil {
		t.Fatalf("ToJobSet: %v", err)
	}
	pod := js.Spec.ReplicatedJobs[0].Template.Spec.Template
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.PodAntiAffinity == nil {
		t.Fatalf("exclusive job must set pod anti-affinity, got %+v", pod.Spec.Affinity)
	}
	terms := pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if len(terms) != 1 {
		t.Fatalf("want exactly 1 required anti-affinity term, got %d", len(terms))
	}
	if terms[0].TopologyKey != "kubernetes.io/hostname" {
		t.Errorf("topologyKey = %q, want kubernetes.io/hostname", terms[0].TopologyKey)
	}
	if terms[0].LabelSelector == nil || terms[0].LabelSelector.MatchLabels[ManagedByLabel] != ManagedByValue {
		t.Errorf("anti-affinity must select %s=%s, got %+v", ManagedByLabel, ManagedByValue, terms[0].LabelSelector)
	}
	if got := pod.Labels[ManagedByLabel]; got != ManagedByValue {
		t.Errorf("pod label %s = %q, want %q (anti-affinity selects on it)", ManagedByLabel, got, ManagedByValue)
	}
}
