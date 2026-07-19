package kueue

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func wlWithConditions(conds ...map[string]any) *unstructured.Unstructured {
	items := make([]any, len(conds))
	for i, c := range conds {
		items[i] = c
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{"conditions": items},
	}}
}

func TestIsEvicted(t *testing.T) {
	cases := []struct {
		name string
		wl   *unstructured.Unstructured
		want bool
	}{
		{"nil", nil, false},
		{"no conditions", wlWithConditions(), false},
		{"evicted true", wlWithConditions(map[string]any{"type": "Evicted", "status": "True", "reason": "Preempted"}), true},
		{"evicted false", wlWithConditions(map[string]any{"type": "Evicted", "status": "False"}), false},
		{"only admitted", wlWithConditions(map[string]any{"type": "Admitted", "status": "True"}), false},
		{"admitted false + evicted true", wlWithConditions(
			map[string]any{"type": "Admitted", "status": "False"},
			map[string]any{"type": "Evicted", "status": "True"},
		), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsEvicted(tc.wl); got != tc.want {
				t.Errorf("IsEvicted() = %v, want %v", got, tc.want)
			}
		})
	}
}
