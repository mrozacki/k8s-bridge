package slurm

import (
	"encoding/json"
	"testing"
)

// TestJobExclusiveParsing pins the --exclusive parsing contract, including the
// deliberate tolerance of the field's uncertain wire shape: an array, a bare
// string, null, an absent field, and an UNEXPECTED shape must all decode
// without failing the whole job (the ListJobs stream must never break on one
// odd field), degrading to "not exclusive" when the value is not a recognised
// exclusivity token.
func TestJobExclusiveParsing(t *testing.T) {
	cases := []struct {
		name string
		json string
		want bool
	}{
		{"array true", `{"exclusive":["true"]}`, true},
		{"array false", `{"exclusive":["false"]}`, false},
		{"bare string true", `{"exclusive":"true"}`, true},
		{"user flavour", `{"exclusive":["user"]}`, true},
		{"topo flavour", `{"exclusive":["topo"]}`, true},
		{"absent", `{}`, false},
		{"null", `{"exclusive":null}`, false},
		{"empty array", `{"exclusive":[]}`, false},
		{"unexpected object shape degrades to not-exclusive", `{"exclusive":{"weird":1}}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var j Job
			if err := json.Unmarshal([]byte(tc.json), &j); err != nil {
				t.Fatalf("unmarshal %s must not fail the job decode: %v", tc.json, err)
			}
			if got := j.IsExclusive(); got != tc.want {
				t.Errorf("IsExclusive() = %v, want %v (json %s)", got, tc.want, tc.json)
			}
		})
	}
}
