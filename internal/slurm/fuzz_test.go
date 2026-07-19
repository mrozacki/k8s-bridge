package slurm

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// FuzzGPUsPerNode fuzzes Job.GPUsPerNode's tres_per_node GRES parser
// (backlog AUD5). Seeded from the exact case table in
// internal/translate/translate_test.go's TestGPUsPerNodeParsing — copied
// here rather than imported (this file must not modify or depend on
// existing test-only symbols) so the parser's documented quirks (bare
// "gres/gpu" defaults to 1, "gres/gpu:0" is 0, "gres/gpufoo" is NOT a GPU
// request, malformed suffixes fall back to 0) all start in the corpus.
//
// Properties asserted: the parser must never panic on arbitrary input, and
// the result must never be negative (translate.go feeds it straight into a
// Kubernetes extended-resource quantity, which cannot be negative).
func FuzzGPUsPerNode(f *testing.F) {
	seeds := []string{
		"gres/gpu:2",
		"gres/gpu=2",
		"gres/gpu:a100:4",
		"gres/gpu",
		"",
		"cpu=4,gres/gpu:1",
		"license/foo:3",
		"gres/gpu:0",
		"gres/gpufoo:2",
		"cpu=4,gres/gpufoo:2",
		// extra shapes worth seeding: negative numbers, huge numbers,
		// trailing/leading separators, repeated commas, non-numeric suffix.
		"gres/gpu:-1",
		"gres/gpu:99999999999999999999",
		"gres/gpu:",
		"gres/gpu=",
		",,gres/gpu:1,,",
		"gres/gpu:a100:",
		"gres/gpu:a100:abc",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, tres string) {
		j := &Job{TresPerNode: tres}
		got := j.GPUsPerNode()
		if got < 0 {
			t.Errorf("GPUsPerNode(%q) = %d, want >= 0", tres, got)
		}
	})
}

// FuzzJobEnvelopeDecode fuzzes the slurmrestd response-decoding path
// (doHTTP's errors/warnings envelope check plus Job JSON unmarshal) via the
// exported Client.ListJobs seam: an httptest server echoes the fuzzed bytes
// as the HTTP response body, exactly like a misbehaving or unexpected
// slurmrestd response would. This is the cleanest seam that doesn't require
// exporting the unexported envelope type or editing client.go.
//
// Property asserted: ListJobs must never panic, regardless of what
// slurmrestd (or a malicious/misconfigured proxy in front of it) returns —
// it may only return an error or a valid job list.
func FuzzJobEnvelopeDecode(f *testing.F) {
	seeds := []string{
		`{"jobs": [], "errors": [], "warnings": []}`,
		`{"jobs": [{"job_id": 3, "name": "wrap", "partition": "mixing", "job_state": ["PENDING"], "state_reason": "JobHeldUser", "hold": true, "priority": {"set": true, "infinite": false, "number": 0}, "tasks": {"set": true, "infinite": false, "number": 2}, "cpus_per_task": {"set": true, "infinite": false, "number": 1}, "memory_per_cpu": {"set": true, "infinite": false, "number": 2048}, "tres_per_node": "gres/gpu:1"}], "errors": [], "warnings": []}`,
		`{"errors": [{"description": "boom"}], "warnings": []}`,
		`{"jobs": [], "errors": [], "warnings": [{"description": "Zero jobs to dump"}]}`,
		`<html>not json</html>`,
		`{}`,
		``,
		`null`,
		`{"jobs": null}`,
		`{"jobs": [{}]}`,
		`{"jobs": [{"job_id": -1}]}`,
		`{"jobs": "not-an-array"}`,
		`[1,2,3]`,
		`{"jobs": [{"tasks": "not-an-object"}]}`,
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, body string) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(body))
		}))
		defer srv.Close()

		tokenFile := filepath.Join(t.TempDir(), "token")
		if err := os.WriteFile(tokenFile, []byte("jwt-secret"), 0o600); err != nil {
			t.Fatal(err)
		}
		c, err := NewClient(Options{BaseURL: srv.URL, User: "root", TokenFile: tokenFile})
		if err != nil {
			t.Fatal(err)
		}

		// Only the decode path is under test; any returned error is fine, a
		// panic is not.
		_ = c.ListJobs(context.Background(), func(Job) error { return nil })
	})
}
