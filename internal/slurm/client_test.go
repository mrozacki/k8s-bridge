package slurm

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

type capturedRequest struct {
	method, path, token, user, body string
}

// newTestServer returns a client wired to an httptest server that answers
// with `response` and records every request.
func newTestServer(t *testing.T, status int, response string) (*Client, *capturedRequest) {
	t.Helper()
	captured := &capturedRequest{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		captured.method, captured.path, captured.body = r.Method, r.URL.Path, string(body)
		captured.token = r.Header.Get("X-SLURM-USER-TOKEN")
		captured.user = r.Header.Get("X-SLURM-USER-NAME")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(response))
	}))
	t.Cleanup(srv.Close)

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("jwt-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(Options{BaseURL: srv.URL, User: "root", TokenFile: tokenFile})
	if err != nil {
		t.Fatal(err)
	}
	return client, captured
}

const emptyOK = `{"jobs": [], "errors": [], "warnings": []}`

func TestAuthHeadersAndTokenTrimming(t *testing.T) {
	c, captured := newTestServer(t, 200, emptyOK)
	if err := c.ListJobs(context.Background(), func(j Job) error { return nil }); err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if captured.token != "jwt-secret" {
		t.Errorf("token = %q, want trimmed jwt-secret", captured.token)
	}
	if captured.user != "root" {
		t.Errorf("user header = %q, want root", captured.user)
	}
}

// TestEmptyUserOmitsUserNameHeader is the L2 fix (scale validation):
// with slurmUser="", the client must OMIT X-SLURM-USER-NAME so slurmrestd
// falls back to the user the JWT was minted for. Sending a name slurmrestd
// doesn't know (the old "k8s-bridge" default) made it reject EVERY job update
// (comment AND release) with 422 "Invalid user id".
func TestEmptyUserOmitsUserNameHeader(t *testing.T) {
	headerPresent := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, headerPresent = r.Header["X-Slurm-User-Name"]
		w.WriteHeader(200)
		_, _ = w.Write([]byte(emptyOK))
	}))
	t.Cleanup(srv.Close)
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("jwt\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := NewClient(Options{BaseURL: srv.URL, User: "", TokenFile: tokenFile})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.SetJobComment(context.Background(), 1, "hi"); err != nil {
		t.Fatalf("SetJobComment: %v", err)
	}
	if headerPresent {
		t.Error("X-SLURM-USER-NAME must be absent when slurmUser is empty (else 422 Invalid user id)")
	}
}

func TestWarningsAreHardErrors(t *testing.T) {
	// Live finding: slurmrestd answers 200 OK and reports "field ignored"
	// only inside warnings — treating that as success caused silent no-ops.
	c, _ := newTestServer(t, 200,
		`{"errors": [], "warnings": [{"description": "Ignoring unknown field"}]}`)
	err := c.ReleaseJob(context.Background(), 5)
	if err == nil || !strings.Contains(err.Error(), "warnings") {
		t.Errorf("expected warnings to fail hard, got %v", err)
	}
}

// TestWarningsOnGETAreBenign is the TESTING.md convention #1 regression the
// audit flagged as missing: live finding from experiment 05 was that an
// empty job queue answers GET /jobs with 200 OK plus a warnings-only
// envelope ("Zero jobs to dump") — this must succeed and decode to an empty
// job list, NOT be treated as the same silent-no-op failure warnings signal
// for mutating (POST) requests.
func TestWarningsOnGETAreBenign(t *testing.T) {
	c, _ := newTestServer(t, 200,
		`{"jobs": [], "errors": [], "warnings": [{"description": "Zero jobs to dump"}]}`)
	var jobs []Job
	err := c.ListJobs(context.Background(), func(j Job) error { jobs = append(jobs, j); return nil })
	if err != nil {
		t.Fatalf("ListJobs with warnings-only envelope: %v", err)
	}
	if len(jobs) != 0 {
		t.Errorf("jobs = %v, want empty", jobs)
	}
}

func TestErrorsArrayFailsHard(t *testing.T) {
	c, _ := newTestServer(t, 200, `{"errors": [{"description": "boom"}], "warnings": []}`)
	if err := c.ReleaseJob(context.Background(), 5); err == nil {
		t.Error("expected error for non-empty errors array")
	}
}

// TestReleaseJobSendsBareJobDescMsg is a live finding (envelope
// discipline): the update endpoint takes a BARE job_desc_msg — a {"job": ...}
// wrapper is silently ignored by slurmrestd.
//
// It also pins that ReleaseJob carries ONLY "hold". P2 (perf fix)
// once merged the constraints update into this same body to save a POST; live
// testing showed slurmrestd may apply "hold": false without applying
// "constraints" from that same body, releasing the job before it is pinned to
// its dynamic nodes (runaway job). The calls are deliberately separate now —
// a "constraints" key reappearing here is that regression.
func TestReleaseJobSendsBareJobDescMsg(t *testing.T) {
	c, captured := newTestServer(t, 200, emptyOK)
	if err := c.ReleaseJob(context.Background(), 42); err != nil {
		t.Fatalf("ReleaseJob: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(captured.body), &payload); err != nil {
		t.Fatal(err)
	}
	if _, wrapped := payload["job"]; wrapped {
		t.Errorf("payload must not be wrapped in {\"job\":...}: %s", captured.body)
	}
	if hold, ok := payload["hold"].(bool); !ok || hold {
		t.Errorf("payload hold = %v, want false", payload["hold"])
	}
	if _, present := payload["constraints"]; present {
		t.Errorf("ReleaseJob must NOT carry constraints (runaway-job regression): %s", captured.body)
	}
	if len(payload) != 1 {
		t.Errorf("payload must carry only \"hold\", got %s", captured.body)
	}
	if captured.path != "/slurm/v0.0.44/job/42" {
		t.Errorf("path = %q", captured.path)
	}
	if captured.method != http.MethodPost {
		t.Errorf("method = %q, want POST", captured.method)
	}
}

// TestSetJobFeaturesSendsBareConstraintsBody pins the other half of the split:
// the node-locking update travels alone, bare, and under the key "constraints"
// — NOT "features", which is what the v0.0.44 job_desc_msg schema calls it.
// Sending "features" would be accepted with a 200 + warning and ignored, so the
// job would be released unpinned.
func TestSetJobFeaturesSendsBareConstraintsBody(t *testing.T) {
	c, captured := newTestServer(t, 200, emptyOK)
	if err := c.SetJobFeatures(context.Background(), 42, "nodes-for-42"); err != nil {
		t.Fatalf("SetJobFeatures: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(captured.body), &payload); err != nil {
		t.Fatal(err)
	}
	if _, wrapped := payload["job"]; wrapped {
		t.Errorf("payload must not be wrapped in {\"job\":...}: %s", captured.body)
	}
	if constraints, ok := payload["constraints"].(string); !ok || constraints != "nodes-for-42" {
		t.Errorf("payload constraints = %v, want %q", payload["constraints"], "nodes-for-42")
	}
	if _, present := payload["features"]; present {
		t.Errorf("field must be \"constraints\", not \"features\": %s", captured.body)
	}
	if _, present := payload["hold"]; present {
		t.Errorf("SetJobFeatures must NOT lift the hold: %s", captured.body)
	}
	if len(payload) != 1 {
		t.Errorf("payload must carry only \"constraints\", got %s", captured.body)
	}
	if captured.path != "/slurm/v0.0.44/job/42" {
		t.Errorf("path = %q", captured.path)
	}
	if captured.method != http.MethodPost {
		t.Errorf("method = %q, want POST", captured.method)
	}
}

// TestSetJobCommentSendsBareSingleFieldBody is the L2 regression (live
// deploy: slurmrestd v0.0.44 answered the comment-update POST with 422
// Unprocessable Entity). ReleaseJob's envelope discipline is the
// proven-working reference for this same /job/{id} endpoint: a bare
// job_desc_msg carrying ONLY the field(s) actually being changed, never
// wrapped in {"job": ...} and never padded with unrelated/unset fields.
// This pins that SetJobComment sends exactly {"comment": "..."} — nothing
// more, nothing wrapped — matching that discipline exactly. (Not verified
// against a live slurmrestd; see internal/slurm client doc and the L2 backlog
// entry — this test can only confirm payload-shape consistency, not that the
// 422 is actually gone.)
func TestSetJobCommentSendsBareSingleFieldBody(t *testing.T) {
	c, captured := newTestServer(t, 200, emptyOK)
	if err := c.SetJobComment(context.Background(), 42, "wm: capacity admitted, provisioning nodes"); err != nil {
		t.Fatalf("SetJobComment: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(captured.body), &payload); err != nil {
		t.Fatal(err)
	}
	if _, wrapped := payload["job"]; wrapped {
		t.Errorf("payload must not be wrapped in {\"job\":...}: %s", captured.body)
	}
	if len(payload) != 1 {
		t.Errorf("payload must carry ONLY the comment field, got %d keys: %s", len(payload), captured.body)
	}
	if comment, ok := payload["comment"].(string); !ok || comment != "wm: capacity admitted, provisioning nodes" {
		t.Errorf("payload comment = %v, want %q", payload["comment"], "wm: capacity admitted, provisioning nodes")
	}
	if captured.path != "/slurm/v0.0.44/job/42" {
		t.Errorf("path = %q, want /slurm/v0.0.44/job/42", captured.path)
	}
	if captured.method != http.MethodPost {
		t.Errorf("method = %q, want POST", captured.method)
	}
}

// TestSetJobCommentSanitizesControlCharacters is the L2 fix (e2e iteration 2):
// a Kueue condition message reaching the comment can carry newlines/tabs/
// control chars that a free-text Slurm field rejects with 422. The sent body
// must have those collapsed to single spaces and be trimmed.
func TestSetJobCommentSanitizesControlCharacters(t *testing.T) {
	c, captured := newTestServer(t, 200, emptyOK)
	raw := "wm: waiting for quota:\n\tcouldn't assign flavors  to  pod set\r\n(0 > 0) "
	if err := c.SetJobComment(context.Background(), 7, raw); err != nil {
		t.Fatalf("SetJobComment: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(captured.body), &payload); err != nil {
		t.Fatal(err)
	}
	got, _ := payload["comment"].(string)
	want := "wm: waiting for quota: couldn't assign flavors to pod set (0 > 0)"
	if got != want {
		t.Errorf("sanitized comment = %q, want %q", got, want)
	}
	if strings.ContainsAny(got, "\n\r\t") {
		t.Errorf("comment still contains control characters: %q", got)
	}
}

// TestSetJobAdminCommentSendsBareSingleFieldBody mirrors
// TestSetJobCommentSendsBareSingleFieldBody for the ADR-0009 priority-ack
// write (SetJobAdminComment), the other comment-shaped mutation on this same
// endpoint. Same L2 caveat: payload-shape consistency only, not live-verified.
func TestSetJobAdminCommentSendsBareSingleFieldBody(t *testing.T) {
	c, captured := newTestServer(t, 200, emptyOK)
	if err := c.SetJobAdminComment(context.Background(), 42, "wm:prio-applied=500"); err != nil {
		t.Fatalf("SetJobAdminComment: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(captured.body), &payload); err != nil {
		t.Fatal(err)
	}
	if _, wrapped := payload["job"]; wrapped {
		t.Errorf("payload must not be wrapped in {\"job\":...}: %s", captured.body)
	}
	if len(payload) != 1 {
		t.Errorf("payload must carry ONLY the admin_comment field, got %d keys: %s", len(payload), captured.body)
	}
	if comment, ok := payload["admin_comment"].(string); !ok || comment != "wm:prio-applied=500" {
		t.Errorf("payload admin_comment = %v, want %q", payload["admin_comment"], "wm:prio-applied=500")
	}
	if captured.path != "/slurm/v0.0.44/job/42" {
		t.Errorf("path = %q, want /slurm/v0.0.44/job/42", captured.path)
	}
	if captured.method != http.MethodPost {
		t.Errorf("method = %q, want POST", captured.method)
	}
}

// TestCancelJobUsesDeleteMethod is the D1 regression: cancelling a Slurm job
// whose JobSet died must hit the same /job/{id} resource as ReleaseJob, but
// with DELETE — slurmrestd's job-cancellation verb (equivalent to
// `scancel <id>`), not a POST-with-a-cancel-field.
func TestCancelJobUsesDeleteMethod(t *testing.T) {
	c, captured := newTestServer(t, 200, emptyOK)
	if err := c.CancelJob(context.Background(), 42); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	if captured.method != http.MethodDelete {
		t.Errorf("method = %q, want DELETE", captured.method)
	}
	if captured.path != "/slurm/v0.0.44/job/42" {
		t.Errorf("path = %q, want /slurm/v0.0.44/job/42", captured.path)
	}
}

func TestGetJobTreats404AsNotFound(t *testing.T) {
	c, _ := newTestServer(t, 404, `{"errors": [{"description": "unknown job"}]}`)
	_, found, err := c.GetJob(context.Background(), 999)
	if err != nil || found {
		t.Errorf("GetJob on 404: found=%v err=%v, want found=false err=nil", found, err)
	}
}

// TestGetJobDoesNotMisreadTransientErrorAsNotFound is the audit D3
// regression: matching on strings.Contains(err.Error(), "404") in the job's
// URL/body misclassified a genuine transient failure (e.g. 500, 503) as
// not-found whenever the job ID itself contained "404" (e.g. job 404, 1404,
// 4040). A typed APIError with the real HTTP status must prevent that.
func TestGetJobDoesNotMisreadTransientErrorAsNotFound(t *testing.T) {
	// Job ID 1404 embeds "404" in the request path; the server answers 503
	// (a transient failure, not a not-found).
	c, _ := newTestServer(t, 503, `{"errors": [{"description": "slurmctld not responding"}]}`)
	job, found, err := c.GetJob(context.Background(), 1404)
	if err == nil {
		t.Fatal("expected a transient error to be reported, got nil")
	}
	if found {
		t.Error("found = true, want false on error")
	}
	if job != nil {
		t.Error("job should be nil on error")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError, got %T: %v", err, err)
	}
	if apiErr.StatusCode != 503 {
		t.Errorf("APIError.StatusCode = %d, want 503", apiErr.StatusCode)
	}
}

// TestGetJobStatusCodeNotBodyDeterminesNotFound pins that only the HTTP
// status decides not-found, even when the response body text mentions "404"
// for an unrelated reason (audit D3).
func TestGetJobStatusCodeNotBodyDeterminesNotFound(t *testing.T) {
	c, _ := newTestServer(t, 500, `{"errors": [{"description": "internal error 404 while paging job table"}]}`)
	_, found, err := c.GetJob(context.Background(), 42)
	if err == nil {
		t.Fatal("expected error for 500 response even though body mentions 404")
	}
	if found {
		t.Error("found = true, want false on transient error")
	}
}

func TestNonJSON200BodyIsHardError(t *testing.T) {
	// audit minor: a 200 OK that isn't the expected envelope shape (e.g. a
	// misconfigured reverse proxy returning an HTML error page) must fail
	// loudly instead of silently bypassing the errors/warnings check.
	c, _ := newTestServer(t, 200, `<html>not json</html>`)
	if err := c.ReleaseJob(context.Background(), 5); err == nil {
		t.Error("expected non-JSON 200 body to be a hard error")
	}
}

func TestListJobsParsesRealShape(t *testing.T) {
	// Field shapes captured from a live slurmrestd v0.0.44 response.
	c, _ := newTestServer(t, 200, `{"jobs": [{
		"job_id": 3, "name": "wrap", "partition": "mixing",
		"job_state": ["PENDING"], "state_reason": "JobHeldUser", "hold": true,
		"priority": {"set": true, "infinite": false, "number": 0},
		"tasks": {"set": true, "infinite": false, "number": 2},
		"cpus_per_task": {"set": true, "infinite": false, "number": 1},
		"memory_per_cpu": {"set": true, "infinite": false, "number": 2048},
		"tres_per_node": "gres/gpu:1"
	}], "errors": [], "warnings": []}`)
	var jobs []Job
	err := c.ListJobs(context.Background(), func(j Job) error { jobs = append(jobs, j); return nil })
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	j := jobs[0]
	if !j.IsPending() || !j.IsHeld() {
		t.Errorf("job should be pending+held: %+v", j)
	}
	if j.NTasks() != 2 || j.CPUs() != 1 || j.MemPerCPUMB() != 2048 || j.GPUsPerNode() != 1 {
		t.Errorf("parsed resources wrong: tasks=%d cpus=%d mem=%d gpus=%d",
			j.NTasks(), j.CPUs(), j.MemPerCPUMB(), j.GPUsPerNode())
	}
}

// -----------------------------------------------------------------------------
// DIAGNOSTIC TODO (follow-up from PR #12 / TC-C1): why did IsHeld() return false
// for genuinely held jobs on a real slurmrestd, so the bridge stopped
// admitting them? (Reverting the loosened admission gate re-exposes this.)
//
// TestListJobsParsesRealShape above is the HAPPY path: hold=true AND priority=0
// AND state_reason="JobHeldUser" are ALL present, so IsHeld() passes trivially.
// The live failure means the real held-job payload differs on at least one of
// those signals — and/or the fetch itself failed ("GET /slurm/v0.0.44/job/<id>
// -> Invalid or unknown URL path", the scratch removed in this PR), which would
// point at an API-version mismatch, not at IsHeld() at all.
//
// To pin the real cause, capture data FROM a real cluster and fill in the
// two skipped tests below, then delete their t.Skip:
//
//  1. Hold a job and dump exactly what slurmrestd returns for it:
//       scontrol hold <id>
//       curl -s -H "X-SLURM-USER-TOKEN: $TOKEN" \
//            "$SLURMRESTD/slurm/v0.0.44/job/<id>" \
//         | jq '.jobs[0] | {hold, priority, state_reason, job_state}'
//     Paste that object into TestListJobsParsesRealHeldJob.
//  2. Confirm the API version the server actually serves (const apiPrefix is
//     "/slurm/v0.0.44"): a 404 / "Invalid or unknown URL path" there means the
//     fix is the client path/version, NOT IsHeld().
//       curl -s "$SLURMRESTD/openapi/v3" | jq '.info.version'
//  3. If hold=false in the capture, record which signal DOES mark the hold
//     (priority==0 + a reason prefix?) and extend both IsHeld() and the
//     representation table below to match it.
// -----------------------------------------------------------------------------

// TestListJobsParsesRealHeldJob pins the held-job payload captured on
// 2026-07-11 from a REAL slurmctld/slurmrestd 26.05.1 (the Slinky
// 26.05-ubuntu26.04 containers, during the real-Slurm e2e validation): two
// jobs, one submitted with `sbatch --hold` (JobHeldUser) and one held with
// `scontrol hold` (JobHeldAdmin — the same mechanism the live lua JobSubmit
// plugin uses). Both representations carried ALL THREE hold signals —
// hold=true, priority set-and-zero, JobHeld* reason — so IsHeld() must be
// true for both. Field values are verbatim from the capture, trimmed to the
// fields the Job struct maps (the streaming decoder ignores the rest by
// design).
//
// Provenance caveat (TC-B7): this capture is from a stock 26.05.1, NOT from
// a real cluster where the original TC-C1 IsHeld() failure occurred.
// It pins the parser against real 26.05 output. The real-cluster
// confirmation ARRIVED (suite E/F session, finding 2): their native slurmrestd
// emits the same {hold:true, priority 0, JobHeldUser} shape and IsHeld()
// matched it live — TC-B7 is closed from both sides. If a future Slurm
// version emits a different representation, add it here and to
// TestIsHeldAcrossHoldRepresentations.
func TestListJobsParsesRealHeldJob(t *testing.T) {
	// Merge resolution (suite-f-fixes x main, 2026-07-12): keep the REAL
	// 26.05.1 capture that replaced the skipped placeholder on main, adapted
	// to the streaming ListJobs signature this branch introduces.
	const capturedHeldJobs = `{"jobs": [
		{"job_id": 1, "name": "wrap", "partition": "mixing",
		 "job_state": ["PENDING"], "hold": true, "state_reason": "JobHeldUser",
		 "priority": {"set": true, "infinite": false, "number": 0},
		 "user_name": "bridge",
		 "submit_time": {"set": true, "infinite": false, "number": 1783798904},
		 "node_count": {"set": true, "infinite": false, "number": 1},
		 "features": ""},
		{"job_id": 2, "name": "wrap", "partition": "mixing",
		 "job_state": ["PENDING"], "hold": true, "state_reason": "JobHeldAdmin",
		 "priority": {"set": true, "infinite": false, "number": 0},
		 "user_name": "bridge",
		 "submit_time": {"set": true, "infinite": false, "number": 1783798950}}
	], "errors": [], "warnings": []}`
	c, _ := newTestServer(t, 200, capturedHeldJobs)
	var jobs []Job
	err := c.ListJobs(context.Background(), func(j Job) error { jobs = append(jobs, j); return nil })
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("ListJobs returned %d jobs, want 2", len(jobs))
	}
	for _, j := range jobs {
		if !j.IsPending() || !j.IsHeld() {
			t.Errorf("IsHeld()/IsPending() false for a genuinely held 26.05.1 job (reason %q): %+v", j.StateReason, j)
		}
	}
	if !jobs[0].SubmitTime.Set || jobs[0].SubmitTime.Number != 1783798904 {
		t.Errorf("submit_time not mapped from the real payload (the A3 identity annotation depends on it): %+v", jobs[0].SubmitTime)
	}
}

// TestIsHeldAcrossHoldRepresentations documents the hold signals IsHeld() must
// tolerate across slurmrestd versions. The JobHeldUser/JobHeldAdmin cases are
// confirmed against a real 26.05.1 (2026-07-11 capture, see
// TestListJobsParsesRealHeldJob): there, both arrive with hold=true AND
// priority set-and-zero, so each signal is redundant — but IsHeld() must
// keep accepting each signal ALONE, because the original TC-C1 live failure
// suggests at least one environment where they do not all co-occur. The
// real-cluster validation (2026-07-12, suite E/F finding 2) matched the
// same three-signal shape, so no fourth case was needed; the original TC-C1
// live failure is therefore attributed to the since-fixed fetch path, not a
// representation gap. Add a case here if a future version diverges.
func TestIsHeldAcrossHoldRepresentations(t *testing.T) {
	cases := []struct {
		name string
		job  Job
		want bool
	}{
		{"hold flag true", Job{JobState: []string{"PENDING"}, Hold: true}, true},
		{"priority 0 + JobHeldUser", Job{JobState: []string{"PENDING"}, StateReason: "JobHeldUser", Priority: Uint64NoVal{Set: true, Number: 0}}, true},
		{"priority 0 + JobHeldAdmin", Job{JobState: []string{"PENDING"}, StateReason: "JobHeldAdmin", Priority: Uint64NoVal{Set: true, Number: 0}}, true},
		{"all three signals together (real 26.05.1 shape)", Job{JobState: []string{"PENDING"}, Hold: true, StateReason: "JobHeldAdmin", Priority: Uint64NoVal{Set: true, Number: 0}}, true},
		{"not held: pending on Priority with nonzero priority", Job{JobState: []string{"PENDING"}, StateReason: "Priority", Priority: Uint64NoVal{Set: true, Number: 4294901759}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.job.IsHeld(); got != tc.want {
				t.Errorf("IsHeld()=%v, want %v for %+v", got, tc.want, tc.job)
			}
		})
	}
}

// TestListJobsStreamingHandlesLargeArray is the P4 regression: ListJobs must
// correctly decode a job count well beyond what any single test previously
// exercised, confirming the streaming json.Decoder walk (jobs array decoded
// element-by-element, not via one io.ReadAll+json.Unmarshal of the whole
// body) doesn't drop, duplicate, or corrupt entries at scale. 4000 mirrors
// the backlog order of magnitude that produced multi-MB
// responses and 23MB heap peaks with the old buffered approach.
func TestListJobsStreamingHandlesLargeArray(t *testing.T) {
	const n = 4000
	var b strings.Builder
	b.WriteString(`{"jobs": [`)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`{"job_id": `)
		b.WriteString(strconv.Itoa(i))
		b.WriteString(`, "partition": "mixing", "job_state": ["PENDING"]}`)
	}
	b.WriteString(`], "errors": [], "warnings": []}`)

	c, _ := newTestServer(t, 200, b.String())
	var jobs []Job
	err := c.ListJobs(context.Background(), func(j Job) error { jobs = append(jobs, j); return nil })
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(jobs) != n {
		t.Fatalf("len(jobs) = %d, want %d", len(jobs), n)
	}
	if jobs[0].JobID != 0 || jobs[n-1].JobID != uint64(n-1) {
		t.Errorf("job IDs at boundaries = %d, %d; want 0, %d", jobs[0].JobID, jobs[n-1].JobID, n-1)
	}
}

// TestListJobsStreamingHandlesFieldOrder confirms the manual top-level-object
// walk in listJobsStreaming does not assume "jobs" comes before
// "errors"/"warnings" in the response — real slurmrestd field order is not a
// documented contract this client should depend on.
func TestListJobsStreamingHandlesFieldOrder(t *testing.T) {
	c, _ := newTestServer(t, 200, `{"warnings": [], "errors": [], "jobs": [{"job_id": 7, "partition": "mixing", "job_state": ["PENDING"]}]}`)
	var jobs []Job
	err := c.ListJobs(context.Background(), func(j Job) error { jobs = append(jobs, j); return nil })
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].JobID != 7 {
		t.Errorf("jobs = %+v, want one job with ID 7", jobs)
	}
}

// TestListJobsStreamingSkipsUnknownTopLevelFields confirms a field this
// client doesn't care about (e.g. "last_backfill", present in some
// slurmrestd parsers) is skipped without breaking the jobs/errors/warnings
// walk.
func TestListJobsStreamingSkipsUnknownTopLevelFields(t *testing.T) {
	c, _ := newTestServer(t, 200, `{"last_backfill": 12345, "jobs": [{"job_id": 1, "partition": "mixing", "job_state": ["PENDING"]}], "meta": {"plugin": {"name": "x"}}, "errors": [], "warnings": []}`)
	var jobs []Job
	err := c.ListJobs(context.Background(), func(j Job) error { jobs = append(jobs, j); return nil })
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].JobID != 1 {
		t.Errorf("jobs = %+v, want one job with ID 1", jobs)
	}
}

// TestListJobsStreamingEmptyObjectIsHardError confirms a 200 OK body that
// decodes as valid JSON but carries none of jobs/errors/warnings (e.g. `{}`
// from a misconfigured proxy) is rejected rather than silently treated as an
// empty job list — parity with doHTTP's non-envelope-shape hard error.
func TestListJobsStreamingEmptyObjectIsHardError(t *testing.T) {
	c, _ := newTestServer(t, 200, `{}`)
	if err := c.ListJobs(context.Background(), func(Job) error { return nil }); err == nil {
		t.Error("expected an empty object with no jobs/errors/warnings to be a hard error")
	}
}

func TestTerminalStates(t *testing.T) {
	for _, s := range []string{"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "PREEMPTED"} {
		j := Job{JobState: []string{s}}
		if !j.IsTerminal() {
			t.Errorf("%s should be terminal", s)
		}
	}
	for _, s := range []string{"PENDING", "RUNNING", "SUSPENDED"} {
		j := Job{JobState: []string{s}}
		if j.IsTerminal() {
			t.Errorf("%s should NOT be terminal", s)
		}
	}
}

// TestOnRequestReportsMethodAndStatusOnSuccess pins the seam main.go uses to
// wire slurmrestd call outcomes into the k8s_bridge_slurm_api_requests_total
// metric (audit AUD2) without importing prometheus into this package.
func TestOnRequestReportsMethodAndStatusOnSuccess(t *testing.T) {
	c, _ := newTestServer(t, 200, emptyOK)
	var gotMethod string
	var gotCode int
	calls := 0
	c.OnRequest = func(method string, code int) {
		calls++
		gotMethod, gotCode = method, code
	}
	if err := c.ListJobs(context.Background(), func(Job) error { return nil }); err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if calls != 1 {
		t.Fatalf("OnRequest called %d times, want 1", calls)
	}
	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotCode != 200 {
		t.Errorf("code = %d, want 200", gotCode)
	}
}

// TestOnRequestReportsErrorStatusCode confirms a non-2xx response still
// reports its real status code through OnRequest — the metric should be
// able to distinguish a 404 from a 503, not just success/failure.
func TestOnRequestReportsErrorStatusCode(t *testing.T) {
	c, _ := newTestServer(t, 503, `{"errors": [{"description": "slurmctld not responding"}]}`)
	var gotCode int
	c.OnRequest = func(_ string, code int) { gotCode = code }
	if _, _, err := c.GetJob(context.Background(), 1); err == nil {
		t.Fatal("expected an error for a 503 response")
	}
	if gotCode != 503 {
		t.Errorf("code = %d, want 503", gotCode)
	}
}

// TestOnRequestReportsZeroCodeOnTransportFailure covers a request that never
// gets an HTTP response at all (DNS/connection failure) — OnRequest must
// still fire, with code 0 rather than being skipped, so a fully-down
// slurmrestd is visible in the request-count metric too.
func TestOnRequestReportsZeroCodeOnTransportFailure(t *testing.T) {
	c, err := NewClient(Options{BaseURL: "http://127.0.0.1:1"}) // nothing listens here
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	calls := 0
	gotCode := -1
	c.OnRequest = func(_ string, code int) {
		calls++
		gotCode = code
	}
	if err := c.ListJobs(context.Background(), func(Job) error { return nil }); err == nil {
		t.Fatal("expected a connection error")
	}
	if calls != 1 {
		t.Fatalf("OnRequest called %d times, want 1", calls)
	}
	if gotCode != 0 {
		t.Errorf("code = %d, want 0 for a transport-level failure", gotCode)
	}
}

// TestNilOnRequestIsSafe confirms the zero-value Client (OnRequest unset)
// still works — the callback is optional.
func TestNilOnRequestIsSafe(t *testing.T) {
	c, _ := newTestServer(t, 200, emptyOK)
	if err := c.ListJobs(context.Background(), func(Job) error { return nil }); err != nil {
		t.Fatalf("ListJobs with nil OnRequest: %v", err)
	}
}

// TestRateLimiterConfiguration pins the A1 limiter wiring in NewClient:
// rate 0 (or unset) means NO limiter at all — the historic unlimited
// behavior — while a positive rate builds a token bucket at exactly that
// rate with the documented burst of max(1, 2x rate).
func TestRateLimiterConfiguration(t *testing.T) {
	unlimited, err := NewClient(Options{BaseURL: "http://x"})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if unlimited.limiter != nil {
		t.Error("RequestsPerSecond 0 must leave the limiter nil (unlimited, previous behavior)")
	}

	limited, err := NewClient(Options{BaseURL: "http://x", RequestsPerSecond: 100})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if limited.limiter == nil {
		t.Fatal("RequestsPerSecond 100 must build a limiter")
	}
	if got := float64(limited.limiter.Limit()); got != 100 {
		t.Errorf("limiter rate = %v, want 100", got)
	}
	if got := limited.limiter.Burst(); got != 200 {
		t.Errorf("limiter burst = %d, want 200 (2x rate)", got)
	}

	// Sub-1-rps rates must keep a functional burst of 1 (a burst of 0 would
	// make every Wait fail forever).
	slow, err := NewClient(Options{BaseURL: "http://x", RequestsPerSecond: 0.2})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if got := slow.limiter.Burst(); got != 1 {
		t.Errorf("limiter burst = %d, want 1 (floor for fractional rates)", got)
	}
}

// TestRateLimiterGatesRequests proves the limiter Wait actually stands
// between the caller and the wire, and that it respects the request context
// (A1) — deterministically, with no sleeps: at 0.001 rps with burst 1, the
// first request consumes the only token; the second request's deadline is
// centuries short of the next token, so rate.Limiter.Wait fails IMMEDIATELY
// ("would exceed context deadline") and the request must never reach the
// server.
func TestRateLimiterGatesRequests(t *testing.T) {
	var served int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		served++
		w.WriteHeader(200)
		_, _ = w.Write([]byte(emptyOK))
	}))
	t.Cleanup(srv.Close)

	c, err := NewClient(Options{BaseURL: srv.URL, RequestsPerSecond: 0.001})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	// First request: the burst token admits it without waiting.
	if lerr := c.ListJobs(context.Background(), func(Job) error { return nil }); lerr != nil {
		t.Fatalf("first request must pass on the burst token: %v", lerr)
	}
	if served != 1 {
		t.Fatalf("server saw %d requests, want 1", served)
	}

	// Second request: token bucket empty, next token ~1000s away, deadline
	// 10ms — Wait must abort fast and the server must NOT see the request.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if lerr := c.ListJobs(ctx, func(Job) error { return nil }); lerr == nil {
		t.Fatal("second request must fail: the rate limiter cannot admit it within the context deadline")
	}
	if served != 1 {
		t.Errorf("server saw %d requests, want still 1 (the limiter must gate the request BEFORE it is sent)", served)
	}
}

// TestRateLimiterCoversMutatingRequests pins that the limiter guards ALL
// request paths, not just the streaming ListJobs one: do()-based mutations
// pass through the same newRequest choke point.
func TestRateLimiterCoversMutatingRequests(t *testing.T) {
	var served int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		served++
		w.WriteHeader(200)
		_, _ = w.Write([]byte(emptyOK))
	}))
	t.Cleanup(srv.Close)

	c, err := NewClient(Options{BaseURL: srv.URL, RequestsPerSecond: 0.001})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if rerr := c.ReleaseJob(context.Background(), 1); rerr != nil {
		t.Fatalf("first request must pass on the burst token: %v", rerr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if rerr := c.ReleaseJob(ctx, 2); rerr == nil {
		t.Fatal("second mutating request must be gated by the same limiter")
	}
	if served != 1 {
		t.Errorf("server saw %d requests, want 1", served)
	}
}

// TestOnRequestDurationReportsPerRequest is the A10 seam test: every request
// that actually went out reports exactly one wall-clock duration alongside
// the method, on both the do() path (mutations) and the streaming ListJobs
// path. main.go wires this to k8s_bridge_slurm_request_duration_seconds.
func TestOnRequestDurationReportsPerRequest(t *testing.T) {
	c, _ := newTestServer(t, 200, emptyOK)
	type obs struct {
		method string
		d      time.Duration
	}
	var got []obs
	c.OnRequestDuration = func(method string, d time.Duration) {
		got = append(got, obs{method, d})
	}

	if err := c.ListJobs(context.Background(), func(Job) error { return nil }); err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if err := c.ReleaseJob(context.Background(), 7); err != nil {
		t.Fatalf("ReleaseJob: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("OnRequestDuration fired %d times, want 2 (one per request)", len(got))
	}
	if got[0].method != http.MethodGet || got[1].method != http.MethodPost {
		t.Errorf("methods = %s, %s; want GET then POST", got[0].method, got[1].method)
	}
	for _, o := range got {
		if o.d <= 0 {
			t.Errorf("%s duration = %v, want > 0", o.method, o.d)
		}
	}
}

// TestOnRequestDurationSkippedWhenRequestNeverSent pins the negative half of
// the A10 contract: a request aborted BEFORE hitting the wire (here: the
// rate limiter refusing within the context deadline) must not observe a
// duration — it would record the bridge's own throttle as slurmrestd latency.
func TestOnRequestDurationSkippedWhenRequestNeverSent(t *testing.T) {
	c, _ := newTestServer(t, 200, emptyOK)
	c.limiter = rate.NewLimiter(rate.Limit(0.001), 1)
	fired := 0
	c.OnRequestDuration = func(string, time.Duration) { fired++ }

	if err := c.ReleaseJob(context.Background(), 1); err != nil { // consumes the burst token
		t.Fatalf("first request: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := c.ReleaseJob(ctx, 2); err == nil {
		t.Fatal("expected the limiter to abort the second request")
	}
	if fired != 1 {
		t.Errorf("OnRequestDuration fired %d times, want 1 (never for the unsent request)", fired)
	}
}

// TestNewClientRequestTimeout covers the configurable per-request timeout
// (config slurmRequestTimeout, suite-E scale run): zero keeps the historic
// 30s default, an explicit value replaces it.
func TestNewClientRequestTimeout(t *testing.T) {
	def, err := NewClient(Options{BaseURL: "http://x", TokenFile: "/dev/null"})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if def.http.Timeout != 30*time.Second {
		t.Errorf("default timeout = %v, want 30s", def.http.Timeout)
	}
	custom, err := NewClient(Options{BaseURL: "http://x", TokenFile: "/dev/null", RequestTimeout: 2 * time.Minute})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if custom.http.Timeout != 2*time.Minute {
		t.Errorf("custom timeout = %v, want 2m", custom.http.Timeout)
	}
}
