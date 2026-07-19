// Package slurm is a minimal slurmrestd client covering only what the bridge
// needs: list pending jobs, release a held job, inspect a job, delete a node.
//
// It targets the v0.0.44 data parser (Slurm 25.11, as deployed by the Slinky
// charts). Field shapes were written from the OpenAPI spec and MUST be
// validated against a live slurmrestd in experiment 02.
package slurm

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	// golang.org/x/time/rate is the one exception to this package's
	// stdlib-only stance (see the package doc note on prometheus): it is
	// already in the module graph via client-go, is maintained by the Go
	// team, and hand-rolling a token bucket correct under a cancelable
	// context is exactly the kind of subtle concurrency code a prototype
	// should not carry. The prometheus exclusion stands — metrics still
	// travel through the OnRequest/OnRequestDuration callbacks.
	"golang.org/x/time/rate"
)

// clampUint64ToInt32 converts an untrusted Slurm count to int32, saturating at
// the maximum instead of silently wrapping (gosec G115). A job asking for
// billions of tasks is nonsensical, but must not turn into a negative or
// truncated value that mis-sizes the JobSet.
func clampUint64ToInt32(v uint64) int32 {
	if v > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(v)
}

const apiPrefix = "/slurm/v0.0.44"

// APIError carries the HTTP status of a failed slurmrestd call so callers can
// distinguish a true "not found" from a transient failure that merely
// mentions the status code in its body (audit D3: matching on
// strings.Contains(err.Error(), "404") misfired for a transient error on job
// IDs like 404, 1404, 4040 — the job ID appears in the request URL/body — and
// caused cleanupFinishedJobs to delete a live job's JobSet).
type APIError struct {
	StatusCode int
	Method     string
	Path       string
	Body       string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("slurmrestd %s %s: %d %s: %s", e.Method, e.Path, e.StatusCode, http.StatusText(e.StatusCode), e.Body)
}

// Uint64NoVal mirrors slurmrestd's {set, infinite, number} wrapper used for
// optional numeric fields.
type Uint64NoVal struct {
	Set      bool   `json:"set"`
	Infinite bool   `json:"infinite"`
	Number   uint64 `json:"number"`
}

// Job is the subset of the slurmrestd job record the bridge cares about.
type Job struct {
	JobID        uint64      `json:"job_id"`
	Name         string      `json:"name"`
	Partition    string      `json:"partition"`
	JobState     []string    `json:"job_state"`
	StateReason  string      `json:"state_reason"`
	Priority     Uint64NoVal `json:"priority"`
	Tasks        Uint64NoVal `json:"tasks"`
	CPUsPerTask  Uint64NoVal `json:"cpus_per_task"`
	MemoryPerCPU Uint64NoVal `json:"memory_per_cpu"`
	TresPerNode  string      `json:"tres_per_node"`
	TresPerJob   string      `json:"tres_per_job,omitempty"`
	// RequiredSwitches mirrors sbatch --switches=N: the maximum number of
	// leaf switches the job's nodes may span (1 = full switch locality).
	// Live finding (experiment 05): plain number in v0.0.44, NOT a
	// {set,number} wrapper like the other optional fields.
	RequiredSwitches uint64 `json:"required_switches"`
	// NodeCount mirrors sbatch --nodes=N (minimum node count).
	NodeCount Uint64NoVal `json:"node_count"`
	// TasksPerNode mirrors --ntasks-per-node.
	TasksPerNode Uint64NoVal `json:"tasks_per_node"`
	// TimeLimit is the job's wall-clock limit in MINUTES; feeds the
	// JobSet's activeDeadlineSeconds resource-leak guard.
	TimeLimit Uint64NoVal `json:"time_limit"`
	// Comment is user/bridge-visible; the bridge writes admission status
	// here so squeue can show WHY a job is waiting.
	Comment string `json:"comment"`
	// AdminComment carries lua-plugin directives to the bridge (ADR-0009),
	// e.g. "wm:prio=500" written by slurm_job_modify.
	AdminComment string `json:"admin_comment"`
	// SubmitTime is the job's submission timestamp (unix seconds, standard
	// {set,infinite,number} wrapper in v0.0.44). The bridge stores it as an
	// annotation on the job's JobSet (translate.SlurmSubmitTimeAnnotation)
	// and uses it as the job's identity anchor: Slurm job IDs are REUSED
	// once slurmctld's counter wraps at MaxJobId, so the ID alone cannot
	// prove that an existing `slurm-job-<id>` JobSet belongs to THIS job
	// (A3 — see bridge.ensureJobSet). A job cannot be resubmitted at the
	// same ID with the same submit second without the previous incarnation
	// having left the system first, so ID+submit-time is unique in practice.
	SubmitTime Uint64NoVal `json:"submit_time"`
	Hold       bool        `json:"hold"`
	// Exclusive mirrors sbatch --exclusive (whole-node access). Its wire shape
	// varies across slurmrestd versions — a bare string in some, a flag array
	// like job_state in v0.0.44 — so it is unmarshalled tolerantly (see
	// exclusiveFlags) and NEEDS-LIVE-VALIDATION against a real slurmrestd for
	// the exact v0.0.44 representation. An unexpected shape degrades to "not
	// exclusive", preserving the historic behaviour of ignoring the flag rather
	// than failing the whole job decode.
	Exclusive exclusiveFlags `json:"exclusive"`
}

// exclusiveFlags holds the job's `exclusive` field. slurmrestd has exposed
// this as a bare string in some versions and as a string array (like
// job_state) in others; UnmarshalJSON therefore accepts string, []string, or
// null, and NEVER fails the enclosing job decode on an unexpected shape — an
// unparseable value degrades to empty (treated as "not exclusive"). This keeps
// one uncertain field from breaking the whole ListJobs stream the bridge polls
// every tick.
type exclusiveFlags []string

func (e *exclusiveFlags) UnmarshalJSON(b []byte) error {
	if len(b) == 0 || string(b) == "null" {
		return nil
	}
	var arr []string
	if err := json.Unmarshal(b, &arr); err == nil {
		*e = arr
		return nil
	}
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		*e = exclusiveFlags{s}
		return nil
	}
	// Unknown shape (object, number, ...): degrade to "not exclusive" rather
	// than propagate a decode error up through ListJobs.
	return nil
}

// IsExclusive reports whether the job requested exclusive node access
// (sbatch --exclusive). Any exclusivity token other than "false" counts —
// Slurm's flavours ("true", "user", "mcs", "topo") all mean the job does not
// share its nodes with other jobs.
func (j *Job) IsExclusive() bool {
	for _, f := range j.Exclusive {
		switch strings.ToLower(strings.TrimSpace(f)) {
		case "true", "user", "mcs", "topo":
			return true
		}
	}
	return false
}

// WantsSwitchLocality reports whether the job asked for topology locality
// via --switches. The bridge maps it to a Kueue TAS "required" constraint;
// the switch COUNT beyond 1 is not representable in TAS (one domain per
// podset), so any positive value means "co-locate within one domain".
func (j *Job) WantsSwitchLocality() bool {
	return j.RequiredSwitches > 0
}

// IsPending reports whether the job is still queued.
func (j *Job) IsPending() bool { return slices.Contains(j.JobState, "PENDING") }

// IsHeld detects the Hold state the JobSubmit plugin applies. slurmrestd
// exposes holds as priority==0 with a "JobHeld*" reason; some versions also
// set the "hold" flag. Accept either signal.
func (j *Job) IsHeld() bool {
	if j.Hold {
		return true
	}
	return j.Priority.Set && j.Priority.Number == 0 &&
		strings.HasPrefix(j.StateReason, "JobHeld")
}

// IsTerminal reports whether the job finished in any way — the trigger for
// JobSet cleanup.
func (j *Job) IsTerminal() bool {
	for _, s := range j.JobState {
		switch s {
		case "COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL", "PREEMPTED", "BOOT_FAIL", "DEADLINE", "OUT_OF_MEMORY":
			return true
		}
	}
	return false
}

// NTasks returns the task count, defaulting to 1 like sbatch does.
func (j *Job) NTasks() int32 {
	if j.Tasks.Set && j.Tasks.Number > 0 {
		return clampUint64ToInt32(j.Tasks.Number)
	}
	return 1
}

// CPUs returns cpus-per-task, defaulting to 1 like sbatch does.
func (j *Job) CPUs() int64 {
	if j.CPUsPerTask.Set && j.CPUsPerTask.Number > 0 {
		return int64(j.CPUsPerTask.Number)
	}
	return 1
}

// MemPerCPUMB returns --mem-per-cpu in MB, defaulting to 1024 (the bridge's
// 1Gi-per-CPU heuristic) when the job did not specify memory.
func (j *Job) MemPerCPUMB() int64 {
	if j.MemoryPerCPU.Set && j.MemoryPerCPU.Number > 0 {
		return int64(j.MemoryPerCPU.Number)
	}
	return 1024
}

// GPUsPerNode parses the job's GRES request (tres_per_node, e.g.
// "gres/gpu:2", "gres/gpu=2", "gres/gpu:a100:2" or bare "gres/gpu").
// Returns 0 when the job requests no GPUs, INCLUDING an explicit
// "gres/gpu:0" (audit minor: a job that explicitly asked for zero GPUs must
func extractGPUCount(tresStr string) (int64, bool) {
	for _, tres := range strings.Split(tresStr, ",") {
		tres = strings.TrimSpace(tres)
		rest, ok := strings.CutPrefix(tres, "gres/gpu")
		if !ok || (rest != "" && rest[0] != ':' && rest[0] != '=') {
			continue
		}
		rest = strings.TrimPrefix(rest, "=")
		rest = strings.TrimPrefix(rest, ":")
		if rest == "" {
			// bare "gres/gpu" means 1
			return 1, true
		}
		parts := strings.Split(rest, ":")
		// last numeric segment is the count.
		for i := len(parts) - 1; i >= 0; i-- {
			if n, err := strconv.ParseInt(parts[i], 10, 64); err == nil {
				if n < 0 {
					return 0, true
				}
				return n, true
			}
		}
		return 0, true
	}
	return 0, false
}

// GPUsPerNode parses the job's GRES request (tres_per_node, e.g.
// "gres/gpu:2", "gres/gpu=2", "gres/gpu:a100:2" or bare "gres/gpu").
// Returns 0 when the job requests no GPUs, INCLUDING an explicit
// "gres/gpu:0" (audit minor: a job that explicitly asked for zero GPUs must
// not be translated as if it asked for one).
func (j *Job) GPUsPerNode() int64 {
	// First check tres_per_node
	if n, found := extractGPUCount(j.TresPerNode); found {
		return n
	}
	// Fallback to tres_per_job (what `sbatch --gpus=N` populates)
	if n, found := extractGPUCount(j.TresPerJob); found {
		nodes := int64(j.Nodes())
		if nodes <= 0 {
			nodes = 1
		}
		// Distribute job GPUs over requested nodes, rounding up to ensure capacity for at least the job-level spec
		return (n + nodes - 1) / nodes
	}
	return 0
}

type Client struct {
	baseURL   string
	user      string
	tokenFile string
	http      *http.Client
	// limiter, when non-nil, paces EVERY request to slurmrestd (A1). Scale
	// testing (suite E, 2026-07) showed slurmrestd — not the Kubernetes API
	// server, which has had an explicit client-go QPS/Burst ceiling since
	// audit P8b — is the fragile dependency: it shares slurmctld's lock and
	// has no server-side throttling of its own, so a bridge burst (parallel
	// creates, watch-nudge storms) can starve the very scheduler the bridge
	// exists to feed. Nil (rate 0 / unset) preserves the historic unlimited
	// behavior. The wait happens in newRequest, the single choke point both
	// request paths (doHTTP and listJobsStreaming) must pass through, so a
	// future request variant cannot accidentally bypass it.
	limiter *rate.Limiter
	// OnRequest, when set, is invoked after every request with the HTTP
	// method and the resulting status code (0 when the request never got a
	// response, e.g. a transport-level error). Optional so this package
	// stays free of prometheus (see the package doc) — main.go/bridge setup
	// wires this to the k8s_bridge_slurm_api_requests_total metric.
	OnRequest func(method string, code int)
	// OnRequestDuration, when set, is invoked after every request that was
	// actually SENT, with the HTTP method and the wall-clock time from
	// starting the request to finishing the response body (A10). Requests
	// that never left the process (rate-limiter abort, token-file read
	// failure, bad URL) do not report a duration — they would only pollute
	// the latency signal this exists to provide: how slow slurmrestd itself
	// is, the early-warning series for the suite-E overload mode. A separate
	// callback rather than a widened OnRequest keeps the two concerns (count
	// by outcome vs. latency by method) independently wireable and the
	// existing OnRequest contract unchanged.
	OnRequestDuration func(method string, duration time.Duration)
}

// Options configures the slurmrestd client's transport. BaseURL, User and
// TokenFile are the request identity; the TLS fields harden the connection
// carrying the bearer-equivalent Slurm token.
type Options struct {
	BaseURL   string
	User      string
	TokenFile string
	// CACertFile, when set, is a PEM bundle added to the roots used to verify
	// the slurmrestd certificate. Empty uses the system roots.
	CACertFile string
	// InsecureSkipVerify disables certificate verification (development only).
	InsecureSkipVerify bool
	// RequestTimeout bounds each request including reading the full response
	// body. Zero means the 30s default. The /jobs payload scales with total
	// queue depth (no server-side paging in v0.0.44), so large-backlog
	// deployments raise this via config slurmRequestTimeout.
	RequestTimeout time.Duration
	// RequestsPerSecond is the client-side rate limit toward slurmrestd
	// (config slurmRequestsPerSecond, A1). Zero or negative disables the
	// limiter entirely — the historic behavior. See Client.limiter for why
	// this exists (slurmrestd was the fragile dependency in scale testing).
	RequestsPerSecond float64
}

// NewClient builds a slurmrestd client. It returns an error only when a
// configured CA bundle cannot be read, so misconfiguration fails at startup
// rather than silently falling back to no verification.
func NewClient(o Options) (*Client, error) {
	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: o.InsecureSkipVerify, //nolint:gosec // opt-in dev escape hatch, gated by config
	}
	if o.CACertFile != "" {
		pem, err := os.ReadFile(o.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("reading slurm CA bundle %q: %w", o.CACertFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("slurm CA bundle %q contains no valid certificates", o.CACertFile)
		}
		tlsCfg.RootCAs = pool
	}
	timeout := o.RequestTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	var limiter *rate.Limiter
	if o.RequestsPerSecond > 0 {
		limiter = rate.NewLimiter(rate.Limit(o.RequestsPerSecond), burstFor(o.RequestsPerSecond))
	}
	return &Client{
		baseURL:   strings.TrimRight(o.BaseURL, "/"),
		user:      o.User,
		tokenFile: o.TokenFile,
		limiter:   limiter,
		http: &http.Client{
			Timeout:   timeout,
			Transport: &http.Transport{TLSClientConfig: tlsCfg},
		},
	}, nil
}

// burstFor derives the limiter's burst from the configured rate: 2x the
// steady rate, rounded down, floored at 1. The 2x headroom lets the tick
// loop's naturally bursty shape (a snapshot's worth of GetJob fallbacks, a
// batch of comment writes) proceed without queuing as long as the AVERAGE
// stays at the configured rate, while the floor of 1 keeps a sub-1-rps
// configuration functional (a burst of 0 would make every Wait fail).
func burstFor(rps float64) int {
	b := int(2 * rps)
	if b < 1 {
		return 1
	}
	return b
}

// do executes an authenticated request and reports its outcome via
// OnRequest and OnRequestDuration (if set) before returning. The JWT is
// re-read on every call so token rotation by the operator needs no bridge
// restart.
func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	code, dur, err := c.doHTTP(ctx, method, path, body, out)
	if c.OnRequest != nil {
		c.OnRequest(method, code)
	}
	// dur == 0 means the request never left the process (see OnRequestDuration
	// on why those must not be observed as slurmrestd latency).
	if c.OnRequestDuration != nil && dur > 0 {
		c.OnRequestDuration(method, dur)
	}
	return err
}

// newRequest builds an authenticated slurmrestd request. The JWT is re-read
// on every call so token rotation by the operator needs no bridge restart.
// Shared by doHTTP (buffered path) and listJobsStreaming (backlog P4).
//
// It is also where the A1 rate limiter waits: BOTH request paths must pass
// through here, so pacing at this choke point cannot be bypassed by a future
// third variant the way a per-call-site Wait could be forgotten. The wait
// respects ctx — a cancelled or short-deadline context makes the request
// fail fast instead of queueing behind the bucket (rate.Limiter.Wait returns
// immediately when the deadline cannot be met).
func (c *Client) newRequest(ctx context.Context, method, path string, body any) (*http.Request, error) {
	if c.limiter != nil {
		if err := c.limiter.Wait(ctx); err != nil {
			return nil, fmt.Errorf("waiting on slurmrestd rate limit (%s %s): %w", method, path, err)
		}
	}
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.tokenFile != "" {
		token, err := os.ReadFile(c.tokenFile)
		if err != nil {
			return nil, fmt.Errorf("reading slurm token: %w", err)
		}
		req.Header.Set("X-SLURM-USER-TOKEN", strings.TrimSpace(string(token)))
	}
	if c.user != "" {
		req.Header.Set("X-SLURM-USER-NAME", c.user)
	}
	return req, nil
}

// doHTTP is the actual request/response handling; split out from do so the
// OnRequest callback fires exactly once, from a single call site, regardless
// of which of doHTTP's several return points was taken. code is 0 when the
// request never produced an HTTP response (DNS failure, timeout, transport
// error, etc.) — a real, if unlabeled-by-number, outcome worth counting.
//
// dur is the wall-clock time from starting http.Do to whichever return point
// was taken (A10) — including reading the full response body, since a
// slurmrestd struggling to stream a large /jobs payload is exactly the
// slowness the latency histogram must surface. dur is 0 when the request was
// never sent (newRequest failed: rate-limiter abort, unreadable token file),
// which do() uses to skip the duration callback. The rate-limiter wait
// itself (inside newRequest, before the clock starts) is deliberately NOT
// counted: it measures the bridge's own throttle, not slurmrestd.
func (c *Client) doHTTP(ctx context.Context, method, path string, body, out any) (code int, dur time.Duration, err error) {
	req, err := c.newRequest(ctx, method, path, body)
	if err != nil {
		return 0, 0, err
	}

	// From here on every return statement's literal 0 for dur is a
	// placeholder: this deferred assignment overwrites the named return with
	// the real elapsed time on EVERY exit path below, including transport
	// errors (a timed-out request is latency signal too, arguably the most
	// important one).
	start := time.Now()
	defer func() { dur = time.Since(start) }()
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, 0, err
	}
	code = resp.StatusCode
	defer func() { _ = resp.Body.Close() }()
	// audit minor: an unreadable body must not be silently treated as an
	// empty (and therefore envelope-check-bypassing) response.
	payload, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return code, 0, fmt.Errorf("reading %s %s response body: %w", method, path, readErr)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// audit D3: carry the real HTTP status as a typed error so callers
		// (GetJob) can distinguish a true not-found from a transient failure
		// whose body/status text happens to contain "404" for an unrelated
		// reason (e.g. the job ID itself is 404, 1404, 4040...).
		//
		// 8192, not 512: slurmrestd's error envelope repeats the whole
		// job_desc_msg before the useful "description", so 512 truncated away
		// the actual cause during live debugging. Still bounded — an error
		// body is logged once per failed request.
		return code, 0, &APIError{StatusCode: resp.StatusCode, Method: method, Path: path, Body: truncate(payload, 8192)}
	}

	// Live finding (experiment 03): slurmrestd answers 200 OK even when it
	// ignored the request — unknown fields only surface in `warnings` and
	// failures in `errors`. Errors always fail hard. Warnings fail hard for
	// MUTATING requests only (the silent-no-op class); reads may warn
	// benignly — live finding (experiment 05): an empty queue answers GET
	// /jobs with warning "Zero jobs to dump".
	var envelope struct {
		Errors   []json.RawMessage `json:"errors"`
		Warnings []json.RawMessage `json:"warnings"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		// audit minor: a 200 OK that isn't the expected JSON envelope must not
		// silently bypass the errors/warnings check — treat it as a hard
		// failure instead of decoding garbage into `out` (or succeeding with
		// no output at all).
		return code, 0, fmt.Errorf("slurmrestd %s %s returned non-JSON 200 body: %w: %s", method, path, err, truncate(payload, 256))
	}
	if len(envelope.Errors) > 0 {
		return code, 0, fmt.Errorf("slurmrestd %s %s returned errors: %s", method, path, rawList(envelope.Errors))
	}
	if len(envelope.Warnings) > 0 && method != http.MethodGet {
		return code, 0, fmt.Errorf("slurmrestd %s %s returned warnings (request likely ignored): %s", method, path, rawList(envelope.Warnings))
	}

	if out != nil {
		if err := json.Unmarshal(payload, out); err != nil {
			return code, 0, fmt.Errorf("decoding %s response: %w", path, err)
		}
	}
	return code, 0, nil
}

func rawList(items []json.RawMessage) string {
	parts := make([]string, len(items))
	for i, it := range items {
		parts[i] = truncate(it, 256)
	}
	return strings.Join(parts, "; ")
}

// ListJobs returns all jobs known to the controller (running and queued).
//
// P4 (audit: multi-MB responses at 3k+ jobs, ~23MB heap peaks): the v0.0.44
// data parser has no server-side paging for GET /jobs — there is no
// page/limit/cursor query parameter in the OpenAPI spec, and no "next page"
// link in the envelope, unlike e.g. the accounting endpoints in later
// parsers. Buffering the whole response with io.ReadAll+json.Unmarshal (the
// pattern every other method in this file uses, appropriate for their
// single-object or tiny-array responses) does not scale to a large backlog:
// the entire multi-MB body plus its fully-materialized []Job slice are both
// live in memory at once. listJobsStreaming instead uses a json.Decoder to
// stream-decode the "jobs" array element by element, so memory stays
// bounded by one Job at a time (plus the output slice, which the caller
// needs anyway) instead of the whole response body.
func (c *Client) ListJobs(ctx context.Context, fn func(Job) error) error {
	return c.listJobsStreaming(ctx, fn)
}

// listJobsStreaming performs the GET and decodes the response with
// json.Decoder instead of io.ReadAll+json.Unmarshal, preserving the same
// envelope semantics as do()/doHTTP(): a non-2xx status becomes a typed
// *APIError, a non-empty "errors" array is always a hard failure, and a
// non-empty "warnings" array is benign for this GET (live finding,
// experiment 05: an empty queue answers with warning "Zero jobs to dump").
// The top-level object's OTHER fields ("jobs" is the only one this client
// reads) are skipped without being buffered.
func (c *Client) listJobsStreaming(ctx context.Context, fn func(Job) error) error {
	req, err := c.newRequest(ctx, http.MethodGet, apiPrefix+"/jobs", nil)
	if err != nil {
		if c.OnRequest != nil {
			// The request never left the process (rate-limiter abort, token
			// read failure): counted, but no duration — see OnRequestDuration.
			c.OnRequest(http.MethodGet, 0)
		}
		return err
	}
	start := time.Now()
	resp, err := c.http.Do(req)
	code := 0
	if resp != nil {
		code = resp.StatusCode
	}
	defer func() {
		if c.OnRequest != nil {
			c.OnRequest(http.MethodGet, code)
		}
		// A10: duration until this function finishes, i.e. including the
		// streaming decode of the response body — for THIS endpoint the body
		// stream is the slow part a struggling slurmrestd drags out, exactly
		// what the histogram must see (mirrors doHTTP, whose duration also
		// spans reading the whole body).
		if c.OnRequestDuration != nil {
			c.OnRequestDuration(http.MethodGet, time.Since(start))
		}
	}()
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Error responses are small (a description string, not a job array) —
		// buffering this one is fine and keeps APIError's Body field useful.
		payload, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("reading %s %s response body: %w", http.MethodGet, apiPrefix+"/jobs", readErr)
		}
		return &APIError{StatusCode: resp.StatusCode, Method: http.MethodGet, Path: apiPrefix + "/jobs", Body: truncate(payload, 512)}
	}

	dec := json.NewDecoder(resp.Body)
	var errs, warnings []json.RawMessage
	sawEnvelope := false

	// Walk the top-level JSON object token by token. Field order in the
	// response is not guaranteed, so this handles "jobs"/"errors"/"warnings"
	// in whatever order slurmrestd sends them; any other field is skipped
	// with dec.Decode(&json.RawMessage{}) to consume it without buffering it
	// meaningfully.
	if err := expectDelim(dec, '{'); err != nil {
		return fmt.Errorf("slurmrestd GET %s returned non-JSON 200 body: %w", apiPrefix+"/jobs", err)
	}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return fmt.Errorf("slurmrestd GET %s returned non-JSON 200 body: %w", apiPrefix+"/jobs", err)
		}
		key, ok := keyTok.(string)
		if !ok {
			return fmt.Errorf("slurmrestd GET %s returned non-JSON 200 body: expected object key, got %v", apiPrefix+"/jobs", keyTok)
		}
		switch key {
		case "jobs":
			sawEnvelope = true
			if err := decodeJobsArray(dec, fn); err != nil {
				return fmt.Errorf("decoding %s response: %w", apiPrefix+"/jobs", err)
			}
		case "errors":
			sawEnvelope = true
			if err := dec.Decode(&errs); err != nil {
				return fmt.Errorf("slurmrestd GET %s returned non-JSON 200 body: %w", apiPrefix+"/jobs", err)
			}
		case "warnings":
			sawEnvelope = true
			if err := dec.Decode(&warnings); err != nil {
				return fmt.Errorf("slurmrestd GET %s returned non-JSON 200 body: %w", apiPrefix+"/jobs", err)
			}
		default:
			// Unknown field (e.g. "last_backfill", "meta" in some parsers):
			// skip its value without caring about its shape.
			var discard json.RawMessage
			if err := dec.Decode(&discard); err != nil {
				return fmt.Errorf("slurmrestd GET %s returned non-JSON 200 body: %w", apiPrefix+"/jobs", err)
			}
		}
	}
	if err := expectDelim(dec, '}'); err != nil {
		return fmt.Errorf("slurmrestd GET %s returned non-JSON 200 body: %w", apiPrefix+"/jobs", err)
	}
	if !sawEnvelope {
		// audit minor parity with doHTTP: a 200 OK whose body decodes as valid
		// JSON but isn't the expected envelope shape at all (e.g. `{}` from a
		// misconfigured proxy) must not silently succeed with an empty list.
		return fmt.Errorf("slurmrestd GET %s returned non-JSON 200 body: missing jobs/errors/warnings envelope", apiPrefix+"/jobs")
	}
	if len(errs) > 0 {
		return fmt.Errorf("slurmrestd GET %s returned errors: %s", apiPrefix+"/jobs", rawList(errs))
	}
	// Warnings are benign for this GET (live finding, experiment 05): an
	// empty queue answers with warning "Zero jobs to dump" and an empty jobs
	// array, which must succeed — see TestWarningsOnGETAreBenign.
	return nil
}

// expectDelim consumes the next JSON token and verifies it is the given
// delimiter, for manual top-level object walking in listJobsStreaming.
func expectDelim(dec *json.Decoder, want json.Delim) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != want {
		return fmt.Errorf("expected delimiter %q, got %v", want, tok)
	}
	return nil
}

// decodeJobsArray streams the "jobs" array one element at a time into *jobs,
// instead of decoding the whole array into memory in one call — the entire
// point of P4 (a 3000+ job backlog's array is the multi-MB part of the
// response).
func decodeJobsArray(dec *json.Decoder, fn func(Job) error) error {
	if err := expectDelim(dec, '['); err != nil {
		return err
	}
	intern := make(map[string]string)
	internString := func(s string) string {
		if s == "" {
			return s
		}
		if existing, ok := intern[s]; ok {
			return existing
		}
		intern[s] = s
		return s
	}

	for dec.More() {
		var j Job
		if err := dec.Decode(&j); err != nil {
			return err
		}
		j.Name = internString(j.Name)
		j.Partition = internString(j.Partition)
		j.StateReason = internString(j.StateReason)
		j.AdminComment = internString(j.AdminComment)
		j.Comment = internString(j.Comment)
		j.TresPerNode = internString(j.TresPerNode)
		for i, s := range j.JobState {
			j.JobState[i] = internString(s)
		}
		if err := fn(j); err != nil {
			return err
		}
	}
	return expectDelim(dec, ']')
}

// GetJob fetches one job; found=false means Slurm no longer knows it
// (finished long ago, purged from memory — treat as terminal).
func (c *Client) GetJob(ctx context.Context, id uint64) (job *Job, found bool, err error) {
	var out struct {
		Jobs []Job `json:"jobs"`
	}
	err = c.do(ctx, http.MethodGet, fmt.Sprintf("%s/job/%d", apiPrefix, id), nil, &out)
	if err != nil {
		// audit D3: only a TRUE HTTP 404 means "Slurm no longer knows this
		// job". Matching on a substring of the error text misclassified any
		// transient failure whose status/body happened to contain "404" —
		// including a genuine 404 for an unrelated job ID like 1404 or 4040 —
		// as not-found, which made cleanupFinishedJobs delete a live job's
		// JobSet. errors.As only matches the typed status carried by do().
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
			return nil, false, nil
		}
		return nil, false, err
	}
	if len(out.Jobs) == 0 {
		return nil, false, nil
	}
	return &out.Jobs[0], true, nil
}

// SetJobFeatures constrains a held job to the dynamic nodes created for it.
// features is the node-locking feature string; the job_desc_msg field is
// "constraints", NOT "features", per the v0.0.44 schema.
//
// This MUST be a separate call that completes BEFORE ReleaseJob. P2 (perf
// fix) had merged the two into one POST to halve per-admission REST
// mutations; live testing showed slurmrestd can apply "hold": false without
// having applied "constraints" from the same body, releasing the job before it
// is pinned to its dynamic nodes — Slurm then schedules it onto arbitrary
// nodes ("runaway job"), breaking the core invariant that a bridge job only
// ever runs on the JobSet it was admitted for. Correctness wins over the one
// saved POST: do NOT re-merge these calls.
//
// Live finding (experiment 03): unlike job SUBMISSION, the update endpoint
// takes a bare job_desc_msg — NOT wrapped in {"job": ...}. A wrapped payload
// is answered with 200 OK plus a warning and ignored entirely.
func (c *Client) SetJobFeatures(ctx context.Context, id uint64, features string) error {
	body := map[string]any{"constraints": features}
	return c.do(ctx, http.MethodPost, fmt.Sprintf("%s/job/%d", apiPrefix, id), body, nil)
}

// ReleaseJob lifts the Hold. Callers must have already pinned the job to its
// dynamic nodes via SetJobFeatures — see that function's note on runaway jobs.
//
// Envelope discipline as above: a bare job_desc_msg carrying only "hold".
func (c *Client) ReleaseJob(ctx context.Context, id uint64) error {
	body := map[string]any{"hold": false}
	return c.do(ctx, http.MethodPost, fmt.Sprintf("%s/job/%d", apiPrefix, id), body, nil)
}

// DeleteNode removes a dynamic node record after its pod is gone.
func (c *Client) DeleteNode(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodDelete, fmt.Sprintf("%s/node/%s", apiPrefix, name), nil, nil)
}

// CancelJob asks slurmctld to cancel/fail a job whose JobSet died (D1: a
// Failed or deadline-exceeded JobSet previously left the Slurm job pending
// forever). Uses the same job resource as ReleaseJob/SetJobFeature but with
// DELETE, matching slurmrestd's job-cancellation endpoint.
func (c *Client) CancelJob(ctx context.Context, id uint64) error {
	return c.do(ctx, http.MethodDelete, fmt.Sprintf("%s/job/%d", apiPrefix, id), nil, nil)
}

// SetJobComment writes a human-readable admission status into the job's
// comment field, surfacing "why am I waiting" in squeue (-o %k).
//
// L2 — SOLVED. The 422
// Unprocessable Entity on this POST was NEVER a payload problem (not the
// body envelope, not the value). Reproduced directly against live slurmrestd
// v0.0.44: the same {"comment":...} body returns HTTP 200 with a valid user
// and HTTP 422 error 2010 "Invalid user id" when X-SLURM-USER-NAME is a name
// slurmrestd does not know. The bridge was configured with
// slurmUser="k8s-bridge" — not a real Slurm user — so slurmrestd rejected
// EVERY job update (this comment AND ReleaseJob, which shares the header).
// Fix: leave slurmUser empty (newRequest then omits the header and the JWT's
// own user is used) or set a real provisioned user; config/chart defaults
// changed accordingly. sanitizeComment below is kept as harmless hardening
// (a free-text field should not carry control chars) but was not the fix.
func (c *Client) SetJobComment(ctx context.Context, id uint64, comment string) error {
	body := map[string]any{"comment": sanitizeComment(comment)}
	return c.do(ctx, http.MethodPost, fmt.Sprintf("%s/job/%d", apiPrefix, id), body, nil)
}

// sanitizeComment makes a string safe for a Slurm job comment field: control
// characters (newlines, tabs, etc.) become spaces, runs of whitespace
// collapse to one, and the result is trimmed. Kueue condition messages that
// feed the comment can contain such characters (L2, e2e iteration 2).
func sanitizeComment(s string) string {
	cleaned := strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, s)
	return strings.Join(strings.Fields(cleaned), " ")
}

// SetJobAdminComment acknowledges a processed directive (ADR-0009). Same L2
// envelope-discipline review as SetJobComment applies here: bare
// {"admin_comment": "..."}, no wrapping, no extra fields.
func (c *Client) SetJobAdminComment(ctx context.Context, id uint64, comment string) error {
	body := map[string]any{"admin_comment": comment}
	return c.do(ctx, http.MethodPost, fmt.Sprintf("%s/job/%d", apiPrefix, id), body, nil)
}

// SetJobPriority mirrors the effective Kueue priority back into Slurm so
// squeue/sprio stop displaying fiction (UX remediation: mutable priority).
func (c *Client) SetJobPriority(ctx context.Context, id uint64, priority uint32) error {
	body := map[string]any{"priority": map[string]any{"set": true, "number": priority}}
	return c.do(ctx, http.MethodPost, fmt.Sprintf("%s/job/%d", apiPrefix, id), body, nil)
}

// Tasks helpers for --nodes translation.

// Nodes returns --nodes if set (0 = not requested).
func (j *Job) Nodes() int32 {
	if j.NodeCount.Set && j.NodeCount.Number > 0 {
		return clampUint64ToInt32(j.NodeCount.Number)
	}
	return 0
}

// TasksPerNodeOrDefault returns --ntasks-per-node, defaulting to 1.
func (j *Job) TasksPerNodeOrDefault() int64 {
	if j.TasksPerNode.Set && j.TasksPerNode.Number > 0 {
		return int64(j.TasksPerNode.Number)
	}
	return 1
}

// TimeLimitSeconds returns the wall-clock limit in seconds (0 = unlimited).
func (j *Job) TimeLimitSeconds() int64 {
	if j.TimeLimit.Set && !j.TimeLimit.Infinite && j.TimeLimit.Number > 0 {
		return int64(j.TimeLimit.Number) * 60
	}
	return 0
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
