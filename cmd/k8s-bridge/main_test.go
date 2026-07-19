package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/mrozacki/k8s-bridge/internal/bridge"
	"github.com/mrozacki/k8s-bridge/internal/config"
	"github.com/mrozacki/k8s-bridge/internal/metrics"
	"github.com/mrozacki/k8s-bridge/internal/slurm"
	"github.com/mrozacki/k8s-bridge/internal/translate"
)

// TestLeaderElectionNamespacePrefersPodNamespace pins the fix for the live
// finding (experiment 09, A4): the leader-election Lease must land in the
// namespace the controller runs in, because that is where the chart's RBAC
// grants it. POD_NAMESPACE (downward API) wins; wmNamespace is the CR-mode
// fallback; "default" only when nothing else is known.
func TestLeaderElectionNamespacePrefersPodNamespace(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "slurm-jobs")
	if got := leaderElectionNamespace("cr-ns"); got != "slurm-jobs" {
		t.Fatalf("POD_NAMESPACE must win, got %q", got)
	}
}

func TestLeaderElectionNamespaceFallsBackToWMNamespace(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "")
	if got := leaderElectionNamespace("cr-ns"); got != "cr-ns" {
		t.Fatalf("wmNamespace fallback expected, got %q", got)
	}
}

func TestLeaderElectionNamespaceDefaultsLast(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "")
	if got := leaderElectionNamespace(""); got != "default" {
		t.Fatalf(`expected "default", got %q`, got)
	}
}

// TestResolveMode pins the ADR-0015 Phase A flag contract: --workloadmixing
// always wins (single-CR compatibility path, implying the CR source
// whatever --config-source says — the flag package cannot distinguish an
// explicit "file" from the default, and erroring on the default would break
// every existing single-CR deployment); otherwise --config-source picks
// file vs supervisor, and anything else is a hard error.
func TestResolveMode(t *testing.T) {
	cases := []struct {
		configSource   string
		workloadMixing string
		want           controllerMode
		wantErr        bool
	}{
		{"file", "", modeFile, false},
		{"cr", "", modeSupervisor, false},
		// The compatibility path: --workloadmixing set means single-CR,
		// with or without an explicit --config-source.
		{"file", "ns/name", modeSingleCR, false},
		{"cr", "ns/name", modeSingleCR, false},
		{"configmap", "", modeFile, true},
		{"", "", modeFile, true},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("source=%q wm=%q", tc.configSource, tc.workloadMixing), func(t *testing.T) {
			got, err := resolveMode(tc.configSource, tc.workloadMixing)
			if (err != nil) != tc.wantErr {
				t.Fatalf("resolveMode error = %v, wantErr = %v", err, tc.wantErr)
			}
			if err == nil && got != tc.want {
				t.Errorf("resolveMode = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestValidKubeRateLimits is the A5 regression: the old comparison
// `*kubeBurst < int(*kubeQPS)` TRUNCATED the qps before comparing, so
// --kube-api-qps=20.5 --kube-api-burst=20 passed validation and handed
// client-go a burst below its qps. The comparison must happen in float64.
func TestValidKubeRateLimits(t *testing.T) {
	cases := []struct {
		qps   float64
		burst int
		want  bool
	}{
		{20, 40, true},
		// burst == qps is the documented floor.
		{20, 20, true},
		// THE truncation bug: int(20.5)==20 made this pass before A5.
		{20.5, 20, false},
		// The next integer burst genuinely covers 20.5 qps.
		{20.5, 21, true},
		// qps must be positive.
		{0, 10, false},
		{-1, 10, false},
		// Fractional qps below 1 works with burst 1; burst 0 covers nothing.
		{0.5, 1, true},
		{0.5, 0, false},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("qps=%v burst=%d", tc.qps, tc.burst), func(t *testing.T) {
			if got := validKubeRateLimits(tc.qps, tc.burst); got != tc.want {
				t.Errorf("validKubeRateLimits(%v, %d) = %v, want %v", tc.qps, tc.burst, got, tc.want)
			}
		})
	}
}

// TestIsBridgeOwnedJobSetMatchesTranslateConstants guards the hardcoded
// label pair in isBridgeOwnedJobSet against drifting from internal/translate
// (the label value is duplicated there on purpose, to avoid a
// cmd -> internal/translate dependency in production code for one string
// pair — this test IS the promised guard; see isBridgeOwnedJobSet's doc).
func TestIsBridgeOwnedJobSetMatchesTranslateConstants(t *testing.T) {
	owned := &jobsetv1alpha2.JobSet{}
	owned.Labels = map[string]string{translate.ManagedByLabel: translate.ManagedByValue}
	if !isBridgeOwnedJobSet(owned) {
		t.Errorf("a JobSet labeled %s=%s (translate's exported constants) must be recognized as bridge-owned",
			translate.ManagedByLabel, translate.ManagedByValue)
	}

	wrongValue := &jobsetv1alpha2.JobSet{}
	wrongValue.Labels = map[string]string{translate.ManagedByLabel: "someone-else"}
	if isBridgeOwnedJobSet(wrongValue) {
		t.Error("a JobSet managed by someone else must not nudge the bridge")
	}

	unlabeled := &jobsetv1alpha2.JobSet{}
	if isBridgeOwnedJobSet(unlabeled) {
		t.Error("an unlabeled JobSet must not be treated as bridge-owned")
	}

	notAJobSet := &corev1.Pod{}
	notAJobSet.Labels = map[string]string{translate.ManagedByLabel: translate.ManagedByValue}
	if isBridgeOwnedJobSet(notAJobSet) {
		t.Error("a non-JobSet object must never pass the filter, whatever its labels")
	}
}

// histogramSampleCount returns a histogram's total observation count (its
// "_count") — testutil.ToFloat64 does not support histograms.
func histogramSampleCount(t *testing.T, h prometheus.Histogram) uint64 {
	t.Helper()
	m := &dto.Metric{}
	if err := h.Write(m); err != nil {
		t.Fatalf("writing histogram metric: %v", err)
	}
	return m.GetHistogram().GetSampleCount()
}

// TestWireSlurmClientMetricsObservesRequestDuration is the A10 end-to-end
// wiring test: a real request through a wired client must land exactly one
// observation in k8s_bridge_slurm_request_duration_seconds under its method
// label (the seam itself is unit-tested in internal/slurm; this covers
// main.go's side of it).
func TestWireSlurmClientMetricsObservesRequestDuration(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"jobs": [], "errors": [], "warnings": []}`))
	}))
	t.Cleanup(srv.Close)

	c, err := slurm.NewClient(slurm.Options{BaseURL: srv.URL})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	wireSlurmClientMetrics(c)

	getHist, ok := metrics.SlurmRequestDuration.WithLabelValues(http.MethodGet).(prometheus.Histogram)
	if !ok {
		t.Fatal("SlurmRequestDuration observer is not a Histogram")
	}
	before := histogramSampleCount(t, getHist)

	if err := c.ListJobs(context.Background(), func(slurm.Job) error { return nil }); err != nil {
		t.Fatalf("ListJobs: %v", err)
	}

	if got := histogramSampleCount(t, getHist); got != before+1 {
		t.Errorf("slurm_request_duration_seconds{method=GET} sample count = %d, want %d (exactly one observation per request)", got, before+1)
	}
}

// testProbeBridge builds the minimal Bridge the metrics mux handlers need.
func testProbeBridge(t *testing.T) *bridge.Bridge {
	t.Helper()
	return bridge.New(&config.Config{}, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// TestStartMetricsServerFailsFastOnBindConflict is the A7 regression: a bind
// failure must surface SYNCHRONOUSLY as an error (main then exits non-zero)
// instead of being logged inside a goroutine and ignored, which left a
// running controller whose /healthz and /readyz did not exist — kubelet then
// restart-looped the pod with symptoms pointing everywhere but the port.
func TestStartMetricsServerFailsFastOnBindConflict(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("occupying a port: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	b := testProbeBridge(t)
	_, err = startMetricsServer(context.Background(), ln.Addr().String(), b.HealthzHandler(), b.ReadyzHandler(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil {
		t.Fatal("startMetricsServer must return an error when the address is already in use")
	}
}

// TestServeMetricsServesProbesAndShutsDownOnContextCancel covers the other
// A7 halves: the consolidated mux actually answers /healthz while running,
// and a context cancel triggers a graceful Shutdown (the listener stops
// accepting) without reporting an error on the failure channel.
func TestServeMetricsServesProbesAndShutsDownOnContextCancel(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("binding test listener: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := testProbeBridge(t)
	errCh := serveMetrics(ctx, ln, b.HealthzHandler(), b.ReadyzHandler(), slog.New(slog.NewTextHandler(io.Discard, nil)))

	url := "http://" + ln.Addr().String() + "/healthz"
	resp, err := http.Get(url) //nolint:gosec // G107: URL targets the test's own 127.0.0.1 listener
	if err != nil {
		t.Fatalf("GET /healthz while running: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /healthz = %d, want 200", resp.StatusCode)
	}

	cancel()
	// Graceful shutdown is asynchronous; poll (bounded, no fixed sleep) until
	// the listener stops accepting connections.
	deadline := time.Now().Add(3 * time.Second)
	for {
		resp, err := http.Get(url) //nolint:gosec // G107: URL targets the test's own 127.0.0.1 listener
		if err != nil {
			break // server is down: shutdown completed
		}
		_ = resp.Body.Close()
		if time.Now().After(deadline) {
			t.Fatal("server still accepting requests 3s after context cancel — graceful shutdown never ran")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// A graceful ErrServerClosed exit must NOT be reported as a failure.
	select {
	case err := <-errCh:
		t.Fatalf("errCh delivered %v after a graceful shutdown, want nothing", err)
	default:
	}
}
