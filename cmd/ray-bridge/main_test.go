package main

import (
	"log/slog"
	"testing"
)

func TestParseLogLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"debug":   slog.LevelDebug,
		"info":    slog.LevelInfo,
		"WARN":    slog.LevelWarn,
		"warning": slog.LevelWarn,
		"error":   slog.LevelError,
		"":        slog.LevelInfo,
		"bogus":   slog.LevelInfo,
	}
	for in, want := range cases {
		if got := parseLogLevel(in); got != want {
			t.Errorf("parseLogLevel(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestSplitAndTrim(t *testing.T) {
	got := splitAndTrim(" rayproject/ , , ghcr.io/x ")
	if len(got) != 2 || got[0] != "rayproject/" || got[1] != "ghcr.io/x" {
		t.Errorf("splitAndTrim = %#v", got)
	}
	if len(splitAndTrim("")) != 0 {
		t.Errorf("empty input should yield no entries")
	}
}

func TestCacheScopedToNamespace(t *testing.T) {
	if got := cacheScopedToNamespace(""); len(got.DefaultNamespaces) != 0 {
		t.Errorf("empty namespace should yield an all-namespaces cache")
	}
	got := cacheScopedToNamespace("ray")
	if _, ok := got.DefaultNamespaces["ray"]; !ok {
		t.Errorf("namespace ray not scoped: %#v", got.DefaultNamespaces)
	}
}

// TestInformerSyncChecker pins the readiness gate's two states (B2): failing
// until the cache-sync channel closes, passing forever after. The channel is
// the only coupling to the manager, so this covers everything testable
// without standing one up.
func TestInformerSyncChecker(t *testing.T) {
	synced := make(chan struct{})
	check := informerSyncChecker(synced)
	if err := check(nil); err == nil {
		t.Errorf("checker must FAIL before the informer caches sync")
	}
	close(synced)
	if err := check(nil); err != nil {
		t.Errorf("checker must pass after sync, got: %v", err)
	}
	// Readiness is sticky: repeated probes keep passing.
	if err := check(nil); err != nil {
		t.Errorf("checker must keep passing on subsequent probes, got: %v", err)
	}
}

func TestMetricsServerOptions(t *testing.T) {
	if got := metricsServerOptions(""); got.BindAddress != "0" {
		t.Errorf("empty addr should disable metrics (BindAddress=0), got %q", got.BindAddress)
	}
	if got := metricsServerOptions(":8080"); got.BindAddress != ":8080" {
		t.Errorf("BindAddress = %q, want :8080", got.BindAddress)
	}
}
