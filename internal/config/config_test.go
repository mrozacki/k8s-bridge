package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

const validConfig = `
slurmRestURL: "http://localhost:6820"
allowInsecureHTTP: true
namespace: "slurm-jobs"
localQueue: "main"
partitionMappings:
  - partitionName: "mixing"
    workloadPriorityClass: "normal-priority"
slurmd:
  image: "slurmd:test"
  confServer: "ctl:6817"
  authSecretName: "slurm-auth-slurm"
pollInterval: 30s
`

func TestLoadValidConfig(t *testing.T) {
	cfg, err := Load(writeConfig(t, validConfig))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.PollInterval.Duration != 30*time.Second {
		t.Errorf("pollInterval = %v, want 30s (string durations must parse)", cfg.PollInterval.Duration)
	}
	if pc, ok := cfg.PriorityClassFor("mixing"); !ok || pc != "normal-priority" {
		t.Errorf("PriorityClassFor(mixing) = %q,%v", pc, ok)
	}
	if _, ok := cfg.PriorityClassFor("other"); ok {
		t.Error("unmapped partition must not resolve")
	}
}

// TestLocalQueueForOverride is the A1b regression: a partition mapping with
// its own LocalQueue must win over the global one.
func TestLocalQueueForOverride(t *testing.T) {
	cfg := &Config{
		LocalQueue: "main",
		PartitionMappings: []PartitionMapping{
			{PartitionName: "mixing", WorkloadPriorityClass: "normal-priority"},
			{PartitionName: "team-a", WorkloadPriorityClass: "normal-priority", LocalQueue: "team-a-queue"},
		},
	}
	if got := cfg.LocalQueueFor("team-a"); got != "team-a-queue" {
		t.Errorf("LocalQueueFor(team-a) = %q, want team-a-queue", got)
	}
}

// TestLocalQueueForFallsBackToGlobal is the counterpart: no override, no
// mapping at all, both fall back to the global LocalQueue.
func TestLocalQueueForFallsBackToGlobal(t *testing.T) {
	cfg := &Config{
		LocalQueue: "main",
		PartitionMappings: []PartitionMapping{
			{PartitionName: "mixing", WorkloadPriorityClass: "normal-priority"},
		},
	}
	if got := cfg.LocalQueueFor("mixing"); got != "main" {
		t.Errorf("LocalQueueFor(mixing) = %q, want main (no override)", got)
	}
	if got := cfg.LocalQueueFor("unmapped"); got != "main" {
		t.Errorf("LocalQueueFor(unmapped) = %q, want main (fallback for unmanaged partition too)", got)
	}
}

func TestLoadAppliesDefaults(t *testing.T) {
	cfg, err := Load(writeConfig(t, strings.ReplaceAll(validConfig, "pollInterval: 30s\n", "")))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.PollInterval.Duration != 10*time.Second {
		t.Errorf("default pollInterval = %v, want 10s", cfg.PollInterval.Duration)
	}
	if cfg.Slurmd.GPUResourceName != "nvidia.com/gpu" {
		t.Errorf("default gpuResourceName = %q, want nvidia.com/gpu", cfg.Slurmd.GPUResourceName)
	}
	if cfg.CreateWorkers != 8 {
		t.Errorf("default createWorkers = %d, want 8 (P5)", cfg.CreateWorkers)
	}
	if cfg.SlurmRequestTimeout.Duration != 30*time.Second {
		t.Errorf("default slurmRequestTimeout = %v, want 30s", cfg.SlurmRequestTimeout.Duration)
	}
}

// TestLoadHonorsExplicitSlurmRequestTimeout confirms a raised timeout (the
// large-backlog escape hatch from the suite-E scale run) is preserved.
func TestLoadHonorsExplicitSlurmRequestTimeout(t *testing.T) {
	cfg, err := Load(writeConfig(t, validConfig+"\nslurmRequestTimeout: 2m\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.SlurmRequestTimeout.Duration != 2*time.Minute {
		t.Errorf("slurmRequestTimeout = %v, want 2m (explicit value honored)", cfg.SlurmRequestTimeout.Duration)
	}
}

// TestLoadHonorsExplicitCreateWorkers confirms an operator-set createWorkers
// value is preserved rather than overridden by the P5 default.
func TestLoadHonorsExplicitCreateWorkers(t *testing.T) {
	cfg, err := Load(writeConfig(t, validConfig+"\ncreateWorkers: 1\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.CreateWorkers != 1 {
		t.Errorf("createWorkers = %d, want 1 (explicit value honored)", cfg.CreateWorkers)
	}
}

func TestLoadRejectsBadInput(t *testing.T) {
	cases := map[string]string{
		"missing required fields":       `namespace: "x"`,
		"no partition mappings":         strings.Replace(validConfig, "partitionMappings:\n  - partitionName: \"mixing\"\n    workloadPriorityClass: \"normal-priority\"\n", "partitionMappings: []\n", 1),
		"unknown field (strict)":        validConfig + "\ntypoField: true\n",
		"invalid duration":              strings.Replace(validConfig, "30s", "soon", 1),
		"plaintext http without optin":  strings.Replace(validConfig, "allowInsecureHTTP: true\n", "", 1),
		"unsupported scheme":            strings.Replace(validConfig, "http://localhost:6820", "ftp://localhost:6820", 1),
		"poll interval too small":       strings.Replace(validConfig, "30s", "100ms", 1),
		"negative maxUserPriority":      validConfig + "\nmaxUserPriority: -1\n",
		"missing slurmd image":          strings.Replace(validConfig, "  image: \"slurmd:test\"\n", "", 1),
		"negative createWorkers":        validConfig + "\ncreateWorkers: -1\n",
		"slurmRequestTimeout too small": validConfig + "\nslurmRequestTimeout: 500ms\n",
		"slurmRequestTimeout too large": validConfig + "\nslurmRequestTimeout: 11m\n",
		// A1: the slurmrestd rate limit must be within [0, 10000].
		"negative slurmRequestsPerSecond":  validConfig + "\nslurmRequestsPerSecond: -1\n",
		"oversized slurmRequestsPerSecond": validConfig + "\nslurmRequestsPerSecond: 10001\n",
		// A4: a negative grace on the knob gating an irreversible scancel
		// was previously coerced to the default 3 by ApplyDefaults — an
		// obvious config mistake silently becoming a live grace period.
		"negative orphanGraceTicks": validConfig + "\norphanGraceTicks: -1\n",
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeConfig(t, content)); err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}

// validConfigMappings is validConfig's partitionMappings block verbatim, so
// tests can swap in variants via strings.Replace (the strict YAML parser
// rejects duplicate top-level keys, so appending a second block is not an
// option).
const validConfigMappings = `partitionMappings:
  - partitionName: "mixing"
    workloadPriorityClass: "normal-priority"
`

// withMappings returns validConfig with its partitionMappings block replaced.
func withMappings(t *testing.T, replacement string) string {
	t.Helper()
	if !strings.Contains(validConfig, validConfigMappings) {
		t.Fatal("validConfigMappings drifted from validConfig; update the fixture")
	}
	return strings.Replace(validConfig, validConfigMappings, replacement, 1)
}

// TestLoadRejectsBadPartitionMappings covers the A4 tightening of the
// per-mapping checks, mirroring rayconfig.Validate: empty names are dead
// config, and a DUPLICATE partitionName silently shadows the later entry
// (PriorityClassFor/LocalQueueFor are first-match-wins), so the operator who
// edited the losing copy sees their change ignored with no signal.
func TestLoadRejectsBadPartitionMappings(t *testing.T) {
	cases := map[string]string{
		"duplicate partitionName": `partitionMappings:
  - partitionName: "mixing"
    workloadPriorityClass: "normal-priority"
  - partitionName: "mixing"
    workloadPriorityClass: "high-priority"
`,
		"empty partitionName": `partitionMappings:
  - partitionName: ""
    workloadPriorityClass: "normal-priority"
`,
		"whitespace-only partitionName": `partitionMappings:
  - partitionName: "   "
    workloadPriorityClass: "normal-priority"
`,
		"empty workloadPriorityClass": `partitionMappings:
  - partitionName: "mixing"
    workloadPriorityClass: ""
`,
	}
	for name, block := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeConfig(t, withMappings(t, block))); err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
	// Distinct partitions must of course still load (guards against the
	// duplicate check over-matching).
	multi := `partitionMappings:
  - partitionName: "mixing"
    workloadPriorityClass: "normal-priority"
  - partitionName: "mixing-high"
    workloadPriorityClass: "high-priority"
`
	if _, err := Load(writeConfig(t, withMappings(t, multi))); err != nil {
		t.Errorf("two DISTINCT partitions must be accepted: %v", err)
	}
}

// TestSlurmRequestsPerSecondDefaultsToUnlimited pins the A1 compatibility
// contract: an untouched config keeps the historic unlimited behavior (0),
// and an explicit in-range value is preserved.
func TestSlurmRequestsPerSecondDefaultsToUnlimited(t *testing.T) {
	cfg, err := Load(writeConfig(t, validConfig))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.SlurmRequestsPerSecond != 0 {
		t.Errorf("slurmRequestsPerSecond = %v, want 0 (unlimited) by default", cfg.SlurmRequestsPerSecond)
	}

	cfg, err = Load(writeConfig(t, validConfig+"\nslurmRequestsPerSecond: 12.5\n"))
	if err != nil {
		t.Fatalf("Load with explicit rate: %v", err)
	}
	if cfg.SlurmRequestsPerSecond != 12.5 {
		t.Errorf("slurmRequestsPerSecond = %v, want 12.5 (explicit value honored, fractional rates allowed)", cfg.SlurmRequestsPerSecond)
	}
	// The bounds themselves are inclusive: exactly 10000 must pass.
	if _, err := Load(writeConfig(t, validConfig+"\nslurmRequestsPerSecond: 10000\n")); err != nil {
		t.Errorf("slurmRequestsPerSecond: 10000 (the documented maximum) must be accepted: %v", err)
	}
}

// TestOrphanGraceTicksZeroStillDefaults pins the boundary A4 must NOT move:
// only a NEGATIVE value is a config error; 0 (or omitted) keeps taking the
// documented default of 3, exactly as the CRD description promises.
func TestOrphanGraceTicksZeroStillDefaults(t *testing.T) {
	cfg, err := Load(writeConfig(t, validConfig+"\norphanGraceTicks: 0\n"))
	if err != nil {
		t.Fatalf("orphanGraceTicks: 0 must still load (defaulting, not an error): %v", err)
	}
	if cfg.OrphanGraceTicks != 3 {
		t.Errorf("OrphanGraceTicks = %d, want the default 3 for an explicit 0", cfg.OrphanGraceTicks)
	}
}

func TestLoadAcceptsHTTPS(t *testing.T) {
	https := strings.Replace(validConfig, "slurmRestURL: \"http://localhost:6820\"\nallowInsecureHTTP: true\n", "slurmRestURL: \"https://slurm-restapi.slurm:6820\"\n", 1)
	if _, err := Load(writeConfig(t, https)); err != nil {
		t.Fatalf("https config should load: %v", err)
	}
}

func TestValidateImageAllowed(t *testing.T) {
	allow := []string{"ghcr.io/slinkyproject/", "registry.example.com/slurmd:pinned"}
	if err := ValidateImageAllowed("ghcr.io/slinkyproject/slurmd:26.05", allow); err != nil {
		t.Errorf("prefix match should pass: %v", err)
	}
	if err := ValidateImageAllowed("registry.example.com/slurmd:pinned", allow); err != nil {
		t.Errorf("exact match should pass: %v", err)
	}
	if err := ValidateImageAllowed("docker.io/evil/slurmd:latest", allow); err == nil {
		t.Error("image outside allowlist must be rejected")
	}
}

func TestPrivilegedDefaultsTrue(t *testing.T) {
	cfg, err := Load(writeConfig(t, validConfig))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Slurmd.PrivilegedOrDefault() {
		t.Error("privileged must default to true when unset")
	}
	off := strings.Replace(validConfig, "  authSecretName: \"slurm-auth-slurm\"\n", "  authSecretName: \"slurm-auth-slurm\"\n  privileged: false\n", 1)
	cfg, err = Load(writeConfig(t, off))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Slurmd.PrivilegedOrDefault() {
		t.Error("privileged: false must be honored")
	}
}
