package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// FuzzConfigLoad fuzzes Load's YAML decode + ApplyDefaults + Validate chain
// (backlog AUD5). Seeded with the repo's example-config.yaml (the one
// documented file-mode config, config/example-config.yaml) plus the
// minimal valid fixture used by config_test.go's TestLoadValidConfig, so
// the fuzzer starts from known-good shapes and mutates from there.
//
// Property asserted: Load must never panic on arbitrary bytes, no matter how
// malformed — it must return either a valid *Config or a non-nil error.
// yaml.UnmarshalStrict rejecting unknown fields, Validate rejecting missing
// required fields, and ApplyDefaults running on partial input are all
// expected, non-panicking outcomes.
func FuzzConfigLoad(f *testing.F) {
	const minimalValid = `
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
	f.Add([]byte(minimalValid))

	if example, err := os.ReadFile(exampleConfigPath()); err == nil {
		f.Add(example)
	}

	// Deliberately malformed/edge shapes worth seeding beyond valid YAML.
	malformed := []string{
		"",
		"{}",
		"not: [valid, yaml: at all",
		"slurmRestURL: null",
		"partitionMappings: not-a-list",
		"pollInterval: -5s",
		"maxUserPriority: -1",
		"unknownField: true",
		strings.Repeat("a: b\n", 500), // pathologically large flat map
	}
	for _, m := range malformed {
		f.Add([]byte(m))
	}

	f.Fuzz(func(t *testing.T, raw []byte) {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Skip("could not write fuzz input to a temp file")
		}
		// Load may legitimately fail on malformed/incomplete input; it must
		// never panic while doing so.
		_, _ = Load(path)
	})
}

// exampleConfigPath locates the repo's documented example config relative
// to this test file, so the seed corpus includes a real, currently-valid
// configuration without hardcoding an absolute path or duplicating its
// contents inline.
func exampleConfigPath() string {
	return filepath.Join("..", "..", "config", "example-config.yaml")
}
