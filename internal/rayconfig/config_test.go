package rayconfig

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func validConfig() *Config {
	c := &Config{
		Namespace:       "ray",
		LocalQueue:      "main",
		ManagedClusters: []ManagedCluster{{Name: "shared", HeadAddress: "shared-head-svc.ray:6379"}},
		PoolMappings: []PoolMapping{
			{Pool: "batch", WorkloadPriorityClass: "normal-priority"},
			{Pool: "hi", WorkloadPriorityClass: "high-priority", LocalQueue: "team-a"},
		},
		Worker: Worker{Image: "rayproject/ray:2.9.0"},
	}
	c.ApplyDefaults()
	return c
}

func TestApplyDefaults(t *testing.T) {
	c := &Config{}
	c.ApplyDefaults()
	if c.Worker.GPUResourceName != "nvidia.com/gpu" {
		t.Errorf("GPUResourceName default = %q", c.Worker.GPUResourceName)
	}
	if c.Worker.DefaultWorkers != 1 || c.Worker.DefaultCPUs != 1 || c.Worker.DefaultMemoryMB != 1024 {
		t.Errorf("worker defaults = %d/%d/%d", c.Worker.DefaultWorkers, c.Worker.DefaultCPUs, c.Worker.DefaultMemoryMB)
	}
}

func TestPriorityClassFor(t *testing.T) {
	c := validConfig()
	if pc, ok := c.PriorityClassFor("batch"); !ok || pc != "normal-priority" {
		t.Errorf("PriorityClassFor(batch) = %q, %v", pc, ok)
	}
	if _, ok := c.PriorityClassFor("unknown"); ok {
		t.Errorf("PriorityClassFor(unknown) should report unmanaged")
	}
}

func TestLocalQueueForPrecedence(t *testing.T) {
	c := validConfig()
	// job override wins over everything
	if got := c.LocalQueueFor("hi", "override"); got != "override" {
		t.Errorf("job override lost: %q", got)
	}
	// pool override wins over global
	if got := c.LocalQueueFor("hi", ""); got != "team-a" {
		t.Errorf("pool override lost: %q", got)
	}
	// falls back to global
	if got := c.LocalQueueFor("batch", ""); got != "main" {
		t.Errorf("global default lost: %q", got)
	}
}

func TestHeadAddressFor(t *testing.T) {
	c := validConfig()
	if addr, ok := c.HeadAddressFor("shared"); !ok || addr != "shared-head-svc.ray:6379" {
		t.Errorf("HeadAddressFor(shared) = %q, %v", addr, ok)
	}
	if _, ok := c.HeadAddressFor("nope"); ok {
		t.Errorf("HeadAddressFor(nope) should report unmanaged")
	}
}

func TestValidate(t *testing.T) {
	if err := validConfig().Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	cases := map[string]func(*Config){
		"no namespace":     func(c *Config) { c.Namespace = "" },
		"no localQueue":    func(c *Config) { c.LocalQueue = "" },
		"no clusters":      func(c *Config) { c.ManagedClusters = nil },
		"cluster no addr":  func(c *Config) { c.ManagedClusters[0].HeadAddress = "" },
		"no pools":         func(c *Config) { c.PoolMappings = nil },
		"pool no priority": func(c *Config) { c.PoolMappings[0].WorkloadPriorityClass = "" },
		"no image":         func(c *Config) { c.Worker.Image = "" },
		// Negative worker-shape defaults survive ApplyDefaults (it only fixes
		// exact zeros), so Validate must reject them instead of leaving them to
		// downstream per-job clamps.
		"negative defaultWorkers":  func(c *Config) { c.Worker.DefaultWorkers = -1 },
		"negative defaultCpus":     func(c *Config) { c.Worker.DefaultCPUs = -4 },
		"negative defaultMemoryMB": func(c *Config) { c.Worker.DefaultMemoryMB = -1024 },
	}
	for name, mutate := range cases {
		c := validConfig()
		mutate(c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: expected validation error", name)
		}
	}
}

// TestValidateNegativeDefaultsMessageNamesField pins that the negative-default
// rejections are actionable: each error message names the offending config
// field, so the operator can fix the file without reading source code.
func TestValidateNegativeDefaultsMessageNamesField(t *testing.T) {
	cases := map[string]func(*Config){
		"worker.defaultWorkers":  func(c *Config) { c.Worker.DefaultWorkers = -1 },
		"worker.defaultCpus":     func(c *Config) { c.Worker.DefaultCPUs = -2 },
		"worker.defaultMemoryMB": func(c *Config) { c.Worker.DefaultMemoryMB = -512 },
	}
	for field, mutate := range cases {
		c := validConfig()
		mutate(c)
		err := c.Validate()
		if err == nil {
			t.Errorf("%s: negative value should be rejected", field)
			continue
		}
		if !strings.Contains(err.Error(), field) {
			t.Errorf("%s: error %q should name the field", field, err)
		}
	}
}

func writeTemp(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "ray-config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	return p
}

func TestLoad(t *testing.T) {
	const valid = `
namespace: ray
localQueue: main
managedClusters:
  - name: shared
    headAddress: shared-head-svc.ray:6379
poolMappings:
  - pool: batch
    workloadPriorityClass: normal-priority
worker:
  image: rayproject/ray:2.9.0
`
	c, err := Load(writeTemp(t, valid))
	if err != nil {
		t.Fatalf("Load(valid): %v", err)
	}
	if c.Worker.GPUResourceName != "nvidia.com/gpu" {
		t.Errorf("defaults not applied on Load: %q", c.Worker.GPUResourceName)
	}
	if pc, ok := c.PriorityClassFor("batch"); !ok || pc != "normal-priority" {
		t.Errorf("loaded mapping wrong: %q %v", pc, ok)
	}

	if _, err := Load(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Errorf("Load(missing file) should error")
	}
	if _, err := Load(writeTemp(t, "namespace: ray\nunknownField: x\n")); err == nil {
		t.Errorf("Load(strict-unknown-field) should error")
	}
	if _, err := Load(writeTemp(t, "namespace: ray\nlocalQueue: main\n")); err == nil {
		t.Errorf("Load(invalid: no clusters) should error")
	}
}
