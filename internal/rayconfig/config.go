// Package rayconfig loads the ray-bridge configuration. It deliberately
// mirrors the shape of internal/config (the Slurm-side WorkloadMixing config)
// so the two bridges feel the same to an operator and can later share a CR:
// a namespace + LocalQueue, a list of pool->WorkloadPriorityClass mappings
// (the partition-mapping analog), and the worker pod spec (the slurmd analog).
//
// The Ray side is watch-driven (ADR-0012), so unlike internal/config there is
// no pollInterval — a RayJob is a native, watchable object.
package rayconfig

import (
	"fmt"
	"os"
	"strings"

	"sigs.k8s.io/yaml"
)

// ManagedCluster names a shared RayCluster under bridge management and the
// address its dedicated workers dial with `ray start --address`. The Name must
// match the value inner RayJobs put in clusterSelector[ray.io/cluster].
type ManagedCluster struct {
	Name string `json:"name"`
	// HeadAddress is the Ray head's GCS address, host:port
	// (e.g. "raycluster-shared-head-svc.ray:6379"), used verbatim in the
	// worker's `ray start --address=<HeadAddress>`.
	HeadAddress string `json:"headAddress"`
}

// PoolMapping ties an inner RayJob's pool (the ray-bridge.x-k8s.io/pool
// annotation) to a Kueue WorkloadPriorityClass — the exact analog of the
// Slurm side's PartitionMapping.
type PoolMapping struct {
	Pool                  string `json:"pool"`
	WorkloadPriorityClass string `json:"workloadPriorityClass"`
	// LocalQueue overrides the global Config.LocalQueue for worker JobSets of
	// this pool (multi-team support), matching PartitionMapping.LocalQueue.
	LocalQueue string `json:"localQueue,omitempty"`
}

// Worker describes the dedicated Ray worker pods the bridge stands up per
// inner workload — the slurmd analog. Image is the container that runs
// `ray start`; the default* fields size a worker when the RayJob omits the
// worker-shape annotations.
type Worker struct {
	Image string `json:"image"`
	// GPUResourceName is the Kubernetes extended resource requested when an
	// inner workload asks for GPUs. Defaults to "nvidia.com/gpu".
	GPUResourceName string `json:"gpuResourceName,omitempty"`
	// DefaultWorkers / DefaultCPUs / DefaultMemoryMB size a worker set when the
	// RayJob carries no worker-shape annotations. Sensible small defaults keep
	// an un-annotated job admissible rather than rejected.
	DefaultWorkers  int32 `json:"defaultWorkers,omitempty"`
	DefaultCPUs     int64 `json:"defaultCpus,omitempty"`
	DefaultMemoryMB int64 `json:"defaultMemoryMB,omitempty"`
	// Privileged is false by default: a Ray worker, unlike slurmd, does not
	// need a privileged container. Exposed for parity/escape-hatch only.
	Privileged bool `json:"privileged,omitempty"`
	// PreferredTopology, when set, is applied as a Kueue TAS
	// podset-preferred-topology (best-effort gang locality) to every worker
	// JobSet that does not request a hard topology via the RayJob's
	// required-topology annotation (R8, mirrors the Slurm side's
	// Topology.PreferredLevel, ADR-0008). Empty disables it.
	PreferredTopology string `json:"preferredTopology,omitempty"`
}

type Config struct {
	// Namespace where the bridge watches inner RayJobs and creates worker
	// JobSets.
	Namespace string `json:"namespace"`
	// LocalQueue is the Kueue queue worker JobSets are submitted to (global
	// default; a PoolMapping or the RayJob's local-queue annotation may override).
	LocalQueue string `json:"localQueue"`

	ManagedClusters []ManagedCluster `json:"managedClusters"`
	PoolMappings    []PoolMapping    `json:"poolMappings"`
	Worker          Worker           `json:"worker"`
}

// PriorityClassFor returns the WorkloadPriorityClass for a pool and whether the
// pool is managed at all (the second return is the "is this in scope" gate,
// mirroring config.PriorityClassFor).
func (c *Config) PriorityClassFor(pool string) (string, bool) {
	for _, m := range c.PoolMappings {
		if m.Pool == pool {
			return m.WorkloadPriorityClass, true
		}
	}
	return "", false
}

// LocalQueueFor resolves the effective Kueue LocalQueue: an explicit per-job
// override wins, then the pool's override, then the global default.
func (c *Config) LocalQueueFor(pool, jobOverride string) string {
	if jobOverride != "" {
		return jobOverride
	}
	for _, m := range c.PoolMappings {
		if m.Pool == pool && m.LocalQueue != "" {
			return m.LocalQueue
		}
	}
	return c.LocalQueue
}

// HeadAddressFor returns the Ray head address for a managed cluster and whether
// that cluster is managed. An inner RayJob whose target cluster is not managed
// is out of scope (the analog of a Slurm job on an unmapped partition).
func (c *Config) HeadAddressFor(cluster string) (string, bool) {
	for _, m := range c.ManagedClusters {
		if m.Name == cluster {
			return m.HeadAddress, true
		}
	}
	return "", false
}

// ApplyDefaults fills optional fields, matching config.ApplyDefaults's role as
// the single source of truth for defaults shared by every loader.
func (c *Config) ApplyDefaults() {
	if c.Worker.GPUResourceName == "" {
		c.Worker.GPUResourceName = "nvidia.com/gpu"
	}
	if c.Worker.DefaultWorkers == 0 {
		c.Worker.DefaultWorkers = 1
	}
	if c.Worker.DefaultCPUs == 0 {
		c.Worker.DefaultCPUs = 1
	}
	if c.Worker.DefaultMemoryMB == 0 {
		c.Worker.DefaultMemoryMB = 1024
	}
}

// Validate enforces the invariants a usable config must satisfy. Runs after
// ApplyDefaults.
func (c *Config) Validate() error {
	if c.Namespace == "" || c.LocalQueue == "" {
		return fmt.Errorf("namespace and localQueue are required")
	}
	if len(c.ManagedClusters) == 0 {
		return fmt.Errorf("at least one managedClusters entry is required")
	}
	for i, m := range c.ManagedClusters {
		if strings.TrimSpace(m.Name) == "" || strings.TrimSpace(m.HeadAddress) == "" {
			return fmt.Errorf("managedClusters[%d]: name and headAddress are required", i)
		}
	}
	if len(c.PoolMappings) == 0 {
		return fmt.Errorf("at least one poolMappings entry is required")
	}
	for i, m := range c.PoolMappings {
		if strings.TrimSpace(m.Pool) == "" || strings.TrimSpace(m.WorkloadPriorityClass) == "" {
			return fmt.Errorf("poolMappings[%d]: pool and workloadPriorityClass are required", i)
		}
	}
	if strings.TrimSpace(c.Worker.Image) == "" {
		return fmt.Errorf("worker.image is required")
	}
	// Negative worker-shape defaults: ApplyDefaults only replaces EXACT zeros,
	// so a negative value written in the config file survives it and would
	// otherwise ride on downstream per-job clamps (ray.FromUnstructured raises
	// workers/cpus to 1) — silently sizing capacity differently from what the
	// operator wrote. Reject them here so the typo surfaces at startup with the
	// field named, the same fail-at-load posture as every other check above.
	if c.Worker.DefaultWorkers < 0 {
		return fmt.Errorf("worker.defaultWorkers must not be negative (got %d)", c.Worker.DefaultWorkers)
	}
	if c.Worker.DefaultCPUs < 0 {
		return fmt.Errorf("worker.defaultCpus must not be negative (got %d)", c.Worker.DefaultCPUs)
	}
	if c.Worker.DefaultMemoryMB < 0 {
		return fmt.Errorf("worker.defaultMemoryMB must not be negative (got %d)", c.Worker.DefaultMemoryMB)
	}
	return nil
}

// Load reads, defaults, and validates a ray-bridge config file.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}
	cfg := &Config{}
	if err := yaml.UnmarshalStrict(raw, cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}
