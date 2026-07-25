// Package config loads the bridge configuration from a YAML file.
//
// Prototype note (ADR-0004): the MVD specifies a WorkloadMixing CustomResource
// as the configuration surface. This prototype uses a plain file to keep the
// first iteration small; the schema deliberately mirrors the CR so promoting
// it later is mechanical.
package config

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"sigs.k8s.io/yaml"
)

// minPollInterval guards against a config that turns the reconcile loop into a
// hot loop hammering the Slurm REST API (a self-inflicted DoS). Anything below
// this is rejected; the default when unset is 10s.
const minPollInterval = time.Second

// Duration wraps time.Duration so YAML values like "10s" parse naturally
// (encoding/json cannot unmarshal a string into time.Duration).
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalJSON(raw []byte) error {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	d.Duration = parsed
	return nil
}

// PartitionMapping ties a Slurm partition to a Kueue WorkloadPriorityClass,
// the unified priority model both schedulers agree on.
type PartitionMapping struct {
	PartitionName         string `json:"partitionName"`
	WorkloadPriorityClass string `json:"workloadPriorityClass"`
	// LocalQueue overrides the top-level Config.LocalQueue for JobSets
	// translated from this partition (backlog A1b: multi-team support — each
	// partition can target its own Kueue LocalQueue instead of sharing one
	// global queue). Empty falls back to the global LocalQueue.
	LocalQueue string `json:"localQueue,omitempty"`
}

// Slurmd describes the pods that will register as dynamic Slurm nodes.
type Slurmd struct {
	Image string `json:"image"`
	// ConfServer is the slurmctld address slurmd registers against,
	// e.g. "slurm-controller.slurm.svc.cluster.local:6817".
	ConfServer string `json:"confServer"`
	// AuthSecretName is the Secret (in Namespace, below) holding slurm.key.
	AuthSecretName string `json:"authSecretName"`
	// GPUResourceName is the Kubernetes extended resource requested when a
	// Slurm job asks for GRES GPUs. Defaults to "nvidia.com/gpu".
	GPUResourceName string `json:"gpuResourceName,omitempty"`
	// Privileged controls the slurmd container's security context. slurmd
	// needs writable cgroups and dies unprivileged; the upstream slurm-operator
	// runs its own nodeset pods privileged for the same reason, so this is an
	// upstream (slurmd/slinky) constraint, not one k8s-bridge can lift alone.
	// Defaults to true. Setting it false switches to a minimal-capability
	// context (SYS_ADMIN etc.) so operators can trial an unprivileged slurmd
	// image without a code change — see the threat model (docs/reference/threat-model.md).
	Privileged *bool `json:"privileged,omitempty"`
	// SharedStorage, when set, mounts a shared filesystem (e.g. /home) into
	// every slurmd pod so job scripts and data resolve like on a static
	// cluster (MVP requirement).
	SharedStorage *SharedStorage `json:"sharedStorage,omitempty"`
}

// PrivilegedOrDefault reports whether slurmd should run privileged, defaulting
// to true when the field is unset.
func (s *Slurmd) PrivilegedOrDefault() bool {
	return s.Privileged == nil || *s.Privileged
}

// SharedStorage describes an NFS export mounted into slurmd pods. NFS was
// chosen for the prototype because it needs no CSI driver and matches the
// /home semantics of classic HPC clusters; production may swap in Filestore.
type SharedStorage struct {
	NFSServer string `json:"nfsServer"`
	NFSPath   string `json:"nfsPath"`
	MountPath string `json:"mountPath"`
}

// Topology controls translation of Slurm topology requests into Kueue
// Topology-Aware Scheduling (TAS) annotations. Levels are node label keys
// registered in the cluster's Topology CR (e.g. "example.com/rack").
type Topology struct {
	// RequiredLevel is applied as podset-required-topology when a Slurm job
	// requests switch locality (--switches=N). Empty disables translation.
	RequiredLevel string `json:"requiredLevel,omitempty"`
	// PreferredLevel is applied as podset-preferred-topology to every other
	// bridge job (best-effort gang locality). Empty disables it.
	PreferredLevel string `json:"preferredLevel,omitempty"`
}

type Config struct {
	// SlurmRestURL is the slurmrestd base URL, e.g. "https://slurm-restapi.slurm:6820".
	SlurmRestURL string `json:"slurmRestURL"`
	// SlurmTokenFile contains a JWT for slurmrestd (mounted Secret).
	SlurmTokenFile string `json:"slurmTokenFile"`
	// SlurmUser is sent as X-SLURM-USER-NAME alongside the token. It MUST be a
	// REAL Slurm user: slurmrestd rejects EVERY job update (comment AND
	// release) with 422 "Invalid user id" (error 2010) if the name is unknown
	// — found live in e2e testing. Empty is the safe default: the client then
	// omits the header entirely (see internal/slurm newRequest) and slurmrestd
	// uses the user the JWT was minted for. Set this only to a dedicated
	// low-privilege Slurm user you have provisioned.
	SlurmUser string `json:"slurmUser"`
	// AllowInsecureHTTP must be set explicitly to permit a plaintext http://
	// SlurmRestURL. The Slurm JWT is bearer-equivalent, so cleartext transport
	// leaks a credential that controls the whole Slurm side; keep this false
	// outside local development.
	AllowInsecureHTTP bool `json:"allowInsecureHTTP,omitempty"`
	// SlurmCACertFile, when set, is the PEM CA bundle used to verify the
	// slurmrestd TLS certificate (mounted Secret/ConfigMap). Empty uses the
	// system roots.
	SlurmCACertFile string `json:"slurmCACertFile,omitempty"`
	// SlurmInsecureSkipTLSVerify disables slurmrestd certificate verification.
	// Development escape hatch only — it defeats the point of TLS.
	SlurmInsecureSkipTLSVerify bool `json:"slurmInsecureSkipTLSVerify,omitempty"`
	// SlurmRequestTimeout bounds each slurmrestd HTTP request, INCLUDING
	// reading the whole response body. Defaults to 30s. The GET /jobs payload
	// grows with total queue depth (v0.0.44 has no server-side paging — see
	// internal/slurm ListJobs); at tens of thousands of queued jobs a
	// slurmrestd under memory pressure can need longer than the default to
	// stream the body out (suite-E scale run, 2026-07). Raise this for
	// deliberate large-backlog deployments rather than disabling the bound.
	SlurmRequestTimeout Duration `json:"slurmRequestTimeout,omitempty"`
	// SlurmRequestsPerSecond is a client-side token-bucket rate limit on ALL
	// requests the bridge sends to slurmrestd (A1). 0 (the default) means
	// unlimited — the historic behavior. Scale testing (suite E, 2026-07)
	// showed slurmrestd is the fragile dependency in the whole chain: it
	// shares slurmctld's internal lock and has no throttling of its own, so
	// a bridge burst (parallel JobSet creates, comment storms, watch-nudge
	// churn) can starve the scheduler itself. The Kubernetes side has had an
	// explicit QPS/Burst ceiling since audit P8b; this is its Slurm-side
	// counterpart. Burst is derived as 2x the rate (min 1) — see
	// internal/slurm burstFor. Validated to [0, 10000]; the upper bound is a
	// sanity rail (a value that high is indistinguishable from unlimited for
	// any real slurmrestd, so asking for more is almost certainly a typo).
	SlurmRequestsPerSecond float64 `json:"slurmRequestsPerSecond,omitempty"`

	// Namespace where slurmd JobSets are created.
	Namespace string `json:"namespace"`
	// LocalQueue is the Kueue queue JobSets are submitted to.
	LocalQueue string `json:"localQueue"`

	PartitionMappings []PartitionMapping `json:"partitionMappings"`
	Slurmd            Slurmd             `json:"slurmd"`
	Topology          Topology           `json:"topology,omitempty"`

	PollInterval Duration `json:"pollInterval,omitempty"`
	// EnablePrioritySync gates the experimental Slurm<->Workload priority
	// mirror. Default OFF: live testing showed Slurm recomputes the field
	// and 0 means hold (ADR-0009); kubectl-side Workload patching is the
	// supported mutation path until the lua-directive channel lands.
	EnablePrioritySync bool `json:"enablePrioritySync,omitempty"`
	// MaxUserPriority caps the priority a user-originated directive (ADR-0009
	// admin_comment "wm:prio=N") or a Slurm-side change may set on a Kueue
	// Workload. Zero disables the extra cap (values are still bounded to the
	// uint32 range). Set this so a job owner cannot jump the whole mixed queue
	// ahead of other tenants by requesting an arbitrarily high priority.
	MaxUserPriority int32 `json:"maxUserPriority,omitempty"`
	// CancelOrphanedJobs gates cancelling a released Slurm job whose JobSet has
	// disappeared (D2). Default OFF: the action is irreversible (scancel) and
	// keys off the ABSENCE of a Kubernetes object, so any read fault — a
	// mis-scoped informer cache, a partial LIST, RBAC losing access to the
	// workload namespace — looks exactly like "every job is orphaned". Operators
	// opt in once they trust the deployment's cache scope.
	CancelOrphanedJobs bool `json:"cancelOrphanedJobs,omitempty"`
	// OrphanGraceTicks is how many CONSECUTIVE ticks a job must be seen without
	// its JobSet before it is cancelled. The snapshot is served from the
	// informer cache, so a JobSet created moments ago may not appear in it yet;
	// requiring several observations spans that lag. Defaults to 3.
	OrphanGraceTicks int `json:"orphanGraceTicks,omitempty"`
	// DrainOnPreemption gates ADR-0016 proactive node drain: when Kueue evicts
	// an admitted, already-released job (preemption / cohort reclamation), the
	// bridge deletes that job's dynamic Slurm node records immediately instead
	// of waiting out SlurmdTimeout, so slurmctld frees and requeues the job at
	// once. The records re-register under the same deterministic names when
	// Kueue re-admits the JobSet. Default OFF: it keys off a Kueue eviction
	// signal and issues DeleteNode, so a misread eviction requeues a healthy job
	// (recoverable — one requeue cycle — but opt-in like CancelOrphanedJobs).
	// SlurmdTimeout remains the backstop for evictions the bridge cannot see
	// (bridge down during the eviction, a node lost without a Kueue event).
	DrainOnPreemption bool `json:"drainOnPreemption,omitempty"`
	// CreateWorkers bounds the worker pool used to parallelize ensureJobSet
	// calls across held jobs (P5: found sequential creates
	// dominating the first tick of a burst). Defaults to 8. The pool's
	// concurrency is bounded on purpose: main.go already sets an explicit
	// client-go QPS/Burst ceiling (audit P8b), and this pool shares that same
	// rate-limited client, so it cannot exceed it regardless of worker count.
	CreateWorkers int `json:"createWorkers,omitempty"`

	// WorkloadMixingName is the name of the WorkloadMixing CR this config was
	// derived from, set PROGRAMMATICALLY by the multi-CR supervisor
	// (ADR-0015 Phase A) — never by the file loader or the CR spec, hence
	// `json:"-"`. When set, translate.ToJobSet stamps it as the
	// WorkloadMixingLabel on every created JobSet and bridge.snapshot selects
	// only JobSets carrying it, so per-CR Bridges sharing one namespace never
	// claim (or clean up) each other's JobSets. Empty in single-CR and file
	// modes, which keeps their historic managed-by-only ownership unchanged.
	WorkloadMixingName string `json:"-"`
}

// PriorityClassFor returns the WorkloadPriorityClass for a partition and
// whether the partition is under workload-mixing management at all.
func (c *Config) PriorityClassFor(partition string) (string, bool) {
	for _, m := range c.PartitionMappings {
		if m.PartitionName == partition {
			return m.WorkloadPriorityClass, true
		}
	}
	return "", false
}

// LocalQueueFor returns the effective Kueue LocalQueue for a partition
// (backlog A1b): the partition's own LocalQueue override when set, otherwise
// the global Config.LocalQueue. Callers should only invoke this for
// partitions already confirmed managed (PriorityClassFor's second return);
// an unmapped partition simply falls back to the global queue here too.
func (c *Config) LocalQueueFor(partition string) string {
	for _, m := range c.PartitionMappings {
		if m.PartitionName == partition && m.LocalQueue != "" {
			return m.LocalQueue
		}
	}
	return c.LocalQueue
}

// ApplyDefaults fills in optional fields the loader leaves empty. Exported so
// both the file loader and the CR loader (crdconfig.go) share one source of
// truth for defaults and validation.
func (c *Config) ApplyDefaults() {
	if c.PollInterval.Duration == 0 {
		c.PollInterval.Duration = 10 * time.Second
	}
	if c.Slurmd.GPUResourceName == "" {
		c.Slurmd.GPUResourceName = "nvidia.com/gpu"
	}
	if c.CreateWorkers == 0 {
		c.CreateWorkers = defaultCreateWorkers
	}
	// Only the ZERO value (unset/omitted) takes the default. A negative
	// value used to be silently coerced to 3 here too (A4), which turned an
	// obvious config mistake into a surprising grace period on the one knob
	// that gates an irreversible scancel; Validate now rejects it instead.
	if c.OrphanGraceTicks == 0 {
		c.OrphanGraceTicks = defaultOrphanGraceTicks
	}
	if c.SlurmRequestTimeout.Duration == 0 {
		c.SlurmRequestTimeout.Duration = defaultSlurmRequestTimeout
	}
}

// defaultSlurmRequestTimeout matches the previous hardcoded http.Client
// timeout in internal/slurm, kept as the default so existing deployments see
// no behavior change.
const defaultSlurmRequestTimeout = 30 * time.Second

// defaultOrphanGraceTicks is how many consecutive ticks a job must look
// orphaned before CancelOrphanedJobs acts on it. Three ticks comfortably
// outlast informer-cache lag after a JobSet create without letting a genuinely
// orphaned job linger for long.
const defaultOrphanGraceTicks = 3

// maxSlurmRequestsPerSecond is the validation ceiling for
// slurmRequestsPerSecond, mirrored in the CRD schema's `maximum` (all three
// copies — deploy/crd, the chart's crds/, and test/crd). Anything near this
// value is effectively unlimited against a real slurmrestd, so a larger
// number is treated as a typo rather than a tuning choice.
const maxSlurmRequestsPerSecond = 10000

// defaultCreateWorkers is the P5 worker-pool size used when createWorkers is
// unset: enough to overlap several in-flight Create calls during a burst
// without opening so many goroutines that the shared, already rate-limited
// (QPS/Burst, audit P8b) Kubernetes client becomes the only thing that
// matters anyway.
const defaultCreateWorkers = 8

// Validate enforces the invariants both config sources must satisfy. It runs
// after applyDefaults so a defaulted PollInterval passes.
func (c *Config) Validate() error {
	if c.SlurmRestURL == "" || c.Namespace == "" || c.LocalQueue == "" {
		return fmt.Errorf("slurmRestURL, namespace and localQueue are required")
	}
	if len(c.PartitionMappings) == 0 {
		return fmt.Errorf("at least one partition mapping is required")
	}
	// A4, mirroring rayconfig.Validate's per-mapping checks: an empty
	// partitionName can never match a job (dead config), an empty
	// workloadPriorityClass produces JobSets Kueue rejects, and a DUPLICATE
	// partitionName silently shadows the later entry (PriorityClassFor and
	// LocalQueueFor are both first-match-wins) — the operator who edited the
	// second copy would see their change ignored with no signal anywhere.
	seenPartitions := make(map[string]struct{}, len(c.PartitionMappings))
	for i, m := range c.PartitionMappings {
		if strings.TrimSpace(m.PartitionName) == "" || strings.TrimSpace(m.WorkloadPriorityClass) == "" {
			return fmt.Errorf("partitionMappings[%d]: partitionName and workloadPriorityClass are required", i)
		}
		if _, dup := seenPartitions[m.PartitionName]; dup {
			return fmt.Errorf("partitionMappings[%d]: duplicate partitionName %q (mappings are first-match-wins, so the duplicate would be silently ignored)", i, m.PartitionName)
		}
		seenPartitions[m.PartitionName] = struct{}{}
	}
	u, err := url.Parse(c.SlurmRestURL)
	if err != nil {
		return fmt.Errorf("invalid slurmRestURL %q: %w", c.SlurmRestURL, err)
	}
	switch u.Scheme {
	case "https":
	case "http":
		if !c.AllowInsecureHTTP {
			return fmt.Errorf("slurmRestURL uses plaintext http (%q): the Slurm token would travel in cleartext; use https or set allowInsecureHTTP: true for local development only", c.SlurmRestURL)
		}
	default:
		return fmt.Errorf("slurmRestURL must be http or https, got %q", u.Scheme)
	}
	if c.PollInterval.Duration < minPollInterval {
		return fmt.Errorf("pollInterval %s is below the minimum %s", c.PollInterval.Duration, minPollInterval)
	}
	if strings.TrimSpace(c.Slurmd.Image) == "" {
		return fmt.Errorf("slurmd.image is required")
	}
	// The CRD schema marks authSecretName required; keep file mode in sync
	// (the envtest schema-drift guard caught the mismatch — audit follow-up).
	if strings.TrimSpace(c.Slurmd.AuthSecretName) == "" {
		return fmt.Errorf("slurmd.authSecretName is required (Secret holding slurm.key)")
	}
	// The int32 type is itself the upper bound (A4): the CRD's maximum was
	// lowered from 4294967294 to 2147483647 (math.MaxInt32) to match, since
	// a CR value above int32 range never reached this check anyway — the
	// strict JSON decode in LoadConfigFromCR fails on the overflow first,
	// with a decoder error instead of a validation message. Keeping the Go
	// field at int32 (not widening to int64 + a range check) is deliberate:
	// the value feeds capUserPriority's int64 comparison and ultimately a
	// Kueue Workload spec.priority, which is itself an int32.
	if c.MaxUserPriority < 0 {
		return fmt.Errorf("maxUserPriority must not be negative")
	}
	if c.CreateWorkers < 0 {
		return fmt.Errorf("createWorkers must not be negative")
	}
	// A4: ApplyDefaults no longer coerces a negative value to the default;
	// an explicit negative grace on the knob gating an irreversible scancel
	// is a config error the operator must see, not a request for "3".
	if c.OrphanGraceTicks < 0 {
		return fmt.Errorf("orphanGraceTicks must not be negative")
	}
	// A1: 0 = unlimited; the 10000 ceiling is a typo rail, not a tuning
	// suggestion — see the SlurmRequestsPerSecond field doc.
	if c.SlurmRequestsPerSecond < 0 || c.SlurmRequestsPerSecond > maxSlurmRequestsPerSecond {
		return fmt.Errorf("slurmRequestsPerSecond %v must be between 0 (unlimited) and %d", c.SlurmRequestsPerSecond, maxSlurmRequestsPerSecond)
	}
	// Lower bound guards against a sub-second timeout that would abort every
	// large /jobs read; the upper bound keeps a wedged slurmrestd from
	// pinning a tick (and the poll loop behind it) for longer than any
	// sensible poll cadence.
	if c.SlurmRequestTimeout.Duration < time.Second || c.SlurmRequestTimeout.Duration > 10*time.Minute {
		return fmt.Errorf("slurmRequestTimeout %s must be between 1s and 10m", c.SlurmRequestTimeout.Duration)
	}
	return nil
}

// ValidateImageAllowed reports whether image matches one of the allowed
// prefixes. An empty allowlist is treated as "allow all" by the caller, so this
// is only called when the platform admin actually configured one. Matching is
// by prefix to permit either an exact ref or a registry/repository lock such as
// "ghcr.io/slinkyproject/".
func ValidateImageAllowed(image string, allowed []string) error {
	for _, prefix := range allowed {
		if strings.HasPrefix(image, prefix) {
			return nil
		}
	}
	return fmt.Errorf("slurmd image %q is not in the allowlist %v", image, allowed)
}

// ValidateFilePathAllowed reports whether a CR-supplied file path is inside one
// of the allowed directory prefixes (security audit H2).
//
// Why this exists: the controller reads slurmTokenFile and slurmCACertFile from
// its OWN filesystem and sends the token's contents to slurmRestURL as the
// X-SLURM-USER-TOKEN header. In supervisor mode (ADR-0015) every WorkloadMixing
// CR in the namespace is adopted, so an unrestricted path would let a CR author
// name any file the controller process can read — including its own
// ServiceAccount token — and a slurmRestURL they control to receive it. Both
// fields are therefore vetted against a deploy-time allowlist, exactly like the
// slurmd image (C1): a trust anchor the CR author cannot widen.
//
// Two hardening details that a naive prefix check would get wrong:
//
//  1. The path is Clean()ed BEFORE matching, so "/var/run/secrets/../../etc/
//     shadow" cannot slip through by carrying its escape inside an allowed
//     prefix. A relative path is rejected outright — it would resolve against
//     the controller's working directory, which is not a stable trust base.
//  2. Matching is at a path BOUNDARY, not a raw string prefix. Allowing
//     "/var/run/secrets" must not also allow "/var/run/secretsomething-else";
//     the candidate has to be the prefix itself or live beneath it. Prefixes
//     are accepted with or without a trailing separator so operators do not
//     have to remember which form the flag wants.
func ValidateFilePathAllowed(field, path string, allowed []string) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("%s %q must be an absolute path (allowlist %v)", field, path, allowed)
	}
	clean := filepath.Clean(path)
	for _, prefix := range allowed {
		if prefix == "" {
			continue
		}
		cleanPrefix := filepath.Clean(prefix)
		if clean == cleanPrefix || strings.HasPrefix(clean, cleanPrefix+string(filepath.Separator)) {
			return nil
		}
	}
	return fmt.Errorf("%s %q is not under any allowed path prefix %v", field, clean, allowed)
}

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
