// Multi-CR supervisor (ADR-0015 Phase A): one controller process reconciles
// EVERY WorkloadMixing object in its own namespace, running one bridge.Bridge
// — its own poll loop, Slurm client and health state — per CR, started and
// stopped as CRs come and go. The single-CR binding (--workloadmixing) and
// file mode bypass this file entirely and behave exactly as before.
//
// Crash isolation: Bridge.Run never returns except on context cancellation —
// a failing tick is logged, counted and retried with capped exponential
// backoff inside the loop (see Run) — so one CR's wedged or erroring Slurm
// cluster can never take another CR's loop down; each loop also polls on its
// own cadence, so a slow slurmrestd only slows ITS OWN CR. A panic inside a
// tick is deliberately NOT recovered (same stance as single-CR mode): it is
// a bridge bug, and hiding it behind a recover would trade a truthful pod
// restart for silent per-CR corruption.
//
// Observability note (ADR-0015 §Decision 4, DEFERRED): the k8s_bridge_*
// metrics do not yet carry a per-CR `workloadmixing` label — that change
// re-shapes every package-level metric into a labeled vector and ripples
// into dashboards and alert rules, so it ships as its own follow-up. Until
// then all per-CR Bridges report into the same process-global series, which
// therefore read as AGGREGATES across CRs (and
// k8s_bridge_last_successful_tick_timestamp reflects the most recent success
// of ANY Bridge — alert on the per-CR Ready conditions, not on that gauge,
// when several CRs are live). Per-CR readiness IS already exposed: on the
// CRs' status.conditions and in the /readyz body (see ReadyzHandler).
package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/mrozacki/k8s-bridge/api/v1alpha1"
	"github.com/mrozacki/k8s-bridge/internal/config"
)

// SlurmClientFactory builds the per-CR Slurm client from a fully defaulted
// and validated config. The production factory (cmd/k8s-bridge/main.go)
// wraps slurm.NewClient plus its metrics wiring; tests substitute fakes.
type SlurmClientFactory func(cfg *config.Config) (SlurmAPI, error)

// defaultConflictRetryInterval is how often a CR blocked on ConflictingSpec
// or StartFailed is re-examined. Blocked CRs receive no watch events of
// their own when the CONFLICTING CR goes away, so a timer is what lets
// "delete the older CR and the newer one starts" work without a cross-CR
// enqueue graph. Short enough to feel responsive, long enough to stay
// negligible API traffic.
const defaultConflictRetryInterval = 10 * time.Second

// startupRetryInterval is the requeue used when a reconcile arrives before
// Start has captured the leader-elected context (the watch controller and
// this Runnable start concurrently in the manager's leader-election group).
const startupRetryInterval = 200 * time.Millisecond

// endpointIdentity captures every config field that is baked into the
// slurm.Client at CONSTRUCTION time (see slurm.Options and the A1 note in
// main.go: request timeout and rate limit are read once, not hot-reloaded).
// This is the supervisor's restart rule: a spec change that leaves the
// endpointIdentity intact is applied via the existing per-Bridge hot-reload
// (setCfg — poll interval, partition mappings, orphan knobs etc. all take
// effect next tick); a change to ANY of these fields cannot take effect
// without a new client, so the supervisor restarts that CR's Bridge instead
// (cancel the loop, build a fresh client, start a new loop). Single-CR mode
// has the same limitation, where it requires a controller restart.
type endpointIdentity struct {
	url                string
	user               string
	tokenFile          string
	caCertFile         string
	insecureSkipVerify bool
	requestTimeout     time.Duration
	requestsPerSecond  float64
}

func endpointIdentityFor(cfg *config.Config) endpointIdentity {
	return endpointIdentity{
		url:                cfg.SlurmRestURL,
		user:               cfg.SlurmUser,
		tokenFile:          cfg.SlurmTokenFile,
		caCertFile:         cfg.SlurmCACertFile,
		insecureSkipVerify: cfg.SlurmInsecureSkipTLSVerify,
		requestTimeout:     cfg.SlurmRequestTimeout.Duration,
		requestsPerSecond:  cfg.SlurmRequestsPerSecond,
	}
}

// bridgeIdentity is what the conflict rule (ADR-0015 §Decision 3) judges
// uniqueness on: the Slurm endpoint plus the set of managed partitions. Two
// CRs naming the same slurmRestURL AND at least one shared partition would
// double-manage those jobs (double JobSets, double releases, double
// cancels), so the second one is refused. URLs are compared as EXACT
// strings, deliberately unnormalized: two spellings of the same endpoint
// (trailing slash, IP vs hostname) evade the guard, and papering over that
// with best-effort normalization would only move the line while making it
// fuzzier — the guard is a fail-loud misconfiguration check, not a security
// boundary.
type bridgeIdentity struct {
	endpoint   endpointIdentity
	partitions map[string]struct{}
}

func identityFor(cfg *config.Config) bridgeIdentity {
	parts := make(map[string]struct{}, len(cfg.PartitionMappings))
	for _, m := range cfg.PartitionMappings {
		parts[m.PartitionName] = struct{}{}
	}
	return bridgeIdentity{endpoint: endpointIdentityFor(cfg), partitions: parts}
}

// conflictsWith reports whether two identities would double-manage jobs:
// same slurmRestURL and a non-empty partition intersection.
func (a bridgeIdentity) conflictsWith(b bridgeIdentity) bool {
	if a.endpoint.url != b.endpoint.url {
		return false
	}
	for p := range a.partitions {
		if _, shared := b.partitions[p]; shared {
			return true
		}
	}
	return false
}

// runningBridge is one CR's live Bridge plus the state the supervisor needs
// to hot-reload, restart, conflict-check and stop it.
type runningBridge struct {
	bridge   *Bridge
	cancel   context.CancelFunc
	done     chan struct{} // closed when Run has returned
	identity bridgeIdentity
	// generation is the CR generation the live config was built from; a
	// reconcile for the same generation (status-only update, resync) is a
	// no-op instead of a redundant setCfg + log line.
	generation int64
}

// Supervisor reconciles WorkloadMixing objects in one namespace into running
// Bridges (ADR-0015 Phase A). It is BOTH a manager Runnable (Start captures
// the leader-elected context all Bridge loops run under; like Bridge.Run it
// does not implement LeaderElectionRunnable, so the manager starts it only
// on the elected leader) and a Reconciler (wired to a WorkloadMixing watch
// in cmd/k8s-bridge/main.go).
//
// Lifecycle per CR: added → build config via FromCR (defaults + validation
// shared with every other mode), construct a Bridge with its own Slurm
// client, run its Run loop in a goroutine with a per-CR cancel; spec changed
// → hot-reload via setCfg, or restart the Bridge when the change touches the
// Slurm client's construction-time fields (see endpointIdentity); deleted →
// cancel the loop and forget it. Deletion performs NO Slurm-side cleanup
// (jobs stay held/running, node records stay registered): the bridge holds
// no finalizer on the CR, and a finalizer-based teardown story is a separate
// roadmap item — deleting the CR means "stop managing", not "tear down".
//
// Concurrency: the map of running Bridges is guarded by mu for the HTTP
// readiness handlers, but reconciles themselves are assumed serialized (the
// controller's default MaxConcurrentReconciles of 1 — do not raise it: the
// conflict check reads the map and the start that follows writes it, and
// two concurrent reconciles could otherwise both pass the check).
type Supervisor struct {
	// Kube is the manager's cached client (used for CR reads, status
	// patches, and handed to every Bridge it starts).
	Kube client.Client
	// Namespace is the single namespace supervised — POD_NAMESPACE in
	// production (Phase B, cluster-wide watch, is explicitly out of scope;
	// keying `running` by namespace/name already leaves room for it).
	Namespace string
	Log       *slog.Logger
	// NewSlurmClient builds each CR's own Slurm client.
	NewSlurmClient SlurmClientFactory
	// Recorder emits Warning events for refused CRs and is handed to every
	// Bridge for its JobSet events. Nil-safe (tests).
	Recorder record.EventRecorder
	// AllowedSlurmdImages is the deploy-time trust anchor (security audit
	// C1) checked against every CR's slurmd image before its Bridge starts.
	// Empty allows any image (main.go already warns loudly about that).
	AllowedSlurmdImages []string
	// ConflictRetryInterval overrides defaultConflictRetryInterval when >0
	// (shrunk by tests; envtest waits on the real requeue path).
	ConflictRetryInterval time.Duration

	mu      sync.Mutex
	started bool
	runCtx  context.Context
	running map[types.NamespacedName]*runningBridge
	// blocked records CRs seen but refused AND not running (ConflictingSpec,
	// InvalidSpec, StartFailed) as "reason: message", purely for the /readyz
	// body — blocked CRs do NOT fail aggregate readiness (their signal is
	// the Ready condition and the Warning event); cleared on start, on a
	// successful hot-reload, or on delete.
	blocked map[types.NamespacedName]string
	// writers holds each CR's ReadyConditionWriter — the single
	// serialization point between this reconciler's rejection writes and the
	// CR's Bridge per-tick health reports, and the fix for the CI-caught
	// race where a tick's True/TickSucceeded landed AFTER a rejection's
	// False/InvalidSpec and stamped the invalid generation as healthy (see
	// ReadyConditionWriter's doc for the chosen override semantics). Keyed
	// by CR rather than hung off runningBridge because a rejection can
	// exist with no Bridge running (start refusals); entries are dropped in
	// stop().
	writers map[types.NamespacedName]*ReadyConditionWriter
}

// writerFor returns (lazily creating) the CR's ReadyConditionWriter.
func (s *Supervisor) writerFor(key types.NamespacedName) *ReadyConditionWriter {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writers == nil {
		s.writers = map[types.NamespacedName]*ReadyConditionWriter{}
	}
	w, ok := s.writers[key]
	if !ok {
		w = &ReadyConditionWriter{Kube: s.Kube, Namespace: key.Namespace, Name: key.Name}
		s.writers[key] = w
	}
	return w
}

// Start implements manager.Runnable: it captures the leader-elected context
// every Bridge loop is derived from, then blocks until that context ends and
// waits for all loops to drain. Reconciles arriving before Start are
// requeued (see startupRetryInterval).
func (s *Supervisor) Start(ctx context.Context) error {
	s.mu.Lock()
	s.runCtx = ctx
	s.started = true
	s.mu.Unlock()
	s.Log.Info("workloadmixing supervisor started", "namespace", s.Namespace)

	<-ctx.Done()
	s.mu.Lock()
	entries := make([]*runningBridge, 0, len(s.running))
	for _, e := range s.running {
		entries = append(entries, e)
	}
	s.mu.Unlock()
	// The Bridges' contexts are children of ctx, so their loops are already
	// stopping; wait for each to actually return so shutdown never leaves a
	// tick mid-flight against a dying process.
	for _, e := range entries {
		<-e.done
	}
	s.Log.Info("workloadmixing supervisor stopped", "bridges", len(entries))
	return nil
}

// Reconcile drives one CR toward "exactly one running Bridge, or a truthful
// Ready=False explaining why not". See the Supervisor doc for the lifecycle.
func (s *Supervisor) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	if req.Namespace != s.Namespace {
		// Defensive: the watch is namespace-scoped via the manager cache;
		// anything else reaching here is a wiring bug, not work.
		return reconcile.Result{}, nil
	}
	if !s.isStarted() {
		return reconcile.Result{RequeueAfter: startupRetryInterval}, nil
	}
	key := req.NamespacedName

	wm := &v1alpha1.WorkloadMixing{}
	if err := s.Kube.Get(ctx, key, wm); err != nil {
		if apierrors.IsNotFound(err) {
			s.stop(key, "WorkloadMixing deleted (no Slurm-side cleanup: deleting the CR means stop managing, not tear down)")
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("fetching WorkloadMixing %s: %w", key, err)
	}

	cfg, err := s.buildConfig(wm)
	if err != nil {
		return s.refuseInvalid(ctx, wm, err)
	}
	id := identityFor(cfg)

	if entry, isRunning := s.get(key); isRunning {
		if wm.Generation == entry.generation {
			return reconcile.Result{}, nil // status-only update or resync
		}
		if entry.identity.endpoint == id.endpoint {
			// Restart rule: endpoint fields unchanged → the existing Slurm
			// client stays valid, so this is the same hot-reload path
			// single-CR mode uses (setCfg; the in-flight tick finishes on
			// its old snapshot, the next one picks this up).
			if conflictKey, conflicted := s.findConflict(id, key); conflicted {
				// The EDITED spec now collides with another running Bridge.
				// Applying it would double-manage jobs and keeping the old
				// config while the CR says otherwise would make the CR lie,
				// so the Bridge stops and the CR reports ConflictingSpec
				// until the spec (or the other CR) changes.
				s.stop(key, "spec changed to conflict with "+conflictKey.String())
				return s.refuseConflict(ctx, wm, conflictKey)
			}
			entry.bridge.setCfg(cfg)
			s.mu.Lock()
			entry.identity = id
			entry.generation = wm.Generation
			delete(s.blocked, key)
			s.mu.Unlock()
			// The latest spec is good again: hand the Ready condition back
			// to tick health (the next tick republishes it even without a
			// success/failure transition — see ClearRejection).
			s.writerFor(key).ClearRejection()
			s.Log.Info("config hot-reloaded from WorkloadMixing CR", "workloadmixing", key.String(), "generation", wm.Generation)
			return reconcile.Result{}, nil
		}
		// Endpoint changed: the Slurm client's construction-time fields are
		// stale, so hot-reload cannot apply this — restart the Bridge.
		s.stop(key, "slurm endpoint/client settings changed, restarting bridge")
	}

	if conflictKey, conflicted := s.findConflict(id, key); conflicted {
		return s.refuseConflict(ctx, wm, conflictKey)
	}
	return s.start(ctx, wm, cfg, id)
}

// buildConfig converts and vets the CR exactly like every other config path:
// FromCR (which runs the shared ApplyDefaults + Validate), then the
// deploy-time slurmd image allowlist (C1 — a per-CR check here, where
// single-CR mode checks it once at startup), then the supervisor-only
// ownership scoping field.
func (s *Supervisor) buildConfig(wm *v1alpha1.WorkloadMixing) (*config.Config, error) {
	cfg, err := FromCR(wm)
	if err != nil {
		return nil, err
	}
	if len(s.AllowedSlurmdImages) > 0 {
		if err := config.ValidateImageAllowed(cfg.Slurmd.Image, s.AllowedSlurmdImages); err != nil {
			return nil, fmt.Errorf("slurmd image rejected by --allowed-slurmd-images: %w", err)
		}
	}
	// Per-CR JobSet ownership (see WorkloadMixingName's doc): every Bridge
	// this supervisor starts stamps and selects its own CR's name.
	cfg.WorkloadMixingName = wm.Name
	return cfg, nil
}

// start builds the Slurm client and Bridge for a CR and launches its Run
// loop under the supervisor's leader-elected context.
func (s *Supervisor) start(ctx context.Context, wm *v1alpha1.WorkloadMixing, cfg *config.Config, id bridgeIdentity) (reconcile.Result, error) {
	key := types.NamespacedName{Namespace: wm.Namespace, Name: wm.Name}
	slurmClient, err := s.NewSlurmClient(cfg)
	if err != nil {
		msg := fmt.Sprintf("creating slurm client: %v", err)
		s.setBlocked(key, ReasonStartFailed+": "+msg)
		s.eventf(wm, ReasonStartFailed, msg)
		if uerr := s.writerFor(key).ReportRejection(ctx, ReasonStartFailed, msg); uerr != nil {
			return reconcile.Result{}, uerr
		}
		// Retry on a timer: the usual cause is a token Secret that is not
		// mounted/readable yet, which produces no watch event when it heals.
		return reconcile.Result{RequeueAfter: s.retryInterval()}, nil
	}

	// The spec is acceptable and a Bridge is starting: lift any standing
	// rejection BEFORE the first tick can report, so tick health owns the
	// condition from the loop's first outcome onward.
	w := s.writerFor(key)
	w.ClearRejection()

	b := New(cfg, s.Kube, slurmClient, s.Log.With("workloadmixing", key.String()))
	b.Recorder = s.Recorder

	s.mu.Lock()
	runCtx := s.runCtx
	bctx, cancel := context.WithCancel(runCtx)
	entry := &runningBridge{bridge: b, cancel: cancel, done: make(chan struct{}), identity: id, generation: wm.Generation}
	if s.running == nil {
		s.running = map[types.NamespacedName]*runningBridge{}
	}
	s.running[key] = entry
	delete(s.blocked, key)
	s.mu.Unlock()

	// Tick health reports go through the CR's writer, which suppresses them
	// while the latest spec is rejected — see ReadyConditionWriter. The
	// supervisor's leader context (not the per-Bridge one) parents the patch
	// context so an in-flight final report during a Bridge restart is not
	// cancelled mid-write.
	b.OnTick = func(tickErr error) { w.ReportTick(runCtx, tickErr) }

	go func() {
		defer close(entry.done)
		// Run only ever returns its context's error (ticks are retried
		// internally, see the crash-isolation note in the package doc), so
		// the return value carries no signal beyond "the loop is done".
		_ = b.Run(bctx)
	}()
	s.Log.Info("bridge started for WorkloadMixing", "workloadmixing", key.String(), "slurmRestURL", cfg.SlurmRestURL, "generation", wm.Generation)
	return reconcile.Result{}, nil
}

// stop cancels a CR's Bridge loop (if any), waits for it to return, and
// forgets it. No Slurm-side cleanup happens here by design — see the
// Supervisor doc.
func (s *Supervisor) stop(key types.NamespacedName, reason string) {
	s.mu.Lock()
	entry, wasRunning := s.running[key]
	delete(s.running, key)
	delete(s.blocked, key)
	delete(s.writers, key)
	s.mu.Unlock()
	if !wasRunning {
		return
	}
	entry.cancel()
	// Wait outside the lock: Run returns as soon as it observes the cancel
	// (immediately from its select; within one slurmrestd request timeout if
	// mid-tick, since the context threads through every HTTP call).
	<-entry.done
	s.Log.Info("bridge stopped for WorkloadMixing", "workloadmixing", key.String(), "reason", reason)
}

// refuseConflict reports ADR-0015 §Decision 3: Ready=False/ConflictingSpec
// naming the CR already holding the (slurmRestURL, partition) claim, a
// Warning event, and a timed retry so deleting/fixing the older CR lets this
// one start without any cross-CR enqueue plumbing.
func (s *Supervisor) refuseConflict(ctx context.Context, wm *v1alpha1.WorkloadMixing, conflictKey types.NamespacedName) (reconcile.Result, error) {
	key := types.NamespacedName{Namespace: wm.Namespace, Name: wm.Name}
	msg := fmt.Sprintf("refusing to start: same slurmRestURL and overlapping partition(s) as running WorkloadMixing %s — starting would double-manage its jobs; delete or change one of the two specs", conflictKey)
	s.setBlocked(key, ReasonConflictingSpec+": conflicts with "+conflictKey.String())
	s.eventf(wm, ReasonConflictingSpec, msg)
	s.Log.Warn("WorkloadMixing refused: conflicting spec", "workloadmixing", key.String(), "conflictsWith", conflictKey.String())
	if err := s.writerFor(key).ReportRejection(ctx, ReasonConflictingSpec, msg); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{RequeueAfter: s.retryInterval()}, nil
}

// refuseInvalid reports a spec that failed conversion/validation/allowlist.
// If a Bridge is already running for the CR it KEEPS RUNNING on its
// last-good config (matching single-CR hot-reload semantics: a bad edit
// must not take a healthy Bridge down), with the condition explaining that.
// That running Bridge's tick reporter shares this CR's
// ReadyConditionWriter, so from ReportRejection onward its healthy ticks
// can no longer overwrite the False condition (the CI-caught race: the
// tick's True/TickSucceeded stamped the INVALID generation as healthy) —
// ticks resume owning the condition when a later reconcile clears the
// rejection. Otherwise there is simply nothing to run. No requeue either
// way — nothing changes until the spec does, which delivers its own watch
// event.
func (s *Supervisor) refuseInvalid(ctx context.Context, wm *v1alpha1.WorkloadMixing, cause error) (reconcile.Result, error) {
	key := types.NamespacedName{Namespace: wm.Namespace, Name: wm.Name}
	_, isRunning := s.get(key)
	msg := fmt.Sprintf("spec invalid: %v", cause)
	if isRunning {
		msg = fmt.Sprintf("hot-reload failed, keeping previous config: %v", cause)
	} else {
		s.setBlocked(key, ReasonInvalidSpec+": "+cause.Error())
	}
	s.eventf(wm, ReasonInvalidSpec, msg)
	s.Log.Warn("WorkloadMixing spec rejected", "workloadmixing", key.String(), "running", isRunning, "error", cause)
	if err := s.writerFor(key).ReportRejection(ctx, ReasonInvalidSpec, msg); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

// NudgeAll forwards a watch nudge to every running Bridge. JobSets from all
// CRs share one namespace with no cheap per-instance routing on the watch
// side, so the fan-out is deliberate: an extra tick is coalesced (size-1
// channel) and damped (minNudgeInterval) per Bridge, exactly like any other
// nudge.
func (s *Supervisor) NudgeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.running {
		e.bridge.Nudge()
	}
}

// findConflict returns the (deterministically first) running CR whose
// identity conflicts with the candidate, excluding the candidate itself.
func (s *Supervisor) findConflict(id bridgeIdentity, self types.NamespacedName) (types.NamespacedName, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]types.NamespacedName, 0, len(s.running))
	for k := range s.running {
		if k != self {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
	for _, k := range keys {
		if id.conflictsWith(s.running[k].identity) {
			return k, true
		}
	}
	return types.NamespacedName{}, false
}

func (s *Supervisor) get(key types.NamespacedName) (*runningBridge, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.running[key]
	return e, ok
}

func (s *Supervisor) isStarted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.started
}

func (s *Supervisor) setBlocked(key types.NamespacedName, note string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.blocked == nil {
		s.blocked = map[types.NamespacedName]string{}
	}
	s.blocked[key] = note
}

func (s *Supervisor) retryInterval() time.Duration {
	if s.ConflictRetryInterval > 0 {
		return s.ConflictRetryInterval
	}
	return defaultConflictRetryInterval
}

// eventf emits a Warning event on the CR if a Recorder is wired (nil-safe,
// same contract as Bridge.eventf).
func (s *Supervisor) eventf(wm *v1alpha1.WorkloadMixing, reason, message string) {
	if s.Recorder == nil {
		return
	}
	s.Recorder.Event(wm, corev1.EventTypeWarning, reason, message)
}

// HealthzHandler serves process liveness in supervisor mode: the process is
// alive iff this handler answers, so it always reports 200 — per-CR loop
// health deliberately does NOT feed liveness (a struggling Slurm cluster
// must trigger backoff and alerting, not container restarts; same stance as
// single-CR Healthy()).
func (s *Supervisor) HealthzHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		n := len(s.running)
		s.mu.Unlock()
		writeProbeResult(w, true, fmt.Sprintf("supervisor process alive; %d bridge loop(s) running", n))
	}
}

// supervisorReadyz is the /readyz response body in supervisor mode: the
// aggregate verdict plus per-CR detail.
type supervisorReadyz struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	// Bridges lists every RUNNING Bridge's own readiness (the same
	// Ready() verdict single-CR mode serves), keyed by namespace/name.
	Bridges map[string]bridgeReadiness `json:"bridges,omitempty"`
	// Blocked lists CRs the supervisor refused (ConflictingSpec,
	// InvalidSpec, StartFailed). Informational only: blocked CRs do not
	// fail aggregate readiness — their operator signal is the CR's Ready
	// condition and the Warning event, and failing the probe for a
	// misconfigured CR would take the healthy CRs' bridge out of
	// service with it.
	Blocked map[string]string `json:"blocked,omitempty"`
}

type bridgeReadiness struct {
	Ready   bool   `json:"ready"`
	Message string `json:"message"`
}

// ReadyzHandler serves aggregate readiness in supervisor mode: 200 iff every
// RUNNING Bridge is Ready. Edge cases, all deliberate:
//   - Zero CRs (nothing running, nothing blocked): READY, with an
//     explanatory message — an empty namespace is not a failure, and a
//     freshly installed controller must not sit unready until someone
//     creates the first CR.
//   - Not (yet) the leader: 503 "not leader" — the standby's loops never
//     run, matching single-CR mode's documented standby-is-503 posture
//     (docs/operations.md), so alerting rules keep one shape across modes.
//   - Blocked CRs: listed in the body, but not part of the verdict (see
//     supervisorReadyz.Blocked).
func (s *Supervisor) ReadyzHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		started := s.started
		bridges := make(map[string]*Bridge, len(s.running))
		for k, e := range s.running {
			bridges[k.String()] = e.bridge
		}
		blocked := make(map[string]string, len(s.blocked))
		for k, note := range s.blocked {
			blocked[k.String()] = note
		}
		s.mu.Unlock()

		resp := supervisorReadyz{Blocked: blocked}
		ok := true
		switch {
		case !started:
			ok = false
			resp.Message = "supervisor not started: this replica has not won leader election (standby readiness is 503 by design; see docs/operations.md)"
		case len(bridges) == 0 && len(blocked) == 0:
			resp.Message = fmt.Sprintf("no WorkloadMixing objects in namespace %q; nothing to bridge (an empty namespace is not a failure)", s.Namespace)
		default:
			ready := 0
			resp.Bridges = make(map[string]bridgeReadiness, len(bridges))
			for k, b := range bridges {
				bOK, msg := b.Ready()
				if bOK {
					ready++
				} else {
					ok = false
				}
				resp.Bridges[k] = bridgeReadiness{Ready: bOK, Message: msg}
			}
			resp.Message = fmt.Sprintf("%d/%d running bridge(s) ready; %d blocked", ready, len(bridges), len(blocked))
		}
		resp.Status = boolToStatus(ok)
		status := http.StatusOK
		if !ok {
			status = http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(resp)
	}
}
