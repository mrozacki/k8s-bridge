// k8s-bridge prototype: translates held Slurm jobs into Kueue-managed JobSets
// whose pods register as dynamic Slurm nodes. See docs/controller.md for scope.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/mrozacki/k8s-bridge/api/v1alpha1"
	"github.com/mrozacki/k8s-bridge/internal/bridge"
	"github.com/mrozacki/k8s-bridge/internal/config"
	"github.com/mrozacki/k8s-bridge/internal/metrics"
	"github.com/mrozacki/k8s-bridge/internal/slurm"
)

func main() {
	configPath := flag.String("config", "config/example-config.yaml", "path to bridge config file")
	// ADR-0015 Phase A: --workloadmixing is now the explicit SINGLE-CR
	// binding, kept as the compatibility/escape-hatch path with exactly its
	// historic behavior. The supervisor default in CR mode is selected via
	// --config-source=cr with this flag UNSET.
	workloadMixing := flag.String("workloadmixing", "", "bind to exactly ONE WorkloadMixing CR (<namespace>/<name>) — compatibility/escape hatch; implies --config-source=cr. Leave unset with --config-source=cr to supervise every WorkloadMixing in POD_NAMESPACE instead (ADR-0015)")
	// configSource picks how configuration is loaded, mirroring the chart's
	// configSource value: "file" (--config, the default) or "cr". With "cr"
	// and --workloadmixing unset, the controller runs the multi-CR
	// supervisor: every WorkloadMixing in POD_NAMESPACE gets its own
	// reconcile loop, started/stopped as CRs come and go (ADR-0015 Phase A).
	configSource := flag.String("config-source", "file", `configuration source: "file" (--config) or "cr" (WorkloadMixing objects in POD_NAMESPACE; see --workloadmixing for single-CR binding)`)
	// pprof is OFF by default: its heap profiles contain the in-memory Slurm
	// token, and an unauthenticated listener is a DoS/exfiltration surface.
	// Pass an explicit address (e.g. 127.0.0.1:6060) to enable it locally.
	pprofAddr := flag.String("pprof-addr", "", "pprof listen address, e.g. 127.0.0.1:6060 (empty disables; bind to localhost only)")
	// allowedSlurmdImages restricts which container image the config/CR may run
	// as the privileged slurmd node (C1 in the security audit). This is a
	// controller-level trust anchor set by the platform admin at deploy time —
	// it must NOT come from the WorkloadMixing CR, whose author is exactly who
	// the allowlist defends against. Empty allows any image (with a warning).
	allowedImages := flag.String("allowed-slurmd-images", "", "comma-separated allowlist of permitted slurmd image prefixes; empty allows any (not recommended)")
	// allowedTokenPaths restricts which files a WorkloadMixing CR may name in
	// slurmTokenFile/slurmCACertFile (H2 in the 2026-07-24 audit). Same trust
	// anchor reasoning as allowedSlurmdImages: the controller reads these paths
	// from its own filesystem and sends the token to a URL the same CR chooses,
	// so in supervisor mode — where every CR in the namespace is adopted — an
	// unrestricted path is an arbitrary-file read wired to an attacker-chosen
	// sink. The default covers the canonical mount points the chart itself
	// uses; widen it here (not in the CR) if you mount tokens elsewhere.
	allowedTokenPaths := flag.String("allowed-token-paths", strings.Join(config.DefaultAllowedTokenPaths, ","), "comma-separated allowlist of directory prefixes a WorkloadMixing CR may read slurmTokenFile/slurmCACertFile from (supervisor mode); empty disables the check (not recommended)")
	// allowInsecureTLS gates the CR's slurmInsecureSkipTLSVerify field (L8).
	// Disabling certificate verification is a platform-admin decision; a CRD
	// CEL rule cannot express it because the CR author is who sets the field.
	allowInsecureTLS := flag.Bool("allow-insecure-tls", false, "permit a WorkloadMixing CR to set slurmInsecureSkipTLSVerify (supervisor mode); development only")
	// metricsAddr consolidates the HTTP surface (audit AUD2): a single mux
	// serves /metrics, /healthz and /readyz on one port. This replaces the
	// former separate --health-addr flag; the Helm chart already wires its
	// probes and Service port to :8080 under this flag name/default.
	//
	// ADR-0011: this stays the bridge's OWN mux rather than switching to the
	// manager's built-in metrics+health servers, because those are two
	// SEPARATE listeners (Options.Metrics.BindAddress and
	// Options.HealthProbeBindAddress) with no supported way to merge them
	// onto one port — and the chart's probes/Service assume exactly one
	// port. The manager's own metrics/health servers are disabled below
	// ("0"); bridge collectors instead register on controller-runtime's own
	// metrics.Registry (internal/metrics), so /metrics still carries the
	// CNCF-standard controller-runtime series (rest_client_*,
	// leader_election_master_status) for free even though this mux serves
	// them, not the manager's.
	metricsAddr := flag.String("metrics-addr", ":8080", "listen address serving /metrics, /healthz and /readyz on one mux (empty disables)")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, or error")
	// leaderElect defaults to true (ADR-0011): the Manager gates the tick
	// loop's Start behind winning a Lease in the config Namespace, so a
	// second replica sits idle instead of double-processing/double-creating
	// JobSets. Disable only for a single-replica local/dev run where the
	// coordination.k8s.io RBAC (deploy/chart/k8s-bridge/templates/rbac.yaml)
	// or API access is unavailable (e.g. a restricted laptop kubeconfig).
	leaderElect := flag.Bool("leader-elect", true, "enable leader election via a coordination.k8s.io Lease before starting the reconcile loop")
	// L9: short lease/renew so a rolling handover completes quickly; paired with
	// the chart's Recreate strategy, which is the actual rollout-deadlock fix.
	leaseDuration := flag.Duration("leader-lease-duration", 15*time.Second, "leader-election lease duration")
	renewDeadline := flag.Duration("leader-renew-deadline", 10*time.Second, "leader-election renew deadline")
	// Deploy-time knobs like --allowed-slurmd-images, NOT config/CR fields:
	// the CR author must not be able to raise the bridge's own API-flood
	// ceiling. The 20k-JobSet burst (suite E, 2026-07) showed GKE admission
	// webhooks (warden) refusing connections under the object stream — the
	// remedy on shared clusters is to LOWER these, trading ramp-up speed for
	// control-plane headroom.
	kubeQPS := flag.Float64("kube-api-qps", 20, "client-go QPS ceiling for the Kubernetes API client")
	kubeBurst := flag.Int("kube-api-burst", 40, "client-go burst ceiling for the Kubernetes API client")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parseLogLevel(*logLevel)}))

	// Route controller-runtime's own logr output into our slog handler.
	// Without this, controller-runtime logs nothing and prints a "SetLogger
	// was never called" stack trace — and, worse, its internal errors
	// (informer cache-sync failures, watch errors) become INVISIBLE, which
	// hid a live cache-sync hang during the first in-cluster deploy. Found
	// live (experiment 09); keep this wired before manager.New.
	ctrl.SetLogger(logr.FromSlogHandler(log.Handler()))

	if *pprofAddr != "" {
		startPprof(*pprofAddr, log)
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		log.Error("registering core scheme", "error", err)
		os.Exit(1)
	}
	if err := jobsetv1alpha2.AddToScheme(scheme); err != nil {
		log.Error("registering jobset scheme", "error", err)
		os.Exit(1)
	}
	// ADR-0014 PR 2: the WorkloadMixing CR is consumed as its typed Go API —
	// both the bootstrap load and the hot-reload watch below go through this
	// registration (the pre-migration unstructured GVK plumbing is gone).
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		log.Error("registering workloadmixing scheme", "error", err)
		os.Exit(1)
	}

	restCfg, err := ctrl.GetConfig() // kubeconfig or in-cluster, standard resolution
	if err != nil {
		log.Error("loading kubeconfig", "error", err)
		os.Exit(1)
	}
	// audit P8b: controller-runtime's client-go default is QPS=-1 (no client
	// side rate limit at all). That's harmless today because every API call
	// in the bridge is sequential, but it leaves no ceiling the moment
	// anything parallelizes (backlog P5). Set an explicit, deliberately
	// modest ceiling now so the stance is a decision, not an accident.
	// Configurable since the suite-E scale run (finding #9): shared clusters
	// may need a lower ceiling to protect admission webhooks.
	if !validKubeRateLimits(*kubeQPS, *kubeBurst) {
		log.Error("invalid --kube-api-qps/--kube-api-burst: qps must be > 0 and burst >= qps", "qps", *kubeQPS, "burst", *kubeBurst)
		os.Exit(1)
	}
	restCfg.QPS = float32(*kubeQPS)
	restCfg.Burst = *kubeBurst

	// audit minor: ctrl.SetupSignalHandler() already cancels its context on
	// SIGTERM/SIGINT; wrapping it again in signal.NotifyContext for the same
	// signals was redundant double handling.
	ctx := ctrl.SetupSignalHandler()

	mode, err := resolveMode(*configSource, *workloadMixing)
	if err != nil {
		log.Error("resolving configuration mode", "error", err)
		os.Exit(1)
	}

	// Namespace the leader-election Lease lives in. In CR mode the CR's
	// namespace is known immediately; file mode has no CR to anchor on, so
	// it falls back to "default" (file mode is documented as the
	// local/laptop path — see docs/controller.md — where a single replica is
	// the expected deployment shape anyway).
	var wmNamespace, wmName string
	if mode == modeSingleCR {
		var ok bool
		wmNamespace, wmName, ok = strings.Cut(*workloadMixing, "/")
		if !ok {
			log.Error("--workloadmixing must be <namespace>/<name>")
			os.Exit(1)
		}
	}
	// Supervisor mode watches POD_NAMESPACE (ADR-0015 Phase A: namespace-
	// scoped on purpose — a cluster-wide watch would force ClusterRoles back
	// in, regressing the chart's namespaced-RBAC secure default; Phase B is
	// the explicit opt-in for that). Without POD_NAMESPACE there is nothing
	// to watch, so fail with instructions rather than guessing.
	podNamespace := os.Getenv("POD_NAMESPACE")
	if mode == modeSupervisor && podNamespace == "" {
		log.Error("supervisor mode (--config-source=cr without --workloadmixing) requires POD_NAMESPACE: the controller watches WorkloadMixing objects in its own namespace only. Set POD_NAMESPACE via the downward API (the Helm chart does this), or bind to a single CR with --workloadmixing <namespace>/<name>")
		os.Exit(1)
	}

	// Load config before manager initialization so cache scope can include
	// cfg.Namespace. Supervisor mode has no single bootstrap config — each
	// CR's config is built by the Supervisor as CRs appear, and the cache
	// needs only POD_NAMESPACE (every supervised CR lives there, and a CR's
	// namespace is where its JobSets go).
	var cfg *config.Config
	var statusUpdater func(error)
	switch mode {
	case modeSingleCR:
		tmpKube, err := client.New(restCfg, client.Options{Scheme: scheme})
		if err != nil {
			log.Error("creating bootstrap client for CR loading", "error", err)
			os.Exit(1)
		}
		cfg, _, err = bridge.LoadConfigFromCR(ctx, tmpKube, wmNamespace, wmName)
		if err != nil {
			log.Error("loading WorkloadMixing CR", "error", err)
			os.Exit(1)
		}
		log.Info("config loaded from WorkloadMixing CR", "cr", *workloadMixing)
	case modeFile:
		cfg, err = config.Load(*configPath)
		if err != nil {
			log.Error("loading config", "error", err)
			os.Exit(1)
		}
		log.Info("config loaded from file", "path", *configPath)
	case modeSupervisor:
		log.Info("supervisor mode: watching all WorkloadMixing objects", "namespace", podNamespace)
	}

	// ADR-0011: adopt a controller-runtime Manager for its cached client,
	// leader election, and graceful-shutdown wiring, while keeping the
	// Slurm polling loop (bridge.Bridge.Run) as the correctness backbone —
	// slurmrestd cannot be watched, so polling stays; the Manager's watches
	// only NUDGE it to run sooner (see internal/bridge/watch.go and
	// docs/adr/0011).
	// Scope the informer cache to the bridge's own namespace and (in
	// single-CR/file mode) the target namespace. In supervisor mode
	// POD_NAMESPACE alone covers everything: the CRs live there and their
	// JobSets are created there (a CR's namespace IS its JobSet namespace).
	cacheNamespaces := []string{podNamespace}
	if cfg != nil {
		cacheNamespaces = append(cacheNamespaces, cfg.Namespace)
	}
	mgrOpts := manager.Options{
		Scheme: scheme,
		Cache:  cacheScopedToNamespace(cacheNamespaces...),
		// P8 note (bug B7):
		// caching UNSTRUCTURED reads (the Kueue Workload snapshot LIST in
		// internal/bridge/kueue.go) through the informer cache deadlocked the
		// first tick live — the lazily-started unstructured Workload informer
		// never returned control to the tick, which hung forever after a
		// successful ListJobs (found live, experiment 09). A pre-warmed
		// informer was tried as a fix (backlog L4) but reintroduced a
		// first-tick failure under envtest (JobSet not created), so it was
		// reverted: live (uncached) unstructured reads restore the proven
		// pre-ADR-0011 behavior. The Workload LIST is small; the per-tick
		// cost the audit flagged stays deferred to P8 as a follow-up.
		// STRUCTURED reads (JobSets) stay cached.
		Client: client.Options{Cache: &client.CacheOptions{Unstructured: false}},
		// The bridge's own consolidated mux (started below via
		// startMetricsServer) already serves /metrics, /healthz, /readyz on
		// --metrics-addr; disable the Manager's separate servers ("0") so
		// there is exactly one HTTP surface, matching the chart's single
		// probe/Service port.
		Metrics:                 metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress:  "0",
		LeaderElection:          *leaderElect,
		LeaderElectionID:        "k8s-bridge-leader",
		LeaderElectionNamespace: leaderElectionNamespace(wmNamespace),
		LeaseDuration:           leaseDuration,
		RenewDeadline:           renewDeadline,
	}
	mgr, err := manager.New(restCfg, mgrOpts)
	if err != nil {
		log.Error("creating controller-runtime manager", "error", err)
		os.Exit(1)
	}

	kube := mgr.GetClient()

	// C1: gate the privileged slurmd image against the deploy-time
	// allowlist. Single-CR/file mode checks the one known config here and
	// fails fast; supervisor mode hands the allowlist to the Supervisor,
	// which enforces it per CR (a disallowed image refuses THAT CR with
	// Ready=False/InvalidSpec instead of killing the shared process).
	allowlist := splitAndTrim(*allowedImages)
	if len(allowlist) == 0 {
		log.Warn("no --allowed-slurmd-images set: any image in config/CR will run as the privileged slurmd node; set an allowlist in production")
	} else if cfg != nil {
		if err := config.ValidateImageAllowed(cfg.Slurmd.Image, allowlist); err != nil {
			log.Error("slurmd image rejected by allowlist", "error", err)
			os.Exit(1)
		}
	}

	// H2: the token-path allowlist is enforced per CR by the Supervisor, for
	// the reason spelled out on Supervisor.AllowedTokenPaths — a CR is only
	// untrusted input when it is adopted automatically. In single-CR/file mode
	// the admin named the config source, so there is nothing to gate here.
	tokenPathAllowlist := splitAndTrim(*allowedTokenPaths)
	if len(tokenPathAllowlist) == 0 && mode == modeSupervisor {
		log.Warn("no --allowed-token-paths set: any WorkloadMixing CR in this namespace may make the controller read an arbitrary file it has access to and send it to the CR's slurmRestURL; set an allowlist in production")
	}

	// The consolidated mux's probe handlers: the single Bridge's own
	// handlers in single-CR/file mode (byte-compatible with every previous
	// release), the Supervisor's aggregating handlers in supervisor mode.
	var healthz, readyz http.HandlerFunc

	// nudgeTarget carries whichever nudge receiver this mode uses (one
	// Bridge, or the Supervisor fanning out to all of them).
	var nudgeJobSet, nudgeWorkload *bridge.NudgeReconciler

	if mode == modeSupervisor {
		sup := &bridge.Supervisor{
			Kube:      kube,
			Namespace: podNamespace,
			Log:       log,
			NewSlurmClient: func(crCfg *config.Config) (bridge.SlurmAPI, error) {
				c, err := slurm.NewClient(slurm.Options{
					BaseURL:            crCfg.SlurmRestURL,
					User:               crCfg.SlurmUser,
					TokenFile:          crCfg.SlurmTokenFile,
					CACertFile:         crCfg.SlurmCACertFile,
					InsecureSkipVerify: crCfg.SlurmInsecureSkipTLSVerify,
					RequestTimeout:     crCfg.SlurmRequestTimeout.Duration,
					// A1: client-side rate limit toward slurmrestd (0 =
					// unlimited). Construction-time, like the timeout — a CR
					// changing these makes the Supervisor RESTART that CR's
					// bridge (see the endpointIdentity restart rule).
					RequestsPerSecond: crCfg.SlurmRequestsPerSecond,
				})
				if err != nil {
					return nil, err
				}
				wireSlurmClientMetrics(c)
				return c, nil
			},
			Recorder:            mgr.GetEventRecorderFor("k8s-bridge"), //nolint:staticcheck // classic record.EventRecorder API; migration is separate
			AllowedSlurmdImages: allowlist,
			AllowedTokenPaths:   tokenPathAllowlist,
			AllowInsecureTLS:    *allowInsecureTLS,
		}
		// The Supervisor is both the WorkloadMixing reconciler (this watch)
		// and a Runnable (below; captures the leader-elected context its
		// Bridge loops run under). Leave MaxConcurrentReconciles at its
		// default of 1 — the Supervisor's conflict-check-then-start sequence
		// documents that assumption.
		if err := builder.ControllerManagedBy(mgr).
			Named("workloadmixing-supervisor").
			For(&v1alpha1.WorkloadMixing{}).
			Complete(sup); err != nil {
			log.Error("wiring WorkloadMixing supervisor", "error", err)
			os.Exit(1)
		}
		if err := mgr.Add(sup); err != nil {
			log.Error("registering supervisor with manager", "error", err)
			os.Exit(1)
		}
		nudgeJobSet = &bridge.NudgeReconciler{Supervisor: sup, Source: "jobset"}
		nudgeWorkload = &bridge.NudgeReconciler{Supervisor: sup, Source: "workload"}
		healthz, readyz = sup.HealthzHandler(), sup.ReadyzHandler()
	} else {
		// The single writer both status feeds share in single-CR mode: the
		// per-tick health reports (OnTick below) and the hot-reload
		// reconciler's failure reports. Sharing ONE ReadyConditionWriter is
		// what makes their writes race-free — without it a reload-failure
		// False and an in-flight tick True could land in either order (the
		// same two-writer race CI caught in supervisor mode; see
		// bridge.ReadyConditionWriter). The writer also owns the AUD2
		// short-timeout patch context derivation the closure here used to do.
		var wmWriter *bridge.ReadyConditionWriter
		if mode == modeSingleCR {
			wmWriter = &bridge.ReadyConditionWriter{Kube: kube, Namespace: wmNamespace, Name: wmName}
			statusUpdater = func(tickErr error) { wmWriter.ReportTick(ctx, tickErr) }
		}

		slurmClient, err := slurm.NewClient(slurm.Options{
			BaseURL:            cfg.SlurmRestURL,
			User:               cfg.SlurmUser,
			TokenFile:          cfg.SlurmTokenFile,
			CACertFile:         cfg.SlurmCACertFile,
			InsecureSkipVerify: cfg.SlurmInsecureSkipTLSVerify,
			RequestTimeout:     cfg.SlurmRequestTimeout.Duration,
			// A1: client-side rate limit toward slurmrestd (0 = unlimited).
			// Read at startup only, like slurmRequestTimeout — a hot-reload
			// change needs a controller restart (the client is built once here).
			RequestsPerSecond: cfg.SlurmRequestsPerSecond,
		})
		if err != nil {
			log.Error("creating slurm client", "error", err)
			os.Exit(1)
		}
		wireSlurmClientMetrics(slurmClient)

		b := bridge.New(cfg, kube, slurmClient, log)
		b.OnTick = statusUpdater
		// AUD2: Events on JobSets (Created/Released/JobSetFailed/TranslationFailed)
		// via the Manager's own recorder, broadcasting through the same client
		// the rest of the bridge uses. GetEventRecorderFor returns the classic
		// record.EventRecorder (Bridge.Recorder's type); the SA1019-suggested
		// GetEventRecorder returns the newer events.EventRecorder with a different
		// Eventf signature, so migrating is a separate change. controller-runtime
		// itself suppresses this same deprecation (manager/internal.go).
		b.Recorder = mgr.GetEventRecorderFor("k8s-bridge") //nolint:staticcheck // classic record.EventRecorder API; migration is separate

		// A1 hot-reload: only wired in single-CR mode. File mode keeps the
		// existing one-shot load — there is no CR to watch. Typed since
		// ADR-0014 PR 2: For() a *v1alpha1.WorkloadMixing, so the
		// reconciler's Get (through the same cached client) is served by
		// this watch's informer. (Supervisor mode needs no separate
		// hot-reload controller: the Supervisor's own watch IS its reload
		// path.)
		if mode == modeSingleCR {
			if err := builder.ControllerManagedBy(mgr).
				Named("workloadmixing-hot-reload").
				For(&v1alpha1.WorkloadMixing{}, builder.WithPredicates(predicate.NewPredicateFuncs(func(obj client.Object) bool {
					return obj.GetNamespace() == wmNamespace && obj.GetName() == wmName
				}))).
				Complete(&bridge.ConfigReconciler{Kube: kube, Bridge: b, Namespace: wmNamespace, Name: wmName, Writer: wmWriter}); err != nil {
				log.Error("wiring WorkloadMixing hot-reload", "error", err)
				os.Exit(1)
			}
		}

		// b.Run is registered as a plain manager.RunnableFunc, which — per
		// controller-runtime's Runnable grouping — starts ONLY after the
		// informer caches have synced AND (since it does not implement
		// LeaderElectionRunnable) this replica has won leader election. See
		// ADR-0011 for the verification trail. Rollback path: drop everything
		// above this comment and call b.Run(ctx) directly with a plain
		// client.New — tick()'s own logic is completely unchanged either way.
		if err := mgr.Add(manager.RunnableFunc(b.Run)); err != nil {
			log.Error("registering bridge reconcile loop with manager", "error", err)
			os.Exit(1)
		}

		nudgeJobSet = &bridge.NudgeReconciler{Bridge: b, Source: "jobset"}
		nudgeWorkload = &bridge.NudgeReconciler{Bridge: b, Source: "workload"}
		healthz, readyz = b.HealthzHandler(), b.ReadyzHandler()
	}

	// P1 watch-nudge (backlog P1 / ADR-0011): JobSet events wake the poll
	// loop immediately instead of waiting for the next timer tick. Filtered
	// to bridge-owned JobSets (the same managed-by label snapshot() and
	// cleanup already use) so unrelated JobSets in the namespace never
	// trigger extra ticks. In supervisor mode the nudge fans out to every
	// running Bridge (see NudgeReconciler.Supervisor).
	if err := builder.ControllerManagedBy(mgr).
		Named("jobset-nudge").
		For(&jobsetv1alpha2.JobSet{}, builder.WithPredicates(predicate.NewPredicateFuncs(isBridgeOwnedJobSet))).
		Complete(nudgeJobSet); err != nil {
		log.Error("wiring JobSet watch-nudge", "error", err)
		os.Exit(1)
	}

	// Workload watch-nudge: all Kueue-GVK knowledge stays behind
	// bridge.WorkloadNudgeSource (internal/bridge/kueue.go) so this file
	// never names the Workload GVK itself (thin-surface guard). No For()
	// object: WatchesRawSource alone is sufficient to drive a controller,
	// per controller-runtime's builder. See that function's doc for why it
	// is intentionally unfiltered by owner (Kueue does not offer a
	// server-side-selectable "bridge-owned" label on Workload objects).
	if err := builder.ControllerManagedBy(mgr).
		Named("workload-nudge").
		WatchesRawSource(bridge.WorkloadNudgeSource(mgr.GetCache(),
			&handler.TypedEnqueueRequestForObject[*unstructured.Unstructured]{})).
		Complete(nudgeWorkload); err != nil {
		log.Error("wiring Workload watch-nudge", "error", err)
		os.Exit(1)
	}

	// metricsErr stays nil when the server is disabled; a nil channel never
	// fires in the select below, so the manager alone decides when to exit.
	var metricsErr <-chan error
	if *metricsAddr != "" {
		metricsErr, err = startMetricsServer(ctx, *metricsAddr, healthz, readyz, log)
		if err != nil {
			// A7: a bind failure used to be logged inside a goroutine and
			// otherwise ignored, leaving a running controller whose
			// /healthz and /readyz do not exist — kubelet then restart-loops the
			// pod on probe failures with symptoms pointing everywhere but
			// here. Fail fast instead: the addr is wrong or taken, and no
			// amount of reconciling fixes that.
			log.Error("starting metrics server", "addr", *metricsAddr, "error", err)
			os.Exit(1)
		}
	}

	log.Info("starting controller-runtime manager", "leaderElect", *leaderElect, "mode", mode.String())
	// A7: run the manager in a goroutine and select its outcome against the
	// metrics server's, so a mid-run ListenAndServe failure (not just a bind
	// failure at startup) also terminates the process non-zero instead of
	// leaving a probe-less controller running. The manager keeps its own
	// graceful-shutdown semantics: on SIGTERM ctx cancels, mgr.Start returns,
	// and the metrics server's shutdown goroutine (see startMetricsServer)
	// drains in parallel.
	mgrDone := make(chan error, 1)
	go func() { mgrDone <- mgr.Start(ctx) }()
	select {
	case err := <-mgrDone:
		if err != nil && ctx.Err() == nil {
			log.Error("manager exited", "error", err)
			os.Exit(1)
		}
	case err := <-metricsErr:
		log.Error("metrics server failed: /metrics, /healthz and /readyz are gone; exiting so kubelet restarts the pod with a truthful signal instead of probe-less limbo", "error", err)
		os.Exit(1)
	}
	log.Info("bridge stopped")
}

// controllerMode is how the process was asked to obtain configuration and
// how many reconcile loops it therefore runs (ADR-0015 Phase A).
type controllerMode int

const (
	// modeFile: one Bridge from a config file (--config), one-shot load.
	modeFile controllerMode = iota
	// modeSingleCR: one Bridge bound to exactly one WorkloadMixing CR
	// (--workloadmixing <ns>/<name>) with hot-reload — the pre-ADR-0015
	// behavior, kept byte-for-byte as the compatibility/upgrade path.
	modeSingleCR
	// modeSupervisor: one Bridge PER WorkloadMixing CR in POD_NAMESPACE,
	// started/stopped as CRs come and go (the new default for
	// --config-source=cr).
	modeSupervisor
)

func (m controllerMode) String() string {
	switch m {
	case modeSingleCR:
		return "single-cr"
	case modeSupervisor:
		return "supervisor"
	default:
		return "file"
	}
}

// resolveMode maps the (--config-source, --workloadmixing) flag pair onto a
// controllerMode. --workloadmixing set always means single-CR binding and
// implies the CR source, whatever --config-source says — it predates that
// flag, and existing deployments pass it alone; erroring on the (redundant
// but coherent) explicit combination with --config-source=cr would break
// nobody, but erroring on the default "file" value WOULD break every
// existing single-CR deployment, and the flag package cannot tell those two
// apart. Only a nonsensical --config-source value errors.
func resolveMode(configSource, workloadMixing string) (controllerMode, error) {
	if workloadMixing != "" {
		return modeSingleCR, nil
	}
	switch configSource {
	case "file":
		return modeFile, nil
	case "cr":
		return modeSupervisor, nil
	default:
		return modeFile, fmt.Errorf("--config-source must be %q or %q, got %q", "file", "cr", configSource)
	}
}

// validKubeRateLimits reports whether the --kube-api-qps/--kube-api-burst
// pair is coherent: qps positive, burst at least qps. The comparison happens
// in float64 on purpose (A5): the previous `*kubeBurst < int(*kubeQPS)`
// TRUNCATED the qps first, so --kube-api-qps=20.5 --kube-api-burst=20 passed
// validation and handed client-go a burst BELOW its qps.
func validKubeRateLimits(qps float64, burst int) bool {
	return qps > 0 && float64(burst) >= qps
}

// wireSlurmClientMetrics connects the slurm client's callback seams to the
// Prometheus metrics. internal/slurm stays free of prometheus (see its
// package doc) — these callbacks are the seam that keeps it that way:
// OnRequest feeds the per-method/per-code counter, OnRequestDuration feeds
// the A10 latency histogram (slurmrestd was the fragile dependency in scale
// testing; this is its early-warning series).
func wireSlurmClientMetrics(c *slurm.Client) {
	c.OnRequest = func(method string, code int) {
		metrics.SlurmAPIRequestsTotal.WithLabelValues(method, strconv.Itoa(code)).Inc()
	}
	c.OnRequestDuration = func(method string, d time.Duration) {
		metrics.SlurmRequestDuration.WithLabelValues(method).Observe(d.Seconds())
	}
}

// leaderElectionNamespace picks the namespace the Lease lives in: the
// WorkloadMixing CR's namespace when in CR mode (known immediately), or
// "default" for file mode, where no CR namespace exists to anchor on and a
// single-replica local/dev run is the expected use of file mode anyway.
// cacheScopedToNamespace confines the Manager's informer cache to one
// namespace so it lists/watches namespaced (matching the chart's namespaced
// RBAC) instead of at cluster scope. An empty ns yields an all-namespaces
// cache (requires cluster-scoped RBAC — only for local runs).
func cacheScopedToNamespace(namespaces ...string) cache.Options {
	m := make(map[string]cache.Config)
	for _, ns := range namespaces {
		if ns != "" {
			m[ns] = cache.Config{}
		}
	}
	if len(m) == 0 {
		return cache.Options{}
	}
	return cache.Options{DefaultNamespaces: m}
}

func leaderElectionNamespace(wmNamespace string) string {
	// Prefer the namespace the controller actually runs in (injected by the
	// chart via the downward API): the RBAC that grants the Lease is scoped
	// to the release namespace, so the lock MUST live there. Falling back to
	// "default" (the previous behavior) meant leader election was forbidden
	// out of the box under the chart's own RBAC — found live on the first
	// in-cluster deploy (experiment 09, A4). wmNamespace (CR mode) is the
	// next-best signal; "default" only as a last resort for `go run` locally.
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}
	if wmNamespace != "" {
		return wmNamespace
	}
	return "default"
}

// isBridgeOwnedJobSet is the P1 watch-nudge filter: only JobSets carrying
// the bridge's managed-by label should trigger an extra tick. Unrelated
// JobSets in the same namespace (should any exist) must not. The label
// value is hardcoded rather than importing internal/translate here purely
// to avoid a cmd -> internal/translate dependency for one string pair; both
// sides are covered by TestIsBridgeOwnedJobSetMatchesTranslateConstants.
func isBridgeOwnedJobSet(obj client.Object) bool {
	js, ok := obj.(*jobsetv1alpha2.JobSet)
	if !ok {
		return false
	}
	return js.Labels["k8s-bridge.x-k8s.io/managed-by"] == "k8s-bridge"
}

// parseLogLevel maps the --log-level flag to a slog.Level, defaulting to
// Info for an empty or unrecognized value so a typo degrades gracefully
// rather than crashing the process before logging even works.
func parseLogLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// startPprof serves the profiling endpoints on a dedicated mux and server with
// timeouts (gosec G114 — the default http.ListenAndServe has none, making it
// slowloris-able). Kept off its own DefaultServeMux so nothing else is exposed.
func startPprof(addr string, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Warn("pprof enabled: exposes heap (incl. the Slurm token) and CPU profiles; bind to localhost only", "addr", addr)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("pprof server stopped", "error", err)
		}
	}()
}

// metricsShutdownTimeout bounds the graceful drain of in-flight scrapes and
// probe requests when the run context is cancelled (A7). Short on purpose:
// every request this mux serves is a sub-second GET, and pod termination
// should not wait on a stuck scraper.
const metricsShutdownTimeout = 5 * time.Second

// startMetricsServer serves the consolidated HTTP surface (audit AUD2):
// /metrics (Prometheus), /healthz and /readyz (liveness/readiness), all on
// one mux and port. This is the contract the Helm chart's Deployment
// template assumes (--metrics-addr :8080, probes on the "metrics" port).
// ADR-0011: metrics.Registry is now controller-runtime's own registry, so
// this single endpoint also carries the Manager's rest_client_* and
// leader_election_master_status series for free. The consolidated-mux design
// (one port, this bridge's own server, Manager's servers disabled) is
// unchanged by A7 — only the failure handling is.
//
// A7 hardening (both failure classes previously logged-and-ignored inside a
// goroutine, leaving a probe-less controller running):
//   - Bind failures surface SYNCHRONOUSLY via the error return: net.Listen
//     runs here, not inside the serve goroutine, so a taken port or bad addr
//     kills the process at startup with a direct error message.
//   - A later Serve failure is delivered on the returned channel, which main
//     selects alongside the manager so the process still exits non-zero.
//   - On ctx cancel the server drains gracefully (Shutdown with
//     metricsShutdownTimeout) instead of having its connections torn down by
//     process exit; ErrServerClosed from that path is not an error.
//
// The server is deliberately NOT a manager Runnable: Runnables without
// LeaderElectionRunnable start only after the informer caches sync, and the
// probe endpoints must be up BEFORE that — a wedged cache sync (the
// experiment-09 live bug) with dead probes would restart-loop the pod and
// hide the very hang the probes exist to expose.
// The probe handlers are injected (ADR-0015 Phase A): the single Bridge's
// own handlers in single-CR/file mode, the Supervisor's aggregating ones in
// supervisor mode — this function neither knows nor cares which.
func startMetricsServer(ctx context.Context, addr string, healthz, readyz http.HandlerFunc, log *slog.Logger) (<-chan error, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("binding metrics/health listener on %q: %w", addr, err)
	}
	log.Info("metrics server listening", "addr", ln.Addr().String())
	return serveMetrics(ctx, ln, healthz, readyz, log), nil
}

// serveMetrics runs the consolidated mux on an already-bound listener (split
// from startMetricsServer so tests can bind :0 themselves and know the
// address). The returned channel delivers at most one Serve failure;
// ErrServerClosed from the graceful ctx-cancel shutdown is not a failure.
func serveMetrics(ctx context.Context, ln net.Listener, healthz, readyz http.HandlerFunc, log *slog.Logger) <-chan error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", healthz)
	mux.HandleFunc("/readyz", readyz)
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	// context.Background() below is deliberate (gosec G118 disagrees, and
	// anchors its finding on this go statement): the goroutine only runs
	// AFTER ctx is done — that is its wake-up condition — so deriving the
	// drain deadline from ctx would hand Shutdown an already-cancelled
	// context and skip the graceful drain entirely.
	go func() { //nolint:gosec // G118: parent ctx is already done when this body runs, see above
		<-ctx.Done()
		log.Debug("shutting down metrics/health server", "timeout", metricsShutdownTimeout)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), metricsShutdownTimeout)
		defer cancel()
		// Best-effort drain; the process is exiting either way once the
		// manager finishes its own stop sequence.
		_ = srv.Shutdown(shutdownCtx)
	}()
	return errCh
}

// splitAndTrim parses a comma-separated flag into non-empty trimmed entries.
func splitAndTrim(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
