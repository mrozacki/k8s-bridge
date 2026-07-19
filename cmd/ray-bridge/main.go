// ray-bridge prototype: brings the inner workloads of a shared, long-lived
// KubeRay RayCluster under Kueue admission (pin-gate model, ADR-0013). For each
// inner RayJob it stands up dedicated Ray-worker capacity as a Kueue-admitted
// JobSet advertising wm-job-<id>; the RayJob's entrypointResources require that
// resource, so its driver cannot run until Kueue admits the workers. It is a
// sibling binary to k8s-bridge, sharing the internal/kueue library (ADR-0012).
// See docs/adr/0006, 0012, 0013.
//
// SECURITY CAVEAT (--enable-webhook, default false): the pin-gate invariant is
// only ENFORCED when the admission webhook is serving. With the webhook
// disabled, nothing injects the pin resource into inner RayJobs, so any inner
// RayJob that does not self-declare entrypointResources runs on the shared
// cluster WITHOUT passing Kueue admission — while the bridge still creates
// dedicated workers for it. The default stays false only because the webhook
// needs a serving certificate (a deploy-time concern); production deployments
// with untrusted submitters must enable it. main logs a prominent warning at
// startup when the webhook is off.
package main

import (
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/mrozacki/k8s-bridge/internal/config"
	"github.com/mrozacki/k8s-bridge/internal/ray"
	"github.com/mrozacki/k8s-bridge/internal/raybridge"
	"github.com/mrozacki/k8s-bridge/internal/rayconfig"
	"github.com/mrozacki/k8s-bridge/internal/raywebhook"
)

func main() {
	configPath := flag.String("config", "config/ray-example-config.yaml", "path to ray-bridge config file")
	metricsAddr := flag.String("metrics-addr", ":8080", "metrics listen address (empty disables)")
	healthAddr := flag.String("health-addr", ":8081", "health/readiness probe listen address (empty disables)")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, or error")
	leaderElect := flag.Bool("leader-elect", true, "enable leader election before starting the reconciler")
	// L9: short lease/renew so a rolling handover completes quickly. Paired with
	// the chart's Recreate strategy, which is the actual rollout-deadlock fix.
	leaseDuration := flag.Duration("leader-lease-duration", 15*time.Second, "leader-election lease duration")
	renewDeadline := flag.Duration("leader-renew-deadline", 10*time.Second, "leader-election renew deadline")
	// The privileged-image trust anchor is set by the platform admin at deploy
	// time, not by the (untrusted) RayJob author — same rationale as the Slurm
	// side's --allowed-slurmd-images (security audit C1).
	allowedImages := flag.String("allowed-worker-images", "", "comma-separated allowlist of permitted Ray worker image prefixes; empty allows any")
	// The pin-injecting admission webhook (internal/raywebhook) is OFF by
	// default: it needs a serving certificate mounted into the pod (cert-manager
	// or manual) and a MutatingWebhookConfiguration (deploy/ray-bridge/webhook.yaml).
	// Enabling it without certs makes the manager fail to start its webhook
	// server, so this stays opt-in. See the package godoc's SECURITY CAVEAT:
	// with the webhook off, the pin gate is unenforced for inner RayJobs that
	// do not self-declare entrypointResources.
	enableWebhook := flag.Bool("enable-webhook", false, "serve the pin-injecting admission webhook on :9443 (requires a serving cert; see deploy/ray-bridge/webhook.yaml). WARNING: when false, nothing injects the pin resource, so inner RayJobs without self-declared entrypointResources bypass Kueue admission entirely")
	// Deploy-time knobs, same rationale and defaults as the Slurm side's
	// --kube-api-qps/--kube-api-burst (finding #9, suite-E scale run).
	kubeQPS := flag.Float64("kube-api-qps", 20, "client-go QPS ceiling for the Kubernetes API client")
	kubeBurst := flag.Int("kube-api-burst", 40, "client-go burst ceiling for the Kubernetes API client")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parseLogLevel(*logLevel)}))
	// Route controller-runtime's logr into slog so its internal errors (cache
	// sync, watch failures) are visible — the hard-won lesson from the Slurm
	// side's first live deploy (experiment 09).
	ctrl.SetLogger(logr.FromSlogHandler(log.Handler()))

	cfg, err := rayconfig.Load(*configPath)
	if err != nil {
		log.Error("loading config", "error", err)
		os.Exit(1)
	}

	allowlist := splitAndTrim(*allowedImages)
	if len(allowlist) == 0 {
		log.Warn("no --allowed-worker-images set: any image in config will run as the Ray worker; set an allowlist in production")
	} else if err := config.ValidateImageAllowed(cfg.Worker.Image, allowlist); err != nil {
		log.Error("worker image rejected by allowlist", "error", err)
		os.Exit(1)
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

	restCfg, err := ctrl.GetConfig()
	if err != nil {
		log.Error("loading kubeconfig", "error", err)
		os.Exit(1)
	}
	// Explicit, modest client-side rate ceiling (audit P8b parity): a decision,
	// not an accident inherited from the -1 (unlimited) default.
	if *kubeQPS <= 0 || *kubeBurst < int(*kubeQPS) {
		log.Error("invalid --kube-api-qps/--kube-api-burst: qps must be > 0 and burst >= qps", "qps", *kubeQPS, "burst", *kubeBurst)
		os.Exit(1)
	}
	restCfg.QPS = float32(*kubeQPS)
	restCfg.Burst = *kubeBurst

	leNamespace := os.Getenv("POD_NAMESPACE")
	if leNamespace == "" {
		leNamespace = cfg.Namespace
	}
	mgr, err := manager.New(restCfg, manager.Options{
		Scheme: scheme,
		// Scope the cache to the bridge's namespace (namespaced RBAC, least
		// privilege); an empty POD_NAMESPACE falls back to all-namespaces for
		// local `go run`, which then needs cluster-scoped RBAC.
		Cache: cacheScopedToNamespace(os.Getenv("POD_NAMESPACE"), cfg.Namespace),
		// Live (uncached) unstructured reads: the Slurm side found a cached
		// unstructured informer deadlocked the first reconcile (B7);
		// RayJob/Workload reads here are small, so read them live and keep only
		// STRUCTURED (JobSet) reads cached.
		Client:                  client.Options{Cache: &client.CacheOptions{Unstructured: false}},
		Metrics:                 metricsServerOptions(*metricsAddr),
		HealthProbeBindAddress:  *healthAddr,
		LeaderElection:          *leaderElect,
		LeaderElectionID:        "ray-bridge-leader",
		LeaderElectionNamespace: leNamespace,
		LeaseDuration:           leaseDuration,
		RenewDeadline:           renewDeadline,
	})
	if err != nil {
		log.Error("creating controller-runtime manager", "error", err)
		os.Exit(1)
	}

	reconciler := &raybridge.Reconciler{
		Kube: mgr.GetClient(),
		Cfg:  cfg,
		Log:  log,
		//nolint:staticcheck // classic record.EventRecorder API (Reconciler.Recorder's type); migration to GetEventRecorder is separate, matching the Slurm side.
		Recorder: mgr.GetEventRecorderFor("ray-bridge"),
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		log.Error("wiring ray-bridge reconciler", "error", err)
		os.Exit(1)
	}

	if *enableWebhook {
		// The manager serves this on its webhook server (:9443 by default),
		// which requires a serving cert mounted at the standard path. Pair with
		// deploy/ray-bridge/webhook.yaml (the MutatingWebhookConfiguration).
		raywebhook.NewHandler(cfg, admission.NewDecoder(scheme)).SetupWithManager(mgr)
		log.Info("pin-injecting admission webhook enabled", "path", "/mutate-ray-io-v1-rayjob")
	} else {
		// B5: the pin gate is only ENFORCED by the webhook. Say so loudly —
		// this is the single most security-relevant default in this binary,
		// and an operator reading startup logs must not be able to miss it.
		// Changing the default is a product decision tracked separately.
		log.Warn("ADMISSION WEBHOOK DISABLED (--enable-webhook=false, the default): " +
			"nothing injects the pin resource into inner RayJobs, so any inner RayJob that does not " +
			"self-declare entrypointResources will run on the shared RayCluster WITHOUT passing Kueue " +
			"admission, while the bridge still creates dedicated workers for it. " +
			"Enable the webhook (with a serving cert; see deploy/ray-bridge/webhook.yaml) in any " +
			"deployment where submitters are not fully trusted to declare the pin themselves")
	}

	// The signal-handler context is created before the probe wiring below so the
	// cache-sync goroutine can share the manager's lifetime.
	ctx := ctrl.SetupSignalHandler()

	// Probe surface (B2): /healthz stays healthz.Ping — pure process liveness,
	// mirroring the Slurm side's deliberately permissive Healthy() (a struggling
	// dependency should page the operator, not restart the pod). /readyz used to
	// be Ping too, which meant a ray-bridge whose informers never synced (bad
	// RBAC, unreachable API server, missing CRDs) still reported Ready and
	// received traffic-shaped trust it had not earned. It now gates on the real
	// startup milestones: informer caches synced and — when the webhook serves —
	// the webhook server actually listening (StartedChecker).
	if *healthAddr != "" {
		if err := mgr.AddHealthzCheck("ping", healthz.Ping); err != nil {
			log.Error("adding healthz check", "error", err)
			os.Exit(1)
		}
		// Create the informers this binary watches EAGERLY, before the manager
		// starts. Controllers register their watches only when they start —
		// which, under leader election, is after this replica wins — so without
		// this the WaitForCacheSync below would return once the (empty) cache
		// starts, a vacuous "synced". With the informers pre-created, readiness
		// means the initial LIST+WATCH of RayJobs and JobSets completed on THIS
		// replica, leader or standby.
		rayJob := &unstructured.Unstructured{}
		rayJob.SetGroupVersionKind(ray.RayJobGVK)
		for _, obj := range []client.Object{rayJob, &jobsetv1alpha2.JobSet{}} {
			if _, err := mgr.GetCache().GetInformer(ctx, obj); err != nil {
				log.Error("creating informer for readiness gate", "gvk", obj.GetObjectKind().GroupVersionKind().String(), "error", err)
				os.Exit(1)
			}
		}
		synced := make(chan struct{})
		go func() {
			// WaitForCacheSync blocks until the manager starts its caches, then
			// until the informers created above finish their initial sync. On
			// shutdown before sync it returns false and the channel stays open —
			// readyz stays failing, which is correct for a dying process.
			if mgr.GetCache().WaitForCacheSync(ctx) {
				close(synced)
			}
		}()
		if err := mgr.AddReadyzCheck("informer-sync", informerSyncChecker(synced)); err != nil {
			log.Error("adding readyz check", "error", err)
			os.Exit(1)
		}
		if *enableWebhook {
			if err := mgr.AddReadyzCheck("webhook-server", mgr.GetWebhookServer().StartedChecker()); err != nil {
				log.Error("adding webhook readyz check", "error", err)
				os.Exit(1)
			}
		}
	}

	log.Info("starting ray-bridge manager", "leaderElect", *leaderElect, "namespace", cfg.Namespace)
	if err := mgr.Start(ctx); err != nil && ctx.Err() == nil {
		log.Error("manager exited", "error", err)
		os.Exit(1)
	}
	log.Info("ray-bridge stopped")
}

// informerSyncChecker returns a healthz.Checker that fails until synced is
// closed (by the WaitForCacheSync goroutine in main). Split out and given a
// channel rather than the manager so the before/after behavior is unit-
// testable without standing up a manager, and so the probe handler never
// blocks: it polls the channel with a non-blocking select instead of calling
// WaitForCacheSync itself, which would hold the probe request open until the
// kubelet's probe timeout on a not-yet-synced cache.
func informerSyncChecker(synced <-chan struct{}) healthz.Checker {
	return func(_ *http.Request) error {
		select {
		case <-synced:
			return nil
		default:
			return errors.New("informer caches not synced yet")
		}
	}
}

// metricsServerOptions disables the metrics server on an empty address,
// otherwise binds it to addr.
func metricsServerOptions(addr string) metricsserver.Options {
	if addr == "" {
		return metricsserver.Options{BindAddress: "0"}
	}
	return metricsserver.Options{BindAddress: addr}
}

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

func splitAndTrim(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
