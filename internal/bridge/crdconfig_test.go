package bridge

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/mrozacki/k8s-bridge/api/v1alpha1"
	"github.com/mrozacki/k8s-bridge/internal/config"
)

// crdConfigTestScheme registers the typed WorkloadMixing API so the fake
// client can serve Get/Status().Patch for it (ADR-0014 PR 2: the
// pre-migration version registered the GVK as unstructured instead).
func crdConfigTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func newWorkloadMixing(name, namespace string, spec v1alpha1.WorkloadMixingSpec) *v1alpha1.WorkloadMixing {
	return &v1alpha1.WorkloadMixing{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec:       spec,
	}
}

func validSpec() v1alpha1.WorkloadMixingSpec {
	return v1alpha1.WorkloadMixingSpec{
		SlurmRestURL: "https://slurm-restapi.slurm:6820",
		LocalQueue:   "main",
		PartitionMappings: []v1alpha1.PartitionMapping{
			{PartitionName: "mixing", WorkloadPriorityClass: "normal-priority"},
		},
		Slurmd: v1alpha1.Slurmd{
			Image:          "slurmd:test",
			ConfServer:     "ctl:6817",
			AuthSecretName: "slurm-auth-slurm",
		},
	}
}

func TestLoadConfigFromCRAppliesDefaultsAndValidates(t *testing.T) {
	cr := newWorkloadMixing("wm", "slurm-jobs", validSpec())
	kube := fake.NewClientBuilder().WithScheme(crdConfigTestScheme(t)).WithObjects(cr).Build()

	cfg, rv, err := LoadConfigFromCR(context.Background(), kube, "slurm-jobs", "wm")
	if err != nil {
		t.Fatalf("LoadConfigFromCR: %v", err)
	}
	if rv == "" {
		t.Error("expected a non-empty resourceVersion")
	}
	// ApplyDefaults must have run: pollInterval and gpuResourceName defaulted.
	if cfg.PollInterval.Duration.String() != "10s" {
		t.Errorf("pollInterval = %s, want 10s default", cfg.PollInterval.Duration)
	}
	if cfg.Slurmd.GPUResourceName != "nvidia.com/gpu" {
		t.Errorf("gpuResourceName = %q, want nvidia.com/gpu default", cfg.Slurmd.GPUResourceName)
	}
	if cfg.Namespace != "slurm-jobs" {
		t.Errorf("namespace = %q, want the CR's namespace", cfg.Namespace)
	}
}

// TestLoadConfigFromCRValidatesLikeFileLoader is the audit D6 regression:
// the CR loader used to skip Validate() entirely. A spec missing the
// required partition mapping must now fail exactly like the file loader
// would.
func TestLoadConfigFromCRValidatesLikeFileLoader(t *testing.T) {
	spec := validSpec()
	spec.PartitionMappings = nil
	cr := newWorkloadMixing("wm", "slurm-jobs", spec)
	kube := fake.NewClientBuilder().WithScheme(crdConfigTestScheme(t)).WithObjects(cr).Build()

	_, _, err := LoadConfigFromCR(context.Background(), kube, "slurm-jobs", "wm")
	if err == nil {
		t.Fatal("expected Validate() to reject a spec with zero partition mappings")
	}
	if !strings.Contains(err.Error(), "partition mapping") {
		t.Errorf("error = %v, want it to mention the missing partition mapping", err)
	}
}

// TestFromCRCopiesEveryField pins the typed conversion (ADR-0014 PR 2): a
// fully-populated spec must land field-for-field on config.Config, with
// pointer fields (privileged, sharedStorage) copied by VALUE so the config
// never aliases the CR the informer cache handed out.
func TestFromCRCopiesEveryField(t *testing.T) {
	privileged := false
	spec := v1alpha1.WorkloadMixingSpec{
		SlurmRestURL:               "https://slurm-restapi.slurm:6820",
		AllowInsecureHTTP:          false,
		SlurmCACertFile:            "/etc/slurm-ca/ca.pem",
		SlurmInsecureSkipTLSVerify: true,
		SlurmUser:                  "bridge",
		SlurmTokenFile:             "/etc/slurm-token/token",
		LocalQueue:                 "main",
		PollInterval:               "30s",
		SlurmRequestTimeout:        "2m",
		SlurmRequestsPerSecond:     12.5,
		EnablePrioritySync:         true,
		MaxUserPriority:            9000,
		CreateWorkers:              4,
		CancelOrphanedJobs:         true,
		OrphanGraceTicks:           5,
		PartitionMappings: []v1alpha1.PartitionMapping{
			{PartitionName: "mixing", WorkloadPriorityClass: "normal-priority"},
			{PartitionName: "team-b", WorkloadPriorityClass: "low-priority", LocalQueue: "team-b-queue"},
		},
		Slurmd: v1alpha1.Slurmd{
			Image:           "slurmd:test",
			ConfServer:      "ctl:6817",
			AuthSecretName:  "slurm-auth-slurm",
			GPUResourceName: "example.com/gpu",
			Privileged:      &privileged,
			SharedStorage: &v1alpha1.SharedStorage{
				NFSServer: "nfs.example.com",
				NFSPath:   "/export/home",
				MountPath: "/home",
			},
		},
		Topology: v1alpha1.Topology{
			RequiredLevel:  "example.com/rack",
			PreferredLevel: "example.com/zone",
		},
	}
	cr := newWorkloadMixing("wm", "slurm-jobs", spec)

	cfg, err := FromCR(cr)
	if err != nil {
		t.Fatalf("FromCR: %v", err)
	}

	if cfg.Namespace != "slurm-jobs" {
		t.Errorf("Namespace = %q, want the CR's namespace", cfg.Namespace)
	}
	if cfg.SlurmRestURL != spec.SlurmRestURL ||
		cfg.SlurmCACertFile != spec.SlurmCACertFile ||
		!cfg.SlurmInsecureSkipTLSVerify ||
		cfg.SlurmUser != spec.SlurmUser ||
		cfg.SlurmTokenFile != spec.SlurmTokenFile ||
		cfg.LocalQueue != spec.LocalQueue {
		t.Errorf("slurm/queue fields not copied 1:1: %+v", cfg)
	}
	if cfg.PollInterval.Duration.String() != "30s" || cfg.SlurmRequestTimeout.Duration.String() != "2m0s" {
		t.Errorf("durations = %s/%s, want 30s/2m0s", cfg.PollInterval.Duration, cfg.SlurmRequestTimeout.Duration)
	}
	if cfg.SlurmRequestsPerSecond != 12.5 || !cfg.EnablePrioritySync || cfg.MaxUserPriority != 9000 ||
		cfg.CreateWorkers != 4 || !cfg.CancelOrphanedJobs || cfg.OrphanGraceTicks != 5 {
		t.Errorf("scalar knobs not copied 1:1: %+v", cfg)
	}
	if len(cfg.PartitionMappings) != 2 ||
		cfg.PartitionMappings[1] != (config.PartitionMapping{PartitionName: "team-b", WorkloadPriorityClass: "low-priority", LocalQueue: "team-b-queue"}) {
		t.Errorf("PartitionMappings = %+v, want both mappings incl. the A1b localQueue override", cfg.PartitionMappings)
	}
	if cfg.Slurmd.Image != "slurmd:test" || cfg.Slurmd.ConfServer != "ctl:6817" ||
		cfg.Slurmd.AuthSecretName != "slurm-auth-slurm" || cfg.Slurmd.GPUResourceName != "example.com/gpu" {
		t.Errorf("Slurmd = %+v, want all fields copied", cfg.Slurmd)
	}
	if cfg.Slurmd.Privileged == nil || *cfg.Slurmd.Privileged {
		t.Error("Slurmd.Privileged = nil/true, want explicit false from the CR")
	}
	if cfg.Slurmd.Privileged == spec.Slurmd.Privileged {
		t.Error("Slurmd.Privileged aliases the CR's pointer; must be copied by value")
	}
	if cfg.Slurmd.SharedStorage == nil ||
		*cfg.Slurmd.SharedStorage != (config.SharedStorage{NFSServer: "nfs.example.com", NFSPath: "/export/home", MountPath: "/home"}) {
		t.Errorf("SharedStorage = %+v, want the CR's NFS export", cfg.Slurmd.SharedStorage)
	}
	if cfg.Topology != (config.Topology{RequiredLevel: "example.com/rack", PreferredLevel: "example.com/zone"}) {
		t.Errorf("Topology = %+v, want both levels copied", cfg.Topology)
	}
}

// TestFromCRRejectsInvalidDuration covers the one decode step the typed
// conversion still owns (the CR carries durations as strings): a malformed
// duration must fail loudly with the pre-migration error framing, which is
// what lands in the Ready condition on a failed hot-reload.
func TestFromCRRejectsInvalidDuration(t *testing.T) {
	spec := validSpec()
	spec.PollInterval = "10 parsecs"
	_, err := FromCR(newWorkloadMixing("wm", "slurm-jobs", spec))
	if err == nil {
		t.Fatal("expected an error for a malformed pollInterval")
	}
	if !strings.Contains(err.Error(), "spec does not match config schema") ||
		!strings.Contains(err.Error(), `invalid duration "10 parsecs"`) {
		t.Errorf("error = %v, want the pre-migration decode framing naming the bad duration", err)
	}

	spec = validSpec()
	spec.SlurmRequestTimeout = "soon"
	_, err = FromCR(newWorkloadMixing("wm", "slurm-jobs", spec))
	if err == nil || !strings.Contains(err.Error(), "slurmRequestTimeout") {
		t.Errorf("error = %v, want a slurmRequestTimeout parse failure", err)
	}
}

func TestLoadConfigFromCRMissingCRReturnsError(t *testing.T) {
	kube := fake.NewClientBuilder().WithScheme(crdConfigTestScheme(t)).Build()
	_, _, err := LoadConfigFromCR(context.Background(), kube, "slurm-jobs", "missing")
	if err == nil {
		t.Fatal("expected an error when the WorkloadMixing CR does not exist")
	}
}

// TestUpdateReadyConditionSetsObservedGenerationAndPreservesOtherConditions
// is the audit AUD2 regression: the previous implementation replaced
// status.conditions wholesale (losing any other condition type a different
// controller/tool might have written) and never stamped observedGeneration.
// Typed since ADR-0014 PR 2 — same pinned semantics, metav1.Condition shape.
func TestUpdateReadyConditionSetsObservedGenerationAndPreservesOtherConditions(t *testing.T) {
	cr := newWorkloadMixing("wm", "slurm-jobs", validSpec())
	cr.Generation = 7
	// Seed an unrelated condition type that must survive the Ready update.
	cr.Status.Conditions = []metav1.Condition{{
		Type:               "SomeOtherCondition",
		Status:             metav1.ConditionTrue,
		Reason:             "Preexisting",
		Message:            "set by something else",
		LastTransitionTime: metav1.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
	}}
	// WithStatusSubresource is required for the fake client to track status
	// as a distinct subresource; without it, Status().Patch 404s even though
	// the object exists (fake client quirk).
	kube := fake.NewClientBuilder().WithScheme(crdConfigTestScheme(t)).WithObjects(cr).WithStatusSubresource(cr).Build()

	if err := UpdateReadyCondition(context.Background(), kube, "slurm-jobs", "wm", true, "reconcile loop healthy"); err != nil {
		t.Fatalf("UpdateReadyCondition: %v", err)
	}

	got := &v1alpha1.WorkloadMixing{}
	if err := kube.Get(context.Background(), client.ObjectKey{Namespace: "slurm-jobs", Name: "wm"}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got.Status.Conditions) != 2 {
		t.Fatalf("conditions = %+v, want 2 (preserved + Ready)", got.Status.Conditions)
	}
	if apimeta.FindStatusCondition(got.Status.Conditions, "SomeOtherCondition") == nil {
		t.Fatal("SomeOtherCondition was dropped by UpdateReadyCondition (should preserve other condition types)")
	}
	ready := apimeta.FindStatusCondition(got.Status.Conditions, "Ready")
	if ready == nil {
		t.Fatal("Ready condition was not written")
	}
	if ready.Status != metav1.ConditionTrue {
		t.Errorf("Ready status = %v, want True", ready.Status)
	}
	if ready.Reason != "TickSucceeded" {
		t.Errorf("Ready reason = %v, want TickSucceeded", ready.Reason)
	}
	// observedGeneration must reflect the CR's metadata.generation.
	if ready.ObservedGeneration != 7 {
		t.Errorf("observedGeneration = %d, want 7", ready.ObservedGeneration)
	}
	if ready.LastTransitionTime.IsZero() {
		t.Error("lastTransitionTime not stamped on the Ready condition")
	}
}

// TestUpdateReadyConditionReflectsFailureStatus pins the False/TickFailed
// path with a truncated message.
func TestUpdateReadyConditionReflectsFailureStatus(t *testing.T) {
	cr := newWorkloadMixing("wm", "slurm-jobs", validSpec())
	kube := fake.NewClientBuilder().WithScheme(crdConfigTestScheme(t)).WithObjects(cr).WithStatusSubresource(cr).Build()

	if err := UpdateReadyCondition(context.Background(), kube, "slurm-jobs", "wm", false, "listing slurm jobs: connection refused"); err != nil {
		t.Fatalf("UpdateReadyCondition: %v", err)
	}

	got := &v1alpha1.WorkloadMixing{}
	if err := kube.Get(context.Background(), client.ObjectKey{Namespace: "slurm-jobs", Name: "wm"}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got.Status.Conditions) != 1 {
		t.Fatalf("conditions = %+v, want 1", got.Status.Conditions)
	}
	cond := got.Status.Conditions[0]
	if cond.Status != metav1.ConditionFalse {
		t.Errorf("status = %v, want False", cond.Status)
	}
	if cond.Reason != "TickFailed" {
		t.Errorf("reason = %v, want TickFailed", cond.Reason)
	}
}

func TestUpdateReadyConditionMissingCRReturnsError(t *testing.T) {
	kube := fake.NewClientBuilder().WithScheme(crdConfigTestScheme(t)).Build()
	if err := UpdateReadyCondition(context.Background(), kube, "slurm-jobs", "missing", true, "ok"); err == nil {
		t.Fatal("expected an error when the WorkloadMixing CR does not exist")
	}
}

// TestConfigReconcilerHotReloadsOnSpecChange is the A1 regression: a
// ConfigReconciler.Reconcile call must re-read the WorkloadMixing CR and
// atomically swap the result into the Bridge's live config — no restart, no
// separate "apply" step.
func TestConfigReconcilerHotReloadsOnSpecChange(t *testing.T) {
	cr := newWorkloadMixing("wm", "slurm-jobs", validSpec())
	kube := fake.NewClientBuilder().WithScheme(crdConfigTestScheme(t)).WithObjects(cr).WithStatusSubresource(cr).Build()

	b := &Bridge{log: slog.Default()}
	b.setCfg(&config.Config{Namespace: "stale", LocalQueue: "stale"})

	r := &ConfigReconciler{Kube: kube, Bridge: b, Namespace: "slurm-jobs", Name: "wm"}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: client.ObjectKey{Namespace: "slurm-jobs", Name: "wm"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := b.cfgSnapshot()
	if got.Namespace != "slurm-jobs" {
		t.Errorf("after reload, cfg.Namespace = %q, want slurm-jobs (the CR's namespace, freshly loaded)", got.Namespace)
	}
	if got.LocalQueue != "main" {
		t.Errorf("after reload, cfg.LocalQueue = %q, want main (from the CR spec)", got.LocalQueue)
	}
}

// TestConfigReconcilerIgnoresUnrelatedRequests pins the defensive
// namespace/name check: a request for a different object must be a no-op,
// leaving the Bridge's config untouched.
func TestConfigReconcilerIgnoresUnrelatedRequests(t *testing.T) {
	cr := newWorkloadMixing("wm", "slurm-jobs", validSpec())
	kube := fake.NewClientBuilder().WithScheme(crdConfigTestScheme(t)).WithObjects(cr).Build()

	b := &Bridge{log: slog.Default()}
	original := &config.Config{Namespace: "unchanged", LocalQueue: "unchanged"}
	b.setCfg(original)

	r := &ConfigReconciler{Kube: kube, Bridge: b, Namespace: "slurm-jobs", Name: "wm"}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: client.ObjectKey{Namespace: "other-ns", Name: "other-name"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if got := b.cfgSnapshot(); got != original {
		t.Error("Reconcile for an unrelated object must not touch the Bridge's config")
	}
}

// readyConditionOf fetches the CR's Ready condition through the given
// client (nil if absent).
func readyConditionOf(t *testing.T, kube client.Client, namespace, name string) *metav1.Condition {
	t.Helper()
	wm := &v1alpha1.WorkloadMixing{}
	if err := kube.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: name}, wm); err != nil {
		t.Fatalf("fetching %s/%s: %v", namespace, name, err)
	}
	return apimeta.FindStatusCondition(wm.Status.Conditions, "Ready")
}

// TestReadyConditionWriterRejectionOverridesTickHealth pins the
// ReadyConditionWriter semantics that fix the CI-caught two-writer race:
// (1) while the latest spec is rejected, tick health reports are suppressed
// — REPEATED healthy ticks must not flip the False condition back to True;
// (2) clearing the rejection resets the debounce, so the very next healthy
// tick republishes True even though the tick state never transitioned
// across the rejection window (without the reset, the stale False would
// stick forever).
func TestReadyConditionWriterRejectionOverridesTickHealth(t *testing.T) {
	cr := newWorkloadMixing("wm", "slurm-jobs", validSpec())
	kube := fake.NewClientBuilder().WithScheme(crdConfigTestScheme(t)).WithObjects(cr).WithStatusSubresource(cr).Build()
	w := &ReadyConditionWriter{Kube: kube, Namespace: "slurm-jobs", Name: "wm"}
	ctx := context.Background()

	w.ReportTick(ctx, nil)
	if cond := readyConditionOf(t, kube, "slurm-jobs", "wm"); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("Ready = %+v after a healthy tick, want True", cond)
	}

	if err := w.ReportRejection(ctx, ReasonInvalidSpec, "spec invalid: boom"); err != nil {
		t.Fatalf("ReportRejection: %v", err)
	}
	// Several healthy ticks AFTER the rejection: every one must be
	// suppressed — this is the deterministic version of the race CI hit,
	// where a tick's True landed after the rejection's False.
	for i := 0; i < 5; i++ {
		w.ReportTick(ctx, nil)
		cond := readyConditionOf(t, kube, "slurm-jobs", "wm")
		if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != ReasonInvalidSpec {
			t.Fatalf("Ready = %+v after healthy tick %d during rejection, want False/InvalidSpec to stick", cond, i+1)
		}
	}
	// A FAILING tick during rejection must not overwrite WHY the spec is
	// bad with a transient Slurm error either.
	w.ReportTick(ctx, errors.New("slurmrestd unreachable"))
	if cond := readyConditionOf(t, kube, "slurm-jobs", "wm"); cond.Reason != ReasonInvalidSpec {
		t.Fatalf("Ready reason = %q after failing tick during rejection, want InvalidSpec preserved", cond.Reason)
	}

	// Rejection lifted: the next healthy tick must write True even though
	// the tick state was "ok" both before and after the rejection window.
	w.ClearRejection()
	w.ReportTick(ctx, nil)
	cond := readyConditionOf(t, kube, "slurm-jobs", "wm")
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != ReasonTickSucceeded {
		t.Fatalf("Ready = %+v after rejection cleared + healthy tick, want True/TickSucceeded", cond)
	}
}

// TestReadyConditionWriterConcurrentTicksCannotOutraceRejection is the race
// itself, exercised under the race detector: healthy tick reports hammer the
// writer from other goroutines while a rejection lands. Whatever the
// interleaving, once ReportRejection has returned and the tick goroutines
// have drained, the condition must read False with the rejection reason —
// the mutex held ACROSS the status write is what guarantees a tick write
// can never land after the rejection's.
func TestReadyConditionWriterConcurrentTicksCannotOutraceRejection(t *testing.T) {
	cr := newWorkloadMixing("wm", "slurm-jobs", validSpec())
	kube := fake.NewClientBuilder().WithScheme(crdConfigTestScheme(t)).WithObjects(cr).WithStatusSubresource(cr).Build()
	w := &ReadyConditionWriter{Kube: kube, Namespace: "slurm-jobs", Name: "wm"}
	ctx := context.Background()

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				// Alternate outcomes so the transition debounce cannot
				// swallow the writes we are trying to race with.
				w.ReportTick(ctx, nil)
				w.ReportTick(ctx, errors.New("flap"))
			}
		}()
	}
	if err := w.ReportRejection(ctx, ReasonConflictingSpec, "conflicts with wm-other"); err != nil {
		t.Fatalf("ReportRejection: %v", err)
	}
	wg.Wait()

	cond := readyConditionOf(t, kube, "slurm-jobs", "wm")
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != ReasonConflictingSpec {
		t.Fatalf("Ready = %+v after rejection with concurrent tick reports, want False/ConflictingSpec (no tick write may land after the rejection's)", cond)
	}
}

// TestConfigReconcilerKeepsPreviousConfigOnReloadFailure is the A1 safety
// requirement implicit in "hot-reload": a CR edited into an invalid state
// must not freeze or crash the bridge — it keeps running on its last-good
// config, with the failure surfaced on the Ready condition (mirroring a
// failed tick).
func TestConfigReconcilerKeepsPreviousConfigOnReloadFailure(t *testing.T) {
	spec := validSpec()
	spec.PartitionMappings = nil // invalid: Validate() requires >=1
	cr := newWorkloadMixing("wm", "slurm-jobs", spec)
	kube := fake.NewClientBuilder().WithScheme(crdConfigTestScheme(t)).WithObjects(cr).WithStatusSubresource(cr).Build()

	b := &Bridge{log: slog.Default()}
	goodCfg := &config.Config{Namespace: "slurm-jobs", LocalQueue: "last-good"}
	b.setCfg(goodCfg)

	r := &ConfigReconciler{Kube: kube, Bridge: b, Namespace: "slurm-jobs", Name: "wm"}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: client.ObjectKey{Namespace: "slurm-jobs", Name: "wm"},
	}); err != nil {
		t.Fatalf("Reconcile should report failure via the Ready condition, not return an error: %v", err)
	}

	if got := b.cfgSnapshot(); got != goodCfg {
		t.Error("a failed reload must leave the previous, still-valid config in place")
	}

	got := &v1alpha1.WorkloadMixing{}
	if err := kube.Get(context.Background(), client.ObjectKey{Namespace: "slurm-jobs", Name: "wm"}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got.Status.Conditions) != 1 {
		t.Fatalf("conditions = %+v, want 1 (Ready=False reporting the reload failure)", got.Status.Conditions)
	}
	if got.Status.Conditions[0].Status != metav1.ConditionFalse {
		t.Errorf("Ready status = %v, want False", got.Status.Conditions[0].Status)
	}
}

// TestConfigReconcilerWriterSuppressesTickHealthAfterReloadFailure is the
// single-CR twin of the supervisor race CI caught: the hot-reload
// reconciler's Ready=False (reload failure) and the Bridge's per-tick
// Ready=True share one condition, and without coordination whichever write
// lands last wins. With the shared ReadyConditionWriter (exactly what
// main.go wires), healthy ticks reported AFTER the reload failure must not
// flip the condition back to True — and once the spec is fixed and
// reconciled, the next tick must hand the condition back to tick health
// even though the tick state never transitioned.
func TestConfigReconcilerWriterSuppressesTickHealthAfterReloadFailure(t *testing.T) {
	cr := newWorkloadMixing("wm", "slurm-jobs", validSpec())
	kube := fake.NewClientBuilder().WithScheme(crdConfigTestScheme(t)).WithObjects(cr).WithStatusSubresource(cr).Build()
	ctx := context.Background()

	b := &Bridge{log: slog.Default()}
	b.setCfg(&config.Config{Namespace: "slurm-jobs", LocalQueue: "last-good"})
	w := &ReadyConditionWriter{Kube: kube, Namespace: "slurm-jobs", Name: "wm"}
	r := &ConfigReconciler{Kube: kube, Bridge: b, Namespace: "slurm-jobs", Name: "wm", Writer: w}
	req := reconcile.Request{NamespacedName: client.ObjectKey{Namespace: "slurm-jobs", Name: "wm"}}

	// Healthy bridge: tick health owns the condition.
	w.ReportTick(ctx, nil)
	if cond := readyConditionOf(t, kube, "slurm-jobs", "wm"); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("Ready = %+v after healthy tick, want True", cond)
	}

	// Break the spec and reconcile: the reload failure must win the
	// condition — and KEEP it through any number of later healthy ticks
	// (the exact sequence that raced before the shared writer existed).
	if err := kube.Get(ctx, client.ObjectKey{Namespace: "slurm-jobs", Name: "wm"}, cr); err != nil {
		t.Fatal(err)
	}
	cr.Spec.PartitionMappings = nil
	if err := kube.Update(ctx, cr); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile (invalid spec): %v", err)
	}
	for i := 0; i < 5; i++ {
		w.ReportTick(ctx, nil)
	}
	cond := readyConditionOf(t, kube, "slurm-jobs", "wm")
	if cond == nil || cond.Status != metav1.ConditionFalse || !strings.Contains(cond.Message, "hot-reload failed, keeping previous config") {
		t.Fatalf("Ready = %+v after reload failure + healthy ticks, want the False reload-failure report to stick", cond)
	}
	// The reason deliberately stays TickFailed on this path (the historic
	// single-CR reason; operators' greps/alerts must survive unchanged).
	if cond.Reason != ReasonTickFailed {
		t.Errorf("Ready reason = %q, want %q (single-CR compatibility)", cond.Reason, ReasonTickFailed)
	}

	// Fix the spec, reconcile, tick: tick health owns the condition again.
	if err := kube.Get(ctx, client.ObjectKey{Namespace: "slurm-jobs", Name: "wm"}, cr); err != nil {
		t.Fatal(err)
	}
	cr.Spec = validSpec()
	if err := kube.Update(ctx, cr); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile (fixed spec): %v", err)
	}
	w.ReportTick(ctx, nil)
	cond = readyConditionOf(t, kube, "slurm-jobs", "wm")
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != ReasonTickSucceeded {
		t.Fatalf("Ready = %+v after fixed spec + healthy tick, want True/TickSucceeded (stale False must not stick)", cond)
	}
}
