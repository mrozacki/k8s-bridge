package bridge

import (
	"context"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/mrozacki/k8s-bridge/internal/config"
	"github.com/mrozacki/k8s-bridge/internal/slurm"
	"github.com/mrozacki/k8s-bridge/internal/translate"
)

// The retention feature exists because the failure path erased its own
// evidence: the bridge failed the Slurm job and deleted the JobSet in the SAME
// tick, so the two commands the tutorial told operators to run after a failure
// (`kubectl get jobset`, `kubectl get events --field-selector
// reason=JobSetFailed`) always came back empty. Reported by a live tutorial run
// on 2026-07-27.
//
// These tests pin the three properties that make the feature safe to enable:
// it is off by default, it only ever retains the failure shapes, and a retained
// object is still eventually collected.

// withRetention copies the bridge's live config, sets the retention window, and
// stores it back — cfg is an atomic.Pointer, so it cannot be mutated in place.
func withRetention(b *Bridge, d time.Duration) {
	cfg := *b.cfgSnapshot()
	cfg.FailedJobSetRetention = config.Duration{Duration: d}
	b.setCfg(&cfg)
}

// cancelledContains reports whether the fake Slurm recorded a cancel for id.
func cancelledContains(ids []uint64, id uint64) bool {
	for _, got := range ids {
		if got == id {
			return true
		}
	}
	return false
}

// jobSetIn fetches a JobSet by name, reporting whether it still exists.
func jobSetIn(t *testing.T, b *Bridge, name string) (*jobsetv1alpha2.JobSet, bool) {
	t.Helper()
	var js jobsetv1alpha2.JobSet
	err := b.kube.Get(context.Background(), types.NamespacedName{Namespace: "slurm-jobs", Name: name}, &js)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		t.Fatalf("getting jobset %s: %v", name, err)
	}
	return &js, true
}

// TestRetentionDisabledByDefaultDeletesImmediately is the backward-compatibility
// guard: every deployment that does not set failedJobSetRetention must behave
// exactly as it did before the field existed.
func TestRetentionDisabledByDefaultDeletesImmediately(t *testing.T) {
	fs := &fakeSlurm{jobs: []slurm.Job{
		{JobID: 810, JobState: []string{"PENDING"}, Hold: false, StateReason: "BadConstraints"},
	}}
	b, _ := testBridge(t, fs, failedJobSet(t, 810, "deadline exceeded"))

	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if _, ok := jobSetIn(t, b, translate.JobSetName(810)); ok {
		t.Error("with retention unset the failed JobSet must be deleted in the same tick, as before")
	}
}

// TestRetentionKeepsFailedJobSetAndStampsDeadline covers the headline behaviour:
// the JobSet survives the tick that fails its Slurm job, and carries a machine
// readable deadline explaining why.
func TestRetentionKeepsFailedJobSetAndStampsDeadline(t *testing.T) {
	fs := &fakeSlurm{jobs: []slurm.Job{
		{JobID: 811, JobState: []string{"PENDING"}, Hold: false, StateReason: "BadConstraints"},
	}}
	b, _ := testBridge(t, fs, failedJobSet(t, 811, "deadline exceeded"))
	withRetention(b, time.Hour)
	rec := record.NewFakeRecorder(10)
	b.Recorder = rec

	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}

	js, ok := jobSetIn(t, b, translate.JobSetName(811))
	if !ok {
		t.Fatal("failed JobSet was deleted despite a 1h retention window")
	}
	raw, stamped := js.Annotations[translate.RetainUntilAnnotation]
	if !stamped {
		t.Fatalf("retained JobSet carries no %s annotation", translate.RetainUntilAnnotation)
	}
	until, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatalf("retain-until %q is not RFC3339: %v", raw, err)
	}
	if !until.After(time.Now()) {
		t.Errorf("retain-until %s is not in the future", until)
	}

	// The Slurm job must still be failed — retention changes cleanup, never the
	// job's outcome. This is the property that would make the feature dangerous
	// if it regressed: a retained JobSet whose Slurm job was left pending is the
	// original D1 bug wearing a new annotation.
	if !cancelledContains(fs.cancelled, 811) {
		t.Error("retention must not stop the bridge from cancelling the Slurm job")
	}
}

// TestRetentionExpiryDeletesJobSet proves a retained object is not a leaked one:
// once the stamped deadline has passed, ordinary cleanup collects it.
func TestRetentionExpiryDeletesJobSet(t *testing.T) {
	fs := &fakeSlurm{jobs: []slurm.Job{
		{JobID: 812, JobState: []string{"CANCELLED"}, Hold: false},
	}}
	js := failedJobSet(t, 812, "deadline exceeded")
	// Stamped by an earlier tick, already expired.
	js.Annotations = map[string]string{
		translate.RetainUntilAnnotation: time.Now().Add(-time.Minute).UTC().Format(time.RFC3339),
	}
	b, _ := testBridge(t, fs, js)
	withRetention(b, time.Hour)

	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if _, ok := jobSetIn(t, b, translate.JobSetName(812)); ok {
		t.Error("an expired retention window must not keep the JobSet alive")
	}
}

// TestRetentionUnparseableStampDoesNotPinObject: a corrupt annotation — hand
// edited, or written by a version with a different format — must not make an
// object immortal. Treat it as expired and clean up.
func TestRetentionUnparseableStampDoesNotPinObject(t *testing.T) {
	fs := &fakeSlurm{jobs: []slurm.Job{
		{JobID: 813, JobState: []string{"CANCELLED"}, Hold: false},
	}}
	js := failedJobSet(t, 813, "deadline exceeded")
	js.Annotations = map[string]string{translate.RetainUntilAnnotation: "not-a-timestamp"}
	b, _ := testBridge(t, fs, js)
	withRetention(b, time.Hour)

	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if _, ok := jobSetIn(t, b, translate.JobSetName(813)); ok {
		t.Error("an unparseable retain-until must be treated as expired, not as forever")
	}
}

// TestRetentionWindowIsNeverExtended: a JobSet whose cancel keeps failing is
// re-visited every tick. Re-stamping it each time would let it drift past its
// original deadline indefinitely, which is exactly the object leak the 7d
// validation cap is meant to prevent.
func TestRetentionWindowIsNeverExtended(t *testing.T) {
	fs := &fakeSlurm{jobs: []slurm.Job{
		{JobID: 814, JobState: []string{"PENDING"}, Hold: false, StateReason: "BadConstraints"},
	}}
	js := failedJobSet(t, 814, "deadline exceeded")
	original := time.Now().Add(30 * time.Minute).UTC().Format(time.RFC3339)
	js.Annotations = map[string]string{translate.RetainUntilAnnotation: original}
	b, _ := testBridge(t, fs, js)
	withRetention(b, 24*time.Hour)

	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	got, ok := jobSetIn(t, b, translate.JobSetName(814))
	if !ok {
		t.Fatal("JobSet inside its window was deleted")
	}
	if got.Annotations[translate.RetainUntilAnnotation] != original {
		t.Errorf("retain-until was rewritten to %q, want the original %q — the window must not creep",
			got.Annotations[translate.RetainUntilAnnotation], original)
	}
}

// TestRetentionValidation pins the config bounds: negative is an operator error
// rather than "use the default", and the upper cap keeps a typo from turning
// retention into an unbounded object leak.
func TestRetentionValidation(t *testing.T) {
	base := func() *config.Config {
		return &config.Config{
			SlurmRestURL:   "https://slurm/",
			SlurmTokenFile: "/tmp/token",
			Namespace:      "slurm-jobs",
			LocalQueue:     "main",
			PartitionMappings: []config.PartitionMapping{
				{PartitionName: "mixing", WorkloadPriorityClass: "normal-priority"},
			},
			Slurmd: config.Slurmd{
				Image:          "slurmd:test",
				ConfServer:     "ctl:6817",
				AuthSecretName: "slurm-auth-slurm",
			},
		}
	}

	for _, tc := range []struct {
		name      string
		retention time.Duration
		wantErr   bool
	}{
		{"zero is the default and valid", 0, false},
		{"an hour is valid", time.Hour, false},
		{"the 7d cap itself is valid", 7 * 24 * time.Hour, false},
		{"negative is rejected", -time.Second, true},
		{"beyond the cap is rejected", 8 * 24 * time.Hour, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			cfg.FailedJobSetRetention = config.Duration{Duration: tc.retention}
			cfg.ApplyDefaults()
			err := cfg.Validate()
			if tc.wantErr && err == nil {
				t.Errorf("retention %s: expected a validation error, got none", tc.retention)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("retention %s: unexpected validation error: %v", tc.retention, err)
			}
		})
	}
}
