package bridge

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/mrozacki/k8s-bridge/internal/config"
	"github.com/mrozacki/k8s-bridge/internal/slurm"
)

func TestCapUserPriority(t *testing.T) {
	cases := []struct {
		name      string
		max       int32
		in        int64
		want      int64
		wantClamp bool
	}{
		{"no cap keeps value", 0, 5000, 5000, false},
		{"negative floors to zero", 0, -3, 0, true},
		{"under cap unchanged", 1000, 500, 500, false},
		{"over cap clamps down", 1000, 999999, 1000, true},
		{"exactly cap unchanged", 1000, 1000, 1000, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := &Bridge{}
			b.setCfg(&config.Config{MaxUserPriority: tc.max})
			got, clamped := b.capUserPriority(tc.in)
			if got != tc.want || clamped != tc.wantClamp {
				t.Errorf("capUserPriority(%d) with max %d = (%d,%v), want (%d,%v)",
					tc.in, tc.max, got, clamped, tc.want, tc.wantClamp)
			}
		})
	}
}

func TestSafeUint32(t *testing.T) {
	cases := []struct {
		in   int64
		want uint32
	}{
		{-1, 0},
		{0, 0},
		{42, 42},
		{math.MaxUint32, math.MaxUint32},
		{math.MaxUint32 + 1, math.MaxUint32},
		{math.MaxInt64, math.MaxUint32},
	}
	for _, tc := range cases {
		if got := safeUint32(tc.in); got != tc.want {
			t.Errorf("safeUint32(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// TestTruncateRunesIsRuneSafe is the audit minor regression: byte-slicing a
// QuotaReserved message containing multi-byte UTF-8 (quoted names or
// non-ASCII text upstream may embed) could split a rune and corrupt the
// tail of the Slurm comment. truncateRunes must cut on rune boundaries.
func TestTruncateRunesIsRuneSafe(t *testing.T) {
	// "é" is a 2-byte rune; repeat well past quotaReasonMaxLen so both a
	// byte-based and a rune-based truncation are exercised meaningfully.
	s := strings.Repeat("é", 100)
	got := truncateRunes(s, quotaReasonMaxLen)
	if n := len([]rune(got)); n != quotaReasonMaxLen {
		t.Errorf("truncateRunes produced %d runes, want %d", n, quotaReasonMaxLen)
	}
	if !strings_ValidUTF8(got) {
		t.Errorf("truncateRunes produced invalid UTF-8: %q", got)
	}
}

// TestTruncateRunesLeavesShortStringsAlone confirms the no-op fast path.
func TestTruncateRunesLeavesShortStringsAlone(t *testing.T) {
	if got := truncateRunes("short", 96); got != "short" {
		t.Errorf("truncateRunes(short) = %q, want unchanged", got)
	}
}

// strings_ValidUTF8 avoids importing unicode/utf8 just for this one check by
// round-tripping through []rune -> string, which cannot represent invalid
// UTF-8 byte sequences as anything other than the replacement character.
func strings_ValidUTF8(s string) bool {
	return string([]rune(s)) == s
}

// TestAdmissionStatusBranches is a table-driven pass over every branch of
// admissionStatus not already covered by the truncation regression below:
// nil Workload, Admitted=True, QuotaReserved=False with a short (untruncated)
// reason, and no relevant conditions at all ("queued for admission").
func TestAdmissionStatusBranches(t *testing.T) {
	cases := []struct {
		name string
		wl   *unstructured.Unstructured
		want string
	}{
		{
			name: "nil workload means capacity request still being created",
			wl:   nil,
			want: "wm: creating capacity request",
		},
		{
			name: "admitted workload is provisioning nodes",
			wl: &unstructured.Unstructured{Object: map[string]any{
				"status": map[string]any{"conditions": []any{
					map[string]any{"type": "Admitted", "status": "True"},
				}},
			}},
			want: "wm: capacity admitted, provisioning nodes",
		},
		{
			name: "quota reserved false with a short reason is passed through untruncated",
			wl: &unstructured.Unstructured{Object: map[string]any{
				"status": map[string]any{"conditions": []any{
					map[string]any{"type": "QuotaReserved", "status": "False", "message": "insufficient quota for cpu"},
				}},
			}},
			want: "wm: waiting for quota: insufficient quota for cpu",
		},
		{
			name: "no relevant conditions falls back to queued for admission",
			wl:   &unstructured.Unstructured{Object: map[string]any{"status": map[string]any{}}},
			want: "wm: queued for admission",
		},
		{
			name: "quota reserved true (message present but status not False) is not surfaced as a wait reason",
			wl: &unstructured.Unstructured{Object: map[string]any{
				"status": map[string]any{"conditions": []any{
					map[string]any{"type": "QuotaReserved", "status": "True", "message": "reserved fine"},
				}},
			}},
			want: "wm: queued for admission",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got, _ := admissionStatus(tc.wl); got != tc.want {
				t.Errorf("admissionStatus() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestPropagateStatusSkipsUnchangedComment pins the early-return branch:
// when the Slurm job's comment already equals the computed admissionStatus,
// propagateStatus must not call SetJobComment at all.
func TestPropagateStatusSkipsUnchangedComment(t *testing.T) {
	fs := &fakeSlurm{}
	b, _ := testBridge(t, fs)
	wl := admittedWorkload("slurm-job-95") // Admitted=True -> "wm: capacity admitted, provisioning nodes"
	job := &slurm.Job{JobID: 95, Comment: "wm: capacity admitted, provisioning nodes"}

	b.propagateStatus(context.Background(), job, wl)

	if _, called := fs.comments[95]; called {
		t.Error("SetJobComment must not be called when the comment already matches admissionStatus")
	}
}

// TestPropagateStatusWritesChangedComment is the counterpart: a stale
// comment must be overwritten with the freshly computed status.
func TestPropagateStatusWritesChangedComment(t *testing.T) {
	fs := &fakeSlurm{}
	b, _ := testBridge(t, fs)
	wl := admittedWorkload("slurm-job-96")
	job := &slurm.Job{JobID: 96, Comment: "wm: queued for admission"}

	b.propagateStatus(context.Background(), job, wl)

	want := "wm: capacity admitted, provisioning nodes"
	if got := fs.comments[96]; got != want {
		t.Errorf("comment = %q, want %q", got, want)
	}
}

// TestAdmissionStatusTruncatesLongQuotaReason pins admissionStatus's use of
// truncateRunes end-to-end via the QuotaReserved condition message path.
func TestAdmissionStatusTruncatesLongQuotaReason(t *testing.T) {
	longReason := strings.Repeat("x", 200)
	wl := &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{"conditions": []any{
			map[string]any{"type": "QuotaReserved", "status": "False", "message": longReason},
		}},
	}}
	got, _ := admissionStatus(wl)
	wantPrefix := "wm: waiting for quota: "
	if !strings.HasPrefix(got, wantPrefix) {
		t.Fatalf("admissionStatus = %q, want prefix %q", got, wantPrefix)
	}
	reasonPart := strings.TrimPrefix(got, wantPrefix)
	if n := len([]rune(reasonPart)); n != quotaReasonMaxLen {
		t.Errorf("truncated reason has %d runes, want %d", n, quotaReasonMaxLen)
	}
}

// TestApplyPriorityDirectiveLogsAckFailureButStillApplied is the audit minor
// regression: a failing SetJobAdminComment (the ack write back to Slurm)
// used to be silently swallowed. It must not be fatal — the Workload patch
// already succeeded — but it must be observable. We can't assert on log
// output directly here without a custom handler, so we assert the documented
// behavior instead: applyPriorityDirective returns normally (no panic) and
// the priority was applied to the Workload despite the ack failure.
func TestApplyPriorityDirectiveHandlesAckFailureNonFatally(t *testing.T) {
	fs := &fakeSlurm{}
	b, kube := testBridge(t, fs)
	wl := admittedWorkload("slurm-job-70")
	if err := kube.Create(context.Background(), wl); err != nil {
		t.Fatal(err)
	}
	// Force the ack write (SetJobAdminComment) to fail.
	fsErr := &fakeSlurmAdminCommentErr{fakeSlurm: fs, err: errors.New("slurmrestd unavailable")}
	b.slurm = fsErr

	job := &slurm.Job{JobID: 70, AdminComment: "wm:prio=250"}
	b.applyPriorityDirective(context.Background(), job, wl)

	got, found, _ := unstructured.NestedInt64(wl.Object, "spec", "priority")
	if !found || got != 250 {
		t.Errorf("workload priority = %v (found=%v), want 250 applied despite the ack failure", got, found)
	}
}

// fakeSlurmAdminCommentErr wraps fakeSlurm to force SetJobAdminComment to
// fail while delegating everything else, for TestApplyPriorityDirectiveHandlesAckFailureNonFatally.
type fakeSlurmAdminCommentErr struct {
	*fakeSlurm
	err error
}

func (f *fakeSlurmAdminCommentErr) SetJobAdminComment(_ context.Context, _ uint64, _ string) error {
	return f.err
}

// TestApplyPriorityDirectiveHappyPathSetsWorkloadPriorityAndAcks is the
// ADR-0009 headline-feature regression the audit flagged as untested: a job
// carrying "wm:prio=N" in admin_comment must (a) merge-patch the Workload's
// spec.priority to N and (b) acknowledge via SetJobAdminComment with
// "wm:prio-applied=N", so the operator sees the directive took effect.
func TestApplyPriorityDirectiveHappyPathSetsWorkloadPriorityAndAcks(t *testing.T) {
	fs := &fakeSlurm{}
	b, kube := testBridge(t, fs)
	wl := admittedWorkload("slurm-job-80")
	if err := kube.Create(context.Background(), wl); err != nil {
		t.Fatal(err)
	}

	job := &slurm.Job{JobID: 80, AdminComment: "wm:prio=250"}
	b.applyPriorityDirective(context.Background(), job, wl)

	got, found, _ := unstructured.NestedInt64(wl.Object, "spec", "priority")
	if !found || got != 250 {
		t.Errorf("workload priority = %v (found=%v), want 250", got, found)
	}
	if ack := fs.adminComments[80]; ack != "wm:prio-applied=250" {
		t.Errorf("admin_comment ack = %q, want %q", ack, "wm:prio-applied=250")
	}
}

// TestApplyPriorityDirectiveInvalidValueDoesNothing pins the failure mode of
// a malformed directive (non-numeric priority): applyPriorityDirective must
// neither patch the Workload nor write an ack, and it must not panic/crash.
func TestApplyPriorityDirectiveInvalidValueDoesNothing(t *testing.T) {
	fs := &fakeSlurm{}
	b, kube := testBridge(t, fs)
	wl := admittedWorkload("slurm-job-81")
	if err := kube.Create(context.Background(), wl); err != nil {
		t.Fatal(err)
	}
	origPrio, _, _ := unstructured.NestedInt64(wl.Object, "spec", "priority")

	job := &slurm.Job{JobID: 81, AdminComment: "wm:prio=not-a-number"}
	b.applyPriorityDirective(context.Background(), job, wl)

	got, _, _ := unstructured.NestedInt64(wl.Object, "spec", "priority")
	if got != origPrio {
		t.Errorf("workload priority changed to %v for an invalid directive, want unchanged %v", got, origPrio)
	}
	if _, wrote := fs.adminComments[81]; wrote {
		t.Error("SetJobAdminComment must not be called for an invalid directive")
	}
}

// syncTestJobSet returns a bare owned JobSet ("slurm-job-<id>") with no
// synced-priority annotation, suitable as the starting point for the
// syncPriority state-machine tests below.
func syncTestJobSet(t *testing.T, id uint64) *jobsetv1alpha2.JobSet {
	t.Helper()
	return ownedJobSet(t, id)
}

// workloadWithPriority is admittedWorkload with spec.priority overridden —
// syncPriority only reads spec.priority and status.conditions, both of which
// admittedWorkload already sets up realistically.
func workloadWithPriority(jobsetName string, priority int64) *unstructured.Unstructured {
	wl := admittedWorkload(jobsetName)
	_ = unstructured.SetNestedField(wl.Object, priority, "spec", "priority")
	return wl
}

// TestSyncPriorityFirstContactMirrorsKueueToSlurmAndRemembers is the
// "no annotation yet" branch: on first contact the bridge must treat Kueue
// as the source of truth, push the Workload's priority into Slurm via
// SetJobPriority, and remember it on the JobSet annotation for future ticks.
func TestSyncPriorityFirstContactMirrorsKueueToSlurmAndRemembers(t *testing.T) {
	fs := &fakeSlurm{}
	js := syncTestJobSet(t, 90)
	b, kube := testBridge(t, fs, js)
	wl := workloadWithPriority("slurm-job-90", 777)
	job := &slurm.Job{JobID: 90}

	b.syncPriority(context.Background(), js, job, wl)

	if got := fs.priorities[90]; got != 777 {
		t.Errorf("SetJobPriority = %v, want 777 (mirrored from Workload on first contact)", got)
	}
	updated := &jobsetv1alpha2.JobSet{}
	if err := kube.Get(context.Background(), client.ObjectKey{Namespace: "slurm-jobs", Name: js.Name}, updated); err != nil {
		t.Fatal(err)
	}
	if got := updated.Annotations[syncedPriorityAnnotation]; got != "777" {
		t.Errorf("synced-priority annotation = %q, want \"777\"", got)
	}
}

// TestSyncPrioritySlurmSideDeviationPatchesWorkload is the "Slurm side
// deviates" branch: once a priority is remembered, a Slurm-side change
// (scontrol update priority=) must be forwarded to the Workload via a
// merge-patch, and the new value remembered.
func TestSyncPrioritySlurmSideDeviationPatchesWorkload(t *testing.T) {
	fs := &fakeSlurm{}
	js := syncTestJobSet(t, 91)
	js.Annotations = map[string]string{syncedPriorityAnnotation: "100"}
	b, kube := testBridge(t, fs, js)
	wl := workloadWithPriority("slurm-job-91", 100)
	if err := kube.Create(context.Background(), wl); err != nil {
		t.Fatal(err)
	}
	// Slurm side moved to 500 (job is not held, so this branch fires).
	job := &slurm.Job{JobID: 91, Priority: slurm.Uint64NoVal{Set: true, Number: 500}}

	b.syncPriority(context.Background(), js, job, wl)

	got, found, _ := unstructured.NestedInt64(wl.Object, "spec", "priority")
	if !found || got != 500 {
		t.Errorf("workload priority = %v (found=%v), want 500 (patched from Slurm-side change)", got, found)
	}
	updated := &jobsetv1alpha2.JobSet{}
	if err := kube.Get(context.Background(), client.ObjectKey{Namespace: "slurm-jobs", Name: js.Name}, updated); err != nil {
		t.Fatal(err)
	}
	if got := updated.Annotations[syncedPriorityAnnotation]; got != "500" {
		t.Errorf("synced-priority annotation = %q, want \"500\"", got)
	}
	if _, called := fs.priorities[91]; called {
		t.Error("SetJobPriority must not be called on the Slurm-side-deviation branch (Slurm is already correct)")
	}
}

// TestSyncPriorityKueueSideDeviationMirrorsToSlurm is the "Kueue side moved"
// branch: a direct kubectl patch/edit of the Workload's priority must be
// mirrored back into Slurm via SetJobPriority.
func TestSyncPriorityKueueSideDeviationMirrorsToSlurm(t *testing.T) {
	fs := &fakeSlurm{}
	js := syncTestJobSet(t, 92)
	js.Annotations = map[string]string{syncedPriorityAnnotation: "100"}
	b, kube := testBridge(t, fs, js)
	// Workload now shows 300 while Slurm still reports the remembered 100 —
	// and the job IS held, so the Slurm-side branch is not the one that fires
	// (its guard is slurmPrio != synced && !IsHeld(); here slurmPrio==synced).
	wl := workloadWithPriority("slurm-job-92", 300)
	if err := kube.Create(context.Background(), wl); err != nil {
		t.Fatal(err)
	}
	job := &slurm.Job{JobID: 92, Priority: slurm.Uint64NoVal{Set: true, Number: 100}}

	b.syncPriority(context.Background(), js, job, wl)

	if got := fs.priorities[92]; got != 300 {
		t.Errorf("SetJobPriority = %v, want 300 (mirrored from Kueue-side change)", got)
	}
	updated := &jobsetv1alpha2.JobSet{}
	if err := kube.Get(context.Background(), client.ObjectKey{Namespace: "slurm-jobs", Name: js.Name}, updated); err != nil {
		t.Fatal(err)
	}
	if got := updated.Annotations[syncedPriorityAnnotation]; got != "300" {
		t.Errorf("synced-priority annotation = %q, want \"300\"", got)
	}
}

// TestSyncPriorityNoExternalChangeIsANoOpAcrossTwoTicks is the
// loop-prevention property: once first contact has synced Slurm and Kueue to
// the same value, two more consecutive calls with NO external change on
// either side must perform zero mutation calls (no SetJobPriority, no
// Workload patch, no JobSet annotation update) — otherwise the sync would
// fight itself forever.
func TestSyncPriorityNoExternalChangeIsANoOpAcrossTwoTicks(t *testing.T) {
	fs := &fakeSlurmCallCounter{fakeSlurm: &fakeSlurm{}}
	js := syncTestJobSet(t, 93)
	b, kube := testBridge(t, fs.fakeSlurm, js)
	b.slurm = fs
	wl := workloadWithPriority("slurm-job-93", 200)
	if err := kube.Create(context.Background(), wl); err != nil {
		t.Fatal(err)
	}
	job := &slurm.Job{JobID: 93, Priority: slurm.Uint64NoVal{Set: true, Number: 200}}

	// First contact: this DOES mutate (mirrors Kueue -> Slurm once).
	b.syncPriority(context.Background(), js, job, wl)
	if got := fs.fakeSlurm.priorities[93]; got != 200 {
		t.Fatalf("first-contact SetJobPriority = %v, want 200", got)
	}
	if fs.setPriorityCalls != 1 {
		t.Fatalf("first-contact SetJobPriority calls = %d, want 1", fs.setPriorityCalls)
	}

	// Refresh js from the fake client (rememberSyncedPriority wrote the
	// annotation via Update, which bumps resourceVersion) before the next
	// two ticks, mirroring how the real tick loop re-reads its snapshot.
	current := &jobsetv1alpha2.JobSet{}
	if err := kube.Get(context.Background(), client.ObjectKey{Namespace: "slurm-jobs", Name: js.Name}, current); err != nil {
		t.Fatal(err)
	}

	// Now wrap the kube client with a Patch interceptor so a Workload patch
	// in the steady state (which would indicate the loop fighting itself) is
	// directly observable, not just inferred from the Workload's value.
	var patchCalls int
	watchable, ok := kube.(client.WithWatch)
	if !ok {
		t.Fatalf("test fake client %T does not implement client.WithWatch", kube)
	}
	b.kube = interceptor.NewClient(watchable, interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			patchCalls++
			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	fs.setPriorityCalls = 0 // reset after the (expected) first-contact call

	for i := 0; i < 2; i++ {
		b.syncPriority(context.Background(), current, job, wl)
	}

	if fs.setPriorityCalls != 0 {
		t.Errorf("SetJobPriority called %d times across two steady-state ticks, want 0", fs.setPriorityCalls)
	}
	if patchCalls != 0 {
		t.Errorf("Workload Patch called %d times across two steady-state ticks, want 0", patchCalls)
	}

	got, _, _ := unstructured.NestedInt64(wl.Object, "spec", "priority")
	if got != 200 {
		t.Errorf("workload priority drifted to %v, want unchanged 200", got)
	}
}

// fakeSlurmCallCounter wraps fakeSlurm to count SetJobPriority invocations
// separately from the recorded map (which a same-value call would leave
// looking identical), for the loop-prevention regression above.
type fakeSlurmCallCounter struct {
	*fakeSlurm
	setPriorityCalls int
}

func (f *fakeSlurmCallCounter) SetJobPriority(ctx context.Context, id uint64, priority uint32) error {
	f.setPriorityCalls++
	return f.fakeSlurm.SetJobPriority(ctx, id, priority)
}

// quotaWorkload fabricates a Workload with QuotaReserved=False and the given
// message, simulating a not-yet-admitted job waiting on quota.
func quotaWorkload(message string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{"conditions": []any{
			map[string]any{"type": "QuotaReserved", "status": "False", "message": message},
		}},
	}}
}

// TestPropagateStatusThrottlesVolatileNumberOnlyChurn is the P3 regression
// (a): testing saw a 2500-comment burst because the QuotaReserved message
// embeds volatile numbers that change almost every tick. When only those
// numbers move and the status CLASS stays the same (still waiting on quota),
// a second propagateStatus call within commentRewriteMinInterval must NOT
// call SetJobComment again.
func TestPropagateStatusThrottlesVolatileNumberOnlyChurn(t *testing.T) {
	fs := &fakeSlurm{}
	b, _ := testBridge(t, fs)
	job := &slurm.Job{JobID: 300}

	wl1 := quotaWorkload("insufficient quota for cpu in flavor spot: needs 4, have 2")
	b.propagateStatus(context.Background(), job, wl1)
	if _, wrote := fs.comments[300]; !wrote {
		t.Fatal("first propagateStatus call should write a comment")
	}
	job.Comment = fs.comments[300] // simulate Slurm now reflecting the write

	delete(fs.comments, 300) // clear so we can detect a second write
	wl2 := quotaWorkload("insufficient quota for cpu in flavor spot: needs 9, have 1")
	b.propagateStatus(context.Background(), job, wl2)
	if _, wrote := fs.comments[300]; wrote {
		t.Error("second call with only volatile numbers changed (same class) must be throttled within the interval")
	}
}

// TestPropagateStatusRewritesImmediatelyOnClassTransition is the P3
// regression (b): a status CLASS transition (waiting-for-quota -> admitted)
// must rewrite the comment immediately, even though it happens well within
// commentRewriteMinInterval of the previous write.
func TestPropagateStatusRewritesImmediatelyOnClassTransition(t *testing.T) {
	fs := &fakeSlurm{}
	b, _ := testBridge(t, fs)
	job := &slurm.Job{JobID: 301}

	wl1 := quotaWorkload("insufficient quota for cpu in flavor spot: needs 4, have 2")
	b.propagateStatus(context.Background(), job, wl1)
	job.Comment = fs.comments[301]
	delete(fs.comments, 301)

	// Class transition: waiting-for-quota -> admitted, immediately after.
	wl2 := admittedWorkload("slurm-job-301")
	b.propagateStatus(context.Background(), job, wl2)
	want := "wm: capacity admitted, provisioning nodes"
	if got := fs.comments[301]; got != want {
		t.Errorf("comment = %q, want %q (class transition must rewrite immediately)", got, want)
	}
}

// TestPruneCommentStateRemovesEntriesForJobsGoneFromSnapshot is the P3
// mandatory-pruning regression: the audit flagged unbounded cross-tick maps,
// so commentState must lose its entry for a job ID absent from a tick's
// ListJobs snapshot (the job left the system: completed, cancelled, failed,
// purged...).
func TestPruneCommentStateRemovesEntriesForJobsGoneFromSnapshot(t *testing.T) {
	fs := &fakeSlurm{}
	b, _ := testBridge(t, fs)
	job := &slurm.Job{JobID: 302}

	b.propagateStatus(context.Background(), job, quotaWorkload("insufficient quota for cpu: needs 4, have 2"))
	if _, tracked := b.commentState[302]; !tracked {
		t.Fatal("expected job 302 to be tracked after propagateStatus wrote a comment")
	}

	// Simulate a tick's ListJobs snapshot in which job 302 is gone (byID does
	// not contain it) and some other job is still present.
	byID := map[uint64]*slurm.Job{999: {JobID: 999}}
	b.pruneCommentState(byID)

	if _, tracked := b.commentState[302]; tracked {
		t.Error("commentState entry for a job absent from the tick snapshot should be pruned")
	}
	if len(b.commentState) != 0 {
		t.Errorf("commentState = %v, want empty after pruning the only tracked (now-gone) job", b.commentState)
	}
}
