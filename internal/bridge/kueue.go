// Kueue-facing helpers: the bridge reads Workload conditions to explain
// admission state to Slurm users, and syncs Workload priority both ways.
// Workloads are accessed as unstructured objects to avoid importing the
// full Kueue API module for two fields.
package bridge

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/mrozacki/k8s-bridge/internal/config"
	"github.com/mrozacki/k8s-bridge/internal/kueue"
	"github.com/mrozacki/k8s-bridge/internal/slurm"
	"github.com/mrozacki/k8s-bridge/internal/translate"
)

// WorkloadNudgeSource builds a watch source over Kueue Workload objects for
// the P1 watch-nudge (backlog P1 / ADR-0011): any Workload event runs
// handler, which main.go wires to call Bridge.Nudge(). It now delegates to the
// shared internal/kueue package (ADR-0012 migration): the Kueue GVKs and watch
// wiring live there, shared with ray-bridge, so this stays a thin wrapper the
// Slurm-side call sites (main.go, watch.go) can keep calling unchanged.
//
// Every Workload event nudges, unfiltered by owner: unlike the JobSet watch
// (which can cheaply label-select to bridge-owned objects server-side via
// the cache, see managedJobSetLabel), Kueue does not stamp a class label on
// Workload objects the bridge could filter on either server- or client-side
// without decoding every event (the same P8c limitation documented on
// snapshot()'s unfiltered LIST). A watch event from an unrelated Workload just
// costs one coalesced extra tick, which is cheap and safe by design (Nudge
// never queues more than one).
func WorkloadNudgeSource(c cache.Cache, h handler.TypedEventHandler[*unstructured.Unstructured, reconcile.Request]) source.Source {
	return kueue.WorkloadNudgeSource(c, h)
}

// tickSnapshot is the once-per-tick view of everything the bridge owns:
// JobSets by name and their Workloads by owning JobSet name.
type tickSnapshot struct {
	ownedJobSets      map[string]*jobsetv1alpha2.JobSet
	workloadsByJobSet map[string]*unstructured.Unstructured
}

// snapshot builds the once-per-tick view of owned JobSets and their
// Workloads. Both LISTs are served from the manager's informer cache
// (ADR-0011 / backlog P8), not a live apiserver round trip: cmd/k8s-bridge
// wires the Bridge's client.Client to the manager's cached client, so this
// call site did not need to change to get that win — the cache-vs-live
// choice lives entirely in main.go's client construction.
func (b *Bridge) snapshot(ctx context.Context, cfg *config.Config) (*tickSnapshot, error) {
	snap := &tickSnapshot{
		ownedJobSets:      map[string]*jobsetv1alpha2.JobSet{},
		workloadsByJobSet: map[string]*unstructured.Unstructured{},
	}
	// ADR-0015 Phase A: in supervisor mode "owned" means "created for THIS
	// WorkloadMixing CR", not just "created by any bridge". Several per-CR
	// Bridges share one JobSet namespace there, and everything downstream of
	// this snapshot (cleanup, the orphan guards, phase-1 create skipping)
	// assumes every listed JobSet answers to THIS Bridge's Slurm cluster —
	// without the extra label term, bridge A would look bridge B's JobSets up
	// in bridge A's Slurm, find nothing, and delete B's live JobSets as
	// "finished". Single-CR/file mode leaves WorkloadMixingName empty and
	// keeps the historic managed-by-only selector. Migration note: JobSets
	// created BEFORE switching a deployment to supervisor mode lack the
	// per-CR label and become invisible here — see docs/installation.md §4.2.
	selector := client.MatchingLabels{translate.ManagedByLabel: translate.ManagedByValue}
	if cfg.WorkloadMixingName != "" {
		selector[translate.WorkloadMixingLabel] = cfg.WorkloadMixingName
	}
	var owned jobsetv1alpha2.JobSetList
	if err := b.kube.List(ctx, &owned,
		client.InNamespace(cfg.Namespace),
		selector,
	); err != nil {
		return nil, fmt.Errorf("listing owned JobSets: %w", err)
	}
	for i := range owned.Items {
		snap.ownedJobSets[owned.Items[i].Name] = &owned.Items[i]
	}
	// audit P8c: this LIST is intentionally unfiltered. Kueue does not stamp
	// a class label on Workload objects that would let us select "only
	// bridge-owned" ones server-side — kueue.x-k8s.io/job-uid is per-object
	// (one Workload's own owning-job UID, not a queryable class), and Kueue
	// does not propagate our translate.ManagedByLabel from the JobSet onto
	// its generated Workload. Matching still happens client-side below via
	// ownerReferences. Hacking a filter in (e.g. guessing at a naming
	// convention) would be fragile. P1/P8 follow-up: this LIST now goes
	// through the manager's cache like the JobSet one above, so the
	// per-tick apiserver cost this comment used to describe is already
	// gone; only the label-selector gap (no class-filterable label exists
	// on Workload) remains open, tracked in the thin-surface note below.
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(kueue.WorkloadListGVK)
	if err := b.kube.List(ctx, list, client.InNamespace(cfg.Namespace)); err != nil {
		return nil, fmt.Errorf("listing workloads: %w", err)
	}
	for i := range list.Items {
		for _, ref := range list.Items[i].GetOwnerReferences() {
			if ref.Kind == "JobSet" {
				snap.workloadsByJobSet[ref.Name] = &list.Items[i]
			}
		}
	}
	return snap, nil
}

// isAdmitted reports whether the Workload passed Kueue admission — the
// gate for even ATTEMPTING node pinning (scale finding: trying
// to pin thousands of not-yet-admitted jobs meant 2 REST mutations per job
// per tick and ~3-minute ticks at 3000 backlog jobs). Delegates to the shared
// internal/kueue package (ADR-0012 migration); kept as a local wrapper so the
// call site in reconciler.go is unchanged.
func isAdmitted(wl *unstructured.Unstructured) bool {
	return kueue.IsAdmitted(wl)
}

// isEvicted reports whether Kueue has evicted the Workload (ADR-0016 preemption
// drain signal). Local wrapper over the shared internal/kueue helper so the
// reconciler call site reads like isAdmitted's.
func isEvicted(wl *unstructured.Unstructured) bool {
	return kueue.IsEvicted(wl)
}

// capUserPriority bounds an untrusted, user-originated priority. Negative
// values floor at 0; when MaxUserPriority is configured, higher requests are
// clamped down to it (H1: a job owner must not be able to jump the whole mixed
// queue ahead of other tenants by requesting an arbitrary priority). Returns the
// effective value and whether it was clamped.
func (b *Bridge) capUserPriority(v int64) (int64, bool) {
	orig := v
	if v < 0 {
		v = 0
	}
	if m := b.cfgSnapshot().MaxUserPriority; m > 0 && v > int64(m) {
		v = int64(m)
	}
	return v, v != orig
}

// applyPriorityDirective implements ADR-0009's safe channel: the lua
// JobSubmit plugin turns `scontrol update priority=N` into an
// admin_comment directive "wm:prio=N"; the bridge applies it to the
// Workload and acknowledges by rewriting the comment.
func (b *Bridge) applyPriorityDirective(ctx context.Context, job *slurm.Job, wl *unstructured.Unstructured) {
	const prefix = "wm:prio="
	if wl == nil || !strings.HasPrefix(job.AdminComment, prefix) {
		return
	}
	prio, err := strconv.ParseInt(strings.TrimPrefix(job.AdminComment, prefix), 10, 32)
	if err != nil {
		b.log.Info("invalid priority directive", "slurmJobID", job.JobID, "adminComment", job.AdminComment)
		return
	}
	prio, clamped := b.capUserPriority(prio)
	if clamped {
		b.log.Info("priority directive clamped to maxUserPriority", "slurmJobID", job.JobID, "requested", job.AdminComment, "applied", prio, "maxUserPriority", b.cfgSnapshot().MaxUserPriority)
	}
	patch := []byte(fmt.Sprintf(`{"spec":{"priority":%d}}`, prio))
	if err := b.kube.Patch(ctx, wl, client.RawPatch(types.MergePatchType, patch)); err != nil {
		// audit AUD2: a genuine failure (Workload patch rejected/unreachable)
		// was logged at Info, invisible to a level>=WARN filter.
		b.log.Warn("priority directive patch failed, will retry", "slurmJobID", job.JobID, "reason", err)
		return
	}
	if err := b.slurm.SetJobAdminComment(ctx, job.JobID, fmt.Sprintf("wm:prio-applied=%d", prio)); err != nil {
		// audit minor: this failure was previously silently dropped; the
		// Workload patch above already succeeded so the priority IS applied,
		// but Slurm's ack write failing means the directive will be reapplied
		// (harmlessly) next tick — worth a log line, not fatal.
		b.log.Info("acknowledging priority directive failed, will retry", "slurmJobID", job.JobID, "reason", err)
		return
	}
	b.log.Info("priority directive applied", "slurmJobID", job.JobID, "priority", prio)
}

// statusClass is the coarse admission stage used by the P3 comment-rewrite
// throttle (a large unadmitted backlog rewrote a comment every
// tick per job because the quota-reason message embeds volatile numbers).
// A CLASS transition (e.g. waiting-for-quota -> admitted) always rewrites
// immediately, regardless of the minimum-interval gate; changes WITHIN a
// class (only the numbers in a quota message moving) are throttled.
type statusClass int

const (
	classCreating statusClass = iota
	classWaitingQuota
	classQueued
	classAdmitted
)

// admissionStatus renders a short human explanation of the workload state
// for the Slurm comment field ("why am I waiting"), plus the coarse class it
// belongs to (P3).
func admissionStatus(wl *unstructured.Unstructured) (string, statusClass) {
	if wl == nil {
		return "wm: creating capacity request", classCreating
	}
	conds, _, _ := unstructured.NestedSlice(wl.Object, "status", "conditions")
	admitted, reason := false, ""
	for _, c := range conds {
		cond, ok := c.(map[string]any)
		if !ok {
			continue
		}
		switch cond["type"] {
		case "Admitted":
			admitted = cond["status"] == "True"
		case "QuotaReserved":
			if cond["status"] != "True" {
				if msg, ok := cond["message"].(string); ok {
					reason = msg
				}
			}
		}
	}
	if admitted {
		return "wm: capacity admitted, provisioning nodes", classAdmitted
	}
	if reason != "" {
		return "wm: waiting for quota: " + truncateRunes(reason, quotaReasonMaxLen), classWaitingQuota
	}
	return "wm: queued for admission", classQueued
}

// quotaReasonMaxLen bounds the Kueue QuotaReserved message embedded into the
// Slurm comment field (audit minor: named instead of a bare "96" literal).
const quotaReasonMaxLen = 96

// truncateRunes shortens s to at most n RUNES (not bytes). Kueue condition
// messages can contain multi-byte UTF-8 (quoted resource names, non-ASCII
// namespaces); byte-slicing with s[:n] can split a multi-byte rune and
// corrupt the tail of the string, so this walks runes instead (audit minor).
func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

// volatileNumberPattern matches runs of ASCII digits (P3 fix):
// Kueue's QuotaReserved message embeds volatile numbers ("needs 4, have 2")
// that change almost every tick as usage shifts, while the flavor/resource
// names around them ("insufficient quota for cpu in flavor spot") stay
// stable. Stripping only digits keeps those names intact for comparison.
var volatileNumberPattern = regexp.MustCompile(`[0-9]+`)

// normalizeStatusForComparison strips volatile numeric fields from a status
// string so the P3 rewrite throttle compares on the STABLE part of the
// message only. The actual comment written to Slurm still carries the real
// numbers (via admissionStatus) — only the comparison uses this normalized
// form.
func normalizeStatusForComparison(status string) string {
	return volatileNumberPattern.ReplaceAllString(status, "#")
}

// commentRewriteMinInterval is the minimum time between two comment
// rewrites for the SAME job when its status class has not changed (P3):
// testing saw a 2500-comment burst (one per unadmitted backlog job, every
// tick, because the quota-reason message's numbers moved) balloon a tick to
// 4m02s. A class TRANSITION (e.g. waiting-for-quota -> admitted) always
// rewrites immediately regardless of this interval — only same-class,
// numbers-only churn is throttled.
const commentRewriteMinInterval = 60 * time.Second

// propagateStatus writes the admission explanation into the Slurm job's
// comment when it changed. Errors are logged, never fatal — this is UX
// sugar, not control flow.
//
// P3: comparison against the job's CURRENT Slurm comment uses the
// normalized (digit-stripped) form, and a same-class rewrite is additionally
// throttled to at most once per commentRewriteMinInterval — UNLESS the
// status class transitions (e.g. waiting -> admitted), which always rewrites
// immediately. Cross-tick memory lives in b.commentState (pruned every tick,
// see pruneCommentState).
func (b *Bridge) propagateStatus(ctx context.Context, job *slurm.Job, wl *unstructured.Unstructured) {
	status, class := admissionStatus(wl)
	if job.Comment == status {
		return
	}
	now := time.Now()
	if prev, tracked := b.commentState[job.JobID]; tracked {
		classChanged := prev.class != class
		withinInterval := now.Sub(prev.lastWrite) < commentRewriteMinInterval
		sameNormalized := normalizeStatusForComparison(job.Comment) == normalizeStatusForComparison(status)
		if !classChanged && withinInterval && sameNormalized {
			// Same class, same stable content (only volatile numbers moved),
			// and still within the throttle window: skip the rewrite.
			return
		}
	}
	if err := b.slurm.SetJobComment(ctx, job.JobID, status); err != nil {
		// audit AUD2: raised to Warn — this is the bridge's only user-facing
		// UX channel (squeue -o %k); a silent failure here means the Slurm
		// user sees a stale or misleading status with no other signal.
		b.log.Warn("comment propagation failed (non-fatal)", "slurmJobID", job.JobID, "reason", err)
		return
	}
	if b.commentState == nil {
		b.commentState = map[uint64]commentTrackEntry{}
	}
	b.commentState[job.JobID] = commentTrackEntry{class: class, lastWrite: now}
}

// syncPriority keeps Slurm's priority field and Workload.spec.priority in
// agreement using a last-synced annotation as memory:
//   - no annotation yet   -> mirror Workload->Slurm (Kueue is source of truth)
//   - Slurm side deviates -> the user ran `scontrol update priority=` ->
//     patch the Workload (works for RUNNING workloads too: it re-ranks
//     preemption-victim selection)
//   - Workload deviates   -> mirror back to Slurm for truthful squeue
func (b *Bridge) syncPriority(ctx context.Context, js *jobsetv1alpha2.JobSet, job *slurm.Job, wl *unstructured.Unstructured) {
	if wl == nil {
		return
	}
	wlPrio, found, _ := unstructured.NestedInt64(wl.Object, "spec", "priority")
	if !found {
		return
	}
	slurmPrio := int64(0)
	if job.Priority.Set {
		slurmPrio = int64(job.Priority.Number)
	}

	synced, hasSynced := js.Annotations[syncedPriorityAnnotation]
	switch {
	case !hasSynced:
		// First contact: Kueue -> Slurm mirror.
		if err := b.slurm.SetJobPriority(ctx, job.JobID, safeUint32(wlPrio)); err == nil {
			b.rememberSyncedPriority(ctx, js, wlPrio)
		}
	case strconv.FormatInt(slurmPrio, 10) != synced && !job.IsHeld():
		// User moved the Slurm side: forward to Kueue, capped (H1: the Slurm
		// side is user-writable via scontrol).
		effective, clamped := b.capUserPriority(slurmPrio)
		if clamped {
			b.log.Info("slurm-side priority clamped to maxUserPriority", "slurmJobID", job.JobID, "requested", slurmPrio, "applied", effective)
		}
		patch := []byte(fmt.Sprintf(`{"spec":{"priority":%d}}`, effective))
		if err := b.kube.Patch(ctx, wl, client.RawPatch(types.MergePatchType, patch)); err != nil {
			// audit AUD2: genuine failure, was Info.
			b.log.Warn("workload priority patch failed", "slurmJobID", job.JobID, "reason", err)
			return
		}
		b.rememberSyncedPriority(ctx, js, effective)
		b.log.Info("priority change propagated to Kueue", "slurmJobID", job.JobID, "priority", effective)
	case strconv.FormatInt(wlPrio, 10) != synced:
		// Kueue side moved (e.g. kubectl patch): mirror back to Slurm.
		if err := b.slurm.SetJobPriority(ctx, job.JobID, safeUint32(wlPrio)); err == nil {
			b.rememberSyncedPriority(ctx, js, wlPrio)
		}
	}
}

// safeUint32 converts a Kueue-side priority to the uint32 Slurm field without
// wrapping on negative or oversized values (gosec G115).
func safeUint32(v int64) uint32 {
	if v < 0 {
		return 0
	}
	if v > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(v)
}

func (b *Bridge) rememberSyncedPriority(ctx context.Context, js *jobsetv1alpha2.JobSet, v int64) {
	if js.Annotations == nil {
		js.Annotations = map[string]string{}
	}
	js.Annotations[syncedPriorityAnnotation] = strconv.FormatInt(v, 10)
	if err := b.kube.Update(ctx, js); err != nil {
		// audit AUD2: genuine failure (JobSet update rejected/conflict), was
		// Info; leaving the synced-priority memory stale can cause repeated
		// re-sync attempts next tick.
		b.log.Warn("annotation update failed", "jobset", js.Name, "reason", err)
	}
}
