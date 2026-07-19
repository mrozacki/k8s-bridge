// Package kueue holds the Kueue-facing mechanics shared by both bridges
// (ADR-0012): reading a Workload's admission state, finding the Workload a
// bridge-created JobSet owns, and the watch source that lets a reconcile wake
// on Workload events. It is deliberately system-agnostic — it knows nothing
// about Slurm or Ray, only about Kueue Workloads and the JobSets that own them.
//
// Workloads are handled as unstructured objects (thin-surface principle): the
// bridges read a few status/spec fields, not the full Kueue API, so this
// package does not import the Kueue Go module. This mirrors the pre-existing
// approach in internal/bridge/kueue.go, which will migrate onto this package
// in a follow-up (ADR-0012 records the transitional duplication).
package kueue

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// L6 (live deploy, shared): Kueue graduated Workload to v1beta2; the
// spec.priority/queueName/podSets and status.conditions fields these helpers
// read are byte-for-byte identical to v1beta1, so this is a version-string
// bump only.
var (
	// WorkloadGVK is the singular kind (watch sources watch single objects).
	WorkloadGVK = schema.GroupVersionKind{Group: "kueue.x-k8s.io", Version: "v1beta2", Kind: "Workload"}
	// WorkloadListGVK is the list kind (cache/API LISTs).
	WorkloadListGVK = schema.GroupVersionKind{Group: "kueue.x-k8s.io", Version: "v1beta2", Kind: "WorkloadList"}
)

// IsAdmitted reports whether the Workload passed Kueue admission — the gate a
// bridge waits on before releasing/unsuspending the external workload onto the
// capacity Kueue just granted. A nil Workload (not yet created by Kueue) is not
// admitted.
func IsAdmitted(wl *unstructured.Unstructured) bool {
	if wl == nil {
		return false
	}
	conds, _, _ := unstructured.NestedSlice(wl.Object, "status", "conditions")
	for _, c := range conds {
		if cond, ok := c.(map[string]any); ok && cond["type"] == "Admitted" {
			return cond["status"] == "True"
		}
	}
	return false
}

// IsEvicted reports whether Kueue has evicted the Workload — the signal that
// its previously-granted capacity was reclaimed (preemption, priority, or
// within-cohort reclamation). Kueue sets an "Evicted" condition (status True,
// reason e.g. Preempted) and suspends the owning JobSet, deleting its pods. A
// nil Workload is not evicted.
//
// NEEDS-LIVE-VALIDATION: confirm the "Evicted" condition type/status against
// the deployed Kueue (v0.18.x) — an unexpected shape degrades to "not evicted",
// which preserves the historic no-drain behaviour rather than draining a live
// job. Callers should additionally require !IsAdmitted so a re-admitted job
// carrying a stale Evicted condition is not treated as still-evicted.
func IsEvicted(wl *unstructured.Unstructured) bool {
	if wl == nil {
		return false
	}
	conds, _, _ := unstructured.NestedSlice(wl.Object, "status", "conditions")
	for _, c := range conds {
		if cond, ok := c.(map[string]any); ok && cond["type"] == "Evicted" {
			return cond["status"] == "True"
		}
	}
	return false
}

// QuotaReason returns the human-readable reason a Workload is not yet
// admitted (Kueue's QuotaReserved message), or "" when admitted or unknown.
// Used to explain "why am I waiting" on the external workload's status.
func QuotaReason(wl *unstructured.Unstructured) string {
	if wl == nil {
		return ""
	}
	conds, _, _ := unstructured.NestedSlice(wl.Object, "status", "conditions")
	for _, c := range conds {
		cond, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if cond["type"] == "QuotaReserved" && cond["status"] != "True" {
			if msg, ok := cond["message"].(string); ok {
				return msg
			}
		}
	}
	return ""
}

// WorkloadForOwner finds the Kueue Workload owned by a given JobSet
// (ownerKind="JobSet", ownerName=<jobset name>), listing Workloads in the
// namespace through whatever client the caller passes (the manager's cache in
// production). Returns (nil, nil) when no matching Workload exists yet — Kueue
// generates it asynchronously after the JobSet is created, so "not there yet"
// is a normal, non-error state the caller retries on.
func WorkloadForOwner(ctx context.Context, c client.Client, namespace, ownerName string) (*unstructured.Unstructured, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(WorkloadListGVK)
	if err := c.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("listing workloads: %w", err)
	}
	for i := range list.Items {
		for _, ref := range list.Items[i].GetOwnerReferences() {
			if ref.Kind == "JobSet" && ref.Name == ownerName {
				return &list.Items[i], nil
			}
		}
	}
	return nil, nil
}

// WorkloadNudgeSource builds a watch source over Kueue Workload objects so a
// reconciler wakes on any Workload event. The handler decides what to enqueue;
// this keeps the Workload GVK confined to this package (thin-surface guard).
func WorkloadNudgeSource(c cache.Cache, h handler.TypedEventHandler[*unstructured.Unstructured, reconcile.Request]) source.Source {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(WorkloadGVK)
	return source.Kind(c, obj, h)
}
