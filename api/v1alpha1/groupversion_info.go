// Package v1alpha1 contains the typed WorkloadMixing API (ADR-0014).
//
// This package is the SOURCE OF TRUTH for the WorkloadMixing CRD schema:
// `make manifests` runs controller-gen over it and regenerates all three
// shipped CRD copies (deploy/crd/, the Helm chart's crds/, and test/crd/ for
// envtest). Do not hand-edit those YAML files — change the Go types/markers
// here and regenerate. CI enforces this with a `make manifests && git diff
// --exit-code` drift gate, and chart-ci's crd-sync-gate keeps proving the
// three copies stay spec-identical.
//
// Comment convention in this package (load-bearing, please keep it):
// controller-gen turns a field's ATTACHED doc comment (no blank line between
// comment and field) into the CRD property's `description`. The published
// descriptions were inherited from the pre-migration hand-written CRD, which
// documented only SOME fields (PR 1 proved byte-level parity against a frozen
// snapshot; that gate retired with PR 2's deliberate conditions change). So:
//   - attached comment  = the published CRD description, verbatim;
//   - detached comment  (blank line before the marker block) = engineering
//     documentation that must NOT leak into the CRD.
//
// Richer field-level rationale lives on the mirror fields in
// internal/config/config.go, which remains the single internal config type.
//
// +kubebuilder:object:generate=true
// +groupName=k8s-bridge.x-k8s.io
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion identifies this API group and version. The group stays
	// k8s-bridge.x-k8s.io for now: renaming the group is a breaking migration
	// and its own decision (ADR-0014, deferred 2026-07-11).
	GroupVersion = schema.GroupVersion{Group: "k8s-bridge.x-k8s.io", Version: "v1alpha1"}

	// SchemeBuilder registers this group-version's types. Built with plain
	// apimachinery (runtime.NewSchemeBuilder) rather than controller-runtime's
	// pkg/scheme helper: that helper is deprecated for exactly this use —
	// api packages should be importable with minimal dependencies (standard
	// library + apimachinery), so downstream consumers of the types never
	// drag in controller-runtime.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme adds the types in this group-version to the given scheme.
	// cmd/k8s-bridge/main.go calls this at startup; internal/bridge/crdconfig.go
	// consumes the CR through these types (ADR-0014 PR 2).
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes registers the WorkloadMixing kinds plus the group-version's
// metav1 types (ListOptions etc.), matching what generated clientsets expect.
func addKnownTypes(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion, &WorkloadMixing{}, &WorkloadMixingList{})
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
}
