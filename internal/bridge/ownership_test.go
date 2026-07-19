// ADR-0015 Phase A ownership tests: several per-CR Bridges share one JobSet
// namespace in supervisor mode, so "owned" must mean "created for THIS
// WorkloadMixing CR". These tests pin the consumer side of the
// translate.WorkloadMixingLabel contract (the producer side is pinned in
// internal/translate); the money test is the cleanup one — before the label
// existed, bridge A would have looked bridge B's JobSets up in bridge A's
// Slurm cluster, found nothing, and deleted B's live JobSets as "finished".
package bridge

import (
	"context"
	"log/slog"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/mrozacki/k8s-bridge/internal/config"
	"github.com/mrozacki/k8s-bridge/internal/translate"
)

// managedJobSet builds a minimal bridge-managed JobSet fixture in the
// test namespace, optionally stamped with a WorkloadMixing instance label
// (empty = legacy/single-CR shape, exactly what pre-ADR-0015 bridges
// produced).
func managedJobSet(name, jobID, instance string) *jobsetv1alpha2.JobSet {
	labels := map[string]string{
		translate.ManagedByLabel:  translate.ManagedByValue,
		translate.SlurmJobIDLabel: jobID,
	}
	if instance != "" {
		labels[translate.WorkloadMixingLabel] = instance
	}
	return &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "slurm-jobs", Labels: labels},
	}
}

func instanceScopedConfig(instance string) *config.Config {
	cfg := &config.Config{
		SlurmRestURL: "http://fake",
		Namespace:    "slurm-jobs",
		LocalQueue:   "main",
		PartitionMappings: []config.PartitionMapping{
			{PartitionName: "mixing", WorkloadPriorityClass: "normal-priority"},
		},
		Slurmd: config.Slurmd{
			Image:          "slurmd:test",
			ConfServer:     "ctl:6817",
			AuthSecretName: "slurm-auth-slurm",
		},
		WorkloadMixingName: instance,
	}
	cfg.ApplyDefaults()
	return cfg
}

// TestSnapshotScopedToWorkloadMixingInstance pins the selector semantics:
// with WorkloadMixingName set, snapshot must see ONLY that instance's
// JobSets; with it empty (single-CR/file mode), it must see every
// bridge-managed JobSet exactly as before — including instance-labeled ones,
// so a single-CR escape-hatch deployment still cleans up after a former
// supervisor deployment rather than leaking its JobSets.
func TestSnapshotScopedToWorkloadMixingInstance(t *testing.T) {
	objs := []client.Object{
		managedJobSet("slurm-job-1", "1", "cluster-a"),
		managedJobSet("slurm-job-2", "2", "cluster-b"),
		managedJobSet("slurm-job-3", "3", ""), // legacy, pre-label
	}
	kube := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).Build()

	b := New(instanceScopedConfig("cluster-a"), kube, &fakeSlurm{}, slog.Default())
	snap, err := b.snapshot(context.Background(), b.cfgSnapshot())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(snap.ownedJobSets) != 1 {
		t.Fatalf("scoped snapshot sees %d JobSets, want exactly 1 (own instance only): %v", len(snap.ownedJobSets), snap.ownedJobSets)
	}
	if _, ok := snap.ownedJobSets["slurm-job-1"]; !ok {
		t.Error("scoped snapshot is missing this instance's own JobSet slurm-job-1")
	}

	b = New(instanceScopedConfig(""), kube, &fakeSlurm{}, slog.Default())
	snap, err = b.snapshot(context.Background(), b.cfgSnapshot())
	if err != nil {
		t.Fatalf("snapshot (unscoped): %v", err)
	}
	if len(snap.ownedJobSets) != 3 {
		t.Fatalf("unscoped snapshot sees %d JobSets, want all 3 (historic managed-by-only behavior)", len(snap.ownedJobSets))
	}
}

// TestTickLeavesOtherInstancesJobSetsAlone is the cross-deletion regression:
// bridge "cluster-a", whose Slurm cluster has NO jobs at all, ticks in a
// namespace where bridge "cluster-b" has a live JobSet. Without instance
// scoping, cluster-a's cleanup would treat slurm-job-99 as finished (job 99
// does not exist in cluster-a's Slurm) and delete cluster-b's JobSet.
func TestTickLeavesOtherInstancesJobSetsAlone(t *testing.T) {
	other := managedJobSet("slurm-job-99", "99", "cluster-b")
	fs := &fakeSlurm{} // cluster-a's Slurm: empty — job 99 is unknown here
	kube := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(other).Build()

	b := New(instanceScopedConfig("cluster-a"), kube, fs, slog.Default())
	if err := b.tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}

	var js jobsetv1alpha2.JobSet
	if err := kube.Get(context.Background(), client.ObjectKey{Namespace: "slurm-jobs", Name: "slurm-job-99"}, &js); err != nil {
		t.Fatalf("cluster-b's JobSet was deleted by cluster-a's tick (cross-instance cleanup): %v", err)
	}
	// It must not even have been LOOKED UP against cluster-a's Slurm — the
	// JobSet never enters the snapshot, so no GetJob confirmation fires.
	if fs.getJobCalls != 0 {
		t.Errorf("GetJob called %d times, want 0 (another instance's JobSet must not be identity-checked)", fs.getJobCalls)
	}
}
