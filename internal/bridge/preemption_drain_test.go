package bridge

import (
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/mrozacki/k8s-bridge/internal/config"
	"github.com/mrozacki/k8s-bridge/internal/metrics"
	"github.com/mrozacki/k8s-bridge/internal/slurm"
	"github.com/mrozacki/k8s-bridge/internal/translate"
)

// drainCfg is a config with ADR-0016 preemption drain ENABLED (or not). Every
// test that wants the default (disabled) says so explicitly, so the default
// cannot silently flip without a test noticing.
func drainCfg(enabled bool) *config.Config {
	cfg := &config.Config{
		Namespace:  "slurm-jobs",
		LocalQueue: "main",
		PartitionMappings: []config.PartitionMapping{
			{PartitionName: "mixing", WorkloadPriorityClass: "normal-priority"},
		},
		DrainOnPreemption: enabled,
	}
	cfg.ApplyDefaults()
	return cfg
}

// drainJobSet is a minimal owned JobSet carrying the job-id label and a
// parallelism, enough for cleanupFinishedJobs to resolve node names.
func drainJobSet(id uint64, tasks int32) *jobsetv1alpha2.JobSet {
	p := tasks
	js := &jobsetv1alpha2.JobSet{}
	js.Name = translate.JobSetName(id)
	js.Namespace = "slurm-jobs"
	js.Labels = map[string]string{
		translate.SlurmJobIDLabel: fmt.Sprintf("%d", id),
		translate.ManagedByLabel:  translate.ManagedByValue,
	}
	js.Spec.ReplicatedJobs = []jobsetv1alpha2.ReplicatedJob{{
		Name:     "workers",
		Replicas: 1,
		Template: batchv1.JobTemplateSpec{Spec: batchv1.JobSpec{Parallelism: &p}},
	}}
	return js
}

// evictedWorkload is an admittedWorkload flipped to the preempted shape: Kueue
// sets Evicted=True and Admitted=False when it reclaims capacity.
func evictedWorkload(jobsetName string) *unstructured.Unstructured {
	wl := admittedWorkload(jobsetName)
	wl.Object["status"] = map[string]any{"conditions": []any{
		map[string]any{"type": "Admitted", "status": "False"},
		map[string]any{"type": "Evicted", "status": "True", "reason": "Preempted"},
	}}
	return wl
}

func equalNodeLists(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestCleanupDrainsPreemptedJob is the ADR-0016 happy path: with the flag on, a
// released job whose Workload is Evicted (and not admitted) has EVERY dynamic
// node record deleted so Slurm requeues immediately, and the drain metric
// increments exactly once.
func TestCleanupDrainsPreemptedJob(t *testing.T) {
	const id, tasks = uint64(55), int32(2)
	fs := &fakeSlurm{}
	b := orphanBridge(t, drainCfg(true), fs)
	js := drainJobSet(id, tasks)
	snap := &tickSnapshot{
		ownedJobSets:      map[string]*jobsetv1alpha2.JobSet{js.Name: js},
		workloadsByJobSet: map[string]*unstructured.Unstructured{js.Name: evictedWorkload(js.Name)},
	}
	byID := map[uint64]*slurm.Job{id: releasedJob(id, "mixing", uint64(tasks))}

	before := testutil.ToFloat64(metrics.PreemptionDrainsTotal)
	if err := b.cleanupFinishedJobs(context.Background(), b.cfgSnapshot(), snap, byID); err != nil {
		t.Fatalf("cleanupFinishedJobs: %v", err)
	}

	want := translate.NodeNames(id, tasks)
	if !equalNodeLists(fs.deletedNodes, want) {
		t.Errorf("drained nodes = %v, want every node record %v", fs.deletedNodes, want)
	}
	// The JobSet must NOT be deleted — Kueue keeps it suspended and re-admits.
	if len(fs.cancelled) != 0 {
		t.Errorf("preemption drain must not cancel the Slurm job, cancelled=%v", fs.cancelled)
	}
	if got := testutil.ToFloat64(metrics.PreemptionDrainsTotal) - before; got != 1 {
		t.Errorf("PreemptionDrainsTotal delta = %v, want 1", got)
	}
}

// TestCleanupDoesNotDrainWhenGuarded pins every guard: the flag off, a still
// admitted (not evicted) workload, and a held job must each leave node records
// untouched.
func TestCleanupDoesNotDrainWhenGuarded(t *testing.T) {
	const id, tasks = uint64(56), int32(2)
	cases := []struct {
		name string
		cfg  *config.Config
		wl   *unstructured.Unstructured
		job  *slurm.Job
	}{
		{"flag off", drainCfg(false), evictedWorkload(translate.JobSetName(id)), releasedJob(id, "mixing", uint64(tasks))},
		{"admitted not evicted", drainCfg(true), admittedWorkload(translate.JobSetName(id)), releasedJob(id, "mixing", uint64(tasks))},
		{"held job", drainCfg(true), evictedWorkload(translate.JobSetName(id)), heldJobRunning(id)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := &fakeSlurm{}
			b := orphanBridge(t, tc.cfg, fs)
			js := drainJobSet(id, tasks)
			snap := &tickSnapshot{
				ownedJobSets:      map[string]*jobsetv1alpha2.JobSet{js.Name: js},
				workloadsByJobSet: map[string]*unstructured.Unstructured{js.Name: tc.wl},
			}
			byID := map[uint64]*slurm.Job{id: tc.job}
			if err := b.cleanupFinishedJobs(context.Background(), b.cfgSnapshot(), snap, byID); err != nil {
				t.Fatalf("cleanupFinishedJobs: %v", err)
			}
			if len(fs.deletedNodes) != 0 {
				t.Errorf("%s: expected NO drain, but deleted %v", tc.name, fs.deletedNodes)
			}
		})
	}
}

// TestDrainPreemptedNodesQuietWhenAlreadyGone: on a repeat tick every DeleteNode
// returns 404 (records already drained), so nothing is counted and the metric
// does not move — the stateless "fire only when we actually removed something"
// contract that keeps repeat ticks silent.
func TestDrainPreemptedNodesQuietWhenAlreadyGone(t *testing.T) {
	fs := &fakeSlurm{deleteNodeErr: &slurm.APIError{StatusCode: 404}}
	b := orphanBridge(t, drainCfg(true), fs)
	before := testutil.ToFloat64(metrics.PreemptionDrainsTotal)
	b.drainPreemptedNodes(context.Background(), releasedJob(9, "mixing", 3), drainJobSet(9, 3))
	if got := testutil.ToFloat64(metrics.PreemptionDrainsTotal) - before; got != 0 {
		t.Errorf("PreemptionDrainsTotal moved by %v on an all-404 drain; repeat ticks must be silent", got)
	}
}

// heldJobRunning is a job the bridge sees as held (so the drain must skip it)
// while otherwise resembling a live job.
func heldJobRunning(id uint64) *slurm.Job {
	return &slurm.Job{
		JobID:       id,
		Partition:   "mixing",
		JobState:    []string{"PENDING"},
		StateReason: "JobHeldUser",
		Priority:    slurm.Uint64NoVal{Set: true, Number: 0},
		Hold:        true,
		Comment:     "wm: held for admission by k8s-bridge",
	}
}
