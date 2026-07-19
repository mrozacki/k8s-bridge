package raytranslate

import (
	"strings"
	"testing"

	"github.com/mrozacki/k8s-bridge/internal/ray"
	"github.com/mrozacki/k8s-bridge/internal/rayconfig"
)

func testConfig() *rayconfig.Config {
	c := &rayconfig.Config{
		Namespace:       "ray",
		LocalQueue:      "main",
		ManagedClusters: []rayconfig.ManagedCluster{{Name: "shared", HeadAddress: "shared-head-svc.ray:6379"}},
		PoolMappings: []rayconfig.PoolMapping{
			{Pool: "batch", WorkloadPriorityClass: "normal-priority"},
			{Pool: "hi", WorkloadPriorityClass: "high-priority", LocalQueue: "team-a"},
		},
		Worker: rayconfig.Worker{Image: "rayproject/ray:2.9.0"},
	}
	c.ApplyDefaults()
	return c
}

func innerJob() *ray.RayJob {
	return &ray.RayJob{
		Name: "job-a", Namespace: "ray", UID: "uid-123",
		TargetCluster: "shared", Pool: "batch",
		Workers: 3, CPUsPerWorker: 4, GPUsPerWorker: 0, MemPerWorkerMB: 2048,
	}
}

func TestToWorkerJobSetCoreFields(t *testing.T) {
	js, err := ToWorkerJobSet(innerJob(), testConfig())
	if err != nil {
		t.Fatalf("ToWorkerJobSet: %v", err)
	}
	if js.Name != "ray-workers-job-a" {
		t.Errorf("name = %q", js.Name)
	}
	if js.Namespace != "ray" {
		t.Errorf("namespace = %q", js.Namespace)
	}
	if got := js.Labels[ManagedByLabel]; got != ManagedByValue {
		t.Errorf("managed-by = %q", got)
	}
	if got := js.Labels[RayJobUIDLabel]; got != "uid-123" {
		t.Errorf("ray-job-uid = %q", got)
	}
	if got := js.Labels["kueue.x-k8s.io/queue-name"]; got != "main" {
		t.Errorf("queue-name = %q, want main", got)
	}
	if got := js.Labels["kueue.x-k8s.io/priority-class"]; got != "normal-priority" {
		t.Errorf("priority-class = %q", got)
	}

	// Each worker is its own child Job: Replicas = workers, one pod per Job.
	if got := js.Spec.ReplicatedJobs[0].Replicas; got != 3 {
		t.Errorf("replicas = %d, want 3 (one Job per worker)", got)
	}
	jobSpec := js.Spec.ReplicatedJobs[0].Template.Spec
	if *jobSpec.Parallelism != 1 || *jobSpec.Completions != 1 {
		t.Errorf("parallelism/completions = %d/%d, want 1/1 (one pod per worker Job)",
			*jobSpec.Parallelism, *jobSpec.Completions)
	}
	c := jobSpec.Template.Spec.Containers[0]
	if got := c.Resources.Requests.Cpu().Value(); got != 4 {
		t.Errorf("cpu request = %d, want 4", got)
	}
	if got := c.Resources.Requests.Memory().Value(); got != 2048*(1<<20) {
		t.Errorf("mem request = %d", got)
	}
	// GPU not requested → no GPU resource key
	if _, ok := c.Resources.Requests["nvidia.com/gpu"]; ok {
		t.Errorf("unexpected gpu request for a cpu-only job")
	}
}

func TestWorkerStartCommandPinsAndDials(t *testing.T) {
	js, err := ToWorkerJobSet(innerJob(), testConfig())
	if err != nil {
		t.Fatalf("ToWorkerJobSet: %v", err)
	}
	cmd := js.Spec.ReplicatedJobs[0].Template.Spec.Template.Spec.Containers[0].Command
	if len(cmd) != 3 || cmd[0] != "/bin/bash" {
		t.Fatalf("command = %v", cmd)
	}
	got := cmd[2]
	for _, want := range []string{
		"ray start",
		"--address=shared-head-svc.ray:6379",
		"--num-cpus=4",
		`--resources='{"wm-job-job-a": 4}'`,
		"--block",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("start command missing %q; got: %s", want, got)
		}
	}
	if strings.Contains(got, "--num-gpus") {
		t.Errorf("cpu-only job should not pass --num-gpus; got: %s", got)
	}
}

func TestToWorkerJobSetGPU(t *testing.T) {
	j := innerJob()
	j.GPUsPerWorker = 2
	js, err := ToWorkerJobSet(j, testConfig())
	if err != nil {
		t.Fatalf("ToWorkerJobSet: %v", err)
	}
	c := js.Spec.ReplicatedJobs[0].Template.Spec.Template.Spec.Containers[0]
	if got := c.Resources.Requests["nvidia.com/gpu"]; got.Value() != 2 {
		t.Errorf("gpu request = %v, want 2", got.Value())
	}
	if !strings.Contains(c.Command[2], "--num-gpus=2") {
		t.Errorf("gpu job should pass --num-gpus=2; got: %s", c.Command[2])
	}
}

func TestLocalQueueOverridePerPool(t *testing.T) {
	j := innerJob()
	j.Pool = "hi"
	js, err := ToWorkerJobSet(j, testConfig())
	if err != nil {
		t.Fatalf("ToWorkerJobSet: %v", err)
	}
	if got := js.Labels["kueue.x-k8s.io/queue-name"]; got != "team-a" {
		t.Errorf("queue-name = %q, want team-a (pool override)", got)
	}
	if got := js.Labels["kueue.x-k8s.io/priority-class"]; got != "high-priority" {
		t.Errorf("priority-class = %q, want high-priority", got)
	}
}

func TestToWorkerJobSetRejectsOutOfScope(t *testing.T) {
	cases := map[string]func(*ray.RayJob){
		"no cluster selector": func(j *ray.RayJob) { j.TargetCluster = "" },
		"unmanaged cluster":   func(j *ray.RayJob) { j.TargetCluster = "other" },
		"unmapped pool":       func(j *ray.RayJob) { j.Pool = "ghost" },
	}
	for name, mutate := range cases {
		j := innerJob()
		mutate(j)
		if _, err := ToWorkerJobSet(j, testConfig()); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestTopologyAnnotations(t *testing.T) {
	const rack = "ray-bridge.x-k8s.io/rack"
	annOf := func(j *ray.RayJob, cfg *rayconfig.Config) map[string]string {
		js, err := ToWorkerJobSet(j, cfg)
		if err != nil {
			t.Fatalf("ToWorkerJobSet: %v", err)
		}
		return js.Spec.ReplicatedJobs[0].Template.Spec.Template.Annotations
	}

	// 1) RayJob required-topology annotation → hard podset-required-topology.
	j := innerJob()
	j.RequiredTopology = rack
	if got := annOf(j, testConfig())["kueue.x-k8s.io/podset-required-topology"]; got != rack {
		t.Errorf("required-topology = %q, want %q", got, rack)
	}

	// 2) No job request, config PreferredTopology set → best-effort preferred.
	cfg := testConfig()
	cfg.Worker.PreferredTopology = rack
	got := annOf(innerJob(), cfg)
	if got["kueue.x-k8s.io/podset-preferred-topology"] != rack {
		t.Errorf("preferred-topology = %q, want %q", got["kueue.x-k8s.io/podset-preferred-topology"], rack)
	}
	if _, ok := got["kueue.x-k8s.io/podset-required-topology"]; ok {
		t.Errorf("must not set required when only preferred is configured")
	}

	// 3) Job required wins over config preferred.
	j2 := innerJob()
	j2.RequiredTopology = rack
	if a := annOf(j2, cfg); a["kueue.x-k8s.io/podset-required-topology"] != rack ||
		a["kueue.x-k8s.io/podset-preferred-topology"] != "" {
		t.Errorf("required must win over preferred: %v", a)
	}

	// 4) Neither → no topology annotations.
	if a := annOf(innerJob(), testConfig()); len(a) != 0 {
		t.Errorf("no topology request should yield no annotations, got %v", a)
	}
}

func TestWorkerPodNamesDeterministic(t *testing.T) {
	got := WorkerPodNames(innerJob())
	want := []string{
		"ray-workers-job-a-workers-0-0",
		"ray-workers-job-a-workers-1-0",
		"ray-workers-job-a-workers-2-0",
	}
	if len(got) != len(want) {
		t.Fatalf("got %d names, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("name[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
