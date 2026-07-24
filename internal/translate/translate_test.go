package translate

import (
	"fmt"
	"strings"
	"testing"

	"github.com/mrozacki/k8s-bridge/internal/config"
	"github.com/mrozacki/k8s-bridge/internal/slurm"
)

func testConfig() *config.Config {
	cfg := &config.Config{
		Namespace:  "slurm-jobs",
		LocalQueue: "main",
		PartitionMappings: []config.PartitionMapping{
			{PartitionName: "mixing", WorkloadPriorityClass: "normal-priority"},
			{PartitionName: "mixing-high", WorkloadPriorityClass: "high-priority"},
		},
		Slurmd: config.Slurmd{
			Image:          "ghcr.io/slinkyproject/slurmd:25.11",
			ConfServer:     "slurm-controller.slurm:6817",
			AuthSecretName: "slurm-auth-key",
		},
	}
	// audit D6 cleanup: every real Config reaches ToJobSet only after a
	// loader (config.Load or bridge.LoadConfigFromCR) has called
	// ApplyDefaults, which is now the single source of truth for
	// GPUResourceName's default — translate.go no longer carries its own
	// fallback. Mirror that here so this test config matches production.
	cfg.ApplyDefaults()
	return cfg
}

func heldJob(id uint64, partition string, tasks, cpus uint64) *slurm.Job {
	return &slurm.Job{
		JobID:       id,
		Name:        "test",
		Partition:   partition,
		JobState:    []string{"PENDING"},
		StateReason: "JobHeldUser",
		Priority:    slurm.Uint64NoVal{Set: true, Number: 0},
		Tasks:       slurm.Uint64NoVal{Set: true, Number: tasks},
		CPUsPerTask: slurm.Uint64NoVal{Set: true, Number: cpus},
	}
}

func TestToJobSetMapsCoreFields(t *testing.T) {
	js, err := ToJobSet(heldJob(42, "mixing", 3, 2), testConfig())
	if err != nil {
		t.Fatalf("ToJobSet: %v", err)
	}

	if js.Name != "slurm-job-42" {
		t.Errorf("name = %q, want slurm-job-42", js.Name)
	}
	if js.Namespace != "slurm-jobs" {
		t.Errorf("namespace = %q, want slurm-jobs", js.Namespace)
	}
	if got := js.Labels["kueue.x-k8s.io/queue-name"]; got != "main" {
		t.Errorf("queue-name label = %q, want main", got)
	}
	if got := js.Labels["kueue.x-k8s.io/priority-class"]; got != "normal-priority" {
		t.Errorf("priority-class label = %q, want normal-priority", got)
	}

	jobSpec := js.Spec.ReplicatedJobs[0].Template.Spec
	if *jobSpec.Parallelism != 3 || *jobSpec.Completions != 3 {
		t.Errorf("parallelism/completions = %d/%d, want 3/3 (one pod per Slurm task)",
			*jobSpec.Parallelism, *jobSpec.Completions)
	}

	container := jobSpec.Template.Spec.Containers[0]
	if got := container.Resources.Requests.Cpu().Value(); got != 2 {
		t.Errorf("cpu request = %d, want 2 (cpus-per-task)", got)
	}
	wantArg := "'Features=nodes-for-42 CPUs=2 RealMemory=2048'"
	found := false
	for _, a := range container.Args {
		if a == wantArg {
			found = true
		}
	}
	if !found {
		t.Errorf("args %v missing %q", container.Args, wantArg)
	}
	// TC-D3 regression: without -b a recreated slurmd pod re-registers
	// under a node name slurmctld still considers busy, leaving the old
	// job as a ghost.
	bootFlag := false
	for _, a := range container.Args {
		if a == "-b" {
			bootFlag = true
		}
	}
	if !bootFlag {
		t.Errorf("args %v missing \"-b\" (boot report; TC-D3 ghost-job guard)", container.Args)
	}
	if container.SecurityContext == nil || container.SecurityContext.Privileged == nil || !*container.SecurityContext.Privileged {
		t.Error("slurmd container must be privileged (cgroup access)")
	}
}

// TestToJobSetWorkloadMixingLabel pins the ADR-0015 Phase A ownership
// contract from the producer side: with cfg.WorkloadMixingName set
// (supervisor mode), the JobSet carries the per-CR instance label that
// bridge.snapshot selects on; with it EMPTY (single-CR and file modes), the
// label must be entirely ABSENT so those modes' JobSets stay byte-identical
// to what they always produced.
func TestToJobSetWorkloadMixingLabel(t *testing.T) {
	cfg := testConfig()
	cfg.WorkloadMixingName = "cluster-a"
	js, err := ToJobSet(heldJob(42, "mixing", 1, 1), cfg)
	if err != nil {
		t.Fatalf("ToJobSet: %v", err)
	}
	if got := js.Labels[WorkloadMixingLabel]; got != "cluster-a" {
		t.Errorf("%s label = %q, want cluster-a (supervisor mode must stamp per-CR ownership)", WorkloadMixingLabel, got)
	}

	js, err = ToJobSet(heldJob(42, "mixing", 1, 1), testConfig())
	if err != nil {
		t.Fatalf("ToJobSet: %v", err)
	}
	if _, present := js.Labels[WorkloadMixingLabel]; present {
		t.Errorf("%s label present with WorkloadMixingName unset; single-CR/file-mode JobSets must be unchanged", WorkloadMixingLabel)
	}
}

// TestToJobSetUsesPartitionLocalQueueOverride is the A1b regression: a
// partition with its own LocalQueue must route its JobSet to that queue
// instead of the global Config.LocalQueue.
func TestToJobSetUsesPartitionLocalQueueOverride(t *testing.T) {
	cfg := testConfig()
	cfg.PartitionMappings = append(cfg.PartitionMappings, config.PartitionMapping{
		PartitionName:         "team-a",
		WorkloadPriorityClass: "normal-priority",
		LocalQueue:            "team-a-queue",
	})
	js, err := ToJobSet(heldJob(43, "team-a", 1, 1), cfg)
	if err != nil {
		t.Fatalf("ToJobSet: %v", err)
	}
	if got := js.Labels["kueue.x-k8s.io/queue-name"]; got != "team-a-queue" {
		t.Errorf("queue-name label = %q, want team-a-queue (partition override)", got)
	}
}

// TestToJobSetFallsBackToGlobalLocalQueue is the A1b counterpart: a
// partition with no LocalQueue override must use the global queue exactly
// as before this feature landed.
func TestToJobSetFallsBackToGlobalLocalQueue(t *testing.T) {
	cfg := testConfig() // "mixing" mapping has no LocalQueue override
	js, err := ToJobSet(heldJob(44, "mixing", 1, 1), cfg)
	if err != nil {
		t.Fatalf("ToJobSet: %v", err)
	}
	if got := js.Labels["kueue.x-k8s.io/queue-name"]; got != cfg.LocalQueue {
		t.Errorf("queue-name label = %q, want global LocalQueue %q", got, cfg.LocalQueue)
	}
}

// TestNodeNamesMatchReplicatedJobName is the audit minor regression: the
// ReplicatedJob's Name in ToJobSet and the "-workers-0-" segment NodeNames
// generates must come from the same constant. If they drifted,
// cleanupFinishedJobs would compute node names for pods that JobSet never
// creates, and dynamic Slurm node records would never be garbage collected.
func TestNodeNamesMatchReplicatedJobName(t *testing.T) {
	job := heldJob(60, "mixing", 2, 1)
	js, err := ToJobSet(job, testConfig())
	if err != nil {
		t.Fatalf("ToJobSet: %v", err)
	}
	replicatedJobName := js.Spec.ReplicatedJobs[0].Name
	names := NodeNames(60, 2)
	for i, name := range names {
		want := fmt.Sprintf("%s-%s-0-%d", js.Name, replicatedJobName, i)
		if name != want {
			t.Errorf("NodeNames()[%d] = %q, want %q (derived from the actual ReplicatedJob name %q)",
				i, name, want, replicatedJobName)
		}
	}
}

func TestToJobSetTranslatesMemPerCPU(t *testing.T) {
	job := heldJob(50, "mixing", 2, 2)
	job.MemoryPerCPU = slurm.Uint64NoVal{Set: true, Number: 2048} // 2 GB per CPU
	js, err := ToJobSet(job, testConfig())
	if err != nil {
		t.Fatalf("ToJobSet: %v", err)
	}
	container := js.Spec.ReplicatedJobs[0].Template.Spec.Template.Spec.Containers[0]
	wantBytes := int64(2048) * 2 * (1 << 20) // mem-per-cpu * cpus, in bytes
	if got := container.Resources.Requests.Memory().Value(); got != wantBytes {
		t.Errorf("memory request = %d, want %d", got, wantBytes)
	}
	wantConf := "'Features=nodes-for-50 CPUs=2 RealMemory=4096'"
	if container.Args[2] != wantConf {
		t.Errorf("conf = %q, want %q", container.Args[2], wantConf)
	}
}

func TestToJobSetTranslatesGres(t *testing.T) {
	job := heldJob(51, "mixing", 1, 4)
	job.TresPerNode = "gres/gpu:2"
	js, err := ToJobSet(job, testConfig())
	if err != nil {
		t.Fatalf("ToJobSet: %v", err)
	}
	container := js.Spec.ReplicatedJobs[0].Template.Spec.Template.Spec.Containers[0]
	if got := container.Resources.Limits["nvidia.com/gpu"]; got.Value() != 2 {
		t.Errorf("gpu limit = %v, want 2", got)
	}
	if want := "Gres=gpu:2"; !strings.Contains(container.Args[2], want) {
		t.Errorf("conf %q missing %q", container.Args[2], want)
	}
}

// TestToJobSetDoesNotMountIntoSlurmdConfCache is the L1 regression (e2e
// iteration 2): a GPU job must NOT bind-mount anything under
// /var/spool/slurmd/conf-cache. Configless slurmd
// writes its controller-fetched config (incl. gres.conf) there, and a
// read-only bind mount makes that rename fail with EBUSY, so slurmd never
// registers. The count-only gres.conf is delivered by the Slurm chart's
// configFiles instead.
func TestToJobSetDoesNotMountIntoSlurmdConfCache(t *testing.T) {
	job := heldJob(52, "mixing", 1, 4)
	job.TresPerNode = "gres/gpu:2"
	js, err := ToJobSet(job, testConfig())
	if err != nil {
		t.Fatalf("ToJobSet: %v", err)
	}
	pod := js.Spec.ReplicatedJobs[0].Template.Spec.Template.Spec
	for _, m := range pod.Containers[0].VolumeMounts {
		if strings.HasPrefix(m.MountPath, "/var/spool/slurmd/conf-cache") {
			t.Errorf("mount into slurmd conf-cache would break dynamic registration (EBUSY): %q", m.MountPath)
		}
	}
	for _, v := range pod.Volumes {
		if v.ConfigMap != nil && v.ConfigMap.Name == "wm-gres-conf" {
			t.Errorf("wm-gres-conf mount was re-added; gres.conf must come from the controller (configFiles), not a pod mount")
		}
	}
}

// TestToJobSetUsesConfigGPUResourceNameWithNoLocalFallback is the audit D6
// cleanup regression: translate.go's own "" -> "nvidia.com/gpu" fallback was
// removed as a third, drift-prone copy of the default that
// config.Config.ApplyDefaults already owns. A Config that has NOT gone
// through ApplyDefaults (GPUResourceName left empty) must therefore produce
// an empty extended-resource name, proving there is no hidden fallback left
// in translate.go.
func TestToJobSetUsesConfigGPUResourceNameWithNoLocalFallback(t *testing.T) {
	cfg := testConfig()
	cfg.Slurmd.GPUResourceName = "" // simulate a Config that skipped ApplyDefaults
	job := heldJob(52, "mixing", 1, 4)
	job.TresPerNode = "gres/gpu:2"
	js, err := ToJobSet(job, cfg)
	if err != nil {
		t.Fatalf("ToJobSet: %v", err)
	}
	container := js.Spec.ReplicatedJobs[0].Template.Spec.Template.Spec.Containers[0]
	if _, ok := container.Resources.Limits["nvidia.com/gpu"]; ok {
		t.Error("translate.go must not silently fall back to nvidia.com/gpu; that default belongs to config.ApplyDefaults only")
	}
	if got := container.Resources.Limits[""]; got.Value() != 2 {
		t.Errorf("resource requested under the empty name = %v, want 2 (proves no hidden fallback)", got.Value())
	}
}

func TestGPUsPerNodeParsing(t *testing.T) {
	cases := map[string]int64{
		"gres/gpu:2":       2,
		"gres/gpu=2":       2,
		"gres/gpu:a100:4":  4,
		"gres/gpu":         1,
		"":                 0,
		"cpu=4,gres/gpu:1": 1,
		"license/foo:3":    0,
		// audit minor regression: an explicit zero-GPU request must parse as
		// 0, not fall through to the bare-"gres/gpu" default of 1.
		"gres/gpu:0": 0,
		// audit minor regression: "gres/gpufoo" shares the "gres/gpu" prefix
		// but names an unrelated GRES type and must NOT be counted as a GPU
		// request (previously matched by a bare strings.HasPrefix check).
		"gres/gpufoo:2":       0,
		"cpu=4,gres/gpufoo:2": 0,
	}
	for tres, want := range cases {
		j := &slurm.Job{TresPerNode: tres}
		if got := j.GPUsPerNode(); got != want {
			t.Errorf("GPUsPerNode(%q) = %d, want %d", tres, got, want)
		}
	}
}

func TestToJobSetHasReadinessProbe(t *testing.T) {
	js, err := ToJobSet(heldJob(52, "mixing", 1, 1), testConfig())
	if err != nil {
		t.Fatalf("ToJobSet: %v", err)
	}
	probe := js.Spec.ReplicatedJobs[0].Template.Spec.Template.Spec.Containers[0].ReadinessProbe
	if probe == nil || probe.TCPSocket == nil || probe.TCPSocket.Port.IntValue() != 6818 {
		t.Error("slurmd container must have a TCP readiness probe on port 6818")
	}
}

func TestTopologyAnnotations(t *testing.T) {
	podAnnotations := func(cfg *config.Config, job *slurm.Job) map[string]string {
		t.Helper()
		js, err := ToJobSet(job, cfg)
		if err != nil {
			t.Fatalf("ToJobSet: %v", err)
		}
		return js.Spec.ReplicatedJobs[0].Template.Spec.Template.Annotations
	}

	cfgTAS := testConfig()
	cfgTAS.Topology = config.Topology{
		RequiredLevel:  "example.com/rack",
		PreferredLevel: "example.com/block",
	}

	t.Run("switches request becomes required topology", func(t *testing.T) {
		job := heldJob(60, "mixing", 2, 1)
		job.RequiredSwitches = 1
		ann := podAnnotations(cfgTAS, job)
		if got := ann["kueue.x-k8s.io/podset-required-topology"]; got != "example.com/rack" {
			t.Errorf("required-topology = %q, want example.com/rack", got)
		}
		if _, has := ann["kueue.x-k8s.io/podset-preferred-topology"]; has {
			t.Error("must not set preferred when required applies")
		}
	})

	t.Run("no switches falls back to preferred topology", func(t *testing.T) {
		ann := podAnnotations(cfgTAS, heldJob(61, "mixing", 2, 1))
		if got := ann["kueue.x-k8s.io/podset-preferred-topology"]; got != "example.com/block" {
			t.Errorf("preferred-topology = %q, want example.com/block", got)
		}
	})

	t.Run("no topology config means no annotations", func(t *testing.T) {
		if ann := podAnnotations(testConfig(), heldJob(62, "mixing", 2, 1)); len(ann) != 0 {
			t.Errorf("annotations = %v, want none", ann)
		}
	})
}

func TestToJobSetRejectsUnmappedPartition(t *testing.T) {
	if _, err := ToJobSet(heldJob(7, "gpu-unmapped", 1, 1), testConfig()); err == nil {
		t.Fatal("expected error for unmapped partition, got nil")
	}
}

func TestToJobSetTranslatesNodesRequest(t *testing.T) {
	// --nodes=3 --ntasks-per-node=2 --cpus-per-task=2 => 3 pods, 4 CPU each.
	job := heldJob(70, "mixing", 0, 2)
	job.Tasks.Set = false
	job.NodeCount = slurm.Uint64NoVal{Set: true, Number: 3}
	job.TasksPerNode = slurm.Uint64NoVal{Set: true, Number: 2}
	js, err := ToJobSet(job, testConfig())
	if err != nil {
		t.Fatalf("ToJobSet: %v", err)
	}
	spec := js.Spec.ReplicatedJobs[0].Template.Spec
	if *spec.Parallelism != 3 {
		t.Errorf("parallelism = %d, want 3 (one pod per node)", *spec.Parallelism)
	}
	if got := spec.Template.Spec.Containers[0].Resources.Requests.Cpu().Value(); got != 4 {
		t.Errorf("cpu = %d, want 4 (cpus-per-task * tasks-per-node)", got)
	}
	if !strings.Contains(spec.Template.Spec.Containers[0].Args[2], "CPUs=4") {
		t.Errorf("node conf must advertise CPUs=4: %v", spec.Template.Spec.Containers[0].Args[2])
	}
}

// TestToJobSetIgnoresAutoPopulatedNodeCount pins the TC-C1 bug fix: slurmrestd
// auto-populates node_count from its own scheduling estimate even when the user
// asked ONLY for --ntasks. The bridge must not read that as a --nodes request —
// doing so collapsed N tasks onto node_count pods and under-requested CPU (the
// "--ntasks=4 got 1 CPU" symptom). With --ntasks set and --ntasks-per-node
// absent, the MVD "one pod per task" rule must win regardless of node_count.
func TestToJobSetIgnoresAutoPopulatedNodeCount(t *testing.T) {
	job := heldJob(74, "mixing", 4, 1)                      // --ntasks=4 --cpus-per-task=1
	job.NodeCount = slurm.Uint64NoVal{Set: true, Number: 2} // slurm's own estimate
	// TasksPerNode intentionally unset: the user never asked --ntasks-per-node.
	js, err := ToJobSet(job, testConfig())
	if err != nil {
		t.Fatalf("ToJobSet: %v", err)
	}
	spec := js.Spec.ReplicatedJobs[0].Template.Spec
	if *spec.Parallelism != 4 {
		t.Errorf("parallelism = %d, want 4 (one pod per task; node_count ignored)", *spec.Parallelism)
	}
	if got := spec.Template.Spec.Containers[0].Resources.Requests.Cpu().Value(); got != 1 {
		t.Errorf("cpu = %d, want 1 (cpus-per-task, not multiplied by node packing)", got)
	}
}

func TestToJobSetSetsActiveDeadlineFromTimeLimit(t *testing.T) {
	job := heldJob(71, "mixing", 1, 1)
	job.TimeLimit = slurm.Uint64NoVal{Set: true, Number: 60} // 60 min
	js, err := ToJobSet(job, testConfig())
	if err != nil {
		t.Fatalf("ToJobSet: %v", err)
	}
	got := js.Spec.ReplicatedJobs[0].Template.Spec.ActiveDeadlineSeconds
	if got == nil || *got != 3600+360 { // limit + 10% buffer
		t.Errorf("activeDeadlineSeconds = %v, want 3960", got)
	}
	// No limit => no deadline (unlimited jobs rely on active monitoring).
	js2, _ := ToJobSet(heldJob(72, "mixing", 1, 1), testConfig())
	if js2.Spec.ReplicatedJobs[0].Template.Spec.ActiveDeadlineSeconds != nil {
		t.Error("no time limit must mean no activeDeadlineSeconds")
	}
}

func TestToJobSetMountsSharedStorage(t *testing.T) {
	cfg := testConfig()
	cfg.Slurmd.SharedStorage = &config.SharedStorage{
		NFSServer: "nfs.default.svc", NFSPath: "/exports", MountPath: "/home",
	}
	js, err := ToJobSet(heldJob(73, "mixing", 1, 1), cfg)
	if err != nil {
		t.Fatalf("ToJobSet: %v", err)
	}
	podSpec := js.Spec.ReplicatedJobs[0].Template.Spec.Template.Spec
	foundMount := false
	for _, m := range podSpec.Containers[0].VolumeMounts {
		if m.MountPath == "/home" {
			foundMount = true
		}
	}
	foundVol := false
	for _, v := range podSpec.Volumes {
		if v.NFS != nil && v.NFS.Server == "nfs.default.svc" {
			foundVol = true
		}
	}
	if !foundMount || !foundVol {
		t.Errorf("shared storage not wired: mount=%v volume=%v", foundMount, foundVol)
	}
}

func TestToJobSetDefaultsToOneTaskOneCPU(t *testing.T) {
	job := heldJob(8, "mixing", 0, 0)
	job.Tasks.Set = false
	job.CPUsPerTask.Set = false
	js, err := ToJobSet(job, testConfig())
	if err != nil {
		t.Fatalf("ToJobSet: %v", err)
	}
	jobSpec := js.Spec.ReplicatedJobs[0].Template.Spec
	if *jobSpec.Parallelism != 1 {
		t.Errorf("parallelism = %d, want 1 (sbatch default)", *jobSpec.Parallelism)
	}
	if got := jobSpec.Template.Spec.Containers[0].Resources.Requests.Cpu().Value(); got != 1 {
		t.Errorf("cpu request = %d, want 1 (sbatch default)", got)
	}
}

// TestToJobSetCarriesSubmitTimeAnnotation is the A3 identity-anchor
// regression: a job with a usable submit_time must have it stamped as
// SlurmSubmitTimeAnnotation (the value bridge.ensureJobSet compares on
// AlreadyExists to detect a reused job ID), and a job WITHOUT one must
// produce nil annotations — byte-identical metadata to what pre-A3 bridge
// versions created, so "identity unknowable" stays distinct from any real
// value.
func TestToJobSetCarriesSubmitTimeAnnotation(t *testing.T) {
	job := heldJob(91, "mixing", 1, 1)
	job.SubmitTime = slurm.Uint64NoVal{Set: true, Number: 1751900000}
	js, err := ToJobSet(job, testConfig())
	if err != nil {
		t.Fatalf("ToJobSet: %v", err)
	}
	if got := js.Annotations[SlurmSubmitTimeAnnotation]; got != "1751900000" {
		t.Errorf("annotation %s = %q, want 1751900000", SlurmSubmitTimeAnnotation, got)
	}

	cases := map[string]slurm.Uint64NoVal{
		"unset":    {},
		"zero":     {Set: true, Number: 0},
		"infinite": {Set: true, Infinite: true, Number: 5},
	}
	for name, st := range cases {
		t.Run(name, func(t *testing.T) {
			job := heldJob(92, "mixing", 1, 1)
			job.SubmitTime = st
			js, err := ToJobSet(job, testConfig())
			if err != nil {
				t.Fatalf("ToJobSet: %v", err)
			}
			if js.Annotations != nil {
				t.Errorf("annotations = %v, want nil when submit_time is unusable (%s)", js.Annotations, name)
			}
		})
	}
}

// TestToJobSetUnprivilegedSlurmdCapabilities pins the minimal-capability set
// of the slurmd.privileged=false path. SETUID/SETGID are load-bearing: the
// 2026-07-11 real-Slurm e2e showed slurmd 26.05.1 dying at startup
// ("Failed to drop supplementary groups, setgroups: Operation not
// permitted") without them, so a pod from this path could never register as
// a node. A capability silently dropped from this list reproduces that
// outage.
func TestToJobSetUnprivilegedSlurmdCapabilities(t *testing.T) {
	cfg := testConfig()
	privileged := false
	cfg.Slurmd.Privileged = &privileged
	js, err := ToJobSet(heldJob(7, "mixing", 1, 1), cfg)
	if err != nil {
		t.Fatalf("ToJobSet: %v", err)
	}
	sc := js.Spec.ReplicatedJobs[0].Template.Spec.Template.Spec.Containers[0].SecurityContext
	if sc == nil || sc.Privileged == nil || *sc.Privileged {
		t.Fatal("privileged=false config must render an explicitly unprivileged container")
	}
	if sc.Capabilities == nil {
		t.Fatal("unprivileged slurmd must carry an explicit capability set")
	}
	want := []string{"SYS_ADMIN", "SYS_NICE", "NET_ADMIN", "SETUID", "SETGID", "CHOWN"}
	got := map[string]bool{}
	for _, c := range sc.Capabilities.Add {
		got[string(c)] = true
	}
	for _, w := range want {
		if !got[w] {
			t.Errorf("capability %s missing from the unprivileged slurmd add-set %v", w, sc.Capabilities.Add)
		}
	}
	if len(sc.Capabilities.Drop) != 1 || sc.Capabilities.Drop[0] != "ALL" {
		t.Errorf("unprivileged slurmd must drop ALL first, got %v", sc.Capabilities.Drop)
	}
}

// TestToJobSetGPUJobsGetTolerationAndNvidiaEnv pins the two TC-F1 live
// findings (suite F, 2026-07-12) that shipped without tests: a GPU job's
// slurmd pod must (a) tolerate the nvidia.com/gpu:NoSchedule taint GKE GPU
// node pools carry — without it the autoscaler refuses to scale up and the
// pod Pends forever — and (b) carry the four NVIDIA env vars the Slinky
// slurmd image lacks, without which slurmd sees 0 GPUs via NVML and the
// node registers INVAL (gres count below configured). A non-GPU job must
// get neither.
func TestToJobSetGPUJobsGetTolerationAndNvidiaEnv(t *testing.T) {
	gpuJob := heldJob(93, "mixing", 1, 1)
	gpuJob.TresPerNode = "gres/gpu:2"
	js, err := ToJobSet(gpuJob, testConfig())
	if err != nil {
		t.Fatalf("ToJobSet: %v", err)
	}
	podSpec := js.Spec.ReplicatedJobs[0].Template.Spec.Template.Spec
	var tolerated bool
	for _, tol := range podSpec.Tolerations {
		if tol.Key == "nvidia.com/gpu" && tol.Operator == "Exists" && tol.Effect == "NoSchedule" {
			tolerated = true
		}
	}
	if !tolerated {
		t.Error("GPU job's pod must tolerate nvidia.com/gpu:NoSchedule (TC-F1: autoscaler never scales the GPU pool otherwise)")
	}
	envs := map[string]string{}
	for _, e := range podSpec.Containers[0].Env {
		envs[e.Name] = e.Value
	}
	for _, want := range []string{"PATH", "LD_LIBRARY_PATH", "NVIDIA_VISIBLE_DEVICES", "NVIDIA_DRIVER_CAPABILITIES"} {
		if envs[want] == "" {
			t.Errorf("GPU job's slurmd container missing env %s (TC-F1: slurmd sees 0 GPUs and the node goes INVAL)", want)
		}
	}

	plainJob := heldJob(94, "mixing", 1, 1)
	js2, err := ToJobSet(plainJob, testConfig())
	if err != nil {
		t.Fatalf("ToJobSet: %v", err)
	}
	plainSpec := js2.Spec.ReplicatedJobs[0].Template.Spec.Template.Spec
	for _, tol := range plainSpec.Tolerations {
		if tol.Key == "nvidia.com/gpu" {
			t.Error("non-GPU job must NOT tolerate GPU taints (it would schedule onto scarce GPU nodes)")
		}
	}
	for _, e := range plainSpec.Containers[0].Env {
		if e.Name == "NVIDIA_VISIBLE_DEVICES" {
			t.Error("non-GPU job must NOT carry NVIDIA env vars")
		}
	}
}
