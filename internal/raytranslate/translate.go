// Package raytranslate converts an inner RayJob into the JobSet of dedicated
// Ray worker pods that will host it. This is the heart of ray-bridge, the
// direct analog of internal/translate on the Slurm side: everything else is
// plumbing around this mapping.
//
// The workers `ray start` into the shared RayCluster's head, advertising a
// unique Ray custom resource wm-job-<id>. Kueue admits this JobSet against the
// shared ClusterQueue exactly like a slurmd JobSet; the reconciler then
// unsuspends the RayJob with entrypointResources requesting wm-job-<id> so
// Ray's scheduler pins its work onto precisely these workers (ADR-0006/0012).
package raytranslate

import (
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/mrozacki/k8s-bridge/internal/ray"
	"github.com/mrozacki/k8s-bridge/internal/rayconfig"
)

const (
	// ManagedByLabel marks JobSets owned by ray-bridge, so reconciliation can
	// find them without touching anything else in the namespace. Distinct from
	// the Slurm bridge's managed-by value so the two never collide in a shared
	// namespace.
	ManagedByLabel = "ray-bridge.x-k8s.io/managed-by"
	ManagedByValue = "ray-bridge"
	// RayJobLabel / RayJobUIDLabel tie the JobSet back to its RayJob. The UID
	// label is the durable identity (a name can be reused); the name label is
	// for human/kubectl convenience.
	RayJobLabel    = "ray-bridge.x-k8s.io/ray-job"
	RayJobUIDLabel = "ray-bridge.x-k8s.io/ray-job-uid"

	queueNameLabel     = "kueue.x-k8s.io/queue-name"
	priorityClassLabel = "kueue.x-k8s.io/priority-class"

	// workersReplicatedJobName mirrors the Slurm side's "workers" ReplicatedJob
	// name; WorkerPodNames depends on it for the deterministic pod hostnames.
	workersReplicatedJobName = "workers"

	// rayWorkerContainerName is the single container per worker pod.
	rayWorkerContainerName = "ray-worker"

	// WorkerJobSetPrefix is prepended to a RayJob's name to form its worker
	// JobSet name. Kept as a constant so the reverse mapping
	// (RayJobNameFromJobSet) and the forward one never drift.
	WorkerJobSetPrefix = "ray-workers-"
)

// WorkerJobSetName is the deterministic name of the worker JobSet for a RayJob,
// making creation an idempotent upsert keyed on the RayJob (retries are safe).
func WorkerJobSetName(job *ray.RayJob) string {
	return WorkerJobSetPrefix + job.Name
}

// RayJobNameFromJobSet reverses WorkerJobSetName: given a worker JobSet name it
// returns the RayJob name and true, or ("", false) for a name that is not one
// of ours. The reconciler uses this to map a Workload/JobSet event back to the
// RayJob it should reconcile.
func RayJobNameFromJobSet(jobSetName string) (string, bool) {
	return strings.CutPrefix(jobSetName, WorkerJobSetPrefix)
}

// PinResource is the Ray custom resource that ties the dedicated workers to
// their inner job — the analog of the Slurm side's Features=nodes-for-<id>.
// The workers advertise it; the RayJob's entrypointResources will request it.
func PinResource(job *ray.RayJob) string {
	return fmt.Sprintf("wm-job-%s", job.Name)
}

// WorkerPodNames returns the deterministic pod hostnames the JobSet's workers
// run under (<jobset>-<replicatedJob>-<i>-0), so the reconciler can identify /
// drain them without tracking pods live — same trick as translate.NodeNames.
// Each worker is its own child Job (ReplicatedJob.Replicas = job.Workers), so
// the varying index is the JOB index and the pod index is always 0.
func WorkerPodNames(job *ray.RayJob) []string {
	names := make([]string, 0, job.Workers)
	base := WorkerJobSetName(job)
	for i := int32(0); i < job.Workers; i++ {
		names = append(names, fmt.Sprintf("%s-%s-%d-0", base, workersReplicatedJobName, i))
	}
	return names
}

// ToWorkerJobSet maps one inner RayJob to the JobSet of dedicated Ray workers.
// It fails (permanent error, the caller should surface it on the RayJob) when
// the job is out of scope: its target cluster is not managed, or its pool has
// no priority-class mapping.
func ToWorkerJobSet(job *ray.RayJob, cfg *rayconfig.Config) (*jobsetv1alpha2.JobSet, error) {
	if !job.IsInnerWorkload() {
		return nil, fmt.Errorf("RayJob %s/%s has no clusterSelector target; not an inner workload", job.Namespace, job.Name)
	}
	headAddr, managed := cfg.HeadAddressFor(job.TargetCluster)
	if !managed {
		return nil, fmt.Errorf("RayJob %s/%s targets unmanaged cluster %q", job.Namespace, job.Name, job.TargetCluster)
	}
	priorityClass, ok := cfg.PriorityClassFor(job.Pool)
	if !ok {
		return nil, fmt.Errorf("RayJob %s/%s pool %q has no priority-class mapping", job.Namespace, job.Name, job.Pool)
	}

	cpu := resource.NewQuantity(job.CPUsPerWorker, resource.DecimalSI)
	mem := resource.NewQuantity(job.MemPerWorkerMB*(1<<20), resource.BinarySI)

	podSpec := corev1.PodSpec{
		RestartPolicy: corev1.RestartPolicyNever,
		Containers: []corev1.Container{{
			Name:            rayWorkerContainerName,
			Image:           cfg.Worker.Image,
			SecurityContext: workerSecurityContext(cfg),
			// bash -lc so the JSON --resources value survives with its quotes
			// intact (the entrypoint word-splits otherwise — the same class of
			// quoting issue the Slurm side hit with slurmd --conf). `exec` so the
			// ray process is PID 1's child and receives signals; `--block` keeps
			// the worker in the foreground for the pod's lifetime.
			Command:   []string{"/bin/bash", "-lc", workerStartCommand(job, headAddr)},
			Resources: podResources(cfg, cpu, mem, job.GPUsPerWorker),
			// Readiness = the worker can reach a healthy GCS at the head, a
			// close proxy for "joined the cluster" and the signal Kueue's
			// WaitForPodsReady gangs on. NOTE (ADR-0012 open item, R0 skipped):
			// the exact worker-join readiness signal must be confirmed against a
			// live KubeRay cluster; `ray health-check` is the current best guess.
			ReadinessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					Exec: &corev1.ExecAction{
						Command: []string{"/bin/bash", "-lc",
							fmt.Sprintf("ray health-check --address %s", headAddr)},
					},
				},
				InitialDelaySeconds: 5,
				PeriodSeconds:       5,
			},
		}},
	}

	backoffLimit := int32(0)
	maxRestarts := int32(2)
	// Each dedicated worker is its OWN child Job (ReplicatedJob.Replicas =
	// job.Workers, one pod per Job) rather than one Job with Parallelism =
	// workers. This makes the readiness gate meaningful: JobSet's
	// ReplicatedJobStatus.Ready counts ready child JOBS, so summing it and
	// comparing to job.Workers is a true "all workers joined" signal. With the
	// single-Job-many-pods shape, Ready would top out at 1 and a multi-worker
	// job could never satisfy the gate (review finding, 2026-07-06).
	one := int32(1)

	return &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: WorkerJobSetName(job),
			// The worker JobSet lives in the same namespace as its RayJob (the
			// reconciler reads and deletes it there). cfg.Namespace only scopes
			// what the bridge watches; it is not the per-object namespace.
			Namespace: job.Namespace,
			Labels: map[string]string{
				ManagedByLabel:     ManagedByValue,
				RayJobLabel:        job.Name,
				RayJobUIDLabel:     job.UID,
				queueNameLabel:     cfg.LocalQueueFor(job.Pool, job.LocalQueue),
				priorityClassLabel: priorityClass,
			},
		},
		Spec: jobsetv1alpha2.JobSetSpec{
			// A worker failure restarts the whole set (fresh workers rejoin);
			// the reconciler re-suspends and retries the RayJob if that happens.
			FailurePolicy: &jobsetv1alpha2.FailurePolicy{MaxRestarts: maxRestarts},
			ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{{
				Name:     workersReplicatedJobName,
				Replicas: job.Workers,
				Template: batchv1.JobTemplateSpec{
					Spec: batchv1.JobSpec{
						Parallelism:  &one,
						Completions:  &one,
						BackoffLimit: &backoffLimit,
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: map[string]string{
									RayJobUIDLabel: job.UID,
								},
								Annotations: topologyAnnotations(job, cfg),
							},
							Spec: podSpec,
						},
					},
				},
			}},
		},
	}, nil
}

// workerStartCommand builds the `ray start` invocation for a dedicated worker:
// it dials the shared cluster's head, contributes its real CPU/GPU capacity,
// and advertises the pin resource so the inner job can be scheduled onto it.
func workerStartCommand(job *ray.RayJob, headAddr string) string {
	cmd := fmt.Sprintf("exec ray start --address=%s --num-cpus=%d", headAddr, job.CPUsPerWorker)
	if job.GPUsPerWorker > 0 {
		cmd += fmt.Sprintf(" --num-gpus=%d", job.GPUsPerWorker)
	}
	// The pin resource carries CPUsPerWorker units, so an inner job requesting
	// N units of wm-job-<id> in entrypointResources lands on N cpus' worth of
	// these dedicated workers.
	cmd += fmt.Sprintf(" --resources='{\"%s\": %d}'", PinResource(job), job.CPUsPerWorker)
	cmd += " --block"
	return cmd
}

// podResources sizes the worker container to exactly the requested per-worker
// capacity, so Kueue quota and the Ray cluster's real capacity stay in
// lockstep — the same invariant the Slurm side maintains.
func podResources(cfg *rayconfig.Config, cpu, mem *resource.Quantity, gpus int64) corev1.ResourceRequirements {
	rl := corev1.ResourceList{
		corev1.ResourceCPU:    *cpu,
		corev1.ResourceMemory: *mem,
	}
	if gpus > 0 {
		rl[corev1.ResourceName(cfg.Worker.GPUResourceName)] = *resource.NewQuantity(gpus, resource.DecimalSI)
	}
	return corev1.ResourceRequirements{Requests: rl, Limits: rl}
}

// workerSecurityContext keeps a Ray worker unprivileged by default (unlike
// slurmd, it does not need writable host cgroups). Exposed as a config
// escape-hatch only for parity.
func workerSecurityContext(cfg *rayconfig.Config) *corev1.SecurityContext {
	if cfg.Worker.Privileged {
		return &corev1.SecurityContext{Privileged: ptr(true)}
	}
	return &corev1.SecurityContext{
		AllowPrivilegeEscalation: ptr(false),
		Privileged:               ptr(false),
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
	}
}

// topologyAnnotations maps a topology request to Kueue TAS podset annotations
// on the worker pods: an inner RayJob's required-topology annotation becomes a
// HARD constraint (all dedicated workers co-located in one topology domain);
// otherwise a config-level preferred level, if set, applies best-effort. This
// mirrors the Slurm side's --switches translation (ADR-0008) and was validated
// live on GKE (2026-07-07: both workers landed in one rack). Returns nil when
// neither is requested, leaving the pod annotations untouched.
func topologyAnnotations(job *ray.RayJob, cfg *rayconfig.Config) map[string]string {
	switch {
	case job.RequiredTopology != "":
		return map[string]string{"kueue.x-k8s.io/podset-required-topology": job.RequiredTopology}
	case cfg.Worker.PreferredTopology != "":
		return map[string]string{"kueue.x-k8s.io/podset-preferred-topology": cfg.Worker.PreferredTopology}
	default:
		return nil
	}
}

func ptr[T any](v T) *T { return &v }
