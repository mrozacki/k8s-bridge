// Package translate converts a held Slurm job into the JobSet that will host
// its dynamic slurmd nodes. This is the heart of k8s-bridge: everything else
// is plumbing around this mapping.
package translate

import (
	"fmt"
	"strconv"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/mrozacki/k8s-bridge/internal/config"
	"github.com/mrozacki/k8s-bridge/internal/slurm"
)

const (
	// ManagedByLabel marks JobSets owned by the bridge so reconciliation can
	// find them without touching anything else in the namespace.
	ManagedByLabel = "k8s-bridge.x-k8s.io/managed-by"
	ManagedByValue = "k8s-bridge"
	// SlurmJobIDLabel carries the originating Slurm job ID.
	SlurmJobIDLabel = "k8s-bridge.x-k8s.io/slurm-job-id"
	// WorkloadMixingLabel carries the name of the WorkloadMixing CR whose
	// Bridge created this JobSet (ADR-0015 Phase A). In supervisor mode
	// several Bridges — one per CR, each bound to its own Slurm cluster —
	// share ONE JobSet namespace (every CR lives in POD_NAMESPACE, and a
	// CR's namespace is where its JobSets are created), so ManagedByLabel
	// alone can no longer answer "is this JobSet MINE?": bridge A's cleanup
	// pass would look bridge B's JobSets up in bridge A's Slurm cluster,
	// find nothing, conclude "job gone" and delete B's LIVE JobSets. The
	// label is stamped only when cfg.WorkloadMixingName is set (supervisor
	// mode); single-CR and file modes leave it off and keep their historic
	// managed-by-only ownership behavior exactly as before.
	WorkloadMixingLabel = "k8s-bridge.x-k8s.io/workloadmixing"
	// SlurmSubmitTimeAnnotation carries the originating Slurm job's submit
	// time (unix seconds) as the job's identity anchor (A3). The JobSet name
	// is derived from the job ID alone, and Slurm REUSES job IDs once
	// slurmctld's counter wraps at MaxJobId — so a brand-new job can collide
	// with a stale, still-live `slurm-job-<id>` JobSet from a previous
	// incarnation and silently inherit capacity shaped for a different job.
	// bridge.ensureJobSet compares this annotation on AlreadyExists to tell
	// "already ours, idempotent retry" from "different job wearing the same
	// ID". An annotation (not a label) because the value is never selected
	// on and a raw unix timestamp is not a great label value anyway.
	SlurmSubmitTimeAnnotation = "k8s-bridge.x-k8s.io/slurm-submit-time"
	// RetainUntilAnnotation carries an RFC3339 timestamp before which a JobSet
	// that outlived its Slurm job must NOT be garbage-collected. It is stamped
	// only on the failure path (a JobSet that turned terminal while its Slurm
	// job had not), and only when config.FailedJobSetRetention is positive.
	//
	// It lives on the object rather than in bridge memory on purpose: by the
	// tick AFTER the failure the Slurm job is cancelled (or purged), so nothing
	// in that later tick's state can still tell that this JobSet is the one
	// worth keeping. The deadline has to be durable, and the JobSet is the only
	// thing that survives.
	RetainUntilAnnotation = "k8s-bridge.x-k8s.io/retain-until"

	queueNameLabel     = "kueue.x-k8s.io/queue-name"
	priorityClassLabel = "kueue.x-k8s.io/priority-class"

	// workersReplicatedJobName is the ReplicatedJob name used in ToJobSet.
	// NodeNames' "<jobset>-workers-0-<i>" pattern is JobSet's own naming
	// convention for pods (<jobset>-<replicatedJob>-<jobIndex>-<podIndex>,
	// jobIndex always 0 here since Replicas is always 1) — the "workers"
	// segment MUST match this constant or NodeNames would compute names for
	// pods that do not exist (audit minor: previously duplicated as a bare
	// string literal in both places).
	workersReplicatedJobName = "workers"
)

// JobSetName builds the deterministic name required by the design: creation
// is an upsert keyed on the Slurm job ID, so retries are idempotent.
func JobSetName(slurmJobID uint64) string {
	return fmt.Sprintf("slurm-job-%d", slurmJobID)
}

// NodeFeature is the Slurm feature tying dynamic nodes to their job.
func NodeFeature(slurmJobID uint64) string {
	return fmt.Sprintf("nodes-for-%d", slurmJobID)
}

// NodeNames returns the Slurm node names the JobSet's pods register under.
// Live finding (experiment 02): the registered name is the pod hostname,
// which is deterministic — <jobset>-<replicatedJob>-0-<completionIndex> —
// so the bridge can delete node records without tracking pods.
func NodeNames(slurmJobID uint64, nTasks int32) []string {
	names := make([]string, 0, nTasks)
	for i := int32(0); i < nTasks; i++ {
		names = append(names, fmt.Sprintf("%s-%s-0-%d", JobSetName(slurmJobID), workersReplicatedJobName, i))
	}
	return names
}

// podShape returns how many pods a job maps to and how many Slurm tasks each
// pod is sized for.
//
// --nodes translation (phase C): when the user asked for N nodes, we create N
// pods, each sized for --ntasks-per-node tasks. Otherwise the MVD rule applies:
// one pod per task. Note slurmrestd auto-populates node_count from its own
// scheduling estimate, so a bare --ntasks job must NOT be read as a --nodes
// request — see TestToJobSetIgnoresAutoPopulatedNodeCount.
func podShape(job *slurm.Job) (pods int32, tasksPerPod int64) {
	pods, tasksPerPod = job.NTasks(), 1
	if job.TasksPerNode.Set && job.TasksPerNode.Number > 0 && job.Nodes() > 0 {
		return job.Nodes(), job.TasksPerNodeOrDefault()
	}
	if (!job.Tasks.Set || job.Tasks.Number == 0) && job.Nodes() > 0 {
		return job.Nodes(), 1
	}
	return pods, tasksPerPod
}

// PodCount reports how many pods — and therefore how many dynamic Slurm node
// records — a job's JobSet has. Callers that must clean up node records for a
// job whose JobSet is already gone cannot read the JobSet's parallelism, so
// they recompute it from the same job fields ToJobSet used.
func PodCount(job *slurm.Job) int32 {
	pods, _ := podShape(job)
	return pods
}

// ToJobSet maps one held Slurm job to its JobSet. The mapping implements the
// MVD rules: one pod per Slurm task, cpus-per-task as the pod's CPU request,
// partition mapped to a Kueue WorkloadPriorityClass.
func ToJobSet(job *slurm.Job, cfg *config.Config) (*jobsetv1alpha2.JobSet, error) {
	priorityClass, ok := cfg.PriorityClassFor(job.Partition)
	if !ok {
		return nil, fmt.Errorf("partition %q has no priority-class mapping", job.Partition)
	}

	nTasks, perPodTasks := podShape(job)
	cpu := resource.NewQuantity(job.CPUs()*perPodTasks, resource.DecimalSI)
	// Memory: honor --mem-per-cpu (MB); MemPerCPUMB defaults to 1024 when
	// the job did not specify memory.
	memMB := job.MemPerCPUMB() * job.CPUs() * perPodTasks
	mem := resource.NewQuantity(memMB*(1<<20), resource.BinarySI)
	gpus := job.GPUsPerNode()

	podSpec := corev1.PodSpec{
		RestartPolicy: corev1.RestartPolicyNever,
		Containers: []corev1.Container{{
			Name:            "slurmd",
			Image:           cfg.Slurmd.Image,
			SecurityContext: slurmdSecurityContext(cfg),
			Args: []string{
				"-Z", // register as a dynamic node
				// Live finding (experiment 02): slurmd autodetects the HOST's
				// hardware, not the pod's cgroup limits, so the node must
				// explicitly advertise the pod-sized resources or Slurm packs
				// multiple tasks onto one pod. Keyword is plural "Features".
				"--conf", slurmdConf(job),
				"--conf-server", cfg.Slurmd.ConfServer,
				// Live finding (TC-D3): when the JobSet recreates a slurmd
				// pod under the same node name, slurmctld still believes the
				// old job occupies it and the job hangs as a ghost. -b makes
				// slurmd report a node boot on startup, so slurmctld requeues
				// or fails the job instead of leaving it pinned to a dead pod.
				"-b",
			},
			// Ready only once slurmd listens on its node port — a close
			// proxy for "registered with slurmctld", giving Kueue's
			// WaitForPodsReady a truthful signal.
			ReadinessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					TCPSocket: &corev1.TCPSocketAction{
						Port: intstr.FromInt32(6818),
					},
				},
				InitialDelaySeconds: 2,
				PeriodSeconds:       3,
			},
			Resources: podResources(cfg, cpu, mem, gpus),
			VolumeMounts: []corev1.VolumeMount{{
				Name:      "slurm-key",
				MountPath: "/etc/slurm/slurm.key",
				SubPath:   "slurm.key",
				ReadOnly:  true,
			}},
		}},
		Volumes: []corev1.Volume{{
			Name: "slurm-key",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  cfg.Slurmd.AuthSecretName,
					DefaultMode: ptr[int32](0o600),
				},
			},
		}},
	}

	// NOTE (L1, ADR-0010 revision — e2e iteration 2, 2026-07-06): we do NOT
	// mount a gres.conf into the slurmd pod. An earlier version
	// bind-mounted a count-only gres.conf at
	// "/var/spool/slurmd/conf-cache/gres.conf" because configless slurmd was
	// believed not to receive gres.conf from the controller. On Slurm 26.05
	// that mount BROKE dynamic-node registration: configless slurmd fetches
	// its whole config from slurmctld and writes it under conf-cache/, and
	// renaming its freshly-written "gres.conf.new" over our bind-mounted,
	// read-only "gres.conf" fails with "_write_conf: ... Device or resource
	// busy", so slurmd aborts before ever registering (found live). The
	// controller already distributes the count-only gres.conf (Name=gpu
	// Count=1) to configless slurmd via the Slurm chart's configFiles, so the
	// per-pod mount was both redundant and the direct cause of the failure —
	// hence removed. The gpus value still drives the extended-resource
	// REQUEST on the pod above.

	if ss := cfg.Slurmd.SharedStorage; ss != nil {
		podSpec.Containers[0].VolumeMounts = append(podSpec.Containers[0].VolumeMounts,
			corev1.VolumeMount{Name: "shared-storage", MountPath: ss.MountPath})
		podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
			Name: "shared-storage",
			VolumeSource: corev1.VolumeSource{
				NFS: &corev1.NFSVolumeSource{Server: ss.NFSServer, Path: ss.NFSPath},
			},
		})
	}

	if gpus > 0 {
		podSpec.Tolerations = append(podSpec.Tolerations, corev1.Toleration{
			Key:      "nvidia.com/gpu",
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoSchedule,
		})
		podSpec.Containers[0].Env = append(podSpec.Containers[0].Env,
			corev1.EnvVar{Name: "PATH", Value: "/usr/local/nvidia/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"},
			corev1.EnvVar{Name: "LD_LIBRARY_PATH", Value: "/usr/local/nvidia/lib64"},
			corev1.EnvVar{Name: "NVIDIA_VISIBLE_DEVICES", Value: "all"},
			corev1.EnvVar{Name: "NVIDIA_DRIVER_CAPABILITIES", Value: "compute,utility"},
		)
	}

	// --exclusive (TC-B6): whole-node access. In the bridge model each slurmd
	// pod already IS a dedicated Slurm node sized to one task, so "exclusive"
	// reduces to "no other bridge slurmd pod shares this pod's Kubernetes node".
	// A required pod anti-affinity keyed on ManagedByLabel enforces exactly one
	// bridge slurmd pod per node — the faithful translation of --exclusive the
	// live test (TC-B6) found missing, where the flag was silently ignored. It
	// is HARD (RequiredDuringScheduling) because --exclusive is a hard request
	// in Slurm: an exclusive job waits for whole nodes rather than packing,
	// mirroring Slurm's own semantics.
	if job.IsExclusive() {
		podSpec.Affinity = &corev1.Affinity{
			PodAntiAffinity: &corev1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
					TopologyKey: "kubernetes.io/hostname",
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{ManagedByLabel: ManagedByValue},
					},
				}},
			},
		}
	}

	maxRestarts := int32(2)
	backoffLimit := int32(0)

	// Resource-leak guard (MVD requirement): cap the JobSet's Jobs at the
	// Slurm wall-clock limit plus a buffer of max(10%, 5 min); a JobSet
	// whose slurmd pods outlive the job is force-completed by Kubernetes.
	var activeDeadline *int64
	if limit := job.TimeLimitSeconds(); limit > 0 {
		buffer := limit / 10
		if buffer < 300 {
			buffer = 300
		}
		activeDeadline = ptr(limit + buffer)
	}

	labels := map[string]string{
		ManagedByLabel:  ManagedByValue,
		SlurmJobIDLabel: fmt.Sprintf("%d", job.JobID),
		// A1b: a partition may target its own LocalQueue instead of
		// the global one (multi-team support); LocalQueueFor falls
		// back to cfg.LocalQueue when no override is set.
		queueNameLabel:     cfg.LocalQueueFor(job.Partition),
		priorityClassLabel: priorityClass,
	}
	// ADR-0015 Phase A: in supervisor mode every JobSet carries its owning
	// CR's name so each per-CR Bridge selects only its OWN JobSets (see
	// WorkloadMixingLabel). Unset in single-CR/file mode — labels stay
	// byte-identical to what those modes always produced.
	if cfg.WorkloadMixingName != "" {
		labels[WorkloadMixingLabel] = cfg.WorkloadMixingName
	}

	return &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      JobSetName(job.JobID),
			Namespace: cfg.Namespace,
			// A3: the submit-time identity anchor. Nil when the job carries
			// no usable submit_time (older slurmrestd, test fakes) — the
			// AlreadyExists identity check then degrades to the historic
			// "name match is enough" behavior; see submitTimeAnnotation.
			Annotations: submitTimeAnnotation(job),
			Labels:      labels,
		},
		Spec: jobsetv1alpha2.JobSetSpec{
			// A pod failure restarts the whole JobSet (fresh dynamic nodes);
			// Slurm requeues the job when its nodes vanish.
			FailurePolicy: &jobsetv1alpha2.FailurePolicy{MaxRestarts: maxRestarts},
			ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{{
				Name:     workersReplicatedJobName,
				Replicas: 1,
				Template: batchv1.JobTemplateSpec{
					Spec: batchv1.JobSpec{
						Parallelism:           &nTasks,
						Completions:           &nTasks,
						BackoffLimit:          &backoffLimit,
						ActiveDeadlineSeconds: activeDeadline,
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: map[string]string{
									SlurmJobIDLabel: fmt.Sprintf("%d", job.JobID),
									// ManagedByLabel on the POD (not just the JobSet)
									// is what --exclusive's pod anti-affinity selects
									// on: it repels every bridge slurmd pod from a node
									// that already hosts one, giving whole-node access.
									ManagedByLabel: ManagedByValue,
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

func ptr[T any](v T) *T { return &v }

// submitTimeAnnotation returns the A3 identity annotation for a job, or nil
// when the job has no usable submit_time ({set:false} from an older parser,
// or a hand-built test job). Returning nil rather than an empty-string value
// keeps "identity unknowable" (skip the check, upgrade-compatible) distinct
// from "identity known" in ensureJobSet's comparison, and leaves JobSets from
// annotation-unaware bridge versions and submit-time-less jobs byte-identical
// to what those versions produced.
func submitTimeAnnotation(job *slurm.Job) map[string]string {
	if !job.SubmitTime.Set || job.SubmitTime.Infinite || job.SubmitTime.Number == 0 {
		return nil
	}
	return map[string]string{
		SlurmSubmitTimeAnnotation: strconv.FormatUint(job.SubmitTime.Number, 10),
	}
}

// slurmdSecurityContext builds the slurmd container's security context.
//
// slurmd needs writable cgroups and dies in a fully unprivileged container.
// This is an upstream (slurmd/slinky) constraint — the slurm-operator runs its
// own nodeset pods privileged for the same reason — not something k8s-bridge
// can lift on its own; reducing it to a minimal-capability set is tracked as an
// upstream dependency in the threat model.
//
// Default: Privileged. Operators who have an unprivileged-capable slurmd image
// can set slurmd.privileged=false to switch to an explicit minimal-capability
// context instead (still incompatible with GKE Autopilot, but closer to the
// restricted Pod Security Standard). The blanket "Privileged + Capabilities.Add"
// of the original was redundant — Privileged already grants all capabilities —
// and is dropped.
func slurmdSecurityContext(cfg *config.Config) *corev1.SecurityContext {
	if cfg.Slurmd.PrivilegedOrDefault() {
		return &corev1.SecurityContext{Privileged: ptr(true)}
	}
	return &corev1.SecurityContext{
		Privileged:               ptr(false),
		AllowPrivilegeEscalation: ptr(true), // slurmd re-exec needs it under the added caps
		Capabilities: &corev1.Capabilities{
			Drop: []corev1.Capability{"ALL"},
			// SETUID/SETGID/CHOWN: live e2e findings (2026-07-11, first runs
			// of the unprivileged path against a real slurmctld 26.05.1),
			// each bisected against a real job launch:
			//   - without SETUID/SETGID, slurmd dies at startup with
			//     `fatal: Failed to drop supplementary groups, setgroups:
			//     Operation not permitted` before it even contacts the conf
			//     server — identity switching is core slurmd behavior;
			//   - without CHOWN, slurmd starts and registers, but EVERY
			//     batch launch dies instantly (`slurmstepd return code -1`)
			//     and the job is requeued forever: slurmstepd chowns the
			//     job's spool/output files to the job owner before dropping
			//     privileges. KILL and DAC_OVERRIDE were bisected as NOT
			//     required (slurmd runs as container root, which signals and
			//     reads its own trees without them).
			// Without these three the privileged=false knob produced a pod
			// that could never run a job, on any cluster.
			Add: []corev1.Capability{"SYS_ADMIN", "SYS_NICE", "NET_ADMIN", "SETUID", "SETGID", "CHOWN"},
		},
	}
}

// slurmdConf builds the dynamic node's advertised configuration: the pinning
// feature and pod-sized resources (CPUs, RealMemory in MB, optional GRES).
//
// Live finding: the slurmd image's entrypoint word-splits its
// arguments, so a multi-parameter conf string MUST carry inner single quotes
// (the slurm-operator does the same: --conf "'Features=slinky'"). Without
// them only the first key=value survives and the node advertises host-sized
// resources.
func slurmdConf(job *slurm.Job) string {
	perPodTasks := int64(1)
	if job.Nodes() > 0 {
		perPodTasks = job.TasksPerNodeOrDefault()
	}
	conf := fmt.Sprintf("Features=%s CPUs=%d RealMemory=%d",
		NodeFeature(job.JobID), job.CPUs()*perPodTasks, job.MemPerCPUMB()*job.CPUs()*perPodTasks)
	if gpus := job.GPUsPerNode(); gpus > 0 {
		conf += fmt.Sprintf(" Gres=gpu:%d", gpus)
	}
	return "'" + conf + "'"
}

// topologyAnnotations maps the Slurm topology request to Kueue TAS podset
// annotations: --switches=N becomes a HARD constraint at the configured
// required level (all task pods in one topology domain); jobs without an
// explicit request get the best-effort preferred level, if configured.
func topologyAnnotations(job *slurm.Job, cfg *config.Config) map[string]string {
	switch {
	case job.WantsSwitchLocality() && cfg.Topology.RequiredLevel != "":
		return map[string]string{
			"kueue.x-k8s.io/podset-required-topology": cfg.Topology.RequiredLevel,
		}
	case cfg.Topology.PreferredLevel != "":
		return map[string]string{
			"kueue.x-k8s.io/podset-preferred-topology": cfg.Topology.PreferredLevel,
		}
	default:
		return nil
	}
}

// podResources sizes the slurmd container to exactly what the Slurm job's
// task needs, so Kueue quota and Slurm node capacity stay in lockstep.
func podResources(cfg *config.Config, cpu, mem *resource.Quantity, gpus int64) corev1.ResourceRequirements {
	rl := corev1.ResourceList{
		corev1.ResourceCPU:    *cpu,
		corev1.ResourceMemory: *mem,
	}
	if gpus > 0 {
		// audit D6 cleanup: the "" fallback to "nvidia.com/gpu" here was a
		// third copy of the same default already owned by
		// config.Config.ApplyDefaults, which both loaders (file and CR) now
		// call unconditionally before a Config is ever used. Keeping it here
		// too invited the three copies drifting out of sync; every Config
		// reaching ToJobSet has GPUResourceName already set.
		rl[corev1.ResourceName(cfg.Slurmd.GPUResourceName)] = *resource.NewQuantity(gpus, resource.DecimalSI)
	}
	return corev1.ResourceRequirements{Requests: rl, Limits: rl}
}
