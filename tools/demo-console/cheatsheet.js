// cheatsheet.js — the numbered steps from experiments/DEMO.md, condensed
// into click-to-copy command blocks for the demo console UI. This is a
// presenter aid, not a generated artifact: keep it in sync with DEMO.md by
// hand when that file's commands change. Loaded by index.html as a plain
// <script> (no bundler, no build step).

const DEMO_STEPS = [
  {
    id: 1,
    title: 'Cluster + full stack bring-up',
    tag: 'GKE',
    say: 'We are standing up one small GKE cluster that hosts Slurm, Kueue, JobSet, KubeRay, and the bridge controller — everything on the same pool of nodes.',
    commands: [
      'cd experiments/01-gke-playground',
      'NUM_NODES=3 MAX_NODES=3 ./scripts/01-create-cluster.sh',
      './scripts/02-install-components.sh',
      './scripts/03-configure-queues.sh',
      'kubectl apply -f ../05-topology/manifests/topology-tas.yaml',
      'kubectl apply -f ../06-multitenant/manifests/cohort-queues.yaml',
      'kubectl apply -f ../../deploy/crd/workloadmixing-crd.yaml',
    ],
  },
  {
    id: 2,
    title: 'Bridge in CRD mode',
    tag: 'GKE',
    say: 'The bridge reads its config from a WorkloadMixing custom resource — the in-cluster production path.',
    commands: [
      "kubectl -n slurm get secret slurm-auth-slurm -o yaml | sed 's/namespace: slurm/namespace: slurm-jobs/' | kubectl apply -f -",
      'kubectl port-forward -n slurm svc/slurm-restapi 6820:6820 &',
      "kubectl -n slurm exec slurm-controller-0 -c slurmctld -- scontrol token username=root lifespan=14400 | sed 's/SLURM_JWT=//' > /tmp/wm-slurm-token",
      'kubectl apply -f ../../deploy/crd/workloadmixing-sample.yaml',
      'cd ../../. && make build',
      './bin/k8s-bridge --workloadmixing slurm-jobs/playground &',
      "kubectl get workloadmixing -n slurm-jobs playground -o yaml | grep -A3 conditions",
    ],
  },
  {
    id: 3,
    title: 'Control plane: leader election, watch-nudge, Events, config hot-reload (ADR-0011)',
    tag: 'GKE',
    say: 'The bridge adopted a controller-runtime Manager: leader election, an informer cache, Kubernetes Events, and watches that nudge the poll loop to run sooner instead of waiting out the full interval.',
    commands: [
      'kubectl get lease -n slurm-jobs k8s-bridge-leader -o yaml | grep -A2 holderIdentity',
      "kubectl -n slurm exec deploy/slurm-login-slinky -- sbatch --partition=mixing --ntasks=1 --wrap='sleep 10'",
      'curl -s http://127.0.0.1:8080/metrics | grep k8s_bridge_tick_trigger_total',
      "kubectl describe jobset -n slurm-jobs $(kubectl get jobset -n slurm-jobs -o jsonpath='{.items[0].metadata.name}') | tail -15",
      'kubectl get workloadmixing -n slurm-jobs playground -o yaml > /tmp/wm-before.yaml',
      "kubectl patch workloadmixing -n slurm-jobs playground --type merge -p '{\"spec\":{\"maxUserPriority\":5000}}'",
    ],
  },
  {
    id: 4,
    title: 'Core loop: plain Slurm job, auto-hold, admit, run, cleanup',
    tag: 'GKE',
    say: 'A plain sbatch, no --hold. The lua plugin auto-holds it, the bridge translates to a JobSet, Kueue admits, nodes register, job runs, everything cleans up.',
    commands: [
      'kubectl -n slurm exec -it deploy/slurm-login-slinky -- bash',
      "sbatch --partition=mixing --ntasks=2 --wrap='srun hostname'",
      'squeue -o "%i %T %k"',
      "sbatch --partition=mixing --array=1-5 --wrap=hostname",
    ],
  },
  {
    id: 5,
    title: 'Resources: memory, GPU simulation, wall-clock guard',
    tag: 'GKE',
    say: 'Memory and GPU requests translate too — GPUs simulated end to end with no real hardware.',
    commands: [
      "sbatch --partition=mixing --ntasks=1 --mem-per-cpu=2G --wrap='sleep 20'",
      "kubectl patch node <NODE> --subresource=status --type=merge -p '{\"status\":{\"capacity\":{\"nvidia.com/gpu\":\"2\"},\"allocatable\":{\"nvidia.com/gpu\":\"2\"}}}'",
      'kubectl create configmap wm-gres-conf -n slurm-jobs --from-literal=gres.conf="Name=gpu File=/dev/null"',
      "sbatch --partition=mixing --gres=gpu:1 --wrap='srun echo GPU job'",
      'sinfo -N -o "%N %G"',
      "sbatch --partition=mixing --nodes=2 --ntasks-per-node=2 --time=10 --wrap='sleep 30'",
    ],
  },
  {
    id: 6,
    title: 'Topology-aware placement',
    tag: 'GKE',
    say: "Slurm's --switches flag flows through to Kueue Topology-Aware Scheduling — co-located dynamic nodes, no Kubernetes awareness needed in Slurm.",
    commands: [
      "sbatch --partition=mixing --ntasks=2 --switches=1 --wrap='sleep 30'",
    ],
  },
  {
    id: 7,
    title: 'Priority — mutable, even while running (ADR-0009)',
    tag: 'GKE',
    say: 'A running job can have its priority bumped after submission; Kueue immediately re-ranks preemption order.',
    commands: [
      "sbatch --partition=mixing --ntasks=1 --time=10 --wrap='sleep 180'",
      'scontrol update job <ID> priority=700',
      'scontrol show job <ID> | grep AdminComment',
      "kubectl get workload -n slurm-jobs -o jsonpath='{.items[0].spec.priority}'",
    ],
  },
  {
    id: 8,
    title: 'Multi-team: cohorts, borrowing, reclaim',
    tag: 'GKE',
    say: 'Two teams share a quota pool; idle capacity is lent out and reclaimed automatically.',
    commands: [
      'kubectl create -f ../experiments/06-multitenant/manifests/cohort-queues.yaml',
      "kubectl get clusterqueue team-b -o jsonpath='{.status.flavorsUsage[0].resources[0]}'",
      "sbatch --partition=mixing --ntasks=6 --wrap='sleep 120'",
      'kubectl get events -n default | grep -i "reclamation within the cohort"',
    ],
  },
  {
    id: 9,
    title: 'Per-partition queues: routing partitions to different Kueue LocalQueues',
    tag: 'GKE',
    say: 'Every partition can target its own Kueue LocalQueue instead of sharing one global queue — the config knob a real multi-team HPC site would use.',
    commands: [
      "kubectl get workloadmixing -n slurm-jobs playground -o jsonpath='{.spec.partitionMappings}' | jq .",
      "kubectl -n slurm exec deploy/slurm-login-slinky -- sbatch --partition=mixing --ntasks=1 --wrap='sleep 20'",
      "kubectl get workload -n slurm-jobs -o jsonpath='{.items[-1:].spec.queueName}'",
    ],
  },
  {
    id: 10,
    title: 'Mixing: Kueue-native batch vs. Slurm job, one quota',
    tag: 'GKE',
    say: 'One Kubernetes Job, one Slurm job, same ClusterQueue, same quota — Kueue does not care which system submitted it.',
    commands: [
      'kubectl create -f ../01-gke-playground/workloads/kueue-batch-job.yaml',
      'kubectl get jobs -n default',
      "kubectl -n slurm exec deploy/slurm-login-slinky -- sbatch --partition=mixing --ntasks=2 --wrap='sleep 90'",
    ],
  },
  {
    id: 11,
    title: 'Serving + batch, two-tier admission (ADR-0007)',
    tag: 'GKE',
    say: 'A high-priority serving scale-up preempts batch outright to get capacity immediately.',
    commands: [
      'kubectl apply -f ../experiments/04-serving-admission/manifests/serving-queued.yaml',
      'kubectl scale deployment queued-inference --replicas=3',
    ],
  },
  {
    id: 12,
    title: 'Ray — shared cluster, inner-job gap, pinned worker, RayService',
    tag: 'GKE',
    say: 'Three Ray patterns: infrastructure cluster outside Kueue, the admission gap for jobs inside it, and the pinned-worker mechanism that closes the gap.',
    commands: [
      'kubectl apply -f ../experiments/03-open-items/manifests/ray-shared-cluster.yaml',
      'kubectl apply -f ../experiments/03-open-items/manifests/ray-inner-job.yaml',
      'kubectl run pinned-worker --image=rayproject/ray:2.46.0 --labels="kueue.x-k8s.io/queue-name=team-a" --overrides=\'{"spec":{"containers":[{"name":"pinned-worker","image":"rayproject/ray:2.46.0","command":["bash","-c","ray start --address=shared-ray-head-svc.default.svc.cluster.local:6379 --num-cpus=1 --resources=\'"\'"\'{\\"wm-job-demo\\": 1}\'"\'"\' --block"],"resources":{"requests":{"cpu":"1","memory":"2Gi"},"limits":{"cpu":"1","memory":"2Gi"}}}]}}\' --restart=Never',
      'kubectl apply -f ../experiments/03-open-items/manifests/ray-service.yaml',
    ],
  },
  {
    id: 13,
    title: 'DWS Flex — queued provisioning (optional add-on)',
    tag: 'GKE optional',
    say: 'Queued node provisioning via Kueue AdmissionCheck. Known limit: GKE rejects CPU-only DWS requests today.',
    commands: [
      'gcloud container node-pools create dws-flex --cluster k8s-bridge-playground --zone europe-west1-b --machine-type e2-standard-2 --num-nodes 0 --enable-autoscaling --min-nodes 0 --max-nodes 2 --flex-start --reservation-affinity=none',
      'kubectl get provisioningrequests -A',
      'kubectl apply -f ../experiments/08-ccc-dws/manifests/compute-classes.yaml',
      'kubectl get nodes -L machine-family,cloud.google.com/gke-spot,compute-class',
    ],
  },
  {
    id: 14,
    title: 'Failure path: JobSet dies, Slurm job left pending (D1)',
    tag: 'GKE',
    say: 'An honest known gap: a dead JobSet does not yet fail the corresponding Slurm job with a reason.',
    commands: [
      'kubectl get jobset -n slurm-jobs',
      'squeue -o "%i %T %r"',
    ],
  },
  {
    id: 15,
    title: 'Scale drill (optional add-on)',
    tag: 'GKE optional',
    say: 'What the queue looks like under real load — thousands of jobs, not two.',
    commands: [
      './experiments/07-scale/scripts/backlog-slurm.sh 500',
      './experiments/07-scale/scripts/backlog-slurm.sh 2500',
      './experiments/07-scale/scripts/backlog-k8s.sh 2000',
      'curl -s http://127.0.0.1:6060/debug/pprof/profile?seconds=25 -o cpu.pprof',
    ],
  },
  {
    id: 16,
    title: 'Reset between runs',
    tag: 'utility',
    say: 'Clear jobs/JobSets/deployments left over from a previous run without tearing down the cluster.',
    commands: [
      "kubectl -n slurm exec deploy/slurm-login-slinky -- bash -c 'squeue -h -o \"%i\" | xargs -r -n1 scancel'",
      'kubectl delete jobsets -n slurm-jobs --all --ignore-not-found',
      "kubectl delete jobs -n default -l 'kueue.x-k8s.io/queue-name' --ignore-not-found",
      'kubectl delete deployment queued-inference sample-inference -n default --ignore-not-found',
      'kubectl delete raycluster,rayservice -n default --all --ignore-not-found',
      'kubectl delete pod pinned-worker -n default --ignore-not-found',
    ],
  },
  {
    id: 17,
    title: 'Teardown + verify zero billing',
    tag: 'utility — not optional',
    say: 'Always run this at the end of a session. An idle cluster still bills for its nodes.',
    commands: [
      'cd experiments/01-gke-playground && ./scripts/99-teardown.sh',
      'gcloud compute instances list --format="table(name,zone,status)"',
      'gcloud compute disks list --format="table(name,zone,sizeGb)"',
      'gcloud container clusters list',
    ],
  },
];

// Bug found live this session (kind + ttyd smoke test): this file is loaded
// as a plain classic <script src="cheatsheet.js"> (see index.html), not a
// module. A top-level `const` in a classic script is scoped to that
// script's own top level — it does NOT become a `window` property the way a
// `var` or bare assignment would, so app.js's `window.DEMO_STEPS` read was
// always `undefined` and the cheat-sheet panel silently rendered zero steps
// since this tool's very first commit. Explicit assignment fixes it for the
// browser path; the module.exports branch below is unaffected (kept for any
// future Node-based consumer/test).
if (typeof window !== 'undefined') {
  window.DEMO_STEPS = DEMO_STEPS;
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { DEMO_STEPS };
}
