# tools/

Small operational scripts for running and presenting the k8s-bridge
playground.

| Path | Purpose |
|---|---|
| `bridge-top.sh` | Terminal dashboard: cluster nodes, Kueue queues/workloads, bridge JobSets, topology, Slurm queue/nodes. Refreshes in place; run alongside the commands in `experiments/DEMO.md`. |
| `demo-console/` | Single-screen presenter console: an embedded, typeable web terminal (ttyd) next to live dashboards/state and a click-to-copy cheat sheet of `experiments/DEMO.md`. Use this instead of `bridge-top.sh` + a separate terminal window when presenting to an audience. See `demo-console/README.md`. |

Both tools are read-mostly against a running cluster (`kubectl` context) —
neither creates or tears down infrastructure; see
`experiments/01-gke-playground/scripts/` for that.
