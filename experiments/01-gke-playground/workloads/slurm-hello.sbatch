#!/bin/bash
#SBATCH --job-name=hello-mixing
#SBATCH --ntasks=2
#SBATCH --cpus-per-task=1
#SBATCH --output=/tmp/hello-%j.out

# Minimal Slurm job for the playground. In phase 1 it runs on the static
# nodeset created by the slurm chart; in phase 2/3 the same script exercises
# the bridge path (submitted with --hold to a mixing partition).
srun bash -c 'echo "task ${SLURM_PROCID} on $(hostname)"; sleep 60'
