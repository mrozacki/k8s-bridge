#!/usr/bin/env bash
# Entrypoint override for the slurmctld container in the real-Slurm kind e2e
# (test/e2e/run-real-slurm.sh). The Slinky slurmctld image's own entrypoint
# is built for the slurm-operator's in-cluster wiring (init containers,
# operator-rendered config); overriding it with this small script keeps the
# standalone docker-network deployment fully under this test's control and
# removes any dependency on undocumented entrypoint behavior.
#
# Contract with the runner script: a directory is bind-mounted read-only at
# /bootstrap containing slurm.conf, slurm.key and jwt_hs256.key (both keys
# generated per run with openssl rand). This script installs them with the
# ownership slurmctld needs and execs slurmctld in the foreground so its log
# is `docker logs slurmctld`.
set -euo pipefail

# The Slinky images are expected to ship a `slurm` system user (their
# daemons run as it); create one defensively if this image build does not.
# NEEDS-RUNNER-VALIDATION: user presence and its uid in the 26.05 images.
getent passwd slurm >/dev/null 2>&1 || useradd --system --shell /usr/sbin/nologin slurm

# `bridge`: the REST identity the e2e mints a JWT for and submits jobs as
# (run-1 finding: slurmrestd rejects a root token presented without a user
# name — "Rejecting thread config token for user (null)" — so the test
# probes identities and prefers this dedicated non-root user). uid pinned so
# the slurmrestd container's copy of the user resolves identically.
getent passwd bridge >/dev/null 2>&1 || useradd --uid 1500 --create-home --shell /usr/sbin/nologin bridge

mkdir -p /etc/slurm /var/spool/slurmctld
chown slurm:slurm /var/spool/slurmctld

install -m 0644 /bootstrap/slurm.conf /etc/slurm/slurm.conf
# cgroup.conf (CgroupPlugin=disabled) rides configless to every slurmd —
# without it slurmd autodetects a cgroup plugin and demands a dbus/systemd
# scope that no container has (run-8 finding; see the file's own header).
install -m 0644 /bootstrap/cgroup.conf /etc/slurm/cgroup.conf
# slurmctld is started as root and drops to SlurmUser=slurm, so both keys
# must be readable by that user; 0600 keeps them private to it (root can
# always read them for `docker exec ... scontrol token` / sbatch).
install -m 0600 -o slurm -g slurm /bootstrap/slurm.key /etc/slurm/slurm.key
install -m 0600 -o slurm -g slurm /bootstrap/jwt_hs256.key /etc/slurm/jwt_hs256.key

# -D: foreground, log to stderr — docker logs is the diagnostic surface the
# workflow uploads on failure.
exec slurmctld -D
