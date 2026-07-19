#!/usr/bin/env bash
# Entrypoint override for the slurmrestd container in the real-Slurm kind e2e
# (test/e2e/run-real-slurm.sh). Same /bootstrap contract as
# slurmctld-init.sh: slurm.conf + slurm.key + jwt_hs256.key mounted read-only.
#
# slurmrestd refuses to run as root AND as SlurmUser (both are deliberate
# upstream safety checks), so this script creates a dedicated `restd` user,
# gives it its own copies of the keys, and drops privileges with setpriv
# before exec'ing the daemon:
#   - slurm.key: slurmrestd authenticates to slurmctld over auth/slurm like
#     any other Slurm component, so it needs the cluster key;
#   - jwt_hs256.key: rest_auth/jwt validates the caller's token locally via
#     the auth/jwt plugin, which reads the key configured in slurm.conf's
#     AuthAltParameters.
set -euo pipefail

getent passwd restd >/dev/null 2>&1 || useradd --system --shell /usr/sbin/nologin restd

# `bridge`: the REST caller identity (see slurmctld-init.sh). rest_auth/jwt
# resolves the token's username against THIS container's passwd, so the user
# must exist here too, with the same uid as on the slurmctld side.
getent passwd bridge >/dev/null 2>&1 || useradd --uid 1500 --create-home --shell /usr/sbin/nologin bridge

# `slurm` (SlurmUser): the ADMIN REST identity the S1 probe prefers — node
# record deletion (S7) is an administrative operation that a plain user's
# token gets a 422 "Invalid user id" for (run-16 finding, and the very same
# error signature as the 2026-07-06 live-deploy finding). The Slinky image
# should ship this user; create defensively like slurmctld-init.sh does.
getent passwd slurm >/dev/null 2>&1 || useradd --system --shell /usr/sbin/nologin slurm

mkdir -p /etc/slurm
install -m 0644 /bootstrap/slurm.conf /etc/slurm/slurm.conf
install -m 0600 -o restd -g restd /bootstrap/slurm.key /etc/slurm/slurm.key
install -m 0600 -o restd -g restd /bootstrap/jwt_hs256.key /etc/slurm/jwt_hs256.key

# disable_unshare_sysv/disable_unshare_files: slurmrestd's default hardening
# unshares SysV IPC and the file-descriptor table, which requires privileges
# a default (non-privileged) container does not grant. Disabling ONLY the
# unshare calls keeps the rest of slurmrestd's hardening (user checks, no
# root) intact — the documented container-deployment setting.
export SLURMRESTD_SECURITY=disable_unshare_sysv,disable_unshare_files

# SLURM_JWT=daemon switches slurmrestd into JWT mode (rest_quickstart.html:
# "export SLURM_JWT=daemon" before launching). Run-2 finding (2026-07-11):
# without this, every presented token — any user, with or without
# X-SLURM-USER-NAME — is rejected with "Rejecting thread config token";
# `-a rest_auth/jwt` alone is NOT sufficient. The literal value "daemon" is
# the documented sentinel meaning "manage tokens as a daemon", not a token.
export SLURM_JWT=daemon

# -a rest_auth/jwt: REST callers authenticate with X-SLURM-USER-TOKEN (and
# optionally X-SLURM-USER-NAME), exactly the headers internal/slurm/client.go
# sends. Listen on all interfaces: the bridge reaches this container from
# inside kind via a selectorless Service + EndpointSlice pointing at this
# container's docker-network IP.
exec setpriv --reuid=restd --regid=restd --clear-groups \
  slurmrestd -a rest_auth/jwt 0.0.0.0:6820
