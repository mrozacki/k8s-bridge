#!/usr/bin/env bash
# Creates N suspended Kueue Jobs spread across queues and priorities -
# the "diverse backlog" half of the 5000-job stress. Applied in chunks of
# 200 multi-doc manifests to keep apiserver requests reasonable.
set -euo pipefail
N="${1:?usage: backlog-k8s.sh <count>}"
[[ "$N" =~ ^[0-9]+$ ]] || { echo "count must be a non-negative integer, got: $N" >&2; exit 2; }
QUEUES=(team-a team-b)
PRIOS=(normal-priority high-priority)
i=0
while [ $i -lt "$N" ]; do
  chunk=""
  for _ in $(seq 1 200); do
    [ $i -ge "$N" ] && break
    q=${QUEUES[$((i % 2))]}
    p=${PRIOS[$((i % 3 == 0 ? 1 : 0))]}
    chunk+="---
apiVersion: batch/v1
kind: Job
metadata:
  name: backlog-${i}
  namespace: default
  labels:
    kueue.x-k8s.io/queue-name: ${q}
    kueue.x-k8s.io/priority-class: ${p}
    scale-test: backlog
spec:
  parallelism: 1
  completions: 1
  suspend: true
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: w
          image: busybox:1.36
          command: [\"sh\", \"-c\", \"sleep 30\"]
          resources:
            requests: { cpu: \"1\", memory: 256Mi }
            limits: { cpu: \"1\", memory: 256Mi }
"
    i=$((i+1))
  done
  echo "$chunk" | kubectl create -f - > /dev/null
  echo "created $i/$N"
done
