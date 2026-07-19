#!/bin/bash
set -ex
export EVID="$HOME/k8s-bridge-testrun/TC-E6"
mkdir -p "$EVID"

kubectl apply -f experiments/08-ccc-dws/manifests/compute-classes.yaml

# Apply a large deployment requesting 'econo'
cat << 'DEPLOY' | kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: starver
  namespace: default
spec:
  replicas: 1000
  selector:
    matchLabels:
      app: starver
  template:
    metadata:
      labels:
        app: starver
    spec:
      nodeSelector:
        cloud.google.com/compute-class: econo
      containers:
        - name: w
          image: busybox:1.36
          command: ["sh", "-c", "sleep 3600"]
          resources:
            requests:
              cpu: "1"
              memory: 1Gi
DEPLOY

# Wait 5-10 minutes for NAP to provision spot nodes, fail, and fallback to on-demand.
sleep 300
for i in {1..5}; do
  kubectl get nodes -l cloud.google.com/compute-class=econo --show-labels | tee -a "$EVID/nodes.txt"
  sleep 60
done

# Teardown
kubectl delete deployment starver || true
kubectl delete -f experiments/08-ccc-dws/manifests/compute-classes.yaml || true
