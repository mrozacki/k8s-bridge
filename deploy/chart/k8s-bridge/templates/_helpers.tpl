{{/*
Chart name, optionally overridden.
*/}}
{{- define "k8s-bridge.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{/*
Fully qualified, release-aware name. Cluster-scoped resources (ClusterRole,
ClusterRoleBinding) use this so two installs on one cluster do not collide.
*/}}
{{- define "k8s-bridge.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
Standard labels applied to every resource (used by policy engines and kubectl).
*/}}
{{- define "k8s-bridge.labels" -}}
app.kubernetes.io/name: {{ include "k8s-bridge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
{{- end }}

{{/*
Selector labels — the stable subset used by the Deployment selector.
*/}}
{{- define "k8s-bridge.selectorLabels" -}}
app.kubernetes.io/name: {{ include "k8s-bridge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Image reference: digest wins over tag when set; tag defaults to appVersion.
*/}}
{{- define "k8s-bridge.image" -}}
{{- $repo := .Values.image.repository -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" $repo .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" $repo (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}
{{- end }}

{{/*
PriorityClass name (backlog P6): defaults to the chart's fullname when
priorityClass.name is empty, so it doesn't collide across releases the same
way every other release-scoped resource in this chart doesn't.
*/}}
{{- define "k8s-bridge.priorityClassName" -}}
{{- default (include "k8s-bridge.fullname" .) .Values.priorityClass.name -}}
{{- end }}

{{/*
Name of the metrics Service — shared by templates/service.yaml and
templates/servicemonitor.yaml so the two never drift apart.
*/}}
{{- define "k8s-bridge.metricsServiceName" -}}
{{- printf "%s-metrics" (include "k8s-bridge.fullname" .) -}}
{{- end }}
