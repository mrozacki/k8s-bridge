{{/*
Chart name, optionally overridden.
*/}}
{{- define "ray-bridge.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{/*
Fully qualified, release-aware name.
*/}}
{{- define "ray-bridge.fullname" -}}
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
Standard labels applied to every resource.
*/}}
{{- define "ray-bridge.labels" -}}
app.kubernetes.io/name: {{ include "ray-bridge.name" . }}
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
{{- define "ray-bridge.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ray-bridge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Image reference: digest wins over tag when set; tag defaults to appVersion.
*/}}
{{- define "ray-bridge.image" -}}
{{- $repo := .Values.image.repository -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" $repo .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" $repo (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}
{{- end }}

{{/*
Name of the webhook Service (port 9443/443) — shared by templates/deployment.yaml
(cert volume mount), templates/webhook-service.yaml and
templates/mutatingwebhookconfiguration.yaml so they never drift apart.
*/}}
{{- define "ray-bridge.webhookServiceName" -}}
{{- printf "%s-webhook" (include "ray-bridge.fullname" .) -}}
{{- end }}

{{/*
Name of the Secret holding the webhook's serving certificate (tls.crt/tls.key),
whether populated by cert-manager's Certificate resource (webhook.certManager.enabled,
the default) or supplied manually when it's disabled. controller-runtime's
webhook server expects exactly these two keys in its cert dir, which is why
this is also where templates/certificate.yaml points its secretName.
*/}}
{{- define "ray-bridge.webhookCertSecretName" -}}
{{- printf "%s-webhook-tls" (include "ray-bridge.fullname" .) -}}
{{- end }}

{{/*
Name of the self-signed Issuer backing the webhook's Certificate.
*/}}
{{- define "ray-bridge.webhookIssuerName" -}}
{{- printf "%s-selfsigned-issuer" (include "ray-bridge.fullname" .) -}}
{{- end }}

{{/*
Name of the webhook's Certificate object (cert-manager.io/v1).
*/}}
{{- define "ray-bridge.webhookCertificateName" -}}
{{- printf "%s-serving-cert" (include "ray-bridge.fullname" .) -}}
{{- end }}
