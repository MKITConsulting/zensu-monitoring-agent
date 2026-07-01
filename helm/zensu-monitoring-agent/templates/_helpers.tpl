{{- define "zensu-monitoring-agent.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "zensu-monitoring-agent.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "zensu-monitoring-agent.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "zensu-monitoring-agent.labels" -}}
app.kubernetes.io/name: {{ include "zensu-monitoring-agent.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: zensu
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "zensu-monitoring-agent.selectorLabels" -}}
app.kubernetes.io/name: {{ include "zensu-monitoring-agent.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "zensu-monitoring-agent.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "zensu-monitoring-agent.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "zensu-monitoring-agent.secretName" -}}
{{- if .Values.zensu.existingSecret -}}
{{- .Values.zensu.existingSecret -}}
{{- else -}}
{{- include "zensu-monitoring-agent.fullname" . -}}
{{- end -}}
{{- end -}}

{{- define "zensu-monitoring-agent.namespaces" -}}
{{- if .Values.agent.namespaces -}}
{{- join "," .Values.agent.namespaces -}}
{{- else -}}
default
{{- end -}}
{{- end -}}
