{{/*
Helper templates — shared name/label helpers for the chart.
Purpose: keep resource names consistent and within Kubernetes DNS limits.
*/}}

{{- define "cache-custom.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "cache-custom.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "cache-custom.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "cache-custom.labels" -}}
helm.sh/chart: {{ include "cache-custom.chart" . }}
{{ include "cache-custom.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "cache-custom.selectorLabels" -}}
app.kubernetes.io/name: {{ include "cache-custom.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
