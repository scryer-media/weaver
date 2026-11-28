{{/* The chart name, cut to the 63 characters a label allows. */}}
{{- define "weaver.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* The name every object of the release shares. */}}
{{- define "weaver.fullname" -}}
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

{{- define "weaver.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "weaver.selectorLabels" -}}
app.kubernetes.io/name: {{ include "weaver.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "weaver.labels" -}}
helm.sh/chart: {{ include "weaver.chart" . }}
{{ include "weaver.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/* metadata.annotations for an object, merged with commonAnnotations. */}}
{{- define "weaver.annotations" -}}
{{- $annotations := merge (deepCopy (default dict .extra)) (deepCopy (default dict .root.Values.commonAnnotations)) }}
{{- with $annotations }}
annotations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end }}

{{- define "weaver.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "weaver.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "weaver.image" -}}
{{- if .Values.image.digest }}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- else }}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) }}
{{- end }}
{{- end }}

{{/* The base path as Weaver normalises it: one leading slash, none trailing, "" for the root. */}}
{{- define "weaver.basePath" -}}
{{- $path := trimAll "/" .Values.basePath }}
{{- if $path }}{{ printf "/%s" $path }}{{ end }}
{{- end }}

{{/* The path a request for the UI root takes, for Ingress and HTTPRoute. */}}
{{- define "weaver.routePath" -}}
{{- default "/" (include "weaver.basePath" .) }}
{{- end }}

{{/* WEAVER_HTTP_ALLOWED_HOSTS: the Service's names, every route host and the extra hosts. */}}
{{- define "weaver.allowedHosts" -}}
{{- $fullname := include "weaver.fullname" . }}
{{- $ns := .Release.Namespace }}
{{- $hosts := list $fullname (printf "%s.%s" $fullname $ns) (printf "%s.%s.svc" $fullname $ns) (printf "%s.%s.svc.%s" $fullname $ns .Values.clusterDomain) }}
{{- if .Values.ingress.enabled }}
{{- $hosts = concat $hosts .Values.ingress.hosts }}
{{- end }}
{{- if .Values.httpRoute.enabled }}
{{- $hosts = concat $hosts .Values.httpRoute.hostnames }}
{{- end }}
{{- $hosts = concat $hosts .Values.extraAllowedHosts }}
{{- $hosts | uniq | join "," }}
{{- end }}

{{/* A PersistentVolumeClaim's storageClassName, by the usual "-" convention. */}}
{{- define "weaver.storageClass" -}}
{{- if eq . "-" }}
storageClassName: ""
{{- else if . }}
storageClassName: {{ . | quote }}
{{- end }}
{{- end }}

{{/* The restricted container securityContext, shared by Weaver and the test pod. */}}
{{- define "weaver.containerSecurityContext" -}}
runAsNonRoot: true
runAsUser: {{ .Values.securityContext.runAsUser }}
runAsGroup: {{ .Values.securityContext.runAsGroup }}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop:
    - ALL
seccompProfile:
  type: RuntimeDefault
{{- end }}
