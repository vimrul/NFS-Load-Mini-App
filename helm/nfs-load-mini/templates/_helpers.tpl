{{- define "nfs-load-mini.name" -}}
nfs-load-mini
{{- end -}}

{{- define "nfs-load-mini.fullname" -}}
{{ include "nfs-load-mini.name" . }}-{{ .Release.Name }}
{{- end -}}

{{- define "nfs-load-mini.labels" -}}
app.kubernetes.io/name: {{ include "nfs-load-mini.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}