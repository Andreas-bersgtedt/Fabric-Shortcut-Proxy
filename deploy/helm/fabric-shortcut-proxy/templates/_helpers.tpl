{{- define "fsp.namespace" -}}
{{- .Values.namespace.name -}}
{{- end -}}

{{- define "fsp.commonLabels" -}}
app.kubernetes.io/part-of: fabric-shortcut-proxy
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "fsp.image" -}}
{{- $repository := required "image repository is required" .repository -}}
{{- if .digest -}}
{{- printf "%s@%s" $repository .digest -}}
{{- else -}}
{{- printf "%s:%s" $repository (required "image tag is required when digest is empty" .tag) -}}
{{- end -}}
{{- end -}}

{{- define "fsp.imagePullSecrets" -}}
{{- if and .Values.images.pullSecrets (not .Values.workloadIdentity.enabled) }}
imagePullSecrets:
{{- range .Values.images.pullSecrets }}
  - name: {{ . | quote }}
{{- end }}
{{- end }}
{{- end -}}

{{- define "fsp.serviceAccountName" -}}
{{- if .Values.workloadIdentity.enabled -}}
{{- .Values.workloadIdentity.serviceAccountName -}}
{{- else -}}
default
{{- end -}}
{{- end -}}

{{- define "fsp.validateNginxTls" -}}
{{- if ne .Values.nginx.enabled .Values.tls.enabled -}}
{{- fail "nginx.enabled and tls.enabled must be enabled or disabled together" -}}
{{- end -}}
{{- end -}}
