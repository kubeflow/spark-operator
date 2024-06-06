{{/*
Create the name of the secret to be used by webhook
*/}}
{{- define "spark-operator.webhookSecretName" -}}
{{ include "spark-operator.fullname" . }}-webhook-certs
{{- end -}}


{{/*
Create the name of the service to be used by webhook
*/}}
{{- define "spark-operator.webhookServiceName" -}}
{{ include "spark-operator.fullname" . }}-webhook-svc
{{- end -}}
