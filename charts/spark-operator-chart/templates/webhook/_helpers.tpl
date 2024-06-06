{{/*
Create the name of webhook configuration
*/}}
{{- define "spark-operator.webhookName" -}}
{{- include "spark-operator.fullname" . }}-webhook
{{- end -}}

{{/*
Create the name of mutating webhook configuration
*/}}
{{- define "spark-operator.mutatingWebhookConfigurationName" -}}
webhook.sparkoperator.k8s.io
{{- end -}}

{{/*
Create the name of mutating webhook configuration
*/}}
{{- define "spark-operator.validatingWebhookConfigurationName" -}}
quotaenforcer.sparkoperator.k8s.io
{{- end -}}

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
