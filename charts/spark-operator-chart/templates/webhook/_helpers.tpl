{{/*
Create the name of service account to be used by webhook
*/}}
{{- define "spark-operator.webhookServiceAccountName" -}}
{{ default (printf "%s-webhook" (include "spark-operator.fullname" .)) .Values.serviceAccounts.webhook.name }}
{{- end -}}

{{/*
Create the name of the secret to be used by webhook
*/}}
{{- define "spark-operator.webhookSecretName" -}}
{{- include "spark-operator.fullname" . }}-webhook-tls
{{- end -}}

{{/*
Create the name of the service to be used by webhook
*/}}
{{- define "spark-operator.webhookServiceName" -}}
{{- include "spark-operator.fullname" . }}-webhook-svc
{{- end -}}

{{/*
Create the name of webhook
*/}}
{{- define "spark-operator.webhookName" -}}
webhook.sparkoperator.k8s.io
{{- end -}}

{{/*
Create the name of the cert-manager issuer
*/}}
{{- define "cert-manager.issuerName" -}}
{{ include "spark-operator.fullname" . }}-issuer
{{- end -}}

{{/*
Create the name of the cert-manager certificate
*/}}
{{- define "cert-manager.certificateName" -}}
{{ include "spark-operator.fullname" . }}-cert
{{- end -}}
