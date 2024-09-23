{{/*
Copyright 2024 The Kubeflow authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/}}

{{/*
Create the name of spark component
*/}}
{{- define "spark-operator.spark.name" -}}
{{- include "spark-operator.fullname" . }}-spark
{{- end -}}

{{/*
Create the name of the service account to be used by spark applications
*/}}
{{- define "spark-operator.spark.serviceAccountName" -}}
{{- if .Values.spark.serviceAccount.create -}}
{{- .Values.spark.serviceAccount.name | default (include "spark-operator.spark.name" .) -}}
{{- else -}}
{{- .Values.spark.serviceAccount.name | default "default" -}}
{{- end -}}
{{- end -}}

{{/*
Create the name of the role to be used by spark service account
*/}}
{{- define "spark-operator.spark.roleName" -}}
{{- include "spark-operator.spark.serviceAccountName" . }}
{{- end -}}

{{/*
Create the name of the role binding to be used by spark service account
*/}}
{{- define "spark-operator.spark.roleBindingName" -}}
{{- include "spark-operator.spark.serviceAccountName" . }}
{{- end -}}
