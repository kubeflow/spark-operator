{{- /*
Copyright 2025 The Kubeflow authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/ -}}

{{- /* Create the name of Helm hook */ -}}
{{- define "spark-operator.hook.name" -}}
{{- include "spark-operator.fullname" . }}-hook
{{- end -}}

{{- /* Common labels for the Helm hook */ -}}
{{- define "spark-operator.hook.labels" -}}
{{ include "spark-operator.labels" . }}
app.kubernetes.io/component: hook
{{- end -}}

{{- /* Selector labels for the Helm hook */ -}}
{{- define "spark-operator.hook.selectorLabels" -}}
{{ include "spark-operator.hook.labels" . }}
{{- end -}}

{{- /* Create the name of the service account to be used by the Helm hooks. */ -}}
{{- define "spark-operator.hook.serviceAccount.name" -}}
{{ include "spark-operator.hook.name" . }}
{{- end -}}

{{- /* Create the name of the cluster role to be used by the Helm hooks. */ -}}
{{- define "spark-operator.hook.clusterRole.name" -}}
{{ include "spark-operator.hook.name" . }}
{{- end -}}

{{- /* Create the name of the cluster role binding to be used by the Helm hooks. */ -}}
{{- define "spark-operator.hook.clusterRoleBinding.name" -}}
{{ include "spark-operator.hook.clusterRole.name" . }}
{{- end -}}

{{- /* Create the name of the Helm hook job. */ -}}
{{- define "spark-operator.hook.job.name" -}}
{{ include "spark-operator.hook.name" . }}
{{- end -}}

{{- /* Create the name of the Helm hook job. */ -}}
{{- define "spark-operator.hook.image" -}}
{{ printf "%s/%s:%s" .Values.hook.image.registry .Values.hook.image.repository (.Values.hook.image.tag | default .Chart.AppVersion | toString) }}
{{- end -}}
