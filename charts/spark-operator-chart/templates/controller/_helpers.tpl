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
Create the name of controller component
*/}}
{{- define "spark-operator.controller.name" -}}
{{- include "spark-operator.fullname" . }}-controller
{{- end -}}

{{/*
Common labels for the controller
*/}}
{{- define "spark-operator.controller.labels" -}}
{{ include "spark-operator.labels" . }}
app.kubernetes.io/component: controller
{{- end -}}

{{/*
Selector labels for the controller
*/}}
{{- define "spark-operator.controller.selectorLabels" -}}
{{ include "spark-operator.selectorLabels" . }}
app.kubernetes.io/component: controller
{{- end -}}

{{/*
Create the name of the service account to be used by the controller
*/}}
{{- define "spark-operator.controller.serviceAccountName" -}}
{{- if .Values.controller.serviceAccount.create -}}
{{ .Values.controller.serviceAccount.name | default (include "spark-operator.controller.name" .) }}
{{- else -}}
{{ .Values.controller.serviceAccount.name | default "default" }}
{{- end -}}
{{- end -}}

{{/*
Create the name of the deployment to be used by controller
*/}}
{{- define "spark-operator.controller.deploymentName" -}}
{{ include "spark-operator.controller.name" . }}
{{- end -}}

{{/*
Create the name of the lease resource to be used by leader election
*/}}
{{- define "spark-operator.controller.leaderElectionName" -}}
{{ include "spark-operator.controller.name" . }}-lock
{{- end -}}

{{/*
Create the name of the pod disruption budget to be used by controller
*/}}
{{- define "spark-operator.controller.podDisruptionBudgetName" -}}
{{ include "spark-operator.controller.name" . }}-pdb
{{- end -}}
