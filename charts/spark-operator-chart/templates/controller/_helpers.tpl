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
Create the name of the cluster role to be used by the controller
*/}}
{{- define "spark-operator.controller.clusterRoleName" -}}
{{ include "spark-operator.controller.name" . }}
{{- end }}

{{/*
Create the name of the cluster role binding to be used by the controller
*/}}
{{- define "spark-operator.controller.clusterRoleBindingName" -}}
{{ include "spark-operator.controller.clusterRoleName" . }}
{{- end }}

{{/*
Create the name of the role to be used by the controller
*/}}
{{- define "spark-operator.controller.roleName" -}}
{{ include "spark-operator.controller.name" . }}
{{- end }}

{{/*
Create the name of the role binding to be used by the controller
*/}}
{{- define "spark-operator.controller.roleBindingName" -}}
{{ include "spark-operator.controller.roleName" . }}
{{- end }}

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

{{/*
Create the name of the service used by controller
*/}}
{{- define "spark-operator.controller.serviceName" -}}
{{ include "spark-operator.controller.name" . }}-svc
{{- end -}}

{{/*
Create the role policy rules for the controller in every Spark job namespace
*/}}
{{- define "spark-operator.controller.policyRules" -}}
- apiGroups:
  - ""
  resources:
  - pods
  verbs:
  - get
  - list
  - watch
  - create
  - update
  - patch
  - delete
  - deletecollection
- apiGroups:
  - ""
  resources:
  - configmaps
  verbs:
  - get
  - list
  - watch
  - create
  - update
  - patch
  - delete
- apiGroups:
  - ""
  resources:
  - persistentvolumeclaims
  verbs:
  - get
  - list
  - watch
  - create
  - update
  - patch
  - delete
- apiGroups:
  - ""
  resources:
  - services
  verbs:
  - get
  - list
  - watch
  - create
  - update
  - patch
  - delete
- apiGroups:
  - ""
  resources:
  - events
  verbs:
  - create
  - update
  - patch
- apiGroups:
  - extensions
  - networking.k8s.io
  resources:
  - ingresses
  verbs:
  - get
  - list
  - watch
  - create
  - update
  - delete
- apiGroups:
  - sparkoperator.k8s.io
  resources:
  - sparkapplications
  - scheduledsparkapplications
  - sparkconnects
  verbs:
  - get
  - list
  - watch
  - create
  - update
  - patch
  - delete
- apiGroups:
  - sparkoperator.k8s.io
  resources:
  - sparkapplications/status
  - sparkapplications/finalizers
  - scheduledsparkapplications/status
  - scheduledsparkapplications/finalizers
  - sparkconnects/status
  - sparkconnects/finalizers
  verbs:
  - get
  - update
  - patch
{{- if .Values.controller.batchScheduler.enable }}
{{/* required for the `volcano` batch scheduler */}}
- apiGroups:
  - scheduling.incubator.k8s.io
  - scheduling.sigs.dev
  - scheduling.volcano.sh
  resources:
  - podgroups
  verbs:
  - "*"
{{- end }}
{{- end -}}
