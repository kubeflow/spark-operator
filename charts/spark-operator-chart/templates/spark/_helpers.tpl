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
Validate jobNamespaceSelector. String form removed in v2.6.0.
*/}}
{{- define "spark-operator.spark.validateNamespaceConfig" -}}
{{- $sel := .Values.spark.jobNamespaceSelector -}}
{{- if and $sel (kindIs "string" $sel) -}}
{{- fail "spark.jobNamespaceSelector no longer accepts a string; use an object with matchLabels/matchExpressions (breaking change in v2.6.0)" -}}
{{- end -}}
{{- if and $sel (not (kindIs "map" $sel)) -}}
{{- fail "spark.jobNamespaceSelector must be an object with matchLabels and/or matchExpressions" -}}
{{- end -}}
{{- if and (kindIs "map" $sel) $sel (not $sel.matchLabels) (not $sel.matchExpressions) -}}
{{- fail "spark.jobNamespaceSelector must set matchLabels or matchExpressions" -}}
{{- end -}}
{{- if and (kindIs "map" $sel) $sel -}}
{{- range $k, $v := ($sel.matchLabels | default dict) -}}
{{- if kindIs "invalid" $v -}}
{{- fail (printf "spark.jobNamespaceSelector.matchLabels.%s must not be null" $k) -}}
{{- end -}}
{{- end -}}
{{- range $expr := ($sel.matchExpressions | default list) -}}
{{- $op := $expr.operator | default "" -}}
{{- if not (or (eq $op "In") (eq $op "NotIn") (eq $op "Exists") (eq $op "DoesNotExist")) -}}
{{- fail (printf "unsupported spark.jobNamespaceSelector operator %q" $op) -}}
{{- end -}}
{{- range $v := ($expr.values | default list) -}}
{{- if kindIs "invalid" $v -}}
{{- fail (printf "spark.jobNamespaceSelector.matchExpressions values for key %s must not be null" $expr.key) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Render jobNamespaceSelector as a --namespace-selector flag string.
*/}}
{{- define "spark-operator.spark.namespaceSelectorFlag" -}}
{{- $sel := .Values.spark.jobNamespaceSelector -}}
{{- if and (kindIs "map" $sel) $sel -}}
{{- $parts := list -}}
{{- $labels := $sel.matchLabels | default dict -}}
{{- range $k := keys $labels | sortAlpha -}}
{{- $parts = append $parts (printf "%s=%v" $k (index $labels $k)) -}}
{{- end -}}
{{- range $expr := ($sel.matchExpressions | default list) -}}
{{- $op := $expr.operator | default "" -}}
{{- if eq $op "In" -}}
{{- $parts = append $parts (printf "%s in (%s)" $expr.key (join "," ($expr.values | default list))) -}}
{{- else if eq $op "NotIn" -}}
{{- $parts = append $parts (printf "%s notin (%s)" $expr.key (join "," ($expr.values | default list))) -}}
{{- else if eq $op "Exists" -}}
{{- $parts = append $parts ($expr.key | toString) -}}
{{- else if eq $op "DoesNotExist" -}}
{{- $parts = append $parts (printf "!%s" $expr.key) -}}
{{- end -}}
{{- end -}}
{{- join "," $parts -}}
{{- end -}}
{{- end -}}

{{/*
Render jobNamespaceSelector as YAML. Values are stringified for the API server.
*/}}
{{- define "spark-operator.spark.structuredJobNamespaceSelector" -}}
{{- $sel := .Values.spark.jobNamespaceSelector -}}
{{- if and (kindIs "map" $sel) $sel -}}
{{- $out := dict -}}
{{- if $sel.matchLabels -}}
{{- $labels := dict -}}
{{- range $k, $v := $sel.matchLabels -}}
{{- $_ := set $labels $k ($v | toString) -}}
{{- end -}}
{{- $_ := set $out "matchLabels" $labels -}}
{{- end -}}
{{- if $sel.matchExpressions -}}
{{- $exprs := list -}}
{{- range $expr := $sel.matchExpressions -}}
{{- $vals := list -}}
{{- range $v := ($expr.values | default list) -}}
{{- $vals = append $vals ($v | toString) -}}
{{- end -}}
{{- $e := dict "key" $expr.key "operator" $expr.operator -}}
{{- if $vals -}}
{{- $_ := set $e "values" $vals -}}
{{- end -}}
{{- $exprs = append $exprs $e -}}
{{- end -}}
{{- $_ := set $out "matchExpressions" $exprs -}}
{{- end -}}
{{- if $out -}}
{{- toYaml $out -}}
{{- end -}}
{{- end -}}
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
