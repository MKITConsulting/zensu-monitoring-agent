{{- define "zensu-monitoring-agent.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "zensu-monitoring-agent.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "zensu-monitoring-agent.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "zensu-monitoring-agent.labels" -}}
app.kubernetes.io/name: {{ include "zensu-monitoring-agent.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: zensu
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "zensu-monitoring-agent.selectorLabels" -}}
app.kubernetes.io/name: {{ include "zensu-monitoring-agent.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "zensu-monitoring-agent.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "zensu-monitoring-agent.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "zensu-monitoring-agent.secretName" -}}
{{- if .Values.zensu.existingSecret -}}
{{- .Values.zensu.existingSecret -}}
{{- else -}}
{{- include "zensu-monitoring-agent.fullname" . -}}
{{- end -}}
{{- end -}}

{{- define "zensu-monitoring-agent.namespaces" -}}
{{- if .Values.agent.namespaces -}}
{{- join "," .Values.agent.namespaces -}}
{{- else -}}
default
{{- end -}}
{{- end -}}

{{/*
Every value invariant lives here. deployment.yaml and cronjob.yaml include it
BEFORE their own mode guard, so Helm evaluates it on every render whichever mode
is selected — one reachable include is all the invariants need, and a template
added later without the include loses nothing. Putting a check inside the
resource it guards is what made the NetworkPolicy rules unreachable for two real
configurations. Emitters below stay pure.
*/}}
{{- define "zensu-monitoring-agent.validateValues" -}}
{{- if not (has .Values.agent.mode (list "deployment" "cronjob")) }}
{{- fail (printf "agent.mode %q is neither deployment nor cronjob; the release would install with no workload at all and report success" (toString .Values.agent.mode)) }}
{{- end }}
{{- with (include "zensu-monitoring-agent.scrapeMaxBytes" .) }}
{{- if not (regexMatch "^[0-9]+$" .) }}
{{- fail (printf "resourceMetrics.scrapeMaxBytes %q is not a plain byte count; the agent parses it with ParseInt and would silently fall back to its own default" .) }}
{{- end }}
{{- end }}
{{- with (include "zensu-monitoring-agent.goMemLimit" .) }}
{{- if not (regexMatch "^[0-9]*[1-9][0-9]*(B|KiB|MiB|GiB|TiB)?$" .) }}
{{- fail (printf "agent.goMemLimit %q is not a Go memory limit: the runtime parses it, not Kubernetes, so use a plain byte count or a B/KiB/MiB/GiB/TiB suffix (e.g. 56MiB), not a Kubernetes quantity, and not zero" .) }}
{{- end }}
{{- end }}
{{/*
The URL rules are enforced HERE as well as at startup, and the render-time copy
is the one that matters: configmap.yaml writes both URLs in clear text, so a
credential caught only by the binary has already been applied to the cluster and
stored in the Helm release by the time the pod refuses it. Failing the render
keeps it out of etcd. The scrape URL is checked whatever resourceMetrics.source
says, because the ConfigMap key is written in every mode.
*/}}
{{- range $name, $url := dict "zensu.apiUrl" .Values.zensu.apiUrl "resourceMetrics.scrapeUrl" .Values.resourceMetrics.scrapeUrl }}
{{- if $url }}
{{- if regexMatch "^[a-zA-Z][a-zA-Z0-9+.-]*://[^/?#]*@" (toString $url) }}
{{- fail (printf "%s carries credentials in its userinfo, which are unsupported and would be stored in a ConfigMap in clear text; put the credential behind a proxy or a Secret-backed header instead" $name) }}
{{- end }}
{{- if not (regexMatch "^https?://[^/?#]+" (toString $url)) }}
{{- fail (printf "%s needs an http:// or https:// scheme and a host" $name) }}
{{- end }}
{{- end }}
{{- end }}
{{- if .Values.metrics.networkPolicy.enabled }}
{{- if not .Values.metrics.networkPolicy.from }}
{{- fail "metrics.networkPolicy.from must name the allowed sources; an empty list allows every source, which is not the hardening the flag implies" }}
{{- end }}
{{- if not .Values.metrics.enabled }}
{{- fail "metrics.networkPolicy.enabled needs metrics.enabled: with the endpoint off there is nothing to restrict, and the policy would silently not be created" }}
{{- end }}
{{- if ne .Values.agent.mode "deployment" }}
{{- fail "metrics.networkPolicy.enabled needs agent.mode=deployment: a cronjob serves no metrics endpoint, and the policy would silently not be created" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
goMemLimit renders the configured value as a string, or the empty string when it
is unset. It is the ONE predicate for "is this set": every other site tests this
helper's output, so the validator and the emitter cannot admit different sets.

Neither `with .Values.agent.goMemLimit` nor `ne (toString .) ""` works alone.
Go template truthiness treats a numeric 0 as empty, so `with` would skip an
operator's --set agent.goMemLimit=0 and disable the ceiling silently — the one
value the validator promises to refuse. And sprig's toString renders an absent
or null value as "<nil>", not "", so the string test alone would fail the render
for a value the operator never set. Hence the kind check first.

A bare byte count is legal for the Go runtime, and YAML parses one in a values
FILE as a float, which toString renders in scientific notation — so a float is
formatted back to a plain integer rather than passed through.
*/}}
{{/*
scrapeMaxBytes has the same float hazard as goMemLimit and needs the same
treatment: YAML parses a byte count in a values FILE as a float, and sprig's
quote and toString share the %v path, which switches to scientific notation past
a million. "4.194304e+06" then fails the binary's ParseInt, envInt64 returns its
zero default, and the operator's lowered cap is silently replaced by the built-in
8 MiB one — no error, no log line. Ports and intervals need no helper: a port
cannot reach the threshold, and the interval is rendered through printf "%ds".
*/}}
{{- define "zensu-monitoring-agent.scrapeMaxBytes" -}}
{{- $v := .Values.resourceMetrics.scrapeMaxBytes -}}
{{- if kindIs "invalid" $v -}}
{{- else if kindIs "float64" $v -}}{{ printf "%.0f" $v }}
{{- else -}}{{ toString $v }}
{{- end -}}
{{- end }}

{{- define "zensu-monitoring-agent.goMemLimit" -}}
{{- $v := .Values.agent.goMemLimit -}}
{{- if kindIs "invalid" $v -}}
{{- else if kindIs "float64" $v -}}{{ printf "%.0f" $v }}
{{- else -}}{{ toString $v }}
{{- end -}}
{{- end }}

{{- define "zensu-monitoring-agent.goMemLimitEnv" -}}
{{- with (include "zensu-monitoring-agent.goMemLimit" $) }}
- name: GOMEMLIMIT
  value: {{ . | quote }}
{{- end }}
{{- end }}
