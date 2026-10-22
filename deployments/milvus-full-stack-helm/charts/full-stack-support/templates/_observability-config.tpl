{{- define "full-stack.alloyConfig" -}}
discovery.kubernetes "pods" {
  role = "pod"
  namespaces { names = [{{ .Release.Namespace | quote }}] }
  selectors {
    role = "pod"
    field = "spec.nodeName=" + sys.env("NODE_NAME")
  }
}
discovery.relabel "logs" {
  targets = discovery.kubernetes.pods.targets
  rule {
    source_labels = ["__meta_kubernetes_pod_container_name"]
    regex = "alloy|loki"
    action = "drop"
  }
  rule {
    source_labels = ["__meta_kubernetes_namespace"]
    target_label = "namespace"
  }
  rule {
    source_labels = ["__meta_kubernetes_pod_name"]
    target_label = "pod"
  }
  rule {
    source_labels = ["__meta_kubernetes_pod_container_name"]
    target_label = "container"
  }
  rule {
    source_labels = ["__meta_kubernetes_pod_label_component"]
    target_label = "component"
  }
  rule {
    target_label = "cluster"
    replacement = {{ .Values.clusterLabel | quote }}
  }
}
loki.source.kubernetes "pods" {
  targets = discovery.relabel.logs.output
  forward_to = [loki.write.local.receiver]
}
loki.write "local" {
  endpoint {
    url = "http://{{ .Release.Name }}-loki:3100/loki/api/v1/push"
  }
}
{{- end -}}

{{- define "full-stack.lokiConfig" -}}
auth_enabled: false
server:
  http_listen_port: 3100
common:
  path_prefix: /var/loki
  replication_factor: 1
  ring:
    kvstore:
      store: inmemory
  storage:
    filesystem:
      chunks_directory: /var/loki/chunks
      rules_directory: /var/loki/rules
schema_config:
  configs:
    - from: "2024-01-01"
      store: tsdb
      object_store: filesystem
      schema: v13
      index:
        prefix: loki_index_
        period: 24h
storage_config:
  tsdb_shipper:
    active_index_directory: /var/loki/index
    cache_location: /var/loki/index-cache
ingester:
  wal:
    enabled: true
    dir: /var/loki/wal
  chunk_idle_period: 5m
  max_chunk_age: 1h
limits_config:
  retention_period: {{ .Values.loki.retention }}
  ingestion_rate_mb: 4
  ingestion_burst_size_mb: 8
compactor:
  working_directory: /var/loki/compactor
  retention_enabled: true
  delete_request_store: filesystem
query_range:
  results_cache:
    cache:
      embedded_cache:
        enabled: true
        max_size_mb: 32
analytics:
  reporting_enabled: false
{{- end -}}
