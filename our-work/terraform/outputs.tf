output "environment_id" { value = module.core.environment_id }
output "kafka_cluster_id" { value = module.core.kafka_cluster_id }
output "schema_registry_rest_endpoint" { value = module.core.schema_registry_rest_endpoint }
output "schema_registry_api_key_id" { value = module.core.schema_registry_api_key_id }
output "schema_registry_api_key_secret" {
  value     = module.core.schema_registry_api_key_secret
  sensitive = true
}
output "flink_api_key_id" { value = module.core.flink_api_key_id }
output "flink_api_key_secret" {
  value     = module.core.flink_api_key_secret
  sensitive = true
}

output "tableflow_enabled" { value = var.enable_tableflow }

output "tableflow_api_key_id" {
  description = "Tableflow API key ID (optional in .env — extraction falls back to the Cloud key)"
  value       = one(confluent_api_key.tableflow[*].id)
}

output "tableflow_api_key_secret" {
  value     = one(confluent_api_key.tableflow[*].secret)
  sensitive = true
}

output "tableflow_tables" {
  description = "Topic name → Iceberg table path for each Tableflow-enabled topic"
  value = {
    for t in concat(
      confluent_tableflow_topic.orders,
      confluent_tableflow_topic.customers,
      confluent_tableflow_topic.order_stats,
    ) : t.display_name => t.table_path
  }
}
