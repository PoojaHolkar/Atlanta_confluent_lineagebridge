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
