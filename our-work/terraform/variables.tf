variable "confluent_cloud_api_key" {
  description = "Confluent Cloud (org-level) API key"
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud (org-level) API secret"
  type        = string
  sensitive   = true
}

variable "cloud_region" {
  description = "AWS region for the Kafka cluster and Flink pool"
  type        = string
  default     = "us-east-1"
}

variable "enable_tableflow" {
  description = <<-EOT
    Materialise orders_v2, customers_v2 and order_stats as Iceberg tables on
    Confluent-managed storage. Adds 3 nodes and 3 edges to the lineage graph.
    The Tableflow API key it needs is created by this configuration.
  EOT
  type        = bool
  default     = true
}
