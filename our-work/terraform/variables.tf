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
