variable "confluent_cloud_api_key" {
  description = "Confluent Cloud organization API key used by Terraform."
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud organization API secret used by Terraform."
  type        = string
  sensitive   = true
}

variable "environment_name" {
  description = "Name of the Confluent Cloud environment."
  type        = string
  default     = "kafka-platform-assessment"
}

variable "cluster_name" {
  description = "Name of the Confluent Cloud Kafka cluster."
  type        = string
  default     = "assessment-kafka"
}

variable "cloud" {
  description = "Confluent Cloud provider."
  type        = string
  default     = "GCP"

  validation {
    condition     = var.cloud == "GCP"
    error_message = "This assessment configuration is intended for GCP."
  }
}

variable "region" {
  description = "GCP region for the Kafka cluster."
  type        = string
  default     = "us-central1"
}
