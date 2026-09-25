variable "environment_name" {
  description = "Confluent Cloud environment name"
  type        = string
}

variable "cluster_name" {
  description = "Kafka cluster name"
  type        = string
}

variable "cloud" {
  description = "Confluent Cloud provider"
  type        = string
  default     = "GCP"
}

variable "region" {
  description = "Confluent Cloud region"
  type        = string
  default     = "us-central1"
}

variable "availability" {
  description = "Kafka cluster availability"
  type        = string
  default     = "SINGLE_ZONE"
}

variable "cluster_type" {
  description = "Kafka cluster type"
  type        = string
  default     = "BASIC"

  validation {
    condition     = contains(["BASIC", "STANDARD"], upper(var.cluster_type))
    error_message = "cluster_type must be BASIC or STANDARD."
  }
}

variable "topic_name" {
  description = "Kafka topic"
  type        = string
  default     = "orders.v1"
}

variable "topic_partitions" {
  description = "Number of topic partitions"
  type        = number
  default     = 6
}

variable "min_insync_replicas" {
  description = "Minimum number of ISR replicas required for writes"
  type        = number
  default     = 2
}

variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API key"
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API secret"
  type        = string
  sensitive   = true
}
