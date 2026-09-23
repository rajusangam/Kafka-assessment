output "environment_id" {
  description = "Confluent Cloud environment ID."
  value       = confluent_environment.this.id
}

output "environment_name" {
  description = "Confluent Cloud environment name."
  value       = confluent_environment.this.display_name
}

output "kafka_cluster_id" {
  description = "Kafka cluster ID."
  value       = confluent_kafka_cluster.this.id
}

output "kafka_cluster_name" {
  description = "Kafka cluster name."
  value       = confluent_kafka_cluster.this.display_name
}

output "kafka_bootstrap_endpoint" {
  description = "Kafka bootstrap endpoint."
  value       = confluent_kafka_cluster.this.bootstrap_endpoint
}

output "kafka_rest_endpoint" {
  description = "Kafka REST endpoint."
  value       = confluent_kafka_cluster.this.rest_endpoint
}

output "schema_registry_cluster_id" {
  description = "Schema Registry cluster ID."
  value       = data.confluent_schema_registry_cluster.this.id
}

output "schema_registry_package" {
  description = "Schema Registry package."
  value       = data.confluent_schema_registry_cluster.this.package
}

output "orders_topic_name" {
  description = "orders.v1 topic name."
  value       = confluent_kafka_topic.orders_v1.topic_name
}

output "orders_partitions" {
  description = "Number of partitions configured for orders.v1."
  value       = local.orders_partitions
}

output "orders_replication_factor" {
  description = "Effective replication factor documented for this managed cluster."
  value       = local.orders_replication_factor
}

output "orders_min_insync_replicas" {
  description = "Minimum in-sync replicas required for writes."
  value       = local.orders_min_insync_replicas
}

output "orders_producer_service_account_id" {
  description = "Orders producer service account ID."
  value       = confluent_service_account.orders_producer.id
}

output "orders_producer_api_key_id" {
  description = "Orders producer Kafka API key ID."
  value       = confluent_api_key.orders_producer.id
}
