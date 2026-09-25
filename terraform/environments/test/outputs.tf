output "environment_id" {
  value = module.kafka_platform.environment_id
}

output "kafka_cluster_id" {
  value = module.kafka_platform.kafka_cluster_id
}

output "bootstrap_endpoint" {
  value = module.kafka_platform.kafka_bootstrap_endpoint
}

output "rest_endpoint" {
  value = module.kafka_platform.kafka_rest_endpoint
}

output "schema_registry_id" {
  value = module.kafka_platform.schema_registry_id
}

output "orders_topic" {
  value = module.kafka_platform.orders_topic
}

output "producer_service_account" {
  value = module.kafka_platform.orders_producer_service_account_id
}
