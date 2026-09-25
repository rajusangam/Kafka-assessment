output "environment_id" {
  value = confluent_environment.this.id
}

output "kafka_cluster_id" {
  value = confluent_kafka_cluster.this.id
}

output "kafka_bootstrap_endpoint" {
  value = confluent_kafka_cluster.this.bootstrap_endpoint
}

output "kafka_rest_endpoint" {
  value = confluent_kafka_cluster.this.rest_endpoint
}

output "schema_registry_id" {
  value = data.confluent_schema_registry_cluster.this.id
}

output "orders_topic" {
  value = confluent_kafka_topic.orders_v1.topic_name
}

output "orders_producer_service_account_id" {
  value = confluent_service_account.orders_producer.id
}

output "orders_producer_api_key" {
  value     = confluent_api_key.orders_producer.id
  sensitive = true
}

output "orders_producer_api_secret" {
  value     = confluent_api_key.orders_producer.secret
  sensitive = true
}
