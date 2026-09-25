module "kafka_platform" {
  source = "../../modules/confluent-kafka-platform"

  environment_name = "kafka-production"
  cluster_name     = "kafka-production-cluster"

  cluster_type = "STANDARD"
  availability = "MULTI_ZONE"

  cloud  = "GCP"
  region = "us-central1"

  topic_name       = "orders.v1"
  topic_partitions = 6

  min_insync_replicas = 2

  confluent_cloud_api_key    = var.confluent_cloud_api_key
  confluent_cloud_api_secret = var.confluent_cloud_api_secret
}
