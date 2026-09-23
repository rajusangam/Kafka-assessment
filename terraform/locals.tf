locals {
  # 6 partitions: useful consumer parallelism while keeping the assessment small.
  orders_partitions = 6

  # Confluent Cloud manages the replication factor for the managed Kafka cluster.
  # The effective RF for this configuration is documented as 3.
  orders_replication_factor = 3

  # RF=3 + min ISR=2 allows one replica to be unavailable while retaining a write quorum.
  orders_min_insync_replicas = 2

  orders_topic_name = "orders.v1"
}
