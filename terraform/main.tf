# ---------------------------------------------------------------------------
# Confluent Cloud Environment
# ---------------------------------------------------------------------------

resource "confluent_environment" "this" {
  display_name = var.environment_name

  stream_governance {
    package = "ESSENTIALS"
  }
}

# ---------------------------------------------------------------------------
# Kafka Cluster
# ---------------------------------------------------------------------------

resource "confluent_kafka_cluster" "this" {
  display_name = var.cluster_name
  availability = "SINGLE_ZONE"
  cloud        = var.cloud
  region       = var.region

  # Basic is sufficient for this assessment.
  basic {}

  environment {
    id = confluent_environment.this.id
  }
}

# ---------------------------------------------------------------------------
# Schema Registry
# ---------------------------------------------------------------------------

data "confluent_schema_registry_cluster" "this" {
  environment {
    id = confluent_environment.this.id
  }

  depends_on = [
    confluent_kafka_cluster.this
  ]
}

# ---------------------------------------------------------------------------
# Terraform Manager Service Account
# ---------------------------------------------------------------------------

resource "confluent_service_account" "manager" {
  display_name = "${var.cluster_name}-terraform-manager"
  description  = "Terraform management identity for ${var.cluster_name}"
}

resource "confluent_role_binding" "manager_cluster_admin" {
  principal   = "User:${confluent_service_account.manager.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.this.rbac_crn
}

resource "confluent_api_key" "manager" {
  display_name = "${var.cluster_name}-terraform-manager-key"
  description  = "Kafka API key used by Terraform to manage Kafka resources"

  owner {
    id          = confluent_service_account.manager.id
    api_version = confluent_service_account.manager.api_version
    kind        = confluent_service_account.manager.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.this.id
    api_version = confluent_kafka_cluster.this.api_version
    kind        = confluent_kafka_cluster.this.kind

    environment {
      id = confluent_environment.this.id
    }
  }

  # Authorization must exist before Terraform uses this Kafka API key.
  depends_on = [
    confluent_role_binding.manager_cluster_admin
  ]

  lifecycle {
    prevent_destroy = true
  }
}

# ---------------------------------------------------------------------------
# Kafka Topic: orders.v1
# ---------------------------------------------------------------------------

resource "confluent_kafka_topic" "orders_v1" {
  kafka_cluster {
    id = confluent_kafka_cluster.this.id
  }

  topic_name      = local.orders_topic_name
  partitions_count = local.orders_partitions
  rest_endpoint   = confluent_kafka_cluster.this.rest_endpoint

  config = {
    # With RF=3, requiring at least 2 ISR members preserves a write quorum.
    "min.insync.replicas" = tostring(local.orders_min_insync_replicas)
  }

  credentials {
    key    = confluent_api_key.manager.id
    secret = confluent_api_key.manager.secret
  }

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [
    confluent_api_key.manager
  ]
}

# ---------------------------------------------------------------------------
# Application Producer Service Account
# ---------------------------------------------------------------------------

resource "confluent_service_account" "orders_producer" {
  display_name = "${var.cluster_name}-orders-producer"
  description  = "Application identity allowed to produce to orders.v1 only"
}

resource "confluent_api_key" "orders_producer" {
  display_name = "${var.cluster_name}-orders-producer-key"
  description  = "Kafka API key for the orders.v1 producer application"

  owner {
    id          = confluent_service_account.orders_producer.id
    api_version = confluent_service_account.orders_producer.api_version
    kind        = confluent_service_account.orders_producer.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.this.id
    api_version = confluent_kafka_cluster.this.api_version
    kind        = confluent_kafka_cluster.this.kind

    environment {
      id = confluent_environment.this.id
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

# ---------------------------------------------------------------------------
# Least-Privilege Kafka ACL
# ---------------------------------------------------------------------------

resource "confluent_kafka_acl" "orders_producer_write" {
  kafka_cluster {
    id = confluent_kafka_cluster.this.id
  }

  resource_type = "TOPIC"
  resource_name = local.orders_topic_name

  # LITERAL means orders.v1 only; it does not grant orders.v2/orders.v3 access.
  pattern_type = "LITERAL"

  principal = "User:${confluent_service_account.orders_producer.id}"
  host      = "*"

  operation  = "WRITE"
  permission = "ALLOW"

  rest_endpoint = confluent_kafka_cluster.this.rest_endpoint

  credentials {
    key    = confluent_api_key.manager.id
    secret = confluent_api_key.manager.secret
  }

  depends_on = [
    confluent_kafka_topic.orders_v1,
    confluent_api_key.orders_producer
  ]
}
