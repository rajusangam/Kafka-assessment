locals {
  topic_config = {
    # Require at least two replicas in ISR before accepting writes.
    "min.insync.replicas" = tostring(var.min_insync_replicas)
  }
}

# ---------------------------------------------------------
# Confluent Cloud Environment
# ---------------------------------------------------------

resource "confluent_environment" "this" {
  display_name = var.environment_name

  stream_governance {
    package = "ESSENTIALS"
  }
}

# ---------------------------------------------------------
# Kafka Cluster
# ---------------------------------------------------------

resource "confluent_kafka_cluster" "this" {
  display_name = var.cluster_name
  availability = var.availability
  cloud        = var.cloud
  region       = var.region

  environment {
    id = confluent_environment.this.id
  }

  dynamic "basic" {
    for_each = upper(var.cluster_type) == "BASIC" ? [1] : []

    content {}
  }

  dynamic "standard" {
    for_each = upper(var.cluster_type) == "STANDARD" ? [1] : []

    content {}
  }
}

# ---------------------------------------------------------
# Schema Registry
# ---------------------------------------------------------

data "confluent_schema_registry_cluster" "this" {
  environment {
    id = confluent_environment.this.id
  }

  depends_on = [
    confluent_kafka_cluster.this
  ]
}

# ---------------------------------------------------------
# Terraform Management Service Account
# ---------------------------------------------------------

resource "confluent_service_account" "manager" {
  display_name = "${var.environment_name}-terraform-manager"

  description = "Terraform management service account"
}

resource "confluent_role_binding" "manager_cluster_admin" {
  principal = "User:${confluent_service_account.manager.id}"

  role_name = "CloudClusterAdmin"

  crn_pattern = confluent_kafka_cluster.this.rbac_crn
}

# ---------------------------------------------------------
# Terraform Management API Key
# ---------------------------------------------------------

resource "confluent_api_key" "manager" {
  display_name = "${var.environment_name}-terraform-manager-key"

  description = "Terraform management API key"

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

  lifecycle {
    prevent_destroy = true
  }
}

# ---------------------------------------------------------
# orders.v1 Topic
# ---------------------------------------------------------

resource "confluent_kafka_topic" "orders_v1" {
  kafka_cluster {
    id = confluent_kafka_cluster.this.id
  }

  topic_name       = var.topic_name
  partitions_count = var.topic_partitions

  rest_endpoint = confluent_kafka_cluster.this.rest_endpoint

  # min.insync.replicas = 2 provides write durability.
  config = local.topic_config

  credentials {
    key    = confluent_api_key.manager.id
    secret = confluent_api_key.manager.secret
  }

  depends_on = [
    confluent_role_binding.manager_cluster_admin
  ]
}

# ---------------------------------------------------------
# Application Producer Service Account
# ---------------------------------------------------------

resource "confluent_service_account" "orders_producer" {
  display_name = "${var.environment_name}-orders-producer"

  description = "Producer with WRITE access to orders.v1 only"
}

# ---------------------------------------------------------
# Producer API Key
# ---------------------------------------------------------

resource "confluent_api_key" "orders_producer" {
  display_name = "${var.environment_name}-orders-producer-key"

  description = "orders.v1 producer API key"

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

# ---------------------------------------------------------
# Least-Privilege ACL
# ---------------------------------------------------------
#
# EXACTLY ONE ACL:
#
# TOPIC       = orders.v1
# PATTERN     = LITERAL
# OPERATION   = WRITE
# PERMISSION  = ALLOW
#
# No READ ACL
# No GROUP ACL
# No PREFIXED access
#
# ---------------------------------------------------------

resource "confluent_kafka_acl" "orders_producer_write" {

  kafka_cluster {
    id = confluent_kafka_cluster.this.id
  }

  resource_type = "TOPIC"

  resource_name = confluent_kafka_topic.orders_v1.topic_name

  # Exact topic matching.
  pattern_type = "LITERAL"

  principal = "User:${confluent_service_account.orders_producer.id}"

  host = "*"

  # ONLY WRITE.
  operation = "WRITE"

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
