# NOTES

## Deliberately deferred / scoped items

### Terraform state backend

Terraform state is intentionally left as local state for this practical assessment. In production, I would use a remote encrypted backend with restricted access and state locking.

### Kafka replication factor

The assessment asks for an explicit replication factor. Confluent Cloud manages replication factor for the managed Kafka cluster, and the current Terraform topic resource does not expose a `replication_factor` argument.

The configuration therefore documents the effective RF as `3` and explicitly configures:
- `partitions_count = 6`
- `min.insync.replicas = 2`

### Kafka health checks

The Ansible rollout uses `serial: 1`, pre-flight health validation, controlled restart, listener recovery wait, post-restart Kafka metadata/ISR validation, and rollout stop on health-check failure.

The exact Kafka CLI location and authentication mechanism vary by Kafka installation. The CLI path and bootstrap endpoint are therefore configurable Ansible variables.

### Production security

No real credentials, private IP addresses, Terraform state files, or production inventory data are included.

## AI usage

AI assistance was used to review the Terraform/Ansible design, identify potential configuration issues, and improve explanations. The final implementation was reviewed manually and comments document the reasoning behind non-obvious decisions.
