# Kafka Platform Engineer — Practical Assessment

This repository contains the practical assessment implementation.

## Contents

- `answers.md` — Section 1 Kafka/platform answers
- `terraform/` — Section 2a Confluent Cloud infrastructure
- `ansible/` — Section 2b safe Kafka configuration-management role
- `scripts/` — Section 3 Kafka health-check script
- `NOTES.md` — deferred scope and AI usage

## Terraform

Terraform provisions:
- Confluent Cloud environment
- Stream Governance Essentials / Schema Registry
- Basic GCP Kafka cluster
- `orders.v1`
- 6 partitions
- effective RF=3 documented for the managed cluster
- `min.insync.replicas=2`
- Terraform management service account and Kafka API key
- application producer service account and Kafka API key
- literal topic-level WRITE ACL for `orders.v1`

### Authentication

Prefer environment variables:

```bash
export CONFLUENT_CLOUD_API_KEY="..."
export CONFLUENT_CLOUD_API_SECRET="..."
```

Then:

```bash
cd terraform
terraform init
terraform fmt -recursive
terraform validate
terraform plan
terraform apply
```

Do not commit credentials or Terraform state.

## Ansible

The role demonstrates a safe Kafka rolling change:
1. `serial: 1`
2. pre-flight health check
3. configuration update
4. immediate handler flush
5. Kafka restart
6. listener recovery wait
7. post-restart health check
8. stop the rollout if the broker/cluster is unhealthy

Run with:

```bash
cd ansible
cp inventory.ini.example inventory.ini
# Edit inventory.ini for the target test environment.
ansible-playbook -i inventory.ini site.yml
```

## Health check

The Python script returns:
- exit code `0` — healthy
- exit code `1` — unhealthy

It checks Kafka service/listener/metadata and detects under-replicated partitions when `kafka-topics.sh --describe` is available.

For secured production Kafka, add the required SASL/TLS CLI options to the health-check implementation.

## Assessment note

Confluent Cloud manages replication factor for its managed Kafka cluster. The Terraform topic resource does not expose a `replication_factor` argument, so the effective RF is documented as `3` rather than passing an unsupported argument.
