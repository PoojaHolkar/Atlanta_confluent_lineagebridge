# ─────────────────────────────────────────────────────────────────────────────
# LineageBridge Demo: Confluent only (no external catalog)
#
# Confluent core (topics, schemas, datagen connectors, Flink pool) plus the two
# Flink SQL statements copied from the UC demo, so the extracted graph has
# derived topics. No Tableflow, no AWS/Databricks/GCP.
# ─────────────────────────────────────────────────────────────────────────────

terraform {
  required_version = ">= 1.5"

  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.0"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.9"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }

  backend "local" {
    path = "terraform.tfstate"
  }
}

provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}

module "core" {
  source = "../../infra/demos/modules/confluent-core"

  demo_label     = "lb"
  cloud_provider = "AWS"
  cloud_region   = var.cloud_region
}

resource "time_sleep" "datagen_warmup" {
  create_duration = "60s"
  depends_on      = [module.core]
}

# ── Flink SQL ───────────────────────────────────────────────────────────────

resource "confluent_flink_statement" "drop_enriched_orders" {
  organization { id = module.core.organization_id }
  environment { id = module.core.environment_id }
  compute_pool { id = module.core.flink_compute_pool_id }
  principal { id = module.core.service_account_id }

  statement_name = "${module.core.demo_prefix}-drop-enriched-orders"
  rest_endpoint  = module.core.flink_region_rest_endpoint
  statement      = "DROP TABLE IF EXISTS `lineage_bridge.enriched_orders`;"

  properties = {
    "sql.current-catalog"  = module.core.environment_display_name
    "sql.current-database" = module.core.kafka_cluster_display_name
  }

  credentials {
    key    = module.core.flink_api_key_id
    secret = module.core.flink_api_key_secret
  }

  depends_on = [time_sleep.datagen_warmup]

  lifecycle { ignore_changes = all }
}

resource "confluent_flink_statement" "drop_order_stats" {
  organization { id = module.core.organization_id }
  environment { id = module.core.environment_id }
  compute_pool { id = module.core.flink_compute_pool_id }
  principal { id = module.core.service_account_id }

  statement_name = "${module.core.demo_prefix}-drop-order-stats"
  rest_endpoint  = module.core.flink_region_rest_endpoint
  statement      = "DROP TABLE IF EXISTS `lineage_bridge.order_stats`;"

  properties = {
    "sql.current-catalog"  = module.core.environment_display_name
    "sql.current-database" = module.core.kafka_cluster_display_name
  }

  credentials {
    key    = module.core.flink_api_key_id
    secret = module.core.flink_api_key_secret
  }

  depends_on = [time_sleep.datagen_warmup]

  lifecycle { ignore_changes = all }
}

resource "confluent_flink_statement" "enriched_orders" {
  organization { id = module.core.organization_id }
  environment { id = module.core.environment_id }
  compute_pool { id = module.core.flink_compute_pool_id }
  principal { id = module.core.service_account_id }

  statement_name = "${module.core.demo_prefix}-enrich-orders"
  rest_endpoint  = module.core.flink_region_rest_endpoint

  statement = <<-SQL
    CREATE TABLE `lineage_bridge.enriched_orders` AS
    SELECT
      o.`order_id`,
      o.`customer_id`,
      c.`name`       AS `customer_name`,
      c.`country`    AS `customer_country`,
      o.`product_name`,
      o.`quantity`,
      o.`price`,
      o.`order_status`,
      o.`created_at`
    FROM `${module.core.orders_topic_name}` o
    LEFT JOIN `${module.core.customers_topic_name}` c
      ON o.`customer_id` = c.`customer_id`;
  SQL

  properties = {
    "sql.current-catalog"  = module.core.environment_display_name
    "sql.current-database" = module.core.kafka_cluster_display_name
  }

  credentials {
    key    = module.core.flink_api_key_id
    secret = module.core.flink_api_key_secret
  }

  depends_on = [confluent_flink_statement.drop_enriched_orders]
}

resource "confluent_flink_statement" "order_stats" {
  organization { id = module.core.organization_id }
  environment { id = module.core.environment_id }
  compute_pool { id = module.core.flink_compute_pool_id }
  principal { id = module.core.service_account_id }

  statement_name = "${module.core.demo_prefix}-order-stats"
  rest_endpoint  = module.core.flink_region_rest_endpoint

  statement = <<-SQL
    CREATE TABLE `lineage_bridge.order_stats` AS
    SELECT
      `order_status`,
      COUNT(*)        AS `order_count`,
      SUM(`quantity`) AS `total_quantity`,
      window_start,
      window_end
    FROM TABLE(
      TUMBLE(TABLE `${module.core.orders_topic_name}`, DESCRIPTOR(`$rowtime`), INTERVAL '1' MINUTE)
    )
    GROUP BY `order_status`, window_start, window_end;
  SQL

  properties = {
    "sql.current-catalog"  = module.core.environment_display_name
    "sql.current-database" = module.core.kafka_cluster_display_name
  }

  credentials {
    key    = module.core.flink_api_key_id
    secret = module.core.flink_api_key_secret
  }

  depends_on = [confluent_flink_statement.drop_order_stats]
}

# ── Tableflow sink ──────────────────────────────────────────────────────────
#
# Materialises topics as Iceberg tables using Confluent-managed storage, so no
# AWS/GCP/Azure account and no S3 bucket are involved. The extractor turns each
# one into a TABLEFLOW_TABLE node with a MATERIALIZES edge from its topic.
#
# There is no catalog integration: confluent_catalog_integration only speaks to
# Glue, Unity Catalog and Snowflake Open Catalog, all of which need a second
# cloud account. The graph therefore ends at the Tableflow table.
#
# Set enable_tableflow = false in terraform.tfvars to leave all of this out.

locals {
  tableflow_count = var.enable_tableflow ? 1 : 0
}

# Tableflow will not accept the Cloud API key: it needs a key scoped to the
# Tableflow resource. The literals below are what that scope looks like to the
# API — they are not placeholders. Taken from the provider's own
# confluent-managed-storage example.
resource "confluent_api_key" "tableflow" {
  count = local.tableflow_count

  display_name = "${module.core.demo_prefix}-tableflow-key"
  description  = "Tableflow API key for the LineageBridge demo"

  owner {
    id          = module.core.service_account_id
    api_version = module.core.service_account_api_version
    kind        = module.core.service_account_kind
  }

  managed_resource {
    id          = "tableflow"
    api_version = "tableflow/v1"
    kind        = "Tableflow"

    environment { id = module.core.environment_id }
  }

  # The role bindings inside the module take about 90 seconds each, and the key
  # is rejected until they exist.
  depends_on = [module.core]
}

resource "confluent_tableflow_topic" "orders" {
  count = local.tableflow_count

  environment { id = module.core.environment_id }
  kafka_cluster { id = module.core.kafka_cluster_id }

  display_name  = module.core.orders_topic_name
  table_formats = ["ICEBERG"]

  managed_storage {}

  credentials {
    key    = confluent_api_key.tableflow[0].id
    secret = confluent_api_key.tableflow[0].secret
  }

  # Tableflow needs the topic to have a registered schema, which the datagen
  # connector writes during the warm-up.
  depends_on = [time_sleep.datagen_warmup]
}

resource "confluent_tableflow_topic" "customers" {
  count = local.tableflow_count

  environment { id = module.core.environment_id }
  kafka_cluster { id = module.core.kafka_cluster_id }

  display_name  = module.core.customers_topic_name
  table_formats = ["ICEBERG"]

  managed_storage {}

  credentials {
    key    = confluent_api_key.tableflow[0].id
    secret = confluent_api_key.tableflow[0].secret
  }

  depends_on = [time_sleep.datagen_warmup]
}

# order_stats is created by Flink, so it only exists once that statement runs.
# enriched_orders is left out on purpose: the LEFT JOIN makes it a changelog
# topic, and the Glue demo this configuration follows does not materialise it.
resource "confluent_tableflow_topic" "order_stats" {
  count = local.tableflow_count

  environment { id = module.core.environment_id }
  kafka_cluster { id = module.core.kafka_cluster_id }

  display_name  = "lineage_bridge.order_stats"
  table_formats = ["ICEBERG"]

  managed_storage {}

  credentials {
    key    = confluent_api_key.tableflow[0].id
    secret = confluent_api_key.tableflow[0].secret
  }

  depends_on = [confluent_flink_statement.order_stats]
}
