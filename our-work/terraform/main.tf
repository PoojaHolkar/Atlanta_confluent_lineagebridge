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
