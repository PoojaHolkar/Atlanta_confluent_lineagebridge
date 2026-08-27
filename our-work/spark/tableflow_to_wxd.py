"""
LineageBridge — Tableflow → watsonx.data Spark bridge
======================================================
Reads the three Confluent Tableflow Iceberg tables (orders_v2, customers_v2,
order_stats) and writes them as NATIVE Apache Iceberg tables into
watsonx.data's IBM Cloud Object Storage catalog.

Pipeline:
    Confluent Tableflow (Iceberg REST, Confluent-managed S3)
        --> [THIS JOB: Spark read + write native]
    iceberg_catalog.lineage_bridge.*  (native tables on IBM COS)
        --> Presto --> watsonx BI

HOW TO RUN (watsonx.data console):
  1. Fill in the CONFLUENT TABLEFLOW and WATSONX.DATA sections below.
  2. Upload this file to COS:
       s3a://<your-bucket>/spark/tableflow_to_wxd.py
  3. Infrastructure manager → Spark engine → Applications → Create application
       Application type : Python
       Application path : s3a://<your-bucket>/spark/tableflow_to_wxd.py
       Spark version    : 3.5
       Spark configuration properties (add ALL FIVE — required):
         spark.hadoop.wxd.apiKey             = Basic <base64>
         spark.hadoop.fs.s3a.endpoint.region = us-east-1
         spark.gluten.sql.columnar.batchscan = false
         spark.gluten.sql.columnar.filescan  = false
         spark.sql.iceberg.vectorization.enabled = false
  4. Submit. On success the tables appear under iceberg_catalog.lineage_bridge.*
     and are queryable in the Presto SQL workspace.

Compute base64 for wxd.apiKey:
    echo -n "ibmlhapikey_YOUR_IBM_USERID:YOUR_WXD_API_KEY" | base64

Why all five Spark config properties are required at submit time:
    The Gluten/Velox engine on watsonx.data initialises before the app-level
    SparkConf is applied. If the three Gluten/vectorization flags are only set
    inside the script, Velox still intercepts the Iceberg S3 scan and fails
    with: ForbiddenException: not authorized to sign the request
    Setting them at submit time forces the JVM reader for ALL catalog scans.
"""

# ── CONFLUENT TABLEFLOW (source) ─────────────────────────────────────────────
# Fill in from: terraform output  (after terraform apply)
REGION           = "us-east-1"                 # AWS region of your Kafka cluster
ORG_ID           = "YOUR_CONFLUENT_ORG_ID"     # confluent org ID (from terraform output or Confluent UI)
ENV_ID           = "YOUR_ENV_ID"               # terraform output environment_id
CLUSTER_ID       = "YOUR_KAFKA_CLUSTER_ID"     # terraform output kafka_cluster_id
TABLEFLOW_APIKEY = "YOUR_TABLEFLOW_API_KEY"    # terraform output tableflow_api_key_id
TABLEFLOW_SECRET = "YOUR_TABLEFLOW_API_SECRET" # terraform output -raw tableflow_api_key_secret

# ── WATSONX.DATA (destination) ───────────────────────────────────────────────
DEST_CATALOG = "iceberg_catalog"        # Iceberg catalog name in your watsonx.data instance
DEST_SCHEMA  = "lineage_bridge"         # will be created if it does not exist
DEST_BUCKET  = "YOUR_WXD_COS_BUCKET"   # IBM COS bucket registered with the Iceberg catalog

# ── TABLES TO BRIDGE ─────────────────────────────────────────────────────────
# topic name → business columns to keep (drops Tableflow $$ metadata columns)
TABLES = {
    "lineage_bridge.orders_v2": [
        "order_id", "customer_id", "product_name",
        "quantity", "price", "order_status", "created_at",
    ],
    "lineage_bridge.customers_v2": [
        "customer_id", "name", "email", "country", "signup_date",
    ],
    "lineage_bridge.order_stats": [
        "order_status", "order_count", "total_quantity",
        "window_start", "window_end",
    ],
}

# enriched_orders: derived in Spark by joining orders_v2 + customers_v2.
# Cannot use Tableflow directly — the LEFT JOIN Flink topic is retract mode,
# which Tableflow does not support (only append and upsert).
ENRICHED_ORDERS_SQL = f"""
    SELECT
        o.`order_id`,
        o.`customer_id`,
        c.`name`    AS `customer_name`,
        c.`country` AS `customer_country`,
        o.`product_name`,
        o.`quantity`,
        o.`price`,
        o.`order_status`,
        o.`created_at`
    FROM tableflow.`{CLUSTER_ID}`.`lineage_bridge.orders_v2` o
    LEFT JOIN tableflow.`{CLUSTER_ID}`.`lineage_bridge.customers_v2` c
        ON o.`customer_id` = c.`customer_id`
"""

# ─────────────────────────────────────────────────────────────────────────────

REST_URI = (
    f"https://tableflow.{REGION}.aws.confluent.cloud/iceberg/catalog/"
    f"organizations/{ORG_ID}/environments/{ENV_ID}"
)


def build_spark():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder
        .appName("lineagebridge-tableflow-to-wxd")
        .enableHiveSupport()

        # ── Source: Confluent Tableflow Iceberg REST catalog ─────────────────
        .config("spark.sql.catalog.tableflow",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.tableflow.type",         "rest")
        .config("spark.sql.catalog.tableflow.uri",          REST_URI)
        .config("spark.sql.catalog.tableflow.credential",
                f"{TABLEFLOW_APIKEY}:{TABLEFLOW_SECRET}")
        .config("spark.sql.catalog.tableflow.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.tableflow.rest-metrics-reporting-enabled",
                "false")
        # Keep remote-signing enabled — the REST catalog vends signed URLs.
        # The JVM S3 reader uses them fine. Velox/Gluten cannot do remote
        # signing, so those are disabled below to force the JVM reader path.
        .config("spark.sql.catalog.tableflow.s3.remote-signing-enabled",
                "true")
        .config("spark.sql.catalog.tableflow.client.region", REGION)
        .config("spark.sql.catalog.tableflow.s3.region",     REGION)

        # ── S3 region hints for the native JVM reader (prevents HTTP 301) ────
        .config("spark.hadoop.fs.s3a.endpoint.region",  REGION)
        .config("spark.hadoop.fs.s3a.endpoint",         f"s3.{REGION}.amazonaws.com")

        # ── Force the JVM Iceberg reader — Velox cannot do remote signing ────
        # These must also be passed as Spark config properties at submit time
        # because Gluten initialises before the app-level SparkConf is applied.
        .config("spark.gluten.sql.columnar.batchscan",         "false")
        .config("spark.gluten.sql.columnar.filescan",          "false")
        .config("spark.sql.iceberg.vectorization.enabled",     "false")

        .getOrCreate()
    )


def main():
    spark = build_spark()

    # Ensure destination schema exists on the watsonx.data COS catalog.
    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {DEST_CATALOG}.{DEST_SCHEMA} "
        f"LOCATION 's3a://{DEST_BUCKET}/{DEST_SCHEMA}/'"
    )
    print(f">>> Schema {DEST_CATALOG}.{DEST_SCHEMA} ready")

    for topic, cols in TABLES.items():
        tbl      = topic.split(".")[-1]
        src      = f"tableflow.`{CLUSTER_ID}`.`{topic}`"
        dst      = f"{DEST_CATALOG}.{DEST_SCHEMA}.{tbl}"
        col_list = ", ".join(f"`{c}`" for c in cols)

        print(f"\n>>> {src}")
        print(f"    → {dst}")

        df = spark.sql(f"SELECT {col_list} FROM {src}")
        df.writeTo(dst).using("iceberg").createOrReplace()

        count = spark.sql(f"SELECT COUNT(*) AS c FROM {dst}").collect()[0]["c"]
        print(f"    wrote {count} rows")

    # enriched_orders — derived via Spark JOIN (not from Tableflow directly)
    dst_enriched = f"{DEST_CATALOG}.{DEST_SCHEMA}.enriched_orders"
    print(f"\n>>> JOIN → {dst_enriched}")
    df_enriched = spark.sql(ENRICHED_ORDERS_SQL)
    df_enriched.writeTo(dst_enriched).using("iceberg").createOrReplace()
    count = spark.sql(f"SELECT COUNT(*) AS c FROM {dst_enriched}").collect()[0]["c"]
    print(f"    wrote {count} rows")

    print("\n>>> Done. Query in Presto:")
    for topic in TABLES:
        tbl = topic.split(".")[-1]
        print(f"    SELECT * FROM {DEST_CATALOG}.{DEST_SCHEMA}.{tbl} LIMIT 20;")
    print(f"    SELECT * FROM {dst_enriched} LIMIT 20;")

    spark.stop()


if __name__ == "__main__":
    main()
