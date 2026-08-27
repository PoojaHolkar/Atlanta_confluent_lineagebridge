# LineageBridge — Lab Guide
## Confluent Cloud + Tableflow + watsonx.data End-to-End Demo

> **Purpose:** Step-by-step guide to provision a Confluent Cloud demo environment,
> install LineageBridge, visualise the lineage graph, and bridge Tableflow Iceberg
> tables into IBM watsonx.data via a Spark job.
>
> **Time required:** ~35 minutes  
> **Prerequisites:** macOS with Homebrew, Confluent Cloud account (free trial is fine), IBM watsonx.data instance  
> **Cost:** ~$1–2/hour while the Standard Kafka cluster is running

---

## What You Will Build

```
Datagen (orders) ──► lineage_bridge.orders_v2 ──► Flink: enriched_orders ──► Tableflow (Iceberg)
                                                 └► Flink: order_stats    ──► Tableflow (Iceberg)
Datagen (customers) ──► lineage_bridge.customers_v2 ──► Tableflow (Iceberg)
                                                                │
                                              Spark bridge (tableflow_to_wxd.py)
                                                                │
                                                                ▼
                                             iceberg_catalog.lineage_bridge.*
                                             (native Iceberg on IBM COS)
                                                                │
                                                       Presto / watsonx BI
```

**Infrastructure provisioned by Terraform:**
| Resource | Count |
|---|---|
| Confluent environment | 1 |
| Kafka cluster (Standard, AWS us-east-1) | 1 |
| Service account + RBAC role bindings | 3 |
| API keys (Kafka, Schema Registry, Flink, Tableflow) | 4 |
| Kafka topics (orders_v2, customers_v2) | 2 |
| Datagen source connectors | 2 |
| Flink compute pool | 1 |
| Flink SQL statements (drop + create for enriched_orders, order_stats) | 4 |
| Tableflow Iceberg tables (orders_v2, customers_v2, order_stats) | 3 |
| Random suffix + time sleep | 2 |
| **Total** | **22** |

---

## STEP 1 — Log In to Confluent Cloud

```bash
confluent login --save
```

**Expected output:**
```
Logged in as "your@email.com" for organization "xxxx" ("your-org").
Wrote login credentials to keychain.
```

---

## STEP 2 — Get Your Confluent Cloud API Key

Create a **Cloud-level** (org-level) API key. This is different from a cluster key.

```bash
confluent api-key create --resource cloud -o json
```

Save the `api_key` and `api_secret` values — you need them in Step 5.

> Alternatively: https://confluent.cloud/settings/api-keys → **Add key** → **My account** scope

---

## STEP 3 — Install Required Tools

### Python Package Manager (uv)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Terraform

```bash
brew install hashicorp/tap/terraform
terraform version    # should show >= 1.5
```

### Confluent CLI (if not already installed)

```bash
brew install confluentinc/tap/cli
confluent version
```

---

## STEP 4 — Install LineageBridge

```bash
cd lineage-bridge          # project root

uv venv                    # creates .venv/
uv pip install -e ".[dev]" # installs all dependencies
```

**Expected output:**
```
Using CPython 3.12.x interpreter
Creating virtual environment at: .venv
...
Successfully installed lineage-bridge-0.6.1 ...
```

---

## STEP 5 — Create Terraform Variables File

Navigate to the Terraform config:

```bash
cd our-work/terraform
```

Create `terraform.tfvars` with your credentials:

```bash
cat > terraform.tfvars <<'EOF'
confluent_cloud_api_key    = "YOUR_API_KEY"
confluent_cloud_api_secret = "YOUR_API_SECRET"
cloud_region               = "us-east-1"
enable_tableflow           = true
EOF
```

> ⚠️ **Important:** Use `us-east-1` (AWS format with dashes), NOT `us-east1` (GCP format).  
> `enable_tableflow = true` materialises the three topics as Iceberg tables — required for the Spark bridge step.

---

## STEP 6 — Initialize Terraform

```bash
terraform init
```

**Expected output:**
```
Initializing modules...
- core in ../../infra/demos/modules/confluent-core

Initializing provider plugins...
- Installing confluentinc/confluent v2.83.0...
- Installing hashicorp/time v0.14.1...
- Installing hashicorp/random v3.9.0...

Terraform has been successfully initialized!
```

---

## STEP 7 — Preview the Infrastructure Plan

```bash
terraform plan
```

Verify the plan shows **22 resources to add, 0 errors**.

---

## STEP 8 — Apply the Infrastructure

```bash
terraform apply
```

Type `yes` when prompted.

> ⏱ **This takes 10–15 minutes.** Role bindings take ~90s each, API keys ~2 min,
> connectors ~3 min, Tableflow topics ~2 min after Flink warms up.

**Expected final output:**
```
Apply complete! Resources: 22 added, 0 changed, 0 destroyed.

Outputs:
environment_id                 = "env-xxxxxxx"
flink_api_key_id               = "XXXXXXXXXX"
kafka_cluster_id               = "lkc-xxxxxxx"
schema_registry_api_key_id     = "XXXXXXXXXX"
schema_registry_rest_endpoint  = "https://psrc-xxxxx.us-east-1.aws.confluent.cloud"
tableflow_api_key_id           = "XXXXXXXXXX"
tableflow_enabled              = true
tableflow_tables               = { ... }
```

---

## STEP 9 — Capture Terraform Outputs

```bash
terraform output
terraform output -json   # to see sensitive values
```

Note down the following:

| Variable | Command to get it |
|---|---|
| Environment ID | `terraform output environment_id` |
| Kafka Cluster ID | `terraform output kafka_cluster_id` |
| Schema Registry endpoint | `terraform output schema_registry_rest_endpoint` |
| Schema Registry API key | `terraform output schema_registry_api_key_id` |
| Schema Registry API secret | `terraform output -json \| python3 -c "import json,sys; print(json.load(sys.stdin)['schema_registry_api_key_secret']['value'])"` |
| Flink API key | `terraform output flink_api_key_id` |
| Flink API secret | `terraform output -json \| python3 -c "import json,sys; print(json.load(sys.stdin)['flink_api_key_secret']['value'])"` |
| Tableflow API key | `terraform output tableflow_api_key_id` |
| Tableflow API secret | `terraform output -json \| python3 -c "import json,sys; print(json.load(sys.stdin)['tableflow_api_key_secret']['value'])"` |
| Kafka API key | `terraform state show 'module.core.confluent_api_key.kafka'` → `id` field |
| Kafka API secret | `terraform show -json \| python3 -c "..."` (see state) |

---

## STEP 10 — Create the `.env` File

```bash
cd ../..    # back to lineage-bridge/
```

```bash
umask 077 && cat > .env <<'EOF'
# LineageBridge — Confluent Cloud Credentials

# Confluent Cloud API key (org-level)
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=YOUR_CLOUD_API_KEY
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=YOUR_CLOUD_API_SECRET

# Schema Registry
LINEAGE_BRIDGE_SCHEMA_REGISTRY_ENDPOINT=https://psrc-XXXXX.us-east-1.aws.confluent.cloud
LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_KEY=YOUR_SR_API_KEY
LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_SECRET=YOUR_SR_API_SECRET

# Flink
LINEAGE_BRIDGE_FLINK_API_KEY=YOUR_FLINK_API_KEY
LINEAGE_BRIDGE_FLINK_API_SECRET=YOUR_FLINK_API_SECRET

# Kafka (cluster-scoped key — needed for consumer groups)
LINEAGE_BRIDGE_KAFKA_API_KEY=YOUR_KAFKA_API_KEY
LINEAGE_BRIDGE_KAFKA_API_SECRET=YOUR_KAFKA_API_SECRET

LINEAGE_BRIDGE_LOG_LEVEL=INFO
EOF
```

Replace all `YOUR_*` placeholders with actual values from Step 9.

> 🔒 `.env` is git-ignored. Never commit it.

Verify the credentials work:

```bash
set -a && . .env && set +a
curl -s -u "$LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY:$LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET" \
  https://api.confluent.cloud/org/v2/environments | python3 -m json.tool | head -20
```

You should see your environment `env-xxxxxxx` in the response.

---

## STEP 11 — Launch the Streamlit UI

```bash
uv run streamlit run lineage_bridge/ui/app.py
```

Your browser opens at **http://localhost:8501**

---

## STEP 12 — Extract Lineage

In the UI:

1. **Welcome dialog** — click **"Skip for Now"** (credentials are already in `.env`)
2. In the **left sidebar** under **Setup**:
   - Select your **Environment** → `env-xxxxxxx`
   - Select your **Cluster** → `lkc-xxxxxxx`
3. Click **"Extract Lineage"**
4. Watch the progress panel — each phase reports its status:
   - Phase 1: Kafka topics + consumer groups
   - Phase 2: Connectors + Flink + Tableflow
   - Phase 3: Schema Registry enrichment
5. Wait for extraction to complete (~15–30 seconds)

---

## STEP 13 — Explore the Graph

Once extraction completes you will see an interactive directed graph:

```
[Datagen Orders]    ──produces──► [orders_v2]    ──► [Flink: enriched_orders] ──► [enriched_orders topic]
[Datagen Customers] ──produces──► [customers_v2] ──► [Flink: order_stats]     ──► [order_stats topic]
                                       │                                                │
                                       ▼                                                ▼
                               [Tableflow: customers_v2]               [Tableflow: order_stats]
[orders_v2] ──► [Tableflow: orders_v2]
```

**Graph interactions:**
| Action | How |
|---|---|
| Zoom in/out | Scroll wheel |
| Move nodes | Drag |
| Select region | Shift + drag |
| Inspect a node | Click it |
| Search by name | Search box in sidebar |
| Export graph | "Export Graph" button in sidebar |

**Click any node** to see its full details:
- Kafka topics → partition count, schema, deep link to Confluent Cloud
- Connectors → class, status, config
- Flink jobs → SQL statement, input/output topics
- Tableflow tables → Iceberg table path, managed storage location

---

## STEP 14 — Bridge Tableflow to watsonx.data (Spark Job)

This step reads the three Tableflow Iceberg tables from Confluent-managed S3 and
writes them as **native Apache Iceberg tables** into your watsonx.data COS catalog,
making them queryable by Presto and IBM watsonx BI.

### 14a — Fill in the Spark script

Open `our-work/spark/tableflow_to_wxd.py` and update the placeholder sections:

**Confluent Tableflow section** — fill from `terraform output`:
```python
REGION           = "us-east-1"                   # your cluster region
ORG_ID           = "<terraform output org_id>"   # your Confluent org ID
ENV_ID           = "<terraform output environment_id>"
CLUSTER_ID       = "<terraform output kafka_cluster_id>"
TABLEFLOW_APIKEY = "<terraform output tableflow_api_key_id>"
TABLEFLOW_SECRET = "<tableflow_api_key_secret from terraform output -json>"
```

**watsonx.data section** — fill from your watsonx.data instance:
```python
DEST_CATALOG = "iceberg_catalog"     # Iceberg catalog name in watsonx.data
DEST_SCHEMA  = "lineage_bridge"      # will be created if missing
DEST_BUCKET  = "<your-wxd-cos-bucket-name>"
```

### 14b — Compute the watsonx.data API key Base64

```bash
echo -n "ibmlhapikey_YOUR_IBM_USERID:YOUR_WXD_API_KEY" | base64
```

This produces the value for `spark.hadoop.wxd.apiKey`.

### 14c — Upload the script to COS

In the watsonx.data console, upload the script to your COS bucket:

```
s3a://<your-bucket>/spark/tableflow_to_wxd.py
```

Or use the IBM Cloud CLI / COS UI to upload from your local path:
```
our-work/spark/tableflow_to_wxd.py
```

### 14d — Submit the Spark application

In the watsonx.data console:

**Infrastructure manager → Spark engine → Applications → Create application**

| Field | Value |
|---|---|
| Application type | Python |
| Application path | `s3a://<your-bucket>/spark/tableflow_to_wxd.py` |
| Spark version | 3.5 |

Add **all five** Spark configuration properties:

| Property | Value |
|---|---|
| `spark.hadoop.wxd.apiKey` | `Basic <your-base64-from-14b>` |
| `spark.hadoop.fs.s3a.endpoint.region` | `us-east-1` |
| `spark.gluten.sql.columnar.batchscan` | `false` |
| `spark.gluten.sql.columnar.filescan` | `false` |
| `spark.sql.iceberg.vectorization.enabled` | `false` |

> ⚠️ **All five properties are required.** The Gluten/Velox engine on watsonx.data
> initialises before the app-level SparkConf, so the three Gluten/vectorization
> flags must be set at submit time — setting them only in the script is not enough.
> Without them, Velox intercepts the Iceberg S3 scan and fails with:
> `ForbiddenException: not authorized to sign the request`

Click **Submit**.

### 14e — Verify success

**Expected log output:**
```
>>> Schema iceberg_catalog.lineage_bridge ready

>>> tableflow.`lkc-xxxxxxx`.`lineage_bridge.orders_v2`
    → iceberg_catalog.lineage_bridge.orders_v2
    wrote 1240 rows

>>> tableflow.`lkc-xxxxxxx`.`lineage_bridge.customers_v2`
    → iceberg_catalog.lineage_bridge.customers_v2
    wrote 430 rows

>>> tableflow.`lkc-xxxxxxx`.`lineage_bridge.order_stats`
    → iceberg_catalog.lineage_bridge.order_stats
    wrote 18 rows

>>> JOIN → iceberg_catalog.lineage_bridge.enriched_orders
    wrote 1240 rows

>>> Done. Query in Presto:
    SELECT * FROM iceberg_catalog.lineage_bridge.orders_v2 LIMIT 20;
    SELECT * FROM iceberg_catalog.lineage_bridge.customers_v2 LIMIT 20;
    SELECT * FROM iceberg_catalog.lineage_bridge.order_stats LIMIT 20;
    SELECT * FROM iceberg_catalog.lineage_bridge.enriched_orders LIMIT 20;
```

### 14f — Query in Presto

In the watsonx.data console → **SQL workspace** → select the Presto engine:

```sql
SELECT * FROM iceberg_catalog.lineage_bridge.orders_v2 LIMIT 20;
SELECT * FROM iceberg_catalog.lineage_bridge.customers_v2 LIMIT 20;
SELECT * FROM iceberg_catalog.lineage_bridge.order_stats LIMIT 20;
SELECT * FROM iceberg_catalog.lineage_bridge.enriched_orders LIMIT 20;
```

---

## Verification Checklist

- [ ] Confluent Cloud login successful
- [ ] API key created (org-level)
- [ ] `uv venv` + `uv pip install -e ".[dev]"` completed
- [ ] `terraform.tfvars` created with correct region (`us-east-1`) and `enable_tableflow = true`
- [ ] `terraform init` succeeded
- [ ] `terraform apply` completed (22 resources created)
- [ ] `.env` created with all credentials
- [ ] `curl` test confirms credentials work
- [ ] Streamlit UI opens at http://localhost:8501
- [ ] Lineage extraction completes — graph shows Tableflow table nodes
- [ ] `tableflow_to_wxd.py` updated with correct ORG_ID, ENV_ID, CLUSTER_ID, DEST_BUCKET
- [ ] All five Spark config properties set at submit time
- [ ] Spark job completes — 4 tables written to `iceberg_catalog.lineage_bridge.*`
- [ ] Presto queries return data

---

## Troubleshooting

### `zsh: command not found: terraform`
```bash
brew install hashicorp/tap/terraform
```

### Terraform error: `there aren't any flink regions with "cloud"="AWS", "region"="us-east1"`
Your `terraform.tfvars` has `us-east1` (GCP format). Fix it:
```
cloud_region = "us-east-1"
```

### Extraction returns 401 Unauthorized
- `LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY` must be an **org-level** key, not cluster-scoped
- Verify key/secret have no extra spaces

### No Tableflow nodes in graph
- Confirm `enable_tableflow = true` in `terraform.tfvars` and that `terraform apply` completed fully
- Tableflow topics only appear once the Flink statements have run and the topics have data

### Spark job fails: `ForbiddenException: not authorized to sign the request`
The Gluten/Velox engine is intercepting the Iceberg S3 scan. All three Gluten flags
must be set **at submit time** in the console's Spark configuration properties — not
only in the script:
```
spark.gluten.sql.columnar.batchscan     = false
spark.gluten.sql.columnar.filescan      = false
spark.sql.iceberg.vectorization.enabled = false
```
Also confirm `spark.hadoop.fs.s3a.endpoint.region` matches your cluster region (`us-east-1`).

### Spark job fails: `AccessDeniedException` writing to COS
- Verify `spark.hadoop.wxd.apiKey = Basic <base64>` is correct
- Recompute base64: `echo -n "ibmlhapikey_YOUR_EMAIL:YOUR_API_KEY" | base64`
- Confirm the IBM API key has **Writer** access to the Spark engine and COS bucket

### Spark job fails: `NoSuchTableException` or `NoSuchNamespaceException`
- Confirm `DEST_CATALOG` matches the Iceberg catalog name in watsonx.data (Infrastructure manager → Catalogs)
- Confirm `DEST_BUCKET` matches the COS bucket registered with that catalog

### Graph appears empty
- Select the correct environment and cluster in the sidebar before extracting

---

## Teardown (When Done)

> ⚠️ A Standard Kafka cluster bills ~$1–2/hour. Destroy when not in use.

```bash
cd our-work/terraform
terraform destroy
```

Type `yes` when prompted. Takes ~3–5 minutes.

Verify it's gone:
```bash
curl -s -u "YOUR_KEY:YOUR_SECRET" \
  https://api.confluent.cloud/org/v2/environments | python3 -m json.tool
```

The environment `env-xxxxxxx` should no longer appear.

---
