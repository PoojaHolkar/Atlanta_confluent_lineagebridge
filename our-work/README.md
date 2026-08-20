# Confluent lineage demo

This exercise creates a small Confluent Cloud environment, then uses
LineageBridge to extract and display its lineage.

The Terraform configuration creates billable Confluent resources. Complete the
teardown when you finish.

## 1. Set up Confluent Cloud

You need:

- a Confluent Cloud account that can create billable resources
- an organization-level Confluent Cloud API key and secret
- Git
- Terraform 1.5 or later

### Get the repository

```bash
git clone <repository-url> lineage-bridge
cd lineage-bridge
```

### Add your Confluent Cloud credentials

Create an organization-level Cloud API key in Confluent Cloud. Use the
‘My account’ scope. A cluster-level key will not provision the demo.

Copy the example environment file:

```bash
cp .env.example .env
chmod 600 .env
```

Set these values in `.env`:

```dotenv
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=<organization-level-key>
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=<organization-level-secret>
```

### Create the demo environment

```bash
cd our-work/terraform
terraform init
terraform apply
```

Review the plan and enter `yes` when Terraform asks for confirmation.
Provisioning usually takes 8 to 12 minutes.

Terraform creates:

- a Confluent environment and Standard Kafka cluster
- `orders` and `customers` topics with Avro schemas
- 2 Datagen source connectors
- a Flink compute pool
- Flink jobs that create `enriched_orders` and `order_stats` topics
- service-specific API keys for Kafka, Schema Registry and Flink

### Add the generated service credentials

After Terraform finishes, run:

```bash
terraform output
```

Add the generated values to the `.env` file in the repository root:

```dotenv
LINEAGE_BRIDGE_SCHEMA_REGISTRY_ENDPOINT=<schema_registry_rest_endpoint>
LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_KEY=<schema_registry_api_key_id>
LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_SECRET=<schema_registry_api_key_secret>

LINEAGE_BRIDGE_FLINK_API_KEY=<flink_api_key_id>
LINEAGE_BRIDGE_FLINK_API_SECRET=<flink_api_key_secret>

LINEAGE_BRIDGE_KAFKA_API_KEY=<kafka_api_key_id>
LINEAGE_BRIDGE_KAFKA_API_SECRET=<kafka_api_key_secret>
```

Terraform hides sensitive outputs in the summary. Read an individual secret
with `terraform output -raw`, for example:

```bash
terraform output -raw schema_registry_api_key_secret
```

The current root module does not expose the Kafka key. Find it in
`terraform.tfstate` under `module.core.confluent_api_key.kafka`.

### Tear down the Confluent environment

Complete this step only after you finish the LineageBridge exercise in section 2.
Terraform will show everything it plans to delete before asking for
confirmation. Destroy the demo resources as soon as the exercise ends:

```bash
cd our-work/terraform
terraform destroy
```

Enter `yes` only after checking that the plan contains the demo resources.

## 2. Set up LineageBridge

You need [uv](https://docs.astral.sh/uv/). Run these commands from the repository
root:

```bash
uv venv
uv pip install -e ".[dev]"
```

Start the LineageBridge interface:

```bash
.venv/bin/streamlit run lineage_bridge/ui/app.py
```

Open <http://localhost:8501> if your browser does not open automatically.

In LineageBridge:

1. Select the Terraform-created Confluent environment.
2. Run the extraction.
3. Explore the generated lineage graph.

A typical extraction shows 13 nodes and 13 edges. These include topics,
connectors, Flink jobs, schemas and an external dataset.

Clicking a node focuses the graph on its neighbours. Select ‘Clear focus’ in
the sidebar to restore the full graph.

Stop LineageBridge with `Ctrl+C` in its terminal.
