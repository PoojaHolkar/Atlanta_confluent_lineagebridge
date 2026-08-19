# Pushing Confluent lineage into watsonx

This folder holds everything we worked out while adding an OpenLineage push path
to LineageBridge, so that lineage extracted from Confluent Cloud can be sent to
IBM watsonx.data intelligence.

Read this file first. It is the only document here.

## What is in this folder

| file | what it is |
|---|---|
| `notes.md` | this file: findings, runbook, teardown |
| `watsonx_lineage_repro.py` | standalone script that reproduces the watsonx ingestion failure. Standard library only |
| `sample_events.json` | a real batch of OpenLineage events we extracted, useful for testing a push without a Confluent account |
| `terraform/` | a Confluent-only demo environment, so you can build a graph to push |

The code changes themselves are not here. They live in the main package, listed
under "what we added" below.

## Current status

The push code works. Every layer we control succeeds: authentication,
authorization, routing and payload validation. The watsonx service then returns
HTTP 500 "could not be persisted" and the events are not stored.

That is an IBM-side problem. `watsonx_lineage_repro.py` demonstrates it in four
steps and prints trace IDs to give to IBM support.

## What LineageBridge is, and is not

LineageBridge does not watch data. It polls Confluent's control-plane REST APIs
and infers a graph from declared configuration:

- the topic list becomes dataset nodes
- connector configs become job nodes, plus edges to external systems
- Flink statement SQL becomes edges, parsed with regular expressions in `clients/flink.py`
- Kafka admin gives consumer groups
- Schema Registry gives column detail

Anything not declared is invisible. A plain producer application with no
connector and no Flink job shows up as a consumer group name at most.

The SQL parsing uses regular expressions, not a parser. That is fine for the
demo SQL and fragile for arbitrary production SQL with common table expressions,
subqueries or dynamic names.

OpenLineage itself is a JSON schema plus the convention of POSTing it to a
receiver. There is no OpenLineage application to run. It has three nouns:
datasets, jobs and runs, plus optional facets.

Graph interfaces such as watsonx are ego-centric. They draw a few hops around
one selected node, so clicking re-roots the picture. You do not get a global
diagram.

## What we added

LineageBridge could already pull lineage from its own API, and push through
vendor SDKs for Databricks Unity Catalog, Glue, DataZone and Google. Nothing
posted OpenLineage to an arbitrary URL. We wrote that.

Two providers, following the pattern the existing catalog providers use:

| file | change |
|---|---|
| `lineage_bridge/catalogs/openlineage_http.py` | new. `OpenLineageHTTPProvider.push_lineage()`: builds events, drops any with no inputs and no outputs, POSTs each with an optional bearer token |
| `lineage_bridge/catalogs/watsonx.py` | new. `WatsonxProvider.push_lineage()`: IAM token exchange, facet allowlist, batch POST, START and COMPLETE pairs |
| `lineage_bridge/config/settings.py` | `openlineage_endpoint`, `openlineage_auth_token`, `watsonx_host`, `watsonx_api_key` |
| `lineage_bridge/services/requests.py` | `"openlineage"` and `"watsonx"` added to `PushProviderName` |
| `lineage_bridge/services/push_service.py` | two dispatch branches |
| `lineage_bridge/ui/extraction.py` | `_run_openlineage_push`, `_run_watsonx_push` |
| `lineage_bridge/ui/sidebar/actions.py` | two publish rows in the sidebar |

We deliberately did not implement `build_node`, `enrich` or `build_url`. There
is nothing to read back and no console page to link to.

Neither provider is registered in `catalogs/__init__.py:_PROVIDERS`. That
registry is for `CATALOG_TABLE` nodes carrying a `catalog_type` discriminator.
Push-only providers stay out of it.

The watsonx provider supports SaaS authentication only. Cloud Pak for Data and
Software Hub would use `/icp4d-api/v1/authorize` with a username and API key
instead. We did not build that because nobody needs it yet.

## Which IBM product

The OpenLineage consumer is watsonx.data intelligence, the lineage engine
derived from Manta. It is not watsonx.governance. watsonx.data and
watsonx.data integration are producers only.

Pooja's instance is software as a service, hosted in Toronto. The console URL
gives you the region:

    https://ca-tor.dai.cloud.ibm.com/projects/<project-id>/manage/general

The API runs on a different host: `api.ca-tor.dai.cloud.ibm.com`. We confirmed
this live. POSTing to the API host without a token returns 401, while the
console host returns 404.

## What the watsonx API needs

Two endpoints:

    POST https://api.<region>.dai.cloud.ibm.com/gov_lineage/v2/lineage_events/openlineage
    POST https://api.<region>.dai.cloud.ibm.com/gov_lineage/v2/lineage_events/openlineage/batch

The batch endpoint takes an array body.

Authentication uses an IBM Cloud IAM bearer token, valid for one hour, exchanged
from a user API key. You create the key under Profile and settings, then User
API key:

    POST https://iam.cloud.ibm.com/identity/token
      grant_type=urn:ibm:params:oauth:grant-type:apikey&apikey=<key>

You do not need a project ID or a catalog ID. This confused us for a while.
Lineage is not stored in a project. The lineage repository is a single
account-level graph. A project only matters for the alternative route, uploading
a .zip metadata import, because an import is itself an asset and assets have to
live somewhere.

You also do not need to pre-create tables, assets, data source definitions or
mapping rules. Nodes are created from the events. Mapping rules only control how
nodes are labelled, and whether they merge with assets a scanner found
independently. That is irrelevant for our graph, which is self-contained.

Hard constraints we found:

- HTTP accepts run events only. Job and dataset events, the design-time ones, need the .zip metadata import flow, which is 5 API calls and project-scoped
- `runId` must be a valid UUID. Ours are UUID version 5, which is fine
- runtime lineage is shown for 30 days from the `eventTime` of the COMPLETE event
- runtime lineage cannot be re-imported, only deleted and pushed again. Repeated test pushes accumulate
- user-defined facets are not supported. Ours are `confluent_kafka` and `confluent_connector`, and the provider strips them
- `sourceCodeLocation` and `parentRun` are not supported either

Two useful debugging surfaces:

- the 400 response carries a `more_info` link to the instance's own live API reference at `/gov_lineage/v2/scanner-service/api/docs/static/index.html`. It needs a login, and it is more authoritative than the public docs
- in the watsonx interface, go to Data, then Data lineage, then Map lineage, then Map OpenLineage. It lists every event received as processed, failed, rejected or pending, with the error text for each

## What we proved by testing

The key authenticates as `poholkar@in.ibm.com`, account
`b4d9c2618fa94c37baa88cc396977c73`.

| layer | result |
|---|---|
| IAM token exchange | 200, bearer token, 3600 seconds |
| authorization for lineage write | passes, no 401 or 403, so permissions are fine |
| routing | correct host confirmed |
| payload validation | passes. Sending `{"nonsense":true}` returns 400 "The required property 'producer' is missing" |
| persistence | 500 "could not be persisted" |

The 500 happens on both the single and the batch endpoint, with fresh tokens,
and both before and after Pooja configured Cloud Object Storage.

Trace IDs for an IBM support ticket:

    c2209d6e-c0d5-4b93-9eda-3d1d4c22ecbc
    23a712ac-588a-4183-bb36-3d046ec3fa6e
    151f769a-9941-488f-8276-e4f9a8accf19

Three explanations remain open: the service plan does not include lineage, the
lineage repository is not provisioned on the instance, or there is a genuine
fault at IBM. We cannot tell these apart from outside. The next step is IBM
support, not more probing.

Verified locally against a real extracted graph: 5 events become 10 after the
START and COMPLETE pairs are built, and the facets kept are `job:sql`,
`job:documentation`, `ds:schema` and `ds:dataSource`. The two custom facets are
dropped. The push runs cleanly through
`run_push(PushRequest(provider="watsonx"), ...)` and reports the 500 in
`PushResult.errors`.

### Running the reproduction on its own

```bash
python3 our-work/watsonx_lineage_repro.py --api-key <IBM Cloud user API key> \
  --host api.ca-tor.dai.cloud.ibm.com
```

Add `--events our-work/sample_events.json` to send our real events instead of
the built-in sample one. The script needs only Python 3.9 and the standard
library, so it runs anywhere, including on an IBM engineer's laptop.

## Namespace handling

watsonx flattens the namespace into the object path and strips the scheme.
`confluent://env-871wjr/lkc-q283w0p` became `confluentenv-871wjrlkc-q283w0p`.

Harmless so far. If nodes render with awkward names once storage works, add
namespace rewriting in `catalogs/watsonx.py`. That is where DataZone and Google
do the same thing, and it is also how you would make `confluent://env-x/lkc-y`
resolve to assets watsonx already knows about, rather than a floating island of
new nodes.

---

# Building a demo environment from scratch

You need a Confluent Cloud account and an organization-level Cloud API key.
Create it from Home, then API keys, with the "My account" scope. A cluster key
will not work.

## 1. Install

```bash
git clone <this repo> lineage-bridge
cd lineage-bridge
uv venv && uv pip install -e ".[dev]"
```

Install the dev extra. Installing with `uv pip install -e .` alone leaves you
without pytest, and running `uv run pytest` later re-resolves the environment
and removes packages. Use `.venv/bin/python` directly, and `uvx ruff` for
linting.

## 2. Credentials

```bash
umask 077 && cat > .env <<'EOF'
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=<org-level key>
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=<org-level secret>
LINEAGE_BRIDGE_WATSONX_HOST=api.ca-tor.dai.cloud.ibm.com
LINEAGE_BRIDGE_WATSONX_API_KEY=<IBM Cloud user API key>
EOF
```

Check the Confluent key works before spending money:

```bash
set -a && . .env && set +a
curl -s -u "$LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY:$LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET" \
  https://api.confluent.cloud/org/v2/environments
```

## 3. Install Terraform

Any 1.5 or later release works. We ran the original build on 1.9.8 and have
since verified the configuration on 1.15.9.

On macOS:

```bash
brew install terraform
```

On Linux with no package manager, this installs it to `~/.local/bin` and checks
the download against HashiCorp's published checksum:

```bash
V=$(curl -s https://checkpoint-api.hashicorp.com/v1/check/terraform \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['current_version'])")
cd "$(mktemp -d)"
curl -sSLO "https://releases.hashicorp.com/terraform/${V}/terraform_${V}_linux_amd64.zip"
curl -sSLO "https://releases.hashicorp.com/terraform/${V}/terraform_${V}_SHA256SUMS"
grep linux_amd64.zip "terraform_${V}_SHA256SUMS" | sha256sum -c -
unzip -oq "terraform_${V}_linux_amd64.zip" terraform
install -m 755 terraform ~/.local/bin/terraform
```

Check `~/.local/bin` is on your PATH, then run `terraform version`.

## 4. Create the Confluent environment

The module in `our-work/terraform` reuses the repository's own shared
`infra/demos/modules/confluent-core`, so run it from inside the repository. The
module source path is relative.

```bash
cd our-work/terraform
terraform init -input=false
set -a && . ../../.env && set +a
export TF_VAR_confluent_cloud_api_key=$LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY
export TF_VAR_confluent_cloud_api_secret=$LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET
nohup terraform apply -input=false -auto-approve > apply.log 2>&1 &
```

Run it detached. A coding agent's shell usually caps commands at about 2
minutes, and a foreground apply gets killed part-way through. That happened to
us once and orphaned 3 API keys: created in Confluent, absent from state,
secrets lost, so they could not be imported. We had to delete them with
`DELETE https://api.confluent.cloud/iam/v2/api-keys/{id}`.

Expect 8 to 12 minutes. Confluent latency is the whole wait: role bindings take
about 90 seconds each, API keys about 2 minutes, connectors about 3 minutes.

It creates 19 resources: an environment, a Standard Kafka cluster, a service
account, 2 role bindings, 3 API keys, `orders` and `customers` topics with Avro
schemas, 2 DatagenSource connectors, a Flink compute pool and 4 Flink
statements.

The connectors fabricate records continuously. There is no source database.

Flink CTAS statements create real Kafka topics, not views. After apply the
cluster holds `lineage_bridge.orders_v2` and `.customers_v2` with 3 partitions
from datagen, plus `.enriched_orders` and `.order_stats` with 6 partitions from
Flink.

### Why this module exists

`infra/demos/uc/` and `infra/demos/glue/` fail `terraform init` without
Databricks and AWS credentials, because they provision the target catalog too.
Ours is about 40 lines: a provider block, one call to the shared module, and the
4 Flink statements copied from `uc/main.tf`. No second cloud account needed.

### What the demo does not cover

Both ends of the bridge. There is no source database, because datagen invents
the records, and no sink, because we skipped Tableflow, Glue and Unity Catalog
on purpose. What it exercises is the Kafka-internal middle. Bridging to external
systems, the part that makes the demo impressive, is untested here.

## 5. Add the Terraform outputs to .env

The extractor needs four more credential sets beyond the Cloud key:

```
LINEAGE_BRIDGE_SCHEMA_REGISTRY_ENDPOINT=<schema_registry_rest_endpoint>
LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_KEY / _SECRET
LINEAGE_BRIDGE_FLINK_API_KEY / _SECRET
LINEAGE_BRIDGE_KAFKA_API_KEY / _SECRET
```

The Kafka one is cluster-scoped. Without it the Kafka admin extractor returns
401 and consumer groups go missing. `outputs.tf` does not expose it, so read it
from `terraform.tfstate`, resource `confluent_api_key.kafka`.

## 6. Run the interface and push

```bash
setsid nohup .venv/bin/streamlit run lineage_bridge/ui/app.py \
  --server.address 127.0.0.1 --server.port 8501 --server.headless true \
  > /tmp/streamlit.log 2>&1 < /dev/null &
```

Do not use `make ui`. It first runs `scripts/ensure-cloud-key.sh`, which prompts
to auto-provision a Confluent key.

If you are on a remote machine, tunnel with `ssh -L 8501:127.0.0.1:8501 <host>`.
Then:

1. Pick the environment and run extraction.
2. Open the publish panel and select "Push to watsonx".

A typical run gives 13 nodes and 13 edges: 4 topics, 2 connectors, 2 Flink jobs,
4 schemas and 1 external dataset. That becomes 5 OpenLineage events.

Two interface quirks. Clicking a node sets a focus and collapses the view to its
neighbourhood, and "Clear focus" in the sidebar brings the rest back. "Hide
disconnected nodes" is on by default.

## Testing without a Confluent account

You can exercise the whole push path with no Confluent account at all. Two ways,
depending on what you want to test.

To test the push from the interface, start it and select "Load Demo Graph". That
loads the bundled fixture at `lineage_bridge/ui/static/sample_graph.json`, which
has 38 nodes and 39 edges. Set the watsonx variables in `.env`, then use the
publish panel as normal. We checked this offline: that fixture produces 38
watsonx events, 19 START and COMPLETE pairs, with the custom facets stripped and
every `runId` a valid UUID. The sidebar upload control takes the same format, a
LineageGraph JSON with `nodes` and `edges`, which is what the export button
produces.

To test watsonx ingestion alone, run the repro script against
`our-work/sample_events.json`. That file holds OpenLineage events, not a graph,
so it does not load into the interface. It is the output of a push, captured
from a real extraction:

```bash
python3 our-work/watsonx_lineage_repro.py --api-key <key> \
  --events our-work/sample_events.json
```

---

# Teardown

Do the Confluent side first. A Standard cluster bills by the hour even when
idle.

```bash
cd our-work/terraform
set -a && . ../../.env && set +a
export TF_VAR_confluent_cloud_api_key=$LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY
export TF_VAR_confluent_cloud_api_secret=$LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET
nohup terraform destroy -auto-approve > destroy.log 2>&1 &
```

Detached again, for the same timeout reason. Then check the environment is gone:

```bash
curl -s -u "$KEY:$SECRET" https://api.confluent.cloud/org/v2/environments
```

Stop the interface:

```bash
kill $(pgrep -f "streamlit run lineage_bridge")
```

`.env` holds live credentials. Keep it at mode 600 and never commit it. The
Confluent entries are dead once `terraform destroy` finishes. Rotate the IBM
user API key when testing is done.

To build it again, go back to step 4. Nothing else needs redoing, though you
need a fresh Confluent key because the old entries in `.env` no longer work. We
have run this loop twice from scratch, and a full rebuild takes about 12
minutes. Every ID changes each time, so the environment, cluster and Schema
Registry endpoint in `.env` all have to be rewritten from the new Terraform
outputs.

---

# Optional: Marquez as a local receiver

Skip this section by default. We used Marquez early on as a local stand-in while
the watsonx endpoint was unknown, and it is no longer needed. Pushing to watsonx
works, and pushing to any OpenLineage receiver works through
`catalogs/openlineage_http.py`.

It is still the fastest way to see the events rendered as a graph on your own
machine, with no cloud account and no IBM instance. Use it if you want to check
what LineageBridge produces before sending it anywhere.

```bash
docker network create lb-net
docker run -d --name lb-postgres --network lb-net \
  -e POSTGRES_USER=marquez -e POSTGRES_PASSWORD=marquez -e POSTGRES_DB=marquez \
  postgres:16
docker run -d --name lb-marquez --network lb-net \
  -p 127.0.0.1:5000:5000 -p 127.0.0.1:5051:5001 \
  -e POSTGRES_HOST=lb-postgres -e POSTGRES_PORT=5432 -e POSTGRES_DB=marquez \
  -e POSTGRES_USER=marquez -e POSTGRES_PASSWORD=marquez \
  marquezproject/marquez:latest
docker run -d --name lb-marquez-web --network lb-net -p 127.0.0.1:3010:3000 \
  -e MARQUEZ_HOST=lb-marquez -e MARQUEZ_PORT=5000 -e WEB_PORT=3000 \
  marquezproject/marquez-web:latest
```

We used unusual host ports because 3000, 5001, 5011 and 5432 were already taken
on that machine. Adjust to taste.

`marquez-web` exits with code 1 unless you set `WEB_PORT=3000`.

The API is on 127.0.0.1:5000, admin on 5051 and the interface on 3010. All bind
to loopback, so tunnel with `ssh -L 3010:127.0.0.1:3010 <host>`. There are no
volumes, so nothing survives `docker rm`.

Point LineageBridge at it, then use the "Push to OpenLineage" row in the publish
panel:

```
LINEAGE_BRIDGE_OPENLINEAGE_ENDPOINT=http://127.0.0.1:5000/api/v1/lineage
```

In the Marquez interface on port 3010, pick the namespace
`confluent://<env>/<cluster>`.

Marquez accepts our namespaces and our custom facets as-is, which watsonx does
not. That difference is worth remembering: a push that works against Marquez can
still be rejected by watsonx.

To remove it:

```bash
docker rm -f lb-marquez-web lb-marquez lb-postgres
docker network rm lb-net
docker rmi marquezproject/marquez-web marquezproject/marquez postgres:16
```
