# Pushing Confluent lineage into watsonx

This folder holds everything we worked out while adding an OpenLineage push path
to LineageBridge, so that lineage extracted from Confluent Cloud can be sent to
IBM watsonx.data intelligence.

Read this file first. It is the only document here.

## What is in this folder

| file | what it is |
|---|---|
| `notes.md` | this file: findings, runbook, teardown |
| `watsonx_lineage_repro.py` | standalone script that reproduces the watsonx ingestion failure. uv fetches its requests dependency |
| `sample_events.json` | a real batch of OpenLineage events we extracted, useful for testing a push without a Confluent account |
| `terraform/` | a Confluent-only demo environment, so you can build a graph to push |

The code changes themselves are not here. They live in the main package, listed
under "what we added" below.

## Current watsonx handoff

No valid event has reached storage. The reproduction proves that IAM
authentication, lineage permissions, routing and schema validation work. It
tests 2 independent watsonx accounts with the same generated payload.

Run the EU-DE account with:

```bash
uv run --with requests --env-file our-work/.env \
  our-work/watsonx_lineage_repro.py
```

Run Pooja's Toronto account with:

```bash
uv run --with requests --env-file our-work/.env \
  our-work/watsonx_lineage_repro.py --profile ca-tor
```

The ignored `our-work/.env` contains both test profiles. The script sends an
intentional invalid request first. HTTP 400 and `PASS` are expected for this
control. Only failures in the valid single and batch checks matter.

Current results from 20 August 2026:

| Profile | Account setup known to us | Valid single | Valid batch |
|---|---|---:|---:|
| EU-DE | Interface says ready, but COS credentials API returns HTTP 503 `WKC server error` | HTTP 503 `WKC server error` | HTTP 503 `WKC server error` |
| Toronto | COS credentials API returns a configured bucket | HTTP 500 `InputMetadata ... could not be persisted` | HTTP 500 `InputMetadata ... could not be persisted` |

Do not describe Toronto as going further through the pipeline. The response
wording suggests a persistence problem, but it does not prove the order of
internal operations. The leading hypotheses are:

- Toronto has a stored Data Lineage Cloud Object Storage record, but the bucket,
  stored API key or IBM persistence service might not work
- EU-DE has completed the visible storage setup, but the service cannot even
  retrieve that setup because its Watson Knowledge Catalog dependency returns
  HTTP 503
- the shared payload is unlikely to be the cause because both schema validators
  accept it and the accounts return different server errors

WKC means Watson Knowledge Catalog in this context. It does not mean Knowledge
Center.

The documented Processed OpenLineage events view is not visible in the EU-DE
interface. We also could not find the documented OpenLineage `.zip` route in a
project metadata-import workflow. Activity Tracker was not already configured,
so it cannot provide historical details for these failures. Do not repeat these
interface searches unless the account features change.

The script prints the useful server correlation values for every failed valid
request: JSON trace, response date, `x-global-transaction-id`, `server-timing`
and `CF-RAY`. These identifiers do not expose internal error details to the
customer. IBM can use them to find the service logs.

The next investigation should focus on the EU-DE account's internal Data
Lineage and Watson Knowledge Catalog provisioning. For Toronto, verify whether
the stored lineage COS credential can still write to its configured bucket.
Creating more caller-side payload variants is unlikely to help either account.

### Final EU-DE prerequisite audit

The final audit on 20 August 2026 found no missing documented prerequisite.
The account passes every check that we can perform outside IBM's internal
services:

- IBM lists OpenLineage import as available in Frankfurt
- the trial plan includes Data Lineage
- the API key belongs to a user, as the HTTP ingestion documentation requires
- the user has the Manager service role, which includes Manage data lineage
- the Data Lineage settings page says that lineage is ready
- the default catalog and Platform assets catalog exist in EU-DE and are active
- both catalog storage records are healthy
- the dedicated lineage bucket exists and is readable through the COS API
- the lineage bucket has no firewall or unusual storage configuration
- the account has all 38 IBM-provided OpenLineage namespace mappings

The correct regional Catalog API host is
`https://api.eu-de.dataplatform.cloud.ibm.com`. Checks against the unqualified
`api.dataplatform.cloud.ibm.com` reach the Dallas deployment and show no EU-DE
catalogs. This is expected and is not the lineage failure. On the EU-DE host:

```text
default catalog:         01a01ecf-24a1-74bb-98a1-a3179e428df8, active
Platform assets catalog: 01a01edf-0981-750a-a990-a2146696fd0a, active
Catalog heartbeat:       HTTP 200, status ok
```

The dedicated lineage bucket is also healthy when accessed directly:

```text
bucket: cloudobjectstoragebucket-lineage-5adf6770982e444e9330389383dd4e
COS instance: 0053a967-b35c-4dae-b0e6-3e5f18f4017e
list buckets: HTTP 200
list lineage bucket objects: HTTP 200
```

We also sent a valid event using only built-in namespace mappings:

```text
job namespace: airflow://lineage-bridge-demo
dataset namespace: kafka://lineage-bridge-demo
result: HTTP 503 WKC server error
trace: 41d7ff76-c3c8-4e3d-82fd-8c84aaeadb8b
x-global-transaction-id: c5516fb2-7654-4e63-93fd-01608dbeb50d
```

This rules out the synthetic namespace as the cause. Mapping rules affect how
accepted events are represented and connected. They do not explain why the
lineage service cannot retrieve its own COS configuration.

The latest normal reproduction produced these IDs:

```text
lineage COS configuration GET:
  trace: 09d8d556-8456-4814-af1e-5056c5d1439e
  x-global-transaction-id: 52761d8e-510b-4880-88f8-01b8a3d3e39c
valid single event:
  trace: 2c218b43-aa6f-42d3-a952-89aa7d6915e7
valid batch:
  trace: e28d1a98-4f4f-43f8-9897-486e62579b7e
```

The remaining fault is inside the Data Lineage service's WKC integration. WKC
itself is reachable and the EU-DE catalogs are healthy, but
`GET /gov_lineage/v2/cos_bucket_credentials` still returns HTTP 503. The likely
fault is a missing or broken internal tenant link or credential record.

Do not spend more time on projects, storage delegation, service-to-service
authorization, payload facets, event timing or custom mappings. A project is
needed for `.zip` metadata import, not HTTP ingestion. Service-to-service
authorization is needed when watsonx.data is the producer, not for this custom
HTTP caller.

We attempted the final destructive experiment with the user's explicit
approval. DELETE did not confirm a change. It returned HTTP 503 before the
client could safely recreate the record:

```text
DELETE /gov_lineage/v2/cos_bucket_credentials
trace: e3a832a5-435a-4426-a0e8-f3063992a6ae
x-global-transaction-id: 64191a5a-91e9-4047-a9e1-8cf4fd618e2d
```

Because this is a disposable trial account, we also made one direct CREATE
request with the existing bucket, COS instance and test API key. It failed with
the same WKC error:

```text
POST /gov_lineage/v2/cos_bucket_credentials
trace: 9bd6a463-37dd-43d4-8fb4-df0c1e326baa
x-global-transaction-id: fdc4c4fd-cf98-4c9f-b6f6-0d2ba1ec153f
```

IBM did not confirm either deletion or creation. The COS bucket itself was not
deleted. A final normal repro still returned HTTP 503 for the credential GET,
single event and batch. Its latest traces were:

```text
credential GET: 87f6d135-ca5f-4020-8507-6f180252efea
single event:   ed8a0c8d-2ed1-41bf-b3f4-1b7b9f62c65b
batch:          7b81c254-16bd-4ec8-b4b9-7249d4fe30a8
```

Do not repeat the credential reset. GET, DELETE and CREATE all fail at the same
WKC dependency. The tenant cannot repair this record through the public API.

## EU-DE investigation log from 20 August 2026

The EU-DE test has a different failure from the earlier Toronto test. A valid
event returns HTTP 503 `WKC server error`. The event passes schema validation.

The reproduction script's HTTP 400 is an intentional control request. Step 2
sends `{"nonsense": true}` and expects the validator to reject it. The script
now labels this response as `PASS`. The valid requests in steps 3 and 4 are the
ones that matter.

Verified results for account `2a3073fe65c54a178927f4107e0be294`:

- IAM authentication succeeds
- the single valid event returns HTTP 503 `WKC server error`
- the valid batch returns HTTP 503 `WKC server error`
- lineage statistics, activity and namespace mapping requests return HTTP 200
- the watsonx.data intelligence service instance is active in EU-DE
- the Cloud Object Storage service instance is active
- the account has a `Sample trial experience catalog`
- `GET /v2/catalogs/default` returns HTTP 404 `Default catalog not found`

The missing default catalog might have caused the WKC dependency failure. We
created one to test this theory. The dedicated Cloud Object Storage bucket
`lineage-bridge-default-catalog-2a3073fe65c5` now exists in `eu-de-smart` under
COS instance `0053a967-b35c-4dae-b0e6-3e5f18f4017e`.

The API responses established these requirements:

- the SaaS environment rejects the documented internal `assetfiles` bucket type
- the request must provide a real Cloud Object Storage bucket
- the request must provide read and write credentials for that bucket
- the existing `WDP-Catalog-ManagerV2` credential has the COS `Manager` role
- the catalog service still reports that the new bucket does not exist when we
  provide its HMAC keys, endpoint, location and full resource CRN

The HMAC credential can read the new bucket. We copied the working sample
catalog's bucket structure and created the default catalog successfully:

    default catalog id: 01a01ecf-24a1-74bb-98a1-a3179e428df8

Creating the default catalog did not fix ingestion. The single endpoint, batch
endpoint and a `START`-only event all still return HTTP 503 `WKC server error`.
This proves the failure happens before Watsonx stores a valid event. It is not
triggered by `COMPLETE` event processing.

IAM and plan checks also pass:

- the API key owner has account-wide `Administrator` and `Manager` roles
- the watsonx.data intelligence instance is active
- the instance uses the `trial` plan, which includes Data Lineage

The user completed Data Lineage setup in the EU-DE interface on 20 August 2026.
The settings page now says `Lineage is ready to use`. It shows a configured
Cloud Object Storage bucket and a created Platform asset catalog.

We reran the unchanged reproduction script immediately after setup. The valid
single and batch requests still returned HTTP 503 `WKC server error`. Their
support IDs were `1603736b-07e8-4bf2-bfdb-d93425441f99` and
`a5103da5-3096-4769-aa7a-9aba9655ff49`.

The visible account-level setup is complete. Waiting, refreshing the interface
and rerunning the script did not change the result. The remaining likely cause
is a broken or incomplete Watson Knowledge Catalog dependency inside IBM.

The live IBM scanner API exposes a read-only storage check:

```text
GET /gov_lineage/v2/cos_bucket_credentials
```

This endpoint returns the lineage bucket, COS endpoint and resource instance ID
without returning the stored API key. The reproduction script runs this check
before sending events.

The EU-DE check returns HTTP 503 `WKC server error`, despite the interface saying
that lineage is ready. The 20 August 2026 check produced:

```text
trace: 8a3fccb0-6a26-4fbd-8d43-da0eecbecae2
x-global-transaction-id: be7667ff-9d8e-4823-926b-30de2fb7e62e
```

The Toronto check returns HTTP 200 and this stored configuration:

```text
bucket: cos665003iqm3bucket-lineage-8bc38e7833354a06b1af2fc4df01cee0
endpoint: s3.ca-tor.cloud-object-storage.appdomain.cloud
COS resource instance: b5887ceb-1674-4394-8fbc-6e962ace32f7
```

The missing URL scheme is allowed. IBM's live schema uses the same host-only
form in its field example. Toronto therefore has a storage configuration
record, but that does not prove its saved API key can still write to the bucket.

The ignored `our-work/.env` contains the EU-DE host and test API key under the
same `LINEAGE_BRIDGE_WATSONX_*` names used by the application. Run the repro
with:

```bash
uv run --with requests --env-file our-work/.env \
  our-work/watsonx_lineage_repro.py
```

The script labels the intentional HTTP 400 as `PASS`. It labels valid request
failures as `FAIL` and exits with status 1 when ingestion does not work.

The script now uses `requests` instead of a custom `urllib` wrapper. uv fetches
the dependency at runtime through `--with requests`. A live run of this version
produced the same HTTP 503 result. The latest support IDs are
`dfb6044c-a50b-4dc3-9917-6dd3acef1cde` and
`d2d84cad-5219-4c2a-a858-1dfaf860a27d`.

The ignored `our-work/.env` also contains the older Toronto host and Pooja's
test API key. Select that account with:

```bash
uv run --with requests --env-file our-work/.env \
  our-work/watsonx_lineage_repro.py --profile ca-tor
```

We sent the same event shape to both accounts on 20 August 2026. Both API keys
authenticate, and both services reject the intentional schema control as
expected. The valid requests fail at different backend stages:

| Profile | Valid single | Valid batch | Error |
|---|---:|---:|---|
| EU-DE test account | HTTP 503 | HTTP 503 | `WKC server error` |
| Toronto Pooja account | HTTP 500 | HTTP 500 | `InputMetadata ... could not be persisted` |

Both accounts have server-side failures with different messages. The wording
is consistent with an unavailable Watson Knowledge Catalog dependency in
EU-DE and a storage configuration problem in Toronto. It does not prove that
one request progressed further than the other.

The HTTP response headers provide more identifiers for IBM support. The script
now prints `x-global-transaction-id`, `Date`, `server-timing` and `CF-RAY` for a
failed check. The JSON trace remains the main support ID.

IBM's SaaS documentation describes 2 OpenLineage views that are not obvious in
the interface:

- the processed-events dashboard is under Data, Data lineage, Map lineage, then
  Map OpenLineage
- manual `.zip` upload is not on the Data Lineage page; it is an external input
  in a project's metadata import job

Neither documented interface route was visible in the EU-DE account. The valid
requests are not stored, so the processed-events dashboard might not list them
even if that view becomes available.

### IBM SDK and API findings

IBM does not publish a watsonx.data intelligence SDK for writing lineage. The
IBM Manta Data Lineage API is a REST API. Its OpenLineage write operation is the
same `/gov_lineage/v2/lineage_events/openlineage` endpoint used by the repro.

IBM's reference to Java and JWT clients means upstream OpenLineage producer
libraries. Those libraries serialize an event and send it to the same HTTP
endpoint. Trying one would change the caller but not the IBM storage path. It
would only be useful if IBM rejected our payload, which it does not.

The IBM Manta API also exposes configuration and graph-reading operations. The
new COS credentials check is the most useful one for this failure. Do not try
the API's POST or DELETE credential operations without an explicit decision:
they change the tenant-wide lineage storage configuration.

IBM's Manta API documentation states that HTTP 500 and 503 responses are logged
as critical service failures. The current errors are therefore the class of
failure IBM expects to diagnose from its internal logs, not client-side schema
errors.

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
uv run --with requests --env-file our-work/.env \
  our-work/watsonx_lineage_repro.py
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
uv run --with requests --env-file our-work/.env \
  our-work/watsonx_lineage_repro.py \
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
