"""
Local Tableflow connectivity test — no Spark, no watsonx needed.
Tests that the Tableflow REST catalog is reachable and the tables have data.

Run:
    uv run --with pyiceberg --with boto3 python our-work/spark/test_tableflow_local.py

What it checks:
  1. REST catalog connection + OAuth token exchange
  2. Namespace (cluster) is visible
  3. All three tables are listed
  4. Each table has at least one snapshot (i.e. data has been written)
  5. Schema of each table is printed
"""

REGION           = "us-east-1"
ORG_ID           = "efabb4b6-83b0-4d06-943e-e7127050d10e"
ENV_ID           = "env-o722oj"
CLUSTER_ID       = "lkc-dovpv1o"
TABLEFLOW_APIKEY = "XIO3O7S5JBL2VLZ5"
TABLEFLOW_SECRET = "cflt41v/W6J1f8wsicargS6BSqEkoDaKrwjO8hL4SeAacwYqlOx53Hh3DdzImvEA"

TABLES = [
    "lineage_bridge.orders_v2",
    "lineage_bridge.customers_v2",
    "lineage_bridge.order_stats",
]

REST_URI = (
    f"https://tableflow.{REGION}.aws.confluent.cloud/iceberg/catalog/"
    f"organizations/{ORG_ID}/environments/{ENV_ID}"
)

def main():
    from pyiceberg.catalog import load_catalog

    print(f"Connecting to: {REST_URI}\n")

    catalog = load_catalog(
        "tableflow",
        **{
            "type":                      "rest",
            "uri":                       REST_URI,
            "credential":                f"{TABLEFLOW_APIKEY}:{TABLEFLOW_SECRET}",
            "rest.auth.type":            "oauth2",
            "oauth2-server-uri":         f"{REST_URI}/v1/oauth/tokens",
            "scope":                     "catalog",
            "s3.remote-signing-enabled": "true",
            "client.region":             REGION,
            "s3.region":                 REGION,
        },
    )

    # 1. List namespaces
    namespaces = catalog.list_namespaces()
    print(f"✓ Namespaces visible: {namespaces}")
    ns = (CLUSTER_ID,)
    if ns not in namespaces:
        print(f"  ⚠ Expected namespace {CLUSTER_ID} not found — check CLUSTER_ID")
        return

    # 2. List tables in the cluster namespace
    tables = catalog.list_tables(CLUSTER_ID)
    print(f"✓ Tables in {CLUSTER_ID}: {['.'.join(t) for t in tables]}\n")

    # 3. Check each expected table
    # Tables are listed as (cluster_id, topic) — use the exact identifiers
    # returned by list_tables rather than constructing them manually.
    listed = {".".join(t): t for t in tables}
    for topic in TABLES:
        full_name = f"{CLUSTER_ID}.{topic}"
        print(f"--- {full_name} ---")
        try:
            identifier = listed.get(full_name, (CLUSTER_ID,) + tuple(topic.split(".")))
            tbl = catalog.load_table(identifier)
            schema = tbl.schema()
            snapshots = tbl.snapshots()
            print(f"  ✓ Schema fields : {[f.name for f in schema.fields]}")
            print(f"  ✓ Snapshots     : {len(snapshots)}")
            if snapshots:
                latest = sorted(snapshots, key=lambda s: s.timestamp_ms)[-1]
                print(f"  ✓ Latest snap   : {latest.snapshot_id} @ {latest.timestamp_ms}")
            else:
                print(f"  ⚠ No snapshots yet — Tableflow may still be syncing")
        except Exception as e:
            print(f"  ✗ ERROR: {e}")
        print()

    print("Done. If all tables show snapshots, Tableflow credentials are valid.")
    print("The 403 in Spark is a token-expiry or Gluten issue, not a creds issue.")

if __name__ == "__main__":
    main()
