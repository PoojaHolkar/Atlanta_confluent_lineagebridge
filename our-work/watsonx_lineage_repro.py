#!/usr/bin/env python3
# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Minimal reproduction of an OpenLineage ingestion failure on watsonx.data intelligence.

Posting a valid OpenLineage run event to the documented HTTP ingestion endpoint
returns HTTP 500 "could not be persisted". Authentication, authorization,
routing and payload validation all succeed — the event fails at the point where
the service writes it to storage.

The script runs four checks and prints what each one proves:

  1. Exchange the user API key for an IAM bearer token.
  2. POST a deliberately malformed body, expecting 400. This shows the
     validation layer is reached and returns precise schema errors.
  3. POST a minimal valid run event, expecting 201/202. This is the failure.
  4. POST the same event to the batch endpoint, to show it is not
     route-specific.

Docs for the endpoint under test:
https://dataplatform.cloud.ibm.com/docs/content/wsj/lineage/openlineage-integration.html

Usage:
    python3 watsonx_lineage_repro.py --api-key <IBM Cloud user API key> \\
        --host api.ca-tor.dai.cloud.ibm.com

Requires only the Python 3.9+ standard library, so it runs anywhere without
installing anything.
"""

from __future__ import annotations

import argparse
import base64
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone

# The edge in front of these hosts rejects the default Python-urllib agent
# with a Cloudflare 403 (error code 1010) before the request reaches IBM.
USER_AGENT = "watsonx-lineage-repro/1.0"

IAM_TOKEN_URL = "https://iam.cloud.ibm.com/identity/token"
IAM_GRANT_TYPE = "urn:ibm:params:oauth:grant-type:apikey"
SINGLE_PATH = "/gov_lineage/v2/lineage_events/openlineage"
BATCH_PATH = "/gov_lineage/v2/lineage_events/openlineage/batch"


def post(url: str, body: bytes, headers: dict[str, str]) -> tuple[int, str]:
    """POST *body* and return (status, response text), treating errors as results."""
    request = urllib.request.Request(
        url, data=body, headers={"User-Agent": USER_AGENT, **headers}, method="POST"
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status, response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode("utf-8", "replace")


def fetch_token(api_key: str) -> str:
    """Exchange an IBM Cloud user API key for an IAM bearer token."""
    form = f"grant_type={urllib.parse.quote(IAM_GRANT_TYPE)}&apikey={urllib.parse.quote(api_key)}"
    status, text = post(
        IAM_TOKEN_URL,
        form.encode(),
        {"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
    )
    if status != 200:
        sys.exit(f"IAM token exchange failed: HTTP {status}\n{text}")
    return json.loads(text)["access_token"]


def token_identity(token: str) -> str:
    """Return 'subject / account' from the token's claims, for the report header."""
    payload = token.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    claims = json.loads(base64.urlsafe_b64decode(payload))
    return f"{claims.get('sub')} / account {claims.get('account', {}).get('bss')}"


def sample_event() -> dict:
    """A minimal OpenLineage run event: one job, one input, one output."""
    namespace = "watsonx-repro"
    return {
        "eventType": "COMPLETE",
        "eventTime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "producer": "https://github.com/takabayashi/lineage-bridge",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        "run": {"runId": str(uuid.uuid4())},
        "job": {"namespace": namespace, "name": "repro-job"},
        "inputs": [{"namespace": namespace, "name": "repro_input"}],
        "outputs": [{"namespace": namespace, "name": "repro_output"}],
    }


def trace_id(text: str) -> str:
    """Pull the support/trace id out of an error body, for an IBM support ticket."""
    try:
        return json.loads(text).get("trace", "-")
    except json.JSONDecodeError:
        return "-"


def check(name: str, expectation: str, url: str, payload, token: str) -> tuple[int, str]:
    """Run one POST and print its outcome against what was expected."""
    print(f"\n--- {name} ---")
    print(f"POST {url}")
    print(f"expected: {expectation}")

    status, text = post(
        url,
        json.dumps(payload).encode(),
        {"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    print(f"actual:   HTTP {status}")
    print(f"response: {text[:400]}")
    return status, text


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-key", required=True, help="IBM Cloud user API key")
    parser.add_argument(
        "--host",
        default="api.ca-tor.dai.cloud.ibm.com",
        help="watsonx.data intelligence API host (default: %(default)s)",
    )
    parser.add_argument(
        "--events",
        help="optional JSON file holding an array of real OpenLineage events to send instead",
    )
    args = parser.parse_args()

    host = args.host.removeprefix("https://").removesuffix("/")
    single_url = f"https://{host}{SINGLE_PATH}"
    batch_url = f"https://{host}{BATCH_PATH}"

    print("=" * 72)
    print("watsonx.data intelligence — OpenLineage HTTP ingestion reproduction")
    print("=" * 72)
    print(f"host: {host}")

    token = fetch_token(args.api_key)
    print("\n--- 1. authentication ---")
    print("IAM token exchange: HTTP 200")
    print(f"authenticated as:   {token_identity(token)}")
    print("proves:             the API key is valid")

    control_status, _ = check(
        "2. validation reachable (control)",
        "HTTP 400 — a schema error, proving the request reaches the validator",
        single_url,
        {"nonsense": True},
        token,
    )
    if control_status == 400:
        print("result:   as expected. Routing and authorization succeed, and the")
        print("          service parses our body and returns a precise schema error.")
    else:
        print("result:   UNEXPECTED. The request is not reaching the validator.")

    events = [sample_event()]
    if args.events:
        with open(args.events) as handle:
            events = json.load(handle)
        print(f"\n(using {len(events)} event(s) from {args.events})")

    status, text = check(
        "3. valid event, single endpoint",
        "HTTP 201/202 — the event is accepted and stored",
        single_url,
        events[0],
        token,
    )
    batch_status, batch_text = check(
        "4. valid event, batch endpoint",
        "HTTP 201/202 — the events are accepted and stored",
        batch_url,
        events,
        token,
    )

    print("\n" + "=" * 72)
    print("SUMMARY")
    print("=" * 72)

    if status < 300 and batch_status < 300:
        print("Both endpoints accepted the events. Ingestion is working.")
        print("Confirm they appear under Data > Data lineage > Map lineage > Map OpenLineage.")
        return

    print("Working:  IAM authentication, authorization for lineage write, routing,")
    print("          and payload validation — every layer the caller controls.")
    print(f"Failing:  the events are not stored — single {status}, batch {batch_status}.")
    print(f"Trace ids for IBM support: {trace_id(text)}, {trace_id(batch_text)}")
    print(
        "\nQuestion for the watsonx team: is OpenLineage HTTP ingestion enabled and\n"
        "provisioned on this instance and service plan, and does the lineage\n"
        "repository behind /gov_lineage have working storage?"
    )


if __name__ == "__main__":
    main()
