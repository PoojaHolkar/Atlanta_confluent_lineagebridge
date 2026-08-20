#!/usr/bin/env python3
# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Test OpenLineage HTTP ingestion on watsonx.data intelligence.

The script runs five checks and prints what each one proves:

  1. Exchange the user API key for an IAM bearer token.
  2. Verify that lineage Cloud Object Storage credentials exist for the tenant.
  3. POST a deliberately malformed body. HTTP 400 is a successful control: it
     shows that the request reached the schema validator.
  4. POST a minimal valid run event, expecting HTTP 201.
  5. POST the same event to the batch endpoint, expecting HTTP 200.

Docs for the endpoint under test:
https://dataplatform.cloud.ibm.com/docs/content/wsj/lineage/openlineage-integration.html

Usage:
    uv run --with requests --env-file our-work/.env \
        our-work/watsonx_lineage_repro.py
    uv run --with requests --env-file our-work/.env \
        our-work/watsonx_lineage_repro.py --profile ca-tor

Requires Python 3.9+. uv fetches requests into a temporary environment.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import typing as tp
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests

IAM_TOKEN_URL = "https://iam.cloud.ibm.com/identity/token"
IAM_GRANT_TYPE = "urn:ibm:params:oauth:grant-type:apikey"
SINGLE_PATH = "/gov_lineage/v2/lineage_events/openlineage"
BATCH_PATH = "/gov_lineage/v2/lineage_events/openlineage/batch"
COS_CREDENTIALS_PATH = "/gov_lineage/v2/cos_bucket_credentials"
PROFILE_ENV_PREFIXES = {
    "eu-de": "LINEAGE_BRIDGE_WATSONX",
    "ca-tor": "LINEAGE_BRIDGE_WATSONX_CA_TOR",
}
DIAGNOSTIC_HEADERS = (
    "Date",
    "x-global-transaction-id",
    "server-timing",
    "CF-RAY",
)


def fetch_token(api_key: str) -> str:
    """Exchange an IBM Cloud user API key for an IAM bearer token."""
    response = requests.post(
        IAM_TOKEN_URL,
        data={"grant_type": IAM_GRANT_TYPE, "apikey": api_key},
        headers={"Accept": "application/json"},
        timeout=60,
    )
    if response.status_code != 200:
        raise SystemExit(
            f"IAM token exchange failed: HTTP {response.status_code}\n{response.text}"
        )
    return response.json()["access_token"]


def token_identity(token: str) -> str:
    """Return 'subject / account' from the token's claims, for the report header."""
    payload = token.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    claims = json.loads(base64.urlsafe_b64decode(payload))
    return f"{claims.get('sub')} / account {claims.get('account', {}).get('bss')}"


def sample_event() -> dict[str, tp.Any]:
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


def print_diagnostic_headers(response: requests.Response) -> None:
    """Print identifiers that IBM can use to find the request in service logs."""
    print("diagnostic response headers:")
    for header in DIAGNOSTIC_HEADERS:
        if value := response.headers.get(header):
            print(f"  {header}: {value}")


def check_storage_configuration(url: str, token: str) -> int:
    """Report whether the tenant has lineage Cloud Object Storage credentials."""
    print("\n--- 2. lineage storage configuration ---")
    print(f"GET {url}")
    print("expected: HTTP 200 — lineage storage credentials exist")

    response = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    result = "PASS" if response.status_code == 200 else "FAIL"
    print(f"actual:   HTTP {response.status_code}")
    print(f"result:   {result}")
    print(f"response: {response.text[:400]}")
    if result == "FAIL":
        print_diagnostic_headers(response)
    return response.status_code


def check(
    name: str,
    expectation: str,
    expected_statuses: set[int],
    url: str,
    payload: tp.Any,
    token: str,
) -> tuple[int, str]:
    """Run one POST and print its outcome against what was expected."""
    print(f"\n--- {name} ---")
    print(f"POST {url}")
    print(f"expected: {expectation}")

    response = requests.post(
        url,
        json=payload,
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    status = response.status_code
    text = response.text
    print(f"actual:   HTTP {status}")
    result = "PASS" if status in expected_statuses else "FAIL"
    print(f"result:   {result}")
    print(f"response: {text[:400]}")
    if result == "FAIL":
        print_diagnostic_headers(response)
    return status, text


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--profile",
        choices=PROFILE_ENV_PREFIXES,
        default="eu-de",
        help="credential profile from the env file (default: %(default)s)",
    )
    parser.add_argument(
        "--api-key",
        help="IBM Cloud user API key; overrides the selected profile",
    )
    parser.add_argument(
        "--host",
        help="watsonx API host; overrides the selected profile",
    )
    parser.add_argument(
        "--events",
        help="optional JSON file holding an array of real OpenLineage events to send instead",
    )
    args = parser.parse_args()
    env_prefix = PROFILE_ENV_PREFIXES[args.profile]
    api_key = args.api_key or os.environ.get(f"{env_prefix}_API_KEY")
    host_value = args.host or os.environ.get(f"{env_prefix}_HOST")
    if not api_key or not host_value:
        parser.error(f"profile {args.profile!r} is missing its host or API key")

    host = host_value.removeprefix("https://").removesuffix("/")
    single_url = f"https://{host}{SINGLE_PATH}"
    batch_url = f"https://{host}{BATCH_PATH}"
    cos_credentials_url = f"https://{host}{COS_CREDENTIALS_PATH}"

    print("=" * 72)
    print("watsonx.data intelligence — OpenLineage HTTP ingestion reproduction")
    print("=" * 72)
    print(f"profile: {args.profile}")
    print(f"host: {host}")

    token = fetch_token(api_key)
    print("\n--- 1. authentication ---")
    print("IAM token exchange: HTTP 200")
    print(f"authenticated as:   {token_identity(token)}")
    print("proves:             the API key is valid")

    storage_status = check_storage_configuration(cos_credentials_url, token)

    control_status, _ = check(
        "3. intentional invalid request (control)",
        "HTTP 400 — this is a passing schema-validation control",
        {400},
        single_url,
        {"nonsense": True},
        token,
    )

    events = [sample_event()]
    if args.events:
        with Path(args.events).open() as handle:
            events = json.load(handle)
        print(f"\n(using {len(events)} event(s) from {args.events})")

    status, text = check(
        "4. valid event, single endpoint",
        "HTTP 201 — the event is accepted and stored",
        {201},
        single_url,
        events[0],
        token,
    )
    batch_status, batch_text = check(
        "5. valid event, batch endpoint",
        "HTTP 200 — the events are accepted and stored",
        {200},
        batch_url,
        events,
        token,
    )

    print("\n" + "=" * 72)
    print("SUMMARY")
    print("=" * 72)

    if (
        storage_status == 200
        and control_status == 400
        and status == 201
        and batch_status == 200
    ):
        print("Both endpoints accepted the events. Ingestion is working.")
        print("Confirm they appear under Data > Data lineage > Map lineage > Map OpenLineage.")
        return

    print("Working:  IAM authentication, authorization for lineage write, routing,")
    print("          and payload validation — every layer the caller controls.")
    print(f"Storage:  credentials endpoint returned HTTP {storage_status}.")
    print(f"Failing:  the events are not stored — single {status}, batch {batch_status}.")
    print(f"Trace ids for IBM support: {trace_id(text)}, {trace_id(batch_text)}")
    print(
        "\nQuestion for the watsonx team: is OpenLineage HTTP ingestion enabled and\n"
        "provisioned on this instance and service plan, and does the lineage\n"
        "repository behind /gov_lineage have working storage?"
    )
    raise SystemExit(1)


if __name__ == "__main__":
    main()
