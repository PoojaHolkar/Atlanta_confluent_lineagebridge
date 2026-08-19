# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""IBM watsonx.data intelligence lineage pusher.

Posts the extracted graph to the `gov_lineage` OpenLineage ingestion API of a
watsonx.data intelligence SaaS instance. Like `openlineage_http`, this is
push-only: there is no catalog to read back from, so `push_lineage` is the
only method of the protocol it implements.

Three things separate it from the generic OpenLineage pusher:

* Auth is an IBM Cloud IAM bearer token exchanged from a user API key. The
  token expires after an hour, which is longer than any push takes, so it is
  fetched once per push rather than cached.
* watsonx rejects user-defined facets, so facets are filtered to the set its
  documentation lists as supported.
* Only run events are accepted over HTTP, and they are emitted as START /
  COMPLETE pairs to match what other OpenLineage producers send.

Design-time (job) and dataset events need a .zip upload through a metadata
import job, which is a different flow and is not implemented here.
"""

from __future__ import annotations

import logging
import typing as tp

import httpx

from lineage_bridge.models.graph import LineageGraph, PushResult

logger = logging.getLogger(__name__)

ProgressCallback = tp.Callable[[str, str], None]

IAM_TOKEN_URL = "https://iam.cloud.ibm.com/identity/token"
_IAM_GRANT_TYPE = "urn:ibm:params:oauth:grant-type:apikey"

# Facets watsonx.data intelligence documents as supported. Anything else —
# notably our own `confluent_kafka` / `confluent_connector` — is a
# user-defined facet, which watsonx does not accept.
_JOB_FACETS = frozenset({"sql", "sourceCode", "documentation", "jobType", "ownership"})
_DATASET_FACETS = frozenset(
    {
        "schema",
        "columnLineage",
        "documentation",
        "dataSource",
        "datasetType",
        "ownership",
        "storage",
        "version",
        "tags",
        "dataQualityMetrics",
        "dataQualityAssertions",
    }
)
_RUN_FACETS = frozenset({"nominalTime"})


class WatsonxProvider:
    """Posts OpenLineage run events to a watsonx.data intelligence instance."""

    catalog_type: str = "WATSONX"

    def __init__(self, host: str | None = None, api_key: str | None = None) -> None:
        self._host = (host or "").removeprefix("https://").removesuffix("/") or None
        self._api_key = api_key

    async def push_lineage(
        self,
        graph: LineageGraph,
        *,
        on_progress: ProgressCallback | None = None,
        confluent_only: bool = False,
    ) -> PushResult:
        """Post every event derived from *graph* and return a `PushResult`."""
        result = PushResult()
        if not self._host or not self._api_key:
            result.errors.append("watsonx host and API key must both be configured")
            return result

        events = build_events(graph, confluent_only=confluent_only)
        if not events:
            return result

        url = f"https://{self._host}/gov_lineage/v2/lineage_events/openlineage/batch"
        if on_progress:
            on_progress("Push", f"Posting {len(events)} lineage event(s) to {self._host}")

        async with httpx.AsyncClient(timeout=60.0) as client:
            token = await self._fetch_token(client)
            response = await client.post(
                url,
                json=events,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
            )

        if response.is_success:
            result.tables_updated = len(events)
            return result

        logger.warning("watsonx push rejected: %s %s", response.status_code, response.text[:300])
        result.errors.append(f"HTTP {response.status_code} {response.text[:300]}")
        return result

    async def _fetch_token(self, client: httpx.AsyncClient) -> str:
        """Exchange the user API key for an IAM bearer token."""
        response = await client.post(
            IAM_TOKEN_URL,
            data={"grant_type": _IAM_GRANT_TYPE, "apikey": self._api_key},
            headers={"Accept": "application/json"},
        )
        response.raise_for_status()
        return response.json()["access_token"]


def build_events(graph: LineageGraph, *, confluent_only: bool = False) -> list[dict[str, tp.Any]]:
    """Serialize *graph* as watsonx-compatible START / COMPLETE run event pairs.

    The translator emits one COMPLETE event per job. watsonx describes run
    events as state changes, so each is paired with a matching START carrying
    the same run id. Events with neither inputs nor outputs carry no lineage
    and are dropped.
    """
    from lineage_bridge.openlineage.translator import graph_to_events

    events: list[dict[str, tp.Any]] = []
    for event in graph_to_events(graph, confluent_only=confluent_only):
        if not event.inputs and not event.outputs:
            continue
        complete = event.model_dump(mode="json", exclude_none=True, by_alias=True)
        _filter_facets(complete)
        events.append({**complete, "eventType": "START"})
        events.append(complete)
    return events


def _filter_facets(event: dict[str, tp.Any]) -> None:
    """Drop facets watsonx does not support, in place."""
    _keep(event.get("job"), _JOB_FACETS)
    _keep(event.get("run"), _RUN_FACETS)
    for dataset in [*event.get("inputs", []), *event.get("outputs", [])]:
        _keep(dataset, _DATASET_FACETS)


def _keep(owner: dict[str, tp.Any] | None, allowed: frozenset[str]) -> None:
    """Restrict ``owner["facets"]`` to *allowed*, removing the key if empty."""
    if not owner or "facets" not in owner:
        return
    kept = {name: value for name, value in owner["facets"].items() if name in allowed}
    if kept:
        owner["facets"] = kept
    else:
        del owner["facets"]
