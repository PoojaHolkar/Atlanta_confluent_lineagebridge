# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Generic OpenLineage HTTP pusher.

Posts the extracted graph as OpenLineage events to any endpoint that speaks
the OpenLineage receiver protocol (Marquez, and catalogs that expose an
OpenLineage ingestion URL). Unlike the catalog providers in this package
this is push-only: there is no catalog to read metadata back from and no
console UI to deep-link into, so it implements `push_lineage` alone.
"""

from __future__ import annotations

import logging
import typing as tp

import httpx

from lineage_bridge.models.graph import LineageGraph, PushResult

logger = logging.getLogger(__name__)

ProgressCallback = tp.Callable[[str, str], None]


class OpenLineageHTTPProvider:
    """Posts OpenLineage events to a configured HTTP endpoint."""

    catalog_type: str = "OPENLINEAGE_HTTP"

    def __init__(self, endpoint: str | None = None, auth_token: str | None = None) -> None:
        self._endpoint = endpoint
        self._auth_token = auth_token

    async def push_lineage(
        self,
        graph: LineageGraph,
        *,
        on_progress: ProgressCallback | None = None,
        confluent_only: bool = False,
    ) -> PushResult:
        """Post every event derived from *graph* and return a `PushResult`."""
        from lineage_bridge.openlineage.translator import graph_to_events

        result = PushResult()
        if not self._endpoint:
            result.errors.append("No OpenLineage endpoint configured")
            return result

        events = [
            e
            for e in graph_to_events(graph, confluent_only=confluent_only)
            if e.inputs or e.outputs
        ]
        if not events:
            return result

        if on_progress:
            on_progress("Push", f"Posting {len(events)} lineage event(s) to {self._endpoint}")

        headers = {"Content-Type": "application/json"}
        if self._auth_token:
            headers["Authorization"] = f"Bearer {self._auth_token}"

        async with httpx.AsyncClient(timeout=30.0, headers=headers) as client:
            for event in events:
                payload = event.model_dump(mode="json", exclude_none=True, by_alias=True)
                response = await client.post(self._endpoint, json=payload)
                if response.is_success:
                    result.tables_updated += 1
                    continue
                logger.warning(
                    "OpenLineage push rejected %s: %s %s",
                    event.job.name,
                    response.status_code,
                    response.text[:200],
                )
                result.errors.append(
                    f"{event.job.name}: HTTP {response.status_code} {response.text[:200]}"
                )

        return result
