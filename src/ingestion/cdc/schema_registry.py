"""CDC pipeline -- Redpanda Schema Registry client.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: August 2026

Purpose: register the CDC event schemas Debezium emits (one subject per topic)
and hold the compatibility level, so an incompatible change to an operational
table is rejected at registration time rather than silently landing. Talks the
Schema Registry REST API directly -- no extra client dependency.
"""

from __future__ import annotations

import json
from typing import Any

import requests

_JSON_SCHEMA = "JSON"
_TIMEOUT = (10, 30)


class SchemaRegistryError(RuntimeError):
    pass


class SchemaRegistry:
    def __init__(self, base_url: str, compatibility: str = "BACKWARD") -> None:
        self._base = base_url.rstrip("/")
        self._compat = compatibility

    def _url(self, path: str) -> str:
        return f"{self._base}{path}"

    def ping(self) -> bool:
        try:
            resp = requests.get(self._url("/subjects"), timeout=_TIMEOUT)
        except requests.RequestException:
            return False
        return resp.status_code == 200

    def set_global_compatibility(self) -> None:
        resp = requests.put(
            self._url("/config"),
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            data=json.dumps({"compatibility": self._compat}),
            timeout=_TIMEOUT,
        )
        if resp.status_code >= 300:
            raise SchemaRegistryError(f"set compatibility failed: {resp.text}")

    def set_subject_compatibility(self, subject: str) -> None:
        resp = requests.put(
            self._url(f"/config/{subject}"),
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            data=json.dumps({"compatibility": self._compat}),
            timeout=_TIMEOUT,
        )
        if resp.status_code >= 300:
            raise SchemaRegistryError(
                f"set compatibility for {subject} failed: {resp.text}"
            )

    def register(self, subject: str, schema: dict[str, Any]) -> int:
        """Register a JSON Schema for a subject. Returns the schema id.

        A schema that breaks the subject's compatibility level is refused by the
        registry with HTTP 409 -- surfaced here as SchemaRegistryError.
        """
        resp = requests.post(
            self._url(f"/subjects/{subject}/versions"),
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            data=json.dumps({"schemaType": _JSON_SCHEMA, "schema": json.dumps(schema)}),
            timeout=_TIMEOUT,
        )
        if resp.status_code == 409:
            raise SchemaRegistryError(
                f"{subject}: schema incompatible with {self._compat} policy"
            )
        if resp.status_code >= 300:
            raise SchemaRegistryError(f"{subject}: registration failed: {resp.text}")
        return int(resp.json()["id"])

    def latest(self, subject: str) -> dict[str, Any] | None:
        resp = requests.get(
            self._url(f"/subjects/{subject}/versions/latest"), timeout=_TIMEOUT
        )
        if resp.status_code == 404:
            return None
        if resp.status_code >= 300:
            raise SchemaRegistryError(f"{subject}: fetch failed: {resp.text}")
        return resp.json()
