"""Metadata filters, the same for the dense, keyword and graph retrievers.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

A filter maps a metadata key to one value or a list of values. A chunk passes when
every key holds one of its values. Only the keys listed here are accepted, so a
key never reaches a query as free text. Library only.
"""

from __future__ import annotations

COMMON_KEYS = ("kind", "ecosystem", "source")
WIKIPEDIA_KEYS = ("title", "revision_year")


def allowed_keys(corpus_id: str) -> tuple[str, ...]:
    return COMMON_KEYS + (WIKIPEDIA_KEYS if corpus_id == "wikipedia" else ())


def normalise(filters: dict | None, corpus_id: str) -> dict[str, list]:
    """Each value becomes a sorted list; an unknown key or an empty value is an error."""
    result: dict[str, list] = {}
    for key, value in (filters or {}).items():
        if key not in allowed_keys(corpus_id):
            raise ValueError(
                f"filter key {key!r} is not allowed for {corpus_id}; "
                f"use one of {allowed_keys(corpus_id)}"
            )
        values = list(value) if isinstance(value, (list, tuple, set)) else [value]
        if not values or any(v is None or v == "" for v in values):
            raise ValueError(f"filter {key!r} has no value")
        result[key] = sorted(set(values), key=str)
    return result


def to_where(filters: dict[str, list]) -> dict | None:
    """The ChromaDB `where` clause of a normalised filter."""
    parts = [
        {key: values[0]} if len(values) == 1 else {key: {"$in": values}}
        for key, values in sorted(filters.items())
    ]
    if not parts:
        return None
    return parts[0] if len(parts) == 1 else {"$and": parts}


def matches(metadata: dict, filters: dict[str, list]) -> bool:
    return all(metadata.get(key) in values for key, values in filters.items())
