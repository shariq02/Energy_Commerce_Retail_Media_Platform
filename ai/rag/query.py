"""Deterministic query processing: no model, no network.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The text is normalised (NFKC, one space between words). Unit ids and metric keys in
the text are detected against the corpus. A glossary term in the text adds the
words of its definition. Detected identifiers and glossary words only add keyword
terms; the dense query stays the normalised user text. Library only.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

from ai.embeddings import keyword_index as ki

_UNIT_ID = re.compile(
    r"\b(?:metric|contract|rule|glossary|governance|pipeline)(?:\.[a-z0-9_]+)+"
)
_TERM = re.compile(r"`([^`]+)`")
MIN_WORD = 3


@dataclass(frozen=True)
class ProcessedQuery:
    original: str
    text: str
    terms: tuple[str, ...]
    identifiers: tuple[str, ...]
    expansions: tuple[str, ...]

    def as_dict(self) -> dict:
        return {
            "text": self.text,
            "terms": list(self.terms),
            "identifiers": list(self.identifiers),
            "expansions": list(self.expansions),
        }


def normalise(text: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", text).split())


def glossary_entries(documents: list[str]) -> list[tuple[re.Pattern, tuple[str, ...]]]:
    """(term pattern, definition words) of every glossary line `- `term`: text`."""
    entries = []
    for document in documents:
        for line in document.splitlines():
            head, colon, definition = line.partition(": ")
            if not (colon and head.startswith("- ")):
                continue
            clause = re.split(r"[.,;(]", definition, maxsplit=1)[0]
            words = tuple(w for w in ki.tokenize(clause) if len(w) >= MIN_WORD)
            for term in _TERM.findall(head):
                regex = re.escape(term.lower()).replace(r"\*", r"\w*")
                entries.append((re.compile(regex), words))
    return entries


def metric_keys(unit_ids) -> dict[str, str]:
    """Metric key (the id without its prefix) to unit id."""
    return {u.split(".", 1)[1]: u for u in unit_ids if u.startswith("metric.")}


def process(
    query: str,
    unit_ids: set[str],
    glossary: list,
    max_expansion: int,
) -> ProcessedQuery:
    text = normalise(query)
    words = ki.tokenize(text)
    keys = metric_keys(unit_ids)
    found: list[str] = []
    for match in _UNIT_ID.finditer(text.lower()):
        if match.group(0) in unit_ids:
            found.append(match.group(0))
    found += [keys[w] for w in words if w in keys]
    identifiers = tuple(dict.fromkeys(found))
    terms = list(dict.fromkeys(words))
    for identifier in identifiers:
        terms += [
            p for p in ki.tokenize(identifier.replace("_", " ")) if p not in terms
        ]
    expansions: list[str] = []
    for pattern, definition in glossary:
        if any(pattern.fullmatch(word) for word in words):
            expansions += [w for w in definition if w not in terms + expansions]
    expansions = expansions[:max_expansion]
    return ProcessedQuery(
        query, text, tuple(terms + expansions), identifiers, tuple(expansions)
    )
