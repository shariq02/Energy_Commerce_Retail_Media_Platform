# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # KNOWLEDGE -- WIKIPEDIA TEXT LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** the selection terms and the wikitext cleaning and section
# MAGIC splitting used by the Wikipedia notebooks. Pulled in with
# MAGIC `%run ./_wikipedia_text`. Definitions only -- no side effects at import.

# COMMAND ----------

# DBTITLE 1,Imports
import hashlib
import re
from collections import namedtuple
from pathlib import Path

import yaml

# COMMAND ----------

# DBTITLE 1,Constants
LEAD_NAME = "Lead"
# An inline section heading such as "== History ==" inside one-line text.
HEADING_PATTERN = r"\s={2,}\s*([^=\n]{1,100}?)\s*={2,}\s"
_HEADING = re.compile(HEADING_PATTERN)
_REDIRECT = re.compile(r"(?i)^\s*#redirect")
_ECOSYSTEM = re.compile(r"^[a-z_]+$")
_CLEAN_STEPS = (
    (re.compile(r"(?s)<ref[^>/]*>.*?</ref>"), ""),
    (re.compile(r"<ref[^>]*/>"), ""),
    (re.compile(r"\{\{[^{}]*\}\}"), ""),
    (re.compile(r"\{\{[^{}]*\}\}"), ""),
    (re.compile(r"\{\{[^{}]*\}\}"), ""),
    (re.compile(r"(?i)\[\[(?:file|image|category):[^\]]*\]\]"), ""),
    (re.compile(r"\[\[(?:[^\]|]*\|)?([^\]]*)\]\]"), r"\1"),
    (re.compile(r"\[https?://[^\s\]]+\s?([^\]]*)\]"), r"\1"),
    (re.compile(r"<[^>]+>"), ""),
    (re.compile(r"https?://\S+"), ""),
    (re.compile(r"'{2,5}"), ""),
    (re.compile(r"\s+"), " "),
)

# COMMAND ----------

# DBTITLE 1,Terms type
Terms = namedtuple("Terms", ["version", "cap_per_term", "pairs", "file_hash"])


# COMMAND ----------

# DBTITLE 1,Load the selection terms


def load_terms(path):
    # scripts/knowledge/wikipedia_prepare.py has the same function for the local
    # commands; both hash the file with line endings normalised.
    raw = Path(path).read_bytes().replace(b"\r\n", b"\n")
    data = yaml.safe_load(raw.decode("utf-8")) or {}
    version, cap = data.get("version"), data.get("cap_per_term")
    if not isinstance(version, int) or not isinstance(cap, int) or cap < 1:
        raise ValueError(f"{path}: version and cap_per_term must be positive integers")
    groups = data.get("terms")
    if not isinstance(groups, dict) or not groups:
        raise ValueError(f"{path}: terms must be a mapping of ecosystem to term list")
    pairs, seen = [], set()
    for ecosystem, terms in groups.items():
        if not _ECOSYSTEM.match(str(ecosystem)) or not isinstance(terms, list):
            raise ValueError(f"{path}: bad ecosystem entry {ecosystem!r}")
        for term in terms:
            key = str(term).strip().lower()
            if not key or key in seen:
                raise ValueError(f"{path}: empty or repeated term {term!r}")
            seen.add(key)
            pairs.append((str(ecosystem), str(term).strip()))
    return Terms(version, cap, tuple(pairs), hashlib.sha256(raw).hexdigest())


# COMMAND ----------

# DBTITLE 1,Term pattern


def term_pattern(term):
    # Whole word, any case; valid in Python and in Spark rlike.
    return r"(?i)\b" + re.escape(term) + r"\b"


# COMMAND ----------

# DBTITLE 1,Clean wikitext


def clean_wikitext(text):
    for pattern, replacement in _CLEAN_STEPS:
        text = pattern.sub(replacement, text)
    return text.strip()


# COMMAND ----------

# DBTITLE 1,Redirect check


def is_redirect(text):
    return bool(_REDIRECT.match(text))


# COMMAND ----------

# DBTITLE 1,Split into sections


def split_sections(text):
    # Split at the inline headings; the lead comes first; empty sections are dropped.
    matches = list(_HEADING.finditer(text))
    bounds = [(LEAD_NAME, 0, matches[0].start() if matches else len(text))]
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        bounds.append((clean_wikitext(match.group(1)), match.end(), end))
    sections = []
    for name, start, end in bounds:
        body = clean_wikitext(text[start:end])
        if body:
            sections.append({"index": len(sections), "section": name, "text": body})
    return sections
