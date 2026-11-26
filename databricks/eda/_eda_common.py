# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # EDA SHARED LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Shared profiling plumbing (repo-root discovery, the
# MAGIC `src/schemas/profiling/<source>.md` writer, figure helpers) and the
# MAGIC reusable data-quality / modelling-risk checks used by every source EDA
# MAGIC notebook, plus the helpers of the Samples Volume sources, article text and
# MAGIC link graphs, and the genomics Gold tables. Pulled in with
# MAGIC `%run ../_eda_common`. Definitions only -- no side effects at import; the
# MAGIC caller owns `spark` / `dbutils`.

# COMMAND ----------

# DBTITLE 1,Imports
import contextlib
import datetime as _dt
import itertools as _it
import math
import os as _os
import re as _re

import matplotlib.pyplot as plt
from pyspark.sql import functions as F

# Do NOT force a backend here -- Databricks installs its own inline backend and
# forcing "Agg" makes plt.show() a no-op, so figures stop rendering in the
# notebook (the PNGs still save, but the cell shows nothing).

# A figure axis stays readable to about this many categorical bars; beyond it
# the label band is unreadable, so the helpers cap and say how many were hidden.
MAX_XTICKS = 40
# Truncate a single category label to this width in a figure (full text stays
# in the exported markdown tables/lists).
LABEL_CLIP = 28

# Germany-focused source data: timestamps arrive in a mix of ISO and German
# wall-clock formats. Order matters -- the first format that parses a row wins.
GERMAN_TS_FORMATS = (
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd HH:mm",
    "yyyy-MM-dd",
    "dd.MM.yyyy HH:mm:ss",
    "dd.MM.yyyy HH:mm",
    "dd.MM.yyyy-HH:mm",
    "dd.MM.yyyy",
    "yyyyMMddHH",
    "yyyyMMdd",
)

# Continental Germany bounding box (lat_min, lat_max, lon_min, lon_max), a
# generous envelope including the North/Baltic Sea stations and the Zugspitze.
DE_BBOX = (47.0, 55.2, 5.7, 15.1)

# The layered modelling-risk checklist every EDA notebook answers, in a fixed
# order so a reader can confirm each was considered. A notebook supplies one
# entry per category -- an explicit "not applicable -- <why>" still counts.
LEAKAGE_CATEGORIES = (
    "Grain / grain drift",
    "Join multiplication (1:N / M:N expansion)",
    "Target contamination",
    "Temporal / post-event leakage",
    "Proxy leakage",
    "Split / entity leakage",
    "Historical-reference (point-in-time) leakage",
    "Survivorship / coverage bias",
    "Missingness leakage",
    "Duplicate-event leakage",
    "Target / feature temporal misalignment",
    "Unit / sign / circular-feature leakage",
    "Data-generation-process leakage",
    "Class / label instability",
    "Label availability lag",
    "Source / version / regime change",
    "Sample-vs-full divergence",
)

# Sources read in place from the Databricks Samples Volume.
SAMPLES_VOLUME_ROOT = "/Volumes/samples/databricks/datasets"
SUPPORT_HINTS = ("readme", "license", "licence", "description", "citation", "notice")
DELIMITERS = {"\t": "tab", ";": "semicolon", ",": "comma", "|": "pipe"}
SEP_CHOICES = ("\t", ";", ",", "|")

# Article text and link-graph sources.
# Words per section above which a section probably exceeds a 256-token chunk.
WORDS_PER_CHUNK_PROXY = 190
QUANTILE_PROBS = (0.01, 0.25, 0.5, 0.75, 0.95, 0.99)
# An inline section heading such as "== History ==" inside one-line text.
HEADING_PATTERN = r"\s={2,}[^=\n]{1,100}?={2,}\s"
CLEAN_STEPS = (
    (r"(?s)<ref[^>/]*>.*?</ref>", ""),
    (r"<ref[^>]*/>", ""),
    (r"\{\{[^{}]*\}\}", ""),
    (r"\{\{[^{}]*\}\}", ""),
    (r"\{\{[^{}]*\}\}", ""),
    (r"\[\[(?:[^\]|]*\|)?([^\]]*)\]\]", "$1"),
    (r"\[https?://[^\s\]]+\s?([^\]]*)\]", "$1"),
    (r"<[^>]+>", ""),
    (r"https?://\S+", ""),
    (r"'{2,5}", ""),
    (r"\s+", " "),
)
MARKUP_PATTERNS = {
    "newline": r"\n",
    "inline_heading": HEADING_PATTERN,
    "wiki_link": r"\[\[",
    "template_brace": r"\{\{",
    "html_tag": r"<[a-zA-Z/][^>]*>",
    "url": r"https?://",
    "redirect": r"(?i)^\s*#redirect",
}

# Genomics Gold tables.
PROVENANCE_COLUMNS = ("source_system", "ecosystem", "_gold_loaded_at", "_gold_run_id")
NUMERIC_DTYPES = ("int", "bigint", "smallint", "tinyint", "double", "float")
KEY_SUFFIXES = ("_key", "_id")
KEY_CANDIDATES = 4
HIGH_NULL_RATE = 0.5
FLAG_SEPARATION = 0.9
LEAK_CORRELATION = 0.9
PAIR_COLUMNS = 30
MISSING_GAP = 0.2
TEXT_ENUM_MAX = 20
NEAR_UNIQUE = 0.9
LIST_LIMIT = 15
GROUP_EDGES = ((1, "1"), (2, "2"), (3, "3 to 5"), (6, "6 or more"))

# COMMAND ----------

# DBTITLE 1,Repo-root discovery + profiling export path


def _repo_root():
    p = _os.path.abspath(_os.getcwd())
    for _ in range(12):
        if _os.path.isdir(_os.path.join(p, "src", "schemas")) and _os.path.isdir(
            _os.path.join(p, "databricks", "eda")
        ):
            return p
        if _os.path.dirname(p) == p:
            break
        p = _os.path.dirname(p)
    with contextlib.suppress(Exception):
        wp = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        i = wp.rfind("/databricks/eda/")
        if i > 0:
            for cand in (wp[:i], "/Workspace" + wp[:i]):
                if _os.path.isdir(_os.path.join(cand, "src", "schemas")):
                    return cand
    raise RuntimeError(
        "repo root not found -- run from <repo>/databricks/eda/<folder>/"
    )


def _profiling_dir():
    d = _os.path.join(_repo_root(), "src", "schemas", "profiling")
    _os.makedirs(d, exist_ok=True)
    return d


# COMMAND ----------

# DBTITLE 1,Small formatting + column helpers


def qcol(name):
    # Column reference that survives names with dots / spaces / hyphens -- German
    # government CSV headers carry them (e.g. dwd_station_geography."Geogr.Breite",
    # dwd_parameter_unit."Zusatz-Info"). A bare F.col("a.b") is read as struct
    # access and fails to resolve.
    return F.col("`" + str(name).replace("`", "``") + "`")


def _de_number(s):
    # Normalise a possibly German-formatted number WITHOUT regex lookaround --
    # Photon's regex engine (RE2) has no lookahead/lookbehind, and a lookaround
    # pattern de-photonises the whole operator (a 40x slowdown on a large agg).
    # Rule: if the value contains a comma, '.' is a thousands separator and ','
    # is the decimal point; otherwise leave it (already dot-decimal / integer).
    has_comma = F.instr(s, ",") > 0
    de = F.regexp_replace(F.regexp_replace(s, r"\.", ""), ",", ".")
    return F.when(has_comma, de).otherwise(s)


def safe_num(colname):
    # ANSI-safe, NULL-safe string -> double. A bare `.cast("double")` THROWS
    # CAST_INVALID_INPUT under ANSI mode (the default on this runtime) whenever a
    # row holds a blank or non-numeric string -- Bronze is all-string, so that is
    # a real hazard. `when(rlike, ...)` short-circuits so the cast only runs on
    # values that are already valid numbers.
    s = _de_number(F.trim(qcol(colname).cast("string")))
    return F.when(s.rlike(r"^-?\d+(\.\d+)?$"), s.cast("double"))


def dup_key_composition(df, key_cols, hash_cols=None, conflict_cols=None):
    # For rows that share `key_cols`: dup_groups (keys with >1 row), and of those
    # how many are byte-identical repeats vs carry a differing value.
    #
    # Uses one row-hash column + approx_count_distinct (a mergeable HLL sketch,
    # single pass). Exact `countDistinct(hash(*cols))` INSIDE a groupBy triggers
    # an Expand + double aggregation -- two full-table shuffles on a
    # high-cardinality key, the slowest pattern in these notebooks. HLL is exact
    # for the tiny cardinalities that matter here (1 vs >1).
    hash_cols = hash_cols or df.columns
    conflict_cols = conflict_cols or hash_cols
    sel = [
        *[qcol(c) for c in key_cols],
        F.xxhash64(*[qcol(c) for c in hash_cols]).alias("__h"),
    ]
    if conflict_cols != hash_cols:
        sel.append(F.xxhash64(*[qcol(c) for c in conflict_cols]).alias("__c"))
    h = df.select(*sel)
    _cv = "__c" if conflict_cols != hash_cols else "__h"
    per_key = h.groupBy(*[qcol(c) for c in key_cols]).agg(
        F.count(F.lit(1)).alias("n"),
        F.approx_count_distinct("__h").alias("row_variants"),
        F.approx_count_distinct(_cv).alias("conflict_variants"),
    )
    r = (
        per_key.agg(
            F.count(F.lit(1)).alias("distinct_keys"),
            F.sum((F.col("n") > 1).cast("long")).alias("dup_groups"),
            F.sum(((F.col("n") > 1) & (F.col("row_variants") <= 1)).cast("long")).alias(
                "identical"
            ),
            F.sum(
                ((F.col("n") > 1) & (F.col("conflict_variants") > 1)).cast("long")
            ).alias("conflicting"),
        )
        .first()
        .asDict()
    )
    return {
        k: (r[k] or 0)
        for k in ("distinct_keys", "dup_groups", "identical", "conflicting")
    }


def fmt_pairs(pairs, n=25):
    # Render (label, value) pairs as markdown list lines, capped at n with a
    # "... (N more)" tail so the profiling .md never carries a 1000-row dump.
    items = list(pairs)
    out = [f"- {lbl}: {val}" for lbl, val in items[:n]]
    if len(items) > n:
        out.append(f"- ... ({len(items) - n} more)")
    return "\n".join(out)


def para(*parts):
    # Join sentence fragments into one string. Lets the notebooks keep prose as
    # short, comma-separated single-line literals -- no implicit string
    # concatenation inside a list/tuple (ISC004) and nothing to re-wrap by hand.
    return " ".join(str(p).strip() for p in parts if p is not None and str(p).strip())


def _clip(s, n=LABEL_CLIP):
    s = str(s)
    return s if len(s) <= n else s[: n - 1] + "…"


def _gini(values):
    xs = sorted(float(v) for v in values)
    n = len(xs)
    s = sum(xs)
    if n == 0 or s == 0:
        return 0.0
    cum = sum((i + 1) * x for i, x in enumerate(xs))
    return round((2 * cum) / (n * s) - (n + 1) / n, 4)


# COMMAND ----------

# DBTITLE 1,Figure helpers -- never write a blank / overlong-label figure


def _apply_xlabels(ax, labels, rot):
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(
        [_clip(x) for x in labels],
        rotation=rot,
        ha="right" if rot else "center",
        fontsize=7,
    )


def _save_and_show(fig, filename=None):
    # Render the figure inline and close it; a figure is never written to a file,
    # so no data reaches the repository (`filename` is accepted and ignored). Prefer Databricks' display() -- it
    # serialises the figure to PNG itself, so it renders regardless of the
    # matplotlib backend (plt.show() is a silent no-op if the session's backend is
    # non-interactive, e.g. left on "Agg" by an earlier run in the same session).
    try:
        display(fig)
    except NameError:
        plt.show()
    plt.close(fig)


def barplot(
    pairs,
    title,
    xlabel,
    ylabel="count",
    rot=0,
    figsize=(10, 4),
    logy=False,
    filename=None,
):
    # Returns True only if a figure with data was shown. `filename` only labels the
    # "no data" message: a figure is shown in the notebook and never stored.
    pairs = [p for p in pairs if p is not None]
    if not pairs:
        print(f"  barplot: no data -> {filename} (figure not written)")
        return False
    hidden = 0
    if len(pairs) > MAX_XTICKS:
        hidden = len(pairs) - MAX_XTICKS
        pairs = pairs[:MAX_XTICKS]
    fig, ax = plt.subplots(figsize=figsize)
    ax.bar(range(len(pairs)), [p[1] for p in pairs])
    if logy:
        ax.set_yscale("log")
    _apply_xlabels(ax, [p[0] for p in pairs], rot)
    ax.set_title(title + (f"  (+{hidden} more not shown)" if hidden else ""))
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    fig.tight_layout()
    _save_and_show(fig)
    return True


def _facet_grid(items, suptitle, filename, ncols=3, panel=(4.6, 3.2)):
    items = [(str(k), draw) for k, draw in items if draw is not None]
    if not items:
        print(f"  _facet_grid: no data -> {filename} (figure not written)")
        return False
    ncols = min(ncols, len(items))
    nrows = -(-len(items) // ncols)
    fig, axes = plt.subplots(
        nrows, ncols, figsize=(panel[0] * ncols, panel[1] * nrows), squeeze=False
    )
    flat = list(axes.flatten())
    for ax, (title, draw) in zip(flat, items):
        draw(ax)
        ax.set_title(title, fontsize=9)
        ax.tick_params(labelsize=7)
    for ax in flat[len(items) :]:
        ax.set_visible(False)
    fig.suptitle(suptitle)
    fig.tight_layout()
    _save_and_show(fig)
    return True


def facet_bars(groups, suptitle, filename, rot=45, ncols=3, logy=False):
    def _mk(pairs):
        pairs = [p for p in pairs if p is not None]
        if not pairs:
            return None
        hidden = max(0, len(pairs) - MAX_XTICKS)
        pairs = pairs[:MAX_XTICKS]

        def draw(ax):
            ax.bar(range(len(pairs)), [p[1] for p in pairs])
            _apply_xlabels(ax, [p[0] for p in pairs], rot)
            if logy:
                ax.set_yscale("log")
            if hidden:
                ax.set_xlabel(f"+{hidden} more not shown", fontsize=7)

        return draw

    src = groups.items() if hasattr(groups, "items") else groups
    return _facet_grid([(k, _mk(list(v))) for k, v in src], suptitle, filename, ncols)


# COMMAND ----------

# DBTITLE 1,Profiling-export writer (src/schemas/profiling/<source>.md)


def write_profiling(source, notebook_key, section_title, blocks, figures=None):
    # One <source>.md per source; each notebook owns one marker-delimited
    # `## ` section, re-run replaces its own, others preserved, order by key.
    # `figures` is accepted and ignored: figures are shown in the notebook only.
    d = _profiling_dir()
    md = _os.path.join(d, source + ".md")
    lines = [f"<!-- BEGIN {source}:{notebook_key} -->", f"## {section_title}", ""]
    for heading, body in blocks:
        if body is None or str(body).strip() == "":
            continue
        lines += [f"### {heading}", "", str(body).rstrip(), ""]
    lines.append(f"<!-- END {source}:{notebook_key} -->")
    block = "\n".join(lines)
    existing = ""
    if _os.path.exists(md):
        with open(md, encoding="utf-8") as fh:
            existing = fh.read()
    pat = _re.compile(
        r"<!-- BEGIN "
        + _re.escape(source)
        + r":([\w.\-]+) -->.*?<!-- END "
        + _re.escape(source)
        + r":\1 -->",
        _re.DOTALL,
    )
    kept = {mm.group(1): mm.group(0) for mm in pat.finditer(existing)}
    kept[notebook_key] = block
    intro = (
        "_Auto-generated by the EDA notebooks (`databricks/eda/`). One "
        "`## ` section per notebook; re-running a notebook replaces its own section, "
        "other sections are preserved._"
    )
    header = f"# {source.upper()} EDA PROFILE\n\n{intro}\n\n"
    body = "\n\n".join(kept[k] for k in sorted(kept))
    out = header + body + "\n"
    tmp = md + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(out)
    _os.replace(tmp, md)
    print(f"profiling export -> {md}  ('{notebook_key}', {len(kept)} section(s))")


def ml_readiness_block(entries):
    # entries: list of (category, text). Renders one bullet per category in
    # LEAKAGE_CATEGORIES order so every notebook's coverage of the checklist is
    # auditable; a category with no entry is shown as an explicit gap.
    by_cat = {}
    for cat, text in entries:
        by_cat.setdefault(cat, []).append(text)
    out = []
    for cat in LEAKAGE_CATEGORIES:
        for t in by_cat.get(cat, []):
            out.append(f"- **{cat}:** {t}")
        if cat not in by_cat:
            out.append(f"- **{cat}:** not evaluated in this notebook.")
    for cat, texts in by_cat.items():
        if cat not in LEAKAGE_CATEGORIES:
            for t in texts:
                out.append(f"- **{cat}:** {t}")
    return "\n".join(out)


# COMMAND ----------

# DBTITLE 1,Key / uniqueness -- exact, not HLL estimate


def exact_uniqueness(df, cols):
    # Exact distinct count, null count and distinct/row ratio for a handful of
    # key-candidate columns in ONE pass. approx_count_distinct is an HLL
    # estimate that routinely lands a few percent either side of the true value
    # (and above the row count), so it cannot decide primary-key uniqueness.
    cols = [c for c in cols if c in df.columns]
    if not cols:
        return {}
    aggs = [F.count(F.lit(1)).alias("__n")]
    for c in cols:
        aggs += [
            F.countDistinct(qcol(c)).alias(c + "__d"),
            F.sum(qcol(c).isNull().cast("long")).alias(c + "__nulls"),
        ]
    r = df.agg(*aggs).first().asDict()
    n = r["__n"] or 0
    return {
        c: {
            "rows": n,
            "distinct": r[c + "__d"],
            "nulls": r[c + "__nulls"],
            "ratio": round(r[c + "__d"] / n, 6) if n else 0.0,
            "unique": bool(n) and r[c + "__d"] == n and r[c + "__nulls"] == 0,
        }
        for c in cols
    }


def pick_entity_key(uniq, candidates, prefer=()):
    # Given exact_uniqueness() output, choose the own-entity key: highest
    # distinct/row ratio, then `prefer` name order, then shortest name.
    cand = [c for c in candidates if c in uniq]
    if not cand:
        return None, None
    order = {name.lower(): i for i, name in enumerate(prefer)}
    ranked = sorted(
        cand,
        key=lambda c: (-uniq[c]["ratio"], order.get(c.lower(), len(prefer)), len(c)),
    )
    return ranked[0], uniq[ranked[0]]


# COMMAND ----------

# DBTITLE 1,Referential integrity + interpretation


# COMMAND ----------

# DBTITLE 1,Timestamp / timezone / granularity semantics


def parse_ts_multi(colname, formats=GERMAN_TS_FORMATS):
    s = qcol(colname).cast("string")
    expr = F.try_to_timestamp(s)
    for fmt in formats:
        expr = F.coalesce(expr, F.try_to_timestamp(s, F.lit(fmt)))
    return expr


def timestamp_semantics(
    df, colname, formats=GERMAN_TS_FORMATS, valid_from="1990-01-01", tz="unspecified"
):
    # Multi-format parse + evidence: parse yield, per-format contribution,
    # observed range, implausible (pre-`valid_from` / future) rows, sub-day
    # granularity, and a timezone/DST note. One aggregation pass for the
    # summary + one for the per-format counts + a tiny sample of unparsed text.
    s = qcol(colname).cast("string")
    present = s.isNotNull() & (F.trim(s) != "")
    parsed = parse_ts_multi(colname, formats)
    lo = F.lit(valid_from).cast("timestamp")
    future = F.col("__p") > F.expr("current_timestamp() + INTERVAL 2 DAYS")
    summ = (
        df.select(
            present.cast("long").alias("__present"),
            parsed.alias("__p"),
        )
        .agg(
            F.sum("__present").alias("present"),
            F.sum(F.col("__p").isNotNull().cast("long")).alias("parsed"),
            F.min("__p").alias("min_ts"),
            F.max("__p").alias("max_ts"),
            F.sum((F.col("__p") < lo).cast("long")).alias("before_valid"),
            F.sum(future.cast("long")).alias("future"),
            F.sum(
                (
                    (F.hour("__p") != 0)
                    | (F.minute("__p") != 0)
                    | (F.second("__p") != 0)
                ).cast("long")
            ).alias("has_time"),
            F.countDistinct(F.date_format("__p", "HH:mm:ss")).alias("distinct_tod"),
        )
        .first()
        .asDict()
    )
    fmt_aggs = [
        F.sum(F.try_to_timestamp(s, F.lit(fmt)).isNotNull().cast("long")).alias(f"f{i}")
        for i, fmt in enumerate(formats)
    ]
    fr = df.agg(*fmt_aggs).first().asDict()
    per_format = {formats[i]: fr[f"f{i}"] for i in range(len(formats)) if fr[f"f{i}"]}
    unparsed_sample = [
        r[0] for r in df.where(present & parsed.isNull()).select(s).limit(8).collect()
    ]
    p = summ["present"] or 0
    parsed_n = summ["parsed"] or 0
    yield_ = parsed_n / p if p else 0.0
    lines = []
    lines.append(
        f"`{colname}`: parsed {parsed_n}/{p} non-empty values ({yield_:.1%}); "
        f"formats matched: { {k: v for k, v in per_format.items()} }."
    )
    if unparsed_sample:
        lines.append(f"unparsed samples: {unparsed_sample}")
    lines.append(
        f"observed range {summ['min_ts']} .. {summ['max_ts']}; "
        f"rows before {valid_from}: {summ['before_valid']}; future-dated (> now+2d): "
        f"{summ['future']}."
    )
    gran = (
        f"sub-daily ({summ['distinct_tod']} distinct times of day)"
        if summ["has_time"]
        else "date-only (no time-of-day component)"
    )
    lines.append(f"granularity: {gran}; source timezone: {tz}.")
    if summ["has_time"] and tz not in ("UTC", "unspecified"):
        lines.append(
            f"timezone `{tz}` is a wall-clock zone with DST -- the spring/autumn transition "
            "hours are ambiguous/missing; convert to UTC on a documented rule before any "
            "hourly join or resampling."
        )
    return {
        "yield": round(yield_, 4),
        "parsed": parsed_n,
        "present": p,
        "per_format": per_format,
        "min_ts": str(summ["min_ts"]),
        "max_ts": str(summ["max_ts"]),
        "before_valid": summ["before_valid"],
        "future": summ["future"],
        "has_time": bool(summ["has_time"]),
        "distinct_tod": summ["distinct_tod"],
        "unparsed_sample": unparsed_sample,
        "lines": lines,
    }


# COMMAND ----------

# DBTITLE 1,Numeric parseability -- stricter than "looks numeric"


# COMMAND ----------

# DBTITLE 1,Value plausibility + sign / mirror-column checks


# COMMAND ----------

# DBTITLE 1,Fixed-step continuity from an independent calendar


_STEP_SECONDS = {
    "1min": 60,
    "5min": 300,
    "15min": 900,
    "quarterhour": 900,
    "30min": 1800,
    "1h": 3600,
    "hour": 3600,
    "day": 86400,
}


def _step_seconds(step):
    return _STEP_SECONDS.get(step, step if isinstance(step, int) else None)


def continuity_grid(df, ts_col, step, entity_col=None, grid_start=None, grid_end=None):
    # Coverage of a fixed-step series measured against an INDEPENDENT expected
    # grid: expected = floor((max_ts - min_ts) / step) + 1, NOT the observed
    # distinct-timestamp count (which makes coverage tautologically 100%).
    # `step` is a label / seconds int, OR a {entity_value: label|seconds} dict
    # when one call covers several fixed steps (e.g. a table holding 1min +
    # 15min + 1h partitions). grid_start/grid_end (ISO strings) measure against
    # a known collection window instead of the series' own first/last row.
    if isinstance(step, dict):
        if entity_col is None:
            raise ValueError("a per-entity step dict needs entity_col")
        step_map = {str(k): _step_seconds(v) for k, v in step.items()}
        step_s_col = F.lit(None).cast("long")
        for k, v in step_map.items():
            step_s_col = F.when(F.col("k") == F.lit(k), F.lit(v)).otherwise(step_s_col)
    else:
        s = _step_seconds(step)
        if s is None:
            raise ValueError(f"unknown step: {step!r}")
        step_map = None
        step_s_col = F.lit(s)
    t = (
        parse_ts_multi(ts_col)
        if dict(df.dtypes).get(ts_col) == "string"
        else qcol(ts_col)
    )
    epoch = F.unix_timestamp(t.cast("timestamp"))
    base = df.select(
        epoch.alias("e"), *([qcol(entity_col).alias("k")] if entity_col else [])
    )
    base = base.where(F.col("e").isNotNull())
    gkeys = ["k"] if entity_col else []
    start_e = (
        F.lit(int(_dt.datetime.fromisoformat(grid_start).timestamp()))
        if grid_start
        else None
    )
    end_e = (
        F.lit(int(_dt.datetime.fromisoformat(grid_end).timestamp()))
        if grid_end
        else None
    )
    # Gap profile from consecutive DISTINCT timestamps (one window pass): the
    # largest gap in steps, and the share of intervals that are exactly one step.
    from pyspark.sql import Window as _W

    win = _W.partitionBy(*gkeys).orderBy("e") if gkeys else _W.orderBy("e")
    dist = base.select(*gkeys, "e").distinct().withColumn("ss", step_s_col)
    dist = dist.withColumn("d", F.col("e") - F.lag("e").over(win))
    gap = dist.groupBy(*gkeys).agg(
        F.first("ss").alias("ss"),
        F.max(F.when(F.col("d") > F.col("ss"), (F.col("d") / F.col("ss")) - 1)).alias(
            "gap_steps"
        ),
        F.sum((F.col("d") == F.col("ss")).cast("long")).alias("on_step"),
        F.sum(F.col("d").isNotNull().cast("long")).alias("intervals"),
    )
    g = base.groupBy(*gkeys).agg(
        F.min("e").alias("mn"),
        F.max("e").alias("mx"),
        F.countDistinct("e").alias("obs"),
    )
    if gkeys:
        g = g.join(gap, on=gkeys, how="left")
    else:
        g = g.crossJoin(gap)
    lo = start_e if start_e is not None else F.col("mn")
    hi = end_e if end_e is not None else F.col("mx")
    g = g.withColumn("expected", F.floor((hi - lo) / F.col("ss")) + F.lit(1))
    g = g.withColumn(
        "coverage_pct",
        F.round(F.least(F.col("obs") / F.col("expected"), F.lit(1.0)) * 100, 2),
    )
    g = g.withColumn("missing", F.greatest(F.col("expected") - F.col("obs"), F.lit(0)))
    rows = g.collect()
    per_entity = {
        (r["k"] if entity_col else "__all"): {
            "observed": r["obs"],
            "expected": int(r["expected"]) if r["expected"] is not None else None,
            "coverage_pct": r["coverage_pct"],
            "missing": int(r["missing"]) if r["missing"] is not None else None,
            "longest_gap_steps": (
                round(r["gap_steps"], 1) if r["gap_steps"] is not None else 0.0
            ),
            "on_step_pct": (
                round(r["on_step"] / r["intervals"] * 100, 2)
                if r["intervals"]
                else None
            ),
        }
        for r in rows
    }
    covs = [
        v["coverage_pct"] for v in per_entity.values() if v["coverage_pct"] is not None
    ]
    return {
        "step_seconds": step_map if step_map is not None else _step_seconds(step),
        "grid_start": grid_start,
        "grid_end": grid_end,
        "per_entity": per_entity,
        "coverage_min": min(covs) if covs else None,
        "coverage_max": max(covs) if covs else None,
    }


# COMMAND ----------

# DBTITLE 1,Spatial validity + categorical domain + coverage bias


def spatial_validity(df, lat_col, lon_col, bbox=DE_BBOX, name="coords"):
    lat, lon = safe_num(lat_col), safe_num(lon_col)
    lo_la, hi_la, lo_lo, hi_lo = bbox
    present = lat.isNotNull() & lon.isNotNull()
    r = (
        df.agg(
            F.sum(present.cast("long")).alias("present"),
            F.sum((~present).cast("long")).alias("missing"),
            F.sum((present & (lat == 0) & (lon == 0)).cast("long")).alias(
                "null_island"
            ),
            F.sum(
                (
                    present
                    & ~((lat.between(lo_la, hi_la)) & (lon.between(lo_lo, hi_lo)))
                ).cast("long")
            ).alias("outside_bbox"),
            F.sum(
                (
                    present & (lon.between(lo_la, hi_la)) & (lat.between(lo_lo, hi_lo))
                ).cast("long")
            ).alias("looks_swapped"),
        )
        .first()
        .asDict()
    )
    r["name"] = name
    r["bbox"] = bbox
    return r


def coverage_bias(counts):
    # counts: {entity: n}. Concentration + zero-coverage summary for a coverage
    # / survivorship-bias read.
    vals = sorted((float(v) for v in counts.values()), reverse=True)
    if not vals:
        return {"entities": 0}
    tot = sum(vals) or 1.0
    k = max(1, len(vals) // 10)
    nz = [v for v in vals if v > 0]
    return {
        "entities": len(vals),
        "zero_coverage": sum(1 for v in vals if v == 0),
        "top10pct_share": round(sum(vals[:k]) / tot, 4),
        "max_min_ratio": round(vals[0] / nz[-1], 2) if nz else None,
        "gini": _gini(vals),
    }


# COMMAND ----------

# DBTITLE 1,Cross-source temporal overlap


def regime_population_shift(df, date_col, cols, cut_iso, formats=GERMAN_TS_FORMATS):
    # Split rows on a documented date cut (e.g. the 2019 MaStR migration bulk
    # load) and measure, per column, the null-rate and distinct-value count on
    # each side -- so a "the 2019 records are structurally different" claim is
    # backed by numbers, not narrative. `flipped` lists columns whose null-rate
    # moves by >= 50 points across the cut.
    d = parse_ts_multi(date_col, formats)
    cut = F.lit(cut_iso).cast("timestamp")
    side = F.when(d.isNull(), "undated").when(d < cut, "pre").otherwise("post")
    cols = [c for c in cols if c in df.columns and c != date_col]
    aggs = [F.count(F.lit(1)).alias("n")]
    for c in cols:
        miss = qcol(c).isNull() | (F.trim(qcol(c).cast("string")) == "")
        aggs += [
            F.sum(miss.cast("long")).alias(c + "__m"),
            # approx (HLL) -- exact countDistinct per column here means an Expand
            # union of the whole table once per column (crippling on 10^8 rows);
            # the distinct count is only used to characterise the two cohorts.
            F.approx_count_distinct(qcol(c)).alias(c + "__d"),
        ]
    rows = {
        r["__s"]: r.asDict()
        for r in df.withColumn("__s", side).groupBy("__s").agg(*aggs).collect()
    }
    pre, post = rows.get("pre"), rows.get("post")

    def _rate(row, c):
        return round(row[c + "__m"] / row["n"], 4) if row and row["n"] else None

    per_column = {
        c: {
            "pre_null_rate": _rate(pre, c),
            "post_null_rate": _rate(post, c),
            "pre_distinct": pre[c + "__d"] if pre else None,
            "post_distinct": post[c + "__d"] if post else None,
        }
        for c in cols
    }
    flipped = [
        c
        for c, v in per_column.items()
        if v["pre_null_rate"] is not None
        and v["post_null_rate"] is not None
        and abs(v["pre_null_rate"] - v["post_null_rate"]) >= 0.5
    ]
    return {
        "cut": cut_iso,
        "pre_rows": (pre or {}).get("n", 0),
        "post_rows": (post or {}).get("n", 0),
        "undated_rows": (rows.get("undated") or {}).get("n", 0),
        "per_column": per_column,
        "flipped": flipped,
    }


def group_attribute_spread(df, group_col, value_col, transform=None):
    # For each distinct `group_col`, how many distinct `value_col` (optionally
    # transformed) does it carry -- an internal geographic / attribute
    # consistency probe that needs no external reference. `inconsistent_groups`
    # is the count with more than one, i.e. a contradiction within the entity.
    g = qcol(group_col).cast("string")
    v = qcol(value_col).cast("string")
    if transform == "prefix2":
        v = F.substring(F.regexp_replace(v, r"\s", ""), 1, 2)
    valid = g.isNotNull() & (F.trim(g) != "") & v.isNotNull() & (F.trim(v) != "")
    per = df.where(valid).groupBy(g.alias("g")).agg(F.countDistinct(v).alias("nd"))
    r = (
        per.agg(
            F.count(F.lit(1)).alias("groups"),
            F.sum((F.col("nd") > 1).cast("long")).alias("bad"),
            F.max("nd").alias("mx"),
        )
        .first()
        .asDict()
    )
    return {
        "group_col": group_col,
        "value_col": value_col,
        "transform": transform,
        "groups": int(r["groups"] or 0),
        "inconsistent_groups": int(r["bad"] or 0),
        "max_distinct_values_in_a_group": int(r["mx"] or 0),
    }


def numeric_order_check(df, smaller_col, larger_col):
    # Count rows where a value that must not exceed another one does. Both cast to
    # double; only rows where BOTH parse are comparable.
    a = safe_num(smaller_col)
    b = safe_num(larger_col)
    both = a.isNotNull() & b.isNotNull()
    r = (
        df.agg(
            F.sum(both.cast("long")).alias("comparable"),
            F.sum((both & (a > b)).cast("long")).alias("bad"),
        )
        .first()
        .asDict()
    )
    comp = r["comparable"] or 0
    bad = r["bad"] or 0
    return {
        "label": f"{smaller_col} <= {larger_col}",
        "smaller": smaller_col,
        "larger": larger_col,
        "comparable_rows": comp,
        "violations": bad,
        "violation_pct": round(bad / comp * 100, 4) if comp else None,
    }


def population_by_group(df, group_col, cols):
    # Per distinct `group_col`: row count and, per column, null-rate + distinct
    # count. The categorical-cut analogue of regime_population_shift -- use it
    # when the regime boundary is a label (archive vintage, month, record type),
    # not a date.
    cols = [c for c in cols if c in df.columns and c != group_col]
    aggs = [F.count(F.lit(1)).alias("__n")]
    for c in cols:
        miss = qcol(c).isNull() | (F.trim(qcol(c).cast("string")) == "")
        aggs += [
            F.sum(miss.cast("long")).alias(c + "__m"),
            # approx (HLL): exact per-column countDistinct here is an Expand
            # union of the full table once per column.
            F.approx_count_distinct(qcol(c)).alias(c + "__d"),
        ]
    rows = (
        df.withColumn("__g", qcol(group_col).cast("string"))
        .groupBy("__g")
        .agg(*aggs)
        .collect()
    )
    out = {}
    for r in rows:
        d = r.asDict()
        n = d["__n"] or 0
        out[d["__g"]] = {
            "rows": n,
            "columns": {
                c: {
                    "null_rate": round(d[c + "__m"] / n, 4) if n else None,
                    "distinct": d[c + "__d"],
                }
                for c in cols
            },
        }
    return out


def parse_ts_py(x):
    # Best-effort Python parse of an ISO-ish timestamp string (a Spark
    # F.min/F.max on a Bronze string column returns a STRING, not a datetime).
    # Tolerates a space or 'T' separator, a trailing 'Z', and date-only input.
    if x is None:
        return None
    if isinstance(x, _dt.datetime):
        return x if x.tzinfo else x.replace(tzinfo=_dt.UTC)
    if isinstance(x, _dt.date):
        return _dt.datetime(x.year, x.month, x.day, tzinfo=_dt.UTC)
    s = str(x).strip().replace("T", " ").replace("Z", "").split(".")[0]
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y%m%d%H",
        "%Y%m%d",
    ):
        try:
            return _dt.datetime.strptime(s, fmt).replace(tzinfo=_dt.UTC)
        except ValueError:
            continue
    return None


def cross_source_overlap(spans):
    # spans: {source: (min_iso, max_iso)}. Returns the common window shared by
    # ALL sources (empty if any pair is disjoint) plus each pairwise overlap in
    # days -- the ceiling on any study that joins these sources on time.
    def _d(x):
        p = parse_ts_py(x)
        return p.date() if p else None

    parsed = {
        k: (_d(a), _d(b))
        for k, (a, b) in spans.items()
        if _d(a) is not None and _d(b) is not None
    }
    if not parsed:
        return {"common_window": None, "pairwise_days": {}}
    common_lo = max(v[0] for v in parsed.values())
    common_hi = min(v[1] for v in parsed.values())
    pair = {}
    keys = list(parsed)
    for i, a in enumerate(keys):
        for b in keys[i + 1 :]:
            lo = max(parsed[a][0], parsed[b][0])
            hi = min(parsed[a][1], parsed[b][1])
            pair[f"{a} x {b}"] = max(0, (hi - lo).days)
    return {
        "common_window": (
            (str(common_lo), str(common_hi)) if common_lo <= common_hi else None
        ),
        "pairwise_days": pair,
    }


# COMMAND ----------

# DBTITLE 1,Observations by area
AREAS = (
    "Domain understanding",
    "Structure and engineering",
    "Temporal",
    "Spatial",
    "Data quality",
    "Statistical patterns",
    "Relationships",
    "Analytics use",
    "ML use",
    "AI / knowledge use",
)


def area_block(evidence):
    # One line per area. Each entry lists only what the data showed; an area
    # with nothing to report says so instead of being left out or assumed.
    lines = []
    for a in AREAS:
        items = [str(e) for e in evidence.get(a, []) if e]
        lines.append(
            f"- **{a}:** "
            + (
                "; ".join(items)
                if items
                else "no supporting evidence in the data read here."
            )
        )
    return "\n".join(lines)


# COMMAND ----------

# DBTITLE 1,Samples Volume -- file listing, delimiter sniffing and reads


def list_tree(root, max_depth=3, cap=400):
    # Recursive dbutils.fs.ls, depth- and count-capped so an unexpectedly large
    # Volume directory cannot flood the notebook.
    out = []
    stack = [(root.rstrip("/"), 0)]
    while stack and len(out) < cap:
        path, depth = stack.pop()
        for fi in dbutils.fs.ls(path):
            if fi.name.endswith("/"):
                if depth + 1 <= max_depth:
                    stack.append((fi.path.rstrip("/"), depth + 1))
                continue
            out.append({"path": fi.path, "name": fi.name, "size": fi.size})
            if len(out) >= cap:
                break
    return sorted(out, key=lambda x: x["path"])


def file_kind(name):
    n = name.lower()
    n = n.removesuffix(".gz")
    if any(h in n for h in SUPPORT_HINTS) or n.endswith((".md", ".rst")):
        return "support"
    ext = n.rsplit(".", 1)[-1] if "." in n else ""
    if ext in ("csv", "tsv", "txt", "dat", "data", "tab"):
        return "delimited"
    if ext in ("json", "jsonl", "ndjson"):
        return "json"
    if ext == "parquet":
        return "parquet"
    return "other"


def classify_files(entries):
    kinds = {}
    for e in entries:
        kinds.setdefault(file_kind(e["name"]), []).append(e)
    return kinds


def head_lines(path, n=3):
    return [r[0] for r in spark.read.text(path).limit(n).collect()]


def sniff_sep(line):
    counts = {s: line.count(s) for s in SEP_CHOICES}
    best = max(counts, key=counts.get)
    return best if counts[best] > 0 else ","


def group_delimited(entries):
    # One group per distinct header line, so files with different columns are
    # never read through a single header.
    groups = {}
    for e in entries:
        lines = head_lines(e["path"], 1)
        first = lines[0] if lines else ""
        groups.setdefault(first, []).append(e["path"])
    return groups


def read_delimited(paths, sep):
    df = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("sep", sep)
        .csv(paths)
    )
    return df.select("*", F.col("_metadata.file_name").alias("__file"))


def read_json_any(paths):
    df = spark.read.json(paths)
    if df.columns == ["_corrupt_record"]:
        df = spark.read.option("multiLine", True).json(paths)
    return df.select("*", F.col("_metadata.file_name").alias("__file"))


def read_parquet_any(paths):
    df = spark.read.parquet(*paths)
    return df.select("*", F.col("_metadata.file_name").alias("__file"))


def looks_delimited(path):
    lines = head_lines(path, 1)
    return bool(lines) and any(sep in lines[0] for sep in SEP_CHOICES)


def load_volume_frames(root, max_depth=3, cap=400):
    # Lists a Volume directory and returns (entries, kinds, frames). One frame
    # per distinct delimited header (or per JSON / Parquet file set), each with
    # a `__file` column so multi-file datasets stay separable. When no file has
    # a recognised extension, extensionless / unknown files whose first line
    # holds a separator are read as delimited text.
    entries = list_tree(root, max_depth, cap)
    kinds = classify_files(entries)
    delim = kinds.get("delimited", [])
    js = [e["path"] for e in kinds.get("json", [])]
    pq = [e["path"] for e in kinds.get("parquet", [])]
    if not (delim or js or pq):
        delim = [e for e in kinds.get("other", []) if looks_delimited(e["path"])]
        if delim:
            kinds["delimited"] = delim
    frames = {}
    for i, (first, paths) in enumerate(group_delimited(delim).items(), start=1):
        sep = sniff_sep(first)
        frames[f"delimited_{i}"] = {
            "df": read_delimited(paths, sep),
            "paths": paths,
            "sep": DELIMITERS.get(sep, sep),
        }
    if js:
        frames["json"] = {"df": read_json_any(js), "paths": js, "sep": None}
    if pq:
        frames["parquet"] = {"df": read_parquet_any(pq), "paths": pq, "sep": None}
    return entries, kinds, frames


def require_frames(frames, root, kinds):
    # Fail fast with the file-kind counts instead of letting every later loop
    # run over nothing.
    if not frames:
        counts = {k: len(v) for k, v in kinds.items()}
        raise RuntimeError(
            f"No readable data file under {root}; file kinds found: {counts}"
        )


def read_support_text(kinds, n=80):
    # First lines of each support file (README / licence) -- provenance evidence.
    out = {}
    for e in kinds.get("support", []):
        out[e["name"]] = head_lines(e["path"], n)
    return out


# COMMAND ----------

# DBTITLE 1,Single-pass profile, numeric scan, epoch timestamps, markdown helpers


def as_str(name):
    return qcol(name).cast("string")


def is_missing(name):
    return qcol(name).isNull() | (F.trim(as_str(name)) == "")


def to_double(name):
    return F.expr(
        "try_cast(trim(cast(`"
        + str(name).replace("`", "``")
        + "` as string)) as double)"
    )


def profile_frame(df, extra=None):
    # Rows, per-column missingness and approx distinct in ONE aggregation. extra
    # maps a name to a further aggregate expression of the same pass.
    cols = df.columns
    exprs = [F.count(F.lit(1)).alias("__rows")]
    for i, c in enumerate(cols):
        exprs += [
            F.sum(is_missing(c).cast("long")).alias(f"m{i}"),
            F.approx_count_distinct(as_str(c)).alias(f"d{i}"),
        ]
    exprs += [e.alias(f"x_{n}") for n, e in (extra or {}).items()]
    r = df.agg(*exprs).first().asDict()
    out = {
        "cols": cols,
        "total": r["__rows"],
        "miss": {c: r[f"m{i}"] or 0 for i, c in enumerate(cols)},
        "acd": {c: r[f"d{i}"] for i, c in enumerate(cols)},
    }
    if extra:
        out["extra"] = {n: r[f"x_{n}"] for n in extra}
    return out


def numeric_scan(df, cols):
    # Per-column numeric parse yield and moments in ONE aggregation. A column is
    # numeric only if ~all non-empty values parse (>= 95%).
    if not cols:
        return {}
    exprs = []
    for i, c in enumerate(cols):
        v = to_double(c)
        good = ~is_missing(c)
        exprs += [
            F.sum(good.cast("long")).alias(f"n{i}"),
            F.sum(v.isNotNull().cast("long")).alias(f"p{i}"),
            F.min(v).alias(f"lo{i}"),
            F.max(v).alias(f"hi{i}"),
            F.avg(v).alias(f"mu{i}"),
            F.stddev(v).alias(f"sd{i}"),
            F.sum((v == 0).cast("long")).alias(f"z{i}"),
            F.sum((v < 0).cast("long")).alias(f"ng{i}"),
        ]
    r = df.agg(*exprs).first().asDict()
    out = {}
    for i, c in enumerate(cols):
        nn = r[f"n{i}"] or 0
        yld = (r[f"p{i}"] or 0) / nn if nn else 0.0
        out[c] = {
            "non_null": nn,
            "yield": round(yld, 4),
            "is_numeric": nn > 0 and yld >= 0.95,
            "min": r[f"lo{i}"],
            "max": r[f"hi{i}"],
            "mean": r[f"mu{i}"],
            "sd": r[f"sd{i}"],
            "zero": r[f"z{i}"],
            "negative": r[f"ng{i}"],
        }
    return out


def constant_column_names(df, prof):
    # Exact check of the columns that approx_count_distinct shows with one value.
    cands = [c for c in prof["cols"] if prof["acd"][c] <= 1]
    if not cands:
        return []
    r = df.agg(*[F.countDistinct(qcol(c)).alias(f"k{i}") for i, c in enumerate(cands)])
    r = r.first().asDict()
    return sorted(c for i, c in enumerate(cands) if (r[f"k{i}"] or 0) <= 1)


def quantiles(df, name, probs=(0.01, 0.25, 0.5, 0.75, 0.99)):
    v = to_double(name)
    q = df.select(v.alias("__v")).where(F.col("__v").isNotNull())
    return dict(zip(probs, q.approxQuantile("__v", list(probs), 0.001), strict=False))


def md_table(header, rows):
    out = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    for row in rows:
        out.append("| " + " | ".join(str(x) for x in row) + " |")
    return "\n".join(out)


def fmt_num(x, nd=4):
    if x is None:
        return "-"
    if isinstance(x, float):
        return f"{x:.{nd}g}"
    return str(x)


# COMMAND ----------

# DBTITLE 1,Article text and link graph -- length, markup, titles, key overlap, degrees


def resolve_roles(columns, hints):
    # First column whose lower-case name equals one of the hints, per role.
    return {
        role: next((c for h in names for c in columns if c.lower() == h), None)
        for role, names in hints.items()
    }


def longest_text_col(df, cols, n=500):
    # Fallback when no name hint matches: the string column with the longest
    # average value in the first n rows.
    str_cols = [c for c in cols if dict(df.dtypes).get(c) == "string"]
    if not str_cols:
        return None
    r = (
        df.limit(n)
        .agg(*[F.avg(F.length(qcol(c))).alias(f"a{i}") for i, c in enumerate(str_cols)])
        .first()
        .asDict()
    )
    best = max(range(len(str_cols)), key=lambda i: r[f"a{i}"] or 0)
    return str_cols[best]


def title_norm(col):
    return F.lower(F.trim(F.regexp_replace(as_str(col), "_", " ")))


def word_count(text_col):
    # Whitespace-separated words; an empty or blank string has zero words.
    t = F.trim(text_col)
    return F.when(F.length(t) == 0, F.lit(0)).otherwise(F.size(F.split(t, r"\s+")))


def text_stats(df, col):
    txt = as_str(col)
    base = df.select(F.length(txt).alias("__chars"), word_count(txt).alias("__words"))
    probs = ",".join(str(p) for p in QUANTILE_PROBS)
    r = (
        base.agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum((F.col("__words") == 0).cast("long")).alias("empty"),
            F.sum((F.col("__chars") < 200).cast("long")).alias("under_200_chars"),
            F.sum((F.col("__words") > WORDS_PER_CHUNK_PROXY).cast("long")).alias(
                "over_proxy_words"
            ),
            F.sum("__words").alias("total_words"),
            F.max("__chars").alias("max_chars"),
            F.expr(f"percentile_approx(__chars, array({probs}))").alias("chars_q"),
            F.expr(f"percentile_approx(__words, array({probs}))").alias("words_q"),
        )
        .first()
        .asDict()
    )
    r["chars_q"] = dict(zip(QUANTILE_PROBS, r["chars_q"], strict=True))
    r["words_q"] = dict(zip(QUANTILE_PROBS, r["words_q"], strict=True))
    return r


def section_stats(df, col, fraction=1.0, seed=7):
    # Sections are the pieces between inline heading markers. Computed on a row
    # sample when fraction < 1.
    src = df.select(as_str(col).alias("__t"))
    if fraction < 1.0:
        src = src.sample(fraction=fraction, seed=seed)
    parts = src.select(F.split(F.col("__t"), HEADING_PATTERN).alias("__p"))
    per_article = (
        parts.agg(
            F.count(F.lit(1)).alias("articles"),
            F.sum((F.size("__p") > 1).cast("long")).alias("with_marker"),
            F.expr("percentile_approx(size(__p), array(0.5, 0.95))").alias("per_q"),
            F.max(F.size("__p")).alias("per_max"),
        )
        .first()
        .asDict()
    )
    flat = (
        parts.select(F.explode("__p").alias("__x"))
        .where(F.trim(F.col("__x")) != "")
        .select(word_count(F.col("__x")).alias("__w"))
    )
    probs = ",".join(str(p) for p in QUANTILE_PROBS)
    r = (
        flat.agg(
            F.count(F.lit(1)).alias("sections"),
            F.sum((F.col("__w") > WORDS_PER_CHUNK_PROXY).cast("long")).alias(
                "over_proxy"
            ),
            F.max("__w").alias("max_words"),
            F.expr(f"percentile_approx(__w, array({probs}))").alias("words_q"),
        )
        .first()
        .asDict()
    )
    r["words_q"] = dict(zip(QUANTILE_PROBS, r["words_q"], strict=True))
    r["sample_fraction"] = fraction
    r["sampled_articles"] = per_article["articles"]
    r["with_marker"] = per_article["with_marker"]
    r["per_article_p50_p95"] = per_article["per_q"]
    r["per_article_max"] = per_article["per_max"]
    return r


def clean_text(text_col):
    # Approximate wikitext stripping by regular expressions.
    out = text_col
    for pattern, repl in CLEAN_STEPS:
        out = F.regexp_replace(out, pattern, repl)
    return out


def cleaned_length_stats(df, col, fraction=1.0, seed=7):
    src = df.select(as_str(col).alias("__t"))
    if fraction < 1.0:
        src = src.sample(fraction=fraction, seed=seed)
    w = src.select(
        word_count(F.col("__t")).alias("__raw"),
        word_count(clean_text(F.col("__t"))).alias("__clean"),
    )
    probs = ",".join(str(p) for p in QUANTILE_PROBS)
    r = (
        w.agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum("__raw").alias("raw_words"),
            F.sum("__clean").alias("clean_words"),
            F.sum((F.col("__clean") < 50).cast("long")).alias("under_50"),
            F.sum((F.col("__clean") > WORDS_PER_CHUNK_PROXY).cast("long")).alias(
                "over_proxy"
            ),
            F.expr(f"percentile_approx(__clean, array({probs}))").alias("words_q"),
        )
        .first()
        .asDict()
    )
    r["words_q"] = dict(zip(QUANTILE_PROBS, r["words_q"], strict=True))
    r["kept_share"] = (
        round(r["clean_words"] / r["raw_words"], 4) if r["raw_words"] else None
    )
    r["sample_fraction"] = fraction
    return r


def key_duplicate_profile(df, key_col, vary):
    # vary: name -> Column expression. For each repeated key, whether the rows
    # differ in that expression (HLL distinct count, exact for 1 versus more).
    per = df.groupBy(as_str(key_col).alias("__k")).agg(
        F.count(F.lit(1)).alias("__n"),
        *[F.approx_count_distinct(e).alias(f"__v_{n}") for n, e in vary.items()],
    )
    exprs = [
        F.count(F.lit(1)).alias("keys"),
        F.sum((F.col("__n") > 1).cast("long")).alias("dup_keys"),
        F.sum(F.when(F.col("__n") > 1, F.col("__n")).otherwise(0)).alias(
            "rows_in_dup_keys"
        ),
        F.max("__n").alias("max_rows_per_key"),
    ]
    exprs += [
        F.sum(((F.col("__n") > 1) & (F.col(f"__v_{n}") > 1)).cast("long")).alias(
            f"varies_{n}"
        )
        for n in vary
    ]
    return per.agg(*exprs).first().asDict()


def timestamp_profile(df, col, top=12):
    ts = qcol(col)
    r = (
        df.agg(
            F.min(ts).alias("lo"),
            F.max(ts).alias("hi"),
            F.sum(ts.isNull().cast("long")).alias("nulls"),
        )
        .first()
        .asDict()
    )
    by_year = (
        df.select(F.year(ts).alias("__y"))
        .groupBy("__y")
        .count()
        .orderBy("__y")
        .collect()
    )
    by_month = (
        df.select(F.date_format(ts, "yyyy-MM").alias("__m"))
        .groupBy("__m")
        .count()
        .orderBy(F.desc("count"))
        .limit(top)
        .collect()
    )
    r["years"] = [(x["__y"], x["count"]) for x in by_year]
    r["top_months"] = [(x["__m"], x["count"]) for x in by_month]
    return r


def log2_histogram(df, value_col):
    # Counts per power-of-two bucket of a numeric Column expression.
    b = F.floor(F.log2(value_col + 1)).cast("int")
    rows = (
        df.select(b.alias("__b"))
        .where(F.col("__b").isNotNull())
        .groupBy("__b")
        .count()
        .orderBy("__b")
        .collect()
    )
    return [(f"2^{r['__b']}", r["count"]) for r in rows]


def markup_indicators(df, col, patterns=None):
    # Rows containing each markup pattern at least once.
    patterns = patterns or MARKUP_PATTERNS
    txt = as_str(col)
    exprs = [F.count(F.lit(1)).alias("rows")]
    exprs += [F.sum(txt.rlike(p).cast("long")).alias(n) for n, p in patterns.items()]
    return df.agg(*exprs).first().asDict()


def char_issues(df, col):
    txt = as_str(col)
    non_ascii = F.length(txt) - F.length(F.regexp_replace(txt, r"[^\x00-\x7F]", ""))
    r = (
        df.agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(txt.rlike("�").cast("long")).alias("replacement_char_rows"),
            F.sum(txt.rlike(r"[\x00-\x08\x0B\x0C\x0E-\x1F]").cast("long")).alias(
                "control_char_rows"
            ),
            F.sum(txt.rlike(r"[^\x00-\x7F]").cast("long")).alias("non_ascii_rows"),
            F.sum(non_ascii).alias("non_ascii_chars"),
            F.sum(F.length(txt)).alias("chars"),
        )
        .first()
        .asDict()
    )
    r["non_ascii_char_share"] = (
        round(r["non_ascii_chars"] / r["chars"], 6) if r["chars"] else None
    )
    return r


def text_duplicates(df, col):
    # Exact duplicate texts through one 64-bit hash per row.
    per = df.select(F.xxhash64(as_str(col)).alias("__h")).groupBy("__h").count()
    return (
        per.agg(
            F.sum("count").alias("rows"),
            F.count(F.lit(1)).alias("distinct_texts"),
            F.sum((F.col("count") > 1).cast("long")).alias("dup_groups"),
            F.sum(F.when(F.col("count") > 1, F.col("count")).otherwise(0)).alias(
                "rows_in_dup_groups"
            ),
        )
        .first()
        .asDict()
    )


def title_forms(df, col):
    t = as_str(col)
    r = (
        df.agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(is_missing(col).cast("long")).alias("empty"),
            F.sum(t.contains("_").cast("long")).alias("with_underscore"),
            F.sum((t != F.trim(t)).cast("long")).alias("edge_space"),
            F.sum(t.contains(":").cast("long")).alias("with_colon"),
            F.sum(t.rlike(r"(?i)\(disambiguation\)").cast("long")).alias(
                "disambiguation"
            ),
            F.sum(t.rlike(r"(?i)^list[ _]of").cast("long")).alias("list_titles"),
            F.sum(t.rlike(r"^\d+$").cast("long")).alias("digits_only"),
            F.sum(t.rlike(r"^[a-z]").cast("long")).alias("starts_lowercase"),
        )
        .first()
        .asDict()
    )
    coll = (
        df.select(title_norm(col).alias("__n"))
        .groupBy("__n")
        .count()
        .agg(
            F.sum((F.col("count") > 1).cast("long")).alias("collision_groups"),
            F.sum(F.when(F.col("count") > 1, F.col("count")).otherwise(0)).alias(
                "collision_rows"
            ),
        )
        .first()
        .asDict()
    )
    return {**r, **coll}


def domain_term_hits(df, col, terms_by_domain, fraction=1.0, seed=7):
    # Rows matching any term of each domain (whole words, case-insensitive).
    src = df.select(as_str(col).alias("__t"))
    if fraction < 1.0:
        src = src.sample(fraction=fraction, seed=seed)
    exprs = [F.count(F.lit(1)).alias("rows")]
    for name, terms in terms_by_domain.items():
        pat = r"(?i)\b(" + "|".join(terms) + r")\b"
        exprs.append(F.sum(F.col("__t").rlike(pat).cast("long")).alias(name))
    r = src.agg(*exprs).first().asDict()
    r["sample_fraction"] = fraction
    return r


def title_term_mass(df, title_col, weight_col, terms_by_domain):
    # Total weight and weight on titles matching each domain's terms.
    t = as_str(title_col)
    w = to_double(weight_col)
    exprs = [F.sum(w).alias("weight")]
    for name, terms in terms_by_domain.items():
        pat = r"(?i)\b(" + "|".join(terms) + r")\b"
        exprs.append(F.sum(F.when(t.rlike(pat), w).otherwise(0.0)).alias(name))
    return df.agg(*exprs).first().asDict()


def key_overlap(a, col_a, b, col_b, normalise=True):
    # Distinct keys in a, in b and in both, from one tagged union.
    def key(col):
        return title_norm(col) if normalise else as_str(col)

    ka = a.select(key(col_a).alias("__k")).where(F.col("__k") != "").distinct()
    kb = b.select(key(col_b).alias("__k")).where(F.col("__k") != "").distinct()
    tagged = ka.select("__k", F.lit(1).alias("__a"), F.lit(0).alias("__b")).unionByName(
        kb.select("__k", F.lit(0).alias("__a"), F.lit(1).alias("__b"))
    )
    r = (
        tagged.groupBy("__k")
        .agg(F.max("__a").alias("a"), F.max("__b").alias("b"))
        .agg(
            F.sum("a").alias("a_distinct"),
            F.sum("b").alias("b_distinct"),
            F.sum(((F.col("a") + F.col("b")) == 2).cast("long")).alias("both"),
        )
        .first()
        .asDict()
    )
    r["a_not_in_b"] = (r["a_distinct"] or 0) - (r["both"] or 0)
    r["a_share_in_b"] = (
        round(r["both"] / r["a_distinct"], 4) if r["a_distinct"] else None
    )
    r["b_share_in_a"] = (
        round(r["both"] / r["b_distinct"], 4) if r["b_distinct"] else None
    )
    return r


def weighted_coverage(edges, title_col, weight_col, known, known_col):
    # Share of edges and of edge weight whose title is a known (article) title.
    ref = (
        known.select(title_norm(known_col).alias("__k"))
        .distinct()
        .withColumn("__known", F.lit(1))
    )
    e = edges.select(
        title_norm(title_col).alias("__k"), to_double(weight_col).alias("__w")
    )
    hit = F.col("__known").isNotNull()
    r = (
        e.join(ref, "__k", "left")
        .agg(
            F.count(F.lit(1)).alias("edges"),
            F.sum(hit.cast("long")).alias("edges_matched"),
            F.sum("__w").alias("weight"),
            F.sum(F.when(hit, F.col("__w")).otherwise(0.0)).alias("weight_matched"),
        )
        .first()
        .asDict()
    )
    r["edge_share"] = round(r["edges_matched"] / r["edges"], 4) if r["edges"] else None
    r["weight_share"] = (
        round(r["weight_matched"] / r["weight"], 4) if r["weight"] else None
    )
    return r


def degree_table(edges, key_col, weight_col):
    # One row per node: edge count and total weight.
    return edges.groupBy(as_str(key_col).alias("__k")).agg(
        F.count(F.lit(1)).alias("edges"), F.sum(to_double(weight_col)).alias("clicks")
    )


def degree_summary(tbl, top=10):
    r = (
        tbl.agg(
            F.count(F.lit(1)).alias("nodes"),
            F.expr("percentile_approx(edges, array(0.5, 0.9, 0.99))").alias("edges_q"),
            F.max("edges").alias("edges_max"),
            F.expr("percentile_approx(clicks, array(0.5, 0.9, 0.99))").alias(
                "clicks_q"
            ),
            F.max("clicks").alias("clicks_max"),
            F.sum("clicks").alias("clicks_total"),
            F.expr("percentile_approx(clicks, 0.99)").alias("clicks_p99"),
        )
        .first()
        .asDict()
    )
    top_mass = (
        tbl.where(F.col("clicks") >= F.lit(r["clicks_p99"]))
        .agg(F.sum("clicks").alias("m"))
        .first()["m"]
    )
    r["top1pct_click_share"] = (
        round(top_mass / r["clicks_total"], 4) if r["clicks_total"] else None
    )
    r["top"] = [
        (row["__k"], row["clicks"], row["edges"])
        for row in tbl.orderBy(F.desc("clicks")).limit(top).collect()
    ]
    return r


# COMMAND ----------

# DBTITLE 1,Genomics Gold tables -- profile, label association, links, findings blocks


def read_tables(catalog, schema, names):
    return {name: spark.table(f"{catalog}.{schema}.{name}") for name in names}


def column_kinds(df):
    kinds = {
        "boolean": [],
        "numeric": [],
        "text": [],
        "timestamp": [],
        "provenance": [],
        "other": [],
    }
    for name, dtype in df.dtypes:
        if name in PROVENANCE_COLUMNS:
            kind = "provenance"
        elif dtype == "boolean":
            kind = "boolean"
        elif dtype in NUMERIC_DTYPES or dtype.startswith("decimal"):
            kind = "numeric"
        elif dtype == "string":
            kind = "text"
        elif dtype == "date" or dtype.startswith("timestamp"):
            kind = "timestamp"
        else:
            kind = "other"
        kinds[kind].append(name)
    return kinds


def table_profile(df):
    prof = profile_frame(df)
    total = prof["total"]
    empty = [c for c in prof["cols"] if total and prof["miss"][c] == total]
    constant = [c for c in constant_column_names(df, prof) if c not in empty]
    return {
        "rows": total,
        "columns": len(prof["cols"]),
        "kinds": {k: len(v) for k, v in column_kinds(df).items()},
        "empty": empty,
        "constant": constant,
        "null_rate": {
            c: round(prof["miss"][c] / total, 4) if total else None
            for c in prof["cols"]
        },
        "distinct": prof["acd"],
    }


def table_key_report(df, preferred=("source_record_id",)):
    # The key is chosen by the exact distinct count, as in the source EDA notebooks.
    candidates = [c for c in preferred if c in df.columns]
    candidates += [
        c for c in df.columns if c.endswith(KEY_SUFFIXES) and c not in candidates
    ]
    candidates = candidates[:KEY_CANDIDATES]
    uniq = exact_uniqueness(df, candidates)
    key, info = pick_entity_key(uniq, candidates, prefer=preferred)
    if key is None:
        return {"key": None, "tested": candidates}
    out = {
        "key": key,
        "ratio": info["ratio"],
        "unique": info["unique"],
        "nulls": info["nulls"],
        "tested": candidates,
    }
    if not info["unique"]:
        out["duplicates"] = dup_key_composition(df, [key])
    return out


def boolean_rates(df, cols):
    if not cols:
        return {}
    exprs = [F.count(F.lit(1)).alias("__n")]
    for i, c in enumerate(cols):
        exprs += [
            F.sum(F.when(qcol(c), 1).otherwise(0)).alias(f"t{i}"),
            F.sum(F.when(~qcol(c), 1).otherwise(0)).alias(f"f{i}"),
        ]
    r = df.agg(*exprs).first().asDict()
    n = r["__n"] or 0
    out = {}
    for i, c in enumerate(cols):
        t, f = r[f"t{i}"] or 0, r[f"f{i}"] or 0
        out[c] = {
            "true": round(t / n, 4) if n else None,
            "false": round(f / n, 4) if n else None,
            "null": round((n - t - f) / n, 4) if n else None,
        }
    return out


def value_counts(df, col, limit=30):
    # (value, rows, share) of one column, a missing value shown as "(missing)".
    rows = df.groupBy(qcol(col).cast("string").alias("v")).count().collect()
    total = sum(r["count"] for r in rows)
    rows = sorted(rows, key=lambda r: -r["count"])[:limit]
    return [
        (r["v"] if r["v"] is not None else "(missing)", r["count"], r["count"] / total)
        for r in rows
    ]


def group_size_shares(df, key_cols):
    # Share of groups by their row count: 1, 2, 3 to 5, 6 or more.
    sizes = df.groupBy(*[qcol(c) for c in key_cols]).count()
    exprs = [F.count(F.lit(1)).alias("__groups")]
    for i, (low, _label) in enumerate(GROUP_EDGES):
        high = GROUP_EDGES[i + 1][0] if i + 1 < len(GROUP_EDGES) else None
        cond = F.col("count") >= low
        if high is not None:
            cond = cond & (F.col("count") < high)
        exprs.append(F.sum(F.when(cond, 1).otherwise(0)).alias(f"s{i}"))
    r = sizes.agg(*exprs).first().asDict()
    groups = r["__groups"] or 0
    return {
        "groups": groups,
        **{
            label: round((r[f"s{i}"] or 0) / groups, 4) if groups else None
            for i, (_low, label) in enumerate(GROUP_EDGES)
        },
    }


def with_target(df, positive_col, negative_col):
    # Rows with a known class; __pos is 1.0 for the positive class, else 0.0.
    known = qcol(positive_col) | qcol(negative_col)
    return df.filter(known).withColumn(
        "__pos", F.when(qcol(positive_col), 1.0).otherwise(0.0)
    )


def flag_vs_label(df, flag_cols):
    # df has __pos. Per flag: how often the flag is true, how often the positive
    # class holds when it is true and when it is not (rates only).
    if not flag_cols:
        return {}
    pos = F.col("__pos")
    exprs = [F.count(F.lit(1)).alias("__n"), F.sum(pos).alias("__p")]
    for i, c in enumerate(flag_cols):
        on = qcol(c)
        exprs += [
            F.sum(F.when(on, 1).otherwise(0)).alias(f"n{i}"),
            F.sum(F.when(on, pos).otherwise(0.0)).alias(f"p{i}"),
        ]
    r = df.agg(*exprs).first().asDict()
    n, p = r["__n"] or 0, r["__p"] or 0.0
    out = {}
    for i, c in enumerate(flag_cols):
        on_n, on_p = r[f"n{i}"] or 0, r[f"p{i}"] or 0.0
        off_n, off_p = n - on_n, p - on_p
        when_true = on_p / on_n if on_n else None
        when_false = off_p / off_n if off_n else None
        gap = (
            abs(when_true - when_false)
            if when_true is not None and when_false is not None
            else None
        )
        out[c] = {
            "coverage": round(on_n / n, 4) if n else None,
            "positive_when_true": round(when_true, 4) if on_n else None,
            "positive_when_false": round(when_false, 4) if off_n else None,
            "separation": round(gap, 4) if gap is not None else None,
        }
    return out


def score_vs_label(df, num_cols):
    # df has __pos. Per numeric column: correlation with the class and the
    # difference between the missing rate in each class.
    if not num_cols:
        return {}
    pos = F.col("__pos")
    exprs = [
        F.sum(pos).alias("__p"),
        F.sum(1.0 - pos).alias("__q"),
    ]
    for i, c in enumerate(num_cols):
        v = qcol(c).cast("double")
        exprs += [
            F.corr(v, pos).alias(f"c{i}"),
            F.sum(F.when(v.isNull(), pos).otherwise(0.0)).alias(f"mp{i}"),
            F.sum(F.when(v.isNull(), 1.0 - pos).otherwise(0.0)).alias(f"mq{i}"),
        ]
    r = df.agg(*exprs).first().asDict()
    p, q = r["__p"] or 0.0, r["__q"] or 0.0
    out = {}
    for i, c in enumerate(num_cols):
        corr = r[f"c{i}"]
        if corr is not None and math.isnan(corr):
            corr = None
        miss_pos = (r[f"mp{i}"] or 0.0) / p if p else None
        miss_neg = (r[f"mq{i}"] or 0.0) / q if q else None
        out[c] = {
            "correlation": round(corr, 4) if corr is not None else None,
            "missing_positive": round(miss_pos, 4) if miss_pos is not None else None,
            "missing_negative": round(miss_neg, 4) if miss_neg is not None else None,
            "missing_gap": (
                round(abs(miss_pos - miss_neg), 4)
                if miss_pos is not None and miss_neg is not None
                else None
            ),
        }
    return out


def pair_correlations(df, num_cols, threshold=LEAK_CORRELATION):
    # Pairs of numeric columns above the threshold, computed in one aggregation.
    cols = num_cols[:PAIR_COLUMNS]
    pairs = [(a, b) for i, a in enumerate(cols) for b in cols[i + 1 :]]
    if not pairs:
        return []
    exprs = [
        F.corr(qcol(a).cast("double"), qcol(b).cast("double")).alias(f"r{i}")
        for i, (a, b) in enumerate(pairs)
    ]
    r = df.agg(*exprs).first().asDict()
    found = [
        (a, b, round(r[f"r{i}"], 4))
        for i, (a, b) in enumerate(pairs)
        if r[f"r{i}"] is not None
        and not math.isnan(r[f"r{i}"])
        and abs(r[f"r{i}"]) >= threshold
    ]
    return sorted(found, key=lambda t: -abs(t[2]))


def top_by(table, field, limit=LIST_LIMIT):
    # Rows of a {column: {field: value}} table ranked by the absolute field value.
    ranked = [(c, v[field]) for c, v in table.items() if v.get(field) is not None]
    return sorted(ranked, key=lambda t: -abs(t[1]))[:limit]


def coverage_by_source(df, prof, group_col="source_system", limit=40):
    # Null rate and distinct count of the columns that are partly filled, per
    # group; a group with one value only gives one row.
    if group_col not in df.columns:
        return {}
    partly = [
        c
        for c, rate in prof["null_rate"].items()
        if rate is not None and 0.01 <= rate <= 0.99 and c != group_col
    ]
    if not partly:
        return {}
    return population_by_group(df, group_col, partly[:limit])


def coverage_block(coverage):
    # Per table: source groups and the columns whose null rate differs between
    # the groups by the missing gap or more.
    lines = []
    for name, groups in coverage.items():
        columns = next(iter(groups.values()))["columns"] if groups else {}
        spread = {}
        for c in columns:
            rates = [g["columns"][c]["null_rate"] for g in groups.values()]
            rates = [r for r in rates if r is not None]
            if len(rates) > 1:
                spread[c] = round(max(rates) - min(rates), 4)
        wide = sorted(
            ((c, s) for c, s in spread.items() if s >= MISSING_GAP),
            key=lambda t: -t[1],
        )
        lines.append(
            para(
                f"- {name}: source groups {len(groups)};",
                f"partly filled columns compared {len(columns)};",
                f"null rate differs by {MISSING_GAP} or more between groups in",
                f"{len(wide)} columns",
                f"({', '.join(c for c, _ in wide[:10])}).",
            )
        )
    return "\n".join(lines)


def link_fill(profiles):
    # Null rate of every column ending in _key, per table.
    return {
        name: {
            c: r
            for c, r in p["null_rate"].items()
            if c.endswith("_key") and r is not None
        }
        for name, p in profiles.items()
    }


def fill_lines(fill):
    # {table: {column: null rate}} as markdown lines.
    lines = []
    for name, cols in fill.items():
        lines.append(f"**{name}** ({len(cols)} columns)")
        lines.append(fmt_pairs([(c, pct(r)) for c, r in cols.items()]) or "- none")
    return "\n".join(lines)


def share_in_several_groups(df, key_col, group_col):
    # Keys that appear in more than one group (for example a split), as a count
    # and as a share of the distinct keys.
    per_key = (
        df.where(qcol(key_col).isNotNull())
        .groupBy(qcol(key_col))
        .agg(F.countDistinct(qcol(group_col)).alias("g"))
    )
    r = per_key.agg(
        F.count(F.lit(1)).alias("keys"),
        F.sum(F.when(F.col("g") > 1, 1).otherwise(0)).alias("several"),
    ).first()
    keys, several = r["keys"] or 0, r["several"] or 0
    return {
        "keys": keys,
        "in_several": several,
        "share": round(several / keys, 4) if keys else None,
    }


def pct(x):
    return "-" if x is None else f"{100 * x:.2f}%"


def profile_block(profiles):
    rows = [
        (
            name,
            f"{p['rows']:,}",
            p["columns"],
            p["kinds"]["numeric"],
            p["kinds"]["boolean"],
            p["kinds"]["text"],
            len(p["empty"]),
            len(p["constant"]),
        )
        for name, p in profiles.items()
    ]
    header = (
        "table",
        "rows",
        "columns",
        "numeric",
        "boolean",
        "text",
        "empty",
        "const",
    )
    return md_table(header, rows)


def quality_block(profiles):
    lines = []
    for name, p in profiles.items():
        high = [
            (c, pct(r))
            for c, r in sorted(p["null_rate"].items(), key=lambda t: -(t[1] or 0))
            if r is not None and HIGH_NULL_RATE <= r < 1.0
        ]
        empty = ", ".join(p["empty"][:25])
        constant = ", ".join(p["constant"][:25])
        lines.append(f"**{name}**")
        lines.append(f"- empty columns ({len(p['empty'])}): {empty}")
        lines.append(f"- constant columns ({len(p['constant'])}): {constant}")
        lines.append(
            f"- null rate of {pct(HIGH_NULL_RATE)} or more, not empty ({len(high)}):"
        )
        lines.append(fmt_pairs(high, LIST_LIMIT))
    return "\n".join(lines)


def key_block(keys):
    rows = []
    for name, k in keys.items():
        dup = k.get("duplicates") or {}
        rows.append(
            (
                name,
                k["key"] or "-",
                "yes" if k.get("unique") else "no",
                k.get("ratio", "-"),
                k.get("nulls", "-"),
                dup.get("dup_groups", 0),
                dup.get("conflicting", 0),
            )
        )
    header = ("table", "key", "unique", "distinct/rows", "nulls", "dups", "conflicting")
    return md_table(header, rows)


def shape_lists(profile, kinds):
    # Text columns with few values (enumerations) and near-unique text columns.
    rows = profile["rows"] or 0
    text = kinds["text"]
    few = [c for c in text if 0 < profile["distinct"][c] <= TEXT_ENUM_MAX]
    unique = [c for c in text if rows and profile["distinct"][c] >= NEAR_UNIQUE * rows]
    return {"enumeration": few, "near_unique": unique}


def distribution_block(profiles, kinds, booleans, scans):
    # scans: numeric_scan result per table (counts of zero and negative values).
    lines = []
    for name, p in profiles.items():
        rows = p["rows"] or 0
        shape = shape_lists(p, kinds[name])
        always = [c for c, v in booleans[name].items() if (v["true"] or 0) >= 0.99]
        never = [c for c, v in booleans[name].items() if (v["true"] or 0) <= 0.01]
        mostly_null = [c for c, v in booleans[name].items() if (v["null"] or 0) >= 0.99]
        zeros = [
            c for c, v in scans[name].items() if rows and (v["zero"] or 0) / rows >= 0.5
        ]
        negatives = [c for c, v in scans[name].items() if (v["negative"] or 0) > 0]
        lines += [
            f"**{name}**",
            para(
                f"- boolean columns: {len(booleans[name])};",
                f"true on 99% or more of rows: {len(always)};",
                f"true on 1% or less: {len(never)};",
                f"null on 99% or more: {len(mostly_null)}",
            ),
            para(
                f"- numeric columns: {len(scans[name])};",
                f"zero on half the rows or more: {len(zeros)};",
                f"with negative values: {len(negatives)}",
            ),
            para(
                f"- text columns with at most {TEXT_ENUM_MAX} values:",
                f"{len(shape['enumeration'])};",
                f"near-unique text columns: {len(shape['near_unique'])}",
            ),
        ]
    return "\n".join(lines)


def table_findings_lines(profiles, keys):
    rows = sum(p["rows"] or 0 for p in profiles.values())
    repeated = [n for n, k in keys.items() if not k.get("unique")]
    with_empty = {n: len(p["empty"]) for n, p in profiles.items() if p["empty"]}
    lines = [
        f"- {len(profiles)} tables, {rows:,} rows in total.",
        f"- tables whose best key is not unique: {', '.join(repeated) or 'none'}.",
        "- tables with fully empty columns: "
        + (", ".join(f"{n} ({c})" for n, c in with_empty.items()) or "none")
        + ".",
    ]
    return "\n".join(lines)


def association_lines(label, flags, scores):
    # Ranked names and rates only (no data values).
    lines = [f"**{label}**"]
    lines.append("- flags by separation (coverage in brackets):")
    lines.append(
        fmt_pairs(
            [
                (c, f"{v} ({pct(flags[c]['coverage'])})")
                for c, v in top_by(flags, "separation")
            ],
            LIST_LIMIT,
        )
    )
    lines.append("- numeric columns by correlation with the class:")
    lines.append(fmt_pairs(top_by(scores, "correlation"), LIST_LIMIT))
    lines.append("- numeric columns by gap between the missing rate of the classes:")
    lines.append(fmt_pairs(top_by(scores, "missing_gap"), LIST_LIMIT))
    return "\n".join(lines)


def profile_standard(frames, tables, coverage=True):
    """The standard profile of a table group: column kinds, profiles, keys, boolean
    and numeric rates and, when asked, coverage by source. One line per table."""
    kinds = {name: column_kinds(frame) for name, frame in frames.items()}
    profiles = {}
    for name, frame in frames.items():
        profiles[name] = table_profile(frame)
        p = profiles[name]
        print(
            f"{name}: rows={p['rows']:,} columns={p['columns']} "
            f"empty={len(p['empty'])} constant={len(p['constant'])}"
        )
    keys = {name: table_key_report(frame) for name, frame in frames.items()}
    for name, key in keys.items():
        print(name, key)
    booleans = {
        name: boolean_rates(frame, kinds[name]["boolean"])
        for name, frame in frames.items()
    }
    numerics = {
        name: numeric_scan(frame, kinds[name]["numeric"])
        for name, frame in frames.items()
    }
    for name in tables:
        print(
            name,
            "boolean columns:",
            len(booleans[name]),
            "numeric:",
            len(numerics[name]),
        )
    covered = {}
    if coverage:
        covered = {
            name: coverage_by_source(frame, profiles[name])
            for name, frame in frames.items()
        }
        for name, groups in covered.items():
            print(
                name,
                "source groups:",
                len(groups),
                {g: v["rows"] for g, v in groups.items()},
            )
    return kinds, profiles, keys, booleans, numerics, covered


def open_group(catalog, schema, tables, coverage=True):
    """Read a table group and run the standard profile. Returns the frames and the six
    profile results (kinds, profiles, keys, booleans, numerics, coverage)."""
    frames = read_tables(catalog, schema, tables)
    return (frames, *profile_standard(frames, tables, coverage=coverage))


def leaking_columns(flags, scores):
    """The flags, numeric columns and missing-rate gaps that reach the leak limits."""
    return {
        "flags": [c for c, v in top_by(flags, "separation") if v >= FLAG_SEPARATION],
        "scores": [
            c for c, v in top_by(scores, "correlation") if abs(v) >= LEAK_CORRELATION
        ],
        "gap": [c for c, v in top_by(scores, "missing_gap") if v >= MISSING_GAP],
    }


def class_association(frames, tables, kinds, key_col, label, skip=()):
    """Flag and numeric association with a class slice (`label` holds `key_col` and
    `__pos`) for every table whose unique key is `key_col`; the columns in `skip` are
    left out. Returns the association and the leaking columns per table."""
    assoc = {}
    for name in tables:
        check = exact_uniqueness(frames[name], [key_col]).get(key_col)
        if not check or not check["unique"]:
            print(f"{name}: {key_col} is not unique, no association")
            continue
        joined = frames[name].join(label, key_col)
        flags = [c for c in kinds[name]["boolean"] if c not in skip]
        scores = [c for c in kinds[name]["numeric"] if c not in skip]
        flag_assoc = flag_vs_label(joined, flags)
        score_assoc = score_vs_label(joined, scores)
        ranked = [c for c, _ in top_by(score_assoc, "correlation", PAIR_COLUMNS)]
        assoc[name] = {
            "rows": joined.count(),
            "flags": flag_assoc,
            "scores": score_assoc,
            "pairs": pair_correlations(joined, ranked),
        }
        print(name, "labelled rows:", assoc[name]["rows"])
    leaking = {
        name: leaking_columns(a["flags"], a["scores"]) for name, a in assoc.items()
    }
    for name, found in leaking.items():
        print(name, {k: len(v) for k, v in found.items()})
    return assoc, leaking


def association_block(assoc, unit, list_pairs=False):
    """The association text of every table; the pair list only when asked."""
    parts = []
    for name, a in assoc.items():
        text = association_lines(
            f"{name} ({a['rows']:,} labelled {unit})", a["flags"], a["scores"]
        )
        text += f"\n- numeric pairs at or above the limit: {len(a['pairs'])}"
        if list_pairs:
            pairs = fmt_pairs([(f"{x} / {y}", r) for x, y, r in a["pairs"]], LIST_LIMIT)
            text += "\n" + pairs
        parts.append(text)
    return "\n".join(parts)


def leak_entries(leaking, prefix="", noun=""):
    """The target, proxy and missingness readiness entries of an association."""
    target = "; ".join(
        f"{n}: {len(v['flags'])} flags, {len(v['scores'])} numeric columns"
        for n, v in leaking.items()
    )
    proxy = "; ".join(
        f"{n}: " + ", ".join((v["flags"] + v["scores"])[:10])
        for n, v in leaking.items()
        if v["flags"] or v["scores"]
    )
    return [
        ("Target contamination", para(prefix, target) if prefix else target),
        (
            "Proxy leakage",
            proxy or f"no {noun}flag or numeric column reaches the limits.",
        ),
        (
            "Missingness leakage",
            "; ".join(f"{n}: {len(v['gap'])} columns" for n, v in leaking.items()),
        ),
    ]


def export_group(
    source,
    nb_key,
    section_title,
    std,
    extra_blocks=(),
    readiness=(),
    evidence=None,
    quality_extra="",
    coverage_extra="",
    findings=None,
    figures=None,
    links=True,
):
    """The findings export of a table group: the standard blocks, the readiness and
    area evidence of the profile, and the extra blocks, readiness entries and evidence
    a section adds (an extra entry of the same name replaces the standard one).
    `std` is the profile result of `open_group`."""
    kinds, profiles, keys, booleans, numerics, covered = std
    fills = link_fill(profiles) if links else {}
    for name, fill in fills.items():
        print(name, fill)
    date_columns = {}
    for name, p in profiles.items():
        names = kinds[name]["timestamp"] + [
            c for c in kinds[name]["text"] if "date" in c.lower()
        ]
        names = [c for c in names if c not in PROVENANCE_COLUMNS]
        date_columns[name] = {c: p["null_rate"][c] for c in names}
        print(name, len(names), "date-like columns")
    if figures is None:
        figures = rows_per_table_figure(profiles, f"{source}_{nb_key[:2]}_rows.png")
    ready = {
        "Grain / grain drift": "; ".join(
            f"`{n}` key `{k['key']}` unique: {k.get('unique')}" for n, k in keys.items()
        ),
        "Duplicate-event leakage": "; ".join(
            para(
                f"{n} dup groups {(k.get('duplicates') or {}).get('dup_groups', 0)},",
                f"conflicting {(k.get('duplicates') or {}).get('conflicting', 0)}",
            )
            for n, k in keys.items()
        ),
        "Survivorship / coverage bias": "; ".join(
            para(
                f"{n}: {sum((r or 0) >= HIGH_NULL_RATE for r in p['null_rate'].values())}",
                f"columns with a null rate of {pct(HIGH_NULL_RATE)} or more",
            )
            for n, p in profiles.items()
        ),
    }
    ready.update(dict(readiness))
    ready["Sample-vs-full divergence"] = (
        "every count and rate is read from the whole table; no sample is used."
    )
    notes = {
        "Data quality": [
            f"{n}: {len(p['empty'])} empty, {len(p['constant'])} constant columns"
            for n, p in profiles.items()
        ],
        "Structure and engineering": [
            f"{n}: {len(shape_lists(p, kinds[n])['near_unique'])} near-unique text columns"
            for n, p in profiles.items()
        ],
        "Temporal": [
            f"{n}: {len(c)} date-like columns" for n, c in date_columns.items()
        ],
    }
    if links:
        notes["Relationships"] = [
            f"{n}: {len(f)} link columns" for n, f in fills.items()
        ]
    notes.update(evidence or {})
    blocks = [
        ("Profile", profile_block(profiles)),
        (
            "Data Quality",
            quality_block(profiles) + (f"\n\n{quality_extra}" if quality_extra else ""),
        ),
        ("Entities / Keys", key_block(keys)),
    ]
    if covered:
        blocks.append(
            (
                "Coverage & Sampling Bias",
                coverage_block(covered)
                + (f"\n{coverage_extra}" if coverage_extra else ""),
            )
        )
    if links:
        blocks.append(("Relationships", fill_lines(fills)))
    blocks += list(extra_blocks)
    blocks += [
        ("Temporal Semantics", fill_lines(date_columns)),
        ("Distributions", distribution_block(profiles, kinds, booleans, numerics)),
        ("EDA Findings", findings or table_findings_lines(profiles, keys)),
        ("ML-Readiness Evidence", ml_readiness_block(list(ready.items()))),
        ("Observations by Area", area_block(notes)),
    ]
    write_profiling(source, nb_key, section_title, blocks, figures)
    return profiles


def run_groups(catalog, source, sections):
    """The standard EDA of every plain table group of a notebook: read, profile and
    export. A section is a dict with `schema`, `nb_key`, `title` and `tables`."""
    for s in sections:
        print("=" * 90, f"\n{s['nb_key']}: {s['title']}")
        _, *std = open_group(catalog, s["schema"], s["tables"], s.get("coverage", True))
        export_group(source, s["nb_key"], s["title"], std, links=s.get("links", True))


def link_bias_blocks(catalog, links, bias, links_title, bias_title):
    """Run the link checks and the spread of rows per key; return the two findings
    blocks. `bias` is a list of (label, schema, table, key column)."""
    link_results = link_checks(catalog, links)
    print(links_block(link_results))
    spread = {
        label: coverage_bias(
            rows_per_key(spark.table(f"{catalog}.{schema}.{table}"), column)
        )
        for label, schema, table, column in bias
    }
    print(bias_block(spread))
    return [
        (links_title, links_block(link_results)),
        (bias_title, bias_block(spread)),
    ]


def frame_intake(
    root, hints, preferred, cap=None, text_fallback=False, raw_lines=0, profile=True
):
    """Read a Samples Volume directory as one frame: file inventory, README texts,
    the first raw lines, the preferred frame, its column roles and nesting and, when
    asked, the missingness profile and the constant columns."""
    entries, kinds, frames = (
        load_volume_frames(root, cap=cap) if cap else load_volume_frames(root)
    )
    require_frames(frames, root, kinds)
    print(f"{root}: {len(entries)} files, {sum(e['size'] for e in entries)} bytes")
    if cap and len(entries) >= cap:
        print(f"WARNING: the listing reached the cap of {cap}; files may be missing")
    for e in entries[:12]:
        print(f"  {file_kind(e['name']):<10} {e['size']:>12}  {e['path']}")
    print({k: len(v) for k, v in kinds.items()})
    support = print_support_files(kinds)
    first_lines = []
    if raw_lines and kinds.get(preferred):
        first_lines = head_lines(kinds[preferred][0]["path"], raw_lines)
    for ln in first_lines:
        print(ln[:300])
    frame, df, data_cols = select_frame(frames, preferred)
    role = resolve_roles(data_cols, hints)
    if text_fallback and role.get("text") is None:
        role["text"] = longest_text_col(df, data_cols)
        print("text column chosen by average length:", role["text"])
    print(role)
    nested, struct = structural_check(df, data_cols)
    d0 = df.drop("__file")
    info = {
        "entries": entries,
        "kinds": kinds,
        "frames": frames,
        "support": support,
        "raw_lines": first_lines,
        "frame": frame,
        "data_cols": data_cols,
        "role": role,
        "nested": nested,
        "struct": struct,
        "d0": d0,
    }
    if profile:
        prof = profile_frame(d0)
        print(f"rows={prof['total']}  cols={len(prof['cols'])}")
        for c in prof["cols"]:
            rate = prof["miss"][c] / prof["total"] if prof["total"] else 0
            print(
                f"  {c[:40]:<40} missing={prof['miss'][c]:>9} rate={rate:.4f} "
                f"approx_distinct={prof['acd'][c]}"
            )
        prof["constant"] = constant_column_names(d0, prof)
        print(f"constant={prof['constant']}")
        info["prof"] = prof
    return info


def intake_blocks(info, root, unit):
    """The profile, provenance and structure blocks of a frame read by `frame_intake`,
    and the missingness line of its data quality block."""
    prof, nested, struct = info["prof"], info["nested"], info["struct"]
    entries, kinds, support = info["entries"], info["kinds"], info["support"]
    paths = len(info["frames"][info["frame"]]["paths"])
    profile = [
        f"| frame | source files | {unit} | cols | constant columns | nested columns |",
        "|---|---|---|---|---|---|",
        para(
            f"| {info['frame']} | {paths} | {prof['total']} | {len(prof['cols'])} |",
            f"{', '.join(prof['constant']) or '-'} | {', '.join(nested) or '-'} |",
        ),
    ]
    provenance = [
        para(
            "The dataset is read in place from the Databricks Samples Volume;",
            "there is no Bronze table and no ECRMAP acquisition step.",
        ),
        (
            f"Files under `{root}`: {len(entries)}, "
            f"{sum(e['size'] for e in entries)} bytes "
            f"({ {k: len(v) for k, v in kinds.items()} })."
        ),
    ]
    if info["raw_lines"]:
        provenance += [
            "First raw lines:",
            *[f"  > {ln[:200]}" for ln in info["raw_lines"]],
        ]
    for name, lines in support.items():
        provenance.append(f"README-type file `{name}` (first lines):")
        provenance += [f"  > {ln[:160]}" for ln in lines[:25] if ln.strip()]
    if not support:
        provenance.append(
            "- No README / licence file in the directory: provenance and licence are "
            "not documented here (LIMITATION)."
        )
    structure = [
        f"- schema: `{struct['schema']}`",
        (
            f"- corrupt-record column present: {struct['corrupt_record']}; "
            f"nested columns: {nested or 'none'}."
        ),
        f"- column roles resolved: { {k: v for k, v in info['role'].items() if v} }.",
    ]
    missing = "Missingness (rate per column): " + ", ".join(
        f"{c}={prof['miss'][c] / prof['total'] if prof['total'] else 0:.4f}"
        for c in prof["cols"]
    )
    blocks = [
        ("Profile", "\n".join(profile)),
        ("Provenance / Source Evidence", "\n".join(provenance)),
        ("Structural Integrity", "\n".join(structure)),
    ]
    return blocks, missing


def print_support_files(kinds):
    """Print the README and licence texts found next to the data files; return them."""
    support = read_support_text(kinds)
    for name, lines in support.items():
        print("=" * 90, f"\n{name}")
        for ln in lines[:40]:
            print("  " + ln[:160])
    if not support:
        print("no README / licence file in the directory")
    return support


def select_frame(frames, preferred):
    """The preferred frame (or the first one), its data columns, and its schema."""
    frame = preferred if preferred in frames else next(iter(frames))
    df = frames[frame]["df"]
    data_cols = [c for c in df.columns if c != "__file"]
    print(f"frame={frame}  files={len(frames[frame]['paths'])}  columns={data_cols}")
    df.printSchema()
    return frame, df, data_cols


def structural_check(df, data_cols):
    """Nested columns and the corrupt-record column of a frame; (nested, summary)."""
    nested = [
        f.name
        for f in df.schema.fields
        if f.dataType.simpleString().startswith(("struct", "array", "map"))
    ]
    struct = {
        "corrupt_record": "_corrupt_record" in data_cols,
        "nested": nested,
        "schema": df.schema.simpleString(),
    }
    print(struct)
    return nested, struct


def rows_per_table_figure(profiles, filename):
    """The rows-per-table bar plot; the figure list for the findings export."""
    drawn = barplot(
        [(n, p["rows"]) for n, p in profiles.items()],
        "Rows per table",
        "table",
        ylabel="rows",
        rot=20,
        logy=True,
        filename=filename,
    )
    return [("Rows per table", filename)] if drawn else []


def link_integrity(child_df, child_col, parent_df, parent_col, child, parent):
    """Link check by joins, with no key set on the driver: child keys without a parent
    key and the rows they hold, parent keys never referenced, and the child rows per
    parent key."""
    ckey = qcol(child_col).cast("string")
    pkey = qcol(parent_col).cast("string")
    per_key = (
        child_df.where(ckey.isNotNull() & (F.trim(ckey) != ""))
        .groupBy(ckey.alias("k"))
        .count()
    )
    parents = (
        parent_df.where(pkey.isNotNull() & (F.trim(pkey) != ""))
        .select(pkey.alias("k"))
        .distinct()
        .withColumn("hit", F.lit(1))
    )
    joined = per_key.join(parents, "k", "left")
    orphan = F.col("hit").isNull()
    s = (
        joined.agg(
            F.count(F.lit(1)).alias("keys"),
            F.sum("count").alias("rows"),
            F.sum(F.when(orphan, 1).otherwise(0)).alias("orphan_keys"),
            F.sum(F.when(orphan, F.col("count")).otherwise(0)).alias("orphan_rows"),
            F.expr("percentile_approx(count, 0.5)").alias("p50"),
            F.expr("percentile_approx(count, 0.9)").alias("p90"),
            F.expr("percentile_approx(count, 0.99)").alias("p99"),
            F.max("count").alias("mx"),
        )
        .first()
        .asDict()
    )
    parent_keys = parents.count()
    referenced = joined.where(~orphan).count()
    return {
        "child": child,
        "parent": parent,
        "child_rows": int(s["rows"] or 0),
        "child_keys": int(s["keys"] or 0),
        "orphan_keys": int(s["orphan_keys"] or 0),
        "orphan_rows": int(s["orphan_rows"] or 0),
        "parent_keys": parent_keys,
        "parent_keys_referenced_pct": (
            round(referenced / parent_keys * 100, 3) if parent_keys else None
        ),
        "rows_per_parent_p50": int(s["p50"] or 0),
        "rows_per_parent_p90": int(s["p90"] or 0),
        "rows_per_parent_p99": int(s["p99"] or 0),
        "max_fanout": int(s["mx"] or 0),
    }


def series_continuity(df, table, ts_col, entity_col="location_key"):
    """continuity_grid of a Gold series table for each source system and step (the
    `interval_seconds` column); label -> result."""
    results = {}
    pairs = df.select("source_system", "interval_seconds").distinct().collect()
    for pair in pairs:
        source, step = pair["source_system"], pair["interval_seconds"]
        if source is None or step is None:
            continue
        part = df.where(
            (F.col("source_system") == source) & (F.col("interval_seconds") == step)
        )
        results[f"{table} / {source} / {step} s"] = continuity_grid(
            part, ts_col, step, entity_col=entity_col
        )
    return results


def link_checks(catalog, checks):
    """link_integrity for each (child schema, child table, child column, parent schema,
    parent table, parent column); the list of results."""
    results = []
    for c_schema, c_table, c_col, p_schema, p_table, p_col in checks:
        results.append(
            link_integrity(
                spark.table(f"{catalog}.{c_schema}.{c_table}"),
                c_col,
                spark.table(f"{catalog}.{p_schema}.{p_table}"),
                p_col,
                f"{c_table}.{c_col}",
                f"{p_table}.{p_col}",
            )
        )
    return results


def rows_per_key(df, key_col):
    """Rows per value of a key column, for coverage_bias; null keys are left out."""
    grouped = df.where(F.col(key_col).isNotNull()).groupBy(key_col).count()
    return {row[key_col]: row["count"] for row in grouped.collect()}


def _table(header, rows):
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    lines += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return "\n".join(lines)


def continuity_block(results):
    """Markdown of continuity_grid results: counts and rates only, no series key."""
    rows = []
    for label, r in results.items():
        per = list(r["per_entity"].values())
        below = sum(1 for v in per if (v["coverage_pct"] or 0) < 95)
        gap = max((v["longest_gap_steps"] for v in per), default=0)
        rows.append((label, len(per), r["coverage_min"], r["coverage_max"], below, gap))
    header = ("series", "series count", "coverage min %", "coverage max %")
    return _table(header + ("series below 95 %", "longest gap (steps)"), rows)


def spatial_block(results):
    """Markdown of spatial_validity results."""
    header = (
        "table",
        "with coordinates",
        "without",
        "at 0/0",
        "outside box",
        "swapped",
    )
    rows = [
        (
            label,
            r["present"],
            r["missing"],
            r["null_island"],
            r["outside_bbox"],
            r["looks_swapped"],
        )
        for label, r in results.items()
    ]
    return _table(header, rows)


def order_block(results):
    """Markdown of numeric_order_check results."""
    header = ("rule", "comparable rows", "violations", "violation %")
    rows = [
        (r["label"], r["comparable_rows"], r["violations"], r["violation_pct"])
        for r in results
    ]
    return _table(header, rows)


def overlap_block(results):
    """Markdown of cross_source_overlap results: lengths in days, no dates."""
    lines = []
    for label, r in results.items():
        window = r["common_window"]
        days = (
            (_dt.date.fromisoformat(window[1]) - _dt.date.fromisoformat(window[0])).days
            if window
            else None
        )
        lines.append(f"- {label}: common window {days} days")
        lines += [
            f"  - {pair}: {days} days" for pair, days in r["pairwise_days"].items()
        ]
    return "\n".join(lines)


def bias_block(results):
    """Markdown of coverage_bias results."""
    header = ("rows per", "groups", "empty", "top 10 % share", "max/min", "gini")
    rows = [
        (
            label,
            r.get("entities"),
            r.get("zero_coverage"),
            r.get("top10pct_share"),
            r.get("max_min_ratio"),
            r.get("gini"),
        )
        for label, r in results.items()
    ]
    return _table(header, rows)


def links_block(results):
    """Markdown of link_integrity results."""
    header = ("child", "parent", "child rows", "child keys", "orphan keys")
    header += ("orphan rows", "parent keys referenced %", "rows per parent p50/p90/p99")
    rows = [
        (
            f"`{r['child']}`",
            f"`{r['parent']}`",
            r["child_rows"],
            r["child_keys"],
            r["orphan_keys"],
            r["orphan_rows"],
            r["parent_keys_referenced_pct"],
            (
                f"{r['rows_per_parent_p50']}/{r['rows_per_parent_p90']}/"
                f"{r['rows_per_parent_p99']} (max {r['max_fanout']})"
            ),
        )
        for r in results
    ]
    return _table(header, rows)


# COMMAND ----------

# DBTITLE 1,ML datasets -- limits and pure helpers
ML_CATALOG = "energy_commerce_retail_media"
ML_SHARED = "ml_datasets_shared"
ML_FIT_STATES = ("fitted", "frozen")
ML_COLUMN_BATCH = 100
ML_NEAR_CONSTANT = 0.999
ML_EDGE_PROBS = [0.01, 0.99]
ML_SHOWN = 10
ML_STEP_FACTOR = 1.5


def batches(items, size=ML_COLUMN_BATCH):
    # Lists of at most `size` items: one wide aggregation per list, so that no
    # single query carries hundreds of expressions.
    items = list(items)
    return [items[i : i + size] for i in range(0, len(items), size)]


def is_number_type(dtype):
    return dtype == "boolean" or dtype in NUMERIC_DTYPES or dtype.startswith("decimal")


def ml_roles(rows):
    """{role: [columns]} of (column name, role) rows, in the order given."""
    roles = {}
    for name, role in rows:
        roles.setdefault(role, []).append(name)
    return roles


def ml_target_kind(dtype, low, high, distinct):
    """binary, numeric, text or other: from the column type and, for a number, its
    smallest and largest value and the number of distinct values."""
    if dtype == "boolean":
        return "binary"
    if dtype == "string":
        return "text"
    if not is_number_type(dtype):
        return "other"
    flag = low is not None and low >= 0 and high <= 1 and distinct <= 2
    return "binary" if flag else "numeric"


def date_gaps(dates):
    """For {partition: dates}: the number of dates, the usual step in days (the most
    frequent one), the steps missing (every gap wider than ML_STEP_FACTOR steps) and
    the longest gap in days."""
    out = {}
    for part, values in dates.items():
        days = sorted(set(values))
        diffs = [(b - a).days for a, b in _it.pairwise(days)]
        if not diffs:
            out[part] = {"dates": len(days), "step": None, "missing": 0, "longest": 0}
            continue
        counts = {}
        for d in diffs:
            counts[d] = counts.get(d, 0) + 1
        step = min(d for d, n in counts.items() if n == max(counts.values()))
        wide = [d for d in diffs if d > ML_STEP_FACTOR * step]
        out[part] = {
            "dates": len(days),
            "step": step,
            "missing": sum(round(d / step) - 1 for d in wide),
            "longest": max(diffs),
        }
    return out


# COMMAND ----------

# DBTITLE 1,ML datasets -- target, shift, constant columns, association, gaps, groups


def ml_number(name):
    # A column as a double; a text that is not a number becomes null.
    return F.expr(f"try_cast(`{name}` AS DOUBLE)")


def _finite(x):
    return x is not None and not math.isnan(x)


def ml_target_stats(frame, target, kind):
    """Per partition: rows, labelled rows, mean (the positive rate of a flag),
    standard deviation and share of zero; the number of levels of a text target."""
    value = ml_number(target)
    if kind == "text":
        extra = [F.countDistinct(qcol(target)).alias("levels")]
        labelled = F.count(qcol(target))
    else:
        extra = [
            F.avg(value).alias("mean"),
            F.stddev_samp(value).alias("std"),
            F.avg((value == 0).cast("double")).alias("zero"),
        ]
        labelled = F.count(value)
    rows = (
        frame.groupBy("partition")
        .agg(F.count(F.lit(1)).alias("rows"), labelled.alias("labelled"), *extra)
        .collect()
    )
    return {r["partition"]: r.asDict() for r in rows}


def ml_text_levels(frame, target):
    """Text target: the share of the top level in train, and for validation and test
    the share of labelled rows whose level train does not have."""
    train = frame.where((F.col("partition") == "train") & qcol(target).isNotNull())
    levels = train.groupBy(qcol(target).alias("__level")).count()
    top = levels.agg(F.max("count"), F.sum("count")).first()
    out = {"top_share": round(top[0] / top[1], 4) if top[1] else None}
    for part in ("validation", "test"):
        rows = frame.where((F.col("partition") == part) & qcol(target).isNotNull())
        total = rows.count()
        unseen = rows.join(levels, qcol(target) == F.col("__level"), "left_anti")
        out[part] = round(unseen.count() / total, 4) if total else None
    return out


def ml_fold_balance(frame, target):
    """{fold: (rows, mean of the target)} of the train rows that have a fold."""
    rows = (
        frame.where((F.col("partition") == "train") & F.col("fold_id").isNotNull())
        .groupBy("fold_id")
        .agg(F.count(F.lit(1)).alias("rows"), F.avg(ml_number(target)).alias("mean"))
        .collect()
    )
    return {r["fold_id"]: (r["rows"], r["mean"]) for r in rows}


def ml_value_shares(train, cols):
    """{column: share of the most frequent value among the non-null train rows}."""
    out = {}
    for group in batches(cols):
        modes = train.agg(*[F.mode(qcol(c)).alias(c) for c in group]).first().asDict()
        known = {c: v for c, v in modes.items() if v is not None}
        if not known:
            continue
        shares = (
            train.agg(
                *[
                    F.avg((qcol(c) == F.lit(v)).cast("double")).alias(c)
                    for c, v in known.items()
                ]
            )
            .first()
            .asDict()
        )
        out.update({c: s for c, s in shares.items() if s is not None})
    return out


def ml_train_edges(train, cols):
    """{column: mean, standard deviation, 1st and 99th percentile} of the train rows."""
    out = {}
    for group in batches(cols):
        exprs = []
        for i, c in enumerate(group):
            v = qcol(c).cast("double")
            exprs += [
                F.avg(v).alias(f"m{i}"),
                F.stddev_samp(v).alias(f"s{i}"),
                F.percentile_approx(v, ML_EDGE_PROBS).alias(f"p{i}"),
            ]
        r = train.agg(*exprs).first().asDict()
        for i, c in enumerate(group):
            p = r[f"p{i}"] or [None, None]
            out[c] = {"mean": r[f"m{i}"], "std": r[f"s{i}"], "low": p[0], "high": p[1]}
    return out


def ml_feature_shift(frame, edges):
    """{column: {partition: (mean difference to train in train standard deviations,
    share of rows outside the train 1st to 99th percentile range)}} for validation
    and test."""
    later = frame.where(F.col("partition").isin("validation", "test"))
    usable = [
        c
        for c, e in edges.items()
        if _finite(e["std"]) and e["std"] > 0 and e["low"] is not None
    ]
    out = {c: {} for c in edges}
    for group in batches(usable):
        exprs = []
        for i, c in enumerate(group):
            v = qcol(c).cast("double")
            outside = ((v < edges[c]["low"]) | (v > edges[c]["high"])).cast("double")
            exprs += [
                F.avg(v).alias(f"m{i}"),
                F.avg(F.when(v.isNotNull(), outside)).alias(f"o{i}"),
            ]
        for r in later.groupBy("partition").agg(*exprs).collect():
            for i, c in enumerate(group):
                if r[f"m{i}"] is None or r[f"o{i}"] is None:
                    continue
                shift = (r[f"m{i}"] - edges[c]["mean"]) / edges[c]["std"]
                out[c][r["partition"]] = (round(shift, 3), round(r[f"o{i}"], 4))
    return out


def ml_target_association(train, cols, target):
    """{column: correlation with the target} of the train rows."""
    out = {}
    goal = ml_number(target)
    for group in batches(cols):
        exprs = [
            F.corr(qcol(c).cast("double"), goal).alias(f"c{i}")
            for i, c in enumerate(group)
        ]
        r = train.agg(*exprs).first().asDict()
        for i, c in enumerate(group):
            if _finite(r[f"c{i}"]):
                out[c] = round(r[f"c{i}"], 4)
    return out


def ml_date_gaps(frame, time_col):
    """date_gaps of the days that each partition holds."""
    rows = (
        frame.select("partition", F.to_date(qcol(time_col)).alias("d"))
        .where(F.col("d").isNotNull())
        .distinct()
        .collect()
    )
    dates = {}
    for r in rows:
        dates.setdefault(r["partition"], []).append(r["d"])
    return date_gaps(dates)


def ml_group_sizes(frame):
    """Per partition: groups, rows, rows per group (median, 99th percentile, largest)
    and the share of the rows that the largest group holds."""
    sizes = (
        frame.where(F.col("group_key").isNotNull())
        .groupBy("partition", "group_key")
        .count()
    )
    rows = (
        sizes.groupBy("partition")
        .agg(
            F.count(F.lit(1)).alias("groups"),
            F.sum("count").alias("rows"),
            F.max("count").alias("largest"),
            F.percentile_approx("count", [0.5, 0.99]).alias("p"),
        )
        .collect()
    )
    return {
        r["partition"]: {
            "groups": r["groups"],
            "rows": r["rows"],
            "median": r["p"][0],
            "p99": r["p"][1],
            "largest": r["largest"],
            "top_share": round(r["largest"] / r["rows"], 4),
        }
        for r in rows
    }


# COMMAND ----------

# DBTITLE 1,ML datasets -- the profile of one dataset


def ml_profile_dataset(frame, roles):
    """The ML EDA results of one dataset frame: target by partition and fold, train
    columns with one dominant value, shift of the numeric features, correlation with
    the target, redundant feature pairs, gaps of the time axis and group sizes.
    `roles` is {role: [columns]} of the feature contract."""
    dtypes = dict(frame.dtypes)
    targets = [c for c in roles.get("target", []) if c in dtypes]
    target = targets[0] if targets else None
    features = [c for c in roles.get("feature", []) if c in dtypes]
    numbers = [c for c in features if is_number_type(dtypes[c])]
    train = frame.where(F.col("partition") == "train")
    out = {"target": target, "kind": None, "features": len(features)}
    out["numbers"] = len(numbers)
    if target:
        low = high = distinct = None
        if is_number_type(dtypes[target]) and dtypes[target] != "boolean":
            value = ml_number(target)
            low, high, distinct = train.agg(
                F.min(value), F.max(value), F.approx_count_distinct(value)
            ).first()
        kind = ml_target_kind(dtypes[target], low, high, distinct)
        out["kind"] = kind
        if kind != "other":
            out["partitions"] = ml_target_stats(frame, target, kind)
        if kind == "text":
            out["levels"] = ml_text_levels(frame, target)
        if kind in ("binary", "numeric"):
            out["folds"] = ml_fold_balance(frame, target)
    if features:
        shares = ml_value_shares(train, features)
        out["dominant"] = {c: s for c, s in shares.items() if s >= ML_NEAR_CONSTANT}
    if numbers:
        edges = ml_train_edges(train, numbers)
        out["shift"] = ml_feature_shift(frame, edges)
    checked = numbers[:PAIR_COLUMNS]
    if numbers and out["kind"] in ("binary", "numeric"):
        found = ml_target_association(train, numbers, target)
        ranked = sorted(found.items(), key=lambda kv: -abs(kv[1]))
        out["association"] = ranked[:ML_SHOWN]
        out["above_limit"] = sum(1 for _c, r in ranked if abs(r) >= LEAK_CORRELATION)
        checked = [c for c, _r in ranked[:PAIR_COLUMNS]]
    out["checked"] = len(checked)
    out["pairs"] = pair_correlations(train, checked) if checked else []
    times = sorted(c for c in roles.get("time", []) if c in dtypes)
    time_type = dtypes[times[0]] if times else ""
    if time_type in ("date", "string") or time_type.startswith("timestamp"):
        out["gaps"] = ml_date_gaps(frame, times[0])
    if "group_key" in dtypes:
        out["groups"] = ml_group_sizes(frame)
    return out


# COMMAND ----------

# DBTITLE 1,ML datasets -- findings blocks and the run of one ecosystem


def _worst_shifts(shift):
    # Per column the partition with the largest shift, then the columns with the
    # largest shifts.
    worst = {
        c: max(parts.items(), key=lambda kv: abs(kv[1][0]))
        for c, parts in shift.items()
        if parts
    }
    ranked = sorted(worst.items(), key=lambda kv: -abs(kv[1][1][0]))
    return [(c, part, s, o) for c, (part, (s, o)) in ranked[:ML_SHOWN]]


def ml_eda_blocks(results):
    """The findings blocks of {dataset: ml_profile_dataset result}: counts, rates
    and column names only."""
    rows = {k: [] for k in ("target", "text", "folds", "shift", "dominant")}
    rows.update({k: [] for k in ("association", "pairs", "gaps", "groups")})
    skipped = []
    for ds, r in results.items():
        if r["target"] is None or r["kind"] == "other":
            skipped.append(ds)
        for part, p in sorted(r.get("partitions", {}).items()):
            rows["target"].append(
                (ds, r["kind"], part, p["rows"], p["labelled"])
                + tuple(fmt_num(p.get(k)) for k in ("mean", "std", "zero", "levels"))
            )
        if "levels" in r:
            lv = r["levels"]
            rows["text"].append((ds, lv["top_share"], lv["validation"], lv["test"]))
        folds = r.get("folds", {})
        if folds:
            counts = [n for n, _m in folds.values()]
            means = [m for _n, m in folds.values() if m is not None]
            lowest = fmt_num(min(means)) if means else "-"
            highest = fmt_num(max(means)) if means else "-"
            rows["folds"].append(
                (ds, len(folds), min(counts), max(counts), lowest, highest)
            )
        for c, part, s, o in _worst_shifts(r.get("shift", {})):
            rows["shift"].append((ds, f"`{c}`", part, s, o))
        dom = r.get("dominant", {})
        names = ", ".join(f"`{c}`" for c in sorted(dom)[:ML_SHOWN])
        rows["dominant"].append((ds, r["features"], len(dom), names))
        if "association" in r:
            top = ", ".join(f"`{c}` {x}" for c, x in r["association"])
            rows["association"].append((ds, r["numbers"], r["above_limit"], top))
        pairs = ", ".join(f"`{a}` / `{b}` {x}" for a, b, x in r["pairs"][:3])
        rows["pairs"].append((ds, r["checked"], len(r["pairs"]), pairs))
        for part, g in sorted(r.get("gaps", {}).items()):
            rows["gaps"].append(
                (ds, part, g["dates"], g["step"], g["missing"], g["longest"])
            )
        for part, g in sorted(r.get("groups", {}).items()):
            rows["groups"].append(
                (ds, part, g["groups"], g["rows"], g["median"], g["p99"])
                + (g["largest"], g["top_share"])
            )
    headers = {
        "target": ("dataset", "target kind", "partition", "rows", "labelled")
        + ("mean or positive rate", "standard deviation", "share zero", "levels"),
        "text": ("dataset", "top level share in train")
        + ("validation rows with a new level", "test rows with a new level"),
        "folds": ("dataset", "train folds", "rows per fold min", "rows per fold max")
        + ("target mean min", "target mean max"),
        "shift": ("dataset", "feature", "partition", "mean shift in train deviations")
        + ("share outside the train 1 to 99 percent range",),
        "dominant": ("dataset", "features", "with a value in 99.9 percent of rows")
        + ("first columns",),
        "association": (
            "dataset",
            "numeric features",
            f"at or above {LEAK_CORRELATION}",
        )
        + ("ten highest correlations with the target",),
        "pairs": ("dataset", "columns checked", f"pairs at or above {LEAK_CORRELATION}")
        + ("first pairs",),
        "gaps": ("dataset", "partition", "days", "usual step in days")
        + ("steps missing", "longest gap in days"),
        "groups": ("dataset", "partition", "groups", "rows", "rows per group median")
        + ("rows per group 99th percentile", "largest group", "largest group share"),
    }
    titles = {
        "target": "Target by partition",
        "text": "Text targets: levels",
        "folds": "Target by train fold",
        "shift": "Feature shift from train (ten largest per dataset)",
        "dominant": "Features with one dominant value in train",
        "association": "Correlation of numeric features with the target (train)",
        "pairs": "Redundant feature pairs (train)",
        "gaps": "Gaps of the time axis inside each partition (day level)",
        "groups": "Group sizes per partition",
    }
    blocks = [(titles[k], md_table(headers[k], rows[k])) for k in titles if rows[k]]
    contaminated = [f"{r[0]}: {r[2]}" for r in rows["association"] if r[2]]
    gapped = sorted({r[0] for r in rows["gaps"] if r[4]})
    ready = [
        (
            "Target contamination",
            f"numeric features at or above {LEAK_CORRELATION} with the target (train): "
            + ("; ".join(contaminated) or "none"),
        ),
        (
            "Temporal / post-event leakage",
            f"datasets with missing steps inside a partition: {gapped or 'none'}",
        ),
        (
            "Sample-vs-full divergence",
            "every count and rate is read from the whole dataset; no sample is used.",
        ),
    ]
    blocks.append(("ML-Readiness Evidence", ml_readiness_block(ready)))
    if skipped:
        blocks.append(("Datasets without a readable target", ", ".join(skipped)))
    return blocks


def run_ml_eda(eco):
    """The ML EDA of every fitted dataset of one ecosystem, written to
    `src/schemas/profiling/ml.md` under the key of the ecosystem."""
    manifest = (
        spark.table(f"{ML_CATALOG}.{ML_SHARED}.dataset_manifest")
        .where((F.col("ecosystem") == eco) & F.col("status").isin(*ML_FIT_STATES))
        .collect()
    )
    contract = {}
    for r in (
        spark.table(f"{ML_CATALOG}.{ML_SHARED}.feature_contract")
        .where(F.col("ecosystem") == eco)
        .select("dataset_id", "column_name", "role")
        .collect()
    ):
        contract.setdefault(r["dataset_id"], []).append((r["column_name"], r["role"]))
    results = {}
    for row in sorted(manifest, key=lambda r: r["dataset_id"]):
        ds = row["dataset_id"]
        frame = spark.table(f"{ML_CATALOG}.{eco}_ml_datasets.dataset_{row['family']}")
        frame = frame.where(F.col("dataset_id") == ds)
        results[ds] = ml_profile_dataset(frame, ml_roles(sorted(contract.get(ds, []))))
        r = results[ds]
        print(
            f"{eco}/{ds}: target={r['target']} kind={r['kind']} "
            f"features={r['features']} dominant={len(r.get('dominant', {}))} "
            f"pairs={len(r['pairs'])}"
        )
    write_profiling("ml", eco, f"ML datasets -- {eco}", ml_eda_blocks(results))
    return results
