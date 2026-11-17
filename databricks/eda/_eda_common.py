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
import math
import os as _os
import re as _re

import matplotlib.pyplot as plt
import yaml
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

# DBTITLE 1,Bronze read (tables merged into another table)
BRONZE_MERGES = _os.path.join("src", "schemas", "mappings", "bronze_merges.yml")


def bronze_frame(full_name):
    """A Bronze table by its full name; a table merged into another
    (bronze_merges.yml) is read from the merged table, filtered by its value. The
    `kept` values of an appended table are named `<source>_<value>`."""
    catalog, schema, table = full_name.split(".")
    with open(_os.path.join(_repo_root(), BRONZE_MERGES), encoding="utf-8") as fh:
        merges = yaml.safe_load(fh)["merges"]
    for merge in merges:
        column = merge["discriminator"]
        if not column or merge["schema"] != schema:
            continue
        source = merge["table"].rsplit("_", 1)[0]
        names = {part: value for value, part in merge["parts"].items()}
        names.update({f"{source}_{value}": value for value in merge.get("kept", [])})
        if table in names:
            return (
                spark.table(f"{catalog}.{schema}.{merge['table']}")
                .filter(F.col(column) == names[table])
                .drop(column)
            )
    return spark.table(full_name)


# COMMAND ----------

# DBTITLE 1,Small formatting + column helpers


def find_col(df, *cands):
    low = {c.lower(): c for c in df.columns}
    for x in cands:
        if x.lower() in low:
            return low[x.lower()]
    return None


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


def key_like_cols(cols, suffix="mastrnummer"):
    return [c for c in cols if c.lower().endswith(suffix)]


def full_row_dup_count(df, total=None):
    # Exact full-row duplicate count. Hash each row to ONE bigint before the
    # distinct shuffle so the shuffle moves 8 bytes/row instead of the whole
    # (often 60-column) row -- a big saving on the wide multi-million-row tables.
    # xxhash64 collisions are negligible even at 10^7 rows.
    if total is None:
        total = df.count()
    distinct = (
        df.select(F.xxhash64(*[qcol(c) for c in df.columns]).alias("__h"))
        .distinct()
        .count()
    )
    return total - distinct


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


def _save_and_show(fig):
    # Render the figure inline and close it; a figure is never written to a file,
    # so no data reaches the repository. Prefer Databricks' display() -- it
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


def histplot(
    values, title, xlabel, bins=50, logy=False, figsize=(10, 4), filename=None
):
    values = [x for x in values if x is not None]
    if not values:
        print(f"  histplot: no data -> {filename} (figure not written)")
        return False
    fig, ax = plt.subplots(figsize=figsize)
    ax.hist(values, bins=bins, log=logy)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("count")
    fig.tight_layout()
    _save_and_show(fig)
    return True


def lineplot(
    pairs, title, xlabel, ylabel="value", rot=90, figsize=(13, 4), filename=None
):
    # pairs: [(x_label, y), ...] plotted in the given order.
    pairs = [p for p in pairs if p is not None]
    if not pairs:
        print(f"  lineplot: no data -> {filename} (figure not written)")
        return False
    fig, ax = plt.subplots(figsize=figsize)
    ax.plot(range(len(pairs)), [p[1] for p in pairs], linewidth=0.9)
    step = max(1, len(pairs) // MAX_XTICKS)
    ax.set_xticks(range(0, len(pairs), step))
    ax.set_xticklabels(
        [str(pairs[i][0]) for i in range(0, len(pairs), step)],
        rotation=rot,
        ha="right" if rot else "center",
        fontsize=7,
    )
    ax.set_title(title)
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


def facet_hists(groups, suptitle, filename, bins=40, ncols=3, logy=True):
    # {panel_title: [values]} -> one histogram per panel. A panel with no
    # (non-None) values is dropped; the whole figure is skipped (returns False,
    # nothing written) when every panel is empty.
    def _mk(vals):
        vals = [v for v in (vals or []) if v is not None]
        if not vals:
            return None

        def draw(ax):
            ax.hist(vals, bins=bins, log=logy)

        return draw

    src = groups.items() if hasattr(groups, "items") else groups
    return _facet_grid([(k, _mk(v)) for k, v in src], suptitle, filename, ncols)


def lines_grid(series, suptitle, filename, ncols=4, panel=(4.0, 2.6)):
    # series: {panel_title: [(x, y), ...]}. Points are plotted in the given
    # order -- callers must pass chronologically sorted points for a real time
    # series (row order out of Spark is arbitrary).
    def _mk(pts):
        pts = list(pts)
        if not pts:
            return None

        def draw(ax):
            ax.plot([p[0] for p in pts], [p[1] for p in pts], linewidth=0.6)

        return draw

    src = series.items() if hasattr(series, "items") else series
    return _facet_grid([(k, _mk(v)) for k, v in src], suptitle, filename, ncols, panel)


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


def collect_key_set(df, colname):
    return {
        r[0]
        for r in df.select(qcol(colname).cast("string")).distinct().collect()
        if r[0] is not None and str(r[0]).strip() != ""
    }


# COMMAND ----------

# DBTITLE 1,Referential integrity + interpretation


def referential_integrity(child_vals, parent_vals, child="child", parent="parent"):
    child_vals = {v for v in child_vals if v is not None and str(v).strip() != ""}
    parent_vals = {v for v in parent_vals if v is not None and str(v).strip() != ""}
    matched = child_vals & parent_vals
    rate = len(matched) / len(child_vals) if child_vals else None
    return {
        "child": child,
        "parent": parent,
        "child_distinct": len(child_vals),
        "parent_distinct": len(parent_vals),
        "orphans": len(child_vals - parent_vals),
        "unused_parent": len(parent_vals - child_vals),
        "match_rate": round(rate, 4) if rate is not None else None,
        "orphan_sample": sorted(str(x) for x in (child_vals - parent_vals))[:10],
    }


def ri_interpretation(ri):
    out = []
    if ri["child_distinct"] == 0:
        return [
            f"`{ri['child']}` has no usable key values -- integrity not assessable."
        ]
    if ri["orphans"]:
        miss = 1 - (ri["match_rate"] or 0)
        out.append(
            f"{ri['orphans']} of {ri['child_distinct']} `{ri['child']}` keys ({miss:.1%}) "
            f"have no `{ri['parent']}` row -> an INNER join silently drops those child rows; "
            "use a LEFT join with an explicit unmatched flag, and treat the orphan rate as a "
            "data-quality signal, not noise."
        )
    else:
        out.append(
            f"every `{ri['child']}` key resolves to a `{ri['parent']}` row -> an inner join on "
            "this key keeps all child rows."
        )
    if ri["unused_parent"]:
        out.append(
            f"{ri['unused_parent']} `{ri['parent']}` keys are never referenced by "
            f"`{ri['child']}` -> fine for a dimension; a right/outer join would add all-null "
            "child rows."
        )
    out.append(
        "set-membership only -- this does NOT rule out fan-out; a parent key repeated in the "
        "child multiplies the parent's attributes across child rows, so confirm row-level 1:1 "
        "vs 1:N before joining at scale."
    )
    return out


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


def numeric_parseability(df, colname, decimal_comma=True):
    # A column is only "numeric" if ~all of its non-empty values parse as a
    # double (after optional German comma-decimal normalisation). This stops a
    # free-text column (e.g. an affected-plant name) being reported as a numeric
    # column with "0 of N parsed".
    s = qcol(colname).cast("string")
    norm = F.trim(s)
    if decimal_comma:
        # German convention: "1.234,56" -> "1234.56" (lookaround-free -- see
        # _de_number -- so the operator stays Photon-accelerated).
        norm = _de_number(norm)
    d = df.select(
        (s.isNotNull() & (F.trim(s) != "")).cast("long").alias("__nn"),
        norm.alias("__norm"),
    ).select(
        F.col("__nn"),
        F.expr("try_cast(__norm as double)").alias("__v"),
    )
    r = (
        d.agg(
            F.sum("__nn").alias("non_null"),
            F.sum(F.col("__v").isNotNull().cast("long")).alias("parsed"),
            F.min("__v").alias("min"),
            F.max("__v").alias("max"),
        )
        .first()
        .asDict()
    )
    nn = r["non_null"] or 0
    y = r["parsed"] / nn if nn else 0.0
    return {
        "column": colname,
        "non_null": nn,
        "parsed": r["parsed"],
        "yield": round(y, 4),
        "is_numeric": nn > 0 and y >= 0.95,
        "min": r["min"],
        "max": r["max"],
    }


# COMMAND ----------

# DBTITLE 1,Value plausibility + sign / mirror-column checks


def plausibility(df, colname, lo=None, hi=None, sentinels=(-999.0,)):
    v = safe_num(colname)
    is_sent = F.lit(False)
    for x in sentinels:
        is_sent = is_sent | (v == F.lit(float(x)))
    good = v.isNotNull() & ~is_sent
    aggs = [
        F.sum(is_sent.cast("long")).alias("sentinel"),
        F.sum((good & (v < 0)).cast("long")).alias("negative"),
        F.sum((good & (v == 0)).cast("long")).alias("zero"),
        F.min(F.when(good, v)).alias("min"),
        F.max(F.when(good, v)).alias("max"),
        F.avg(F.when(good, v)).alias("mean"),
        F.stddev(F.when(good, v)).alias("sd"),
    ]
    if lo is not None:
        aggs.append(F.sum((good & (v < F.lit(lo))).cast("long")).alias("below"))
    if hi is not None:
        aggs.append(F.sum((good & (v > F.lit(hi))).cast("long")).alias("above"))
    r = df.agg(*aggs).first().asDict()
    r["column"] = colname
    r["bounds"] = (lo, hi)
    return r


def mirror_columns(stats):
    # stats: {col: {'mean':, 'sd':, ...}}. Flags column pairs where one is an
    # exact copy or an exact sign-flip of the other -- a circular feature/target
    # risk that a correlation matrix alone would only hint at.
    out = []
    cols = [c for c in stats if stats[c].get("sd") is not None]
    for i, a in enumerate(cols):
        for b in cols[i + 1 :]:
            sa, sb = stats[a], stats[b]
            tol_sd = 1e-6 * max(abs(sa["sd"]), abs(sb["sd"]), 1.0)
            tol_m = 1e-6 * max(abs(sa["mean"]), abs(sb["mean"]), 1.0)
            if abs(sa["sd"] - sb["sd"]) > tol_sd:
                continue
            if abs(sa["mean"] - sb["mean"]) <= tol_m:
                out.append(
                    (a, b, "identical distribution -- one column duplicates the other")
                )
            elif abs(sa["mean"] + sb["mean"]) <= tol_m:
                out.append((a, b, "exact sign mirror -- one column is -1x the other"))
    return out


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


def categorical_domain(df, colname, allowed, name=None, normalize=None):
    # `normalize` (optional callable) is applied to BOTH the observed values and
    # `allowed` before comparing -- use it when the source encodes a value
    # differently from the reference list (umlaut transliteration, spacing,
    # concatenation, e.g. BNetzA's "BadenWuerttemberg" vs "Baden-Wurttemberg").
    norm = normalize or (lambda x: x)
    present = {
        r[0]
        for r in df.select(qcol(colname).cast("string")).distinct().collect()
        if r[0] is not None and str(r[0]).strip() != ""
    }
    present_n = {norm(v) for v in present}
    allowed_n = {norm(str(a)) for a in allowed}
    unexpected = sorted(v for v in present if norm(v) not in allowed_n)
    unused = sorted(str(a) for a in allowed if norm(str(a)) not in present_n)
    return {
        "column": name or colname,
        "unexpected": unexpected[:25],
        "unexpected_count": len(unexpected),
        "unused_allowed": unused[:25],
    }


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


def dwd_timestamp(col):
    # DWD MESS_DATUM: yyyyMMddHH, a double-inferred "2025021300.0", or minutes.
    # try_to_timestamp gives NULL on bad input; to_timestamp throws under ANSI.
    s = F.regexp_replace(F.col(col).cast("string"), r"\.0$", "")
    return F.coalesce(
        F.try_to_timestamp(s, F.lit("yyyyMMddHH")),
        F.try_to_timestamp(s, F.lit("yyyyMMddHHmm")),
        F.try_to_timestamp(F.substring(s, 1, 10), F.lit("yyyyMMddHH")),
    )


def _norm_token(s):
    # Lower-case, drop every non-alphanumeric char -- so "Betriebs-Status" and
    # "einheitbetriebsstatus" can be compared as substrings.
    return _re.sub(r"[^a-z0-9]", "", str(s).lower())


def load_mastr_catalog(spark, catalog, schema="bronze"):
    # Build the authoritative code sets from mastr_katalogwerte (+ its category
    # names from mastr_katalogkategorien). Returns the global set of valid
    # value-ids and, per category name, the ids in that category. The MaStR
    # Gesamtdatenexport carries no explicit column -> category binding, so a
    # column is matched to a category by NAME later (best-effort); membership in
    # the global id set is the weaker but always-available fallback.
    kw = bronze_frame(f"{catalog}.{schema}.mastr_katalogwerte")
    kk = spark.table(f"{catalog}.{schema}.mastr_katalogkategorien")
    kw_id = find_col(kw, "Id", "KatalogWertId", "Wert_Id", "KatalogwertId")
    kw_cat = next((c for c in kw.columns if "kategorie" in c.lower()), None)
    kw_name = find_col(kw, "Wert", "Name", "Bezeichnung", "Beschreibung")
    kk_id = find_col(kk, "Id", "KatalogKategorieId", "KatalogkategorieId")
    kk_name = find_col(kk, "Name", "Bezeichnung", "Kategorie", "KategorieName")
    cat_name = {}
    if kk_id and kk_name:
        cat_name = {r[kk_id]: r[kk_name] for r in kk.select(kk_id, kk_name).collect()}
    all_ids = set()
    by_category = {}
    labels = {}
    sel = [c for c in (kw_id, kw_cat, kw_name) if c]
    for r in kw.select(*sel).collect():
        vid = None if r[kw_id] is None else str(r[kw_id]).strip()
        if vid in (None, ""):
            continue
        all_ids.add(vid)
        cn = cat_name.get(r[kw_cat], str(r[kw_cat])) if kw_cat else "__all"
        by_category.setdefault(cn, set()).add(vid)
        if kw_name:
            labels.setdefault(cn, {})[vid] = r[kw_name]
    return {
        "all_ids": all_ids,
        "by_category": by_category,
        "category_labels": labels,
        "n_values": len(all_ids),
        "n_categories": len(by_category),
    }


def reconcile_codes(df, colname, catalog, category_hint=None):
    # Compare the distinct values of a low-cardinality code column against the
    # MaStR reference catalog. "coded" is True only when every non-empty value is
    # an integer literal -- a free-text column is not a code column and is
    # returned as coded=False (no false "unknown code" noise). When the column
    # name matches a catalog category name, the check is scoped to that category;
    # otherwise it falls back to global value-id membership, which cannot prove a
    # code is valid FOR THIS column, only that MaStR uses it somewhere.
    vc = df.groupBy(qcol(colname).cast("string").alias("v")).count().collect()
    counts = {}
    for r in vc:
        v = r["v"].strip() if r["v"] is not None else None
        counts[v] = counts.get(v, 0) + r["count"]
    present = [v for v in counts if v not in (None, "")]
    coded = bool(present) and all(_re.fullmatch(r"-?\d+", v) for v in present)
    if not coded:
        return {"column": colname, "coded": False, "distinct": len(present)}
    hint = _norm_token(category_hint or colname)
    cat_key, ref = None, catalog["all_ids"]
    for k in catalog["by_category"]:
        nk = _norm_token(k)
        if nk and len(nk) >= 4 and (nk in hint or hint in nk):
            cat_key, ref = k, catalog["by_category"][k]
            break
    unknown = sorted((v for v in present if v not in ref), key=lambda v: -counts[v])
    return {
        "column": colname,
        "coded": True,
        "distinct": len(present),
        "matched_category": cat_key,
        "checked_against": "category" if cat_key else "all catalog value-ids",
        "unknown_values": unknown[:25],
        "unknown_count": len(unknown),
        "unknown_rows": sum(counts[v] for v in unknown),
        "total_rows": sum(counts[v] for v in present),
    }


def cardinality_profile(child_df, ccol, child_key_set, parent_set):
    # Full relationship cardinality, not just an orphan rate: child rows, distinct
    # child keys, how many parent entities are actually referenced, and the
    # child-rows-per-parent distribution (p50/p90/p99 + max fan-out). `ccol` is
    # the FK on the child; because the FK value IS the parent key, rows grouped by
    # `ccol` is exactly the per-parent child count.
    key = qcol(ccol).cast("string")
    per_key = (
        child_df.where(key.isNotNull() & (F.trim(key) != ""))
        .groupBy(key.alias("k"))
        .count()
    )
    s = (
        per_key.agg(
            F.count(F.lit(1)).alias("keys"),
            F.sum("count").alias("rows"),
            F.max("count").alias("mx"),
            F.expr("percentile_approx(count, 0.5)").alias("p50"),
            F.expr("percentile_approx(count, 0.9)").alias("p90"),
            F.expr("percentile_approx(count, 0.99)").alias("p99"),
        )
        .first()
        .asDict()
    )
    matched = child_key_set & parent_set
    return {
        "child_rows": int(s["rows"] or 0),
        "distinct_child_keys": int(s["keys"] or 0),
        "matched_parent_keys": len(matched),
        "orphan_child_keys": len(child_key_set - parent_set),
        "parent_keys_total": len(parent_set),
        "parent_keys_referenced_pct": (
            round(len(matched) / len(parent_set) * 100, 3) if parent_set else None
        ),
        "child_rows_per_parent_p50": int(s["p50"] or 0),
        "child_rows_per_parent_p90": int(s["p90"] or 0),
        "child_rows_per_parent_p99": int(s["p99"] or 0),
        "max_fanout": int(s["mx"] or 0),
    }


def date_order_check(df, earlier_col, later_col, formats=GERMAN_TS_FORMATS, label=None):
    # Count rows where a date that must not be later than another one is. Both
    # columns are multi-format parsed; only rows where BOTH parse are comparable.
    e = parse_ts_multi(earlier_col, formats)
    ln = parse_ts_multi(later_col, formats)
    # `both` must be recomputed from the aliased columns -- the parse expressions
    # reference the source column names, which the .select() has projected away.
    both = F.col("e").isNotNull() & F.col("l").isNotNull()
    r = (
        df.select(e.alias("e"), ln.alias("l"))
        .agg(
            F.sum(both.cast("long")).alias("comparable"),
            F.sum((both & (F.col("e") > F.col("l"))).cast("long")).alias("bad"),
        )
        .first()
        .asDict()
    )
    comp = r["comparable"] or 0
    bad = r["bad"] or 0
    return {
        "label": label or f"{earlier_col} <= {later_col}",
        "earlier": earlier_col,
        "later": later_col,
        "comparable_rows": comp,
        "violations": bad,
        "violation_pct": round(bad / comp * 100, 3) if comp else None,
    }


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


def monotonic_series_check(df, value_col, order_col, partition_col=None):
    # Count steps where a series that must be non-decreasing (a cumulative meter)
    # goes DOWN, ordered by `order_col` within each `partition_col`. Returns the
    # comparable step count, the decreasing-step count, and the largest drop.
    from pyspark.sql import Window as _W

    v = safe_num(value_col)
    keys = [partition_col] if partition_col else []
    win = (
        _W.partitionBy(*[qcol(k) for k in keys]).orderBy(qcol(order_col))
        if keys
        else _W.orderBy(qcol(order_col))
    )
    d = df.select(
        *[qcol(k) for k in keys],
        v.alias("__v"),
        (v - F.lag(v).over(win)).alias("__d"),
    )
    r = (
        d.agg(
            F.sum(F.col("__d").isNotNull().cast("long")).alias("steps"),
            F.sum((F.col("__d") < 0).cast("long")).alias("down"),
            F.min("__d").alias("min_delta"),
        )
        .first()
        .asDict()
    )
    steps = r["steps"] or 0
    return {
        "column": value_col,
        "comparable_steps": steps,
        "decreasing_steps": r["down"] or 0,
        "decreasing_pct": round((r["down"] or 0) / steps * 100, 4) if steps else None,
        "largest_drop": r["min_delta"],
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


def additive_identity_check(
    df, lhs_col, rhs_cols, rel_tol=0.02, abs_floor=1.0, signs=None
):
    # Test a claimed identity  lhs ~ sum(sign_i * rhs_col_i)  row-by-row. `signs`
    # (list of +1 / -1, one per rhs col; default all +1) lets a subtraction like
    # residual_load = load - wind - pv be expressed without synthetic columns.
    # Returns the share of comparable rows whose relative residual exceeds
    # rel_tol (with an absolute floor), plus residual percentiles.
    signs = signs or [1] * len(rhs_cols)
    lhs = safe_num(lhs_col)
    rhs = None
    for c, sg in zip(rhs_cols, signs):
        term = safe_num(c) * F.lit(int(sg))
        rhs = term if rhs is None else rhs + term
    comparable = lhs.isNotNull() & rhs.isNotNull()
    resid = lhs - rhs
    denom = F.greatest(F.abs(lhs), F.lit(float(abs_floor)))
    rel = F.abs(resid) / denom
    r = (
        df.select(
            comparable.alias("__c"),
            resid.alias("__r"),
            rel.alias("__rel"),
        )
        .agg(
            F.sum(F.col("__c").cast("long")).alias("comparable"),
            F.sum((F.col("__c") & (F.col("__rel") > rel_tol)).cast("long")).alias(
                "bad"
            ),
            F.expr("percentile_approx(__r, array(0.01, 0.5, 0.99))").alias("resid_p"),
            F.max(F.abs(F.col("__r"))).alias("max_abs_resid"),
        )
        .first()
        .asDict()
    )
    comp = r["comparable"] or 0
    _terms = " ".join(
        f"{'-' if sg < 0 else '+'} {c}" for c, sg in zip(rhs_cols, signs)
    ).lstrip("+ ")
    return {
        "identity": f"{lhs_col} = {_terms}",
        "comparable_rows": comp,
        "violations": r["bad"] or 0,
        "violation_pct": round((r["bad"] or 0) / comp * 100, 4) if comp else None,
        "residual_p01_p50_p99": r["resid_p"],
        "max_abs_residual": r["max_abs_resid"],
        "rel_tol": rel_tol,
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


def iso_midpoint(a, b):
    # Midpoint (naive ISO string, space separator -- Spark-castable) between two
    # ISO-ish timestamp strings/datetimes; None if either cannot be parsed.
    pa, pb = parse_ts_py(a), parse_ts_py(b)
    if pa is None or pb is None:
        return None
    lo, hi = sorted((pa, pb))
    return (lo + (hi - lo) / 2).replace(microsecond=0, tzinfo=None).isoformat(sep=" ")


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


def hint_cols(cols, hints):
    return [c for c in cols if any(h in c.lower() for h in hints)]


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


def hist_counts(df, name, lo, hi, bins=40):
    # Fixed-width histogram computed in Spark (no collect of raw values).
    if lo is None or hi is None or hi <= lo:
        return []
    w = (hi - lo) / bins
    idx = F.floor((to_double(name) - F.lit(float(lo))) / F.lit(float(w))).cast("int")
    b = F.least(idx, F.lit(bins - 1))
    rows = (
        df.select(b.alias("__b"))
        .where(F.col("__b").isNotNull())
        .groupBy("__b")
        .count()
        .collect()
    )
    m = {r["__b"]: r["count"] for r in rows}
    return [(f"{lo + i * w:.3g}", m.get(i, 0)) for i in range(bins)]


def epoch_scan(df, name):
    # Numeric timestamp columns: infer the unit from the magnitude, then report
    # range / granularity in UTC.
    v = to_double(name)
    r = df.agg(
        F.min(v).alias("lo"),
        F.max(v).alias("hi"),
        F.sum(v.isNotNull().cast("long")).alias("n"),
    ).first()
    hi, n = r["hi"], r["n"] or 0
    if not n or hi is None:
        return {"column": name, "n": 0}
    top = abs(hi)
    unit = "seconds"
    div = 1.0
    if top >= 1e17:
        unit, div = "nanoseconds", 1e9
    elif top >= 1e14:
        unit, div = "microseconds", 1e6
    elif top >= 1e11:
        unit, div = "milliseconds", 1e3
    ts = F.to_timestamp(F.from_unixtime((v / F.lit(div)).cast("long")))
    g = (
        df.select(ts.alias("__t"))
        .agg(
            F.min("__t").alias("min_ts"),
            F.max("__t").alias("max_ts"),
            F.countDistinct(F.to_date("__t")).alias("distinct_days"),
            F.countDistinct("__t").alias("distinct_ts"),
        )
        .first()
    )
    return {
        "column": name,
        "n": n,
        "unit": unit,
        "min_ts": str(g["min_ts"]),
        "max_ts": str(g["max_ts"]),
        "distinct_days": g["distinct_days"],
        "distinct_ts": g["distinct_ts"],
        "div": div,
    }


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
