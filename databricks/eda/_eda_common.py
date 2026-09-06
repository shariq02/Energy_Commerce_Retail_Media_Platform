# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
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
# MAGIC notebook. Pulled in with `%run ../_eda_common`. Definitions only -- no
# MAGIC side effects at import; the caller owns `spark` / `dbutils`.

# COMMAND ----------

# DBTITLE 1,Imports
import contextlib
import datetime as _dt
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
        "repo root not found -- run from <repo>/databricks/eda/<source>/"
    )


def _profiling_dir():
    d = _os.path.join(_repo_root(), "src", "schemas", "profiling")
    _os.makedirs(_os.path.join(d, "figures"), exist_ok=True)
    return d


def fig_path(name):
    return _os.path.join(_profiling_dir(), "figures", name)


# COMMAND ----------

# DBTITLE 1,Small formatting + column helpers


def find_col(df, *cands):
    low = {c.lower(): c for c in df.columns}
    for x in cands:
        if x.lower() in low:
            return low[x.lower()]
    return None


def _qc(name):
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
    s = _de_number(F.trim(_qc(colname).cast("string")))
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
        df.select(F.xxhash64(*[_qc(c) for c in df.columns]).alias("__h"))
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
        *[_qc(c) for c in key_cols],
        F.xxhash64(*[_qc(c) for c in hash_cols]).alias("__h"),
    ]
    if conflict_cols != hash_cols:
        sel.append(F.xxhash64(*[_qc(c) for c in conflict_cols]).alias("__c"))
    h = df.select(*sel)
    _cv = "__c" if conflict_cols != hash_cols else "__h"
    per_key = h.groupBy(*[_qc(c) for c in key_cols]).agg(
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


def _save_and_show(fig, filename):
    # Save the PNG under src/schemas/profiling/figures/, print where it landed
    # (so a run makes it obvious whether files are being written), then render
    # it inline and close it. Prefer Databricks' display() -- it serialises the
    # figure to PNG itself, so it renders regardless of the matplotlib backend
    # (plt.show() is a silent no-op if the session's backend is non-interactive,
    # e.g. left on "Agg" by an earlier run in the same Python session).
    if filename:
        path = fig_path(filename)
        fig.savefig(path, dpi=110, bbox_inches="tight")
        size = _os.path.getsize(path) if _os.path.exists(path) else -1
        print(f"  figure saved -> {path}  ({size} bytes)")
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
    # Returns True only if a figure with data was written -- callers gate the
    # markdown figure reference on the return value so an empty result never
    # leaves a blank PNG referenced in the profile.
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
    _save_and_show(fig, filename)
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
    _save_and_show(fig, filename)
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
    _save_and_show(fig, filename)
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
    _save_and_show(fig, filename)
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
    d = _profiling_dir()
    md = _os.path.join(d, source + ".md")
    lines = [f"<!-- BEGIN {source}:{notebook_key} -->", f"## {section_title}", ""]
    for heading, body in blocks:
        if body is None or str(body).strip() == "":
            continue
        lines += [f"### {heading}", "", str(body).rstrip(), ""]
    for cap, name in figures or []:
        if not _os.path.exists(_os.path.join(d, "figures", name)):
            print(f"  profiling export: skipping absent figure {name}")
            continue
        lines += [f"### Figure -- {cap}", "", f"![{cap}](figures/{name})", ""]
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
        f"_Auto-generated by the EDA notebooks (`databricks/eda/{source}/`). One "
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
            F.countDistinct(_qc(c)).alias(c + "__d"),
            F.sum(_qc(c).isNull().cast("long")).alias(c + "__nulls"),
        ]
    r = df.agg(*aggs).first().asDict()
    n = r["__n"] or 0
    return {
        c: {
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
        for r in df.select(_qc(colname).cast("string")).distinct().collect()
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
    s = _qc(colname).cast("string")
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
    s = _qc(colname).cast("string")
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
    s = _qc(colname).cast("string")
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
        else _qc(ts_col)
    )
    epoch = F.unix_timestamp(t.cast("timestamp"))
    base = df.select(
        epoch.alias("e"), *([_qc(entity_col).alias("k")] if entity_col else [])
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
        for r in df.select(_qc(colname).cast("string")).distinct().collect()
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
    kw = spark.table(f"{catalog}.{schema}.mastr_katalogwerte")
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
    vc = df.groupBy(_qc(colname).cast("string").alias("v")).count().collect()
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
    key = _qc(ccol).cast("string")
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
        miss = _qc(c).isNull() | (F.trim(_qc(c).cast("string")) == "")
        aggs += [
            F.sum(miss.cast("long")).alias(c + "__m"),
            # approx (HLL) -- exact countDistinct per column here means an Expand
            # union of the whole table once per column (crippling on 10^8 rows);
            # the distinct count is only used to characterise the two cohorts.
            F.approx_count_distinct(_qc(c)).alias(c + "__d"),
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
    g = _qc(group_col).cast("string")
    v = _qc(value_col).cast("string")
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
        _W.partitionBy(*[_qc(k) for k in keys]).orderBy(_qc(order_col))
        if keys
        else _W.orderBy(_qc(order_col))
    )
    d = df.select(
        *[_qc(k) for k in keys],
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
        miss = _qc(c).isNull() | (F.trim(_qc(c).cast("string")) == "")
        aggs += [
            F.sum(miss.cast("long")).alias(c + "__m"),
            # approx (HLL): exact per-column countDistinct here is an Expand
            # union of the full table once per column.
            F.approx_count_distinct(_qc(c)).alias(c + "__d"),
        ]
    rows = (
        df.withColumn("__g", _qc(group_col).cast("string"))
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
