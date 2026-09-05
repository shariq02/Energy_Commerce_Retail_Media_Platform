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

import matplotlib
import matplotlib.pyplot as plt
from pyspark.sql import functions as F

matplotlib.use("Agg")  # headless render on serverless; plt.show() is a no-op

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


def key_like_cols(cols, suffix="mastrnummer"):
    return [c for c in cols if c.lower().endswith(suffix)]


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


def barplot(
    pairs, title, xlabel, ylabel="count", rot=0, figsize=(10, 4), filename=None
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
    _apply_xlabels(ax, [p[0] for p in pairs], rot)
    ax.set_title(title + (f"  (+{hidden} more not shown)" if hidden else ""))
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    fig.tight_layout()
    if filename:
        fig.savefig(fig_path(filename), dpi=110, bbox_inches="tight")
    plt.close(fig)
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
    fig.savefig(fig_path(filename), dpi=110, bbox_inches="tight")
    plt.close(fig)
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
            F.countDistinct(F.col(c)).alias(c + "__d"),
            F.sum(F.col(c).isNull().cast("long")).alias(c + "__nulls"),
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
        for r in df.select(F.col(colname).cast("string")).distinct().collect()
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
    s = F.col(colname).cast("string")
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
    s = F.col(colname).cast("string")
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
    s = F.col(colname).cast("string")
    norm = F.trim(s)
    if decimal_comma:
        # German convention: "1.234,56" -> "1234.56". Drop a dot only when it
        # groups thousands; turn a decimal comma into a dot.
        norm = F.regexp_replace(norm, r"\.(?=\d{3}(\D|$))", "")
        norm = F.regexp_replace(norm, r"(?<=\d),(?=\d)", ".")
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
    v = F.col(colname).cast("double")
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


def continuity_grid(df, ts_col, step, entity_col=None, grid_start=None, grid_end=None):
    # Coverage of a fixed-step series measured against an INDEPENDENT expected
    # grid: expected = floor((max_ts - min_ts) / step) + 1, NOT the observed
    # distinct-timestamp count (which makes coverage tautologically 100%).
    # Pass grid_start/grid_end (ISO strings) to measure against a known
    # collection window instead of the series' own first/last row.
    step_s = _STEP_SECONDS.get(step, step if isinstance(step, int) else None)
    if step_s is None:
        raise ValueError(f"unknown step: {step!r}")
    t = (
        parse_ts_multi(ts_col)
        if dict(df.dtypes).get(ts_col) == "string"
        else F.col(ts_col)
    )
    epoch = F.unix_timestamp(t.cast("timestamp"))
    base = df.select(
        epoch.alias("e"), *([F.col(entity_col).alias("k")] if entity_col else [])
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
    g = base.groupBy(*gkeys).agg(
        F.min("e").alias("mn"),
        F.max("e").alias("mx"),
        F.countDistinct("e").alias("obs"),
    )
    lo = start_e if start_e is not None else F.col("mn")
    hi = end_e if end_e is not None else F.col("mx")
    g = g.withColumn("expected", F.floor((hi - lo) / F.lit(step_s)) + F.lit(1))
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
        }
        for r in rows
    }
    covs = [
        v["coverage_pct"] for v in per_entity.values() if v["coverage_pct"] is not None
    ]
    return {
        "step_seconds": step_s,
        "grid_start": grid_start,
        "grid_end": grid_end,
        "per_entity": per_entity,
        "coverage_min": min(covs) if covs else None,
        "coverage_max": max(covs) if covs else None,
    }


# COMMAND ----------

# DBTITLE 1,Spatial validity + categorical domain + coverage bias


def spatial_validity(df, lat_col, lon_col, bbox=DE_BBOX, name="coords"):
    lat, lon = F.col(lat_col).cast("double"), F.col(lon_col).cast("double")
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


def categorical_domain(df, colname, allowed, name=None):
    present = {
        r[0]
        for r in df.select(F.col(colname).cast("string")).distinct().collect()
        if r[0] is not None
    }
    allowed = {str(a) for a in allowed}
    return {
        "column": name or colname,
        "unexpected": sorted(present - allowed)[:25],
        "unexpected_count": len(present - allowed),
        "unused_allowed": sorted(allowed - present)[:25],
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


def cross_source_overlap(spans):
    # spans: {source: (min_iso, max_iso)}. Returns the common window shared by
    # ALL sources (empty if any pair is disjoint) plus each pairwise overlap in
    # days -- the ceiling on any study that joins these sources on time.
    def _d(x):
        return _dt.date.fromisoformat(str(x)[:10])

    parsed = {k: (_d(a), _d(b)) for k, (a, b) in spans.items() if a and b}
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
