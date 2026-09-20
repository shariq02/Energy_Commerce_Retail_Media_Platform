# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA SHARED LIBRARY -- SAMPLES-HOSTED SOURCES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Discovery and profiling plumbing for sources that are read
# MAGIC in place from the Databricks Samples Volume / catalog instead of a Bronze
# MAGIC table: recursive file listing, file-kind classification, delimiter
# MAGIC sniffing, schema-grouped multi-path reads, and single-pass profile /
# MAGIC numeric scans. Pulled in with `%run ../_samples_eda_common` after
# MAGIC `%run ../_eda_common`. Definitions only -- no side effects at import; the
# MAGIC caller owns `spark` / `dbutils`.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Constants
SAMPLES_VOLUME_ROOT = "/Volumes/samples/databricks/datasets"
SUPPORT_HINTS = ("readme", "license", "licence", "description", "citation", "notice")
DELIMITERS = {"\t": "tab", ";": "semicolon", ",": "comma", "|": "pipe"}
SEP_CHOICES = ("\t", ";", ",", "|")

# COMMAND ----------

# DBTITLE 1,File listing


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


# COMMAND ----------

# DBTITLE 1,Delimiter sniffing + schema-grouped reads


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


# COMMAND ----------

# DBTITLE 1,Column helpers


def qcol(name):
    return F.col("`" + str(name).replace("`", "``") + "`")


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


# COMMAND ----------

# DBTITLE 1,Single-pass profile + numeric scan


def profile_frame(df):
    # Rows, per-column missingness and approx distinct in ONE aggregation.
    cols = df.columns
    exprs = [F.count(F.lit(1)).alias("__rows")]
    for i, c in enumerate(cols):
        exprs += [
            F.sum(is_missing(c).cast("long")).alias(f"m{i}"),
            F.approx_count_distinct(as_str(c)).alias(f"d{i}"),
        ]
    r = df.agg(*exprs).first().asDict()
    return {
        "cols": cols,
        "total": r["__rows"],
        "miss": {c: r[f"m{i}"] or 0 for i, c in enumerate(cols)},
        "acd": {c: r[f"d{i}"] for i, c in enumerate(cols)},
    }


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


def constant_cols(df, prof):
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


# COMMAND ----------

# DBTITLE 1,Epoch timestamps


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


# COMMAND ----------

# DBTITLE 1,Volume directory loader


def load_volume_frames(root, max_depth=3):
    # Lists a Volume directory and returns (entries, kinds, frames). One frame
    # per distinct delimited header (or per JSON / Parquet file set), each with
    # a `__file` column so multi-file datasets stay separable.
    entries = list_tree(root, max_depth)
    kinds = classify_files(entries)
    frames = {}
    delim = kinds.get("delimited", [])
    for i, (first, paths) in enumerate(group_delimited(delim).items(), start=1):
        sep = sniff_sep(first)
        frames[f"delimited_{i}"] = {
            "df": read_delimited(paths, sep),
            "paths": paths,
            "sep": DELIMITERS.get(sep, sep),
        }
    js = [e["path"] for e in kinds.get("json", [])]
    if js:
        frames["json"] = {"df": read_json_any(js), "paths": js, "sep": None}
    pq = [e["path"] for e in kinds.get("parquet", [])]
    if pq:
        frames["parquet"] = {"df": read_parquet_any(pq), "paths": pq, "sep": None}
    return entries, kinds, frames


def read_support_text(kinds, n=80):
    # First lines of each support file (README / licence) -- provenance evidence.
    out = {}
    for e in kinds.get("support", []):
        out[e["name"]] = head_lines(e["path"], n)
    return out


# COMMAND ----------

# DBTITLE 1,Markdown helpers


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
