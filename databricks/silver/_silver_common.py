# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER SHARED LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the reusable Silver-transform plumbing pulled into every
# MAGIC source Silver notebook with `%run ../_silver_common` (or
# MAGIC `%run ../../_silver_common`). Definitions only -- no side effects at
# MAGIC import; the caller owns `spark` / `dbutils`.
# MAGIC
# MAGIC Silver = source-scoped, typed, cleaned, localised, governed. Primary
# MAGIC Silver tables preserve source semantics and source grain. Reference /
# MAGIC decode / bridge tables are additive and never replace the source table.
# MAGIC No ML targets/features, no segmentation, no scores, no universal
# MAGIC envelope, no cross-source facts/dimensions, no Gold grain changes, no
# MAGIC `canonical_id`.

# COMMAND ----------

# DBTITLE 1,Imports
import datetime as _dt
import os as _os

import yaml as _yaml
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Configuration constants
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "energy_silver"
QUALITY_SCHEMA = "quality"

QUARANTINE_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.quarantine"
FIELD_CLASS_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.field_class_registry"
AUDIT_TABLE = f"{CATALOG}.{QUALITY_SCHEMA}.quality_audit_log"
WATERMARK_TABLE = f"{CATALOG}.{QUALITY_SCHEMA}.pipeline_watermarks"

ECOSYSTEM = "energy"
STAGE = "silver"

# Value strings that mean "missing" in DWD measurement columns. Stripped to
# NULL before any cast. Kept here (not per-notebook) so the list is one place.
DWD_SENTINELS = ("-999", "-999.0", "-99.9", "")

# Germany bounding box (lat_min, lat_max, lon_min, lon_max) -- a generous
# envelope including the North/Baltic Sea stations and the Zugspitze.
DE_BBOX = (47.0, 55.2, 5.7, 15.1)

# Ordered Europe/Berlin + ISO timestamp formats. First format that parses a
# row wins. Mirrors databricks/eda/_eda_common.GERMAN_TS_FORMATS.
GERMAN_TS_FORMATS = (
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd HH:mm",
    "yyyy-MM-dd",
    "dd.MM.yyyy HH:mm:ss",
    "dd.MM.yyyy HH:mm",
    "dd.MM.yyyy-HH:mm",
    "dd.MM.yyyy",
    "yyyyMMddHHmm",
    "yyyyMMddHH",
    "yyyyMMdd",
)

# The 16 German Bundesland AGS codes (2 digits), keyed by the short name the
# DWD download uses for its station folders. This is the only geography spine
# resolution achievable now -- dim_geography carries no polygon geometry, so
# DWD lat/lon is retained unchanged and attribution is a curated city lookup.
BUNDESLAND_AGS = {
    "schleswig_holstein": "01",
    "hamburg": "02",
    "niedersachsen": "03",
    "bremen": "04",
    "nordrhein_westfalen": "05",
    "hessen": "06",
    "rheinland_pfalz": "07",
    "baden_wuerttemberg": "08",
    "bayern": "09",
    "saarland": "10",
    "berlin": "11",
    "brandenburg": "12",
    "mecklenburg_vorpommern": "13",
    "sachsen": "14",
    "sachsen_anhalt": "15",
    "thueringen": "16",
}

# COMMAND ----------

# DBTITLE 1,Run identity + repo-root discovery


def run_id() -> str:
    """A single deterministic-per-execution run id, timestamp-based."""
    return _dt.datetime.now(_dt.UTC).strftime("silver-%Y%m%dT%H%M%SZ")


def now_utc() -> _dt.datetime:
    return _dt.datetime.now(_dt.UTC)


def repo_root() -> str:
    """Locate the repo root from the working dir or the notebook path."""
    p = _os.path.abspath(_os.getcwd())
    for _ in range(12):
        if _os.path.isdir(_os.path.join(p, "src", "schemas")) and _os.path.isdir(
            _os.path.join(p, "databricks")
        ):
            return p
        if _os.path.dirname(p) == p:
            break
        p = _os.path.dirname(p)
    try:
        wp = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
    except Exception as exc:
        raise RuntimeError(
            "repo root not found -- run from inside the repo's Databricks Git folder"
        ) from exc
    i = wp.rfind("/databricks/")
    if i > 0:
        for cand in (wp[:i], "/Workspace" + wp[:i]):
            if _os.path.isdir(_os.path.join(cand, "src", "schemas")):
                return cand
    raise RuntimeError(
        "repo root not found -- run from inside the repo's Databricks Git folder"
    )


def load_yaml(relpath: str) -> dict:
    """Read a YAML file under the repo root."""
    with open(_os.path.join(repo_root(), relpath), encoding="utf-8") as fh:
        return _yaml.safe_load(fh)


def load_contract(source: str) -> dict:
    return load_yaml(f"src/schemas/contracts/{source}.yml")


def load_mapping(source: str) -> dict:
    return load_yaml(f"src/schemas/mappings/{source}.yml")


def contract_tables(contract: dict) -> dict:
    """name -> table entry, accepting the legacy single-table contract form."""
    if "tables" in contract:
        return {t["name"]: t for t in contract["tables"] if "columns" in t}
    if "columns" in contract:
        name = contract.get("bronze_table", contract["source"]).split(".")[-1]
        return {name: {"name": name, "columns": contract["columns"]}}
    return {}


# COMMAND ----------

# DBTITLE 1,Bronze read + typed casts


def read_bronze(table: str) -> DataFrame:
    return spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{table}")


_LOGICAL_TO_SPARK = {
    "string": "string",
    "int": "int",
    "integer": "int",
    "long": "bigint",
    "double": "double",
    "float": "double",
    "decimal": "double",
    "boolean": "boolean",
    "date": "date",
    "timestamp": "timestamp",
}


def strip_sentinels(df: DataFrame, cols, sentinels=DWD_SENTINELS) -> DataFrame:
    """Replace sentinel strings with NULL in the named columns, before any cast."""
    for c in cols:
        df = df.withColumn(
            c,
            F.when(F.trim(F.col(c)).isin(list(sentinels)), F.lit(None)).otherwise(
                F.col(c)
            ),
        )
    return df


def german_decimal(colname: str):
    """Column expr: parse a German-comma decimal string to double.

    "1.234,56" -> 1234.56 ; "12,5" -> 12.5 ; "" / null -> NULL.
    Non-numeric leftovers become NULL (the caller quarantines those).
    """
    cleaned = F.regexp_replace(
        F.regexp_replace(F.trim(F.col(colname)), r"\.", ""), r",", "."
    )
    return F.when(cleaned.rlike(r"^-?\d+(\.\d+)?$"), cleaned.cast("double")).otherwise(
        F.lit(None).cast("double")
    )


def cast_logical(df: DataFrame, columns: list[dict]) -> DataFrame:
    """Cast every contract column to its logical Spark type. `decimal`/`double`
    columns known to carry a German comma are handled by the caller before this;
    here a plain `.cast` is used."""
    for col in columns:
        name, ltype = col["name"], col["type"]
        if name not in df.columns or ltype == "string":
            continue
        df = df.withColumn(
            name, F.col(name).cast(_LOGICAL_TO_SPARK.get(ltype, "string"))
        )
    return df


# COMMAND ----------

# DBTITLE 1,Timestamp / MESS_DATUM parsing (Europe/Berlin -> UTC)


def parse_ts(colname: str, formats=GERMAN_TS_FORMATS, src_tz: str = "Europe/Berlin"):
    """Column expr: parse a wall-clock string under the ordered formats, treat
    it as `src_tz`, return a UTC timestamp. The first format that parses wins."""
    parsed = F.lit(None).cast("timestamp")
    for fmt in formats:
        parsed = F.coalesce(
            parsed, F.try_to_timestamp(F.trim(F.col(colname)), F.lit(fmt))
        )
    if src_tz.upper() == "UTC":
        return parsed
    return F.to_utc_timestamp(parsed, src_tz)


def parse_mess_datum(colname: str, src_tz: str = "UTC"):
    """DWD hourly MESS_DATUM: `yyyyMMddHH`, tolerate a trailing `.0` from a
    double-inferred column. DWD's hourly-historical reference time is UTC."""
    base = F.regexp_replace(F.trim(F.col(colname)), r"\.0$", "")
    parsed = F.try_to_timestamp(base, F.lit("yyyyMMddHH"))
    return parsed if src_tz.upper() == "UTC" else F.to_utc_timestamp(parsed, src_tz)


def parse_mess_datum_10min(colname: str, src_tz: str = "UTC"):
    """DWD solar MESS_DATUM: `yyyyMMddHHmm` (10-minute grid)."""
    base = F.regexp_replace(F.trim(F.col(colname)), r"\.0$", "")
    parsed = F.try_to_timestamp(base, F.lit("yyyyMMddHHmm"))
    return parsed if src_tz.upper() == "UTC" else F.to_utc_timestamp(parsed, src_tz)


# COMMAND ----------

# DBTITLE 1,Deterministic keys + within-group ordinal


def sha_key(*cols):
    """Column expr: a deterministic, reproducible key from the given columns
    (each a column name or a Column). Every layer uses this one mechanism --
    never monotonically_increasing_id()."""
    parts = [
        F.coalesce(
            (c if isinstance(c, Column) else F.col(c)).cast("string"), F.lit("∅")
        )
        for c in cols
    ]
    return F.sha2(F.concat_ws("¦", *parts), 256)


def within_group_ordinal(df: DataFrame, key_cols: list[str], content_cols: list[str]):
    """Add `_src_id_ord` (1-based, deterministic by a content hash) and
    `_src_id_disambiguated` (ord > 1). Used only where a source has no unique
    key -- makes the composite `source_record_id` unique and reproducible."""
    content_hash = F.sha2(
        F.concat_ws(
            "¦", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in content_cols]
        ),
        256,
    )
    w = Window.partitionBy(*key_cols).orderBy(content_hash)
    return df.withColumn("_src_id_ord", F.row_number().over(w)).withColumn(
        "_src_id_disambiguated", F.col("_src_id_ord") > 1
    )


# COMMAND ----------

# DBTITLE 1,Provenance + field-class validation


def add_provenance(
    df: DataFrame, source_system: str, sr_id_col: str, rid: str
) -> DataFrame:
    """Attach the fixed Silver identity/provenance columns. No `canonical_id`."""
    df = (
        df.withColumnRenamed(sr_id_col, "source_record_id")
        if sr_id_col != "source_record_id"
        else df
    )
    return (
        df.withColumn("source_system", F.lit(source_system))
        .withColumn("ecosystem", F.lit(ECOSYSTEM))
        .withColumn("_silver_loaded_at", F.current_timestamp())
        .withColumn("_silver_run_id", F.lit(rid))
    )


def validate_field_classes(df: DataFrame, silver_table: str) -> None:
    """Hard-fail if any output column is not classified in the central registry.
    Notebooks never write the registry -- it is a seeded build artifact."""
    short = silver_table.split(".")[-1]
    try:
        reg = spark.table(FIELD_CLASS_TABLE).filter(F.col("table_name") == short)
    except Exception as exc:
        raise RuntimeError(
            f"{FIELD_CLASS_TABLE} missing -- run databricks/silver/00_silver_setup"
        ) from exc
    classified = {r["column_name"] for r in reg.select("column_name").collect()}
    missing = sorted(set(df.columns) - classified)
    if missing:
        raise RuntimeError(
            f"field_class_registry has no entry for {short}.{{{', '.join(missing)}}} "
            f"-- update the seed (src/schemas/_generate_field_classes.py) and reload"
        )


# COMMAND ----------

# DBTITLE 1,Code decoding (source code + German label + English business label)


def decode_via_ref(
    df: DataFrame,
    code_col: str,
    ref_df: DataFrame,
    ref_code_col: str,
    ref_label_col: str,
    out_prefix: str,
    english_map: dict | None = None,
) -> DataFrame:
    """Add `<out_prefix>_code` (source code, verbatim), `<out_prefix>_label_de`
    (German label from the reference table) and `<out_prefix>` (English business
    label from `english_map`, else the German label lowered). Broadcast join."""
    r = ref_df.select(
        F.col(ref_code_col).cast("string").alias("_rc"),
        F.col(ref_label_col).cast("string").alias("_rl"),
    ).dropDuplicates(["_rc"])
    out = (
        df.withColumn(f"{out_prefix}_code", F.col(code_col).cast("string"))
        .join(F.broadcast(r), F.col(f"{out_prefix}_code") == F.col("_rc"), "left")
        .withColumnRenamed("_rl", f"{out_prefix}_label_de")
        .drop("_rc")
    )
    if english_map:
        m = F.create_map([F.lit(x) for kv in english_map.items() for x in kv])
        out = out.withColumn(f"{out_prefix}", m[F.col(f"{out_prefix}_code")])
    else:
        out = out.withColumn(
            f"{out_prefix}",
            F.lower(F.regexp_replace(F.col(f"{out_prefix}_label_de"), r"\s+", "_")),
        )
    return out


def decode_via_map(
    df: DataFrame, src_col: str, out_col: str, mapping: dict
) -> DataFrame:
    """Add `<out_col>` = English label for `src_col`'s value via an authored map;
    the source value is kept in `src_col`. Unknown values -> NULL (caller
    quarantines)."""
    m = F.create_map([F.lit(x) for kv in mapping.items() for x in kv])
    return df.withColumn(out_col, m[F.col(src_col)])


# COMMAND ----------

# DBTITLE 1,Geography helpers


def bbox_outside_de(lat_col: str, lon_col: str):
    """Column bool: coordinate present and outside the Germany bounding box."""
    lat, lon = F.col(lat_col), F.col(lon_col)
    return (
        lat.isNotNull()
        & lon.isNotNull()
        & (
            (lat < DE_BBOX[0])
            | (lat > DE_BBOX[1])
            | (lon < DE_BBOX[2])
            | (lon > DE_BBOX[3])
        )
    )


def ags_from_gemeindeschluessel(colname: str):
    """Column expr: the 2-digit Bundesland AGS prefix of an 8-digit AGS
    (Gemeindeschluessel). NULL when the value is not a plausible AGS."""
    v = F.regexp_replace(F.trim(F.col(colname)), r"\D", "")
    return F.when(F.length(v) >= 2, F.lpad(F.substring(v, 1, 2), 2, "0")).otherwise(
        F.lit(None).cast("string")
    )


# COMMAND ----------

# DBTITLE 1,Conflict resolution (byte-identical / revision / contradiction)


def resolve_conflicts(
    df: DataFrame,
    key_cols: list[str],
    content_cols: list[str],
    *,
    qn_col: str | None = None,
    valid_qn=("1", "2", "3", "5", "7", "9", "10"),
    bronze_table: str = "",
    rule_id: str = "unresolved_key_conflict",
):
    """Return (kept_df, row_quarantine_df).

    1. byte-identical rows on a key -> collapse to one;
    2. exactly one row with a valid QN and the rest invalid -> keep the valid one,
       quarantine the invalid (validity distinction, not a quality ranking);
    3. more than one distinct valid-QN row -> quarantine the whole group
       (no documented DWD authoritative rule is assumed here).

    No first() / dropDuplicates() with an arbitrary winner.
    """
    content_hash = F.sha2(
        F.concat_ws(
            "¦", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in content_cols]
        ),
        256,
    )
    d = df.withColumn("_ch", content_hash)

    # 1. collapse byte-identical
    d = d.dropDuplicates([*key_cols, "_ch"])

    grp = Window.partitionBy(*key_cols)
    d = d.withColumn("_grp_n", F.count(F.lit(1)).over(grp))
    singles = d.filter(F.col("_grp_n") == 1).drop("_ch", "_grp_n")
    multi = d.filter(F.col("_grp_n") > 1)

    if qn_col is not None:
        valid = F.trim(F.col(qn_col)).isin(list(valid_qn))
        multi = multi.withColumn("_qn_valid", valid)
        multi = multi.withColumn(
            "_valid_n", F.sum(F.col("_qn_valid").cast("int")).over(grp)
        )
        # step 2: exactly one valid QN, rest invalid -> keep the valid row
        pick_valid = multi.filter((F.col("_valid_n") == 1) & F.col("_qn_valid")).drop(
            "_ch", "_grp_n", "_qn_valid", "_valid_n"
        )
        drop_invalid = multi.filter((F.col("_valid_n") == 1) & ~F.col("_qn_valid"))
        # step 3: >1 distinct valid-QN row -> quarantine the whole group
        contradiction = multi.filter(F.col("_valid_n") != 1)
        kept = singles.withColumn("_had_key_conflict", F.lit(False)).unionByName(
            pick_valid.withColumn("_had_key_conflict", F.lit(True))
        )
        quarantined = drop_invalid.drop("_qn_valid", "_valid_n").unionByName(
            contradiction.drop("_qn_valid", "_valid_n")
        )
        reasons = F.when(
            F.trim(F.col(qn_col)).isin(list(valid_qn)),
            F.lit("unresolved_key_conflict"),
        ).otherwise(F.lit("invalid_qn_flag"))
    else:
        kept = singles.withColumn("_had_key_conflict", F.lit(False))
        quarantined = multi
        reasons = F.lit(rule_id)

    q = quarantined.drop("_grp_n").select(
        F.lit("").alias("source_system"),
        F.lit(bronze_table).alias("bronze_table"),
        F.concat_ws("¦", *[F.col(c).cast("string") for c in key_cols]).alias(
            "source_record_id"
        ),
        reasons.alias("rule_id"),
        reasons.alias("reason"),
        F.lit(",".join(content_cols)).alias("field_name"),
        F.col("_ch").alias("offending_value"),
    )
    return kept, q


# COMMAND ----------

# DBTITLE 1,Quarantine writers (slim -- never a whole-row JSON dump)

_Q_COLS = [
    "source_system",
    "bronze_table",
    "source_record_id",
    "rule_id",
    "reason",
    "field_name",
    "offending_value",
    "run_id",
    "quarantined_at",
]


def row_quarantine(
    df: DataFrame,
    predicate,
    *,
    rule_id: str,
    reason: str,
    field_name: str,
    value_col: str,
    sr_id_col: str,
    source_system: str,
    bronze_table: str,
):
    """Split `df` on `predicate`: return (kept_df, quarantine_rows). The offending
    rows are excluded from the primary Silver table; recoverable from Bronze."""
    bad = df.filter(predicate)
    kept = df.filter(~predicate | predicate.isNull())
    q = _q_rows(
        bad,
        rule_id,
        reason,
        field_name,
        value_col,
        sr_id_col,
        source_system,
        bronze_table,
    )
    return kept, q


def value_quarantine(
    df: DataFrame,
    predicate,
    *,
    flag_col: str,
    rule_id: str,
    reason: str,
    field_name: str,
    value_col: str,
    sr_id_col: str,
    source_system: str,
    bronze_table: str,
):
    """Row stays in Silver; add `flag_col` (bool) marking the offending rows and
    record them slim in quarantine. Use for a bad value on an otherwise-valid row."""
    flagged = df.withColumn(flag_col, F.coalesce(predicate, F.lit(False)))
    q = _q_rows(
        df.filter(predicate),
        rule_id,
        reason,
        field_name,
        value_col,
        sr_id_col,
        source_system,
        bronze_table,
    )
    return flagged, q


def _q_rows(
    bad, rule_id, reason, field_name, value_col, sr_id_col, source_system, bronze_table
):
    return bad.select(
        F.lit(source_system).alias("source_system"),
        F.lit(bronze_table).alias("bronze_table"),
        F.col(sr_id_col).cast("string").alias("source_record_id"),
        F.lit(rule_id).alias("rule_id"),
        F.lit(reason).alias("reason"),
        F.lit(field_name).alias("field_name"),
        F.col(value_col).cast("string").alias("offending_value"),
    )


def write_quarantine(q: DataFrame, rid: str) -> None:
    if q is None:
        return
    out = q.withColumn("run_id", F.lit(rid)).withColumn(
        "quarantined_at", F.current_timestamp()
    )
    out.select(*_Q_COLS).write.format("delta").mode("append").saveAsTable(
        QUARANTINE_TABLE
    )


# COMMAND ----------

# DBTITLE 1,Audit + watermark + Silver write


def audit(
    component: str,
    source: str,
    metric_name: str,
    metric_value: float | None,
    *,
    threshold: float | None = None,
    status: str = "PASS",
    error: str | None = None,
    rid: str,
) -> None:
    row = spark.createDataFrame(
        [
            (
                rid,
                _dt.datetime.now(_dt.UTC).date(),
                source,
                STAGE,
                component,
                metric_name,
                None if metric_value is None else float(metric_value),
                threshold,
                status,
                error,
                _dt.datetime.now(_dt.UTC),
            )
        ],
        "run_id string, run_date date, source string, stage string, component string, "
        "metric_name string, metric_value double, threshold double, status string, "
        "error_detail string, recorded_at timestamp",
    )
    row.write.format("delta").mode("append").saveAsTable(AUDIT_TABLE)


def _delta_rows_written(table: str) -> int | None:
    try:
        m = (
            spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1")
            .select("operationMetrics")
            .first()[0]
        )
        return int(m.get("numOutputRows")) if m and "numOutputRows" in m else None
    except Exception:
        return None


def watermark(
    component: str,
    source: str,
    rows: int | None,
    status: str,
    rid: str,
    started: _dt.datetime,
) -> None:
    row = spark.createDataFrame(
        [
            (
                rid,
                source,
                STAGE,
                component,
                None,
                None,
                None if rows is None else int(rows),
                status,
                started,
                _dt.datetime.now(_dt.UTC),
            )
        ],
        "run_id string, source string, stage string, component string, "
        "last_processed_ts timestamp, last_processed_row bigint, rows_written bigint, "
        "status string, started_at timestamp, completed_at timestamp",
    )
    row.write.format("delta").mode("append").saveAsTable(WATERMARK_TABLE)


def write_silver(
    df: DataFrame, silver_table: str, *, source: str, component: str, rid: str
) -> int | None:
    """Deterministic full overwrite. Validates field classes first, prints the
    Delta rollback line, records rows_written from Delta metrics (no pre-write
    count), writes a watermark row."""
    started = now_utc()
    full = f"{CATALOG}.{SILVER_SCHEMA}.{silver_table}"
    validate_field_classes(df, full)
    if spark.catalog.tableExists(full):
        v = spark.sql(f"DESCRIBE HISTORY {full} LIMIT 1").first()["version"]
        print(f"ROLLBACK IF NEEDED: RESTORE TABLE {full} TO VERSION AS OF {v}")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full)
    )
    n = _delta_rows_written(full)
    watermark(component, source, n, "COMPLETE", rid, started)
    audit(component, source, "rows_written", n, status="PASS", rid=rid)
    print(f"OK  {full}: {n if n is not None else '?'} rows")
    return n


# COMMAND ----------

# DBTITLE 1,Mapping-driven renames + coded-column discovery + decode
# The three helpers below mirror src/schemas/_generate_field_classes.py exactly
# so the columns a notebook emits stay aligned with the field-class registry.


def flatten_business_names(mapping: dict, source: str) -> dict:
    """Bronze column -> English business name, per the mapping's own structure."""
    bn = mapping.get("business_names", {}) or {}
    out: dict = {}
    if source == "dwd":
        for code, spec in (bn.get("parameters") or {}).items():
            out[code] = spec["business_name"] if isinstance(spec, dict) else spec
        out.update(bn.get("metadata_fields") or {})
    elif source == "smard":
        out.update(bn.get("columns") or {})
    elif source == "mastr":
        out.update(bn.get("identifiers") or {})
        out.update(bn.get("common_fields") or {})
    else:
        out.update(bn.get("fields") or {})
    return out


def coded_columns(mapping: dict, source: str) -> dict:
    """Bronze column -> decode hint. For MaStR the hint is the katalog category
    name; for the map-based sources it is the code -> English label dict."""
    labels = mapping.get("coded_value_labels", {}) or {}
    if source == "mastr":
        return dict(labels.get("named_category_bindings", {}) or {})
    out: dict = {}
    for spec in labels.values():
        if isinstance(spec, dict) and "scope" in spec:
            col = spec["scope"].split(" -- ")[0].split(".")[-1].strip()
            out[col] = spec.get("map")
    return out


def apply_renames(df: DataFrame, name_map: dict, skip=()) -> DataFrame:
    """Rename every present Bronze column to its English business name. Skips a
    target that already exists (a decode already produced it) and `skip` names."""
    for src, tgt in name_map.items():
        if src in skip or src == tgt or src not in df.columns or tgt in df.columns:
            continue
        df = df.withColumnRenamed(src, tgt)
    return df


# DWD hourly-historical quality level -> English label (mappings/dwd.yml).
DWD_QN_LABELS = {
    "1": "only_formal_check",
    "2": "checked_against_individual_criteria",
    "3": "automatic_check_and_correction",
    "5": "historic_subjective_check",
    "7": "second_check_before_correction",
    "9": "not_all_parameters_corrected",
    "10": "quality_check_and_correction_complete",
}


def decode_qn(df: DataFrame, qn_src_col: str) -> DataFrame:
    """Add the DWD quality-level triple: `qn_level_code` (source code verbatim),
    `qn_level_label_de` (no German gloss is published -- NULL) and `qn_level`
    (English label). The source QN column is left in place unchanged."""
    m = F.create_map([F.lit(x) for kv in DWD_QN_LABELS.items() for x in kv])
    code = F.regexp_replace(F.trim(F.col(qn_src_col)), r"\.0$", "")
    return (
        df.withColumn("qn_level_code", code)
        .withColumn("qn_level_label_de", F.lit(None).cast("string"))
        .withColumn("qn_level", m[code])
    )


def decode_via_labeled_map(
    df: DataFrame, src_col: str, out_prefix: str, en_map: dict | None
) -> DataFrame:
    """Map-based decode triple where the source value is itself the German label
    (power_plant_list / redispatch) or a small numeric code (DWD WRTR): adds
    `<p>_code` (source), `<p>_label_de` (= source text) and `<p>` (English)."""
    code = F.regexp_replace(F.trim(F.col(src_col).cast("string")), r"\.0$", "")
    df = df.withColumn(f"{out_prefix}_code", code).withColumn(
        f"{out_prefix}_label_de", code
    )
    if en_map:
        pairs = [(k, v) for k, v in en_map.items() if k is not None and v is not None]
        m = F.create_map([F.lit(x) for kv in pairs for x in kv])
        return df.withColumn(out_prefix, m[code])
    return df.withColumn(out_prefix, F.lit(None).cast("string"))


# COMMAND ----------

# DBTITLE 1,Silver reads + geography attribution


def read_silver(table: str) -> DataFrame:
    return spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{table}")


def attach_city_ags(df: DataFrame, city_col: str = "city") -> DataFrame:
    """Broadcast-join the curated `dwd_city_bundesland_xref` to add `ags_code`,
    `ags_level` ('bundesland') and `ags_method` ('city_lookup'). Bundesland level
    only -- the conformed spine carries no polygon geometry."""
    xref = read_silver("dwd_city_bundesland_xref").select(
        F.col("city").alias("_xc"), F.col("ags_code").alias("_xa")
    )
    return (
        df.join(F.broadcast(xref), F.col(city_col) == F.col("_xc"), "left")
        .withColumn("ags_code", F.col("_xa"))
        .withColumn("ags_level", F.lit("bundesland"))
        .withColumn("ags_method", F.lit("city_lookup"))
        .drop("_xc", "_xa")
    )


def attach_ags_prefix(df: DataFrame, ags_source_col: str) -> DataFrame:
    """AGS attribution from an 8-digit Gemeindeschluessel: keep the 2-digit
    Bundesland prefix. `ags_level` = 'bundesland', `ags_method` = 'ags_prefix'."""
    return (
        df.withColumn("ags_code", ags_from_gemeindeschluessel(ags_source_col))
        .withColumn("ags_level", F.lit("bundesland"))
        .withColumn("ags_method", F.lit("ags_prefix"))
    )


_MASTR_NUM_SUFFIX = ("_kw", "_m", "_km")
_MASTR_NUM_EXACT = {"capacity_increase"}


def _is_mastr_date(col: str) -> bool:
    return "_date" in col or col.endswith(("_at", "_deadline"))


def mastr_standardise(
    df: DataFrame, name_map: dict, coded_map: dict, *, source: str = "mastr"
) -> DataFrame:
    """Decode every present coded column against its katalog category, apply the
    English business renames, then type the date / capacity / geometry /
    coordinate columns. Column names come out aligned with the registry."""
    for raw, category in coded_map.items():
        if raw not in df.columns:
            continue
        pref = name_map.get(raw, raw)
        df = decode_via_ref(
            df, raw, mastr_catalog_ref(category), "cat_id", "cat_wert", pref
        )
        if raw != pref:
            df = df.drop(raw)
    df = apply_renames(df, name_map)
    for c in df.columns:
        if _is_mastr_date(c):
            df = df.withColumn(c, parse_ts(c, GERMAN_TS_FORMATS, "Europe/Berlin"))
        elif c.endswith(_MASTR_NUM_SUFFIX) or c in _MASTR_NUM_EXACT:
            df = df.withColumn(c, F.col(c).cast("double"))
    for c in ("latitude", "longitude"):
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("double"))
    return df


def explode_link_bridge(
    df: DataFrame,
    parent_col: str,
    link_col: str,
    silver_table: str,
    *,
    source: str,
    component: str,
    bronze_table: str,
    rid: str,
    sep: str = r"[,;\s]+",
) -> None:
    """Explode a delimited link array to an additive `(parent_id, linked_id)`
    bridge -- one row per pair, deterministic `source_record_id`. The bridge
    never replaces the owning source table."""
    b = (
        df.select(
            F.col(parent_col).cast("string").alias("parent_id"),
            F.explode(F.split(F.trim(F.col(link_col)), sep)).alias("linked_id"),
        )
        .filter((F.col("linked_id") != "") & F.col("linked_id").isNotNull())
        .dropDuplicates(["parent_id", "linked_id"])
        .withColumn("_srid", sha_key("parent_id", "linked_id"))
    )
    b = add_provenance(b, source, "_srid", rid)
    write_silver(b, silver_table, source=source, component=component, rid=rid)


def mastr_catalog_ref(category_name: str) -> DataFrame:
    """The MaStR katalogwerte rows for one named category, as (cat_id, cat_wert)
    -- the decode reference for a coded column bound to that category."""
    kw = read_silver("mastr_katalogwerte")
    kk = read_silver("mastr_katalogkategorien")
    cat = kk.filter(F.col("Name") == category_name).select(F.col("Id").alias("_cid"))
    return kw.join(
        F.broadcast(cat), F.col("KatalogKategorieId") == F.col("_cid"), "inner"
    ).select(
        F.col("Id").cast("string").alias("cat_id"), F.col("Wert").alias("cat_wert")
    )
