# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SILVER SHARED LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the reusable Silver-transform plumbing, the post-Silver
# MAGIC inspection functions and `write_inspect_export`. `_semantic_common` and
# MAGIC `_genomics_common` pull it in with `%run ./_silver_common`, so a notebook
# MAGIC has one `%run` cell. Definitions only -- no side effects at import; the
# MAGIC caller owns `spark` / `dbutils`.
# MAGIC
# MAGIC Energy and commerce Silver = source-scoped, typed, cleaned, localised,
# MAGIC governed. Primary Silver tables preserve source semantics and source
# MAGIC grain. Reference / decode / bridge tables are additive and never replace
# MAGIC the source table. No ML targets/features, no segmentation, no scores, no
# MAGIC universal envelope, no cross-source facts/dimensions, no Gold grain
# MAGIC changes, no `canonical_id`.
# MAGIC
# MAGIC Silver tables are flat: no array, struct or map column.
# MAGIC Genomics Silver has its own rules: see `_genomics_common`.

# COMMAND ----------

# DBTITLE 1,Imports
import datetime as _dt
import os as _os
import re as _re

import yaml as _yaml
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Configuration constants
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
QUALITY_SCHEMA = "quality"

# Silver data lives in two schemas per ecosystem ({eco}_silver and
# {eco}_silver_reference); which one a given TABLE belongs to is resolved from the seeded
# field_class_registry's own target_schema column (src/schemas/_generate_
# field_classes.py), not re-derived here -- a source can own both a primary
# and a reference table (e.g. dwd), so ecosystem-per-source alone can't
# express this, only a per-table lookup can. quarantine and
# field_class_registry themselves are shared across every ecosystem, same as
# quality_audit_log / pipeline_watermarks.
_DEFAULT_ECOSYSTEM = "energy"

QUARANTINE_TABLE = f"{CATALOG}.{QUALITY_SCHEMA}.quarantine"
FIELD_CLASS_TABLE = f"{CATALOG}.{QUALITY_SCHEMA}.field_class_registry"
AUDIT_TABLE = f"{CATALOG}.{QUALITY_SCHEMA}.quality_audit_log"
WATERMARK_TABLE = f"{CATALOG}.{QUALITY_SCHEMA}.pipeline_watermarks"

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


_ecosystem_by_source: dict[str, str] | None = None  # cached after first load


def ecosystem_for(source: str, *, strict: bool = False) -> str:
    """source_system -> ecosystem, from the governed source_ecosystem_map.yml
    (loaded once per session). An unmapped source gets the default ecosystem;
    `strict` raises instead."""
    global _ecosystem_by_source
    if _ecosystem_by_source is None:
        doc = load_yaml("src/schemas/reference/source_ecosystem_map.yml")
        _ecosystem_by_source = {
            m["source_system"]: m["ecosystem"] for m in doc["mappings"]
        }
    if strict and source not in _ecosystem_by_source:
        message = f"source {source!r} is not in source_ecosystem_map.yml"
        raise ValueError(message)
    return _ecosystem_by_source.get(source, _DEFAULT_ECOSYSTEM)


_target_schema_by_table: dict[str, str] | None = None  # cached after first load


def target_schema_for(silver_table: str, *, source: str | None = None) -> str:
    """silver_table (short name) -> its schema. Prefers the seeded
    field_class_registry's own target_schema column (authoritative once a table
    is classified -- a source can own both a primary and a reference table,
    e.g. dwd, which ecosystem-per-source alone can't express).

    A brand-new Silver table with no registry entry yet is NOT blocked: pass
    `source=` and it falls back to that source's ecosystem default schema
    (`{ecosystem}_silver`), prints a NEW TABLE notice, and lets the write
    proceed. Classification happens after inspecting the actual Silver output
    (see inspect_table below), never as a precondition for writing it."""
    global _target_schema_by_table
    if _target_schema_by_table is None:
        try:
            rows = (
                spark.table(FIELD_CLASS_TABLE)
                .select("table_name", "target_schema")
                .distinct()
                .collect()
            )
        except Exception as exc:
            raise RuntimeError(
                f"{FIELD_CLASS_TABLE} missing -- run databricks/silver/00_silver_setup"
            ) from exc
        _target_schema_by_table = {r["table_name"]: r["target_schema"] for r in rows}
    schema = _target_schema_by_table.get(silver_table)
    if schema is not None:
        return schema
    if source is None:
        raise RuntimeError(
            f"{silver_table} has no field_class_registry entry and no source= "
            "was given to derive a default -- pass source= to write_silver for a "
            "new table, or register it in the seed"
        )
    schema = f"{ecosystem_for(source)}_silver"
    print(
        f"NEW TABLE  {silver_table} has no field_class_registry entry -- "
        f"defaulting to {schema} (ecosystem default for source={source!r}); "
        "this write is NOT blocked, but read_silver() has no such fallback -- "
        "declare it in the field-class generator and re-run 00_silver_setup "
        "before any other notebook reads this table."
    )
    return schema


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


BRONZE_MERGES_PATH = "src/schemas/mappings/bronze_merges.yml"
_bronze_parts: dict | None = None  # cached after first load


def bronze_parts() -> dict:
    """(schema, table name a part had) -> (merged table, discriminator, values,
    keep the discriminator), from bronze_merges.yml. An appended table reads
    only its `kept` rows."""
    global _bronze_parts
    if _bronze_parts is None:
        _bronze_parts = {}
        for merge in load_yaml(BRONZE_MERGES_PATH)["merges"]:
            column = merge["discriminator"]
            if not column:
                continue
            for value, part in merge["parts"].items():
                key = (merge["schema"], part)
                _bronze_parts[key] = (merge["table"], column, [value], False)
            if merge.get("append"):
                key = (merge["schema"], merge["table"])
                _bronze_parts[key] = (merge["table"], column, merge["kept"], True)
    return _bronze_parts


def read_bronze(table: str, schema: str | None = None) -> DataFrame:
    """A Bronze table by the name Silver knows; a merged part is filtered out of
    its merged table and keeps the empty columns of the other parts."""
    schema = schema or BRONZE_SCHEMA
    merged = bronze_parts().get((schema, table))
    if merged is None:
        return spark.table(f"{CATALOG}.{schema}.{table}")
    name, column, values, keep = merged
    df = spark.table(f"{CATALOG}.{schema}.{name}").filter(F.col(column).isin(values))
    return df if keep else df.drop(column)


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


def parse_ts(colname, formats=GERMAN_TS_FORMATS, src_tz: str = "Europe/Berlin"):
    """Column expr: parse a wall-clock string under the ordered formats, treat
    it as `src_tz`, return a UTC timestamp. The first format that parses wins.
    `colname` is a column name (str) or an already-built Column expression
    (e.g. `F.concat_ws(...)`). Tolerates a trailing `.0` from a column staged
    through pandas with no dtype=str -- any all-digit column with a blank
    somewhere in it (e.g. an open-ended Bis_Datum) gets float-inferred and
    re-serialized with the suffix; a clean value never legitimately ends in
    `.0`, so stripping it is a no-op everywhere else. Same tolerance
    parse_mess_datum already applies for MESS_DATUM."""
    src_col = F.col(colname) if isinstance(colname, str) else colname
    stripped = F.regexp_replace(F.trim(src_col), r"\.0$", "")
    # Drop fractional seconds (any digit count) after HH:mm:ss
    stripped = F.regexp_replace(stripped, r"(\d{2}:\d{2}:\d{2})\.\d+$", "$1")
    parsed = F.lit(None).cast("timestamp")
    for fmt in formats:
        parsed = F.coalesce(parsed, F.try_to_timestamp(stripped, F.lit(fmt)))
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
    """DWD solar MESS_DATUM: `yyyyMMddHH:mm` (hourly sums over true-solar-time
    hours, so the UTC minute varies). Tolerates a trailing `.0`."""
    base = F.regexp_replace(F.trim(F.col(colname)), r"\.0$", "")
    base = F.regexp_replace(base, ":", "")
    parsed = F.try_to_timestamp(base, F.lit("yyyyMMddHHmm"))
    return parsed if src_tz.upper() == "UTC" else F.to_utc_timestamp(parsed, src_tz)


# COMMAND ----------

# DBTITLE 1,Deterministic keys + within-group ordinal


def strip_float_suffix(col):
    """Trim and drop a trailing `.0` (a numeric id or code staged as double)."""
    c = F.col(col) if isinstance(col, str) else col
    return F.regexp_replace(F.trim(c), r"\.0$", "")


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
    """Add `_source_id_ordinal` (1-based, deterministic by a content hash) and
    `_source_id_disambiguated` (ord > 1). Used only where a source has no unique
    key -- makes the composite `source_record_id` unique and reproducible."""
    content_hash = F.sha2(
        F.concat_ws(
            "¦", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in content_cols]
        ),
        256,
    )
    w = Window.partitionBy(*key_cols).orderBy(content_hash)
    return df.withColumn("_source_id_ordinal", F.row_number().over(w)).withColumn(
        "_source_id_disambiguated", F.col("_source_id_ordinal") > 1
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
        .withColumn("ecosystem", F.lit(ecosystem_for(source_system)))
        .withColumn("_silver_loaded_at", F.current_timestamp())
        .withColumn("_silver_run_id", F.lit(rid))
    )


def validate_field_classes(df: DataFrame, silver_table: str) -> list[str]:
    """Detect output columns not yet classified in the central registry and
    report them -- does NOT block the write. A legitimate new Silver column (or
    a brand-new table, see target_schema_for) is allowed to exist before it is
    registered; classification is a post-hoc reconciliation step against the
    actual Silver output (see inspect_table below), not a pre-run whitelist.
    Notebooks never write the registry itself -- it is a seeded build artifact,
    reconciled separately. Returns the sorted list of unclassified column names
    (empty if none)."""
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
        print(
            f"UNCLASSIFIED  {short}.{{{', '.join(missing)}}} -- not yet in "
            "field_class_registry; write proceeds. Classify in the field-class "
            "generator and re-run 00_silver_setup when convenient."
        )
    return missing


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
    multi_code: bool = False,
) -> DataFrame:
    """Add `<out_prefix>_code` (source code, verbatim), `<out_prefix>_label_de`
    (German label from the reference table) and `<out_prefix>` (English business
    label from `english_map`, else the German label lowered). Broadcast join.
    `multi_code` also decodes comma-separated code lists; a list with any
    unmatched code keeps a NULL label."""
    r = (
        ref_df.select(
            F.col(ref_code_col).cast("string").alias("_rc"),
            F.col(ref_label_col).cast("string").alias("_rl"),
        )
        .groupBy("_rc")
        .agg(F.min("_rl").alias("_rl"))
    )
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
    if not multi_code:
        return out
    lookup = r.agg(
        F.map_from_entries(F.collect_list(F.struct("_rc", "_rl"))).alias("_lk")
    )
    labels = F.transform(
        F.split(F.trim(F.col(f"{out_prefix}_code")), r"\s*,\s*"),
        lambda c: F.col("_lk")[c],
    )
    is_list = F.col(f"{out_prefix}_code").contains(",") & F.forall(
        labels, lambda x: x.isNotNull()
    )
    return (
        out.crossJoin(F.broadcast(lookup))
        .withColumn(
            f"{out_prefix}_label_de",
            F.when(is_list, F.concat_ws(", ", labels)).otherwise(
                F.col(f"{out_prefix}_label_de")
            ),
        )
        .withColumn(
            out_prefix,
            F.when(
                is_list,
                F.concat_ws(
                    ",",
                    F.transform(
                        labels, lambda x: F.lower(F.regexp_replace(x, r"\s+", "_"))
                    ),
                ),
            ).otherwise(F.col(out_prefix)),
        )
        .drop("_lk")
    )


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

    1. byte-identical rows on a key -> collapse to one (with `qn_col`: rows
       differing only in QN collapse deterministically and are flagged);
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
    if qn_col is None:
        d = d.dropDuplicates([*key_cols, "_ch"])
    else:
        # identical data, differing QN: valid QN first, then lowest code wins;
        # `_qn_only_dropped` = rows lost where the QN differed
        qn = F.trim(F.col(qn_col))
        same = Window.partitionBy(*key_cols, "_ch")
        ranked = same.orderBy(qn.isin(list(valid_qn)).desc(), qn.asc_nulls_last())
        d = (
            d.withColumn("_rn", F.row_number().over(ranked))
            .withColumn("_same_n", F.count(F.lit(1)).over(same))
            .withColumn(
                "_qn_n", F.size(F.collect_set(F.coalesce(qn, F.lit(""))).over(same))
            )
            .filter(F.col("_rn") == 1)
            .withColumn(
                "_qn_only_dropped",
                F.when(F.col("_qn_n") > 1, F.col("_same_n") - 1).otherwise(0),
            )
            .drop("_rn", "_same_n", "_qn_n")
        )

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
        kept = singles.withColumn(
            "_had_key_conflict", F.col("_qn_only_dropped") > 0
        ).unionByName(pick_valid.withColumn("_had_key_conflict", F.lit(True)))
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
    """Deterministic full overwrite. Detects unclassified columns (non-blocking
    -- see validate_field_classes), prints the Delta rollback line, records
    rows_written from Delta metrics (no pre-write count), writes a watermark
    row. A brand-new table (no field_class_registry entry at all) defaults to
    its ecosystem's schema via `source` rather than failing -- see
    target_schema_for."""
    started = now_utc()
    full = f"{CATALOG}.{target_schema_for(silver_table, source=source)}.{silver_table}"
    unclassified = validate_field_classes(df, full)
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
    if unclassified:
        audit(
            component,
            source,
            "unclassified_columns",
            float(len(unclassified)),
            status="WARN",
            error=",".join(unclassified),
            rid=rid,
        )
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
    return spark.table(f"{CATALOG}.{target_schema_for(table)}.{table}")


def attach_ags_prefix(df: DataFrame, ags_source_col: str) -> DataFrame:
    """AGS attribution from an 8-digit Gemeindeschluessel: keep the 2-digit
    Bundesland prefix. Level = 'bundesland', method = 'municipality_key_prefix'."""
    return (
        df.withColumn(
            "official_municipality_key", ags_from_gemeindeschluessel(ags_source_col)
        )
        .withColumn("official_municipality_key_level", F.lit("bundesland"))
        .withColumn(
            "official_municipality_key_method", F.lit("municipality_key_prefix")
        )
    )


def bundesland_ags(name_col):
    """Bundesland name -> 2-digit AGS code. Case, hyphens, spaces and the
    u-umlaut spelling are normalised, so "Nordrhein-Westfalen",
    "NordrheinWestfalen" and "Thueringen" all resolve; NULL otherwise."""
    c = F.col(name_col) if isinstance(name_col, str) else name_col
    key = F.regexp_replace(
        F.regexp_replace(F.lower(F.trim(c)), "\u00fc", "ue"), "[^a-z]", ""
    )
    m = F.create_map(
        [F.lit(x) for k, v in BUNDESLAND_AGS.items() for x in (k.replace("_", ""), v)]
    )
    return m[key]


_MASTR_NUM_SUFFIX = ("_kw", "_m", "_km")
_MASTR_NUM_EXACT = {"capacity_increase"}
_MASTR_FLAG_COLS = ("FernsteuerbarkeitNb", "FernsteuerbarkeitDv")


def _is_mastr_date(col: str) -> bool:
    return "_date" in col or col.endswith(("_at", "_deadline"))


def untranslated_source_columns(columns) -> list:
    """Columns still carrying a source-style (uppercase) name. The `_nv`
    companions are excluded until their meaning is settled."""
    return [c for c in columns if c != c.lower() and not c.endswith("_nv")]


def mastr_standardise(
    df: DataFrame, name_map: dict, coded_map: dict, *, source: str = "mastr"
) -> DataFrame:
    """Decode every present coded column against its katalog category, apply the
    English business renames, then type the date / capacity / geometry /
    coordinate columns. Column names come out aligned with the registry."""
    decoded = []
    for raw, category in coded_map.items():
        if raw not in df.columns:
            continue
        if raw in _MASTR_FLAG_COLS:
            v = strip_float_suffix(raw)
            df = df.withColumn(
                raw,
                F.when(v == "1", F.lit(True))
                .when(v == "0", F.lit(False))
                .otherwise(F.lit(None).cast("boolean")),
            )
            continue
        pref = name_map.get(raw, raw)
        df = decode_via_ref(
            df,
            raw,
            mastr_catalog_ref(category, fallback_global=True),
            "cat_id",
            "cat_wert",
            pref,
            multi_code=True,
        )
        decoded.append(pref)
        if raw != pref:
            df = df.drop(raw)
    df = df.withColumn(
        "_unmatched_code_columns",
        F.nullif(
            F.concat_ws(
                ",",
                *[
                    F.when(
                        F.col(f"{p}_code").isNotNull()
                        & F.col(f"{p}_label_de").isNull(),
                        F.lit(p),
                    )
                    for p in decoded
                ],
            ),
            F.lit(""),
        ),
    )
    df = apply_renames(df, name_map)
    for c in df.columns:
        if _is_mastr_date(c):
            df = df.withColumn(c, parse_ts(c, GERMAN_TS_FORMATS, "Europe/Berlin"))
        elif c.endswith(_MASTR_NUM_SUFFIX) or c in _MASTR_NUM_EXACT:
            df = df.withColumn(c, F.col(c).cast("double"))
    for c in ("latitude", "longitude"):
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("double"))
    if untranslated := untranslated_source_columns(df.columns):
        message = f"source columns without an English name: {untranslated}"
        raise ValueError(message)
    return df


def link_pairs(df: DataFrame, parent_col: str, link_col: str, sep=r"[,;\s]+"):
    """Distinct (parent_id, linked_id) pairs from a delimited link column."""
    return (
        df.select(
            F.col(parent_col).cast("string").alias("parent_id"),
            F.explode(F.split(F.trim(F.col(link_col)), sep)).alias("linked_id"),
        )
        .filter((F.col("linked_id") != "") & F.col("linked_id").isNotNull())
        .dropDuplicates(["parent_id", "linked_id"])
    )


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
) -> DataFrame:
    """Explode a delimited link array to an additive `(parent_id, linked_id)`
    bridge -- one row per pair, deterministic `source_record_id`. The bridge
    never replaces the owning source table. Returns the written DataFrame so
    the caller can run inspect_table() on it without re-deriving it."""
    b = link_pairs(df, parent_col, link_col, sep).withColumn(
        "_srid", sha_key("parent_id", "linked_id")
    )
    b = add_provenance(b, source, "_srid", rid)
    write_silver(b, silver_table, source=source, component=component, rid=rid)
    return b


def mastr_catalog_ref(category_name: str, fallback_global: bool = False) -> DataFrame:
    """The MaStR katalog values of one named category, as (cat_id, cat_wert)
    -- the decode reference for a coded column bound to that category.
    `fallback_global` adds every other katalog value id (ids are unique across
    categories), so a binding to an absent category name still decodes."""
    codes = read_silver("reference_code_list")
    cat = codes.filter(
        (F.col("catalog_kind") == "katalogkategorien")
        & (F.col("label") == category_name)
    ).select(F.col("code_id").alias("_cid"))
    values = codes.filter(F.col("catalog_kind") == "katalogwerte")
    scoped = values.join(
        F.broadcast(cat), F.col("parent_id") == F.col("_cid"), "inner"
    ).select(F.col("code_id").alias("cat_id"), F.col("label").alias("cat_wert"))
    if not fallback_global:
        return scoped
    rest = values.select(
        F.col("code_id").alias("cat_id"), F.col("label").alias("cat_wert")
    ).join(
        scoped.select(F.col("cat_id").alias("_sid")),
        F.col("cat_id") == F.col("_sid"),
        "left_anti",
    )
    return scoped.unionByName(rest)


# COMMAND ----------

# DBTITLE 1,Inspection configuration
# Columns skipped for approx_count_distinct -- expected-high-cardinality /
# free-text fields where a distinct count is not diagnostic and only adds cost.
HIGH_CARDINALITY_SKIP = {
    "source_record_id",
    "url",
    "page_location",
    "page_title",
    "search_term",
    "unique_search_term",
    "plant_name",
    "station_name",
    "offending_value",
}

# Boolean/status columns this framework recognises as DQ/trust flags by name.
_FLAG_COL_PATTERN = _re.compile(
    r"^_.*_(flag|violated|violation|outlier|suspected|ambiguous|out_of_set|conflict)$"
    r"|^is_[a-z_]+$"
    r"|_status$"
    r"|^currency_unknown$"
)

# COMMAND ----------

# DBTITLE 1,Findings export (src/findings/silver_findings/<source>.md)
# A repo file, not a table: one `## ` section per notebook and table, a re-run
# replaces its own section. The file name is the ecosystem (energy, commerce,
# genomics).


def _findings_dir():
    d = _os.path.join(repo_root(), "src", "findings", "silver_findings")
    _os.makedirs(d, exist_ok=True)
    return d


STALE_AFTER_HOURS = 24
_GEN_PAT = _re.compile(r"_Generated: (\d{4}-\d\d-\d\dT\d\d:\d\dZ)_")


def _mark_stale(block: str, now) -> str:
    """Label a preserved block STALE when it is unstamped or older than
    STALE_AFTER_HOURS; a re-run of its notebook replaces it with a fresh one."""
    if "> STALE" in block:
        return block
    m = _GEN_PAT.search(block)
    if m:
        gen = _dt.datetime.strptime(m.group(1), "%Y-%m-%dT%H:%MZ").replace(
            tzinfo=_dt.UTC
        )
        if now - gen <= _dt.timedelta(hours=STALE_AFTER_HOURS):
            return block
    head, _, rest = block.partition("\n## ")
    title, _, tail = rest.partition("\n")
    return f"{head}\n## {title}\n\n> STALE: not rewritten in the last {STALE_AFTER_HOURS}h\n{tail}"


def write_silver_findings(
    source: str, notebook_key: str, section_title: str, blocks: list
) -> None:
    d = _findings_dir()
    md = _os.path.join(d, source + ".md")
    now = _dt.datetime.now(tz=_dt.UTC)
    lines = [
        f"<!-- BEGIN {source}:{notebook_key} -->",
        f"## {section_title}",
        "",
        f"_Generated: {now.strftime('%Y-%m-%dT%H:%MZ')}_",
        "",
    ]
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
    kept = {mm.group(1): _mark_stale(mm.group(0), now) for mm in pat.finditer(existing)}
    kept[notebook_key] = block
    intro = (
        "_Auto-generated by the Silver notebooks (`databricks/silver/`). One "
        "`## ` section per (notebook, table); re-running a notebook replaces "
        "its own sections, other sections are preserved and marked STALE once older "
        "than 24h._"
    )
    header = f"# {source.upper()} SILVER FINDINGS\n\n{intro}\n\n"
    body_out = "\n\n".join(kept[k] for k in sorted(kept))
    out = header + body_out + "\n"
    tmp = md + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(out)
    _os.replace(tmp, md)
    print(f"findings export -> {md}  ('{notebook_key}', {len(kept)} section(s))")


def _findings_markdown(rows: list[tuple]) -> list:
    by_cat: dict[str, list] = {}
    for r in rows:
        cat, name = r[5], r[6]
        metric, obs, exp, status, sev, details = r[7], r[8], r[9], r[10], r[11], r[12]
        by_cat.setdefault(cat, []).append(
            (name, metric, obs, exp, status, sev, details)
        )
    blocks = []
    for cat in sorted(by_cat):
        table = [
            "| check | metric | observed | expected | status | severity | details |",
            "|---|---|---|---|---|---|---|",
        ]
        for name, metric, obs, exp, status, sev, details in by_cat[cat]:
            table.append(
                f"| {name} | {metric} | {obs if obs is not None else ''} | "
                f"{exp if exp is not None else ''} | {status} | {sev} | {details} |"
            )
        blocks.append((cat, "\n".join(table)))
    return blocks


# COMMAND ----------

# DBTITLE 1,Common inspection


def inspect_table(
    df_after: DataFrame,
    silver_table: str,
    *,
    source: str,
    component: str,
    rid: str,
    key_cols: list[str] | None = None,
    df_before: DataFrame | None = None,
    extra_checks: dict | None = None,
) -> list:
    """Run the common post-Silver checks on `df_after` and RETURN the findings
    as `blocks` ready for `write_silver_findings()` -- does not write anything
    itself (`write_inspect_export` does both steps).

    `key_cols` -- the table's business grain key (not `_srid`) -- enables the
    duplicate/grain check; omit where no single business key applies.
    `df_before` -- the Bronze (or pre-transformation) read -- enables the
    input/output row-count reconciliation check; omit where not meaningful
    (e.g. a derived reference table with no 1:1 Bronze counterpart).
    `extra_checks` -- {check_name: value} for source-specific findings the
    caller already computed -- passed through as `transformation_specific`
    rows, never invented by this function.
    """
    ts = now_utc()
    rows: list[tuple] = []
    full = f"{CATALOG}.{target_schema_for(silver_table, source=source)}.{silver_table}"
    cols = df_after.columns
    dtypes = dict(df_after.dtypes)

    def add(category, name, metric, observed, expected, status, severity, details=""):
        rows.append(
            (
                ts,
                rid,
                source,
                component,
                full,
                category,
                name,
                metric,
                None if observed is None else float(observed),
                None if expected is None else float(expected),
                status,
                severity,
                details,
            )
        )

    # Schema
    add("schema", "table_exists", "exists", 1.0, 1.0, "PASS", "INFO")
    add("schema", "column_count", "count", float(len(cols)), None, "INFO", "INFO")

    # Volume
    n_after = df_after.count()
    add("volume", "row_count", "rows", float(n_after), None, "INFO", "INFO")
    if df_before is not None:
        n_before = df_before.count()
        delta = n_after - n_before
        ratio = (n_after / n_before) if n_before else None
        vol_status = (
            "WARN" if (ratio is not None and (ratio < 0.5 or ratio > 2.0)) else "PASS"
        )
        add(
            "volume",
            "input_output_reconciliation",
            "row_delta",
            float(delta),
            float(n_before),
            vol_status,
            "WARN" if vol_status == "WARN" else "INFO",
            f"before={n_before} after={n_after}",
        )

    # Completeness (one agg pass across every column)
    if cols:
        null_exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]
        null_row = df_after.agg(*null_exprs).first()
        if null_row and n_after:
            for c in cols:
                rate = (null_row[c] or 0) / n_after
                add(
                    "completeness",
                    f"null_rate:{c}",
                    "null_rate",
                    rate,
                    None,
                    "INFO",
                    "INFO",
                )

    # Cardinality (one agg pass, skipping known-high-cardinality columns)
    card_cols = [c for c in cols if c not in HIGH_CARDINALITY_SKIP]
    if card_cols:
        card_exprs = [F.approx_count_distinct(F.col(c)).alias(c) for c in card_cols]
        card_row = df_after.agg(*card_exprs).first()
        for c in card_cols:
            add(
                "cardinality",
                f"distinct:{c}",
                "approx_distinct",
                float(card_row[c]),
                None,
                "INFO",
                "INFO",
            )

    # Grain / duplicate-key check
    if key_cols:
        dup_n = df_after.groupBy(*key_cols).count().filter(F.col("count") > 1).count()
        grain_status = "WARN" if dup_n else "PASS"
        add(
            "grain",
            "duplicate_keys",
            "count",
            float(dup_n),
            0.0,
            grain_status,
            "WARN" if dup_n else "INFO",
            f"key_cols={key_cols}",
        )

    # Numeric behaviour (one agg pass over numeric columns)
    numeric_types = {"double", "float", "int", "bigint", "smallint", "decimal"}
    numeric_cols = [c for c in cols if dtypes.get(c) in numeric_types]
    if numeric_cols:
        stat_exprs = []
        for c in numeric_cols:
            stat_exprs += [
                F.min(c).alias(f"{c}__min"),
                F.max(c).alias(f"{c}__max"),
                F.sum((F.col(c) == 0).cast("int")).alias(f"{c}__zero"),
                F.sum((F.col(c) < 0).cast("int")).alias(f"{c}__neg"),
            ]
        stat_row = df_after.agg(*stat_exprs).first()
        for c in numeric_cols:
            add(
                "numeric",
                f"min:{c}",
                "min",
                stat_row[f"{c}__min"],
                None,
                "INFO",
                "INFO",
            )
            add(
                "numeric",
                f"max:{c}",
                "max",
                stat_row[f"{c}__max"],
                None,
                "INFO",
                "INFO",
            )
            add(
                "numeric",
                f"zero_count:{c}",
                "count",
                float(stat_row[f"{c}__zero"] or 0),
                None,
                "INFO",
                "INFO",
            )
            add(
                "numeric",
                f"negative_count:{c}",
                "count",
                float(stat_row[f"{c}__neg"] or 0),
                None,
                "INFO",
                "INFO",
            )

    # DQ / semantic-status flag counts (recognised by name, not forced on tables
    # that carry none)
    flag_cols = [c for c in cols if _FLAG_COL_PATTERN.search(c)]
    bool_flag_cols = [c for c in flag_cols if dtypes.get(c) == "boolean"]
    if bool_flag_cols:
        flag_exprs = [F.sum(F.col(c).cast("int")).alias(c) for c in bool_flag_cols]
        flag_row = df_after.agg(*flag_exprs).first()
        for c in bool_flag_cols:
            add(
                "dq",
                f"flag_true_count:{c}",
                "count",
                float(flag_row[c] or 0),
                None,
                "INFO",
                "INFO",
            )
    for c in flag_cols:
        if dtypes.get(c) == "string":
            vc = df_after.groupBy(c).count().collect()
            details = ", ".join(f"{r[c]}={r['count']}" for r in vc)
            add("dq", f"value_counts:{c}", "count", None, None, "INFO", "INFO", details)

    # Source-specific checks the caller already computed -- never invented here
    if extra_checks:
        for name, value in extra_checks.items():
            numeric_value = (
                value
                if isinstance(value, (int, float)) and not isinstance(value, bool)
                else None
            )
            add(
                "transformation_specific",
                name,
                "value",
                numeric_value,
                None,
                "INFO",
                "INFO",
                str(value),
            )

    print(f"INSPECTED {full}: {len(rows)} check(s)")
    return _findings_markdown(rows)


# COMMAND ----------

# DBTITLE 1,Write, inspect and export one table


def write_inspect_export(
    df: DataFrame,
    table: str,
    *,
    source: str,
    component: str,
    rid: str,
    findings: str,
    key_cols: list[str] | None = None,
    df_before: DataFrame | None = None,
    extra_checks=None,
    extra_blocks: list | None = None,
    writer=None,
    replace_where: str | None = None,
    link_method_counts: bool = False,
    section: str | None = None,
) -> DataFrame:
    """Write `df` as Silver table `table`, inspect what was written and export
    the findings section; returns the written table. One call replaces the
    write, inspect and export steps of a table.

    `writer` is `write_silver` (default) or `write_semantic`; `replace_where`
    needs `write_semantic`. `extra_checks` is a dict (a value may be a function
    of the written table) or a function of the written table that returns one. `link_method_counts` adds the counts of every
    `*_link_method` column. `findings` is the findings file (the ecosystem);
    `extra_blocks` are findings blocks the caller already built."""
    write = writer or write_silver
    options = {"replace_where": replace_where} if replace_where else {}
    write(df, table, source=source, component=component, rid=rid, **options)
    full = f"{CATALOG}.{target_schema_for(table, source=source)}.{table}"
    written = spark.table(full)
    if replace_where:
        written = written.filter(replace_where)
    extra = extra_checks(written) if callable(extra_checks) else (extra_checks or {})
    checks = {
        name: (value(written) if callable(value) else value)
        for name, value in extra.items()
    }
    if link_method_counts:
        for column in written.columns:
            if column.endswith("_link_method"):
                counts = written.groupBy(column).count().collect()
                checks[f"{column}_counts"] = str(
                    {row[column]: row["count"] for row in counts}
                )
    blocks = inspect_table(
        written,
        table,
        source=findings,
        component=component,
        rid=rid,
        key_cols=key_cols,
        df_before=df_before,
        extra_checks=checks,
    )
    write_silver_findings(
        findings,
        f"{component.split('/')[-1]}__{table}",
        section or table,
        [*blocks, *(extra_blocks or [])],
    )
    return written
