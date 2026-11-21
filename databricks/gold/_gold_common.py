# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # GOLD SHARED LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the plumbing of the Gold, analytics and ML notebooks, loaded with
# MAGIC `%run` (it loads `_silver_common`, whose audit, watermark and inspection code
# MAGIC it reuses). A notebook puts its tables in one dict, table -> (frame, grain);
# MAGIC one cell each gates, writes, inspects and validates every table. Gold joins
# MAGIC parent attributes, unions, pivots Silver attributes, builds typed tables
# MAGIC (`typed_table`) and adds keys; scores, classes, counts and flags come from
# MAGIC Silver. Definitions only.

# COMMAND ----------

# DBTITLE 1,Shared Silver library (audit, watermark, inspect_table, sha_key, ...)
# MAGIC %run ../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Imports
import datetime as _dt

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration constants
GOLD_STAGE = "gold"
ANALYTICS_STAGE = "analytics"
SHARED_CONFORMED_SCHEMA = "shared_conformed"
GENOMICS_GOLD_SCHEMA = "genomics_gold"
ANALYTICS_SCHEMAS = {
    "energy": "energy_analytics",
    "commerce": "commerce_analytics",
    "genomics": "genomics_analytics",
}
SILVER_TECHNICAL = ("_silver_loaded_at", "_silver_run_id")
# Columns a parent join does not copy: the child keeps its own provenance.
PARENT_SKIP = (
    *SILVER_TECHNICAL,
    "source_system",
    "source_dataset",
    "source_record_id",
    "ecosystem",
)


def gold_schema_for(source: str) -> str:
    """source_system -> its `{ecosystem}_gold` schema."""
    return f"{ecosystem_for(source)}_gold"


# COMMAND ----------

# DBTITLE 1,Run identity and provenance


def stage_of(rid: str) -> str:
    """`analytics` for an analytics run id, else `gold`."""
    return ANALYTICS_STAGE if rid.startswith(f"{ANALYTICS_STAGE}-") else GOLD_STAGE


def gold_run_id() -> str:
    return _dt.datetime.now(_dt.UTC).strftime("gold-%Y%m%dT%H%M%SZ")


def analytics_run_id() -> str:
    return _dt.datetime.now(_dt.UTC).strftime("analytics-%Y%m%dT%H%M%SZ")


def add_provenance(
    df: DataFrame, source_system: str, rid: str, ecosystem: str | None = None
) -> DataFrame:
    """The load columns of the stage (`_gold_*` or `_analytics_*`). A row keeps
    its own `source_system` and `ecosystem`; the arguments fill missing ones."""
    if "source_system" not in df.columns:
        df = df.withColumn("source_system", F.lit(source_system))
    if "ecosystem" not in df.columns:
        df = df.withColumn(
            "ecosystem", F.lit(ecosystem or ecosystem_for(source_system))
        )
    stage = stage_of(rid)
    return df.withColumn(f"_{stage}_loaded_at", F.current_timestamp()).withColumn(
        f"_{stage}_run_id", F.lit(rid)
    )


def from_silver(df: DataFrame) -> DataFrame:
    """A Silver frame without its Silver load columns."""
    return df.drop(*[c for c in SILVER_TECHNICAL if c in df.columns])


def from_gold(df: DataFrame) -> DataFrame:
    """A Gold frame without its Gold load columns."""
    return df.drop(*[c for c in ("_gold_loaded_at", "_gold_run_id") if c in df.columns])


# COMMAND ----------

# DBTITLE 1,Hard-fail gate


def check(
    component: str,
    source: str,
    metric_name: str,
    condition: bool,
    *,
    detail: str = "",
    metric_value: float | None = None,
    rid: str,
) -> None:
    """Logs the result (Silver `audit`, stage from the run id) and RAISES when
    `condition` is False: a failed check stops the notebook."""
    audit(
        component,
        source,
        metric_name,
        metric_value,
        status="PASS" if condition else "FAIL",
        error=detail or None,
        rid=rid,
        stage=stage_of(rid),
    )
    if not condition:
        raise RuntimeError(
            f"{stage_of(rid).upper()} GATE FAILED: {component}.{metric_name} -- "
            f"{detail or 'no detail given'}"
        )


def assert_unique_grain(
    df: DataFrame, key_cols: list[str], *, component: str, source: str, rid: str
) -> None:
    """Raises via check() when a key_cols group has more than one row."""
    dup_n = df.groupBy(*key_cols).count().filter(F.col("count") > 1).count()
    check(
        component,
        source,
        "grain_duplicate_keys",
        dup_n == 0,
        detail=f"key_cols={key_cols} duplicate_groups={dup_n}",
        metric_value=float(dup_n),
        rid=rid,
    )


# COMMAND ----------


# DBTITLE 1,Read
def read_gold(table: str, *, source: str, schema: str | None = None) -> DataFrame:
    return spark.table(f"{CATALOG}.{schema or gold_schema_for(source)}.{table}")


def read_shared_conformed(table: str) -> DataFrame:
    return spark.table(f"{CATALOG}.{SHARED_CONFORMED_SCHEMA}.{table}")


# COMMAND ----------

# DBTITLE 1,Gate, write, inspect and validate every table of a notebook
# `tables` maps a table name to (frame, grain columns); `schema` is the target.


def gate_tables(tables: dict, *, source: str, component: str, rid: str) -> None:
    for frame, grain in tables.values():
        assert_unique_grain(frame, grain, component=component, source=source, rid=rid)


def write_tables(
    tables: dict, *, schema: str, source: str, component: str, rid: str
) -> None:
    """Full overwrite of each table; prints the rollback line, writes a
    watermark row (Silver `watermark`, stage from the run id)."""
    for table, (frame, _) in tables.items():
        started = now_utc()
        full = f"{CATALOG}.{schema}.{table}"
        if spark.catalog.tableExists(full):
            v = spark.sql(f"DESCRIBE HISTORY {full} LIMIT 1").first()["version"]
            print(f"ROLLBACK IF NEEDED: RESTORE TABLE {full} TO VERSION AS OF {v}")
        frame.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(full)
        n = _delta_rows_written(full)
        watermark(component, source, n, "COMPLETE", rid, started, stage=stage_of(rid))
        print(f"OK  {full}: {n if n is not None else '?'} rows")


# Findings rows that would show a data value (a minimum, a maximum, the values
# of a category column); they are left out of the Gold and analytics findings.
VALUE_CHECKS = tuple(f"| {prefix}" for prefix in ("min:", "max:", "value_counts:"))


def without_values(blocks: list) -> list:
    """The findings blocks without the rows of VALUE_CHECKS; an emptied block
    (header lines only) is dropped."""
    kept = []
    for category, table in blocks:
        lines = [
            line for line in table.splitlines() if not line.startswith(VALUE_CHECKS)
        ]
        if len(lines) > 2:
            kept.append((category, "\n".join(lines)))
    return kept


def inspect_tables(
    tables: dict, *, schema: str, findings: str, source: str, component: str, rid: str
) -> None:
    """Silver `inspect_table` on each written table, without data values; the
    section goes to `src/findings/<stage>_findings/<findings>.md` (kept local)."""
    for table, (_, grain) in tables.items():
        full = f"{CATALOG}.{schema}.{table}"
        blocks = inspect_table(
            spark.table(full),
            table,
            source=findings,
            component=component,
            rid=rid,
            key_cols=grain,
            full_name=full,
        )
        write_silver_findings(
            findings,
            f"{component.split('/')[-1]}__{table}",
            table,
            without_values(blocks),
            stage=stage_of(rid),
        )


def validate_tables(
    tables, *, schema: str, source: str, component: str, rid: str
) -> None:
    """The last cell of a notebook: every table exists and has rows."""
    for table in tables:
        full = f"{CATALOG}.{schema}.{table}"
        rows = spark.table(full).count() if spark.catalog.tableExists(full) else 0
        check(
            component,
            source,
            f"{table}_has_rows",
            rows > 0,
            detail=full,
            metric_value=float(rows),
            rid=rid,
        )
        print(f"OK  {full}: {rows} rows")


# COMMAND ----------

# DBTITLE 1,Column helpers


def ratio(numerator, denominator):
    """numerator / denominator; null when the denominator is 0."""
    num = F.col(numerator) if isinstance(numerator, str) else numerator
    den = F.col(denominator) if isinstance(denominator, str) else denominator
    return num / F.nullif(den, F.lit(0))


def sorted_values(column: str):
    """The distinct values of a group as one `|`-separated sorted text."""
    return F.concat_ws("|", F.array_sort(F.collect_set(column)))


def one_resolution(df: DataFrame, keys: list[str], column: str, *, largest: bool):
    """The rows whose `column` is the largest (or smallest) of their `keys`."""
    window = Window.partitionBy(*keys)
    pick = F.max(column) if largest else F.min(column)
    return (
        df.withColumn("_pick", pick.over(window))
        .filter(F.col(column) == F.col("_pick"))
        .drop("_pick")
    )


def columns_to_rows(df: DataFrame, keep: list[str], rows: list) -> DataFrame:
    """One output row per struct of `rows` (same fields), with the `keep` columns."""
    return df.select(*keep, F.explode(F.array(*rows)).alias("_row")).select(
        *keep, "_row.*"
    )


# COMMAND ----------

# DBTITLE 1,Joins of parent attributes


def with_parent(
    df: DataFrame,
    parent: DataFrame,
    *,
    on: str,
    prefix: str,
    parent_key: str | None = None,
) -> DataFrame:
    """Left join of every parent column (provenance excluded), renamed
    `<prefix><column>`; `parent_key` defaults to `on`."""
    key = parent_key or on
    columns = [c for c in parent.columns if c != key and c not in PARENT_SKIP]
    slim = parent.select(
        F.col(key).alias("_parent_key"), *[F.col(c).alias(prefix + c) for c in columns]
    )
    return df.join(slim, F.col(on) == F.col("_parent_key"), "left").drop("_parent_key")


def drop_empty_columns(df: DataFrame) -> DataFrame:
    """`df` without the columns that are null in every row (one aggregation)."""
    counts = df.agg(*[F.count(F.col(c)).alias(c) for c in df.columns]).first()
    return df.select(*[c for c in df.columns if counts[c]])


def master_rows(master: DataFrame, entity_type: str, key: str) -> DataFrame:
    """The `entity_master` rows of one entity type, keyed by `key`, without the
    columns that no entity of the type fills."""
    rows = from_silver(master).filter(F.col("entity_type") == entity_type)
    return drop_empty_columns(rows.drop(key).withColumnRenamed("entity_key", key))


# COMMAND ----------

# DBTITLE 1,Genomics profile rules and typed tables
# Variant rules kept out of `variant_profile`: `structural_variant_profile` holds
# their attributes (variant rows and variant x gene rows).
STRUCTURAL_RULES = ("structural_variant_features",)
# The widest typed table (owner, 2026-11): record kinds that would pass it stay apart.
MAX_TYPED_COLUMNS = 140
GENOMICS_SILVER_SCHEMAS = ("genomics_silver", "genomics_silver_reference")
ENERGY_SILVER_SCHEMAS = ("energy_silver", "energy_silver_reference")
COMMERCE_SILVER_SCHEMAS = ("commerce_silver",)


def mixed_columns(dtypes: list) -> set:
    """The column names that have more than one type in the (name, type) lists."""
    seen: dict[str, set] = {}
    for listing in dtypes:
        for name, dtype in listing:
            seen.setdefault(name, set()).add(dtype)
    return {name for name, kinds in seen.items() if len(kinds) > 1}


def typed_table(parts: dict) -> tuple:
    """One table from `parts`, kind -> (frame, key columns). The columns are the
    union of the parts; `record_kind` names the part and `record_key` is the hash
    of the kind and the part's key columns. A name with two types becomes text in
    every part. Returns (frame, grain); raises when the table is wider than
    MAX_TYPED_COLUMNS (two Gold load columns are added after this call)."""
    mixed = mixed_columns([frame.dtypes for frame, _ in parts.values()])
    out = None
    for kind, (frame, keys) in parts.items():
        for name in mixed & set(frame.columns):
            frame = frame.withColumn(name, F.col(name).cast("string"))
        part = frame.withColumn("record_kind", F.lit(kind)).withColumn(
            "record_key", sha_key("record_kind", *keys)
        )
        out = part if out is None else out.unionByName(part, allowMissingColumns=True)
    first = ["record_kind", "record_key"]
    out = out.select(*first, *[c for c in out.columns if c not in first])
    if len(out.columns) + 2 > MAX_TYPED_COLUMNS:
        raise RuntimeError(
            f"typed table of {sorted(parts)} has {len(out.columns) + 2} columns, "
            f"more than {MAX_TYPED_COLUMNS}"
        )
    return out, first


def unfed_tables(feeds: dict, catalog: set) -> list:
    """The Silver tables of `catalog` that no Gold table of `feeds` (Gold table ->
    its Silver tables) is built from."""
    fed = {name for names in feeds.values() for name in names}
    return sorted(catalog - fed)


def silver_tables(schemas) -> set:
    """The table names of the given Silver schemas."""
    return {
        row["tableName"]
        for schema in schemas
        for row in spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}").collect()
    }


def pick_column(df: DataFrame, attribute: str, rule: str | None = None) -> str:
    """The column of a profile table that holds a pivoted Silver attribute: the
    attribute name, else `<rule>__<attribute>` (the pivot name when two rules
    share the attribute), else its one `<any rule>__<attribute>` name. Raises
    when no column or more than one column matches."""
    if attribute in df.columns:
        return attribute
    if rule and f"{rule}__{attribute}" in df.columns:
        return f"{rule}__{attribute}"
    found = [c for c in df.columns if c.endswith(f"__{attribute}")]
    if len(found) != 1:
        raise RuntimeError(f"attribute {attribute!r}: matching columns {found}")
    return found[0]


# COMMAND ----------

# DBTITLE 1,Pivot of Silver attributes (entity_derived_attribute)
VALUE_COLUMNS = {
    "text": "value_text",
    "number": "value_number",
    "boolean": "value_boolean",
}
# One profile cell: an entity, a rule, an attribute and a value type.
VALUE_GROUP = ("entity_type", "entity_key", "rule_name", "attribute", "value_type")
MULTI_VALUE_COLUMNS = (
    *VALUE_GROUP,
    *VALUE_COLUMNS.values(),
    "method",
    "matched_text",
    "text_source",
    "source_record_id",
)


def with_value_count(attributes: DataFrame) -> DataFrame:
    """`attributes` with `_values`: the rows of the same VALUE_GROUP."""
    return attributes.withColumn(
        "_values", F.count(F.lit(1)).over(Window.partitionBy(*VALUE_GROUP))
    )


def multi_value_attributes(attributes: DataFrame) -> DataFrame:
    """The attribute rows of a cell with more than one value; the profiles
    leave such a cell empty."""
    return (
        with_value_count(attributes)
        .filter(F.col("_values") > 1)
        .select(*MULTI_VALUE_COLUMNS)
    )


def attribute_columns(attributes: DataFrame, entity_type: str) -> dict:
    """(rule, attribute, value type) -> Gold column name for one entity type.
    A name used by two rules becomes `<rule>__<attribute>`; a name with two
    value types gets `__<type>`."""
    names = (
        attributes.filter(F.col("entity_type") == entity_type)
        .select("rule_name", "attribute", "value_type")
        .distinct()
        .collect()
    )
    rules_of: dict[str, set] = {}
    types_of: dict[tuple, set] = {}
    for r in names:
        rules_of.setdefault(r["attribute"], set()).add(r["rule_name"])
        types_of.setdefault((r["rule_name"], r["attribute"]), set()).add(
            r["value_type"]
        )
    columns = {}
    for r in names:
        rule, attribute, kind = r["rule_name"], r["attribute"], r["value_type"]
        name = attribute if len(rules_of[attribute]) == 1 else f"{rule}__{attribute}"
        if len(types_of[(rule, attribute)]) > 1:
            name = f"{name}__{kind}"
        columns[(rule, attribute, kind)] = name
    return columns


def pivot_attributes(
    attributes: DataFrame,
    entity_type: str,
    columns: dict,
    *,
    key: str,
    component: str,
    source: str,
    rid: str,
    rules=None,
    exclude=(),
) -> DataFrame:
    """One row per entity, one column per attribute of the chosen rules (all
    rules but `exclude` when `rules` is None); a keyword match adds
    `<column>__matched_text`. A cell with more than one value stays empty (its
    rows are in `multi_value_attribute`); raises when two attributes share a
    column."""
    chosen = {
        k: name
        for k, name in columns.items()
        if (k[0] in rules if rules is not None else k[0] not in exclude)
    }
    names = spark.createDataFrame(
        [(*k, name) for k, name in chosen.items()],
        "rule_name string, attribute string, value_type string, _column string",
    )
    rows = with_value_count(
        attributes.filter(F.col("entity_type") == entity_type).join(
            F.broadcast(names), ["rule_name", "attribute", "value_type"]
        )
    )
    rows = rows.filter(F.col("_values") == 1).drop("_values")
    assert_unique_grain(
        rows, ["entity_key", "_column"], component=component, source=source, rid=rid
    )
    parts = []
    for kind, value in VALUE_COLUMNS.items():
        values = sorted({n for k, n in chosen.items() if k[2] == kind})
        if values:
            parts.append(
                rows.filter(F.col("value_type") == kind)
                .groupBy("entity_key")
                .pivot("_column", values)
                .agg(F.first(value))
            )
    matched = rows.filter(F.col("matched_text").isNotNull())
    matched_names = [
        r["_column"] for r in matched.select("_column").distinct().collect()
    ]
    if matched_names:
        parts.append(
            matched.withColumn("_column", F.concat("_column", F.lit("__matched_text")))
            .groupBy("entity_key")
            .pivot("_column", sorted(f"{n}__matched_text" for n in matched_names))
            .agg(F.first("matched_text"))
        )
    if not parts:
        return rows.select(F.col("entity_key").alias(key)).limit(0)
    out = parts[0]
    for part in parts[1:]:
        out = out.join(part, "entity_key", "full")
    return out.withColumnRenamed("entity_key", key)


def join_profile(
    base: DataFrame,
    pivot: DataFrame,
    key: str,
    how: str = "full",
    suffix: str = "__attribute",
) -> DataFrame:
    """`base` joined with pivoted columns; a column named like a base column gets
    `suffix`, so no column name repeats."""
    for column in pivot.columns:
        if column != key and column in base.columns:
            pivot = pivot.withColumnRenamed(column, f"{column}{suffix}")
    return base.join(pivot, key, how)


def full_profile(
    base: DataFrame,
    attributes: DataFrame,
    entity_type: str,
    *,
    key: str,
    component: str,
    source: str,
    rid: str,
    exclude=(),
    columns: dict | None = None,
) -> DataFrame:
    """`base` (one row per entity) and every attribute of the entity type but the
    `exclude` rules; an entity in only one of the two keeps its row. `columns`
    (from `attribute_columns`) is read once per entity type when not given."""
    pivot = pivot_attributes(
        attributes,
        entity_type,
        columns or attribute_columns(attributes, entity_type),
        key=key,
        exclude=exclude,
        component=component,
        source=source,
        rid=rid,
    )
    return add_provenance(
        join_profile(base, pivot, key), source, rid, ecosystem="genomics"
    )
