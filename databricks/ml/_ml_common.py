# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ML SHARED LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the reusable ML-dataset plumbing pulled into every ML
# MAGIC notebook with `%run ../../_ml_common`. Definitions only -- no side
# MAGIC effects at import; the caller owns `spark`.
# MAGIC
# MAGIC Covers as-of joins, causal gap fill, fitted imputers, split calendars and
# MAGIC partition helpers, registry tables and the hard-fail gate. Gold is read,
# MAGIC never changed.

# COMMAND ----------

# DBTITLE 1,Analytics and Gold shared libraries (read_gold, read_analytics, CATALOG, ...)
# MAGIC %run ../analytics/_analytics_common

# COMMAND ----------

# DBTITLE 1,Imports
import datetime as _dt
import json as _json

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Configuration constants
ML_STAGE = "ml"
ML_SCHEMAS = {"energy": "energy_ml_datasets", "commerce": "commerce_ml_datasets"}
SCHEMA_OBJECT_QUOTA = 100
SCHEMA_OBJECT_WARN = 80
SCHEMA_OBJECT_STOP = 95
DATASET_VERSION = "v1"
SPLIT_VERSION = "s1"
NULL_DROP_THRESHOLD = 0.95
MIN_REALISED_LAG_DAYS = 2
PROJECT_TIMEZONE = "Europe/Berlin"

# Null classes: structural, declared gap, missing static attribute, absent by
# definition, pipeline defect, target or label.
NULL_CLASSES = ("A", "B", "C", "D", "E", "F")

# Columns that are entirely null in Gold (pipeline defects); never an input.
CLASS_E_COLUMNS = (
    "realised_value",
    "realised_window_coverage",
    "forecast_issue_timestamp",
)

# Upper bound in hours for a causal gap fill; the final limits come from
# utility/01_gap_limit_evaluation and are stored in the gap_limits table.
GAP_LIMIT_CAPS_HOURS = {
    "air_temperature": 3,
    "dew_point_temperature": 3,
    "relative_humidity": 3,
    "pressure_station": 3,
    "pressure_sea_level": 3,
    "wind_speed": 2,
    "wind_direction": 1,
    "cloud_cover": 2,
    "visibility": 2,
    "soil_temperature": 6,
    "smard_quarter_hour": 1,
}
# Never filled: precipitation, radiation, sunshine duration, gusts, present
# weather codes and Honda increments.
NEVER_FILLED = (
    "precipitation",
    "global_radiation",
    "diffuse_radiation",
    "longwave_radiation",
    "sunshine_duration",
    "wind_gust",
    "present_weather",
    "honda_increment",
)

# Split calendars: (start, end) per partition; end None means open.
SPLIT_CALENDARS = {
    "energy_daily": {
        "train": ("2018-10-01", "2023-12-31"),
        "validation": ("2024-01-01", "2024-12-31"),
        "test": ("2025-01-01", None),
    },
    "quarter_hour": {
        "train": ("2024-09-08", "2025-06-30"),
        "validation": ("2025-07-01", "2025-12-31"),
        "test": ("2026-01-01", None),
    },
    "redispatch": {
        "train": ("2013-04-02", "2018-12-31"),
        "validation": ("2019-01-01", "2019-12-31"),
        "test": ("2020-01-01", "2020-12-31"),
    },
    "honda": {
        "train": ("2018-01-01", "2021-12-31"),
        "validation": ("2022-01-01", "2022-12-31"),
        "test": ("2023-01-01", "2023-12-31"),
    },
}
# Embargo in days between partitions: label horizon plus the realised lag.
EMBARGO_DAYS = {
    "energy_daily": 2,
    "quarter_hour": 2,
    "redispatch": 2,
    "honda": 1,
}
# Rolling-origin validation blocks inside the training partition.
ROLLING_BLOCKS = {
    "energy_daily": [
        ("2021-01-01", "2021-12-31"),
        ("2022-01-01", "2022-12-31"),
        ("2023-01-01", "2023-12-31"),
    ],
    "quarter_hour": [("2025-01-01", "2025-03-31"), ("2025-04-01", "2025-06-30")],
    "redispatch": [
        ("2016-01-01", "2016-12-31"),
        ("2017-01-01", "2017-12-31"),
        ("2018-01-01", "2018-12-31"),
    ],
    "honda": [
        ("2019-01-01", "2019-12-31"),
        ("2020-01-01", "2020-12-31"),
        ("2021-01-01", "2021-12-31"),
    ],
}
USER_SPLIT_PERCENT = (70, 15, 15)
USER_HASH_SEED = "ecrmap-ml-s1"
GROUPED_FOLDS = 5
REES46_EVALUATION_START = "2019-11-01"
GA4_LAPSE_TRAIN_CUTOFF = "2020-11-20"
GA4_LAPSE_EVALUATION_CUTOFF = "2020-12-01"
SURVIVAL_WINDOW_START = "2021-01-01"
SURVIVAL_CENSOR_DATE = "2026-09-03"
SURVIVAL_CHECK_FIT_THROUGH = "2024-12-31"

# COMMAND ----------

# DBTITLE 1,Schema and table naming


def ml_schema_for(ecosystem: str) -> str:
    return ML_SCHEMAS[ecosystem]


def ml_fqn(table: str, ecosystem: str) -> str:
    return f"{CATALOG}.{ml_schema_for(ecosystem)}.{table}"


def ml_run_id() -> str:
    return _dt.datetime.now(_dt.UTC).strftime("ml-%Y%m%dT%H%M%SZ")


# COMMAND ----------

# DBTITLE 1,Registry table definitions (created by 00_ml_setup)
REGISTRY_DDL = {
    "dataset_manifest": (
        "dataset_id string, dataset_version string, ecosystem string, "
        "capability string, grain_key array<string>, dependency_group int, "
        "target_name string, provenance_tier string, status string, "
        "freeze_status string, frozen_delta_version bigint, notes string, "
        "updated_at timestamp"
    ),
    "split_specification": (
        "dataset_id string, split_version string, family string, rule string, "
        "train_start string, train_end string, validation_start string, "
        "validation_end string, test_start string, test_end string, "
        "embargo_days int, hash_seed string, fold_scheme string, notes string"
    ),
    "partition_manifest": (
        "dataset_id string, split_version string, grain_key string, "
        "partition string, fold_id int, group_key string, rule_id string, "
        "diagnostic_group string"
    ),
    "null_class_registry": (
        "dataset_id string, column_name string, null_class string, "
        "method string, parameters string, decided_at timestamp"
    ),
    "imputer_parameters": (
        "dataset_id string, split_version string, fold_id int, "
        "column_name string, method string, parameters string, "
        "fit_partition string, fit_rows bigint, fitted_at timestamp"
    ),
    "mask_specification": (
        "dataset_id string, split_version string, seed int, "
        "mask_length_hours int, weight double, block_start string, "
        "block_end string, held_out_units string"
    ),
    "gap_limits": (
        "variable string, cap_hours int, final_limit_hours int, method string, "
        "score double, baseline_score double, evaluated_at timestamp"
    ),
    "null_rate_profile": (
        "dataset_id string, column_name string, null_rate_train double, "
        "decision string, evaluated_at timestamp"
    ),
    "feature_contract": (
        "dataset_id string, column_name string, role string, clock string, "
        "lag_days int, source_table string, notes string"
    ),
    "gate_results": (
        "dataset_id string, gate string, check_name string, status string, "
        "detail string, run_id string, run_at timestamp"
    ),
}
# Column roles and the clocks a feature may declare. A load clock is never a
# feature; realised observations need lag_days >= MIN_REALISED_LAG_DAYS.
CONTRACT_ROLES = ("key", "time", "target", "label_input", "feature", "flag", "group")
FEATURE_CLOCKS = (
    "observation_realised",
    "publication_before_day",
    "calendar",
    "static_registry",
    "registry_effective",
    "session_prefix",
    "user_history_before_session",
    "derived_from_earlier",
    "perfect_prognosis",
    "context_window",
    "state_at_step",
)
REGISTRY_ECOSYSTEM_ONLY = {"gap_limits": "energy"}

# COMMAND ----------

# DBTITLE 1,ML provenance


def add_ml_provenance(
    df: DataFrame,
    dataset_id: str,
    ecosystem: str,
    rid: str,
    version: str = DATASET_VERSION,
) -> DataFrame:
    return (
        df.withColumn("dataset_id", F.lit(dataset_id))
        .withColumn("dataset_version", F.lit(version))
        .withColumn("ecosystem", F.lit(ecosystem))
        .withColumn("_ml_loaded_at", F.current_timestamp())
        .withColumn("_ml_run_id", F.lit(rid))
    )


def grain_key(*cols: str):
    return F.concat_ws("|", *[F.col(c).cast("string") for c in cols])


# COMMAND ----------

# DBTITLE 1,Hard-fail gate


def record_check(
    component: str,
    source: str,
    metric_name: str,
    condition: bool,
    *,
    detail: str = "",
    metric_value: float | None = None,
    rid: str,
) -> str:
    """Log a check to the audit table with stage='ml' without raising."""
    status = "PASS" if condition else "FAIL"
    row = spark.createDataFrame(
        [
            (
                rid,
                _dt.datetime.now(_dt.UTC).date(),
                source,
                ML_STAGE,
                component,
                metric_name,
                metric_value,
                status,
                detail or None,
                _dt.datetime.now(_dt.UTC),
            )
        ],
        "run_id string, run_date date, source string, stage string, component string, "
        "metric_name string, metric_value double, status string, error_detail string, "
        "recorded_at timestamp",
    )
    row.write.format("delta").mode("append").saveAsTable(AUDIT_TABLE)
    return status


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
    """Same mechanism as Gold's check(), logged with stage='ml'."""
    record_check(
        component,
        source,
        metric_name,
        condition,
        detail=detail,
        metric_value=metric_value,
        rid=rid,
    )
    if not condition:
        raise RuntimeError(
            f"ML GATE FAILED: {component}.{metric_name} -- {detail or 'no detail given'}"
        )


def assert_unique_grain(
    df: DataFrame, key_cols: list[str], *, component: str, source: str, rid: str
) -> None:
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


def assert_no_forbidden_columns(
    df: DataFrame, *, component: str, source: str, rid: str
) -> None:
    bad = [c for c in df.columns if c in CLASS_E_COLUMNS]
    check(
        component,
        source,
        "no_class_e_columns",
        not bad,
        detail=f"class E columns present: {bad}",
        metric_value=float(len(bad)),
        rid=rid,
    )


def assert_values_present(
    df: DataFrame,
    column: str,
    expected: list,
    *,
    component: str,
    source: str,
    rid: str,
) -> None:
    seen = {r[0] for r in df.select(column).distinct().collect()}
    missing = [v for v in expected if v not in seen]
    check(
        component,
        source,
        f"values_present:{column}",
        not missing,
        detail=f"missing={missing} seen={sorted(str(s) for s in seen)[:30]}",
        metric_value=float(len(missing)),
        rid=rid,
    )


# COMMAND ----------

# DBTITLE 1,Write, view and read with a per-schema object quota guard


def _schema_object_count(ecosystem: str) -> int:
    return spark.sql(f"SHOW TABLES IN {CATALOG}.{ml_schema_for(ecosystem)}").count()


def _quota_guard(full: str, ecosystem: str) -> None:
    if spark.catalog.tableExists(full):
        return
    n = _schema_object_count(ecosystem)
    if n >= SCHEMA_OBJECT_STOP:
        raise RuntimeError(
            f"{ml_schema_for(ecosystem)} holds {n} objects (quota "
            f"{SCHEMA_OBJECT_QUOTA}); merge structures before adding {full}"
        )
    if n >= SCHEMA_OBJECT_WARN:
        print(f"WARN {ml_schema_for(ecosystem)} holds {n} of {SCHEMA_OBJECT_QUOTA}")


def _ml_delta_rows(table: str) -> int | None:
    try:
        m = (
            spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1")
            .select("operationMetrics")
            .first()[0]
        )
        return int(m.get("numOutputRows")) if m and "numOutputRows" in m else None
    except Exception:
        return None


def write_ml(
    df: DataFrame,
    table: str,
    *,
    ecosystem: str,
    source: str,
    component: str,
    rid: str,
) -> int | None:
    """Deterministic full overwrite into the ecosystem's ML schema."""
    started = now_utc()
    full = ml_fqn(table, ecosystem)
    _quota_guard(full, ecosystem)
    if spark.catalog.tableExists(full):
        v = spark.sql(f"DESCRIBE HISTORY {full} LIMIT 1").first()["version"]
        print(f"ROLLBACK IF NEEDED: RESTORE TABLE {full} TO VERSION AS OF {v}")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full)
    )
    n = _ml_delta_rows(full)
    row = spark.createDataFrame(
        [
            (
                rid,
                source,
                ML_STAGE,
                component,
                None,
                None,
                None if n is None else int(n),
                "COMPLETE",
                started,
                _dt.datetime.now(_dt.UTC),
            )
        ],
        "run_id string, source string, stage string, component string, "
        "last_processed_ts timestamp, last_processed_row bigint, rows_written bigint, "
        "status string, started_at timestamp, completed_at timestamp",
    )
    row.write.format("delta").mode("append").saveAsTable(WATERMARK_TABLE)
    print(f"OK  {full}: {n if n is not None else '?'} rows")
    return n


def write_ml_view(select_sql: str, table: str, *, ecosystem: str) -> None:
    """CREATE OR REPLACE VIEW; select_sql uses fully-qualified table names."""
    full = ml_fqn(table, ecosystem)
    _quota_guard(full, ecosystem)
    spark.sql(f"CREATE OR REPLACE VIEW {full} AS\n{select_sql}")
    print(f"OK  {full}: view created")


def read_ml(table: str, *, ecosystem: str) -> DataFrame:
    return spark.table(ml_fqn(table, ecosystem))


def replace_rows(df: DataFrame, table: str, *, ecosystem: str, predicate: str) -> None:
    """Replace only the registry rows matching the predicate."""
    full = ml_fqn(table, ecosystem)
    cols = spark.table(full).columns
    (
        df.select(*cols)
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .saveAsTable(full)
    )
    print(f"OK  {full}: rows replaced where {predicate}")


# COMMAND ----------

# DBTITLE 1,Dataset, split and null-class registration


def register_dataset(
    dataset_id: str,
    ecosystem: str,
    *,
    capability: str,
    grain: list[str],
    dependency_group: int,
    target_name: str | None,
    provenance_tier: str | None,
    status: str,
    notes: str = "",
) -> None:
    row = spark.createDataFrame(
        [
            (
                dataset_id,
                DATASET_VERSION,
                ecosystem,
                capability,
                grain,
                dependency_group,
                target_name,
                provenance_tier,
                status,
                "not_frozen",
                None,
                notes or None,
                now_utc(),
            )
        ],
        REGISTRY_DDL["dataset_manifest"],
    )
    replace_rows(
        row,
        "dataset_manifest",
        ecosystem=ecosystem,
        predicate=f"dataset_id = '{dataset_id}'",
    )


def set_dataset_status(dataset_id: str, ecosystem: str, status: str) -> None:
    full = ml_fqn("dataset_manifest", ecosystem)
    spark.sql(
        f"UPDATE {full} SET status = '{status}', updated_at = current_timestamp() "
        f"WHERE dataset_id = '{dataset_id}'"
    )


def register_null_classes(dataset_id: str, ecosystem: str, rows: list[tuple]) -> None:
    """rows: (column_name, null_class, method, parameters_dict_or_None)."""
    data = [
        (
            dataset_id,
            c,
            k,
            m,
            None
            if p is None
            else (
                p if isinstance(p, str) else _json.dumps(p, sort_keys=True, default=str)
            ),
            now_utc(),
        )
        for c, k, m, p in rows
    ]
    df = spark.createDataFrame(data, REGISTRY_DDL["null_class_registry"])
    replace_rows(
        df,
        "null_class_registry",
        ecosystem=ecosystem,
        predicate=f"dataset_id = '{dataset_id}'",
    )


def register_feature_contract(
    dataset_id: str, ecosystem: str, rows: list[tuple]
) -> None:
    """rows: (column_name, role, clock, lag_days, source_table, notes)."""
    for _c, role, clock, lag, _s, _n in rows:
        if role not in CONTRACT_ROLES:
            raise ValueError(f"unknown role {role}")
        if role == "feature" and clock not in FEATURE_CLOCKS:
            raise ValueError(f"feature clock {clock} not allowed for {_c}")
        if (
            role == "feature"
            and clock == "observation_realised"
            and (lag or 0) < MIN_REALISED_LAG_DAYS
        ):
            raise ValueError(
                f"realised input {_c} needs lag >= {MIN_REALISED_LAG_DAYS}"
            )
    df = spark.createDataFrame(
        [(dataset_id, *r) for r in rows], REGISTRY_DDL["feature_contract"]
    )
    replace_rows(
        df,
        "feature_contract",
        ecosystem=ecosystem,
        predicate=f"dataset_id = '{dataset_id}'",
    )


def record_imputer(
    dataset_id: str,
    ecosystem: str,
    *,
    fold_id: int,
    rows: list[tuple],
    fit_partition: str = "train",
    fit_rows: int | None = None,
    split_version: str = SPLIT_VERSION,
) -> None:
    """rows: (column_name, method, parameters_dict)."""
    data = [
        (
            dataset_id,
            split_version,
            fold_id,
            c,
            m,
            _json.dumps(p, sort_keys=True, default=str),
            fit_partition,
            fit_rows,
            now_utc(),
        )
        for c, m, p in rows
    ]
    df = spark.createDataFrame(data, REGISTRY_DDL["imputer_parameters"])
    replace_rows(
        df,
        "imputer_parameters",
        ecosystem=ecosystem,
        predicate=(
            f"dataset_id = '{dataset_id}' AND split_version = '{split_version}' "
            f"AND fold_id = {fold_id}"
        ),
    )


def write_split_specification(rows: list[tuple], ecosystem: str) -> None:
    """rows follow the split_specification column order."""
    df = spark.createDataFrame(rows, REGISTRY_DDL["split_specification"])
    ids = ", ".join(f"'{r[0]}'" for r in rows)
    replace_rows(
        df,
        "split_specification",
        ecosystem=ecosystem,
        predicate=f"split_version = '{SPLIT_VERSION}' AND dataset_id IN ({ids})",
    )


def write_partition_manifest(
    keyed: DataFrame,
    dataset_id: str,
    ecosystem: str,
    *,
    key_cols: list[str],
    partition_col: str = "partition",
    fold_col: str | None = None,
    group_col: str | None = None,
    rule_id: str,
    diagnostic_col: str | None = None,
) -> None:
    """keyed: one row per grain key with a partition column."""
    out = keyed.select(
        F.lit(dataset_id).alias("dataset_id"),
        F.lit(SPLIT_VERSION).alias("split_version"),
        grain_key(*key_cols).alias("grain_key"),
        F.col(partition_col).alias("partition"),
        (F.col(fold_col).cast("int") if fold_col else F.lit(None).cast("int")).alias(
            "fold_id"
        ),
        (
            F.col(group_col).cast("string") if group_col else F.lit(None).cast("string")
        ).alias("group_key"),
        F.lit(rule_id).alias("rule_id"),
        (
            F.col(diagnostic_col).cast("string")
            if diagnostic_col
            else F.lit(None).cast("string")
        ).alias("diagnostic_group"),
    )
    replace_rows(
        out,
        "partition_manifest",
        ecosystem=ecosystem,
        predicate=f"dataset_id = '{dataset_id}' AND split_version = '{SPLIT_VERSION}'",
    )


def read_partition_manifest(dataset_id: str, ecosystem: str) -> DataFrame:
    return read_ml("partition_manifest", ecosystem=ecosystem).filter(
        (F.col("dataset_id") == dataset_id) & (F.col("split_version") == SPLIT_VERSION)
    )


def attach_partition(
    df: DataFrame, dataset_id: str, ecosystem: str, key_cols: list[str]
) -> DataFrame:
    """Join the stored partition, fold and group onto a dataset by grain key."""
    m = read_partition_manifest(dataset_id, ecosystem).select(
        "grain_key", "partition", "fold_id", "group_key"
    )
    return (
        df.withColumn("_grain_key", grain_key(*key_cols))
        .join(m, F.col("_grain_key") == m["grain_key"], "inner")
        .drop("grain_key", "_grain_key")
    )


# COMMAND ----------

# DBTITLE 1,Split rules -- date partitions, entity hashes, rolling folds


def calendar_partition(date_col: str, calendar_key: str):
    """train / embargo / validation / embargo / test / excluded by local date."""
    cal = SPLIT_CALENDARS[calendar_key]
    emb = EMBARGO_DAYS[calendar_key]
    d = F.col(date_col).cast("date")
    t0 = F.lit(cal["train"][0]).cast("date")
    v0 = F.lit(cal["validation"][0]).cast("date")
    s0 = F.lit(cal["test"][0]).cast("date")
    s_end = cal["test"][1]
    out = (
        F.when(d < t0, F.lit("excluded"))
        .when(d < F.date_sub(v0, emb), F.lit("train"))
        .when(d < v0, F.lit("embargo"))
        .when(d < F.date_sub(s0, emb), F.lit("validation"))
        .when(d < s0, F.lit("embargo"))
    )
    if s_end:
        out = out.when(d <= F.lit(s_end).cast("date"), F.lit("test")).otherwise(
            F.lit("excluded")
        )
    else:
        out = out.otherwise(F.lit("test"))
    return out


def rolling_fold(date_col: str, calendar_key: str):
    """Index of the rolling validation block a training date belongs to."""
    d = F.col(date_col).cast("date")
    out = F.lit(None).cast("int")
    for i, (a, b) in reversed(list(enumerate(ROLLING_BLOCKS[calendar_key]))):
        out = F.when(
            d.between(F.lit(a).cast("date"), F.lit(b).cast("date")), F.lit(i)
        ).otherwise(out)
    return out


def user_hash_bucket(key_cols: list[str], seed: str = USER_HASH_SEED):
    """0..99 stable bucket for an entity id (same entity, same bucket, always)."""
    parts = [F.lit(seed)] + [F.col(c).cast("string") for c in key_cols]
    return F.pmod(F.xxhash64(*parts), F.lit(100))


def user_partition(key_cols: list[str], seed: str = USER_HASH_SEED):
    b = user_hash_bucket(key_cols, seed)
    tr, va, _ = USER_SPLIT_PERCENT
    return (
        F.when(b < tr, F.lit("train"))
        .when(b < tr + va, F.lit("validation"))
        .otherwise(F.lit("test"))
    )


def grouped_fold(
    key_cols: list[str], seed: str = USER_HASH_SEED, k: int = GROUPED_FOLDS
):
    parts = [F.lit(seed + "-fold")] + [F.col(c).cast("string") for c in key_cols]
    return F.pmod(F.xxhash64(*parts), F.lit(k))


def partition_for_date_py(date_iso: str, calendar_key: str) -> str:
    """Pure-Python twin of calendar_partition (tests and manifests)."""
    cal = SPLIT_CALENDARS[calendar_key]
    emb = EMBARGO_DAYS[calendar_key]
    d = _dt.date.fromisoformat(date_iso)

    def dt(s):
        return _dt.date.fromisoformat(s)

    v0, s0 = dt(cal["validation"][0]), dt(cal["test"][0])
    if d < dt(cal["train"][0]):
        return "excluded"
    if d < v0 - _dt.timedelta(days=emb):
        return "train"
    if d < v0:
        return "embargo"
    if d < s0 - _dt.timedelta(days=emb):
        return "validation"
    if d < s0:
        return "embargo"
    end = cal["test"][1]
    if end and d > dt(end):
        return "excluded"
    return "test"


def fold_frames(df: DataFrame, calendar_key: str, k: int, date_col: str):
    """(train, validation) rows for rolling fold k from a manifest-joined frame."""
    a, _b = ROLLING_BLOCKS[calendar_key][k]
    emb = EMBARGO_DAYS[calendar_key]
    d = F.col(date_col).cast("date")
    train = df.filter(
        (F.col("partition") == "train") & (d < F.date_sub(F.lit(a).cast("date"), emb))
    )
    valid = df.filter((F.col("partition") == "train") & (F.col("fold_id") == k))
    return train, valid


# COMMAND ----------

# DBTITLE 1,As-of join and date lags


def as_of_join(
    left: DataFrame,
    right: DataFrame,
    *,
    entity_keys: list[str],
    grain: list[str],
    left_ts: str,
    right_ts: str,
    right_cols: list[str],
    lag_days: int = 0,
    lookback_days: int = 30,
) -> DataFrame:
    """Latest right row at or before left_ts minus lag_days, per left grain row.
    lookback_days bounds the join fan-out; unmatched rows keep null columns."""
    r = right.select(*entity_keys, right_ts, *right_cols)
    cond = [left[k] == r[k] for k in entity_keys]
    upper = left[left_ts] - F.expr(f"INTERVAL {int(lag_days)} DAYS")
    lower = upper - F.expr(f"INTERVAL {int(lookback_days)} DAYS")
    joined = left.join(r, cond + [r[right_ts] <= upper, r[right_ts] > lower], "left")
    w = Window.partitionBy(*[left[g] for g in grain]).orderBy(
        F.col(right_ts).desc_nulls_last()
    )
    keep = [left[c] for c in left.columns] + [F.col(c) for c in right_cols]
    return (
        joined.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(*keep, F.col(right_ts).alias(f"{right_ts}_used"))
    )


def add_date_lags(
    df: DataFrame,
    *,
    keys: list[str],
    date_col: str,
    cols: list[str],
    lags: list[int],
    realised: bool = True,
) -> DataFrame:
    """value at date - lag as `<col>_lag<N>`; realised inputs need lag >= 2 days."""
    if realised and min(lags) < MIN_REALISED_LAG_DAYS:
        raise ValueError(f"realised lags must be >= {MIN_REALISED_LAG_DAYS} days")
    out = df
    for n in lags:
        shifted = df.select(
            *keys,
            F.date_add(F.col(date_col), n).alias(date_col),
            *[F.col(c).alias(f"{c}_lag{n}") for c in cols],
        )
        out = out.join(shifted, [*keys, date_col], "left")
    return out


def add_timestamp_lags(
    df: DataFrame,
    *,
    keys: list[str],
    ts_col: str,
    cols: list[str],
    lag_days: list[int],
    realised: bool = True,
) -> DataFrame:
    """value at timestamp - N days as `<col>_lag<N>d` (dense timestamps)."""
    if realised and min(lag_days) < 1:
        raise ValueError("lags must be at least one day")
    out = df
    for n in lag_days:
        shifted = df.select(
            *keys,
            (F.col(ts_col) + F.expr(f"INTERVAL {int(n)} DAYS")).alias(ts_col),
            *[F.col(c).alias(f"{c}_lag{n}d") for c in cols],
        )
        out = out.join(shifted, [*keys, ts_col], "left")
    return out


# COMMAND ----------

# DBTITLE 1,Regular grid and causal gap fill


def regular_grid(
    df: DataFrame, *, keys: list[str], ts_col: str, step_seconds: int
) -> DataFrame:
    """One row per key and step between each key's first and last timestamp."""
    span = df.groupBy(*keys).agg(F.min(ts_col).alias("_a"), F.max(ts_col).alias("_b"))
    grid = span.select(
        *keys,
        F.explode(
            F.sequence("_a", "_b", F.expr(f"INTERVAL {int(step_seconds)} SECONDS"))
        ).alias(ts_col),
    )
    return grid.join(df, [*keys, ts_col], "left")


def causal_fill(
    df: DataFrame,
    *,
    keys: list[str],
    ts_col: str,
    value_col: str,
    cap_hours: float,
    method: str = "locf",
    climatology: DataFrame | None = None,
) -> DataFrame:
    """Fill a null with earlier values only, up to cap_hours after the last
    observation. Adds `<v>_filled` and `<v>_gap_hours`; original in `<v>_raw`.
    Methods: locf, locf_diurnal (needs climatology keys + hour_of_day, month,
    climatology_value), linear_extrapolation."""
    v = value_col
    w = Window.partitionBy(*keys).orderBy(ts_col)
    wc = w.rowsBetween(Window.unboundedPreceding, 0)
    base = df.withColumn(f"{v}_raw", F.col(v))
    obs_ts = F.when(F.col(v).isNotNull(), F.col(ts_col))
    base = base.withColumn("_last_ts", F.last(obs_ts, ignorenulls=True).over(wc))
    base = base.withColumn("_last_val", F.last(F.col(v), ignorenulls=True).over(wc))
    gap_h = (F.col(ts_col).cast("long") - F.col("_last_ts").cast("long")) / 3600.0
    base = base.withColumn("_gap_h", gap_h)
    cand = F.col("_last_val")
    if method == "locf_diurnal":
        if climatology is None:
            raise ValueError("locf_diurnal needs a climatology frame")
        clim = climatology.select(
            *keys,
            F.col("hour_of_day").alias("_hod"),
            F.col("month").alias("_mon"),
            F.col("climatology_value").alias("_clim_now"),
        )
        base = (
            base.withColumn("_hod", F.hour(ts_col))
            .withColumn("_mon", F.month(ts_col))
            .join(clim, [*keys, "_hod", "_mon"], "left")
        )
        w2 = (
            Window.partitionBy(*keys)
            .orderBy(ts_col)
            .rowsBetween(Window.unboundedPreceding, 0)
        )
        base = base.withColumn(
            "_clim_last",
            F.last(
                F.when(F.col(v).isNotNull(), F.col("_clim_now")), ignorenulls=True
            ).over(w2),
        )
        cand = F.col("_last_val") + (F.col("_clim_now") - F.col("_clim_last"))
    elif method == "linear_extrapolation":
        obs = base.filter(F.col(v).isNotNull())
        wo = Window.partitionBy(*keys).orderBy(ts_col)
        obs = obs.select(
            *keys,
            ts_col,
            (
                (F.col(v) - F.lag(v).over(wo))
                / (
                    (F.col(ts_col).cast("long") - F.lag(ts_col).over(wo).cast("long"))
                    / 3600.0
                )
            ).alias("_slope_obs"),
        )
        base = base.join(obs, [*keys, ts_col], "left")
        base = base.withColumn(
            "_slope", F.last("_slope_obs", ignorenulls=True).over(wc)
        )
        cand = F.col("_last_val") + F.coalesce(F.col("_slope"), F.lit(0.0)) * F.col(
            "_gap_h"
        )
    fillable = (
        F.col(v).isNull()
        & F.col("_last_val").isNotNull()
        & (F.col("_gap_h") <= F.lit(float(cap_hours)))
    )
    out = (
        base.withColumn(f"{v}_filled", fillable)
        .withColumn(
            f"{v}_gap_hours",
            F.when(F.col(v).isNull(), F.col("_gap_h")).cast("double"),
        )
        .withColumn(v, F.when(fillable, cand).otherwise(F.col(v)))
    )
    drop = [
        c
        for c in out.columns
        if c
        in (
            "_last_ts",
            "_last_val",
            "_gap_h",
            "_hod",
            "_mon",
            "_clim_now",
            "_clim_last",
            "_slope",
            "_slope_obs",
        )
    ]
    return out.drop(*drop)


def load_gap_limits(ecosystem: str = "energy") -> dict:
    """Final limits from the evaluation table when present, else the caps."""
    limits = dict(GAP_LIMIT_CAPS_HOURS)
    full = ml_fqn("gap_limits", ecosystem)
    if spark.catalog.tableExists(full):
        for r in spark.table(full).collect():
            if r["final_limit_hours"] is not None:
                limits[r["variable"]] = min(
                    int(r["final_limit_hours"]),
                    GAP_LIMIT_CAPS_HOURS.get(r["variable"], 0),
                )
    return limits


# COMMAND ----------

# DBTITLE 1,Null profile, missing indicators, drop rule


def null_rates(df: DataFrame, cols: list[str] | None = None) -> dict:
    """Null share per column in one pass."""
    cols = cols or df.columns
    row = df.agg(
        *[F.avg(F.col(c).isNull().cast("double")).alias(c) for c in cols]
    ).first()
    return {c: (0.0 if row[c] is None else float(row[c])) for c in cols}


def columns_to_drop(
    rates: dict, *, threshold: float = NULL_DROP_THRESHOLD, exempt: set | None = None
) -> list[str]:
    """Columns at or above the threshold (always at 1.0); exempt ones stay."""
    exempt = exempt or set()
    return sorted(
        c for c, r in rates.items() if c not in exempt and (r >= threshold or r >= 1.0)
    )


def add_missing_indicators(df: DataFrame, cols: list[str]) -> DataFrame:
    for c in cols:
        df = df.withColumn(f"{c}_is_missing", F.col(c).isNull())
    return df


# COMMAND ----------

# DBTITLE 1,Fitted imputers -- fit on the training partition only


def assert_train_only(df: DataFrame, *, partition_col: str = "partition") -> None:
    """A fit input must hold training rows only."""
    other = df.filter(F.col(partition_col) != "train").limit(1).count()
    if other:
        raise RuntimeError("fit input contains non-training rows")


def fit_group_median(train: DataFrame, value_col: str, group_cols: list[str]) -> dict:
    known = train.filter(F.col(value_col).isNotNull())
    rows = (
        known.groupBy(*group_cols)
        .agg(F.percentile_approx(value_col, 0.5).alias("m"), F.count("*").alias("n"))
        .collect()
    )
    overall = known.agg(F.percentile_approx(value_col, 0.5)).first()[0]
    return {
        "group_cols": group_cols,
        "overall": overall,
        "groups": [[*(r[c] for c in group_cols), r["m"], r["n"]] for r in rows],
    }


def apply_group_median(
    df: DataFrame, value_col: str, params: dict, *, out_col: str | None = None
) -> DataFrame:
    gc = params["group_cols"]
    cols = [*gc, "_gm"]
    ref = spark.createDataFrame(
        [(*g[: len(gc)], g[len(gc)]) for g in params["groups"]], cols
    )
    out = df.join(F.broadcast(ref), gc, "left")
    return out.withColumn(
        out_col or value_col,
        F.coalesce(F.col(value_col), F.col("_gm"), F.lit(params["overall"])),
    ).drop("_gm")


def fit_category_levels(
    train: DataFrame, col: str, min_count: int = 20, max_levels: int = 2000
) -> list:
    """Most frequent levels of a categorical (at least min_count rows, at most
    max_levels); everything else becomes the rare level when applied."""
    rows = (
        train.filter(F.col(col).isNotNull())
        .groupBy(col)
        .count()
        .filter(F.col("count") >= min_count)
        .orderBy(F.col("count").desc())
        .limit(max_levels)
        .collect()
    )
    return sorted(str(r[col]) for r in rows)


def apply_category_levels(
    df: DataFrame,
    col: str,
    levels: list,
    *,
    unknown: str = "unknown",
    rare: str = "other",
) -> DataFrame:
    return df.withColumn(
        col,
        F.when(F.col(col).isNull(), F.lit(unknown))
        .when(F.col(col).cast("string").isin(levels), F.col(col).cast("string"))
        .otherwise(F.lit(rare)),
    )


def _np():
    import numpy as np

    return np


def static_impute_scores(
    pdf,
    target: str,
    features: list[str],
    group_cols: list[str],
    *,
    seed: int = 42,
    mask_fraction: float = 0.2,
    k: int = 5,
) -> dict:
    """Mask known values, refill with each method, return MAE per method.
    pdf is a small pandas frame (driver side, guarded by size)."""
    np = _np()
    if len(pdf) > 1_000_000:
        raise RuntimeError("static imputation is for small entity tables only")
    known = pdf[pdf[target].notna()].reset_index(drop=True)
    rng = np.random.default_rng(seed)
    mask = rng.random(len(known)) < mask_fraction
    train, test = known[~mask], known[mask]
    scores = {}
    med = train.groupby(group_cols)[target].median()
    overall = float(train[target].median())
    pred = test.set_index(group_cols).index.map(lambda i: med.get(i, overall))
    scores["group_median"] = float(
        np.mean(np.abs(np.asarray(pred, float) - test[target].values))
    )
    scores["knn"] = float(
        np.mean(
            np.abs(_knn_predict(train, test, target, features, k) - test[target].values)
        )
    )
    scores["regression"] = float(
        np.mean(
            np.abs(_ols_predict(train, test, target, features) - test[target].values)
        )
    )
    return scores


def _standardise(train, test, features):
    np = _np()
    a = train[features].astype(float).fillna(train[features].astype(float).median())
    b = test[features].astype(float).fillna(train[features].astype(float).median())
    mu, sd = a.mean(), a.std().replace(0, 1.0)
    return ((a - mu) / sd).values, ((b - mu) / sd).values, np


def _knn_predict(train, test, target, features, k):
    a, b, np = _standardise(train, test, features)
    y = train[target].values
    out = np.empty(len(b))
    for i in range(0, len(b), 2000):
        d = ((b[i : i + 2000, None, :] - a[None, :, :]) ** 2).sum(axis=2)
        idx = np.argpartition(d, kth=min(k, d.shape[1] - 1), axis=1)[:, :k]
        out[i : i + 2000] = y[idx].mean(axis=1)
    return out


def _ols_predict(train, test, target, features):
    a, b, np = _standardise(train, test, features)
    a1 = np.hstack([a, np.ones((len(a), 1))])
    b1 = np.hstack([b, np.ones((len(b), 1))])
    coef, *_ = np.linalg.lstsq(a1, train[target].values, rcond=None)
    return b1 @ coef


def static_impute_predict(
    train,
    target_frame,
    target: str,
    features: list[str],
    method: str,
    group_cols: list[str],
    k: int = 5,
):
    """Predictions for target_frame rows using a method fitted on train rows."""
    np = _np()
    if method == "group_median":
        med = train.groupby(group_cols)[target].median()
        overall = float(train[target].median())
        idx = target_frame.set_index(group_cols).index
        return np.asarray(idx.map(lambda i: med.get(i, overall)), float)
    if method == "knn":
        return _knn_predict(train, target_frame, target, features, k)
    return _ols_predict(train, target_frame, target, features)


# COMMAND ----------

# DBTITLE 1,Calendar helpers (rule-based holidays)


def easter_sunday(year: int) -> _dt.date:
    a, b, c = year % 19, year // 100, year % 100
    d, e = b // 4, b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    lval = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * lval) // 451
    month = (h + lval - 7 * m + 114) // 31
    day = ((h + lval - 7 * m + 114) % 31) + 1
    return _dt.date(year, month, day)


# Fixed-date holidays by country, plus Easter offsets in days.
HOLIDAY_RULES = {
    "DE": {
        "fixed": [(1, 1), (5, 1), (10, 3), (12, 25), (12, 26)],
        "easter": [-2, 1, 39, 50],
    },
    "AT": {
        "fixed": [
            (1, 1),
            (1, 6),
            (5, 1),
            (8, 15),
            (10, 26),
            (11, 1),
            (12, 8),
            (12, 25),
            (12, 26),
        ],
        "easter": [1, 39, 50, 60],
    },
    "LU": {
        "fixed": [
            (1, 1),
            (5, 1),
            (5, 9),
            (6, 23),
            (8, 15),
            (11, 1),
            (12, 25),
            (12, 26),
        ],
        "easter": [1, 39, 50],
    },
    "PL": {
        "fixed": [
            (1, 1),
            (1, 6),
            (5, 1),
            (5, 3),
            (8, 15),
            (11, 1),
            (11, 11),
            (12, 25),
            (12, 26),
        ],
        "easter": [0, 1, 49, 60],
    },
}
MARKET_AREA_COUNTRY = {
    "de_lu": "DE",
    "fifty_hertz": "DE",
    "amprion": "DE",
    "tennet_de": "DE",
    "transnetbw": "DE",
    "pse_poland": "PL",
}


def holiday_dates(country: str, year: int) -> list[_dt.date]:
    rule = HOLIDAY_RULES[country]
    easter = easter_sunday(year)
    out = [_dt.date(year, m, d) for m, d in rule["fixed"]]
    out += [easter + _dt.timedelta(days=o) for o in rule["easter"]]
    return sorted(set(out))


# COMMAND ----------

# DBTITLE 1,Wind unit selection and the unit to control-zone chain
WIND_OFFSHORE_PATTERN = "offshore|off_shore|auf_see|see"
OPERATING_STATES = ("operating", "decommissioned", "provisionally_shutdown")


def wind_units(gu: DataFrame) -> DataFrame:
    """Wind units of Gold generation_unit with an offshore flag."""
    return gu.filter(F.col("wind_onshore_or_offshore").isNotNull()).withColumn(
        "is_offshore",
        F.col("offshore_sea_area").isNotNull()
        | F.lower(F.col("wind_onshore_or_offshore")).rlike(WIND_OFFSHORE_PATTERN),
    )


def normalise_zone_key(col):
    return F.regexp_replace(F.lower(F.trim(col)), r"[^a-z0-9]", "")


def unit_zone_map(units: DataFrame, connection_points: DataFrame, zone_map: DataFrame):
    """unit_id -> market_area_code through location -> connection point ->
    control zone. Multi-zone locations and units without a location or zone are
    kept with a status and no zone."""
    cp = connection_points.filter(
        F.col("location_id").isNotNull() & F.col("control_zone").isNotNull()
    )
    loc = cp.groupBy("location_id").agg(
        F.countDistinct("control_zone").alias("n_zones"),
        F.first("control_zone").alias("control_zone"),
    )
    zm = zone_map.select("control_zone_key", "market_area_code")
    j = (
        units.select("unit_id", "location_id")
        .join(loc, "location_id", "left")
        .withColumn("control_zone_key", normalise_zone_key(F.col("control_zone")))
        .join(zm, "control_zone_key", "left")
    )
    return j.select(
        "unit_id",
        F.when(F.col("n_zones") == 1, F.col("market_area_code")).alias(
            "market_area_code"
        ),
        F.when(F.col("location_id").isNull(), F.lit("no_location"))
        .when(F.col("n_zones").isNull(), F.lit("no_zone"))
        .when(F.col("n_zones") > 1, F.lit("multi_zone_location"))
        .when(F.col("market_area_code").isNull(), F.lit("zone_not_mapped"))
        .otherwise(F.lit("zoned"))
        .alias("zone_status"),
    )


# COMMAND ----------

# DBTITLE 1,Session time zone and local-day timestamps


def set_utc_session() -> None:
    spark.conf.set("spark.sql.session.timeZone", "UTC")


def local_day_start_utc(date_col: str):
    """UTC instant of local midnight (Europe/Berlin) for a local date column."""
    return F.to_utc_timestamp(F.col(date_col).cast("timestamp"), PROJECT_TIMEZONE)


def local_day_hours(date_col: str):
    """23, 24 or 25 for a local date."""
    nxt = F.to_utc_timestamp(
        F.date_add(F.col(date_col), 1).cast("timestamp"), PROJECT_TIMEZONE
    )
    return (nxt.cast("long") - local_day_start_utc(date_col).cast("long")) / 3600.0


# COMMAND ----------

# DBTITLE 1,Assembled dataset SQL and contract derivation
import re as _re

PROVENANCE_COLUMNS = (
    "dataset_id",
    "dataset_version",
    "ecosystem",
    "_ml_loaded_at",
    "_ml_run_id",
)
CALENDAR_COLUMNS = (
    "day_of_week",
    "is_weekend",
    "month",
    "day_of_year",
    "iso_week",
    "year",
    "is_holiday",
    "is_bridge_day",
    "local_day_hours",
    "hour_of_day",
    "quarter_of_hour",
    "commissioning_month_number",
)
DERIVED_SUFFIXES_ML = ("_is_missing", "_filled", "_gap_hours", "_raw", "_imputed")


def assembled_select(
    dataset_id: str,
    target_table: str,
    ecosystem: str,
    joins: list[dict],
    *,
    where: str | None = None,
    target_drop: list[str] | None = None,
    extra_select: list[str] | None = None,
) -> str:
    """SQL for an assembled view: the target table plus feature tables joined at
    the dataset grain. Each join: table, keys, drop (extra columns to exclude),
    how (default left). Provenance columns of every side are dropped."""
    t_drop = ", ".join([*PROVENANCE_COLUMNS, *(target_drop or [])])
    sel = [
        f"'{dataset_id}' AS dataset_id",
        f"t.* EXCEPT ({t_drop})",
        *(extra_select or []),
    ]
    frm = [f"{ml_fqn(target_table, ecosystem)} t"]
    for i, j in enumerate(joins):
        alias = f"f{i}"
        drop = ", ".join([*j["keys"], *j.get("drop", []), *PROVENANCE_COLUMNS])
        sel.append(f"{alias}.* EXCEPT ({drop})")
        on = " AND ".join(f"t.{k} = {alias}.{k}" for k in j["keys"])
        frm.append(
            f"{j.get('how', 'left').upper()} JOIN {ml_fqn(j['table'], ecosystem)} {alias} ON {on}"
        )
    sql = "SELECT " + ",\n       ".join(sel) + "\nFROM " + "\n".join(frm)
    if where:
        sql += f"\nWHERE {where}"
    return sql


def derive_contract(
    columns: list[str],
    *,
    keys: list[str],
    time_col: str | None,
    static: tuple = (),
    registry: tuple = (),
    perfect_prognosis: tuple = (),
    context: tuple = (),
    group_col: str | None = None,
    overrides: dict | None = None,
) -> tuple[list, list, list]:
    """(contract rows, null-class rows, defaulted columns) from column names.
    contract row: (column, role, clock, lag_days, source_table, notes);
    null-class row: (column, class, method, parameters)."""
    overrides = overrides or {}
    contract, classes, defaulted = [], [], []
    for c in columns:
        if c in PROVENANCE_COLUMNS:
            continue
        if c in overrides:
            role, clock, lag = overrides[c]
        elif c in keys:
            role, clock, lag = "key", None, None
        elif c == time_col:
            role, clock, lag = "time", None, None
        elif group_col and c == group_col:
            role, clock, lag = "group", None, None
        elif c.startswith("target_"):
            role, clock, lag = "target", None, None
        elif (
            c == "provenance_tier" or c == "as_of_ts" or c.endswith(DERIVED_SUFFIXES_ML)
        ):
            role, clock, lag = "flag", None, None
        elif _re.search(r"_lag(\d+)d?$", c):
            role, clock = "feature", "observation_realised"
            lag = int(_re.search(r"_lag(\d+)d?$", c).group(1))
        elif c.startswith("forecast_"):
            role, clock, lag = "feature", "publication_before_day", None
        elif c.startswith(("prefix_", "seq_")):
            role, clock, lag = "feature", "session_prefix", None
        elif c.startswith(("user_hist_", "lapse_")):
            role, clock, lag = "feature", "user_history_before_session", None
        elif c.startswith("product_pop_"):
            role, clock, lag = "feature", "derived_from_earlier", None
        elif c in CALENDAR_COLUMNS:
            role, clock, lag = "feature", "calendar", None
        elif c in static:
            role, clock, lag = "feature", "static_registry", None
        elif c in registry:
            role, clock, lag = "feature", "registry_effective", None
        elif c in perfect_prognosis:
            role, clock, lag = "feature", "perfect_prognosis", None
        elif c in context:
            role, clock, lag = "feature", "context_window", None
        else:
            role, clock, lag = "feature", "derived_from_earlier", None
            defaulted.append(c)
        contract.append((c, role, clock, lag, None, None))
        if role == "target":
            classes.append((c, "F", "drop_rows_without_target", None))
        elif role == "feature":
            k = (
                "C"
                if clock == "static_registry"
                else ("A" if clock == "calendar" else "B")
            )
            m = {
                "C": "group_median_or_unknown",
                "A": "none_expected",
                "B": "leave_null_with_flag",
            }[k]
            classes.append((c, k, m, None))
    return contract, classes, defaulted