# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # MODEL SHARED LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** plumbing shared by every model notebook, pulled in with
# MAGIC `%run ../lib/_model_common`. Definitions only; the one side effect is adding the
# MAGIC model library folder to the import path.
# MAGIC
# MAGIC Covers the tasks of the task file, frozen-dataset reads (test partition never
# MAGIC read), feature resolution, the feature encoder, the isolated candidate runner,
# MAGIC result recording, MLflow logging, the metrics and the gate `guard`. No model
# MAGIC logic lives here.

# COMMAND ----------

# DBTITLE 1,ML shared library (read_ml, ml_fqn, check, user_hash_bucket, ...)
# MAGIC %run ../../ml/_ml_common

# COMMAND ----------

# DBTITLE 1,Imports
import contextlib as _contextlib
import datetime as _dt
import gc as _gc
import hashlib as _hashlib
import importlib as _importlib
import importlib.metadata as _importlib_metadata
import json as _json
import logging as _logging
import math as _math
import os as _os
import platform as _platform
import re as _re
import signal as _signal
import sys as _sys
import tempfile as _tempfile
import threading as _threading
import time as _time
import traceback as _traceback

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration constants
MODEL_STAGE = "models"
MODEL_SCHEMA = "ml_models"
ECOSYSTEMS = tuple(ML_SCHEMAS)
MODEL_SCHEMAS = dict.fromkeys(ECOSYSTEMS, MODEL_SCHEMA)
# a check recorded per ecosystem carries the ecosystem after a colon
OTHER_ECOSYSTEM_SUFFIXES = {
    eco: tuple(f":{other}" for other in ECOSYSTEMS if other != eco)
    for eco in ECOSYSTEMS
}
TASK_SPEC = load_yaml("src/schemas/mappings/model_tasks.yml")
VOLUME_SCHEMA = "energy_ml_models"
MODEL_SEED = 42
LIBRARY_VOLUME = "model_libraries"
MODEL_SPLIT_VERSION = "e1"
ALLOWED_PARTITIONS = ("train", "validation")
SMOKE_ROWS = 4000
TRAIN_ROW_CAP = 6_000_000
TUNE_MAX_ROWS = 500_000
TUNE_MAX_FOLDS = 3
USER_SAMPLE_PERCENT = 10
USER_SAMPLE_SEED = "ecrmap-model-sample-s1"
MIN_CLASS_ROWS = 10
TUNE_HASH_FOLDS = 5
SURVIVAL_HORIZONS_YEARS = (1, 3, 5)
QUANTILES = (0.1, 0.5, 0.9)
RANK_KS = (5, 10, 20)
FORWARD_TOP_N = 2
MLFLOW_EXPERIMENT_PREFIX = "/Shared/ecrmap_models"
RESULT_COLUMNS = (
    "dataset_id string, task_id string, model_name string, family string, "
    "stage string, partition string, metric string, value double, n_rows bigint, "
    "status string, detail string, frozen_delta_version bigint, "
    "mlflow_run_id string, artifact_status string, params string, smoke boolean, "
    "run_id string, run_at timestamp"
)
RUN_CONTEXT_COLUMNS = (
    "task_id string, dataset_id string, smoke boolean, "
    "run_id string, status string, notebook_path string, job_id string, "
    "job_run_id string, frozen_delta_version bigint, data_notes string, "
    "library_versions string, detail string, recorded_at timestamp"
)
MODEL_DDL = {
    "evaluation_spec": (
        "dataset_id string, spec_key string, spec_value string, run_id string, "
        "recorded_at timestamp"
    ),
    "candidate_results": RESULT_COLUMNS,
    "candidate_selection": (
        "task_id string, dataset_id string, model_name string, family string, "
        "rank int, primary_metric string, primary_value double, is_baseline boolean, "
        "mlflow_run_id string, frozen_delta_version bigint, run_id string, "
        "selected_at timestamp"
    ),
    "library_availability": (
        "library string, version string, available boolean, checked_at timestamp"
    ),
    "task_run_context": RUN_CONTEXT_COLUMNS,
    "evaluation_run_context": RUN_CONTEXT_COLUMNS,
    "evaluation_results": (
        "dataset_id string, task_id string, model_name string, family string, "
        "stage string, partition string, metric string, value double, "
        "validation_value double, n_rows bigint, status string, detail string, "
        "frozen_delta_version bigint, mlflow_run_id string, smoke boolean, "
        "run_id string, run_at timestamp"
    ),
    "evaluation_flags": (
        "task_id string, model_name string, stage string, flag string, "
        "detail string, value double, threshold double, run_id string, "
        "flagged_at timestamp"
    ),
    "approval_spec": (
        "rule_id string, rule_class string, description string, parameter string, "
        "parameter_value string, run_id string, recorded_at timestamp"
    ),
    "model_approval": (
        "task_id string, dataset_id string, model_name string, role string, "
        "rank int, decision string, restriction string, "
        "recommended_decision string, recommended_restriction string, "
        "overridden boolean, override_reason string, rules string, "
        "conditions string, segments string, notes string, primary_metric string, "
        "validation_value double, evaluated_value double, "
        "frozen_delta_version bigint, mlflow_run_id string, decided_by string, "
        "recommended_at timestamp, decided_at timestamp, run_id string"
    ),
    "model_registry": (
        "task_id string, dataset_id string, model_name string, role string, "
        "version int, lifecycle_status string, decision string, restriction string, "
        "conditions string, primary_metric string, validation_value double, "
        "frozen_delta_version bigint, mlflow_run_id string, artifact_uri string, "
        "approval_run_id string, approval_decided_at timestamp, card_path string, "
        "fit_library_versions string, reason string, run_id string, "
        "registered_at timestamp, code_version string, data_fingerprint string, "
        "artifact_sha256 string, replaces string, task_spec_hash string"
    ),
    "registry_check": (
        "task_id string, model_name string, role string, registry_version int, "
        "processor_type string, python_version string, library_versions string, "
        "reload_status string, reload_detail string, rescore_status string, "
        "recorded_value double, rescored_value double, gap_relative double, "
        "tolerance double, status string, detail string, run_id string, "
        "checked_at timestamp"
    ),
    "monitoring_spec": (
        "parameter string, parameter_value string, description string, "
        "run_id string, recorded_at timestamp"
    ),
    "reference_profile": (
        "task_id string, model_name string, registry_version int, "
        "dataset_id string, frozen_delta_version bigint, subject string, "
        "value_kind string, n_rows bigint, null_rate double, min_value double, "
        "max_value double, bin_edges string, bin_shares string, run_id string, "
        "recorded_at timestamp"
    ),
    "model_predictions": (
        "task_id string, model_name string, registry_version int, "
        "window_id string, row_key string, prediction double, "
        "prediction_label string, frozen_delta_version bigint, run_id string, "
        "scored_at timestamp"
    ),
    "monitoring_results": (
        "task_id string, model_name string, registry_version int, "
        "window_id string, check_kind string, subject string, value double, "
        "warning_threshold double, flag_threshold double, level string, "
        "n_reference bigint, n_window bigint, detail string, run_id string, "
        "measured_at timestamp"
    ),
    "monitoring_flags": (
        "task_id string, model_name string, registry_version int, "
        "window_id string, check_kind string, subject string, level string, "
        "value double, threshold double, detail string, run_id string, "
        "flagged_at timestamp"
    ),
    "monitoring_triggers": (
        "task_id string, model_name string, registry_version int, "
        "trigger_kind string, source string, condition string, detail string, "
        "value double, threshold double, run_id string, raised_at timestamp"
    ),
    "scoring_requests": (
        "request_id string, task_id string, model_name string, "
        "registry_version int, key_filter string, window_start string, "
        "window_end string, row_key string, prediction double, "
        "prediction_q10 double, prediction_q90 double, "
        "frozen_delta_version bigint, run_id string, scored_at timestamp"
    ),
}
# imported instead of the bare name so a partial install counts as missing
LIBRARY_PROBES = {"torch": "torch.nn", "sksurv": "sksurv.ensemble"}
LIBRARY_OTHER_DISTRIBUTIONS = {"mlflow": ("mlflow-skinny",)}
LIBRARIES = {
    "sklearn": "scikit-learn",
    "lightgbm": "lightgbm",
    "xgboost": "xgboost",
    "lifelines": "lifelines",
    "sksurv": "scikit-survival",
    "torch": "torch",
    "mlflow": "mlflow",
    "cloudpickle": "cloudpickle",
    "psutil": "psutil",
}

# COMMAND ----------

# DBTITLE 1,Tasks from the task file
TASK_FIELDS = (
    "dataset_id",
    "ecosystem",
    "paradigm",
    "task_type",
    "target",
    "baseline",
    "primary_metric",
    "higher_is_better",
    "runner",
    "fit",
    "eval",
)
TASKS = [
    {"task_id": task_id, **{k: v for k, v in entry.items()}}
    for task_id, entry in TASK_SPEC["tasks"].items()
]
TASK_BY_ID = {t["task_id"]: t for t in TASKS}
CHECK_ONLY = {
    name: {"task_id": f"{name}.check", **entry}
    for name, entry in TASK_SPEC["check_only"].items()
}
# a check-only dataset is reported like a task: the identity of the task that scores it
CHECK_TASKS = [
    {
        **TASK_BY_ID[entry["scored_by"]],
        "task_id": entry["task_id"],
        "dataset_id": entry["dataset_id"],
    }
    for entry in CHECK_ONLY.values()
]
FIT_ONLY_KEYS = tuple(TASK_SPEC["fit_only_keys"])
BLOCK_RULES = TASK_SPEC["blocks"]


def tasks_of(runner: str | tuple) -> list[dict]:
    """The tasks of one runner (or several), in the order of the task file."""
    names = (runner,) if isinstance(runner, str) else tuple(runner)
    return [t for t in TASKS if t["runner"] in names]


def eval_spec(task_id: str) -> dict:
    """The evaluation specification: the fit specification without the keys that
    only the training reads (the baseline name included)."""
    fit = TASK_BY_ID[task_id]["fit"]
    spec = {k: v for k, v in fit.items() if k not in FIT_ONLY_KEYS}
    if isinstance(spec.get("baseline"), dict):
        spec["baseline"] = {k: v for k, v in spec["baseline"].items() if k != "name"}
    return spec


def findings_title(task_id: str) -> str:
    return task_id.replace("_", " ").replace(".", " ").title()


# COMMAND ----------

# DBTITLE 1,Naming, run identity and registry access


def model_schema_for(ecosystem: str) -> str:
    return MODEL_SCHEMAS[ecosystem]


def model_fqn(table: str, ecosystem: str) -> str:
    return f"{CATALOG}.{model_schema_for(ecosystem)}.{table}"


def model_run_id() -> str:
    return _dt.datetime.now(_dt.UTC).strftime("model-%Y%m%dT%H%M%SZ")


def read_model(table: str, *, ecosystem: str) -> DataFrame:
    """The rows of one ecosystem; both ecosystems share each model table."""
    return spark.table(model_fqn(table, ecosystem)).filter(
        F.col(ECOSYSTEM_COLUMN) == ecosystem
    )


def row_columns(table: str, *, ecosystem: str) -> list[str]:
    """The columns of a model table that a row tuple holds (no ecosystem column)."""
    return [
        c
        for c in spark.table(model_fqn(table, ecosystem)).columns
        if c != ECOSYSTEM_COLUMN
    ]


def ddl_columns(ddl: str) -> list[tuple[str, str]]:
    """(name, type) of each column of a DDL text."""
    return [tuple(part.split(" ", 1)) for part in ddl.split(", ")]


def create_model_table(table: str, ddl: str, ecosystem: str) -> str:
    """Create the shared model table if it is missing; returns its full name."""
    full = model_fqn(table, ecosystem)
    columns = ddl_with_ecosystem(ddl)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {full} ({columns}) USING delta")
    return full


def append_model_rows(df: DataFrame, table: str, *, ecosystem: str) -> None:
    """Append rows to a shared model table, in the column order of the table."""
    full = model_fqn(table, ecosystem)
    cols = spark.table(full).columns
    with_ecosystem(df, ecosystem).select(*cols).write.format("delta").mode(
        "append"
    ).saveAsTable(full)


def add_missing_columns(table: str, *, ecosystem: str, ddl: str) -> list[str]:
    """Add the DDL columns that the table lacks, at its end; old rows hold NULL.
    Returns the names added."""
    full = model_fqn(table, ecosystem)
    have = {name.lower() for name in spark.table(full).columns}
    missing = [(n, t) for n, t in ddl_columns(ddl) if n.lower() not in have]
    if missing:
        columns = ", ".join(f"{name} {kind}" for name, kind in missing)
        spark.sql(f"ALTER TABLE {full} ADD COLUMNS ({columns})")
        print(f"OK  {full}: columns added: {[n for n, _ in missing]}")
    return [name for name, _ in missing]


def replace_model_rows(
    df: DataFrame, table: str, *, ecosystem: str, predicate: str
) -> None:
    """Replace only the registry rows of this ecosystem matching the predicate."""
    full = model_fqn(table, ecosystem)
    cols = spark.table(full).columns
    where = ecosystem_predicate(ecosystem, predicate)
    (
        with_ecosystem(df, ecosystem)
        .select(*cols)
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", where)
        .saveAsTable(full)
    )
    print(f"OK  {full}: rows replaced where {where}")


# COMMAND ----------

# DBTITLE 1,Library availability


def library_folder() -> str:
    """One folder per processor type: compiled packages only import on their own."""
    return (
        f"/Volumes/{CATALOG}/{VOLUME_SCHEMA}/{LIBRARY_VOLUME}"
        f"/site_packages_{_platform.machine()}"
    )


def front_library_folder() -> str:
    """Packages that must win over the environment's copy (a version check reads the
    first copy on the path)."""
    return f"{library_folder()}_front"


def use_model_libraries() -> None:
    """Append the installed-once library folder; packages already present win. The
    small front folder goes first."""
    folder, front = library_folder(), front_library_folder()
    if _os.path.isdir(folder) and folder not in _sys.path:
        _sys.path.append(folder)
    if _os.path.isdir(front) and front not in _sys.path:
        _sys.path.insert(0, front)


use_model_libraries()


def library_available(name: str) -> bool:
    """True only when the library really imports; a half-copied folder does not."""
    try:
        module = _importlib.import_module(LIBRARY_PROBES.get(name, name))
    except Exception:
        return False
    return getattr(module, "__file__", None) is not None


def library_version(name: str) -> str | None:
    """The installed version; a library that serverless installs under another
    distribution name (mlflow as mlflow-skinny) is found under that name."""
    for dist in (LIBRARIES.get(name, name), *LIBRARY_OTHER_DISTRIBUTIONS.get(name, ())):
        try:
            return _importlib_metadata.version(dist)
        except _importlib_metadata.PackageNotFoundError:
            continue
    return None


def missing_libraries(requires) -> list[str]:
    return [lib for lib in requires if not library_available(lib)]


# COMMAND ----------

# DBTITLE 1,Frozen dataset reads -- the test partition is never read


def frozen_version(dataset_id: str, ecosystem: str) -> int:
    row = (
        read_ml("dataset_manifest", ecosystem=ecosystem)
        .filter(F.col("dataset_id") == dataset_id)
        .select("freeze_status", "frozen_delta_version")
        .first()
    )
    if row is None or row["freeze_status"] != "frozen":
        raise RuntimeError(
            f"{ecosystem}.{dataset_id} is not frozen; models read frozen datasets only"
        )
    return int(row["frozen_delta_version"])


class TaskContext:
    """Identity of one modelling task for one notebook run."""

    def __init__(
        self, task_id: str, rid: str, smoke: bool = False, record: bool = True
    ):
        t = TASK_BY_ID[task_id]
        self.task_id = task_id
        self.dataset_id = t["dataset_id"]
        self.ecosystem = t["ecosystem"]
        self.primary_metric = t["primary_metric"]
        self.higher_is_better = t["higher_is_better"]
        self.rid = rid
        self.smoke = bool(smoke)
        self.frozen_version = frozen_version(self.dataset_id, self.ecosystem)
        self.component = f"models/train/{task_id}"
        if record:
            record_task_context(self, "started")

    def fingerprint(self) -> str:
        """Changes when the task file entry of this task changes."""
        return settings_fingerprint(TASK_BY_ID[self.task_id])


def assert_no_test_partition(partitions) -> None:
    bad = [p for p in partitions if p not in ALLOWED_PARTITIONS]
    if bad:
        raise RuntimeError(
            f"model notebooks may read {ALLOWED_PARTITIONS} only, asked for {bad}"
        )


def smoke_subset(df: DataFrame, rows: int = SMOKE_ROWS) -> DataFrame:
    """A few rows of each partition, enough to exercise every code path."""
    parts = [df.filter(F.col("partition") == p).limit(rows) for p in ALLOWED_PARTITIONS]
    return parts[0].unionByName(parts[1])


def _spec_strings(value) -> set:
    """Every string of a specification (names of columns among them)."""
    if isinstance(value, str):
        return {value}
    if isinstance(value, dict):
        return set().union(*(_spec_strings(v) for v in value.values()), set())
    if isinstance(value, (list, tuple)):
        return set().union(*(_spec_strings(v) for v in value), set())
    return set()


def own_columns(ctx: TaskContext, columns, spec=None) -> list[str]:
    """The columns of the dataset of a task inside its family table: the declared
    contract and null-class columns, the partition columns, the columns the
    specification names and their fitted replacements. The other datasets of the
    family add columns that hold no value for this dataset."""
    declared = {
        r["column_name"]
        for table in ("feature_contract", "null_class_registry")
        for r in read_ml(table, ecosystem=ctx.ecosystem)
        .filter(F.col("dataset_id") == ctx.dataset_id)
        .select("column_name")
        .collect()
    }
    wanted = declared | set(PARTITION_COLUMNS) | _spec_strings(spec or {})
    wanted |= {f"{c}{s}" for c in wanted for s in ("_imputed", "_is_missing")}
    return [c for c in columns if c in wanted]


def dataset_at_version(ctx: TaskContext, spec=None) -> DataFrame:
    """The rows of the task's dataset at its frozen Delta version, with the columns
    of that dataset only."""
    full = ml_fqn(dataset_table(ctx.dataset_id), ctx.ecosystem)
    rows_of = getattr(ctx, "rows_dataset_id", ctx.dataset_id)
    df = spark.sql(f"SELECT * FROM {full} VERSION AS OF {ctx.frozen_version}").filter(
        F.col("dataset_id") == rows_of
    )
    return df.select(*own_columns(ctx, df.columns, spec))


def with_tuning_folds(df: DataFrame, key_cols) -> DataFrame:
    """Folds for tuning where the source gives none: a hash of the key columns, for
    the train rows only (a dataset without a validation partition)."""
    fold = F.pmod(
        F.xxhash64(*[F.col(c).cast("string") for c in key_cols]),
        F.lit(TUNE_HASH_FOLDS),
    ).cast("int")
    return df.withColumn(
        "fold_id",
        F.when(F.col("partition") == "train", F.coalesce(F.col("fold_id"), fold)),
    )


def read_frozen(
    ctx: TaskContext, partitions=ALLOWED_PARTITIONS, apply_smoke: bool = True
) -> DataFrame:
    """The dataset at its frozen Delta version, train and validation rows only."""
    assert_no_test_partition(partitions)
    task = TASK_BY_ID[ctx.task_id]
    df = dataset_at_version(ctx, task["fit"]).filter(
        F.col("partition").isin(*partitions)
    )
    if task["fit"].get("tuning_hash"):
        df = with_tuning_folds(df, task["fit"]["tuning_hash"])
    if task.get("read_apply_smoke") is False:
        apply_smoke = False
    return smoke_subset(df) if ctx.smoke and apply_smoke else df


def sample_users(df: DataFrame, user_col: str = "user_id") -> DataFrame:
    """Stable user-level sample; the same users fall in every partition."""
    bucket = user_hash_bucket([user_col], seed=USER_SAMPLE_SEED)
    return df.filter(bucket < USER_SAMPLE_PERCENT)


LOAD_KEYS = ["market_area_code", "load_kind"]
CALENDAR_COLUMNS = [
    "day_of_week",
    "is_weekend",
    "month",
    "day_of_year",
    "iso_week",
    "year",
    "is_holiday",
    "is_bridge_day",
    "local_day_hours",
]
REALISED_LAGS = [2, 7, 14]


def complete_load_features(frame, ctx):
    """The frozen features exist for one market area only. Calendar columns come
    from the calendar table for every area; realised load at least two days back
    comes from the frame itself (earlier partitions included)."""
    keys = ["market_area_code", "local_date"]
    have = [c for c in CALENDAR_COLUMNS if c in frame.columns]
    calendar = read_ml("features_calendar_market_area", ecosystem=ctx.ecosystem)
    out = frame.drop(*have).join(calendar.select(*keys, *have), keys, "left")
    realised = frame.select(
        *LOAD_KEYS, "local_date", F.col("target_value_mwh").alias("_realised")
    )
    for n in REALISED_LAGS:
        shifted = realised.select(
            *LOAD_KEYS,
            F.date_add("local_date", n).alias("local_date"),
            F.col("_realised").alias(f"realised_lag{n}"),
        )
        out = out.join(shifted, [*LOAD_KEYS, "local_date"], "left")
    return out


# COMMAND ----------

# DBTITLE 1,Feature resolution and the pandas conversion


def select_features(roles: dict, columns, *, id_features=(), drop=()) -> list[str]:
    """Contract features, with the fitted replacement where one exists.

    roles maps column -> contract role; `x_imputed` replaces `x` and
    `x_is_missing` is kept alongside it. Keys, time, target and provenance
    columns never enter; `id_features` promotes listed key columns."""
    have = set(columns)
    out: list[str] = []
    for c, r in roles.items():
        if r != "feature" or c not in have or c in drop:
            continue
        out.append(f"{c}_imputed" if f"{c}_imputed" in have else c)
        if f"{c}_is_missing" in have:
            out.append(f"{c}_is_missing")
    out += [c for c in id_features if c in have and c not in drop]
    return list(dict.fromkeys(out))


def resolve_features(
    ctx: TaskContext, columns, *, id_features=(), drop=()
) -> list[str]:
    rows = (
        read_ml("feature_contract", ecosystem=ctx.ecosystem)
        .filter(F.col("dataset_id") == ctx.dataset_id)
        .select("column_name", "role")
        .collect()
    )
    roles = {r["column_name"]: r["role"] for r in rows}
    feats = select_features(roles, columns, id_features=id_features, drop=drop)
    if not feats:
        raise RuntimeError(f"no features resolved for {ctx.dataset_id}")
    return feats


def training_row_cap(n_columns: int) -> int:
    """Rows that fit comfortably in driver memory for this width."""
    cap = TRAIN_ROW_CAP
    if library_available("psutil"):
        import psutil

        avail = psutil.virtual_memory().available
        cap = min(cap, int(0.4 * avail / (8 * max(n_columns, 1) * 3)))
    return max(cap, 10_000)


def to_training_frame(df: DataFrame, *, key_cols: list[str], columns=None):
    """(pandas frame, row fraction). Loads up to the memory-derived cap; above it
    a stable hash sample over key_cols is taken. The fraction is rounded down to a
    multiple of 0.05 so it does not drift with free memory."""
    cols = list(dict.fromkeys(columns or df.columns))
    d = df.select(*cols)
    cap = training_row_cap(len(cols))
    pdf = d.limit(cap + 1).toPandas()
    fraction, total = 1.0, None
    if len(pdf) > cap:
        total = d.count()
        fraction = max(_math.floor(cap / total * 20) / 20, 0.05)
        bucket = F.pmod(
            F.xxhash64(*[F.col(c).cast("string") for c in key_cols]), F.lit(10000)
        )
        pdf = d.filter(bucket < int(fraction * 10000)).toPandas()
    _note_frame(pdf, fraction, cap=cap, total=total)
    return pdf, fraction


class FeatureEncoder:
    """Frame to float matrix: numeric as is, bool to 0/1, dates to days, text to
    train-fitted level codes (unseen level -> NaN). Plain Python, picklable."""

    def __init__(self, features, max_levels: int = 50):
        self.features = list(features)
        self.max_levels = max_levels
        self.kinds: dict = {}
        self.levels: dict = {}
        self.medians = np.zeros(len(self.features))

    @staticmethod
    def _kind(s: pd.Series) -> str:
        if s.dtype == bool:
            return "num"
        if pd.api.types.is_datetime64_any_dtype(s):
            return "date"
        if pd.api.types.is_numeric_dtype(s):
            return "num"
        nn = s.dropna()
        if not len(nn):
            return "num"
        if isinstance(nn.iloc[0], _dt.date):
            return "date"
        num = pd.to_numeric(nn, errors="coerce")
        return "num" if num.notna().mean() >= 0.95 else "cat"

    def _column(self, pdf: pd.DataFrame, c: str) -> np.ndarray:
        s = pdf[c]
        kind = self.kinds[c]
        if kind == "num":
            return pd.to_numeric(s, errors="coerce").to_numpy(dtype="float64")
        if kind == "date":
            t = pd.to_datetime(s, errors="coerce")
            days = t.astype("datetime64[ns]").astype("int64") / 86_400_000_000_000
            return np.where(t.isna(), np.nan, days)
        codes = s.map(self.levels[c])
        return pd.to_numeric(codes, errors="coerce").to_numpy(dtype="float64")

    def fit(self, pdf: pd.DataFrame):
        for c in self.features:
            self.kinds[c] = self._kind(pdf[c])
            if self.kinds[c] == "cat":
                top = pdf[c].value_counts().head(self.max_levels).index
                self.levels[c] = {v: i for i, v in enumerate(top)}
        mat = self.transform(pdf)
        with np.errstate(all="ignore"):
            med = np.nanmedian(mat, axis=0) if len(mat) else np.zeros(mat.shape[1])
        self.medians = np.where(np.isfinite(med), med, 0.0)
        return self

    def transform(self, pdf: pd.DataFrame, fill: bool = False) -> np.ndarray:
        cols = [self._column(pdf, c) for c in self.features]
        mat = np.column_stack(cols) if cols else np.zeros((len(pdf), 0))
        if fill:
            mat = np.where(np.isnan(mat), self.medians, mat)
        return mat


# COMMAND ----------

# DBTITLE 1,Folds inside the training partition


def fold_splits(
    pdf: pd.DataFrame,
    mode: str,
    *,
    date_col: str | None = None,
    gap_days: int = 0,
    max_folds: int = TUNE_MAX_FOLDS,
):
    """[(train mask, validation mask)] from fold_id over training rows.

    rolling: a fold trains on earlier rows only (rows without a fold are the
    earliest data), with a gap before the block; grouped: a fold trains on all
    other folds."""
    if mode == "none" or "fold_id" not in pdf.columns:
        return []
    tr_rows = (pdf["partition"] == "train").to_numpy()
    fold = pd.to_numeric(pdf["fold_id"], errors="coerce").to_numpy()
    ids = sorted(int(f) for f in np.unique(fold[np.isfinite(fold)]))[-max_folds:]
    out = []
    for f in ids:
        va = tr_rows & (fold == f)
        if mode == "rolling":
            tr = tr_rows & (~np.isfinite(fold) | (fold < f))
            if date_col and gap_days:
                d = pd.to_datetime(pdf[date_col], errors="coerce")
                start = d[va].min()
                tr = tr & (d < start - pd.Timedelta(days=gap_days)).to_numpy()
        else:
            tr = tr_rows & np.isfinite(fold) & (fold != f)
        if tr.any() and va.any():
            out.append((tr, va))
    return out


def pick_params(grid, folds, fit_score, lower_is_better: bool = True) -> dict:
    """Best parameter set by mean fold score; the first set when there are no folds."""
    if not folds or len(grid) == 1:
        return grid[0]
    best, best_score = grid[0], None
    for params in grid:
        scores = [fit_score(params, tr, va) for tr, va in folds]
        mean = float(np.nanmean(scores)) if np.isfinite(scores).any() else None
        if mean is None:
            continue
        better = best_score is None or (
            mean < best_score if lower_is_better else mean > best_score
        )
        if better:
            best, best_score = params, mean
    return best


# COMMAND ----------

# DBTITLE 1,Findings markdown (pure text, no Spark)


def _cell(value, width: int = 160) -> str:
    text = "" if value is None else str(value)
    return " ".join(text.replace("|", "/").split())[:width]


def _number(value) -> str:
    return "" if value is None else f"{float(value):.4g}"


_USER_FOLDER = _re.compile(r"(?:/Workspace)?/Users/[^/\s|]+/")
_EMAIL = _re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")


def scrub_private(text: str) -> str:
    """Remove workspace user folders and e-mail addresses from committed text."""
    return _EMAIL.sub("<email>", _USER_FOLDER.sub("/Users/<user>/", text))


def _repo_relative(path):
    """A notebook path from the repository's `databricks/` folder onwards."""
    if not path:
        return path
    i = path.find("/databricks/")
    return path[i + 1 :] if i >= 0 else path


def markdown_table(headers, rows) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "|" + "|".join("---" for _ in headers) + "|",
    ]
    lines += ["| " + " | ".join(_cell(c) for c in row) + " |" for row in rows]
    return "\n".join(lines)


def _context_by_task(context, smoke: bool) -> dict:
    """Latest run's context rows per task for this mode, oldest first."""
    rows = [c for c in context or [] if bool(c["smoke"]) == smoke]
    latest = {}
    for c in sorted(rows, key=lambda c: str(c["recorded_at"])):
        latest[c["task_id"]] = c["run_id"]
    out = {}
    for c in sorted(rows, key=lambda c: str(c["recorded_at"])):
        if latest.get(c["task_id"]) == c["run_id"]:
            out.setdefault(c["task_id"], []).append(c)
    return out


def _data_note_lines(rows) -> list:
    """Bullet lines for what the models saw, from the data_ready context row."""
    ready = [r for r in rows if r["status"] == "data_ready" and r.get("data_notes")]
    if not ready:
        return []
    try:
        notes = _json.loads(ready[-1]["data_notes"])
    except ValueError:
        return []
    lines = []
    for i, n in enumerate(notes, 1):
        if "target" in n:
            for part, st in n["target"].items():
                shown = ", ".join(f"{k} {_number(v)}" for k, v in st.items())
                lines.append(f"- target, {part}: {shown}")
        elif "diagnostics" in n:
            shown = ", ".join(f"{k} {_number(v)}" for k, v in n["diagnostics"].items())
            lines.append(f"- diagnostics: {shown}")
        else:
            cap = (
                f", row cap {n['row_cap']} of {n['rows_before_cap']} rows"
                if "row_cap" in n
                else ""
            )
            lines.append(
                f"- frame {i}: {n.get('rows')} rows x {n.get('columns')} columns, "
                f"sample fraction {n.get('sample_fraction')}{cap}, rows by partition "
                f"{n.get('rows_by_partition')}, highest null rates "
                f"{n.get('highest_null_rates') or 'none'}"
            )
    return lines


TRAIN_PAIRS_LOWER = (
    ("train_mae", "mae"),
    ("train_rmse", "rmse"),
    ("train_pinball_q50", "pinball_q50"),
    ("train_log_loss", "log_loss"),
    ("train_action_mae", "action_mae"),
)
TRAIN_PAIRS_HIGHER = (
    ("train_pr_auc", "pr_auc"),
    ("train_concordance", "concordance"),
    ("train_action_agreement", "action_agreement"),
    ("train_sign_agreement", "sign_agreement"),
    ("train_skill_mae_mean", "skill_mae_mean"),
    ("train_recall_at_10", "recall_at_10"),
    ("train_ndcg_at_10", "ndcg_at_10"),
    ("train_mrr", "mrr"),
    ("train_balanced_accuracy", "balanced_accuracy"),
)


BOUNDED_PRIMARY_METRICS = (
    "pr_auc",
    "concordance",
    "balanced_accuracy",
    "action_agreement",
    "ndcg_at_10",
    "skill_mae_mean",
)
# a primary metric at or above this (bounded, higher is better), or an error of zero,
# is a score a model does not reach on honest inputs
NEAR_PERFECT = 0.995


def is_near_perfect(metric: str, higher_is_better: bool, value) -> bool:
    if value is None:
        return False
    if higher_is_better:
        return metric in BOUNDED_PRIMARY_METRICS and value >= NEAR_PERFECT
    return abs(value) < 1e-9


def _sampled_frames(rows) -> list:
    """Row fractions below 1 among the model frames of the latest data_ready row."""
    ready = [r for r in rows if r["status"] == "data_ready" and r.get("data_notes")]
    if not ready:
        return []
    try:
        notes = _json.loads(ready[-1]["data_notes"])
    except ValueError:
        return []
    return [n["sample_fraction"] for n in notes if n.get("sample_fraction", 1.0) < 1.0]


def findings_flags(tasks, results, context=None) -> list:
    """(task, model, flag, detail) for results a reader should look at first."""
    flags = []
    ctx_rows = context or {}
    for t in tasks:
        tid, primary, higher = t["task_id"], t["primary_metric"], t["higher_is_better"]
        by = {}
        for r in results:
            if r["task_id"] == tid:
                by.setdefault(r["model_name"], []).append(r)
        values = {}
        for m, rs in by.items():
            if rs[0]["status"] != "ok":
                label = "failed" if rs[0]["status"] == "failed" else "skipped"
                flags.append((tid, m, label, rs[0]["detail"]))
                continue
            values[m] = {
                r["metric"]: r["value"]
                for r in rs
                if r["metric"] and r["value"] is not None
            }
        for m, v in values.items():
            is_baseline = by[m][0]["stage"] == "baseline"
            is_candidate = by[m][0]["stage"] == "candidate"
            p = v.get(primary)
            if is_near_perfect(primary, higher, p):
                flags.append((tid, m, "near-perfect score", f"{primary} = {p:.4g}"))
            for tr_key, va_key in TRAIN_PAIRS_LOWER:
                tr, va = v.get(tr_key), v.get(va_key)
                if tr is not None and va is not None and va > 0 and va > 2 * tr:
                    flags.append(
                        (
                            tid,
                            m,
                            "validation error far above training error",
                            f"{va_key} {va:.4g} vs {tr_key} {tr:.4g}",
                        )
                    )
            for tr_key, va_key in TRAIN_PAIRS_HIGHER:
                tr, va = v.get(tr_key), v.get(va_key)
                if tr is not None and va is not None and tr - va > 0.3:
                    flags.append(
                        (
                            tid,
                            m,
                            "validation score far below training score",
                            f"{va_key} {va:.4g} vs {tr_key} {tr:.4g}",
                        )
                    )
            for key in ("skill_mae", "skill_pinball_q50"):
                if key in v and v[key] <= 0:
                    flags.append(
                        (tid, m, "no skill over the baseline", f"{key} = {v[key]:.4g}")
                    )
            for key, val in v.items():
                if is_candidate and key.startswith("skill_mae_vs_") and val <= 0:
                    flags.append(
                        (
                            tid,
                            m,
                            f"no skill over the {key[13:]} diagnostic",
                            f"{key} = {val:.4g}",
                        )
                    )
            rel = v.get("mean_error_relative")
            if rel is not None and abs(rel) > 0.25:
                flags.append(
                    (
                        tid,
                        m,
                        "biased predictions",
                        f"mean error is {rel:+.0%} of the mean absolute target",
                    )
                )
            ratio = v.get("pred_std_ratio")
            if ratio is not None and ratio < 0.1 and not is_baseline:
                flags.append(
                    (
                        tid,
                        m,
                        "near-constant predictions",
                        f"prediction spread is {ratio:.2g} of the target spread",
                    )
                )
            share = v.get("pred_positive_share")
            if share is not None and share in (0.0, 1.0) and not is_baseline:
                flags.append(
                    (
                        tid,
                        m,
                        "a single class is predicted",
                        f"share of positive predictions = {share:g}",
                    )
                )
        estimable = [
            x
            for v in values.values()
            for k, x in v.items()
            if k.startswith("estimable__")
        ]
        if estimable and sum(estimable) / len(values) < 2:
            flags.append(
                (
                    tid,
                    "",
                    "fewer than two classes estimable in the evaluation",
                    "the primary metric rests on a single class",
                )
            )
        if any(v.get("base_rate") in (0.0, 1.0) for v in values.values()):
            flags.append(
                (
                    tid,
                    "",
                    "single-class evaluation target",
                    "validation base rate is 0 or 1",
                )
            )
        scored = {m: v[primary] for m, v in values.items() if primary in v}
        counts = {}
        for val in scored.values():
            counts.setdefault(round(val, 10), []).append(1)
        if any(len(c) >= 3 for c in counts.values()):
            flags.append(
                (
                    tid,
                    "",
                    "identical primary metric across models",
                    f"{primary} repeats on 3 or more models",
                )
            )
        base = [m for m in scored if by[m][0]["stage"] == "baseline"]
        cand = [m for m in scored if by[m][0]["stage"] == "candidate"]
        if base and cand:
            top = max if higher else min
            best_candidate = top(scored[m] for m in cand)
            floor = scored[base[0]]
            if (higher and best_candidate <= floor) or (
                not higher and best_candidate >= floor
            ):
                flags.append((tid, base[0], "no candidate beats the baseline", primary))
        rows = ctx_rows.get(tid)
        if rows and _sampled_frames(rows):
            flags.append(
                (
                    tid,
                    "",
                    "model frame is a sample of the dataset",
                    f"row fraction {_sampled_frames(rows)} (memory cap)",
                )
            )
        if rows and rows[-1]["status"] != "finished":
            flags.append(
                (
                    tid,
                    "",
                    "run did not finish",
                    f"last recorded step: {rows[-1]['status']}",
                )
            )
    return flags


def render_findings(
    ecosystem: str,
    results,
    selection,
    *,
    smoke: bool,
    stamp: str,
    context=None,
    other_mode=None,
    checks=None,
) -> str:
    """One markdown document: a summary table, then per task the candidates with
    status and stored-artifact state, every metric per successful model, and the
    models forwarded for test-partition evaluation.

    results: dicts with task_id, model_name, stage, status, metric, value, n_rows,
    detail, frozen_delta_version, artifact_status, run_id; selection: dicts with
    task_id, model_name, rank, primary_value; context: task_run_context rows;
    other_mode: {task_id: (rows, last write)} for the other run mode; checks:
    quality-log rows of the model notebooks."""
    tasks = [t for t in TASKS if t["ecosystem"] == ecosystem]
    by_task_context = _context_by_task(context, smoke)
    kind = "SMOKE RUN" if smoke else "MODEL"
    out = [
        f"# {ecosystem.upper()} {kind} FINDINGS",
        "",
        (
            "_Auto-generated by `databricks/models/train/02_candidate_gate`. One section "
            f"per task; re-running replaces the file. Generated: {stamp}_"
        ),
        "",
    ]
    summary = []
    for t in tasks:
        rows = [r for r in results if r["task_id"] == t["task_id"]]
        models = {}
        for r in rows:
            models.setdefault(r["model_name"], r)
        counts = {"ok": 0, "failed": 0, "skipped_library_unavailable": 0}
        for r in models.values():
            counts[r["status"]] = counts.get(r["status"], 0) + 1
        base = any(
            r["stage"] == "baseline" and r["status"] == "ok" for r in models.values()
        )
        summary.append(
            (
                t["task_id"],
                "yes" if base else "NO",
                sum(
                    r["stage"] == "candidate" and r["status"] == "ok"
                    for r in models.values()
                ),
                counts["failed"],
                counts["skipped_library_unavailable"],
            )
        )
    out += [
        "## summary",
        "",
        markdown_table(
            ["task", "baseline ran", "candidates ok", "failed", "skipped (library)"],
            summary,
        ),
        "",
    ]
    if context is not None:
        run_rows = []
        for t in tasks:
            rs = by_task_context.get(t["task_id"], [])
            if rs:
                last = rs[-1]
                run_rows.append(
                    (
                        t["task_id"],
                        last["run_id"],
                        " > ".join(r["status"] for r in rs),
                        str(bool(last["smoke"])).lower(),
                        last["job_id"],
                        last["job_run_id"],
                        _repo_relative(last["notebook_path"]),
                        str(rs[0]["recorded_at"])[:19],
                        str(last["recorded_at"])[:19],
                    )
                )
        out += ["## run context", ""]
        if run_rows:
            out += [
                markdown_table(
                    [
                        "task",
                        "run id",
                        "steps",
                        "smoke",
                        "job id",
                        "job run id",
                        "notebook",
                        "first",
                        "last",
                    ],
                    run_rows,
                ),
                "",
            ]
            try:
                versions = _json.loads(
                    next(
                        r["library_versions"]
                        for r in reversed(by_task_context[run_rows[-1][0]])
                        if r["library_versions"]
                    )
                )
                out += [
                    "libraries: " + ", ".join(f"{k} {v}" for k, v in versions.items()),
                    "",
                ]
            except (StopIteration, ValueError):
                pass
        else:
            out += ["_no run context recorded for this mode_", ""]
    flags = findings_flags(tasks, results, by_task_context)
    out += ["## flags", ""]
    if flags:
        out += [markdown_table(["task", "model", "flag", "detail"], flags), ""]
    else:
        out += ["_no automatic flags raised_", ""]
    if checks:
        latest = {}
        for c in sorted(checks, key=lambda c: str(c["recorded_at"])):
            latest[(c["component"], c["metric_name"])] = c
        out += [
            "## check results",
            "",
            markdown_table(
                ["component", "check", "status", "detail", "recorded"],
                [
                    (
                        c["component"],
                        c["metric_name"],
                        c["status"],
                        c["error_detail"],
                        str(c["recorded_at"])[:19],
                    )
                    for c in latest.values()
                ],
            ),
            "",
        ]
    for t in tasks:
        rows = [r for r in results if r["task_id"] == t["task_id"]]
        out += [f"## {t['task_id']}", ""]
        if not rows:
            out += ["_no results recorded_", ""]
            other = (other_mode or {}).get(t["task_id"])
            if other:
                out += [
                    f"_the other run mode holds {other[0]} result row(s) for this task, last written {other[1]}_",
                    "",
                ]
            continue
        out += [
            (
                f"_dataset `{t['dataset_id']}`, frozen version "
                f"{rows[0]['frozen_delta_version']}, primary metric `{t['primary_metric']}` "
                f"({'higher' if t['higher_is_better'] else 'lower'} is better)_"
            ),
            "",
        ]
        notes = _data_note_lines(by_task_context.get(t["task_id"], []))
        if notes:
            out += ["### data seen by the models", "", *notes, ""]
        models = {}
        for r in rows:
            models.setdefault(r["model_name"], []).append(r)
        order = sorted(models, key=lambda m: (models[m][0]["stage"] != "baseline", m))
        cand = []
        for m in order:
            first = models[m][0]
            primary = next(
                (r["value"] for r in models[m] if r["metric"] == t["primary_metric"]),
                None,
            )
            cand.append(
                (
                    m,
                    first["stage"],
                    first["status"],
                    first["artifact_status"],
                    first["n_rows"],
                    _number(primary),
                    first["detail"] if first["status"] != "ok" else "",
                )
            )
        out += [
            "### candidates",
            "",
            markdown_table(
                [
                    "model",
                    "stage",
                    "status",
                    "artifact",
                    "rows",
                    t["primary_metric"],
                    "detail",
                ],
                cand,
            ),
            "",
        ]
        ok = [m for m in order if models[m][0]["status"] == "ok"]
        names = list(
            dict.fromkeys(r["metric"] for m in ok for r in models[m] if r["metric"])
        )
        if ok and names:
            grid = []
            for name in names:
                row = [name]
                for m in ok:
                    v = next(
                        (r["value"] for r in models[m] if r["metric"] == name), None
                    )
                    row.append(_number(v))
                grid.append(row)
            out += [
                "### metrics (validation partition)",
                "",
                markdown_table(["metric", *ok], grid),
                "",
            ]
        chosen = [s for s in selection if s["task_id"] == t["task_id"]]
        if chosen and not smoke:
            out += [
                "### forwarded for test evaluation",
                "",
                markdown_table(
                    ["rank", "model", t["primary_metric"]],
                    [
                        (s["rank"], s["model_name"], _number(s["primary_value"]))
                        for s in sorted(chosen, key=lambda s: s["rank"])
                    ],
                ),
                "",
            ]
    return scrub_private("\n".join(out))


# COMMAND ----------

# DBTITLE 1,Run context: where and how a task ran, and what data it saw
_FRAME_NOTES: list = []


def _note_frame(pdf, fraction, cap=None, total=None) -> None:
    """Remember what the model frame looks like; written by start_task."""
    try:
        by = pdf["partition"].value_counts().to_dict() if "partition" in pdf else {}
        nulls = pdf.isna().mean()
        worst = nulls[nulls > 0].sort_values(ascending=False).head(5)
        note = {
            "rows": len(pdf),
            "columns": int(pdf.shape[1]),
            "sample_fraction": round(float(fraction), 4),
            "rows_by_partition": {str(k): int(v) for k, v in by.items()},
            "highest_null_rates": {c: round(float(v), 4) for c, v in worst.items()},
        }
        if total is not None:
            note["row_cap"], note["rows_before_cap"] = int(cap), int(total)
        _FRAME_NOTES.append(note)
    except Exception as exc:
        print(f"WARN frame notes not recorded: {type(exc).__name__}: {exc}")


def note_diagnostics(values: dict) -> None:
    """Named diagnostic figures of the task's data, shown with the data notes."""
    try:
        _FRAME_NOTES.append(
            {"diagnostics": {k: round(float(v), 4) for k, v in values.items()}}
        )
    except Exception as exc:
        print(f"WARN diagnostics not recorded: {type(exc).__name__}: {exc}")


def note_target(y, tr_m, va_m, kind: str) -> None:
    """Target distribution per partition: positive rate, or mean and spread."""
    try:
        out = {}
        for label, mask in (("train", tr_m), ("validation", va_m)):
            v = np.asarray(y, dtype="float64")[mask]
            v = v[np.isfinite(v)]
            if not len(v):
                continue
            if kind == "classification":
                out[label] = {
                    "rows": len(v),
                    "positive_rate": round(float(v.mean()), 4),
                }
            else:
                out[label] = {
                    "rows": len(v),
                    "mean": float(v.mean()),
                    "std": float(v.std()),
                    "min": float(v.min()),
                    "max": float(v.max()),
                }
        _FRAME_NOTES.append({"target": out})
    except Exception as exc:
        print(f"WARN target notes not recorded: {type(exc).__name__}: {exc}")


def _take_notes() -> list:
    notes = list(_FRAME_NOTES)
    _FRAME_NOTES.clear()
    return notes


def _notebook_identity():
    """(notebook path, job id, job run id); None where the platform hides them."""
    try:
        info = _json.loads(
            dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
        )
    except Exception:
        return None, None, None
    tags = info.get("tags") or {}
    extra = info.get("extraContext") or {}
    path = extra.get("notebook_path") or tags.get("notebookPath")
    job = tags.get("jobId") or extra.get("jobId")
    run = tags.get("runId") or info.get("currentRunId") or extra.get("runId")
    return path, job, str(run) if run is not None else None


def train_side(fn) -> dict:
    """Run a train-side metric computation; a failure is printed and never fails
    the candidate."""
    try:
        return fn()
    except Exception as exc:
        print(f"WARN train-side metrics not computed: {type(exc).__name__}: {exc}")
        return {}


def record_task_context(
    ctx, status: str, detail=None, data_notes=None, table: str | None = None
) -> None:
    """Append one run-context row (started, data_ready, finished). Never raises."""
    try:
        table = table or getattr(ctx, "context_table", "task_run_context")
        ddl = MODEL_DDL.get(table, RUN_CONTEXT_COLUMNS)
        create_model_table(table, ddl, ctx.ecosystem)
        path, job, run = _notebook_identity()
        versions = {k: library_version(k) or "missing" for k in LIBRARIES}
        row = (
            ctx.task_id,
            ctx.dataset_id,
            ctx.smoke,
            ctx.rid,
            status,
            path,
            job,
            run,
            int(ctx.frozen_version),
            _json.dumps(data_notes, default=str)[:30000] if data_notes else None,
            _json.dumps(versions),
            detail[:900] if detail else None,
            _dt.datetime.now(_dt.UTC),
        )
        append_model_rows(
            spark.createDataFrame([row], ddl), table, ecosystem=ctx.ecosystem
        )
    except Exception as exc:
        print(f"WARN run context not recorded: {type(exc).__name__}: {exc}")


# COMMAND ----------

# DBTITLE 1,Skip a task that already finished with the same data and settings


def settings_fingerprint(*parts) -> str:
    """Twelve characters that change when any of the settings a result depends on
    change."""
    text = _json.dumps(parts, sort_keys=True, default=str)
    return _hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]


def finished_detail(baseline_ok: bool, fingerprint: str) -> str:
    """The detail of a finished context row: the baseline state and the settings."""
    return f"baseline_ok={baseline_ok}; fingerprint={fingerprint}"


def run_is_current(rows, frozen_version: int, fingerprint: str) -> bool:
    """True when the latest context row of a task (one ecosystem, one run kind) is a
    finished run with a working baseline, at this frozen version and with these
    settings. A later started or data_ready row means a run that did not finish."""
    if not rows:
        return False
    last = max(rows, key=lambda r: r["recorded_at"])
    detail = last["detail"] or ""
    return (
        last["status"] == "finished"
        and int(last["frozen_delta_version"]) == int(frozen_version)
        and "baseline_ok=True" in detail
        and f"fingerprint={fingerprint}" in detail
    )


def context_rows(
    ecosystem: str, task_id: str, smoke: bool, table: str = "task_run_context"
) -> list:
    """The context rows of one task and one run kind; empty before the first run."""
    if not spark.catalog.tableExists(model_fqn(table, ecosystem)):
        return []
    frame = read_model(table, ecosystem=ecosystem).filter(
        (F.col("task_id") == task_id) & (F.col("smoke") == smoke)
    )
    cols = ["status", "frozen_delta_version", "detail", "recorded_at"]
    return [r.asDict() for r in frame.select(*cols).collect()]


def has_ok_baseline(table: str, ecosystem: str, task_id: str, smoke: bool) -> bool:
    """True when the result table still holds a working baseline row of the task."""
    if not spark.catalog.tableExists(model_fqn(table, ecosystem)):
        return False
    rows = read_model(table, ecosystem=ecosystem).filter(
        (F.col("task_id") == task_id)
        & (F.col("smoke") == smoke)
        & (F.col("stage") == "baseline")
        & (F.col("status") == "ok")
    )
    return rows.limit(1).count() > 0


def train_is_current(task_id: str, rid: str, smoke: bool) -> bool:
    """True when the training of the task finished earlier with the same frozen data
    and the same task file entry, and its baseline row is still stored."""
    ctx = TaskContext(task_id, rid, smoke, record=False)
    return run_is_current(
        context_rows(ctx.ecosystem, task_id, smoke),
        ctx.frozen_version,
        ctx.fingerprint(),
    ) and has_ok_baseline("candidate_results", ctx.ecosystem, task_id, smoke)


def skip_notes(skipped, what: str) -> dict:
    """A findings note per ecosystem for the tasks a run skipped. Each entry starts
    with a task id; a label may follow after a space."""
    eco_of = {t["task_id"]: t["ecosystem"] for t in (*TASKS, *CHECK_TASKS)}
    by_eco: dict = {}
    for entry in skipped:
        by_eco.setdefault(eco_of[entry.split(" ")[0]], []).append(entry)
    tail = "Their rows stay in the tables under the run that wrote them."
    return {
        eco: [f"Skipped, {what}: {', '.join(names)}. {tail}"]
        for eco, names in by_eco.items()
    }


# COMMAND ----------

# DBTITLE 1,Result recording


def _finite(v):
    return None if v is None or not np.isfinite(float(v)) else float(v)


def record_result(
    ctx: TaskContext,
    name: str,
    family: str,
    stage: str,
    status: str,
    metrics: dict,
    n_rows,
    detail: str,
    mlflow_run_id,
    artifact_status: str,
    params: dict,
) -> None:
    now = _dt.datetime.now(_dt.UTC)
    base = (
        ctx.dataset_id,
        ctx.task_id,
        name,
        family,
        stage,
        "validation",
    )
    tail = (
        None if n_rows is None else int(n_rows),
        status,
        detail[:900] if detail else None,
        int(ctx.frozen_version),
        mlflow_run_id,
        artifact_status,
        str(params)[:900],
        ctx.smoke,
        ctx.rid,
        now,
    )
    items = list(metrics.items()) if status == "ok" else [(None, None)]
    rows = [(*base, m, None if v is None else _finite(v), *tail) for m, v in items] or [
        (*base, None, None, *tail)
    ]
    table = getattr(ctx, "results_table", "candidate_results")
    append_model_rows(
        spark.createDataFrame(rows, RESULT_COLUMNS), table, ecosystem=ctx.ecosystem
    )


def start_task(ctx: TaskContext) -> None:
    """Drop this task's earlier rows of the same kind (smoke or full)."""
    table = getattr(ctx, "results_table", "candidate_results")
    where = ecosystem_predicate(
        ctx.ecosystem,
        f"task_id = '{ctx.task_id}' AND smoke = {str(ctx.smoke).lower()}",
    )
    spark.sql(f"DELETE FROM {model_fqn(table, ctx.ecosystem)} WHERE {where}")
    print(
        f"task {ctx.task_id} (smoke={ctx.smoke}) at frozen version {ctx.frozen_version}"
    )
    record_task_context(ctx, "data_ready", data_notes=_take_notes())


def log_candidate_to_mlflow(ctx, name, family, params, metrics, bundle):
    """(run id, artifact status). Never raises: a logging failure is a status."""
    if not library_available("mlflow"):
        return None, "skipped_mlflow_unavailable"
    try:
        import mlflow

        # serverless blocks the notebook-context call MLflow uses for run tags
        _logging.getLogger("mlflow.tracking.context.registry").setLevel(_logging.ERROR)
        mlflow.set_experiment(f"{MLFLOW_EXPERIMENT_PREFIX}_{ctx.dataset_id}")
        with mlflow.start_run(run_name=f"{ctx.task_id}:{name}") as run:
            mlflow.set_tags(
                {
                    "task_id": ctx.task_id,
                    "dataset_id": ctx.dataset_id,
                    "model_name": name,
                    "family": family,
                    "frozen_delta_version": str(ctx.frozen_version),
                    "smoke": str(ctx.smoke),
                    "run_id": ctx.rid,
                }
            )
            mlflow.log_params({str(k)[:200]: str(v)[:450] for k, v in params.items()})
            mlflow.log_metrics(
                {k: float(v) for k, v in metrics.items() if _finite(v) is not None}
            )
            if bundle is None:
                return run.info.run_id, "none"
            if not library_available("cloudpickle"):
                return run.info.run_id, "skipped_cloudpickle_unavailable"
            import cloudpickle

            with _tempfile.TemporaryDirectory() as tmp:
                path = _os.path.join(tmp, "candidate.pkl")
                with open(path, "wb") as fh:
                    cloudpickle.dump(bundle, fh)
                mlflow.log_artifact(path, artifact_path="candidate")
            return run.info.run_id, "ok"
    except Exception as exc:
        return None, f"failed: {type(exc).__name__}: {str(exc)[:200]}"


# COMMAND ----------

# DBTITLE 1,Candidate guard -- time limit, stack print, stage times
CANDIDATE_LIMIT_SECONDS = 1200
STACK_PRINT_SECONDS = 300
STACK_PRINT_ROWS = 12


def candidate_limit(family: str) -> int:
    """Seconds one candidate may run."""
    return CANDIDATE_LIMIT_SECONDS


def _print_main_stack(label: str, waited: float, main_id: int) -> None:
    frame = _sys._current_frames().get(main_id)
    rows = _traceback.format_stack(frame)[-STACK_PRINT_ROWS:] if frame else []
    print(f"STACK {label} after {waited:.0f} s:")
    for row in rows:
        for line in row.rstrip().splitlines():
            print(f"  | {line}")


@_contextlib.contextmanager
def candidate_guard(
    label: str, limit_seconds: int, stack_seconds: float = STACK_PRINT_SECONDS
):
    """Guard for one candidate. Every `stack_seconds` it prints the stack of the
    calling thread, so a wait shows its line. After `limit_seconds` it raises
    TimeoutError in the calling thread (SIGALRM, main thread only). A wait inside a
    native call is interrupted only when that call returns to Python; the stack print
    still shows where it waits."""
    stop = _threading.Event()
    started = _time.time()
    main_id = _threading.get_ident()

    def watch() -> None:
        while not stop.wait(stack_seconds):
            _print_main_stack(label, _time.time() - started, main_id)

    def on_alarm(signum, frame):
        raise TimeoutError(f"{label}: timeout after {int(limit_seconds)} s")

    watcher = None
    if stack_seconds and stack_seconds > 0:
        watcher = _threading.Thread(target=watch, daemon=True)
        watcher.start()
    previous = None
    armed = (
        int(limit_seconds) > 0
        and _threading.current_thread() is _threading.main_thread()
    )
    # Without SIGALRM (Windows) a timer thread raises SIGINT in the main thread
    # instead; this also wakes a sleep. The guard turns the interrupt into the timeout.
    use_alarm = armed and hasattr(_signal, "SIGALRM")
    fired = _threading.Event()
    timer = None

    def on_timer() -> None:
        fired.set()
        _signal.raise_signal(_signal.SIGINT)

    if use_alarm:
        previous = _signal.signal(_signal.SIGALRM, on_alarm)
        _signal.alarm(int(limit_seconds))
    elif armed:
        timer = _threading.Timer(int(limit_seconds), on_timer)
        timer.daemon = True
        timer.start()
    try:
        yield
    except KeyboardInterrupt:
        if fired.is_set():
            message = f"{label}: timeout after {int(limit_seconds)} s"
            raise TimeoutError(message) from None
        raise
    finally:
        if use_alarm:
            _signal.alarm(0)
            _signal.signal(_signal.SIGALRM, previous)
        if timer is not None:
            timer.cancel()
            timer.join(timeout=1)
        stop.set()
        if watcher is not None:
            watcher.join(timeout=1)


@_contextlib.contextmanager
def timed_stage(label: str):
    """Prints the seconds of one stage inside a candidate, for example the checkpoint
    load, the fit or one prediction batch."""
    started = _time.time()
    print(f"  .. {label}")
    try:
        yield
    except BaseException:
        print(f"  .. {label}: failed after {_time.time() - started:.1f} s")
        raise
    print(f"  .. {label}: done in {_time.time() - started:.1f} s")


# COMMAND ----------

# DBTITLE 1,In-context sampling -- rows scored by a context model, stored bundles
IN_CONTEXT_SAMPLE_ROWS = 100_000


def context_positions(n_rows: int, max_rows: int, seed: int) -> np.ndarray:
    """Positions of the rows kept: all of them, or a seeded sample in the original
    order. Used for the context rows and for the scored validation sample."""
    if n_rows <= max_rows:
        return np.arange(n_rows)
    rng = np.random.default_rng(seed)
    return np.sort(rng.choice(n_rows, size=max_rows, replace=False))


def sample_params(total: int, used: int) -> dict:
    """Recorded with a candidate that was scored on a sample of the rows."""
    return {
        "validation_rows": int(used),
        "validation_rows_total": int(total),
        "validation_fraction": round(used / total, 4) if total else 1.0,
    }


def load_bundle(model: dict):
    """The stored fitted model of one candidate; the artifacts are our own runs."""
    if not model["mlflow_run_id"]:
        raise RuntimeError("no stored artifact for this model")
    import cloudpickle
    import mlflow

    path = mlflow.artifacts.download_artifacts(
        artifact_uri=f"runs:/{model['mlflow_run_id']}/candidate/candidate.pkl"
    )
    with open(path, "rb") as fh:
        return cloudpickle.load(fh)


# COMMAND ----------

# DBTITLE 1,Isolated candidate runner


def run_candidate(
    ctx: TaskContext,
    name: str,
    family: str,
    fit_fn,
    *,
    requires=(),
    params=None,
    stage: str = "candidate",
    limit_seconds=None,
):
    """Run one candidate in isolation and record exactly what happened.

    fit_fn() returns (bundle, metrics dict, n_rows). A missing library, a timeout
    (`limit_seconds`, default by family) or an exception becomes a recorded row and
    the notebook goes on; a failing baseline is re-raised because it means the
    plumbing is wrong."""
    if params is None:
        params = {}
    missing = missing_libraries(requires)
    if missing:
        record_result(
            ctx,
            name,
            family,
            stage,
            "skipped_library_unavailable",
            {},
            None,
            f"missing: {', '.join(missing)}",
            None,
            "none",
            params,
        )
        print(f"SKIP {name}: missing {missing}")
        return None
    limit = candidate_limit(family) if limit_seconds is None else limit_seconds
    print(f"START {name} [{family}] task={ctx.task_id} limit={limit} s")
    started = _time.time()
    try:
        with candidate_guard(f"{name} {ctx.task_id}", limit):
            bundle, metrics, n_rows = fit_fn()
    except Exception as exc:
        detail = f"{type(exc).__name__}: {str(exc)[:600]}"
        record_result(
            ctx, name, family, stage, "failed", {}, None, detail, None, "none", params
        )
        print(f"FAIL {name}: {detail}")
        if stage == "baseline":
            raise
        _gc.collect()
        return None
    run_id, artifact = log_candidate_to_mlflow(
        ctx, name, family, params, metrics, bundle
    )
    record_result(
        ctx,
        name,
        family,
        stage,
        "ok",
        metrics,
        n_rows,
        f"seconds={_time.time() - started:.1f}",
        run_id,
        artifact,
        params,
    )
    shown = {k: round(v, 4) for k, v in metrics.items() if _finite(v) is not None}
    print(f"OK   {name} [{artifact}]: {shown}")
    del bundle
    _gc.collect()
    return metrics


def finish_task(ctx: TaskContext) -> None:
    """Hard-fail unless the baseline ran; print the per-status summary."""
    mine = read_model("candidate_results", ecosystem=ctx.ecosystem).filter(
        (F.col("run_id") == ctx.rid) & (F.col("task_id") == ctx.task_id)
    )
    mine.filter(
        F.col("metric").isNull() | (F.col("metric") == ctx.primary_metric)
    ).select("model_name", "stage", "status", "metric", "value").show(
        50, truncate=False
    )
    baseline_ok = (
        mine.filter((F.col("stage") == "baseline") & (F.col("status") == "ok"))
        .limit(1)
        .count()
        > 0
    )
    record_task_context(
        ctx, "finished", detail=finished_detail(baseline_ok, ctx.fingerprint())
    )
    check(
        ctx.component,
        "models",
        "baseline_recorded",
        baseline_ok,
        detail=f"task={ctx.task_id}",
        rid=ctx.rid,
    )


# COMMAND ----------

# DBTITLE 1,Guard -- one check of a gate notebook


def guard(component, rid, name, offenders, detail) -> None:
    """A gate check that passes when `offenders` is empty; the detail shows the first
    ten and the metric value is their count."""
    shown = list(offenders)[:10]
    check(
        component,
        MODEL_STAGE,
        name,
        not offenders,
        detail=f"{detail}: {shown}",
        metric_value=float(len(offenders)),
        rid=rid,
    )


# COMMAND ----------

# DBTITLE 1,Findings section of one notebook (src/findings/model_findings)

NOTEBOOK_FINDINGS_SUBDIR = "src/findings/model_findings"
FINDINGS_ROW_LIMIT = 30
FINDINGS_COLUMNS = {
    "candidate_results": [
        "task_id",
        "model_name",
        "family",
        "stage",
        "partition",
        "metric",
        "value",
        "n_rows",
        "status",
        "detail",
    ],
    "candidate_selection": [
        "task_id",
        "model_name",
        "rank",
        "primary_metric",
        "primary_value",
        "is_baseline",
    ],
    "task_run_context": [
        "task_id",
        "status",
        "smoke",
        "frozen_delta_version",
        "detail",
        "recorded_at",
    ],
    "evaluation_run_context": [
        "task_id",
        "status",
        "smoke",
        "frozen_delta_version",
        "detail",
        "recorded_at",
    ],
    "evaluation_results": [
        "task_id",
        "model_name",
        "stage",
        "partition",
        "metric",
        "value",
        "validation_value",
        "n_rows",
        "status",
        "detail",
    ],
    "evaluation_flags": [
        "task_id",
        "model_name",
        "stage",
        "flag",
        "detail",
        "value",
        "threshold",
    ],
    "evaluation_spec": ["dataset_id", "spec_key", "spec_value"],
    "approval_spec": ["rule_id", "rule_class", "parameter", "parameter_value"],
    "model_approval": [
        "task_id",
        "model_name",
        "role",
        "decision",
        "restriction",
        "overridden",
        "primary_metric",
        "validation_value",
        "evaluated_value",
    ],
    "model_registry": [
        "task_id",
        "model_name",
        "role",
        "version",
        "lifecycle_status",
        "decision",
        "validation_value",
        "reason",
    ],
    "registry_check": [
        "task_id",
        "model_name",
        "role",
        "registry_version",
        "processor_type",
        "reload_status",
        "rescore_status",
        "gap_relative",
        "status",
        "detail",
    ],
    "monitoring_spec": ["parameter", "parameter_value"],
    "reference_profile": [
        "task_id",
        "model_name",
        "subject",
        "value_kind",
        "n_rows",
        "null_rate",
    ],
    "model_predictions": [
        "task_id",
        "model_name",
        "window_id",
        "row_key",
        "prediction",
    ],
    "monitoring_results": [
        "task_id",
        "model_name",
        "window_id",
        "check_kind",
        "subject",
        "value",
        "level",
    ],
    "monitoring_flags": [
        "task_id",
        "model_name",
        "window_id",
        "check_kind",
        "subject",
        "level",
        "value",
        "threshold",
    ],
    "monitoring_triggers": [
        "task_id",
        "model_name",
        "trigger_kind",
        "source",
        "condition",
        "value",
        "threshold",
    ],
}
# tables without a run id are summarised by these columns
FINDINGS_GROUPED: dict = {}


def _findings_dir() -> str:
    path = _os.path.abspath(_os.getcwd())
    for _ in range(12):
        if _os.path.isdir(_os.path.join(path, "src", "schemas")) and _os.path.isdir(
            _os.path.join(path, "databricks")
        ):
            return _os.path.join(path, NOTEBOOK_FINDINGS_SUBDIR)
        if _os.path.dirname(path) == path:
            break
        path = _os.path.dirname(path)
    notebook = (
        dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath()
    ).get()
    cut = notebook.rfind("/databricks/")
    for root in (notebook[:cut], "/Workspace" + notebook[:cut]):
        if cut > 0 and _os.path.isdir(_os.path.join(root, "src", "schemas")):
            return _os.path.join(root, NOTEBOOK_FINDINGS_SUBDIR)
    raise RuntimeError("repo root not found -- run from the Databricks Git folder")


def findings_file_name(stem: str, smoke: bool) -> str:
    return f"{stem}{'_smoke' if smoke else ''}.md"


def findings_file_text(heading: str, title: str, body: str, smoke: bool, now) -> str:
    """One notebook, one file: no other notebook writes it, so parallel tasks of a
    job cannot overwrite each other."""
    return scrub_private(
        "\n".join(
            [
                f"# {heading}{' (SMOKE RUN)' if smoke else ''}",
                "",
                (
                    "_Auto-generated by the model notebooks (`databricks/models/`). "
                    "One file per notebook; a re-run replaces it. "
                    f"Generated: {now.strftime('%Y-%m-%dT%H:%MZ')}_"
                ),
                "",
                f"## {title}",
                "",
                body,
            ]
        )
        + "\n"
    )


def write_findings_section(stem, heading, title, body, smoke, now) -> None:
    """Write the findings of one notebook run to notebooks/<stem>[_smoke].md."""
    folder = _os.path.join(_findings_dir(), "notebooks")
    _os.makedirs(folder, exist_ok=True)
    path = _os.path.join(folder, findings_file_name(stem, smoke))
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(findings_file_text(heading, title, body, smoke, now))
    print(f"findings export -> {path}")


SECTION_MARK = "<!-- section: "


def notebook_findings_group(name: str) -> tuple[str, bool, str]:
    """(file prefix, smoke run, section key) of a per-notebook findings file name."""
    stem = name[: -len(".md")]
    smoke = stem.endswith("_smoke")
    key = stem[: -len("_smoke")] if smoke else stem
    return key.split("__", 1)[0], smoke, key


def section_of_findings_file(text: str) -> str:
    """The part of a per-notebook file from its first second-level heading on."""
    return text[text.index("\n## ") + 1 :].rstrip("\n")


def read_merged_sections(text: str) -> dict[str, str]:
    sections = {}
    for part in text.split(SECTION_MARK)[1:]:
        key, _, body = part.partition(" -->\n")
        sections[key] = body.strip("\n")
    return sections


def merged_findings_text(prefix: str, sections: dict[str, str], smoke, now) -> str:
    title = prefix.upper().replace("_", " ")
    lines = [
        f"# {title} NOTEBOOK FINDINGS{' (SMOKE RUN)' if smoke else ''}",
        "",
        (
            "_Merged by the findings gate from the files of the model notebooks; "
            "a section is replaced when its notebook runs again. "
            f"Generated: {now.strftime('%Y-%m-%dT%H:%MZ')}_"
        ),
    ]
    for key in sorted(sections):
        lines += ["", f"{SECTION_MARK}{key} -->", sections[key]]
    return "\n".join(lines) + "\n"


def merge_notebook_findings() -> list[str]:
    """Merge notebooks/*.md into <prefix>_notebooks[_smoke].md, then delete them.
    Only a gate task may call it: it is the single writer of the merged files."""
    root = _findings_dir()
    folder = _os.path.join(root, "notebooks")
    names = (
        sorted(n for n in _os.listdir(folder) if n.endswith(".md"))
        if _os.path.isdir(folder)
        else []
    )
    groups: dict[tuple[str, bool], dict[str, str]] = {}
    for name in names:
        prefix, smoke, key = notebook_findings_group(name)
        groups.setdefault((prefix, smoke), {})[key] = name
    now = _dt.datetime.now(tz=_dt.UTC)
    written = []
    for (prefix, smoke), files in sorted(groups.items()):
        target = f"{prefix}_notebooks{'_smoke' if smoke else ''}.md"
        path = _os.path.join(root, target)
        sections = {}
        if _os.path.isfile(path):
            with open(path, encoding="utf-8") as fh:
                sections = read_merged_sections(fh.read())
        for key, name in files.items():
            with open(_os.path.join(folder, name), encoding="utf-8") as fh:
                sections[key] = section_of_findings_file(fh.read())
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(merged_findings_text(prefix, sections, smoke, now))
        written.append(target)
        print(f"OK  {target}: {len(files)} section(s) merged")
    for name in names:
        _os.remove(_os.path.join(folder, name))
    print(f"{len(names)} notebook file(s) merged and removed")
    return written


def notebook_stems(stages) -> set:
    """File names (without .py) of the notebooks in the stage folders of this
    checkout; a check row of a notebook that is gone is no current check."""
    root = _os.path.dirname(_os.path.dirname(_os.path.dirname(_findings_dir())))
    names = set()
    for stage in stages:
        folder = _os.path.join(root, "databricks", "models", stage)
        if _os.path.isdir(folder):
            names |= {n[:-3] for n in _os.listdir(folder) if n.endswith(".py")}
    return names


def current_check_rows(rows, stems) -> list:
    """The latest row of every (component, check) whose component is `models/<stage>/
    <name>` with a name that is a notebook of `stems`, a task or a check-only task.
    Rows of earlier layouts and earlier runs stay in the audit table, not in the
    findings."""
    names = set(stems) | set(TASK_BY_ID) | {e["task_id"] for e in CHECK_ONLY.values()}
    latest: dict = {}
    for r in rows:
        parts = r["component"].split("/")
        if len(parts) != 3 or parts[2] not in names:
            continue
        key = (r["component"], r["metric_name"])
        if key not in latest or r["recorded_at"] > latest[key]["recorded_at"]:
            latest[key] = r
    return sorted(latest.values(), key=lambda r: r["recorded_at"])


def table_findings(table, ecosystem, run_id, task_ids=None, smoke=None) -> str:
    """Markdown for the rows one run wrote to a model table."""
    full = model_fqn(table, ecosystem)
    if not spark.catalog.tableExists(full):
        return f"_table `{table}` does not exist_"
    frame = read_model(table, ecosystem=ecosystem)
    columns = [c for c in frame.columns if c != ECOSYSTEM_COLUMN]
    if table in FINDINGS_GROUPED:
        keys = FINDINGS_GROUPED[table]
        rows = frame.groupBy(*keys).count().orderBy(*keys).collect()
        return markdown_table([*keys, "rows"], [tuple(r) for r in rows])
    if "run_id" in columns:
        frame = frame.filter(F.col("run_id") == run_id)
    if task_ids and "task_id" in columns:
        frame = frame.filter(F.col("task_id").isin(*task_ids))
    if smoke is not None and "smoke" in columns:
        frame = frame.filter(F.col("smoke") == smoke)
    total = frame.count()
    if total == 0:
        return "_no rows for this run_"
    shown = [c for c in FINDINGS_COLUMNS.get(table, columns) if c in columns]
    rows = frame.select(*shown).limit(FINDINGS_ROW_LIMIT).collect()
    note = f"{total} row(s)"
    if total > FINDINGS_ROW_LIMIT:
        note += f", first {FINDINGS_ROW_LIMIT} shown"
    table_text = markdown_table(
        shown,
        [tuple(round(v, 4) if isinstance(v, float) else v for v in r) for r in rows],
    )
    return f"{note}\n\n{table_text}"


def write_model_findings(
    ecosystems,
    notebook_key,
    title,
    tables,
    run_id,
    task_ids=None,
    smoke=None,
    notes=None,
) -> None:
    """Write what one notebook run recorded in `tables`, one file per ecosystem
    (notebooks/<ecosystem>__<notebook key>.md; smoke runs: ..._smoke.md).
    `notes` maps an ecosystem to extra lines for the section."""
    now = _dt.datetime.now(tz=_dt.UTC)
    for eco in ecosystems:
        lines = (notes or {}).get(eco, [])
        body = [*lines, ""] if lines else []
        for table in tables:
            body += [
                f"### {table}",
                "",
                table_findings(table, eco, run_id, task_ids, smoke),
                "",
            ]
        write_findings_section(
            f"{eco}__{notebook_key}",
            f"{eco.upper()} MODEL NOTEBOOK FINDINGS",
            title,
            "\n".join(body),
            bool(smoke),
            now,
        )


# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np

# COMMAND ----------

# DBTITLE 1,Regression


def _pair(y, p):
    y = np.asarray(y, dtype="float64")
    p = np.asarray(p, dtype="float64")
    ok = np.isfinite(y) & np.isfinite(p)
    return y[ok], p[ok]


def mae(y, p) -> float:
    y, p = _pair(y, p)
    return float(np.mean(np.abs(y - p))) if len(y) else float("nan")


def rmse(y, p) -> float:
    y, p = _pair(y, p)
    return float(np.sqrt(np.mean((y - p) ** 2))) if len(y) else float("nan")


def skill(model_error: float, baseline_error: float) -> float:
    """1 - model / baseline; positive means better than the baseline."""
    if not np.isfinite(baseline_error) or baseline_error <= 0:
        return float("nan")
    return float(1.0 - model_error / baseline_error)


def pinball(y, q_pred, tau: float) -> float:
    y, q = _pair(y, q_pred)
    if not len(y):
        return float("nan")
    d = y - q
    return float(np.mean(np.maximum(tau * d, (tau - 1.0) * d)))


def bias_metrics(y, pred) -> dict:
    """Signed mean error, that error relative to the mean absolute target, and the
    spread of the predictions relative to the spread of the target."""
    y_, p = _pair(y, pred)
    if not len(y_):
        return {}
    out = {"mean_error": float(np.mean(p - y_))}
    scale = float(np.mean(np.abs(y_)))
    if scale > 0:
        out["mean_error_relative"] = out["mean_error"] / scale
    spread = float(np.std(y_))
    if spread > 0:
        out["pred_std_ratio"] = float(np.std(p)) / spread
    return out


def regression_metrics(y, pred, baseline_pred=None) -> dict:
    out = {"mae": mae(y, pred), "rmse": rmse(y, pred)}
    out.update(bias_metrics(y, pred))
    if baseline_pred is not None:
        out["skill_mae"] = skill(out["mae"], mae(y, baseline_pred))
        out["skill_rmse"] = skill(out["rmse"], rmse(y, baseline_pred))
    return out


def quantile_metrics(y, preds: dict, baseline_preds: dict | None = None) -> dict:
    """preds: {tau: predictions}; pinball per quantile plus coverage of the
    central interval when tau 0.1 and 0.9 are both present."""
    out = {}
    for tau, q in preds.items():
        out[f"pinball_q{round(tau * 100):02d}"] = pinball(y, q, tau)
        if baseline_preds is not None and tau in baseline_preds:
            out[f"skill_pinball_q{round(tau * 100):02d}"] = skill(
                out[f"pinball_q{round(tau * 100):02d}"],
                pinball(y, baseline_preds[tau], tau),
            )
    if 0.1 in preds and 0.9 in preds:
        y_, lo = _pair(y, preds[0.1])
        _, hi = _pair(y, preds[0.9])
        if len(y_) and len(lo) == len(hi) == len(y_):
            out["interval_coverage_80"] = float(np.mean((y_ >= lo) & (y_ <= hi)))
    return out


# COMMAND ----------

# DBTITLE 1,Classification


def average_precision(y_true, score) -> float:
    """Area under the precision-recall curve as average precision."""
    y, s = _pair(y_true, score)
    n_pos = float(np.sum(y > 0))
    if not len(y) or n_pos == 0:
        return float("nan")
    if np.ptp(s) == 0:
        return n_pos / len(y)
    order = np.argsort(-s, kind="mergesort")
    hit = (y[order] > 0).astype("float64")
    tp = np.cumsum(hit)
    precision = tp / np.arange(1, len(hit) + 1)
    return float(np.sum(precision * hit) / n_pos)


def log_loss(y_true, p, eps: float = 1e-15) -> float:
    y, q = _pair(y_true, p)
    if not len(y):
        return float("nan")
    q = np.clip(q, eps, 1 - eps)
    return float(-np.mean(y * np.log(q) + (1 - y) * np.log(1 - q)))


def calibration_error(y_true, p, bins: int = 10) -> float:
    """Expected calibration error over equal-width probability bins."""
    y, q = _pair(y_true, p)
    if not len(y):
        return float("nan")
    idx = np.minimum((np.clip(q, 0, 1) * bins).astype(int), bins - 1)
    err = 0.0
    for b in range(bins):
        m = idx == b
        if m.any():
            err += m.sum() / len(y) * abs(float(y[m].mean()) - float(q[m].mean()))
    return float(err)


def classification_metrics(y_true, p) -> dict:
    y, q = _pair(y_true, p)
    return {
        "pr_auc": average_precision(y, q),
        "log_loss": log_loss(y, q),
        "calibration_error": calibration_error(y, q),
        "base_rate": float(np.mean(y)) if len(y) else float("nan"),
        "pred_mean": float(np.mean(q)) if len(q) else float("nan"),
        "pred_positive_share": float(np.mean(q >= 0.5)) if len(q) else float("nan"),
    }


# COMMAND ----------

# DBTITLE 1,Survival


def concordance_index(duration, event, risk) -> float:
    """Harrell's C: among comparable pairs, the share where the higher risk
    failed first; risk ties count one half."""
    d = np.asarray(duration, dtype="float64")
    e = np.asarray(event, dtype="float64") > 0
    r = np.asarray(risk, dtype="float64")
    ok = np.isfinite(d) & np.isfinite(r)
    d, e, r = d[ok], e[ok], r[ok]
    conc = 0.0
    comparable = 0.0
    for i in np.flatnonzero(e):
        later = d > d[i]
        n = float(later.sum())
        if n == 0:
            continue
        comparable += n
        conc += float(np.sum(r[i] > r[later])) + 0.5 * float(np.sum(r[i] == r[later]))
    return float(conc / comparable) if comparable else float("nan")


def kaplan_meier(duration, event):
    """(sorted unique event times, survival after each) for right-censored data."""
    d = np.asarray(duration, dtype="float64")
    e = np.asarray(event, dtype="float64") > 0
    ok = np.isfinite(d)
    d, e = d[ok], e[ok]
    times = np.unique(d[e])
    surv = np.ones(len(times))
    s = 1.0
    for k, t in enumerate(times):
        at_risk = float(np.sum(d >= t))
        deaths = float(np.sum((d == t) & e))
        if at_risk > 0:
            s *= 1.0 - deaths / at_risk
        surv[k] = s
    return times, surv


def km_at(times, surv, t) -> np.ndarray:
    """Step-function value of a Kaplan-Meier curve at t (1 before the first event)."""
    t = np.atleast_1d(np.asarray(t, dtype="float64"))
    if len(times) == 0:
        return np.ones(len(t))
    idx = np.searchsorted(times, t, side="right") - 1
    return np.where(idx >= 0, np.asarray(surv)[np.maximum(idx, 0)], 1.0)


def brier_at(duration, event, surv_at_t, horizon: float) -> float:
    """Inverse-probability-of-censoring weighted Brier score at one horizon."""
    d = np.asarray(duration, dtype="float64")
    e = np.asarray(event, dtype="float64") > 0
    s = np.asarray(surv_at_t, dtype="float64")
    ok = np.isfinite(d) & np.isfinite(s)
    d, e, s = d[ok], e[ok], s[ok]
    if not len(d):
        return float("nan")
    ct, cs = kaplan_meier(d, ~e)
    g_d = np.maximum(km_at(ct, cs, d), 1e-6)
    g_h = float(np.maximum(km_at(ct, cs, horizon)[0], 1e-6))
    score = np.zeros(len(d))
    failed = (d <= horizon) & e
    alive = d > horizon
    score[failed] = (s[failed] ** 2) / g_d[failed]
    score[alive] = ((1.0 - s[alive]) ** 2) / g_h
    return float(np.mean(score))


def survival_calibration_gap(duration, event, surv_at_t, horizon: float) -> float:
    """Absolute gap between the mean predicted survival and the Kaplan-Meier
    observed survival at the horizon."""
    times, surv = kaplan_meier(duration, event)
    observed = float(km_at(times, surv, horizon)[0])
    s = np.asarray(surv_at_t, dtype="float64")
    s = s[np.isfinite(s)]
    return float(abs(np.mean(s) - observed)) if len(s) else float("nan")


# COMMAND ----------

# DBTITLE 1,Ranking


def ranking_metrics(ranks, ks=(5, 10, 20)) -> dict:
    """ranks: 1-based rank of the true item per case; inf when it is not ranked."""
    r = np.asarray(ranks, dtype="float64")
    if not len(r):
        return {}
    out = {}
    for k in ks:
        hit = r <= k
        out[f"recall_at_{k}"] = float(np.mean(hit))
        out[f"ndcg_at_{k}"] = float(np.mean(np.where(hit, 1.0 / np.log2(r + 1.0), 0.0)))
    out["mrr"] = float(np.mean(np.where(np.isfinite(r), 1.0 / r, 0.0)))
    return out


# COMMAND ----------

# DBTITLE 1,Reconstruction, anomaly and agreement


def masked_mae(y_true, y_pred, mask) -> float:
    m = np.asarray(mask, dtype=bool)
    return mae(np.asarray(y_true)[m], np.asarray(y_pred)[m])


def detection_metrics(injected, flagged, flagged_uninjected_rate: float) -> dict:
    """Precision and recall of flags against injected anomalies, plus the flag
    rate on the same series before injection."""
    inj = np.asarray(injected, dtype=bool)
    fl = np.asarray(flagged, dtype=bool)
    tp = float(np.sum(inj & fl))
    precision = tp / float(fl.sum()) if fl.sum() else float("nan")
    recall = tp / float(inj.sum()) if inj.sum() else float("nan")
    return {
        "precision": float(precision),
        "recall": float(recall),
        "flag_rate": float(flagged_uninjected_rate),
    }


def agreement_metrics(logged, predicted, reward) -> dict:
    """Top-1 action agreement and the logged reward where the policy agrees
    and disagrees with the logged action."""
    lg = np.asarray(logged)
    pr = np.asarray(predicted)
    rw = np.asarray(reward, dtype="float64")
    agree = lg == pr
    out = {"action_agreement": float(np.mean(agree)) if len(agree) else float("nan")}
    out["reward_when_agree"] = (
        float(np.mean(rw[agree])) if agree.any() else float("nan")
    )
    out["reward_when_disagree"] = (
        float(np.mean(rw[~agree])) if (~agree).any() else float("nan")
    )
    total = float(np.sum(rw))
    out["reward_weighted_agreement"] = (
        float(np.sum(rw[agree]) / total) if total != 0 else float("nan")
    )
    return out


# COMMAND ----------

# DBTITLE 1,Multi-class metrics


def per_class_precision_recall(y_true, y_pred, labels, min_rows: int = 10) -> dict:
    """{label: (precision, recall, n_true, estimable)}; a class with fewer than
    min_rows true rows is marked not estimable and its scores are nan."""
    yt = np.asarray(y_true)
    yp = np.asarray(y_pred)
    out = {}
    for lab in labels:
        n_true = int(np.sum(yt == lab))
        tp = float(np.sum((yt == lab) & (yp == lab)))
        n_pred = float(np.sum(yp == lab))
        ok = n_true >= min_rows
        precision = tp / n_pred if n_pred else float("nan")
        recall = tp / n_true if n_true else float("nan")
        out[lab] = (
            precision if ok else float("nan"),
            recall if ok else float("nan"),
            n_true,
            ok,
        )
    return out


def multiclass_metrics(y_true, y_pred, labels) -> dict:
    """Accuracy, balanced accuracy and, per class, precision, recall and the true
    row count; a class below the minimum rows is marked not estimable."""
    per = per_class_precision_recall(y_true, y_pred, labels, MIN_CLASS_ROWS)
    yt, yp = np.asarray(y_true), np.asarray(y_pred)
    out = {"accuracy": float(np.mean(yt == yp))}
    recalls = []
    for lab, (p, r, n, ok) in per.items():
        out[f"precision__{lab}"] = p
        out[f"recall__{lab}"] = r
        out[f"n_true__{lab}"] = float(n)
        out[f"estimable__{lab}"] = 1.0 if ok else 0.0
        if n:
            recalls.append(float(np.sum((yt == lab) & (yp == lab))) / n)
    out["estimable_classes"] = float(sum(v[3] for v in per.values()))
    out["balanced_accuracy"] = float(np.mean(recalls)) if recalls else float("nan")
    return out
