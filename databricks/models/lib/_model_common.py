# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MODEL SHARED LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** plumbing shared by every model notebook, pulled in with
# MAGIC `%run ../lib/_model_common`. Definitions only; the one side effect is adding the
# MAGIC model library folder to the import path.
# MAGIC
# MAGIC Covers the task registry, frozen-dataset reads (test partition never read),
# MAGIC feature resolution, the feature encoder, the isolated candidate runner, result
# MAGIC recording and MLflow logging. No model logic lives here.

# COMMAND ----------

# DBTITLE 1,ML shared library (read_ml, ml_fqn, check, user_hash_bucket, ...)
# MAGIC %run ../../ml/_ml_common

# COMMAND ----------

# DBTITLE 1,Imports
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
import sys as _sys
import tempfile as _tempfile
import time as _time

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration constants
MODEL_STAGE = "models"
MODEL_SCHEMAS = {"energy": "energy_ml_models", "commerce": "commerce_ml_models"}
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
WEAK_SPLIT_PERCENT = (70, 15, 15)
MATCHING_SPLIT_PERCENT = (60, 20, 20)
MIN_TIER_ROWS = 10
MIN_SERIES_ENTITIES = 10
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
    "task_id string, dataset_id string, smoke boolean, smoke_widget string, "
    "run_id string, status string, notebook_path string, job_id string, "
    "job_run_id string, frozen_delta_version bigint, data_notes string, "
    "library_versions string, detail string, recorded_at timestamp"
)
MODEL_DDL = {
    "task_registry": (
        "task_id string, dataset_id string, ecosystem string, paradigm string, "
        "task_type string, target string, baseline string, primary_metric string, "
        "higher_is_better boolean, notebook string, registered_at timestamp"
    ),
    "evaluation_split_manifest": (
        "dataset_id string, frozen_delta_version bigint, grain_key string, "
        "partition string, fold_id int, group_key string, stratum string, "
        "rule_id string, split_version string"
    ),
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
        "registered_at timestamp"
    ),
    "registry_check": (
        "task_id string, model_name string, role string, registry_version int, "
        "processor_type string, python_version string, library_versions string, "
        "reload_status string, reload_detail string, rescore_status string, "
        "recorded_value double, rescored_value double, gap_relative double, "
        "tolerance double, status string, detail string, run_id string, "
        "checked_at timestamp"
    ),
}
# imported instead of the bare name so a partial install counts as missing
LIBRARY_PROBES = {"torch": "torch.nn", "sksurv": "sksurv.ensemble"}
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


def _task(
    task_id,
    dataset_id,
    ecosystem,
    paradigm,
    task_type,
    target,
    baseline,
    primary,
    higher,
    notebook,
):
    return {
        "task_id": task_id,
        "dataset_id": dataset_id,
        "ecosystem": ecosystem,
        "paradigm": paradigm,
        "task_type": task_type,
        "target": target,
        "baseline": baseline,
        "primary_metric": primary,
        "higher_is_better": higher,
        "notebook": notebook,
    }


TASKS = [
    _task(
        "price_daily.price",
        "price_daily",
        "energy",
        "supervised",
        "regression_price",
        "target_price_eur_per_mwh",
        "seasonal_naive",
        "mae",
        False,
        "energy/01_price_daily.py",
    ),
    _task(
        "price_quarter_hour.price",
        "price_quarter_hour",
        "energy",
        "supervised",
        "regression_price",
        "target_price_eur_per_mwh",
        "persistence",
        "mae",
        False,
        "energy/02_price_quarter_hour.py",
    ),
    _task(
        "price_quarter_hour.negative_price",
        "price_quarter_hour",
        "energy",
        "supervised",
        "classification",
        "target_is_negative_price",
        "base_rate",
        "pr_auc",
        True,
        "energy/02_price_quarter_hour.py",
    ),
    _task(
        "load.load",
        "load",
        "energy",
        "supervised",
        "regression",
        "target_value_mwh",
        "seasonal_mean",
        "mae",
        False,
        "energy/03_load.py",
    ),
    _task(
        "bias.bias",
        "bias",
        "energy",
        "supervised",
        "regression",
        "target_bias_mwh",
        "zero_correction",
        "mae",
        False,
        "energy/04_bias.py",
    ),
    _task(
        "zone_generation.generation",
        "zone_generation",
        "energy",
        "supervised",
        "regression",
        "target_generation_mwh",
        "capacity_factor",
        "mae",
        False,
        "energy/05_zone_generation.py",
    ),
    _task(
        "honda_forecast.increment",
        "honda_forecast",
        "energy",
        "supervised",
        "regression",
        "target_increment",
        "persistence",
        "mae",
        False,
        "energy/06_honda_forecast.py",
    ),
    _task(
        "honda_anomaly.anomaly",
        "honda_anomaly",
        "energy",
        "unsupervised",
        "anomaly",
        "injected_anomaly",
        "seasonal_zscore",
        "pr_auc",
        True,
        "energy/07_honda_anomaly.py",
    ),
    _task(
        "ccpp.output",
        "ccpp",
        "energy",
        "supervised",
        "regression",
        "target_net_output_mw",
        "train_mean",
        "mae",
        False,
        "energy/08_ccpp.py",
    ),
    _task(
        "capacity_additions.additions",
        "capacity_additions",
        "energy",
        "supervised",
        "regression_count",
        "target_capacity_added_mw",
        "seasonal_mean",
        "mae",
        False,
        "energy/09_capacity_additions.py",
    ),
    _task(
        "redispatch.any_event",
        "redispatch",
        "energy",
        "supervised",
        "classification",
        "target_any_event",
        "base_rate",
        "pr_auc",
        True,
        "energy/10_redispatch.py",
    ),
    _task(
        "redispatch.event_energy",
        "redispatch",
        "energy",
        "supervised",
        "regression",
        "target_log_event_energy_mwh",
        "zone_mean",
        "mae",
        False,
        "energy/10_redispatch.py",
    ),
    _task(
        "survival.unit_lifetime",
        "survival",
        "energy",
        "survival",
        "survival",
        "target_duration_years",
        "kaplan_meier",
        "concordance",
        True,
        "energy/11_survival.py",
    ),
    _task(
        "redispatch_matching.tier",
        "redispatch_matching",
        "energy",
        "matching",
        "multiclass",
        "target_match_tier",
        "majority_tier",
        "balanced_accuracy",
        True,
        "energy/12_redispatch_matching.py",
    ),
    _task(
        "weak_supervision.labels",
        "weak_supervision",
        "energy",
        "weak_supervision",
        "label_model",
        "lf_label",
        "majority_vote",
        "coverage",
        True,
        "energy/13_weak_supervision.py",
    ),
    _task(
        "weather_imputation.reconstruction",
        "weather_imputation",
        "energy",
        "self_supervised",
        "reconstruction",
        "masked_value",
        "carry_forward",
        "skill_mae_mean",
        True,
        "energy/14_weather_imputation.py",
    ),
    _task(
        "self_supervised_other.reconstruction",
        "self_supervised_other",
        "energy",
        "self_supervised",
        "reconstruction",
        "masked_or_shifted_value",
        "seasonal_carry",
        "skill_mae_mean",
        True,
        "energy/15_self_supervised_other.py",
    ),
    _task(
        "rl_pumped_storage.policy",
        "rl_pumped_storage",
        "energy",
        "offline_rl",
        "policy",
        "action_net_mwh",
        "rule_policy",
        "reward_timing",
        True,
        "energy/16_rl_pumped_storage.py",
    ),
    _task(
        "rl_redispatch.policy",
        "rl_redispatch",
        "energy",
        "offline_rl",
        "policy",
        "action_direction",
        "majority_action",
        "action_agreement",
        True,
        "energy/17_rl_redispatch.py",
    ),
    _task(
        "session_purchase_ga4.purchase",
        "session_purchase_ga4",
        "commerce",
        "supervised",
        "classification",
        "target_purchase_after_prefix",
        "base_rate",
        "pr_auc",
        True,
        "commerce/01_session_purchase_ga4.py",
    ),
    _task(
        "session_purchase_rees46.purchase",
        "session_purchase_rees46",
        "commerce",
        "supervised",
        "classification",
        "target_purchase_after_prefix",
        "base_rate",
        "pr_auc",
        True,
        "commerce/02_session_purchase_rees46.py",
    ),
    _task(
        "lapse_ga4.return",
        "lapse_ga4",
        "commerce",
        "supervised",
        "classification",
        "target_returned",
        "base_rate",
        "pr_auc",
        True,
        "commerce/03_lapse_ga4.py",
    ),
    _task(
        "lapse_rees46.return",
        "lapse_rees46",
        "commerce",
        "supervised",
        "classification",
        "target_returned",
        "base_rate",
        "pr_auc",
        True,
        "commerce/04_lapse_rees46.py",
    ),
    _task(
        "next_item_ga4.next_item",
        "next_item_ga4",
        "commerce",
        "ranking",
        "ranking",
        "target_next_product_id",
        "popularity",
        "ndcg_at_10",
        True,
        "commerce/05_next_item_ga4.py",
    ),
    _task(
        "next_item_rees46.next_item",
        "next_item_rees46",
        "commerce",
        "ranking",
        "ranking",
        "target_next_product_id",
        "popularity",
        "ndcg_at_10",
        True,
        "commerce/06_next_item_rees46.py",
    ),
    _task(
        "rl_session_sequences.policy",
        "rl_session_sequences",
        "commerce",
        "offline_rl",
        "policy",
        "target_action_event_type",
        "most_frequent_event",
        "action_agreement",
        True,
        "commerce/07_rl_session_sequences.py",
    ),
]
TASK_BY_ID = {t["task_id"]: t for t in TASKS}

# COMMAND ----------

# DBTITLE 1,Naming, run identity and registry access


def model_schema_for(ecosystem: str) -> str:
    return MODEL_SCHEMAS[ecosystem]


def model_fqn(table: str, ecosystem: str) -> str:
    return f"{CATALOG}.{model_schema_for(ecosystem)}.{table}"


def model_run_id() -> str:
    return _dt.datetime.now(_dt.UTC).strftime("model-%Y%m%dT%H%M%SZ")


def read_model(table: str, *, ecosystem: str) -> DataFrame:
    return spark.table(model_fqn(table, ecosystem))


def replace_model_rows(
    df: DataFrame, table: str, *, ecosystem: str, predicate: str
) -> None:
    """Replace only the registry rows matching the predicate."""
    full = model_fqn(table, ecosystem)
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

# DBTITLE 1,Library availability


def library_folder() -> str:
    """One folder per processor type: compiled packages only import on their own."""
    return (
        f"/Volumes/{CATALOG}/{MODEL_SCHEMAS['energy']}/{LIBRARY_VOLUME}"
        f"/site_packages_{_platform.machine()}"
    )


def use_model_libraries() -> None:
    """Append the installed-once library folder; packages already present win."""
    folder = library_folder()
    if _os.path.isdir(folder) and folder not in _sys.path:
        _sys.path.append(folder)


use_model_libraries()


def library_available(name: str) -> bool:
    """True only when the library really imports; a half-copied folder does not."""
    try:
        module = _importlib.import_module(LIBRARY_PROBES.get(name, name))
    except Exception:
        return False
    return getattr(module, "__file__", None) is not None


def library_version(name: str) -> str | None:
    try:
        return _importlib_metadata.version(LIBRARIES.get(name, name))
    except _importlib_metadata.PackageNotFoundError:
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
        self.component = f"models/{t['notebook'].removesuffix('.py')}"
        if record:
            record_task_context(self, "started")


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


def read_frozen(
    ctx: TaskContext, partitions=ALLOWED_PARTITIONS, apply_smoke: bool = True
) -> DataFrame:
    """The dataset at its frozen Delta version, train and validation rows only."""
    assert_no_test_partition(partitions)
    full = ml_fqn(f"dataset_{ctx.dataset_id}", ctx.ecosystem)
    df = spark.sql(f"SELECT * FROM {full} VERSION AS OF {ctx.frozen_version}")
    df = df.filter(F.col("partition").isin(*partitions))
    return smoke_subset(df) if ctx.smoke and apply_smoke else df


def attach_evaluation_partition(
    df: DataFrame, ctx: TaskContext, key_cols: list[str]
) -> DataFrame:
    """Replace partition, fold and group by the evaluation split manifest."""
    m = (
        read_model("evaluation_split_manifest", ecosystem=ctx.ecosystem)
        .filter(F.col("dataset_id") == ctx.dataset_id)
        .select("grain_key", "partition", "fold_id", "group_key")
    )
    base = df.drop("partition", "fold_id", "group_key").withColumn(
        "_grain_key", grain_key(*key_cols)
    )
    out = base.join(m, base["_grain_key"] == m["grain_key"], "inner").drop(
        "grain_key", "_grain_key"
    )
    out = out.filter(F.col("partition").isin(*ALLOWED_PARTITIONS))
    return smoke_subset(out) if ctx.smoke else out


def sample_users(df: DataFrame, user_col: str = "user_id") -> DataFrame:
    """Stable user-level sample; the same users fall in every partition."""
    bucket = user_hash_bucket([user_col], seed=USER_SAMPLE_SEED)
    return df.filter(bucket < USER_SAMPLE_PERCENT)


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

# DBTITLE 1,Group-stratified split assignment and text normalisation


def assign_group_partitions(groups, fractions, seed: str = "ecrmap-model-split-s1"):
    """groups: [(group_key, stratum)] -> {group_key: partition}.

    Inside each stratum the groups are ordered by a seeded hash and cut by the
    fractions, so every group lands in exactly one partition. A stratum with
    fewer than three groups stays entirely in train."""
    _, va_f, te_f = (f / 100.0 for f in fractions)
    by_stratum: dict = {}
    for g, s in groups:
        by_stratum.setdefault(s, []).append(g)
    out: dict = {}
    for members in by_stratum.values():
        members = sorted(
            set(members),
            key=lambda g: _hashlib.md5(
                f"{seed}|{g}".encode(), usedforsecurity=False
            ).hexdigest(),
        )
        n = len(members)
        if n < 3:
            n_va = n_te = 0
        else:
            n_va = max(1, round(n * va_f))
            n_te = max(1, round(n * te_f))
            if n - n_va - n_te < 1:
                n_va, n_te = 1, 1
        n_tr = n - n_va - n_te
        for i, g in enumerate(members):
            out[g] = (
                "train" if i < n_tr else ("validation" if i < n_tr + n_va else "test")
            )
    return out


def normalise_asset_text(text) -> str | None:
    """Collapse whitespace, trim and upper-case -- the same normalisation the
    redispatch name match applies."""
    if text is None:
        return None
    return " ".join(str(text).split()).upper()


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
            if p is not None and (
                (higher and primary in BOUNDED_PRIMARY_METRICS and p >= 0.995)
                or (not higher and abs(p) < 1e-9)
            ):
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
            "_Auto-generated by `databricks/models/gate/03_export_findings`. One section "
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
                        last["smoke_widget"],
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
                        "smoke widget",
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


def _smoke_widget():
    try:
        return dbutils.widgets.get("smoke")
    except Exception:
        return None


def train_side(fn) -> dict:
    """Run a train-side metric computation; a failure is printed and never fails
    the candidate."""
    try:
        return fn()
    except Exception as exc:
        print(f"WARN train-side metrics not computed: {type(exc).__name__}: {exc}")
        return {}


def record_task_context(
    ctx, status: str, detail=None, data_notes=None, table: str = "task_run_context"
) -> None:
    """Append one run-context row (started, data_ready, finished). Never raises."""
    try:
        full = model_fqn(table, ctx.ecosystem)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {full} ({MODEL_DDL[table]}) USING delta")
        path, job, run = _notebook_identity()
        versions = {k: library_version(k) or "missing" for k in LIBRARIES}
        row = (
            ctx.task_id,
            ctx.dataset_id,
            ctx.smoke,
            _smoke_widget(),
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
        spark.createDataFrame([row], MODEL_DDL[table]).write.format("delta").mode(
            "append"
        ).saveAsTable(full)
    except Exception as exc:
        print(f"WARN run context not recorded: {type(exc).__name__}: {exc}")


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
    spark.createDataFrame(rows, RESULT_COLUMNS).write.format("delta").mode(
        "append"
    ).saveAsTable(model_fqn("candidate_results", ctx.ecosystem))


def start_task(ctx: TaskContext) -> None:
    """Drop this task's earlier rows of the same kind (smoke or full)."""
    spark.sql(
        f"DELETE FROM {model_fqn('candidate_results', ctx.ecosystem)} "
        f"WHERE task_id = '{ctx.task_id}' AND smoke = {str(ctx.smoke).lower()}"
    )
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


def run_candidate(
    ctx: TaskContext,
    name: str,
    family: str,
    fit_fn,
    *,
    requires=(),
    params=None,
    stage: str = "candidate",
):
    """Run one candidate in isolation and record exactly what happened.

    fit_fn() returns (bundle, metrics dict, n_rows). A missing library or an
    exception becomes a recorded row and the notebook goes on; a failing
    baseline is re-raised because it means the plumbing is wrong."""
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
    started = _time.time()
    try:
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
    record_task_context(ctx, "finished", detail=f"baseline_ok={baseline_ok}")
    check(
        ctx.component,
        "models",
        "baseline_recorded",
        baseline_ok,
        detail=f"task={ctx.task_id}",
        rid=ctx.rid,
    )
