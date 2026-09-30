# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # NULL HANDLING AUDIT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** audit null handling of every fitted dataset: classes recorded, fits on the
# MAGIC training partition, causal fills within the final gap limits, indicators
# MAGIC present and no column entirely null.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECOSYSTEMS = ("energy", "commerce")
SOURCE = "ml"
FIT_STATES = ("fitted", "frozen")
DERIVED_SUFFIXES = ("_is_missing", "_filled", "_gap_hours", "_raw", "_imputed")
PASSTHROUGH = {
    "dataset_id",
    "dataset_version",
    "ecosystem",
    "_ml_loaded_at",
    "_ml_run_id",
    "partition",
    "fold_id",
    "group_key",
}
MIN_LABELLED_ROWS = 30

GATE = "02_null_handling_audit"
TWO_SIDED_ALLOWED = {"weather_imputation", "honda_anomaly"}

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Helper -- collect one audit result
results = []  # (ecosystem, dataset_id, check_name, status, detail)


def audit(eco, dataset_id, check_name, ok, detail=""):
    results.append((eco, dataset_id, check_name, "PASS" if ok else "FAIL", detail))
    record_check(
        f"ml/gate/{GATE}/{dataset_id}",
        SOURCE,
        check_name,
        ok,
        detail=detail,
        rid=rid,
    )


def fitted_datasets(eco):
    return (
        read_ml("dataset_manifest", ecosystem=eco)
        .filter(F.col("status").isin(*FIT_STATES))
        .collect()
    )


def base_column(name):
    for s in DERIVED_SUFFIXES:
        if name.endswith(s):
            return name[: -len(s)]
    return name


# COMMAND ----------

# DBTITLE 1,Check -- every feature has a recorded null class
for eco in ECOSYSTEMS:
    for d in fitted_datasets(eco):
        ds = d["dataset_id"]
        feats = {
            r["column_name"]
            for r in read_ml("feature_contract", ecosystem=eco)
            .filter((F.col("dataset_id") == ds) & F.col("role").isin("feature", "flag"))
            .collect()
        }
        classed = {
            r["column_name"]
            for r in read_ml("null_class_registry", ecosystem=eco)
            .filter(F.col("dataset_id") == ds)
            .collect()
        }
        missing = sorted(feats - classed)
        audit(
            eco,
            ds,
            "null_class_recorded_for_every_feature",
            not missing,
            f"missing={missing[:15]}",
        )

# COMMAND ----------

# DBTITLE 1,Check -- no feature column is entirely null
for eco in ECOSYSTEMS:
    for d in fitted_datasets(eco):
        ds = d["dataset_id"]
        df = read_ml(f"dataset_{ds}", ecosystem=eco)
        cols = [c for c in df.columns if c not in PASSTHROUGH]
        rates = null_rates(df, cols)
        full = [c for c, r in rates.items() if r >= 1.0]
        audit(eco, ds, "no_entirely_null_column", not full, f"columns={full}")

# COMMAND ----------

# DBTITLE 1,Check -- imputed columns keep a missing indicator
for eco in ECOSYSTEMS:
    for d in fitted_datasets(eco):
        ds = d["dataset_id"]
        cols = set(read_ml(f"dataset_{ds}", ecosystem=eco).columns)
        imputed = {
            r["column_name"]
            for r in read_ml("imputer_parameters", ecosystem=eco)
            .filter((F.col("dataset_id") == ds) & (F.col("fold_id") == -1))
            .collect()
        }
        lacking = sorted(
            c for c in imputed if c in cols and f"{c}_is_missing" not in cols
        )
        audit(
            eco, ds, "imputed_columns_have_indicator", not lacking, f"columns={lacking}"
        )

# COMMAND ----------

# DBTITLE 1,Check -- causal fills stay within the final gap limits
_limits = load_gap_limits("energy")
for d in fitted_datasets("energy"):
    ds = d["dataset_id"]
    df = read_ml(f"dataset_{ds}", ecosystem="energy")
    bad = []
    for c in df.columns:
        if not c.endswith("_gap_hours"):
            continue
        base = c[: -len("_gap_hours")]
        variable = next((k for k in _limits if base.endswith(k) or k in base), None)
        if variable is None:
            continue
        over = (
            df.filter(
                F.col(f"{base}_filled") & (F.col(c) > F.lit(float(_limits[variable])))
            )
            .limit(1)
            .count()
        )
        if over:
            bad.append(f"{base}>{_limits[variable]}h")
    audit("energy", ds, "fills_within_gap_limits", not bad, f"violations={bad}")

# COMMAND ----------

# DBTITLE 1,Check -- fill methods are causal for forecasting targets
for eco in ECOSYSTEMS:
    for d in fitted_datasets(eco):
        ds = d["dataset_id"]
        two_sided = (
            read_ml("null_class_registry", ecosystem=eco)
            .filter(
                (F.col("dataset_id") == ds)
                & (F.col("method") == "two_sided_interpolation")
            )
            .count()
        )
        audit(
            eco,
            ds,
            "fills_are_causal",
            two_sided == 0 or ds in TWO_SIDED_ALLOWED,
            f"two_sided_columns={two_sided}",
        )

# COMMAND ----------

# DBTITLE 1,Note -- ablation with and without imputation
print(
    "INFO ablation (model with and without imputation) needs a trained model; "
    "it is produced in modelling and attached to the freeze record, not run here"
)

# COMMAND ----------

# DBTITLE 1,Write the gate results and fail on any FAIL
_rows = [(d, GATE, c, s, det, rid, now_utc()) for (_e, d, c, s, det) in results]
for eco in ECOSYSTEMS:
    mine = [r for r in _rows if any(x[0] == eco and x[1] == r[0] for x in results)]
    if not mine:
        continue
    df = spark.createDataFrame(mine, REGISTRY_DDL["gate_results"])
    ids = ", ".join(f"'{r[0]}'" for r in mine)
    replace_rows(
        df,
        "gate_results",
        ecosystem=eco,
        predicate=f"gate = '{GATE}' AND dataset_id IN ({ids})",
    )
_blocks = [
    (
        "results",
        "\n".join(
            [
                "| ecosystem | dataset | check | status | detail |",
                "|---|---|---|---|---|",
            ]
            + [f"| {e} | {d} | {c} | {s} | {det} |" for (e, d, c, s, det) in results]
        ),
    )
]
write_ml_findings("gate", GATE, GATE, _blocks)
_failed = [r for r in results if r[3] == "FAIL"]
if _failed:
    raise RuntimeError(
        f"{GATE}: {len(_failed)} check(s) failed: "
        + "; ".join(f"{r[1]}.{r[2]}" for r in _failed[:20])
    )
print(f"{GATE}: {len(results)} check(s) passed")
