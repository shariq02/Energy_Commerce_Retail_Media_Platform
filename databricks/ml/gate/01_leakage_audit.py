# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # LEAKAGE AUDIT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** audit every fitted dataset for leakage: declared clocks and lags, label
# MAGIC inputs, disjoint partitions, embargo, group disjointness, train-only fits
# MAGIC and label coverage per partition.

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

GATE = "01_leakage_audit"
FIT_NOTEBOOK_DIRS = ("energy/fit", "commerce/fit")

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

# DBTITLE 1,Check -- feature contract clocks and lags
for eco in ECOSYSTEMS:
    for d in fitted_datasets(eco):
        ds = d["dataset_id"]
        contract = (
            read_ml("feature_contract", ecosystem=eco)
            .filter(F.col("dataset_id") == ds)
            .collect()
        )
        by_col = {r["column_name"]: r for r in contract}
        cols = read_ml(f"dataset_{ds}", ecosystem=eco).columns
        uncovered = [
            c for c in cols if c not in PASSTHROUGH and base_column(c) not in by_col
        ]
        audit(
            eco,
            ds,
            "columns_have_a_declared_role",
            not uncovered,
            f"undeclared={uncovered[:15]}",
        )
        bad_load = [r["column_name"] for r in contract if (r["clock"] or "") == "load"]
        audit(eco, ds, "no_load_clock_feature", not bad_load, f"columns={bad_load}")
        bad_lag = [
            r["column_name"]
            for r in contract
            if r["role"] == "feature"
            and r["clock"] == "observation_realised"
            and (r["lag_days"] or 0) < MIN_REALISED_LAG_DAYS
        ]
        audit(eco, ds, "realised_inputs_lagged", not bad_lag, f"columns={bad_lag}")
        leaked = [
            r["column_name"]
            for r in contract
            if r["role"] == "label_input" and r["column_name"] in cols
        ]
        audit(eco, ds, "no_label_input_in_dataset", not leaked, f"columns={leaked}")
        forbidden = [c for c in cols if c in CLASS_E_COLUMNS]
        audit(eco, ds, "no_class_e_column", not forbidden, f"columns={forbidden}")

# COMMAND ----------

# DBTITLE 1,Check -- partitions are disjoint and complete
for eco in ECOSYSTEMS:
    for d in fitted_datasets(eco):
        ds = d["dataset_id"]
        m = read_partition_manifest(ds, eco)
        dup = m.groupBy("grain_key").count().filter(F.col("count") > 1).count()
        audit(eco, ds, "partition_keys_unique", dup == 0, f"duplicate_keys={dup}")
        assembled = read_ml(f"assembled_{ds}", ecosystem=eco)
        orphan = (
            assembled.withColumn("_gk", grain_key(*list(d["grain_key"])))
            .join(
                m.select("grain_key"), F.col("_gk") == F.col("grain_key"), "left_anti"
            )
            .count()
        )
        audit(
            eco,
            ds,
            "every_assembled_row_has_a_partition",
            orphan == 0,
            f"rows_without_partition={orphan}",
        )

# COMMAND ----------

# DBTITLE 1,Check -- embargo between partitions
for eco in ECOSYSTEMS:
    for d in fitted_datasets(eco):
        ds = d["dataset_id"]
        time_cols = [
            r["column_name"]
            for r in read_ml("feature_contract", ecosystem=eco)
            .filter((F.col("dataset_id") == ds) & (F.col("role") == "time"))
            .collect()
        ]
        spec = (
            read_ml("split_specification", ecosystem=eco)
            .filter(
                (F.col("dataset_id") == ds) & (F.col("split_version") == SPLIT_VERSION)
            )
            .first()
        )
        if not time_cols or spec is None or not spec["embargo_days"]:
            audit(
                eco,
                ds,
                "embargo_respected",
                True,
                "no time axis or no embargo required",
            )
            continue
        t = F.col(time_cols[0]).cast("date")
        df = (
            read_ml(f"dataset_{ds}", ecosystem=eco)
            .groupBy("partition")
            .agg(F.min(t).alias("lo"), F.max(t).alias("hi"))
        )
        rng = {r["partition"]: (r["lo"], r["hi"]) for r in df.collect()}
        need = int(spec["embargo_days"])
        ok = True
        detail = []
        for a, b in (("train", "validation"), ("validation", "test")):
            if a in rng and b in rng:
                gap = (rng[b][0] - rng[a][1]).days
                detail.append(f"{a}->{b} gap={gap}d")
                ok = ok and gap >= need
        audit(
            eco, ds, "embargo_respected", ok, "; ".join(detail) + f" required={need}d"
        )

# COMMAND ----------

# DBTITLE 1,Check -- grouped splits keep a group in one partition
for eco in ECOSYSTEMS:
    for d in fitted_datasets(eco):
        ds = d["dataset_id"]
        m = read_partition_manifest(ds, eco).filter(
            F.col("group_key").isNotNull()
            & F.col("partition").isin("train", "validation", "test")
        )
        split = (
            m.groupBy("group_key")
            .agg(F.countDistinct("partition").alias("n"))
            .filter(F.col("n") > 1)
            .count()
        )
        audit(
            eco,
            ds,
            "groups_not_split_across_partitions",
            split == 0,
            f"split_groups={split}",
        )

# COMMAND ----------

# DBTITLE 1,Check -- fits used the training partition only
for eco in ECOSYSTEMS:
    for d in fitted_datasets(eco):
        ds = d["dataset_id"]
        bad = (
            read_ml("imputer_parameters", ecosystem=eco)
            .filter((F.col("dataset_id") == ds) & (F.col("fit_partition") != "train"))
            .count()
        )
        audit(
            eco, ds, "imputers_fitted_on_train_only", bad == 0, f"non_train_fits={bad}"
        )

# COMMAND ----------

# DBTITLE 1,Check -- fit notebooks never read the test partition
import os as _os
import re as _re

_root = _os.path.join(repo_root(), "databricks", "ml")
_pat = _re.compile(r"""(==|!=)\s*['"]test['"]|isin\([^)]*['"]test['"]""")
_offenders = []
for sub in FIT_NOTEBOOK_DIRS:
    d = _os.path.join(_root, sub)
    if not _os.path.isdir(d):
        continue
    for name in sorted(_os.listdir(d)):
        if not name.endswith(".py"):
            continue
        with open(_os.path.join(d, name), encoding="utf-8") as fh:
            if _pat.search(fh.read()):
                _offenders.append(f"{sub}/{name}")
audit(
    "all",
    "fit_notebooks",
    "no_test_partition_reads",
    not _offenders,
    f"offenders={_offenders}",
)

# COMMAND ----------

# DBTITLE 1,Report -- label coverage, balance and regime per partition
for eco in ECOSYSTEMS:
    for d in fitted_datasets(eco):
        ds = d["dataset_id"]
        targets = [
            r["column_name"]
            for r in read_ml("feature_contract", ecosystem=eco)
            .filter((F.col("dataset_id") == ds) & (F.col("role") == "target"))
            .collect()
        ]
        if not targets:
            audit(
                eco,
                ds,
                "label_coverage_per_partition",
                True,
                "no target column (self-supervised or specification)",
            )
            continue
        t = targets[0]
        rows = (
            read_ml(f"dataset_{ds}", ecosystem=eco)
            .groupBy("partition")
            .agg(
                F.count("*").alias("n"),
                F.count(t).alias("labelled"),
                F.expr(f"avg(try_cast(`{t}` AS DOUBLE))").alias("mean"),
            )
            .collect()
        )
        stats = {r["partition"]: r for r in rows}
        detail = "; ".join(
            f"{p}: rows={r['n']} labelled={r['labelled']} mean={r['mean']}"
            for p, r in sorted(stats.items())
        )
        thin = [
            p
            for p in ("train", "validation", "test")
            if p in stats and stats[p]["labelled"] < MIN_LABELLED_ROWS
        ]
        audit(
            eco, ds, "label_coverage_per_partition", not thin, f"{detail}; thin={thin}"
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
