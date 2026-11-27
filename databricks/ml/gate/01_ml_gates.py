# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # ML GATES -- LEAKAGE, NULL HANDLING AND FREEZE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** audit every fitted dataset of every ecosystem (the list comes from
# MAGIC the dataset manifest): the leakage checks, the null-handling checks and, when a
# MAGIC whole family passes both, the freeze of its family table at one Delta version.
# MAGIC Run after the build notebooks. ONLY names the datasets to audit again; the stored
# MAGIC results of the others stay, and the freeze still covers every family.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECOSYSTEMS = tuple(ML_SCHEMAS)
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
LEAKAGE = "01_leakage_audit"
NULLS = "02_null_handling_audit"
TWO_SIDED_ALLOWED = {"weather_imputation", "honda_anomaly"}
RETENTION = "interval 3650 days"
BUILD_DIRS = ("energy", "commerce", "genomics")
# Empty: audit every fitted dataset. Dataset ids: audit only these.
ONLY = ()

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Helpers -- one audit result, the datasets, the columns of a dataset
results = []  # (ecosystem, dataset_id, gate, check_name, status, detail)
_columns = {}


def audit(gate, eco, dataset_id, check_name, ok, detail=""):
    results.append(
        (eco, dataset_id, gate, check_name, "PASS" if ok else "FAIL", detail)
    )
    record_check(
        f"ml/gate/{gate}/{dataset_id}", SOURCE, check_name, ok, detail=detail, rid=rid
    )


def fitted_datasets(eco):
    return (
        read_ml("dataset_manifest", ecosystem=eco)
        .filter(F.col("status").isin(*FIT_STATES))
        .collect()
    )


def audited(eco):
    """The fitted datasets this run audits: all of them, or those named in ONLY."""
    return [d for d in fitted_datasets(eco) if not ONLY or d["dataset_id"] in ONLY]


_known = {d["dataset_id"] for eco in ECOSYSTEMS for d in fitted_datasets(eco)}
_unknown = [d for d in ONLY if d not in _known]
if _unknown:
    raise RuntimeError(f"ONLY names datasets that are not fitted: {_unknown}")


def base_column(name):
    for s in DERIVED_SUFFIXES:
        if name.endswith(s):
            return name[: -len(s)]
    return name


def dataset_columns(ds):
    """The columns that hold a value in the rows of the dataset (the family table
    also holds the columns of the other datasets of its family, all null here)."""
    if ds not in _columns:
        df = read_dataset(ds)
        counts = df.agg(
            *[F.count(F.col(f"`{c}`")).alias(c) for c in df.columns]
        ).first()
        _columns[ds] = [c for c in df.columns if counts[c] > 0]
    return _columns[ds]


def contract_rows(eco, ds):
    return (
        read_ml("feature_contract", ecosystem=eco)
        .filter(F.col("dataset_id") == ds)
        .collect()
    )


# COMMAND ----------

# DBTITLE 1,Leakage -- contract clocks, lags, label inputs and label-derived columns
for eco in ECOSYSTEMS:
    for d in audited(eco):
        ds = d["dataset_id"]
        contract = contract_rows(eco, ds)
        by_col = {r["column_name"]: r for r in contract}
        cols = dataset_columns(ds)
        uncovered = [
            c
            for c in cols
            if c not in PASSTHROUGH and c not in by_col and base_column(c) not in by_col
        ]
        audit(
            LEAKAGE,
            eco,
            ds,
            "columns_have_a_declared_role",
            not uncovered,
            f"undeclared={uncovered[:15]}",
        )
        bad_load = [r["column_name"] for r in contract if (r["clock"] or "") == "load"]
        audit(
            LEAKAGE,
            eco,
            ds,
            "no_load_clock_feature",
            not bad_load,
            f"columns={bad_load}",
        )
        bad_lag = [
            r["column_name"]
            for r in contract
            if r["role"] == "feature"
            and r["clock"] == "observation_realised"
            and (r["lag_days"] or 0) < MIN_REALISED_LAG_DAYS
        ]
        audit(
            LEAKAGE,
            eco,
            ds,
            "realised_inputs_lagged",
            not bad_lag,
            f"columns={bad_lag}",
        )
        label_inputs = [
            r["column_name"] for r in contract if r["role"] == "label_input"
        ]
        leaked = [c for c in label_inputs if c in cols]
        audit(
            LEAKAGE,
            eco,
            ds,
            "no_label_input_in_dataset",
            not leaked,
            f"columns={leaked}",
        )
        declared = set(dataset_spec(ds).get("label_derived", []))
        like = tuple(dataset_spec(ds).get("label_derived_like", ()))
        used = [
            r["column_name"]
            for r in contract
            if r["role"] == "feature"
            and is_label_derived(base_column(r["column_name"]), declared, like)
        ]
        audit(
            LEAKAGE,
            eco,
            ds,
            "no_label_derived_column_is_a_feature",
            not used,
            f"columns={used}",
        )
        biological_targets = [
            r["column_name"]
            for r in contract
            if r["role"] == "target"
            and r["column_name"] != dataset_spec(ds).get("target_name")
            and r["column_name"].startswith(BIOLOGICAL_TARGET_PREFIXES)
        ]
        audit(
            LEAKAGE,
            eco,
            ds,
            "no_biological_column_has_the_target_role",
            not biological_targets,
            f"columns={biological_targets}",
        )
        forbidden = [c for c in cols if c in CLASS_E_COLUMNS]
        audit(
            LEAKAGE, eco, ds, "no_class_e_column", not forbidden, f"columns={forbidden}"
        )

# COMMAND ----------

# DBTITLE 1,Leakage -- no few columns reproduce the label, and the null gap by partition
PROXY_SAMPLE_ROWS = 60_000
NULL_GAP_SHOWN = 0.2


def feature_columns(eco, ds, cols):
    """The raw feature columns of a dataset that hold a value (the null pattern of a
    column is part of what a model sees)."""
    roles = {r["column_name"]: r["role"] for r in contract_rows(eco, ds)}
    return [c for c, role in roles.items() if role == "feature" and c in cols]


for eco in ECOSYSTEMS:
    for d in audited(eco):
        ds = d["dataset_id"]
        target = dataset_spec(ds).get("target_name")
        cols = dataset_columns(ds)
        features = feature_columns(eco, ds, cols)
        frame = read_dataset(ds)
        train = frame.filter(F.col("partition") == "train")
        values = (
            {r[0] for r in train.select(target).distinct().limit(3).collect()}
            if target and target in cols
            else set()
        )
        if features and values and values <= {0, 1}:
            total = train.count()
            share = min(1.0, PROXY_SAMPLE_ROWS * 1.2 / max(total, 1))
            sample = (
                train.select(target, *[f"`{c}`" for c in features])
                .sample(False, share, seed=43)
                .limit(PROXY_SAMPLE_ROWS)
                .toPandas()
            )
            found = label_proxy_columns(sample, target, features)
            audit(
                LEAKAGE,
                eco,
                ds,
                "no_few_columns_reproduce_the_label",
                not found,
                f"columns={[f'{c} ({how})' for c, how in found[:12]]}",
            )
        rates = (
            frame.groupBy("partition")
            .agg(
                *[
                    F.avg(F.col(f"`{c}`").isNull().cast("double")).alias(c)
                    for c in features
                ]
            )
            .collect()
            if features
            else []
        )
        by_part = {r["partition"]: r.asDict() for r in rates}
        gaps = sorted(
            (
                (abs(by_part["test"][c] - by_part["train"][c]), c)
                for c in features
                if "test" in by_part and "train" in by_part
            ),
            reverse=True,
        )
        shown = [f"{c} {g:.2f}" for g, c in gaps if g >= NULL_GAP_SHOWN][:10]
        audit(
            LEAKAGE,
            eco,
            ds,
            "null_rate_gap_train_to_test_listed",
            True,
            f"gap of at least {NULL_GAP_SHOWN}: {shown}",
        )

# COMMAND ----------

# DBTITLE 1,Leakage -- partitions are disjoint, complete and keep their embargo
for eco in ECOSYSTEMS:
    for d in audited(eco):
        ds = d["dataset_id"]
        m = read_partition_manifest(ds, eco)
        dup = m.groupBy("grain_key").count().filter(F.col("count") > 1).count()
        audit(
            LEAKAGE, eco, ds, "partition_keys_unique", dup == 0, f"duplicate_keys={dup}"
        )
        orphan = (
            read_dataset(ds)
            .withColumn("_gk", grain_key(*list(d["grain_key"])))
            .join(
                m.select("grain_key"), F.col("_gk") == F.col("grain_key"), "left_anti"
            )
            .count()
        )
        audit(
            LEAKAGE,
            eco,
            ds,
            "every_dataset_row_has_a_partition",
            orphan == 0,
            f"rows_without_partition={orphan}",
        )
        time_cols = [
            r["column_name"] for r in contract_rows(eco, ds) if r["role"] == "time"
        ]
        need = int(d["embargo_days"] or 0)
        if not time_cols or not need:
            audit(
                LEAKAGE,
                eco,
                ds,
                "embargo_respected",
                True,
                "no time axis or no embargo required",
            )
            continue
        t = F.col(time_cols[0]).cast("date")
        data = read_dataset(ds)
        # a dataset that unions series families splits each family on its own calendar
        families = ["series_family"] if "series_family" in data.columns else []
        spans = data.groupBy(*families, "partition").agg(
            F.min(t).alias("lo"), F.max(t).alias("hi")
        )
        by_family = {}
        for r in spans.collect():
            fam = r["series_family"] if families else ""
            by_family.setdefault(fam, {})[r["partition"]] = (r["lo"], r["hi"])
        ok, detail = True, []
        for fam, rng in sorted(by_family.items()):
            for a, b in (("train", "validation"), ("validation", "test")):
                if a in rng and b in rng:
                    gap = (rng[b][0] - rng[a][1]).days
                    detail.append(f"{fam + ' ' if fam else ''}{a}->{b} gap={gap}d")
                    ok = ok and gap >= need
        audit(
            LEAKAGE,
            eco,
            ds,
            "embargo_respected",
            ok,
            "; ".join(detail) + f" required={need}d",
        )

# COMMAND ----------

# DBTITLE 1,Leakage -- groups stay in one partition, fits use training rows only
for eco in ECOSYSTEMS:
    for d in audited(eco):
        ds = d["dataset_id"]
        manifest = read_partition_manifest(ds, eco)
        if not manifest.filter(F.col("rule_id").startswith("hash:")).limit(1).count():
            audit(
                LEAKAGE,
                eco,
                ds,
                "groups_not_split_across_partitions",
                True,
                "time-based or source split; groups only define folds inside train",
            )
        else:
            m = manifest.filter(
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
                LEAKAGE,
                eco,
                ds,
                "groups_not_split_across_partitions",
                split == 0,
                f"split_groups={split}",
            )
        bad = (
            read_ml("imputer_parameters", ecosystem=eco)
            .filter((F.col("dataset_id") == ds) & (F.col("fit_partition") != "train"))
            .count()
        )
        audit(
            LEAKAGE,
            eco,
            ds,
            "imputers_fitted_on_train_only",
            bad == 0,
            f"non_train_fits={bad}",
        )

# COMMAND ----------

# DBTITLE 1,Leakage -- build code never reads the test partition
import os as _os
import re as _re

_root = _os.path.join(repo_root(), "databricks", "ml")
_pat = _re.compile(r"""(==|!=)\s*['"]test['"]|isin\([^)]*['"]test['"]""")
_offenders = []
for sub in (*BUILD_DIRS, "."):
    d = _os.path.join(_root, sub)
    if not _os.path.isdir(d):
        continue
    for name in sorted(_os.listdir(d)):
        if not name.endswith(".py") or (sub == "." and name != "_ml_fit.py"):
            continue
        with open(_os.path.join(d, name), encoding="utf-8") as fh:
            if _pat.search(fh.read()):
                _offenders.append(f"{sub}/{name}")
audit(
    LEAKAGE,
    "all",
    "build_notebooks",
    "no_test_partition_reads",
    not _offenders,
    f"offenders={_offenders}",
)

# COMMAND ----------

# DBTITLE 1,Leakage -- label coverage and balance per partition
for eco in ECOSYSTEMS:
    for d in audited(eco):
        ds = d["dataset_id"]
        targets = [
            r["column_name"] for r in contract_rows(eco, ds) if r["role"] == "target"
        ]
        if not targets:
            audit(
                LEAKAGE,
                eco,
                ds,
                "label_coverage_per_partition",
                True,
                "no target column (self-supervised or specification)",
            )
            continue
        t = targets[0]
        stats = {
            r["partition"]: r
            for r in read_dataset(ds)
            .groupBy("partition")
            .agg(
                F.count("*").alias("n"),
                F.count(t).alias("labelled"),
                F.expr(f"avg(try_cast(`{t}` AS DOUBLE))").alias("mean"),
            )
            .collect()
        }
        detail = "; ".join(
            f"{p}: rows={r['n']} labelled={r['labelled']} mean={r['mean']}"
            for p, r in sorted(stats.items())
        )
        floor = dataset_spec(ds).get("min_labelled_rows", MIN_LABELLED_ROWS)
        thin = [
            p
            for p in ("train", "validation", "test")
            if p in stats and stats[p]["labelled"] < floor
        ]
        audit(
            LEAKAGE,
            eco,
            ds,
            "label_coverage_per_partition",
            not thin,
            f"{detail}; thin={thin}",
        )

# COMMAND ----------

# DBTITLE 1,Null handling -- classes, empty columns and missing indicators
for eco in ECOSYSTEMS:
    for d in audited(eco):
        ds = d["dataset_id"]
        feats = {
            r["column_name"] for r in contract_rows(eco, ds) if r["role"] == "feature"
        }
        classed = {
            r["column_name"]
            for r in read_ml("null_class_registry", ecosystem=eco)
            .filter(F.col("dataset_id") == ds)
            .collect()
        }
        missing = sorted(feats - classed)
        audit(
            NULLS,
            eco,
            ds,
            "null_class_recorded_for_every_feature",
            not missing,
            f"missing={missing[:15]}",
        )
        cols = [c for c in dataset_columns(ds) if c not in PASSTHROUGH]
        full = [c for c, r in null_rates(read_dataset(ds), cols).items() if r >= 1.0]
        audit(NULLS, eco, ds, "no_entirely_null_column", not full, f"columns={full}")
        present = set(cols)
        imputed = {
            r["column_name"]
            for r in read_ml("imputer_parameters", ecosystem=eco)
            .filter(
                (F.col("dataset_id") == ds)
                & (F.col("fold_id") == -1)
                & (F.col("method") == "group_median")
            )
            .collect()
        }
        lacking = sorted(
            c for c in imputed if c in present and f"{c}_is_missing" not in present
        )
        audit(
            NULLS,
            eco,
            ds,
            "imputed_columns_have_indicator",
            not lacking,
            f"columns={lacking}",
        )
        two_sided = (
            read_ml("null_class_registry", ecosystem=eco)
            .filter(
                (F.col("dataset_id") == ds)
                & (F.col("method") == "two_sided_interpolation")
            )
            .count()
        )
        audit(
            NULLS,
            eco,
            ds,
            "fills_are_causal",
            two_sided == 0 or ds in TWO_SIDED_ALLOWED,
            f"two_sided_columns={two_sided}",
        )

# COMMAND ----------

# DBTITLE 1,Null handling -- causal fills stay within the final gap limits
_limits = load_gap_limits("energy")
for d in audited("energy"):
    ds = d["dataset_id"]
    df = read_dataset(ds)
    bad = []
    for c in dataset_columns(ds):
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
    audit(NULLS, "energy", ds, "fills_within_gap_limits", not bad, f"violations={bad}")
print(
    "INFO ablation (model with and without imputation) needs a trained model; it "
    "is produced in modelling and attached to the freeze record, not run here"
)

# COMMAND ----------

# DBTITLE 1,Write the gate results and fail on any FAIL
for eco in ECOSYSTEMS:
    for gate in (LEAKAGE, NULLS):
        mine = [
            (ds, g, c, s, det, rid, now_utc())
            for (e, ds, g, c, s, det) in results
            if e == eco and g == gate
        ]
        if not mine:
            continue
        ids = ", ".join(f"'{r[0]}'" for r in {r[0]: r for r in mine}.values())
        replace_rows(
            spark.createDataFrame(mine, REGISTRY_DDL["gate_results"]),
            "gate_results",
            ecosystem=eco,
            predicate=f"gate = '{gate}' AND dataset_id IN ({ids})",
        )
_shown = results
if ONLY:
    # a partial run keeps the findings of the datasets it did not audit
    _shown = [
        (eco, r["dataset_id"], r["gate"], r["check_name"], r["status"], r["detail"])
        for eco in ECOSYSTEMS
        for r in read_ml("gate_results", ecosystem=eco).collect()
    ]
_blocks = [
    (
        "results",
        "\n".join(
            [
                "| ecosystem | dataset | gate | check | status | detail |",
                "|---|---|---|---|---|---|",
            ]
            + [
                f"| {e} | {d} | {g} | {c} | {s} | {det} |"
                for (e, d, g, c, s, det) in _shown
            ]
        ),
    )
]
write_ml_findings("gate", "01_ml_gates", "ML gates", _blocks)
_failed = [r for r in results if r[4] == "FAIL"]
if _failed:
    raise RuntimeError(
        f"{len(_failed)} gate check(s) failed: "
        + "; ".join(f"{r[1]}.{r[3]}" for r in _failed[:20])
    )
print(f"{len(results)} gate check(s) passed")

# COMMAND ----------


# DBTITLE 1,Freeze -- protect and read one version of a family table
def protect_and_read(full: str, version: int) -> str | None:
    """Set long retention, then read every file of the version; the error or None."""
    spark.sql(
        f"ALTER TABLE {full} SET TBLPROPERTIES ("
        f"'delta.deletedFileRetentionDuration' = '{RETENTION}', "
        f"'delta.logRetentionDuration' = '{RETENTION}')"
    )
    try:
        (
            spark.sql(f"SELECT * FROM {full} VERSION AS OF {version}")
            .write.format("noop")
            .mode("overwrite")
            .save()
        )
    except Exception as exc:
        return str(exc).splitlines()[0][:200]
    return None


# COMMAND ----------

# DBTITLE 1,Freeze -- a family is frozen when every dataset of it passed both gates
_frozen, _refused = [], []
for eco in ECOSYSTEMS:
    manifest = read_ml("dataset_manifest", ecosystem=eco).collect()
    gates = read_ml("gate_results", ecosystem=eco)
    for family in sorted({m["family"] for m in manifest if m["family"]}):
        members = [m["dataset_id"] for m in manifest if m["family"] == family]
        built = [d for d in members if d in built_datasets(eco)]
        statuses = {m["dataset_id"]: m["status"] for m in manifest}
        waiting = [d for d in built if statuses.get(d) not in FIT_STATES]
        failed = []
        for ds in built:
            mine = gates.filter(F.col("dataset_id") == ds)
            for gate in (LEAKAGE, NULLS):
                run = mine.filter(F.col("gate") == gate).agg(F.max("run_at")).first()[0]
                if run is None:
                    failed.append(f"{ds}:{gate} not run")
                    continue
                bad = mine.filter(
                    (F.col("gate") == gate)
                    & (F.col("run_at") == run)
                    & (F.col("status") == "FAIL")
                ).count()
                if bad:
                    failed.append(f"{ds}:{gate} {bad} failed")
        if waiting or failed:
            _refused.append((eco, family, f"not fitted={waiting} gates={failed}"))
            continue
        full = ml_fqn(f"dataset_{family}", eco)
        version = int(spark.sql(f"DESCRIBE HISTORY {full} LIMIT 1").first()["version"])
        error = protect_and_read(full, version)
        if error:
            _refused.append((eco, family, f"version {version} unreadable: {error}"))
            continue
        where = ecosystem_predicate(eco, f"family = '{family}'")
        spark.sql(
            f"UPDATE {ml_fqn('dataset_manifest', eco)} SET status = 'frozen', "
            f"freeze_status = 'frozen', frozen_delta_version = {version}, "
            f"updated_at = current_timestamp() WHERE {where}"
        )
        _frozen.append((eco, family, version))
for eco, family, v in _frozen:
    print(f"FROZEN  {eco}.dataset_{family} at Delta version {v}")
for eco, family, why in _refused:
    print(f"REFUSED {eco}.dataset_{family}: {why}")
print(f"frozen={len(_frozen)} refused={len(_refused)}")
