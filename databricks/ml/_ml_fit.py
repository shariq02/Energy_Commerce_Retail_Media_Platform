# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # ML FIT AND TRANSFORM LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the build of a dataset, shared by every build notebook, pulled in
# MAGIC with `%run ../_ml_fit` after `_ml_common`. Definitions only.
# MAGIC
# MAGIC Order is fixed: write the assembled rows once, declare the contract, split
# MAGIC by the spec rule, attach the stored partition, drop columns by the null
# MAGIC rule, fit imputers on the training partition only, apply them to every
# MAGIC partition, record the fitted parameters, write the final rows.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration constants
USED_PARTITIONS = ("train", "validation", "test")
NUMERIC_TYPES = ("double", "float", "int", "bigint", "smallint", "tinyint")

# COMMAND ----------

# DBTITLE 1,Attach the stored partition to the assembled rows


def load_partitioned(
    assembled: DataFrame, dataset_id: str, ecosystem: str
) -> DataFrame:
    """The assembled rows joined to the partition manifest; embargo and excluded
    rows are dropped."""
    m = (
        read_ml("dataset_manifest", ecosystem=ecosystem)
        .filter(F.col("dataset_id") == dataset_id)
        .first()
    )
    if m is None:
        raise RuntimeError(f"{dataset_id} is not registered")
    df = attach_partition(assembled, dataset_id, ecosystem, list(m["grain_key"]))
    return df.filter(F.col("partition").isin(*USED_PARTITIONS))


def contract_columns(dataset_id: str, ecosystem: str, role: str) -> list[str]:
    return [
        r["column_name"]
        for r in read_ml("feature_contract", ecosystem=ecosystem)
        .filter((F.col("dataset_id") == dataset_id) & (F.col("role") == role))
        .collect()
    ]


# COMMAND ----------

# DBTITLE 1,Group medians for many columns in one pass


def fit_group_medians(train: DataFrame, cols: list[str], group_cols: list[str]) -> dict:
    """Median per group (and overall) for every column, from the training rows."""
    if not cols:
        return {}
    aggs = [F.percentile_approx(c, 0.5).alias(c) for c in cols]
    overall = train.agg(*aggs).first().asDict()
    groups = []
    if group_cols:
        for r in train.groupBy(*group_cols).agg(*aggs).collect():
            groups.append({**{g: r[g] for g in group_cols}, **{c: r[c] for c in cols}})
    return {"group_cols": group_cols, "overall": overall, "groups": groups}


def apply_group_medians(df: DataFrame, cols: list[str], params: dict) -> DataFrame:
    """Add `<col>_imputed` = value, else group median, else overall median."""
    gc = params["group_cols"]
    out = df
    if gc and params["groups"]:
        ref = spark.createDataFrame(
            [
                tuple([g[k] for k in gc] + [g[c] for c in cols])
                for g in params["groups"]
            ],
            [*gc, *[f"_gm_{c}" for c in cols]],
        )
        out = out.join(F.broadcast(ref), gc, "left")
    for c in cols:
        gm = F.col(f"_gm_{c}") if (gc and params["groups"]) else F.lit(None)
        out = out.withColumn(
            f"{c}_imputed",
            F.coalesce(F.col(c), gm, F.lit(params["overall"][c])).cast("double"),
        )
    return out.drop(*[f"_gm_{c}" for c in cols])


# COMMAND ----------

# DBTITLE 1,Fit on one training frame and record the parameters


def _fit_params(
    train: DataFrame, numeric: list[str], categorical: list[str], group_cols
):
    return {
        "numeric": fit_group_medians(train, numeric, group_cols),
        "categorical": {c: fit_category_levels(train, c) for c in categorical},
    }


def _record(dataset_id, ecosystem, fold_id, params, numeric, categorical, n_rows):
    rows = []
    for c in numeric:
        rows.append(
            (
                c,
                "group_median",
                {
                    "group_cols": params["numeric"]["group_cols"],
                    "overall": params["numeric"]["overall"].get(c),
                    "groups": [
                        {k: g[k] for k in [*params["numeric"]["group_cols"], c]}
                        for g in params["numeric"]["groups"]
                    ][:500],
                },
            )
        )
    for c in categorical:
        rows.append((c, "category_levels", {"levels": params["categorical"][c][:500]}))
    if rows:
        record_imputer(
            dataset_id, ecosystem, fold_id=fold_id, rows=rows, fit_rows=n_rows
        )


def merge_null_classes(dataset_id: str, ecosystem: str, updates: list[tuple]) -> None:
    """Update the recorded null classes without losing the other columns."""
    prev = {
        r["column_name"]: (
            r["column_name"],
            r["null_class"],
            r["method"],
            r["parameters"],
        )
        for r in read_ml("null_class_registry", ecosystem=ecosystem)
        .filter(F.col("dataset_id") == dataset_id)
        .collect()
    }
    for c, k, m, p in updates:
        prev[c] = (c, k, m, p)
    register_null_classes(dataset_id, ecosystem, list(prev.values()))


def fold_train_frames(
    df: DataFrame, mode: str, calendar_key: str | None, date_col: str | None
):
    """(fold_id, training rows of that fold) for every fold of the scheme."""
    if mode == "rolling":
        for k in range(len(ROLLING_BLOCKS[calendar_key])):
            tr, _ = fold_frames(df, calendar_key, k, date_col)
            yield k, tr
    elif mode == "grouped":
        base = df.filter(F.col("partition") == "train")
        for k in range(GROUPED_FOLDS):
            yield k, base.filter(F.col("fold_id") != k)


# COMMAND ----------

# DBTITLE 1,Split, fit, transform


def fit_transform(
    assembled: DataFrame,
    dataset_id: str,
    ecosystem: str,
    *,
    group_cols: list[str],
    fold_mode: str = "none",
    calendar_key: str | None = None,
    date_col: str | None = None,
    impute: bool = True,
    exempt: tuple = (),
    rid: str,
    source: str,
) -> DataFrame:
    """Apply the null rule, fit imputers on the training partition only, apply
    them to every partition and record final and per-fold parameters."""
    df = load_partitioned(assembled, dataset_id, ecosystem)
    if not impute:
        merge_null_classes(
            dataset_id,
            ecosystem,
            [
                (c, "A", "no_imputation", None)
                for c in contract_columns(dataset_id, ecosystem, "feature")
            ],
        )
        return df
    feats = [
        c for c in contract_columns(dataset_id, ecosystem, "feature") if c in df.columns
    ]
    train = df.filter(F.col("partition") == "train")
    rates = null_rates(train, feats)
    classed = {
        r["column_name"]
        for r in read_ml("null_class_registry", ecosystem=ecosystem)
        .filter(
            (F.col("dataset_id") == dataset_id) & F.col("null_class").isin("D", "F")
        )
        .collect()
    }
    drop = columns_to_drop(rates, exempt=classed | set(exempt))
    df = df.drop(*drop)
    feats = [c for c in feats if c not in drop]
    dtypes = dict(df.dtypes)
    numeric = [c for c in feats if dtypes[c] in NUMERIC_TYPES and rates[c] > 0]
    categorical = [c for c in feats if dtypes[c] == "string"]
    print(
        f"{dataset_id}: dropped {len(drop)} column(s) {drop[:10]}; imputing {len(numeric)} numeric, {len(categorical)} categorical"
    )

    train = df.filter(F.col("partition") == "train")
    final = _fit_params(train, numeric, categorical, group_cols)
    _record(dataset_id, ecosystem, -1, final, numeric, categorical, train.count())
    if fold_mode != "none":
        for k, tr in fold_train_frames(df, fold_mode, calendar_key, date_col):
            p = _fit_params(tr, numeric, categorical, group_cols)
            _record(dataset_id, ecosystem, k, p, numeric, categorical, tr.count())

    out = df
    for c in numeric:
        out = out.withColumn(f"{c}_is_missing", F.col(c).isNull())
    if numeric:
        out = apply_group_medians(out, numeric, final["numeric"])
    for c in categorical:
        out = apply_category_levels(out, c, final["categorical"][c])
    merge_null_classes(
        dataset_id,
        ecosystem,
        [
            (c, "A", "dropped_at_null_threshold", {"threshold": NULL_DROP_THRESHOLD})
            for c in drop
        ]
        + [(c, "C", "group_median", {"group_cols": group_cols}) for c in numeric]
        + [(c, "C", "unknown_level", None) for c in categorical]
        + [
            (c, "B", "leave_null_with_flag", None)
            for c in feats
            if c not in numeric and c not in categorical
        ],
    )
    record_null_rates(
        dataset_id,
        ecosystem,
        [
            (
                c,
                float(r),
                "exempt_missingness_is_signal"
                if c in classed
                else "drop"
                if c in drop
                else "keep_indicator_impute_if_beats_baseline"
                if r >= 0.5
                else "keep",
            )
            for c, r in rates.items()
        ],
    )
    return out


# COMMAND ----------

# DBTITLE 1,Declare, build and finish datasets


def declare_dataset(dataset_id: str, columns: list[str]) -> list[str]:
    """Register the manifest row, the feature contract and the null classes of a
    dataset from the spec file and the columns of its frame; the columns that got
    the default clock are returned."""
    spec = dataset_spec(dataset_id)
    eco = spec["ecosystem"]
    rules = spec.get("contract", {})
    label_inputs = rules.get("label_inputs", [])
    contract, classes, defaulted = derive_contract(
        [c for c in columns if c not in label_inputs],
        keys=spec["grain"],
        time_col=spec.get("time_col"),
        static=tuple(rules.get("static", ())),
        registry=tuple(rules.get("registry", ())),
        perfect_prognosis=tuple(rules.get("perfect_prognosis", ())),
        context=tuple(rules.get("context", ())),
        group_col=spec.get("group_col"),
        overrides={k: tuple(v) for k, v in rules.get("overrides", {}).items()},
        label_derived=tuple(spec.get("label_derived", ())),
        default_clock=DEFAULT_CLOCK[eco],
    )
    contract += [
        (
            c,
            "label_input",
            "observation_realised",
            0,
            None,
            "defines the target; excluded from the dataset",
        )
        for c in label_inputs
    ]
    register_feature_contract(dataset_id, eco, contract)
    register_null_classes(dataset_id, eco, classes)
    register_dataset(dataset_id, status="assembled")
    return defaulted


def build_dataset(
    dataset_id: str,
    frame: DataFrame,
    *,
    rid: str,
    source: str,
    blocks: list | None = None,
) -> None:
    """The whole build of one dataset from its assembled frame: the heavy frame is
    computed and written once (stage 1), the contract, the split and the fit read
    those rows, and the final rows replace them in the family table."""
    spec = dataset_spec(dataset_id)
    eco = spec["ecosystem"]
    component = f"ml/{eco}/{dataset_id}"
    label_inputs = spec.get("contract", {}).get("label_inputs", [])
    grain = list(spec["grain"])
    columns = frame.columns
    write_dataset_rows(
        add_ml_provenance(frame, dataset_id, eco, rid),
        dataset_id,
        source=source,
        component=component + "/assembled",
        rid=rid,
    )
    assembled = read_dataset(dataset_id).select(*columns)
    assert_unique_grain(assembled, grain, component=component, source=source, rid=rid)
    assert_no_forbidden_columns(
        assembled.drop(*label_inputs), component=component, source=source, rid=rid
    )
    defaulted = declare_dataset(dataset_id, columns)
    if defaulted:
        print(f"WARN {dataset_id}: columns given the default clock:", defaulted)
    write_dataset_partitions(partition_frame(assembled, dataset_id), dataset_id)
    write_diagnostic_partitions(assembled, dataset_id)
    fit = spec["fit"]
    out = fit_transform(
        assembled.drop(*label_inputs),
        dataset_id,
        eco,
        group_cols=fit["group_cols"],
        fold_mode=fit["fold_mode"],
        calendar_key=fit["calendar_key"],
        date_col=fit["date_col"],
        impute=fit["impute"],
        rid=rid,
        source=source,
    )
    out = add_ml_provenance(out, dataset_id, eco, rid)
    assert_unique_grain(out, grain, component=component, source=source, rid=rid)
    assert_no_forbidden_columns(out, component=component, source=source, rid=rid)
    write_dataset_rows(
        out, dataset_id, source=source, component=component + "/fit", rid=rid
    )
    set_dataset_status(dataset_id, eco, "fitted")
    final = read_dataset(dataset_id).select(*out.columns)
    targets = contract_columns(dataset_id, eco, "target")
    inspected = inspect_ml_table(
        final,
        dataset_table(dataset_id),
        ecosystem=eco,
        key_cols=grain,
        partition_col="partition",
        target_col=targets[0] if targets else None,
    )
    write_ml_findings(
        eco, "build__" + dataset_id, dataset_id, [*inspected, *(blocks or [])]
    )


def build_datasets(frames: dict, *, rid: str, source: str, blocks: dict | None = None):
    """Build every dataset of `frames` (dataset id -> assembled frame) in order;
    `blocks` holds extra findings blocks per dataset (the dropped target rows)."""
    for dataset_id, frame in frames.items():
        print(f"=== {dataset_id}")
        build_dataset(
            dataset_id,
            frame,
            rid=rid,
            source=source,
            blocks=(blocks or {}).get(dataset_id),
        )


def register_waiting_datasets(ecosystem: str) -> None:
    """Register the datasets that are declared but not built (a status in the spec)."""
    for name, spec in ML_SPEC["datasets"].items():
        if spec["ecosystem"] == ecosystem and spec.get("registered_only"):
            register_dataset(name)
            print(f"registered {name}: {spec['status']}")


def write_stored_table(
    df: DataFrame,
    table: str,
    ecosystem: str,
    *,
    key_cols: list[str],
    source: str,
    component: str,
    rid: str,
    section: str = "stored",
    target_col: str | None = None,
    blocks: list | None = None,
) -> None:
    """One table that is kept (a shared target, a shared feature, a helper): the
    provenance, the grain and forbidden-column gates, the write, the findings."""
    out = add_ml_provenance(df, table, ecosystem, rid)
    assert_unique_grain(out, key_cols, component=component, source=source, rid=rid)
    assert_no_forbidden_columns(out, component=component, source=source, rid=rid)
    check(
        component,
        source,
        "non_empty",
        out.limit(1).count() > 0,
        detail="no rows produced; check the input filters",
        rid=rid,
    )
    write_ml(
        out, table, ecosystem=ecosystem, source=source, component=component, rid=rid
    )
    inspected = inspect_ml_table(
        read_ml(table, ecosystem=ecosystem),
        table,
        ecosystem=ecosystem,
        key_cols=key_cols,
        target_col=target_col,
    )
    write_ml_findings(
        ecosystem, f"{section}__{table}", table, [*inspected, *(blocks or [])]
    )
