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
# MAGIC Order is fixed: check the spec against the frames, write the assembled rows
# MAGIC with their partition columns once, declare the contract, store the partition
# MAGIC manifest, drop columns by the null rule, fit the imputers of the final fit
# MAGIC and of every fold on the training rows only (one pass per kind of
# MAGIC parameter), apply the final fit to every partition, record the fitted
# MAGIC parameters, write the final rows.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Configuration constants
USED_PARTITIONS = ("train", "validation", "test")
NUMERIC_TYPES = ("double", "float", "int", "bigint", "smallint", "tinyint")
FIT_BATCH_AGGREGATES = 400
FIT_BATCH_TEXT_COLUMNS = 25

# COMMAND ----------

# DBTITLE 1,The partitioned rows of a dataset


def load_partitioned(assembled: DataFrame) -> DataFrame:
    """The assembled rows of the used partitions; embargo and excluded rows are
    dropped. The partition, fold and group columns are stored with the rows."""
    return assembled.drop("diagnostic_group").filter(
        F.col("partition").isin(*USED_PARTITIONS)
    )


def contract_columns(dataset_id: str, ecosystem: str, role: str) -> list[str]:
    return [
        r["column_name"]
        for r in read_ml("feature_contract", ecosystem=ecosystem)
        .filter((F.col("dataset_id") == dataset_id) & (F.col("role") == role))
        .collect()
    ]


# COMMAND ----------

# DBTITLE 1,The fits of a dataset -- the final fit and every fold in one pass


def fit_memberships(mode: str, calendar_key: str | None, date_col: str | None) -> dict:
    """Fit id -> the condition that selects its training rows; -1 is the final fit
    (the whole train partition), 0 and up are the folds of the scheme."""
    train = F.col("partition") == "train"
    fits = {-1: train}
    if mode == "rolling":
        for k in range(len(ROLLING_BLOCKS[calendar_key])):
            fits[k] = rolling_train_condition(date_col, calendar_key, k)
    elif mode == "grouped":
        for k in range(GROUPED_FOLDS):
            fits[k] = train & (F.col("fold_id") != k)
    return fits


def _member(condition):
    return F.when(condition, F.lit(1)).otherwise(F.lit(0))


def fit_medians(
    df: DataFrame, cols: list[str], group_cols: list[str], fits: dict
) -> tuple[dict, dict]:
    """Median per column, overall and per group, for every fit, in one pass per
    batch of columns. A fit reads only its own training rows. Returns the
    parameters and the number of training rows per fit id."""
    ids = list(fits)
    df = df.filter(fits[-1])
    counts = [F.sum(_member(fits[j])).alias(f"n{i}") for i, j in enumerate(ids)]
    first = df.agg(*counts).first()
    params = {j: {"group_cols": group_cols, "overall": {}, "groups": {}} for j in ids}
    size = max(1, FIT_BATCH_AGGREGATES // len(ids))
    for start in range(0, len(cols), size):
        batch = cols[start : start + size]
        aggs = [
            F.percentile_approx(F.when(fits[j], F.col(c)), 0.5).alias(f"m{i}_{b}")
            for i, j in enumerate(ids)
            for b, c in enumerate(batch)
        ]
        row = df.agg(*aggs).first()
        for i, j in enumerate(ids):
            for b, c in enumerate(batch):
                params[j]["overall"][c] = row[f"m{i}_{b}"]
        if not group_cols:
            continue
        for r in df.groupBy(*group_cols).agg(*aggs, *counts).collect():
            key = tuple(r[g] for g in group_cols)
            for i, j in enumerate(ids):
                if r[f"n{i}"]:
                    entry = params[j]["groups"].setdefault(
                        key, dict(zip(group_cols, key))
                    )
                    for b, c in enumerate(batch):
                        entry[c] = r[f"m{i}_{b}"]
    for j in ids:
        params[j]["groups"] = list(params[j]["groups"].values())
    return params, {j: int(first[f"n{i}"] or 0) for i, j in enumerate(ids)}


def fit_levels(df: DataFrame, cols: list[str], fits: dict) -> dict:
    """Most frequent levels of every text column for every fit (at least
    CATEGORY_MIN_COUNT training rows, at most CATEGORY_MAX_LEVELS); everything else
    becomes the rare level when applied. One pass per batch of columns."""
    ids = list(fits)
    levels = {j: {c: [] for c in cols} for j in ids}
    members = [_member(fits[j]).alias(f"m{i}") for i, j in enumerate(ids)]
    rows = df.filter(fits[-1])
    for start in range(0, len(cols), FIT_BATCH_TEXT_COLUMNS):
        batch = cols[start : start + FIT_BATCH_TEXT_COLUMNS]
        pairs = []
        for c in batch:
            name, ident = c.replace("'", "''"), c.replace("`", "``")
            pairs.append(f"'{name}', cast(`{ident}` as string)")
        stack = f"stack({len(batch)}, {', '.join(pairs)}) as (column_name, value)"
        stacked = rows.select(F.expr(stack), *members).filter(
            F.col("value").isNotNull()
        )
        counted = stacked.groupBy("column_name", "value").agg(
            *[F.sum(F.col(f"m{i}")).alias(f"n{i}") for i in range(len(ids))]
        )
        top = F.col("n0")
        for i in range(1, len(ids)):
            top = F.greatest(top, F.col(f"n{i}"))
        counted = counted.filter(top >= CATEGORY_MIN_COUNT)
        keep = F.lit(False)
        for i in range(len(ids)):
            order = Window.partitionBy("column_name").orderBy(
                F.col(f"n{i}").desc(), F.col("value")
            )
            counted = counted.withColumn(f"r{i}", F.row_number().over(order))
            keep = keep | (
                (F.col(f"n{i}") >= CATEGORY_MIN_COUNT)
                & (F.col(f"r{i}") <= CATEGORY_MAX_LEVELS)
            )
        for r in counted.filter(keep).collect():
            for i, j in enumerate(ids):
                n, rank = r[f"n{i}"], r[f"r{i}"]
                if n >= CATEGORY_MIN_COUNT and rank <= CATEGORY_MAX_LEVELS:
                    levels[j][r["column_name"]].append(r["value"])
    return {j: {c: sorted(v) for c, v in by.items()} for j, by in levels.items()}


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

# DBTITLE 1,Record the fitted parameters and the null classes


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
    df = load_partitioned(assembled)
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

    fits = fit_memberships(fold_mode, calendar_key, date_col)
    medians, n_rows = fit_medians(df, numeric, group_cols, fits)
    levels = fit_levels(df, categorical, fits)
    for j in fits:
        params = {"numeric": medians[j], "categorical": levels[j]}
        _record(dataset_id, ecosystem, j, params, numeric, categorical, n_rows[j])
    final = {"numeric": medians[-1], "categorical": levels[-1]}

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


CAP_SEED = "row_cap"


def cap_rows(frame: DataFrame, grain: list[str], max_rows: int | None) -> DataFrame:
    """About `max_rows` rows of a frame that holds more: the rows whose grain falls in
    the first whole percent of the stable buckets (`user_hash_bucket`, its own seed, so
    the sample does not follow the split hash). The same keys give the same rows on
    every build. A frame with fewer rows, or a spec with no `max_rows`, is returned as
    it is."""
    if not max_rows:
        return frame
    total = frame.count()
    if total <= max_rows:
        return frame
    limit = max(1, int(max_rows / total * 100))
    print(f"capped {total} rows to about {max_rows} (buckets 0 to {limit - 1} of 100)")
    return frame.filter(user_hash_bucket(grain, CAP_SEED) < limit)


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
        label_derived_like=tuple(spec.get("label_derived_like", ())),
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
    computed once and written with its partition, fold and group columns (stage 1),
    the contract, the split manifest and the fit read those rows, and the final rows
    replace them in the family table."""
    spec = dataset_spec(dataset_id)
    eco = spec["ecosystem"]
    component = f"ml/{eco}/{dataset_id}"
    label_inputs = spec.get("contract", {}).get("label_inputs", [])
    grain = list(spec["grain"])
    frame = cap_rows(frame, grain, spec.get("max_rows"))
    columns = frame.columns
    write_dataset_rows(
        add_ml_provenance(partition_frame(frame, dataset_id), dataset_id, eco, rid),
        dataset_id,
        source=source,
        component=component + "/assembled",
        rid=rid,
    )
    assembled = read_dataset(dataset_id).select(*columns, *PARTITION_COLUMNS)
    assert_unique_grain(assembled, grain, component=component, source=source, rid=rid)
    assert_no_forbidden_columns(
        assembled.drop(*label_inputs), component=component, source=source, rid=rid
    )
    defaulted = declare_dataset(dataset_id, columns)
    if defaulted:
        print(f"WARN {dataset_id}: columns given the default clock:", defaulted)
    write_dataset_partitions(assembled, dataset_id)
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
    `blocks` holds extra findings blocks per dataset (the dropped target rows). The
    spec is checked against every frame first; one problem stops the run before any
    write. The family of each dataset is reopened once, so a rerun after a freeze
    works; the gate freezes the family again at its next Delta version."""
    problems = [
        p for name, frame in frames.items() for p in spec_problems(name, frame.columns)
    ]
    if problems:
        detail = "; ".join(problems)
        raise RuntimeError(f"spec and frames disagree, nothing written: {detail}")
    by_family = {}
    for dataset_id in frames:
        spec = dataset_spec(dataset_id)
        by_family.setdefault((spec["ecosystem"], spec["family"]), dataset_id)
    for dataset_id in by_family.values():
        reopen_family(dataset_id)
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
