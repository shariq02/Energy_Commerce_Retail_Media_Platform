# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EVALUATION SHARED LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** plumbing shared by the evaluation notebooks, pulled in with
# MAGIC `%run ../_eval_common` after `_model_common` and `_model_metrics`. Definitions
# MAGIC only. Reads the held-out partition, loads the stored models, scores them exactly
# MAGIC as stored, and records results, intervals, breakdowns and run context.

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Configuration constants
TEST_PARTITION = "test"
EVAL_SPEC_DATASET = "evaluation"
EVAL_CONTEXT_TABLE = "evaluation_run_context"
EVAL_SEED = MODEL_SEED + 1
BOOTSTRAP_MAX_ROWS = 100_000
SEGMENT_MAX_LEVELS = 30
EVAL_SPEC_DEFAULTS = {
    "scoring_rule": "models scored as stored, no refit, no re-selection",
    "bootstrap_resamples": "200",
    "bootstrap_seed": str(EVAL_SEED),
    "interval_flag": "skill interval includes zero",
    "degradation_flag_relative": "0.2",
    "min_bootstrap_blocks": "5",
    "reproduction_tolerance_relative": "1e-06",
    "prediction_range_share_flag": "0.1",
    "min_segment_rows": "30",
    "injection_seed": str(EVAL_SEED),
    "reconstruction_event_seed": str(EVAL_SEED),
}

# COMMAND ----------

# DBTITLE 1,Thresholds recorded before the first read


def read_thresholds(ecosystem: str) -> dict:
    """The recorded evaluation settings; a missing key means the setup did not run."""
    rows = (
        read_model("evaluation_spec", ecosystem=ecosystem)
        .filter(F.col("dataset_id") == EVAL_SPEC_DATASET)
        .collect()
    )
    spec = {r["spec_key"]: r["spec_value"] for r in rows}
    missing = [k for k in EVAL_SPEC_DEFAULTS if k not in spec]
    if missing:
        raise RuntimeError(
            f"evaluation settings not recorded for {ecosystem}: {missing}; "
            "run evaluate/00_evaluation_setup first"
        )
    tolerance = float(spec["reproduction_tolerance_relative"])
    return {
        "bootstrap_resamples": int(spec["bootstrap_resamples"]),
        "bootstrap_seed": int(spec["bootstrap_seed"]),
        "degradation_flag_relative": float(spec["degradation_flag_relative"]),
        "min_bootstrap_blocks": int(spec["min_bootstrap_blocks"]),
        "reproduction_tolerance_relative": tolerance,
        "prediction_range_share_flag": float(spec["prediction_range_share_flag"]),
        "min_segment_rows": int(spec["min_segment_rows"]),
        "injection_seed": int(spec["injection_seed"]),
        "reconstruction_event_seed": int(spec["reconstruction_event_seed"]),
    }


# COMMAND ----------

# DBTITLE 1,Evaluation context and the partition reads


class EvalContext(TaskContext):
    """One task evaluated on the held-out partition. A smoke run scores the
    validation partition instead, so the held-out rows are not touched."""

    def __init__(self, task_id: str, rid: str, smoke: bool = False):
        super().__init__(task_id, rid, smoke, record=False)
        self.partition = "validation" if smoke else TEST_PARTITION
        self.component = f"models/evaluate/{task_id}"
        self.thresholds = read_thresholds(self.ecosystem)


def read_partitions(ctx: EvalContext, partitions) -> DataFrame:
    """The dataset at its frozen Delta version, restricted to the given partitions
    (None keeps every row)."""
    allowed = {*ALLOWED_PARTITIONS, ctx.partition}
    if partitions is not None and not set(partitions) <= allowed:
        raise RuntimeError(f"{ctx.task_id} reads {sorted(allowed)}, not {partitions}")
    full = ml_fqn(f"dataset_{ctx.dataset_id}", ctx.ecosystem)
    df = spark.sql(f"SELECT * FROM {full} VERSION AS OF {ctx.frozen_version}")
    return df if partitions is None else df.filter(F.col("partition").isin(*partitions))


def attach_manifest_partition(df: DataFrame, ctx: EvalContext, key_cols) -> DataFrame:
    """Replace partition, fold and group by the evaluation split manifest; every
    partition is kept."""
    m = (
        read_model("evaluation_split_manifest", ecosystem=ctx.ecosystem)
        .filter(F.col("dataset_id") == ctx.dataset_id)
        .select("grain_key", "partition", "fold_id", "group_key")
    )
    base = df.drop("partition", "fold_id", "group_key").withColumn(
        "_grain_key", grain_key(*key_cols)
    )
    return base.join(m, base["_grain_key"] == m["grain_key"], "inner").drop(
        "grain_key", "_grain_key"
    )


def note_read(ctx: EvalContext, frame, time_col=None, group_col=None) -> None:
    """Remember what was read: partition, version, rows, time range, group count."""
    note = {
        "partition": ctx.partition,
        "frozen_delta_version": int(ctx.frozen_version),
        "rows": len(frame),
    }
    if time_col and time_col in frame.columns:
        t = pd.to_datetime(frame[time_col], errors="coerce")
        note["time_min"], note["time_max"] = str(t.min()), str(t.max())
    if group_col and group_col in frame.columns:
        note["groups"] = int(frame[group_col].nunique())
    _FRAME_NOTES.append({"read": note})


# COMMAND ----------

# DBTITLE 1,Forwarded models and their stored artifacts


def _selection_rows(ctx: EvalContext) -> list[dict]:
    sel = [
        r.asDict()
        for r in read_model("candidate_selection", ecosystem=ctx.ecosystem)
        .filter(F.col("task_id") == ctx.task_id)
        .collect()
    ]
    if not sel:
        raise RuntimeError(f"{ctx.task_id}: no models forwarded by the selection")
    wrong = [
        s["model_name"]
        for s in sel
        if int(s["frozen_delta_version"]) != ctx.frozen_version
    ]
    if wrong:
        raise RuntimeError(f"{ctx.task_id}: selected at another version: {wrong}")
    return sel


def forwarded_models(ctx: EvalContext) -> list[dict]:
    """The selected models of the task (baseline first), each with its stored run id
    and recorded validation metrics, plus any other baseline kept for its metrics."""
    sel = _selection_rows(ctx)
    rows = (
        read_model("candidate_results", ecosystem=ctx.ecosystem)
        .filter(
            (F.col("task_id") == ctx.task_id)
            & ~F.col("smoke")
            & (F.col("status") == "ok")
        )
        .select(
            "model_name",
            "family",
            "stage",
            "metric",
            "value",
            "mlflow_run_id",
            "params",
        )
        .collect()
    )
    by: dict = {}
    for r in rows:
        d = by.setdefault(
            r["model_name"],
            {
                "model_name": r["model_name"],
                "family": r["family"],
                "stage": r["stage"],
                "mlflow_run_id": r["mlflow_run_id"],
                "params": r["params"],
                "validation": {},
            },
        )
        if r["metric"] and r["value"] is not None:
            d["validation"][r["metric"]] = r["value"]
    rank = {s["model_name"]: int(s["rank"]) for s in sel}
    missing = [n for n in rank if n not in by]
    if missing:
        raise RuntimeError(f"{ctx.task_id}: forwarded without a result: {missing}")
    out = []
    for name, d in by.items():
        if name in rank:
            d["rank"] = rank[name]
        elif d["stage"] == "baseline":
            d["rank"] = 99
        else:
            continue
        out.append(d)
    return sorted(out, key=lambda d: (d["rank"], d["model_name"]))


def row_fraction_of(models) -> float:
    """The row fraction the validation frame was built with (recorded in params)."""
    for m in models:
        found = _re.search(r"'row_fraction': ([0-9.]+)", m.get("params") or "")
        if found:
            return float(found.group(1))
    return 1.0


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

# DBTITLE 1,Grouped and blocked resampling


def block_labels(frame, rule: dict) -> np.ndarray:
    """Block label per row: a calendar month, a stored group, or the row itself."""
    col = rule.get("col")
    if rule["by"] == "month" and col in frame.columns:
        t = pd.to_datetime(frame[col], errors="coerce")
        return t.dt.strftime("%Y-%m").fillna("none").to_numpy(dtype=object)
    if rule["by"] == "group" and col in frame.columns:
        return frame[col].astype(str).to_numpy(dtype=object)
    return np.arange(len(frame)).astype(str).astype(object)


class BlockSampler:
    """Draws whole blocks with replacement and returns the row positions. Above
    max_rows a seeded subset of blocks is used and the figures say so."""

    def __init__(self, labels, max_rows: int, seed: int):
        codes, uniques = pd.factorize(np.asarray(labels, dtype=object))
        n = len(codes)
        self.order = np.argsort(codes, kind="stable")
        self.counts = np.bincount(codes, minlength=len(uniques))
        self.starts = np.cumsum(self.counts) - self.counts
        ids = np.arange(len(uniques))
        if n > max_rows and len(uniques) > 1:
            keep = np.random.default_rng(seed).random(len(uniques)) < max_rows / n
            if keep.sum() >= 2:
                ids = np.flatnonzero(keep)
        self.ids = ids
        self.rows_total = int(n)
        self.rows_used = int(self.counts[ids].sum())
        self.n_blocks = len(ids)

    def draw(self, rng) -> np.ndarray:
        pick = self.ids[rng.integers(0, len(self.ids), len(self.ids))]
        lens = self.counts[pick]
        offsets = np.cumsum(lens) - lens
        pos = (
            np.arange(int(lens.sum()))
            - np.repeat(offsets, lens)
            + np.repeat(self.starts[pick], lens)
        )
        return self.order[pos]


def bootstrap_interval(stat_fn, resamples: int, seed: int):
    """(2.5th, 97.5th percentile, valid resamples) of stat_fn(rng)."""
    rng = np.random.default_rng(seed)
    vals = np.array([stat_fn(rng) for _ in range(resamples)], dtype="float64")
    vals = vals[np.isfinite(vals)]
    if len(vals) < max(10, resamples // 2):
        return float("nan"), float("nan"), len(vals)
    low, high = np.percentile(vals, [2.5, 97.5])
    return float(low), float(high), len(vals)


def orient_skill(model_value, base_value, higher: bool) -> float:
    """Positive means better than the baseline: the difference for scores where
    higher is better, the relative error reduction otherwise."""
    if model_value is None or base_value is None:
        return float("nan")
    if higher:
        return float(model_value - base_value)
    return skill(float(model_value), float(base_value))


def degradation(test_value, validation_value, higher: bool) -> float:
    """Relative worsening from validation to the evaluated partition; positive
    means worse."""
    if test_value is None or validation_value is None:
        return float("nan")
    t, v = float(test_value), float(validation_value)
    if not (np.isfinite(t) and np.isfinite(v)) or v == 0:
        return float("nan")
    return ((v - t) if higher else (t - v)) / abs(v)


# COMMAND ----------

# DBTITLE 1,Prediction sanity and breakdowns


def prediction_sanity(pred, target) -> dict:
    """Finite share, spread and the share of predictions outside the target range."""
    if pred is None:
        return {}
    p = np.asarray(pred, dtype="float64")
    finite = np.isfinite(p)
    out = {"pred_finite_share": float(finite.mean()) if len(p) else float("nan")}
    if finite.any():
        out["pred_std"] = float(p[finite].std())
    if target is not None and finite.any():
        t = np.asarray(target, dtype="float64")
        t = t[np.isfinite(t)]
        if len(t):
            outside = (p[finite] < t.min()) | (p[finite] > t.max())
            out["pred_outside_range_share"] = float(outside.mean())
    return out


def breakdown(frame, row_stat, name, columns, time_col, min_rows) -> dict:
    """The primary metric by level of each segment column and by calendar month."""
    out: dict = {}
    if row_stat is None or frame is None:
        return out
    groupings = [
        (c, frame[c].astype(str).to_numpy(dtype=object))
        for c in columns
        if c in frame.columns
    ]
    if time_col and time_col in frame.columns:
        month = pd.to_datetime(frame[time_col], errors="coerce").dt.strftime("%Y-%m")
        groupings.append(("month", month.fillna("none").to_numpy(dtype=object)))
    for c, labels in groupings:
        levels = pd.Series(labels).value_counts().head(SEGMENT_MAX_LEVELS).index
        for v in sorted(levels):
            idx = np.flatnonzero(labels == v)
            if len(idx) >= min_rows:
                out[f"{name}__{c}__{v}"] = row_stat(idx)
    return out


# COMMAND ----------

# DBTITLE 1,Scored model and the evaluator base class


class Scored:
    """One model scored on one frame: metrics plus what the checks need."""

    def __init__(self, metrics, n_rows, row_stat=None, pred=None, target=None):
        self.metrics = metrics
        self.n_rows = int(n_rows)
        self.row_stat = row_stat
        self.pred = pred
        self.target = target


class Evaluator:
    """Per-paradigm scoring. A subclass reads and converts its frames in load(),
    inspects the stored models in attach(), and returns Scored from score()."""

    can_reproduce = True

    def __init__(self, ctx: EvalContext, cfg: dict):
        self.ctx, self.cfg = ctx, cfg
        self.spec = cfg["spec"]
        self.frames: dict = {"eval": None, "validation": None}
        self.sampler = None
        self.base_name = None
        self.recorded: dict = {}

    def attach(self, bundles: dict, base_name: str) -> None:
        self.base_name = base_name

    @staticmethod
    def name_of(bundle, bundles: dict) -> str:
        return next(n for n, b in bundles.items() if b is bundle)

    def frame(self, part: str):
        return self.frames["eval" if part == "eval" else "validation"]

    def extra_columns(self, df) -> list:
        """Columns the checks need beyond the model inputs."""
        wanted = [
            self.cfg["blocks"].get("col"),
            self.cfg.get("time_col"),
            "group_key",
            *self.cfg.get("segments", []),
        ]
        return [c for c in wanted if c and c in df.columns]

    def block_sampler(self) -> BlockSampler:
        if self.sampler is None:
            labels = block_labels(self.frames["eval"], self.cfg["blocks"])
            self.sampler = BlockSampler(
                labels,
                self.cfg.get("bootstrap_max_rows", BOOTSTRAP_MAX_ROWS),
                self.ctx.thresholds["bootstrap_seed"],
            )
        return self.sampler

    def can_interval(self, sc: Scored) -> bool:
        return sc.row_stat is not None

    def interval(self, sc: Scored, base: Scored):
        """(low, high, valid resamples, blocks, rows used) of the skill difference
        against the baseline on resampled blocks."""
        sampler = self.block_sampler()
        higher = self.ctx.higher_is_better

        def stat(rng):
            rows = sampler.draw(rng)
            return orient_skill(sc.row_stat(rows), base.row_stat(rows), higher)

        low, high, n = bootstrap_interval(
            stat,
            self.ctx.thresholds["bootstrap_resamples"],
            self.ctx.thresholds["bootstrap_seed"],
        )
        return low, high, n, sampler.n_blocks, sampler.rows_used

    def segments(self, sc: Scored) -> dict:
        return breakdown(
            self.frames["eval"],
            sc.row_stat,
            self.ctx.primary_metric,
            self.cfg.get("segments", []),
            self.cfg.get("time_col"),
            self.ctx.thresholds["min_segment_rows"],
        )


# COMMAND ----------

# DBTITLE 1,Result recording


def start_evaluation(ctx: EvalContext) -> None:
    """Drop this task's earlier rows of the same kind, then note the data read."""
    spark.sql(
        f"DELETE FROM {model_fqn('evaluation_results', ctx.ecosystem)} "
        f"WHERE task_id = '{ctx.task_id}' AND smoke = {str(ctx.smoke).lower()}"
    )
    print(f"task {ctx.task_id} on {ctx.partition}, frozen version {ctx.frozen_version}")
    record_task_context(
        ctx, "data_ready", data_notes=_take_notes(), table=EVAL_CONTEXT_TABLE
    )


def record_evaluation(ctx: EvalContext, model, status, metrics, n_rows, detail) -> None:
    now = now_utc()
    base = (
        ctx.dataset_id,
        ctx.task_id,
        model["model_name"],
        model["family"],
        model["stage"],
        ctx.partition,
    )
    tail = (
        None if n_rows is None else int(n_rows),
        status,
        detail[:900] if detail else None,
        int(ctx.frozen_version),
        model["mlflow_run_id"],
        ctx.smoke,
        ctx.rid,
        now,
    )
    items = list(metrics.items()) if status == "ok" else [(None, None)]
    rows = [
        (
            *base,
            m,
            None if v is None else _finite(v),
            None if m is None else _finite(model["validation"].get(m)),
            *tail,
        )
        for m, v in items
    ]
    spark.createDataFrame(rows, MODEL_DDL["evaluation_results"]).write.format(
        "delta"
    ).mode("append").saveAsTable(model_fqn("evaluation_results", ctx.ecosystem))


# COMMAND ----------

# DBTITLE 1,One model: metrics, change from validation, interval, reproduction


def enrich(ctx, evaluator, model, bundle, sc, base_sc, bundles) -> dict:
    """The scored metrics plus the post-training checks of one model."""
    primary, higher = ctx.primary_metric, ctx.higher_is_better
    metrics = dict(sc.metrics)
    value = metrics.get(primary)
    recorded = model["validation"].get(primary)
    metrics["primary_change_relative"] = degradation(value, recorded, higher)
    is_reference = model["model_name"] == evaluator.base_name
    if value is not None and base_sc is not None and not is_reference:
        metrics["skill_primary"] = orient_skill(
            value, base_sc.metrics.get(primary), higher
        )
        if evaluator.can_interval(sc):
            low, high, n, blocks, rows = evaluator.interval(sc, base_sc)
            metrics["skill_primary_ci_low"] = low
            metrics["skill_primary_ci_high"] = high
            metrics["bootstrap_resamples_valid"] = float(n)
            metrics["bootstrap_blocks"] = float(blocks)
            metrics["bootstrap_rows_used"] = float(rows)
    metrics.update(evaluator.segments(sc))
    metrics.update(prediction_sanity(sc.pred, sc.target))
    if evaluator.can_reproduce and evaluator.frame("validation") is not None:
        again = evaluator.score(bundle, "validation", bundles).metrics.get(primary)
        if again is not None and recorded is not None and np.isfinite(recorded):
            gap = abs(again - recorded) / max(abs(recorded), 1e-12)
            metrics["reproduced_validation_value"] = again
            metrics["reproduction_gap_relative"] = gap
    return metrics


def check_partition_integrity(ctx: EvalContext, evaluator: Evaluator) -> None:
    """The evaluated partition lies after validation in time and shares no group
    with it. Skipped in a smoke run, where both are the validation partition."""
    held, earlier = evaluator.frames["eval"], evaluator.frames["validation"]
    if ctx.smoke or held is None or earlier is None:
        return
    time_col = evaluator.cfg.get("time_col")
    if time_col and time_col in held.columns and time_col in earlier.columns:
        first = pd.to_datetime(held[time_col], errors="coerce").min()
        last = pd.to_datetime(earlier[time_col], errors="coerce").max()
        check(
            ctx.component,
            "models",
            "evaluated_partition_after_validation",
            bool(first > last),
            detail=f"evaluated from {first}, validation until {last}",
            rid=ctx.rid,
        )
    if (
        evaluator.cfg["blocks"]["by"] == "group"
        and "group_key" in held.columns
        and "group_key" in earlier.columns
    ):
        shared = set(held["group_key"].astype(str)) & set(
            earlier["group_key"].astype(str)
        )
        check(
            ctx.component,
            "models",
            "evaluated_partition_shares_no_group",
            not shared,
            detail=f"groups in both partitions: {len(shared)}",
            metric_value=float(len(shared)),
            rid=ctx.rid,
        )


def evaluate_task(ctx: EvalContext, evaluator: Evaluator) -> None:
    """Score every forwarded model of the task once and record the results. The
    evaluator has already read its frames."""
    record_task_context(ctx, "started", table=EVAL_CONTEXT_TABLE)
    check_partition_integrity(ctx, evaluator)
    models = forwarded_models(ctx)
    base_name = next(m["model_name"] for m in models if m["rank"] == 0)
    bundles, errors = {}, {}
    for m in models:
        try:
            bundles[m["model_name"]] = load_bundle(m)
        except Exception as exc:
            errors[m["model_name"]] = f"{type(exc).__name__}: {str(exc)[:300]}"
    evaluator.recorded = {m["model_name"]: m["validation"] for m in models}
    evaluator.attach(bundles, base_name)
    start_evaluation(ctx)
    scored: dict = {}
    for m in models:
        name = m["model_name"]
        if name in errors:
            record_evaluation(ctx, m, "failed", {}, None, errors[name])
            print(f"FAIL {name}: {errors[name]}")
            if name == base_name:
                raise RuntimeError(f"{ctx.task_id}: the baseline could not be loaded")
            continue
        try:
            sc = evaluator.score(bundles[name], "eval", bundles)
            scored[name] = sc
            base_sc = scored.get(base_name)
            metrics = enrich(ctx, evaluator, m, bundles[name], sc, base_sc, bundles)
            record_evaluation(ctx, m, "ok", metrics, sc.n_rows, f"on {ctx.partition}")
            shown = {
                k: round(v, 4)
                for k, v in metrics.items()
                if "__" not in k and _finite(v) is not None
            }
            print(f"OK   {name}: {shown}")
        except Exception as exc:
            detail = f"{type(exc).__name__}: {str(exc)[:600]}"
            record_evaluation(ctx, m, "failed", {}, None, detail)
            print(f"FAIL {name}: {detail}")
            if name == base_name:
                raise
        _gc.collect()
    ok = base_name in scored
    record_task_context(
        ctx, "finished", detail=f"baseline_ok={ok}", table=EVAL_CONTEXT_TABLE
    )
    check(
        ctx.component,
        "models",
        "baseline_evaluated",
        ok,
        detail=f"task={ctx.task_id}",
        rid=ctx.rid,
    )


# COMMAND ----------

# DBTITLE 1,Flags raised from the recorded results


def _flag(flags, key, flag, detail, value=None, limit=None) -> None:
    flags.append((*key, flag, detail, value, limit))


def evaluation_flag_rows(results, thresholds: dict) -> list:
    """(task, model, stage, flag, detail, value, threshold) for results a reader
    should look at first. Diagnostic only; nothing here rejects a model."""
    by: dict = {}
    for r in results:
        by.setdefault((r["task_id"], r["model_name"]), []).append(r)
    flags: list = []
    for (task, model), rows in by.items():
        stage = rows[0]["stage"]
        key = (task, model, stage)
        if rows[0]["status"] != "ok":
            _flag(flags, key, "model not evaluated", rows[0]["detail"])
            continue
        v = {
            r["metric"]: r["value"]
            for r in rows
            if r["metric"] and r["value"] is not None
        }
        cut = thresholds["degradation_flag_relative"]
        change = v.get("primary_change_relative")
        if change is not None and change > cut:
            detail = f"primary metric {change:+.1%} worse than on validation"
            name = "validation-to-test degradation above the threshold"
            _flag(flags, key, name, detail, change, cut)
        if stage == "candidate" and "skill_primary" in v:
            low, high = v.get("skill_primary_ci_low"), v.get("skill_primary_ci_high")
            if low is None:
                _flag(flags, key, "skill interval not estimable", "too few resamples")
            elif low <= 0:
                detail = f"interval {low:.4g} to {high:.4g}"
                _flag(flags, key, "skill interval includes zero", detail, low, 0.0)
            if v["skill_primary"] <= 0:
                detail = f"skill {v['skill_primary']:.4g}"
                name = "no skill over the baseline"
                _flag(flags, key, name, detail, v["skill_primary"], 0.0)
        blocks = v.get("bootstrap_blocks")
        floor = float(thresholds["min_bootstrap_blocks"])
        if blocks is not None and blocks < floor:
            _flag(flags, key, "few resampling blocks", f"{blocks:.0f}", blocks, floor)
        finite = v.get("pred_finite_share")
        if finite is not None and finite < 1.0:
            detail = f"finite share {finite:.4g}"
            _flag(flags, key, "non-finite predictions", detail, finite, 1.0)
        if stage == "candidate" and v.get("pred_std") == 0.0:
            _flag(flags, key, "constant predictions", "prediction spread is zero")
        outside = v.get("pred_outside_range_share")
        limit = thresholds["prediction_range_share_flag"]
        if outside is not None and outside > limit:
            name = "predictions outside the target range"
            _flag(flags, key, name, f"share {outside:.4g}", outside, limit)
        gap = v.get("reproduction_gap_relative")
        tol = thresholds["reproduction_tolerance_relative"]
        if gap is not None and gap > tol:
            name = "stored model does not reproduce its validation result"
            _flag(flags, key, name, f"relative gap {gap:.3g}", gap, tol)
    return flags


def flag_dicts(rows) -> list:
    """Flag tuples as the dicts the findings text expects."""
    names = ("task_id", "model_name", "stage", "flag", "detail", "value", "threshold")
    return [dict(zip(names, row, strict=True)) for row in rows]


# COMMAND ----------

# DBTITLE 1,Findings markdown (pure text, no Spark)


def _task_models(results, task_id):
    models: dict = {}
    for r in results:
        if r["task_id"] == task_id:
            models.setdefault(r["model_name"], []).append(r)
    return models


def _read_lines(notes) -> list:
    lines = []
    for n in notes:
        if "read" in n:
            shown = ", ".join(f"{k} {v}" for k, v in n["read"].items())
            lines.append(f"- read: {shown}")
        elif "rows" in n:
            lines.append(
                f"- frame: {n['rows']} rows x {n.get('columns')} columns, sample "
                f"fraction {n.get('sample_fraction')}, highest null rates "
                f"{n.get('highest_null_rates') or 'none'}"
            )
    return lines


def _model_row(model, rs, primary):
    v = {r["metric"]: r for r in rs if r["metric"]}

    def pick(name, field="value"):
        return _number(v[name][field]) if name in v else ""

    return (
        model,
        rs[0]["stage"],
        rs[0]["status"],
        pick(primary, "validation_value"),
        pick(primary),
        pick("primary_change_relative"),
        pick("skill_primary"),
        pick("skill_primary_ci_low"),
        pick("skill_primary_ci_high"),
        rs[0]["detail"] if rs[0]["status"] != "ok" else "",
    )


def _task_section(t, models, reads) -> list:
    first = next(iter(models.values()))[0]
    out = [
        f"## {t['task_id']}",
        "",
        (
            f"_dataset `{t['dataset_id']}`, frozen version "
            f"{first['frozen_delta_version']}, partition `{first['partition']}`, "
            f"primary metric `{t['primary_metric']}` "
            f"({'higher' if t['higher_is_better'] else 'lower'} is better)_"
        ),
        "",
    ]
    for c in reversed(reads.get(t["task_id"], [])):
        if c["status"] == "data_ready" and c.get("data_notes"):
            try:
                lines = _read_lines(_json.loads(c["data_notes"]))
            except ValueError:
                lines = []
            if lines:
                out += ["### data seen", "", *lines, ""]
            break
    order = sorted(models, key=lambda m: (models[m][0]["stage"] != "baseline", m))
    heads = ["model", "stage", "status", "validation", "evaluated", "change"]
    heads += ["skill", "ci low", "ci high", "detail"]
    table = [_model_row(m, models[m], t["primary_metric"]) for m in order]
    out += ["### models", "", markdown_table(heads, table), ""]
    ok = [m for m in order if models[m][0]["status"] == "ok"]
    names = list(
        dict.fromkeys(r["metric"] for m in ok for r in models[m] if r["metric"])
    )
    for title, wanted in (
        ("metrics (evaluated partition)", [n for n in names if "__" not in n]),
        ("breakdowns (evaluated partition)", [n for n in names if "__" in n]),
    ):
        if not (ok and wanted):
            continue
        grid = []
        for name in wanted:
            row = [name]
            for m in ok:
                x = next((r["value"] for r in models[m] if r["metric"] == name), None)
                row.append(_number(x))
            grid.append(row)
        out += [f"### {title}", "", markdown_table(["metric", *ok], grid), ""]
    return out


def render_evaluation_findings(
    ecosystem, results, flags, context, checks, spec, *, smoke, stamp
) -> str:
    """One markdown document: settings, run context, flags, check results, then per
    task the validation and evaluated-partition values side by side and every
    metric of every model."""
    tasks = [t for t in TASKS if t["ecosystem"] == ecosystem]
    kind = "EVALUATION SMOKE RUN" if smoke else "EVALUATION"
    out = [
        f"# {ecosystem.upper()} {kind} FINDINGS",
        "",
        (
            "_Auto-generated by `databricks/models/evaluate/gate/03_export_findings`. "
            f"Models are scored as stored. Generated: {stamp}_"
        ),
        "",
        "## settings",
        "",
        markdown_table(
            ["setting", "value", "recorded"],
            [
                (s["spec_key"], s["spec_value"], str(s["recorded_at"])[:19])
                for s in spec
            ],
        ),
        "",
    ]
    summary = []
    for t in tasks:
        models = _task_models(results, t["task_id"])
        n_ok = sum(rs[0]["status"] == "ok" for rs in models.values())
        n_flags = sum(f["task_id"] == t["task_id"] for f in flags)
        summary.append((t["task_id"], len(models), n_ok, len(models) - n_ok, n_flags))
    out += [
        "## summary",
        "",
        markdown_table(["task", "models", "evaluated", "failed", "flags"], summary),
        "",
        "## run context",
        "",
    ]
    reads: dict = {}
    for c in sorted(context, key=lambda c: str(c["recorded_at"])):
        if bool(c["smoke"]) == smoke:
            reads.setdefault(c["task_id"], []).append(c)
    run_rows = []
    for t in tasks:
        rs = reads.get(t["task_id"], [])
        if rs:
            last = [c for c in rs if c["run_id"] == rs[-1]["run_id"]]
            run_rows.append(
                (
                    t["task_id"],
                    last[-1]["run_id"],
                    " > ".join(c["status"] for c in last),
                    len({c["run_id"] for c in rs}),
                    _repo_relative(last[-1]["notebook_path"]),
                    str(last[0]["recorded_at"])[:19],
                    str(last[-1]["recorded_at"])[:19],
                )
            )
    if run_rows:
        heads = ["task", "run id", "steps", "runs", "notebook", "first", "last"]
        out += [markdown_table(heads, run_rows), ""]
    else:
        out += ["_no run context recorded for this mode_", ""]
    out += ["## flags", ""]
    flag_rows = [
        (
            f["task_id"],
            f["model_name"],
            f["flag"],
            f["detail"],
            _number(f["value"]),
            _number(f["threshold"]),
        )
        for f in flags
    ]
    if flag_rows:
        heads = ["task", "model", "flag", "detail", "value", "threshold"]
        out += [markdown_table(heads, flag_rows), ""]
    else:
        out += ["_no flags raised_", ""]
    if checks:
        latest = {}
        for c in sorted(checks, key=lambda c: str(c["recorded_at"])):
            latest[(c["component"], c["metric_name"])] = c
        rows = [
            (
                c["component"],
                c["metric_name"],
                c["status"],
                c["error_detail"],
                str(c["recorded_at"])[:19],
            )
            for c in latest.values()
        ]
        heads = ["component", "check", "status", "detail", "recorded"]
        out += ["## check results", "", markdown_table(heads, rows), ""]
    for t in tasks:
        models = _task_models(results, t["task_id"])
        if models:
            out += _task_section(t, models, reads)
        else:
            out += [f"## {t['task_id']}", "", "_no evaluation recorded_", ""]
    return scrub_private("\n".join(out))
