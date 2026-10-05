# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # REGISTRY CHECK LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** the reproducibility check of the registered models. Pulled in with
# MAGIC `%run ../lib/_registry_check` after the model, evaluation and registry
# MAGIC libraries. Each registered model is reloaded from its stored run and scored on
# MAGIC the validation rows again with the evaluation code. The result is compared
# MAGIC with the recorded validation value. No later partition is read, nothing is
# MAGIC refitted.

# COMMAND ----------

# DBTITLE 1,Check context and the validation frame


class ReproContext(EvalContext):
    """One task scored on the validation partition."""

    def __init__(self, task_id: str, rid: str):
        super().__init__(task_id, rid, smoke=False)
        self.partition = "validation"
        self.component = f"models/register/{task_id}"


def validation_frame(ctx: ReproContext) -> DataFrame:
    """The validation rows of the task; only the earlier partitions are kept."""
    frame = task_frame(ctx, ctx.partition)
    return frame.filter(F.col("partition").isin(*ALLOWED_PARTITIONS))


# COMMAND ----------

# DBTITLE 1,One check row


def check_row(entry: dict, env: dict, rid: str, now, **fields) -> dict:
    row = {
        "task_id": entry["task_id"],
        "model_name": entry["model_name"],
        "role": entry["role"],
        "registry_version": int(entry["version"]),
        **env,
        "reload_status": None,
        "reload_detail": None,
        "rescore_status": None,
        "recorded_value": entry["validation_value"],
        "rescored_value": None,
        "gap_relative": None,
        "tolerance": None,
        "status": CHECK_FAILED,
        "detail": None,
        "run_id": rid,
        "checked_at": now,
    }
    return {**row, **fields}


def _fail_text(exc: Exception) -> str:
    return f"{type(exc).__name__}: {str(exc)[:300]}"


# COMMAND ----------

# DBTITLE 1,Check the models of one task


def _score_entry(evaluator, ctx, bundles, entry, make) -> dict:
    """Score one reloaded model on the validation rows and compare."""
    if not evaluator.can_reproduce:
        return make(
            entry,
            reload_status="ok",
            rescore_status="not_comparable",
            status=check_status("ok", "not_comparable"),
            detail="events are drawn again, the validation value cannot be reproduced",
        )
    try:
        sc = evaluator.score(bundles[entry["model_name"]], "validation", bundles)
        value = sc.metrics.get(ctx.primary_metric)
        sc.release()
    except Exception as exc:
        return make(
            entry,
            reload_status="ok",
            rescore_status="score_failed",
            detail=_fail_text(exc),
        )
    tolerance = ctx.thresholds["reproduction_tolerance_relative"]
    status, gap = reproduction_outcome(entry["validation_value"], value, tolerance)
    return make(
        entry,
        reload_status="ok",
        rescore_status=status,
        rescored_value=_finite(value),
        gap_relative=gap,
        status=check_status("ok", status),
        detail=f"{ctx.primary_metric} on validation",
    )


def reproduce_task(task_id: str, entries: list, rid: str) -> list:
    """One check row per registered entry of the task."""
    env, now = environment_record(), now_utc()
    ctx = ReproContext(task_id, rid)
    tolerance = ctx.thresholds["reproduction_tolerance_relative"]

    def make(entry, **fields):
        return check_row(entry, env, rid, now, tolerance=tolerance, **fields)

    cfg = EVAL_TASKS[task_id]
    evaluator = globals()[EVALUATOR_NAMES[cfg["kind"]]](ctx, cfg)
    models = forwarded_models(ctx)
    by_name = {m["model_name"]: m for m in models}
    base_name = next(m["model_name"] for m in models if m["rank"] == 0)
    bundles, errors, rows = {}, {}, []
    try:
        if evaluator.can_reproduce:
            frame = validation_frame(ctx)
            evaluator.load(frame, frame, row_fraction_of(models))
            evaluator.recorded = {n: m["validation"] for n, m in by_name.items()}
        wanted = {base_name, *(e["model_name"] for e in entries)}
        if evaluator.needs_all_bundles:
            wanted |= set(by_name)
        for name in sorted(wanted):
            try:
                bundles[name] = load_bundle(by_name[name])
            except Exception as exc:
                errors[name] = _fail_text(exc)
        if evaluator.can_reproduce and base_name in bundles:
            evaluator.attach(bundles, base_name)
        for entry in entries:
            name = entry["model_name"]
            if name in errors:
                rows.append(
                    make(
                        entry,
                        reload_status="failed",
                        reload_detail=errors[name],
                        rescore_status="not_scored",
                        detail="the stored model did not reload",
                    )
                )
            else:
                rows.append(_score_entry(evaluator, ctx, bundles, entry, make))
    finally:
        evaluator.frames = {"eval": None, "validation": None}
        evaluator.sampler = None
        bundles.clear()
        del evaluator
        _take_notes()
        _gc.collect()
    return rows


# COMMAND ----------

# DBTITLE 1,Check every registered entry of one ecosystem


def reproduce_ecosystem(ecosystem: str, rid: str) -> list:
    """Check rows for the latest version of every entry that is not retired. A task
    that fails as a whole gets a failed row for each of its entries."""
    latest = latest_versions(registry_rows(ecosystem))
    by_task: dict = {}
    for entry in latest.values():
        if entry["lifecycle_status"] != "retired":
            by_task.setdefault(entry["task_id"], []).append(entry)
    env, rows = environment_record(), []
    for task in (t for t in TASKS if t["ecosystem"] == ecosystem):
        tid = task["task_id"]
        if tid not in by_task:
            continue
        try:
            got = reproduce_task(tid, by_task[tid], rid)
        except Exception as exc:
            got = [
                check_row(
                    e,
                    env,
                    rid,
                    now_utc(),
                    reload_status="failed",
                    reload_detail=_fail_text(exc),
                    rescore_status="not_scored",
                    detail="the task could not be checked",
                )
                for e in by_task[tid]
            ]
        for r in got:
            mark = "OK  " if r["status"] == CHECK_PASSED else "FAIL"
            print(
                f"{mark} {tid} {r['model_name']} ({r['role']}): reload "
                f"{r['reload_status']}, re-score {r['rescore_status']}, "
                f"gap {r['gap_relative']}"
            )
        rows += got
    return rows


def record_checks(ecosystem: str, rows: list) -> None:
    """Replace the check rows of this processor type; rows of other processor types
    stay."""
    if not rows:
        return
    cols = read_model("registry_check", ecosystem=ecosystem).columns
    tuples = [tuple(r[c] for c in cols) for r in rows]
    replace_model_rows(
        spark.createDataFrame(tuples, MODEL_DDL["registry_check"]),
        "registry_check",
        ecosystem=ecosystem,
        predicate=f"processor_type = '{rows[0]['processor_type']}'",
    )
