# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # OPERATE GUARDS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** check the reference and prediction record. Hard failures: a registered model
# MAGIC without a reference profile at its registered version, a reference recorded after
# MAGIC a window was scored, a model without predictions in every window, a prediction at
# MAGIC another registry or frozen version, a row for a model that is not registered, and
# MAGIC more stored rows than the limit.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Registry library
# MAGIC %run ../../lib/_registry_rules

# COMMAND ----------

# DBTITLE 1,Operate rules library
# MAGIC %run ../../lib/_operate_rules

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/operate/gate/01_operate_guards"
SOURCE = "models"
ALL_WINDOWS = (REFERENCE_WINDOW, *MONITOR_WINDOWS)

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Read the registry, the reference profile and the prediction record
record = {}
for eco in ("energy", "commerce"):
    entries = operate_entries(eco)
    selected = {
        (r["task_id"], r["model_name"])
        for r in registry_rows(eco)
        if r["role"] == "selected"
    }
    reference = [
        r.asDict() for r in read_model("reference_profile", ecosystem=eco).collect()
    ]
    predictions = [
        r.asDict()
        for r in read_model("model_predictions", ecosystem=eco)
        .groupBy(
            "task_id",
            "model_name",
            "registry_version",
            "window_id",
            "frozen_delta_version",
        )
        .agg(F.count("*").alias("rows"), F.min("scored_at").alias("first_scored"))
        .collect()
    ]
    spec_rows = read_model("monitoring_spec", ecosystem=eco).collect()
    spec = operate_spec_from_rows([r.asDict() for r in spec_rows])
    spec_at = min((r["recorded_at"] for r in spec_rows), default=None)
    record[eco] = (entries, selected, reference, predictions, spec, spec_at)
    print(
        f"{eco}: {len(entries)} registered model(s), {len(reference)} reference row(s)"
    )

# COMMAND ----------

# DBTITLE 1,Every registered model has a reference profile at its registered version
_none = []
for eco, (entries, _s, reference, _p, _spec, _at) in record.items():
    for e in entries:
        mine = [
            r
            for r in reference
            if (r["task_id"], r["model_name"], r["registry_version"])
            == (e["task_id"], e["model_name"], e["version"])
            and r["frozen_delta_version"] == e["frozen_delta_version"]
        ]
        if PREDICTION_SUBJECT not in {r["subject"] for r in mine}:
            _none.append((eco, e["task_id"]))
check(
    COMPONENT,
    SOURCE,
    "every_registered_model_has_a_reference_profile",
    not _none,
    detail=f"no reference profile: {_none}",
    metric_value=float(len(_none)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Settings and reference were recorded before any window was scored
_late = []
for eco, (entries, _s, reference, predictions, _spec, spec_at) in record.items():
    first_reference = min((r["recorded_at"] for r in reference), default=None)
    if spec_at is None or (first_reference is not None and spec_at > first_reference):
        _late.append((eco, "settings after the reference"))
    for e in entries:
        key = (e["task_id"], e["model_name"])
        recorded = max(
            (
                r["recorded_at"]
                for r in reference
                if (r["task_id"], r["model_name"]) == key
            ),
            default=None,
        )
        scored = [
            p["first_scored"]
            for p in predictions
            if (p["task_id"], p["model_name"]) == key
            and p["window_id"] in MONITOR_WINDOWS
        ]
        if recorded is not None and scored and recorded > min(scored):
            _late.append((eco, e["task_id"], "reference after a window"))
check(
    COMPONENT,
    SOURCE,
    "reference_recorded_before_any_window_was_scored",
    not _late,
    detail=f"recorded late: {_late}",
    metric_value=float(len(_late)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Every registered model has predictions in every window
_gaps = []
for eco, (entries, _s, _r, predictions, _spec, _at) in record.items():
    for e in entries:
        have = {
            p["window_id"]
            for p in predictions
            if (p["task_id"], p["model_name"]) == (e["task_id"], e["model_name"])
            and p["rows"] > 0
        }
        missing = [w for w in ALL_WINDOWS if w not in have]
        if missing:
            _gaps.append((eco, e["task_id"], missing))
check(
    COMPONENT,
    SOURCE,
    "every_registered_model_scored_in_every_window",
    not _gaps,
    detail=f"task, missing windows: {_gaps}",
    metric_value=float(len(_gaps)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Predictions are at the registered version
_moved = []
for eco, (entries, _s, _r, predictions, _spec, _at) in record.items():
    current = {(e["task_id"], e["model_name"]): e for e in entries}
    for p in predictions:
        e = current.get((p["task_id"], p["model_name"]))
        if e is None:
            continue
        same = (p["registry_version"], p["frozen_delta_version"]) == (
            e["version"],
            e["frozen_delta_version"],
        )
        if not same:
            _moved.append((eco, p["task_id"], p["window_id"]))
check(
    COMPONENT,
    SOURCE,
    "predictions_at_the_registered_and_frozen_version",
    not _moved,
    detail=f"task, window: {_moved}",
    metric_value=float(len(_moved)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Nothing is stored for a model that is not registered
_stray = []
for eco, (_e, selected, reference, predictions, _spec, _at) in record.items():
    seen = {(r["task_id"], r["model_name"]) for r in reference}
    seen |= {(p["task_id"], p["model_name"]) for p in predictions}
    _stray += [(eco, *k) for k in sorted(seen - selected)]
check(
    COMPONENT,
    SOURCE,
    "nothing_stored_for_an_unregistered_model",
    not _stray,
    detail=f"eco, task, model: {_stray}",
    metric_value=float(len(_stray)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,No window stores more rows than the limit
_over = [
    (eco, p["task_id"], p["window_id"], p["rows"])
    for eco, (_e, _s, _r, predictions, spec, _at) in record.items()
    for p in predictions
    if p["rows"] > spec["max_window_rows"]
]
check(
    COMPONENT,
    SOURCE,
    "no_window_over_the_row_limit",
    not _over,
    detail=f"eco, task, window, rows: {_over}",
    metric_value=float(len(_over)),
    rid=rid,
)
