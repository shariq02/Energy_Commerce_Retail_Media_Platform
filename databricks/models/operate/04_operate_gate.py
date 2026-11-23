# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # OPERATE GATE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** check the reference, prediction and monitoring record, then write the
# MAGIC monitoring record of each ecosystem to the repository as markdown. Hard
# MAGIC failures: a registered model without a reference profile at its registered
# MAGIC version, a reference recorded after a window was scored, a model without
# MAGIC predictions in every window, a prediction at another registry or frozen version,
# MAGIC a row in any operate table for a model that is not a registered, not retired
# MAGIC selected model, more stored rows than the limit, a registered model without
# MAGIC results in both windows or without a performance result, a result level that is
# MAGIC not valid, a threshold that differs from the recorded setting, flag rows that do
# MAGIC not match the results, and a flag without its trigger. The number of flags is
# MAGIC reported; a flag never fails a guard.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Model evaluation library
# MAGIC %run ../lib/_model_evaluate

# COMMAND ----------

# DBTITLE 1,Model governance library
# MAGIC %run ../lib/_model_govern

# COMMAND ----------

# DBTITLE 1,Model operate library
# MAGIC %run ../lib/_model_operate

# COMMAND ----------

# DBTITLE 1,Imports
import os as _os

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/operate/04_operate_gate"
ALL_WINDOWS = (REFERENCE_WINDOW, *MONITOR_WINDOWS)
TRIGGER_KINDS = (
    "performance_degradation",
    "drift_flag",
    "new_frozen_dataset_version",
)
FINDINGS_DIR = _findings_dir()
_os.makedirs(FINDINGS_DIR, exist_ok=True)

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Read the registry, the references, the predictions and the monitoring record
record = {}
for eco in ECOSYSTEMS:
    spec_rows = read_model("monitoring_spec", ecosystem=eco).collect()
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
    record[eco] = {
        "entries": operate_entries(eco),
        "reference": [
            r.asDict() for r in read_model("reference_profile", ecosystem=eco).collect()
        ],
        "predictions": predictions,
        "spec_rows": [r.asDict() for r in spec_rows],
        "spec": operate_spec_from_rows([r.asDict() for r in spec_rows]),
        "spec_at": min((r["recorded_at"] for r in spec_rows), default=None),
        "results": [
            r.asDict()
            for r in read_model("monitoring_results", ecosystem=eco).collect()
        ],
        "flags": [
            r.asDict() for r in read_model("monitoring_flags", ecosystem=eco).collect()
        ],
        "triggers": [
            r.asDict()
            for r in read_model("monitoring_triggers", ecosystem=eco).collect()
        ],
    }
    print(
        f"{eco}: {len(record[eco]['entries'])} registered model(s), "
        f"{len(record[eco]['reference'])} reference row(s)"
    )

# COMMAND ----------

# DBTITLE 1,Every registered model has a reference profile at its registered version
_none = []
for eco, rec in record.items():
    for e in rec["entries"]:
        mine = [
            r
            for r in rec["reference"]
            if (r["task_id"], r["model_name"], r["registry_version"])
            == (e["task_id"], e["model_name"], e["version"])
            and r["frozen_delta_version"] == e["frozen_delta_version"]
        ]
        if PREDICTION_SUBJECT not in {r["subject"] for r in mine}:
            _none.append((eco, e["task_id"]))
guard(
    COMPONENT,
    rid,
    "every_registered_model_has_a_reference_profile",
    _none,
    "no profile",
)

# COMMAND ----------

# DBTITLE 1,Settings and reference were recorded before any window was scored
_late = []
for eco, rec in record.items():
    first_reference = min((r["recorded_at"] for r in rec["reference"]), default=None)
    if rec["spec_at"] is None or (
        first_reference is not None and rec["spec_at"] > first_reference
    ):
        _late.append((eco, "settings after the reference"))
    for e in rec["entries"]:
        key = (e["task_id"], e["model_name"])
        recorded = max(
            (
                r["recorded_at"]
                for r in rec["reference"]
                if (r["task_id"], r["model_name"]) == key
            ),
            default=None,
        )
        scored = [
            p["first_scored"]
            for p in rec["predictions"]
            if (p["task_id"], p["model_name"]) == key
            and p["window_id"] in MONITOR_WINDOWS
        ]
        if recorded is not None and scored and recorded > min(scored):
            _late.append((eco, e["task_id"], "reference after a window"))
guard(COMPONENT, rid, "reference_recorded_before_any_window_was_scored", _late, "late")

# COMMAND ----------

# DBTITLE 1,Predictions: every window, the registered version, no stray rows, the limit
_gaps, _moved, _stray, _over = [], [], [], []
for eco, rec in record.items():
    current = {(e["task_id"], e["model_name"]): e for e in rec["entries"]}
    for key, e in current.items():
        have = {
            p["window_id"]
            for p in rec["predictions"]
            if (p["task_id"], p["model_name"]) == key and p["rows"] > 0
        }
        missing = [w for w in ALL_WINDOWS if w not in have]
        if missing:
            _gaps.append((eco, key[0], missing))
    for p in rec["predictions"]:
        e = current.get((p["task_id"], p["model_name"]))
        if e is not None and (p["registry_version"], p["frozen_delta_version"]) != (
            e["version"],
            e["frozen_delta_version"],
        ):
            _moved.append((eco, p["task_id"], p["window_id"]))
        if p["rows"] > rec["spec"]["max_window_rows"]:
            _over.append((eco, p["task_id"], p["window_id"], p["rows"]))
    _stray += [
        (eco, *k) for k in models_without_entry(stored_models(eco), rec["entries"])
    ]
guard(
    COMPONENT,
    rid,
    "every_registered_model_scored_in_every_window",
    _gaps,
    "task, windows",
)
guard(
    COMPONENT,
    rid,
    "predictions_at_the_registered_and_frozen_version",
    _moved,
    "task, window",
)
guard(
    COMPONENT,
    rid,
    "nothing_stored_for_an_unregistered_model",
    _stray,
    "eco, task, model",
)
guard(COMPONENT, rid, "no_window_over_the_row_limit", _over, "eco, task, window, rows")

# COMMAND ----------

# DBTITLE 1,Every registered model has results in both windows and a performance result
_gaps, _none = [], []
for eco, rec in record.items():
    done = {
        (r["task_id"], r["model_name"])
        for r in rec["results"]
        if r["check_kind"] == "performance"
    }
    for e in rec["entries"]:
        have = {
            r["window_id"]
            for r in rec["results"]
            if (r["task_id"], r["model_name"]) == (e["task_id"], e["model_name"])
            and r["check_kind"] != "performance"
        }
        missing = [w for w in MONITOR_WINDOWS if w not in have]
        if missing:
            _gaps.append((eco, e["task_id"], missing))
        if (e["task_id"], e["model_name"]) not in done:
            _none.append((eco, e["task_id"]))
guard(
    COMPONENT,
    rid,
    "every_registered_model_monitored_in_both_windows",
    _gaps,
    "task, missing windows",
)
guard(
    COMPONENT,
    rid,
    "every_registered_model_has_a_performance_result",
    _none,
    "no performance result",
)

# COMMAND ----------

# DBTITLE 1,Result levels, thresholds and flag rows
_bad, _off, _mismatch = [], [], []
for eco, rec in record.items():
    spec = rec["spec"]
    for r in rec["results"]:
        if r["level"] not in LEVELS:
            _bad.append((eco, r["task_id"], r["check_kind"], r["level"]))
        if r["check_kind"].endswith("drift"):
            want = (spec["psi_warning"], spec["psi_flag"])
            got = (r["warning_threshold"], r["flag_threshold"])
        elif r["check_kind"].endswith("null_rate"):
            want = (None, spec["null_rate_increase_pp"])
            got = (None, r["flag_threshold"])
        else:
            continue
        if got != want and r["level"] != "not_applicable":
            _off.append((eco, r["task_id"], r["check_kind"], got))
    want_flags = sum(1 for r in rec["results"] if r["level"] in ("warning", "flag"))
    if want_flags != len(rec["flags"]):
        _mismatch.append((eco, want_flags, len(rec["flags"])))
guard(COMPONENT, rid, "result_levels_valid", _bad, "invalid levels")
guard(
    COMPONENT,
    rid,
    "thresholds_equal_the_recorded_settings",
    _off,
    "eco, task, check, thresholds",
)
guard(
    COMPONENT,
    rid,
    "flag_rows_match_the_results",
    _mismatch,
    "eco, results with a level, flag rows",
)

# COMMAND ----------

# DBTITLE 1,A flag has its trigger and a trigger is of a known kind
_open = []
for eco, rec in record.items():
    kinds = {}
    for t in rec["triggers"]:
        kinds.setdefault((t["task_id"], t["model_name"]), set()).add(t["trigger_kind"])
    for e in rec["entries"]:
        key = (e["task_id"], e["model_name"])
        mine = [r for r in rec["results"] if (r["task_id"], r["model_name"]) == key]
        have = kinds.get(key, set())
        slow = any(
            r["check_kind"] == "performance" and r["level"] == "flag" for r in mine
        )
        if slow and "performance_degradation" not in have:
            _open.append((eco, e["task_id"], "performance_degradation"))
        drift = [
            r for r in mine if r["level"] == "flag" and r["check_kind"] in DRIFT_KINDS
        ]
        if drift and "drift_flag" not in have:
            _open.append((eco, e["task_id"], "drift_flag"))
    _open += [
        (eco, t["task_id"], t["trigger_kind"])
        for t in rec["triggers"]
        if t["trigger_kind"] not in TRIGGER_KINDS
    ]
guard(COMPONENT, rid, "every_flag_has_its_trigger", _open, "eco, task, trigger")
for eco, rec in record.items():
    levels = {lv: sum(1 for r in rec["results"] if r["level"] == lv) for lv in LEVELS}
    print(eco, levels, f"{len(rec['triggers'])} trigger(s)")

# COMMAND ----------

# DBTITLE 1,Write the monitoring findings


def _rows_per_window(predictions: list) -> list:
    """Stored rows per task, model and window, from the counts read once above."""
    total: dict = {}
    for p in predictions:
        key = (p["task_id"], p["model_name"], p["window_id"])
        total[key] = total.get(key, 0) + p["rows"]
    return [
        {"task_id": t, "model_name": m, "window_id": w, "rows": n}
        for (t, m, w), n in total.items()
    ]


checks = [
    r.asDict()
    for r in spark.table(AUDIT_TABLE)
    .filter((F.col("stage") == ML_STAGE) & F.col("component").like("models/operate/%"))
    .select("component", "metric_name", "status", "error_detail", "recorded_at")
    .collect()
]
checks = current_check_rows(checks, notebook_stems(("operate",)))
stamp = now_utc().strftime("%Y-%m-%dT%H:%MZ")
for eco, rec in record.items():
    data = {
        "spec": rec["spec_rows"],
        "reference": [
            {
                k: r[k]
                for k in (
                    "task_id",
                    "model_name",
                    "registry_version",
                    "frozen_delta_version",
                    "subject",
                    "n_rows",
                )
            }
            for r in rec["reference"]
        ],
        "predictions": _rows_per_window(rec["predictions"]),
        "results": rec["results"],
        "triggers": rec["triggers"],
        "checks": checks,
    }
    text = render_monitoring_findings(eco, data, stamp)
    with open(
        _os.path.join(FINDINGS_DIR, f"{eco}_monitoring.md"), "w", encoding="utf-8"
    ) as fh:
        fh.write(text + "\n")
    print(f"OK  {eco}_monitoring.md")

# COMMAND ----------

# DBTITLE 1,Merge the findings of the notebooks
merge_notebook_findings()
