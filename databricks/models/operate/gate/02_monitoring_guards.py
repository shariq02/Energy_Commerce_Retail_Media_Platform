# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MONITORING GUARDS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** check the monitoring record. Hard failures: a registered model without results
# MAGIC in both windows or without a performance result, a result level that is not
# MAGIC valid, a threshold that differs from the recorded setting, flag rows that do not
# MAGIC match the results, and a flag without its trigger. The number of flags is reported;
# MAGIC a flag never fails a guard.

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
COMPONENT = "models/operate/gate/02_monitoring_guards"
SOURCE = "models"
TRIGGER_KINDS = (
    "performance_degradation",
    "drift_flag",
    "new_frozen_dataset_version",
)

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Read the registry, the results, the flags and the triggers
record = {}
for eco in ("energy", "commerce"):
    spec_rows = read_model("monitoring_spec", ecosystem=eco).collect()
    record[eco] = (
        operate_entries(eco),
        [r.asDict() for r in read_model("monitoring_results", ecosystem=eco).collect()],
        [r.asDict() for r in read_model("monitoring_flags", ecosystem=eco).collect()],
        [
            r.asDict()
            for r in read_model("monitoring_triggers", ecosystem=eco).collect()
        ],
        operate_spec_from_rows([r.asDict() for r in spec_rows]),
    )
    print(eco, [len(x) for x in record[eco][:4]])

# COMMAND ----------

# DBTITLE 1,Every registered model has results in both windows
_gaps = []
for eco, (entries, results, _f, _t, _spec) in record.items():
    for e in entries:
        have = {
            r["window_id"]
            for r in results
            if (r["task_id"], r["model_name"]) == (e["task_id"], e["model_name"])
            and r["check_kind"] != "performance"
        }
        missing = [w for w in MONITOR_WINDOWS if w not in have]
        if missing:
            _gaps.append((eco, e["task_id"], missing))
check(
    COMPONENT,
    SOURCE,
    "every_registered_model_monitored_in_both_windows",
    not _gaps,
    detail=f"task, missing windows: {_gaps}",
    metric_value=float(len(_gaps)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Every registered model has a performance result
_none = []
for eco, (entries, results, _f, _t, _spec) in record.items():
    done = {
        (r["task_id"], r["model_name"])
        for r in results
        if r["check_kind"] == "performance"
    }
    _none += [
        (eco, e["task_id"])
        for e in entries
        if (e["task_id"], e["model_name"]) not in done
    ]
check(
    COMPONENT,
    SOURCE,
    "every_registered_model_has_a_performance_result",
    not _none,
    detail=f"no performance result: {_none}",
    metric_value=float(len(_none)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Result levels are valid
_bad = [
    (eco, r["task_id"], r["check_kind"], r["level"])
    for eco, (_e, results, _f, _t, _spec) in record.items()
    for r in results
    if r["level"] not in LEVELS
]
check(
    COMPONENT,
    SOURCE,
    "result_levels_valid",
    not _bad,
    detail=f"invalid levels: {_bad[:10]}",
    metric_value=float(len(_bad)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Thresholds equal the recorded settings
_off = []
for eco, (_e, results, _f, _t, spec) in record.items():
    for r in results:
        if r["check_kind"].endswith("drift"):
            want = (spec["psi_warning"], spec["psi_flag"])
            got = (r["warning_threshold"], r["flag_threshold"])
        elif r["check_kind"].endswith("null_rate"):
            want, got = (
                (None, spec["null_rate_increase_pp"]),
                (None, r["flag_threshold"]),
            )
        else:
            continue
        if got != want and r["level"] != "not_applicable":
            _off.append((eco, r["task_id"], r["check_kind"], got))
check(
    COMPONENT,
    SOURCE,
    "thresholds_equal_the_recorded_settings",
    not _off,
    detail=f"eco, task, check, thresholds: {_off[:10]}",
    metric_value=float(len(_off)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Flag rows match the warning and flag results
_mismatch = []
for eco, (_e, results, flags, _t, _spec) in record.items():
    want = sum(1 for r in results if r["level"] in ("warning", "flag"))
    if want != len(flags):
        _mismatch.append((eco, want, len(flags)))
check(
    COMPONENT,
    SOURCE,
    "flag_rows_match_the_results",
    not _mismatch,
    detail=f"eco, results with a level, flag rows: {_mismatch}",
    metric_value=float(len(_mismatch)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,A flag has its trigger and a trigger is of a known kind
_open = []
for eco, (entries, results, _f, triggers, _spec) in record.items():
    kinds = {}
    for t in triggers:
        kinds.setdefault((t["task_id"], t["model_name"]), set()).add(t["trigger_kind"])
    for e in entries:
        key = (e["task_id"], e["model_name"])
        mine = [r for r in results if (r["task_id"], r["model_name"]) == key]
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
        for t in triggers
        if t["trigger_kind"] not in TRIGGER_KINDS
    ]
check(
    COMPONENT,
    SOURCE,
    "every_flag_has_its_trigger",
    not _open,
    detail=f"eco, task, trigger: {_open}",
    metric_value=float(len(_open)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Flags and triggers reported
for eco, (_e, results, flags, triggers, _spec) in record.items():
    levels = {lv: sum(1 for r in results if r["level"] == lv) for lv in LEVELS}
    print(eco, levels, f"{len(triggers)} trigger(s)")
