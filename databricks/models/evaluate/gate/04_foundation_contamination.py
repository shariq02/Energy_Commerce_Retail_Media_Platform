# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FOUNDATION CONTAMINATION CHECK
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** for every foundation candidate forwarded to the held-out evaluation,
# MAGIC record whether its pretraining data may overlap the evaluated data. The text of
# MAGIC the pretraining data in the model register is compared with the data names of the
# MAGIC task; the evaluated time range comes from the evaluation run. A flag is never a
# MAGIC rejection. A flag stays `unresolved` until the owner has read the model card and
# MAGIC names the model and task in the widget `cleared` (`model_key:task_id`, separated
# MAGIC by commas); a cleared flag is kept while the revision stays the same.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Foundation time-series library
# MAGIC %run ../../lib/_fit_foundation_ts

# COMMAND ----------

# DBTITLE 1,Imports
import ast as _ast

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/evaluate/gate/04_foundation_contamination"
SOURCE = "models"
ECOSYSTEMS = ("energy", "commerce")
EVAL_CONTEXT = "evaluation_run_context"
# words that name the data of a task in a model card
DATA_TERMS = {
    "price": ("smard", "entso", "bundesnetzagentur", "day-ahead", "electricity"),
    "load": ("smard", "entso", "bundesnetzagentur", "electricity"),
    "bias": ("smard", "entso", "bundesnetzagentur", "electricity"),
    "zone_generation": ("mastr", "smard", "entso", "generation"),
    "honda": ("honda",),
    "ccpp": ("combined cycle", "uci", "power plant"),
    "redispatch": ("redispatch", "entso", "bundesnetzagentur"),
    "capacity_additions": ("mastr", "bundesnetzagentur"),
    "ga4": ("ga4", "google analytics", "merchandise"),
    "rees46": ("rees46", "ecommerce", "e-commerce"),
    "survival": ("mastr", "bundesnetzagentur", "wind turbine"),
    "matching": ("redispatch", "bundesnetzagentur", "entso"),
    "weather": ("dwd", "deutscher wetterdienst", "weather station", "era5"),
    "pumped": ("smard", "entso", "pumped"),
}

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("cleared", "")
CLEARED = {
    item.strip() for item in dbutils.widgets.get("cleared").split(",") if item.strip()
}

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Terms of a task


def task_terms(task_id: str) -> tuple:
    found = []
    for key, terms in DATA_TERMS.items():
        if key in task_id or key in TASK_BY_ID[task_id]["dataset_id"]:
            found += terms
    return tuple(dict.fromkeys(found))


def read_window(context_rows) -> str:
    """The evaluated time range from the data notes of the latest evaluation read."""
    ready = [r for r in context_rows if r["status"] == "data_ready" and r["data_notes"]]
    if not ready:
        return "not recorded"
    last = max(ready, key=lambda r: str(r["recorded_at"]))
    for note in _json.loads(last["data_notes"]):
        read = note.get("read")
        if read and "time_min" in read:
            return f"{read['time_min']} to {read['time_max']}"
    return "no time column"


# COMMAND ----------

# DBTITLE 1,Read the forwarded foundation models, the register and the evaluation reads
record = {}
for eco in ECOSYSTEMS:
    if not spark.catalog.tableExists(model_fqn(FOUNDATION_SELECTION, eco)):
        continue
    selected = [
        r.asDict() for r in read_model(FOUNDATION_SELECTION, ecosystem=eco).collect()
    ]
    params = {
        (r["task_id"], r["model_name"]): _ast.literal_eval(r["params"] or "{}")
        for r in read_model(FOUNDATION_RESULTS, ecosystem=eco)
        .filter(~F.col("smoke") & (F.col("status") == "ok"))
        .select("task_id", "model_name", "params")
        .distinct()
        .collect()
    }
    register = {
        r["model_key"]: r.asDict()
        for r in read_model(FOUNDATION_REGISTER, ecosystem=eco).collect()
    }
    context = [
        r.asDict()
        for r in read_model(EVAL_CONTEXT, ecosystem=eco)
        .filter(~F.col("smoke"))
        .select("task_id", "status", "data_notes", "recorded_at")
        .collect()
    ]
    earlier = (
        {
            (r["model_key"], r["task_id"]): r.asDict()
            for r in read_model(FOUNDATION_CONTAMINATION, ecosystem=eco).collect()
        }
        if spark.catalog.tableExists(model_fqn(FOUNDATION_CONTAMINATION, eco))
        else {}
    )
    record[eco] = (selected, params, register, context, earlier)
    print(f"{eco}: {len(selected)} forwarded foundation model(s)")

# COMMAND ----------

# DBTITLE 1,Decide the flag of each forwarded model and task
now = now_utc()
rows = {eco: [] for eco in record}
for eco, (selected, params, register, context, earlier) in record.items():
    for s in selected:
        if not str(s.get("family") or "").startswith("foundation_"):
            continue
        tid = s["task_id"]
        info = params.get((tid, s["model_name"]), {})
        key = info.get("model_key")
        entry = register.get(key, {})
        revision = str(entry.get("revision") or info.get("revision") or "")
        text = str(entry.get("pretraining_data") or "")
        hits = [t for t in task_terms(tid) if t in text.lower()]
        window = read_window([c for c in context if c["task_id"] == tid])
        kept = earlier.get((key, tid))
        keep_cleared = bool(
            kept
            and revision
            and kept["flag"] == "cleared"
            and f"revision {revision[:12]};" in (kept["overlap_note"] or "")
        )
        if f"{key}:{tid}" in CLEARED or keep_cleared:
            flag = "cleared"
        elif hits:
            flag = "overlap_named"
        else:
            flag = "unresolved"
        note = (
            f"revision {revision[:12]}; pretraining data per register: {text[:160]}; "
            f"task data names found: {hits or 'none'}"
        )
        rows[eco].append((key, tid, s["dataset_id"], window, note, flag, rid, now))
        print(f"{flag:14s} {tid} / {key}: window {window}")

# COMMAND ----------

# DBTITLE 1,Write the contamination record
for eco, items in rows.items():
    replace_model_rows(
        spark.createDataFrame(items, FOUNDATION_DDL[FOUNDATION_CONTAMINATION]),
        FOUNDATION_CONTAMINATION,
        ecosystem=eco,
        predicate="task_id IS NOT NULL",
    )
    print(f"OK  {len(items)} row(s) for {eco}")

# COMMAND ----------

# DBTITLE 1,Every forwarded foundation model has a recorded flag
_without = [
    (r[1], r[0]) for items in rows.values() for r in items if not r[0] or not r[5]
]
check(
    COMPONENT,
    SOURCE,
    "every_forwarded_foundation_model_has_a_flag",
    not _without,
    detail=f"no model key or flag: {_without}",
    metric_value=float(len(_without)),
    rid=rid,
)
