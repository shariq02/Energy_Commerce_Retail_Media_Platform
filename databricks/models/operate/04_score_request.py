# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SCORE REQUEST
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** score a window of one task with the registered selected model and
# MAGIC write one row per scored row, with a request id, to `scoring_requests`. A request
# MAGIC carries keys and a window, never feature values: the rows are read from the
# MAGIC frozen dataset and prepared as the evaluation does. It is a replay of stored
# MAGIC data in batch; no endpoint is involved. Without keys and a window, the default request
# MAGIC scores the newest day (at most 100 rows) of the task in `task_id`, or of every
# MAGIC tabular task when `task_id` is empty. Widgets, all
# MAGIC optional: `task_id`, `keys` (a JSON object of key columns and values),
# MAGIC `window_start` and `window_end` (dates, only for a task with a time column),
# MAGIC `request_id` (empty gives a new one). Run on the compute type of the registered
# MAGIC model.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Model metrics library
# MAGIC %run ../lib/_model_metrics

# COMMAND ----------

# DBTITLE 1,Tabular model library
# MAGIC %run ../lib/_fit_tabular

# COMMAND ----------

# DBTITLE 1,Foundation time-series library
# MAGIC %run ../lib/_fit_foundation_ts

# COMMAND ----------

# DBTITLE 1,Foundation tabular library
# MAGIC %run ../lib/_fit_foundation_tab

# COMMAND ----------

# DBTITLE 1,Evaluation shared library
# MAGIC %run ../lib/_eval_common

# COMMAND ----------

# DBTITLE 1,Tabular evaluation library
# MAGIC %run ../lib/_eval_tabular

# COMMAND ----------

# DBTITLE 1,Evaluation task specifications
# MAGIC %run ../lib/_eval_specs

# COMMAND ----------

# DBTITLE 1,Registry library
# MAGIC %run ../lib/_registry_rules

# COMMAND ----------

# DBTITLE 1,Operate rules library
# MAGIC %run ../lib/_operate_rules

# COMMAND ----------

# DBTITLE 1,Operate windows library
# MAGIC %run ../lib/_operate_windows

# COMMAND ----------

# DBTITLE 1,Operate requests library
# MAGIC %run ../lib/_operate_requests

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/operate/04_score_request"
SOURCE = "models"

# COMMAND ----------

# DBTITLE 1,Widgets
for _name in ("task_id", "keys", "window_start", "window_end", "request_id"):
    dbutils.widgets.text(_name, "")
TASK_ID = dbutils.widgets.get("task_id").strip()
KEYS = json.loads(dbutils.widgets.get("keys").strip() or "{}")
WINDOW_START = dbutils.widgets.get("window_start").strip()
WINDOW_END = dbutils.widgets.get("window_end").strip()
REQUEST_ID = dbutils.widgets.get("request_id").strip()

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Score the request
if TASK_ID and (KEYS or WINDOW_START or WINDOW_END):
    results = [score_request(TASK_ID, KEYS, WINDOW_START, WINDOW_END, REQUEST_ID, rid)]
    failed = []
elif TASK_ID:
    results = [score_default_request(TASK_ID, rid, REQUEST_ID)]
    failed = []
else:
    results, failed = score_default_requests(rid)

# COMMAND ----------

# DBTITLE 1,Every request produced rows and the id is new
_bad = [
    (r["task_id"], r["rows"], r["stored"])
    for r in results
    if r["rows"] == 0 or r["stored"] != r["rows"]
]
check(
    COMPONENT,
    SOURCE,
    "score_request_wrote_rows",
    not _bad and not failed,
    detail=f"task, rows, stored: {_bad}; failed: {failed}",
    metric_value=float(len(_bad) + len(failed)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write the findings of this notebook
write_model_findings(
    tuple(sorted({r["ecosystem"] for r in results})) or REQUEST_ECOSYSTEMS,
    "operate__04_score_request",
    "Score Request",
    ["scoring_requests"],
    rid,
    task_ids=[r["task_id"] for r in results],
)
