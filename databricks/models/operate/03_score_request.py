# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SCORE REQUEST
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** score a window of one task with the registered selected model and
# MAGIC write one row per scored row, with a request id, to `scoring_requests`. A request
# MAGIC carries keys and a window, never feature values: the rows are read from the
# MAGIC frozen dataset and prepared as the evaluation does. It is a replay of stored
# MAGIC data in batch; no endpoint is involved. Without keys and a window, the default
# MAGIC request scores the newest day (at most 100 rows) of the task in `task_id`, or of
# MAGIC every tabular task of the energy and commerce ecosystems when `task_id` is empty.
# MAGIC Widgets, all optional: `task_id`, `keys` (a JSON object of key columns and
# MAGIC values), `window_start` and `window_end` (dates, only for a task with a time
# MAGIC column), `request_id` (empty gives a new one). Run on the compute type of the
# MAGIC registered model.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Model fit library
# MAGIC %run ../lib/_model_fit

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

# DBTITLE 1,Configuration
COMPONENT = "models/operate/03_score_request"

# COMMAND ----------

# DBTITLE 1,Widgets
import json

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
guard(
    COMPONENT,
    rid,
    "score_request_wrote_rows",
    [
        (r["task_id"], r["rows"], r["stored"])
        for r in results
        if r["rows"] == 0 or r["stored"] != r["rows"]
    ]
    + list(failed),
    "task, rows, stored; failed",
)

# COMMAND ----------

# DBTITLE 1,Write the findings of this notebook
write_model_findings(
    tuple(sorted({r["ecosystem"] for r in results})) or REQUEST_ECOSYSTEMS,
    "operate__03_score_request",
    "Score Request",
    ["scoring_requests"],
    rid,
    task_ids=[r["task_id"] for r in results],
)
