# Databricks notebook source
# /// script
# [tool.databricks.environment]
# base_environment = "databricks_ai_v5"
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
# MAGIC data in batch; no endpoint is involved. Widgets: `task_id`, `keys` (a JSON object
# MAGIC of key columns and values), `window_start` and `window_end` (dates, may be empty
# MAGIC for a task without a time column), `request_id` (empty gives a new one). Run on
# MAGIC the compute type of the registered model.

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

# DBTITLE 1,Imports
import datetime as _datetime
import math as _math_request
import uuid as _uuid

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
REQUEST_ID = dbutils.widgets.get("request_id").strip() or _uuid.uuid4().hex[:12]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,The request names keys and a window only
cfg = EVAL_TASKS.get(TASK_ID)
if cfg is None or cfg["kind"] != "tabular":
    raise ValueError(f"{TASK_ID!r} is not a task that this request can score")
if not isinstance(KEYS, dict) or not set(KEYS) <= set(key_columns(cfg)):
    raise ValueError(f"keys must be key columns {key_columns(cfg)}, not {sorted(KEYS)}")
TIME_COL = cfg.get("time_col")
if not KEYS and not (WINDOW_START or WINDOW_END):
    raise ValueError("give keys or a window")
if (WINDOW_START or WINDOW_END) and not TIME_COL:
    raise ValueError(f"{TASK_ID} has no time column, so a window cannot be used")
START = _datetime.date.fromisoformat(WINDOW_START) if WINDOW_START else None
END = _datetime.date.fromisoformat(WINDOW_END) if WINDOW_END else None
print(f"request {REQUEST_ID}: {TASK_ID}, keys {KEYS}, window {START} to {END}")

# COMMAND ----------

# DBTITLE 1,The registered selected model of the task
ECOSYSTEM = TASK_BY_ID[TASK_ID]["ecosystem"]
_entries = sorted(
    (e for e in operate_entries(ECOSYSTEM) if e["task_id"] == TASK_ID),
    key=lambda e: str(e["registered_at"]),
)
if not _entries:
    raise RuntimeError(f"{TASK_ID}: no registered selected model")
entry = _entries[-1]
print(f"model {entry['model_name']} version {entry['version']}")

# COMMAND ----------

# DBTITLE 1,Load the stored model
bundle = load_bundle({"mlflow_run_id": entry["mlflow_run_id"]})

# COMMAND ----------

# DBTITLE 1,Days of history a forecast needs before the window


def lookback_days(task_id: str) -> int:
    """A time-series model needs its context and gap before the first requested
    row; the other models need none."""
    ts = TS_TASKS.get(task_id)
    if ts is None:
        return 0
    seconds = ts["seconds"] or 3600
    return _math_request.ceil(ts["context"] * seconds / 86400) + ts["gap_days"] + 1


# COMMAND ----------

# DBTITLE 1,Read the rows of the request from the frozen dataset
ctx = MonitorContext(TASK_ID, rid, "held_out")
if int(entry["frozen_delta_version"]) != int(ctx.frozen_version):
    raise RuntimeError(
        f"registered at version {entry['frozen_delta_version']}, "
        f"frozen dataset is at {ctx.frozen_version}"
    )
df = read_partitions(ctx, None)
if cfg.get("history"):
    df = complete_load_features(df, ctx)
for _col, _value in KEYS.items():
    df = df.filter(F.col(_col).cast("string") == str(_value))
if TIME_COL and START:
    _first = START - _datetime.timedelta(days=lookback_days(TASK_ID))
    df = df.filter(F.to_date(F.col(TIME_COL)) >= F.lit(_first.isoformat()))
if TIME_COL and END:
    df = df.filter(F.to_date(F.col(TIME_COL)) <= F.lit(END.isoformat()))

# COMMAND ----------

# DBTITLE 1,Prepare the rows as the evaluation does
evaluator = TabularEvaluator(ctx, cfg)
evaluator.load(df, None, 1.0)
frame = evaluator.frames["eval"].reset_index(drop=True)
_take_notes()
print(f"OK  {len(frame)} row(s) read")

# COMMAND ----------

# DBTITLE 1,Score the rows
raw = bundle.predict(frame)
if isinstance(raw, dict):
    median = np.asarray(raw[0.5], dtype="float64")
    low = np.asarray(raw[0.1], dtype="float64")
    high = np.asarray(raw[0.9], dtype="float64")
else:
    median = np.asarray(raw, dtype="float64")
    low = high = np.full(len(median), np.nan)

# COMMAND ----------

# DBTITLE 1,Keep the rows of the window and build the result rows
keep = np.ones(len(frame), dtype=bool)
if TIME_COL:
    _day = pd.to_datetime(frame[TIME_COL], errors="coerce").dt.date
    if START:
        keep &= (_day >= START).to_numpy()
    if END:
        keep &= (_day <= END).to_numpy()
_keys = row_keys(cfg, frame)
_now = now_utc()


def _num(value):
    return None if not np.isfinite(value) else float(value)


request_rows = [
    (
        REQUEST_ID,
        TASK_ID,
        entry["model_name"],
        int(entry["version"]),
        json.dumps(KEYS, sort_keys=True),
        WINDOW_START,
        WINDOW_END,
        _keys[i],
        _num(median[i]),
        _num(low[i]),
        _num(high[i]),
        int(entry["frozen_delta_version"]),
        rid,
        _now,
    )
    for i in np.flatnonzero(keep)
]
print(f"{len(request_rows)} row(s) in the window")

# COMMAND ----------

# DBTITLE 1,Write the result rows
_table = model_fqn(FOUNDATION_REQUESTS, ECOSYSTEM)
spark.sql(
    f"CREATE TABLE IF NOT EXISTS {_table} ({FOUNDATION_DDL[FOUNDATION_REQUESTS]}) "
    "USING delta"
)
(
    spark.createDataFrame(request_rows, FOUNDATION_DDL[FOUNDATION_REQUESTS])
    .write.format("delta")
    .mode("append")
    .saveAsTable(_table)
)
print(f"OK  request {REQUEST_ID} written to {_table}")

# COMMAND ----------

# DBTITLE 1,The request produced rows and the id is new
_existing = spark.table(_table).filter(F.col("request_id") == REQUEST_ID).count()
check(
    COMPONENT,
    SOURCE,
    "score_request_wrote_rows",
    len(request_rows) > 0 and _existing == len(request_rows),
    detail=f"request {REQUEST_ID}: {len(request_rows)} row(s), {_existing} in the table",
    metric_value=float(len(request_rows)),
    rid=rid,
)
