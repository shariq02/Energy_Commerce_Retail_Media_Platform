# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # OPERATE REQUESTS LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** score the rows of a request with the registered selected model of a
# MAGIC tabular task. Pulled in with `%run ../lib/_operate_requests` after the operate
# MAGIC windows library. A request names a task, keys and a window, never feature values.
# MAGIC Without a task, the default request scores the newest days of every tabular task.

# COMMAND ----------

# DBTITLE 1,Imports
import datetime as _datetime
import math as _math_request
import time as _time_request
import uuid as _uuid

# COMMAND ----------

# DBTITLE 1,Default request settings
DEFAULT_WINDOW_DAYS = 1
DEFAULT_ROW_LIMIT = 100
REQUEST_LIMIT_SECONDS = 300
REQUEST_STACK_SECONDS = 60
REQUEST_ECOSYSTEMS = ("energy", "commerce")

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

# DBTITLE 1,Check what the request names


def check_request(
    task_id: str, keys, window_start: str, window_end: str, latest_days=None
):
    """The task settings, the time column and the window dates of a request."""
    cfg = EVAL_TASKS.get(task_id)
    if cfg is None or cfg["kind"] != "tabular":
        raise ValueError(f"{task_id!r} is not a task that this request can score")
    if not isinstance(keys, dict) or not set(keys) <= set(key_columns(cfg)):
        raise ValueError(
            f"keys must be key columns {key_columns(cfg)}, not {sorted(keys)}"
        )
    time_col = cfg.get("time_col")
    if not keys and not (window_start or window_end) and not latest_days:
        raise ValueError("give keys or a window")
    if (window_start or window_end) and not time_col:
        raise ValueError(f"{task_id} has no time column, so a window cannot be used")
    start = _datetime.date.fromisoformat(window_start) if window_start else None
    end = _datetime.date.fromisoformat(window_end) if window_end else None
    return cfg, time_col, start, end


# COMMAND ----------

# DBTITLE 1,The registered selected model of a task


def registered_entry(task_id: str):
    """The ecosystem and the newest registered selected entry of a task."""
    ecosystem = TASK_BY_ID[task_id]["ecosystem"]
    entries = sorted(
        (e for e in operate_entries(ecosystem) if e["task_id"] == task_id),
        key=lambda e: str(e["registered_at"]),
    )
    if not entries:
        raise RuntimeError(f"{task_id}: no registered selected model")
    return ecosystem, entries[-1]


# COMMAND ----------

# DBTITLE 1,A stable sample of the rows before the model scores them


def key_sample(df, key_cols: list, rows: int):
    """About `rows` rows chosen by a stable hash of the key columns."""
    if not key_cols or not set(key_cols) <= set(df.columns):
        return df.limit(rows)
    total = df.count()
    if total <= rows:
        return df
    bucket = F.pmod(
        F.xxhash64(*[F.col(c).cast("string") for c in key_cols]), F.lit(1_000_000)
    )
    return df.filter(bucket < _math_request.ceil(rows / total * 1_000_000))


# COMMAND ----------

# DBTITLE 1,Read the rows of the request from the frozen dataset


def request_frame(
    ctx, cfg, task_id, keys, time_col, start, end, latest_days, row_limit=None
):
    """The Spark frame of the request and its window. With `latest_days` and no
    window, the window is the newest days of the data. A task without a time column
    and without keys is sampled to `row_limit` rows before it is scored."""
    df = read_partitions(ctx, None)
    if cfg.get("history"):
        df = complete_load_features(df, ctx)
    for col, value in keys.items():
        df = df.filter(F.col(col).cast("string") == str(value))
    if row_limit and not time_col and not keys:
        df = key_sample(df, key_columns(cfg), row_limit)
    if latest_days and time_col and not (start or end):
        end = df.agg(F.max(F.to_date(F.col(time_col)))).first()[0]
        if end is not None:
            start = end - _datetime.timedelta(days=latest_days - 1)
    if time_col and start:
        first = start - _datetime.timedelta(days=lookback_days(task_id))
        df = df.filter(F.to_date(F.col(time_col)) >= F.lit(first.isoformat()))
    if time_col and end:
        df = df.filter(F.to_date(F.col(time_col)) <= F.lit(end.isoformat()))
    return df, start, end


# COMMAND ----------

# DBTITLE 1,Positions of the rows that belong to the request


def request_positions(frame, row_keys_list, time_col, start, end, row_limit):
    """Positions of the frame rows inside the window. With `row_limit` only the
    last rows are kept, in time order (key order without a time column)."""
    keep = np.ones(len(frame), dtype=bool)
    if time_col:
        day = pd.to_datetime(frame[time_col], errors="coerce").dt.date
        if start:
            keep &= (day >= start).to_numpy()
        if end:
            keep &= (day <= end).to_numpy()
    picked = np.flatnonzero(keep)
    if row_limit and len(picked) > row_limit:
        if time_col:
            order = pd.to_datetime(frame[time_col], errors="coerce").to_numpy()
        else:
            order = np.array(row_keys_list, dtype=object)
        picked = picked[np.argsort(order[picked], kind="stable")][-row_limit:]
    return picked


# COMMAND ----------

# DBTITLE 1,Score the rows of one request


def score_request(
    task_id: str,
    keys: dict,
    window_start: str,
    window_end: str,
    request_id: str,
    rid: str,
    latest_days=None,
    row_limit=None,
) -> dict:
    """Score one request and append one row per scored row to `scoring_requests`."""
    cfg, time_col, start, end = check_request(
        task_id, keys, window_start, window_end, latest_days
    )
    request_id = request_id or _uuid.uuid4().hex[:12]
    ecosystem, entry = registered_entry(task_id)
    print(f"request {request_id}: {task_id}, keys {keys}, window {start} to {end}")
    print(f"model {entry['model_name']} version {entry['version']}")
    with timed_stage("load the model"):
        bundle = load_bundle({"mlflow_run_id": entry["mlflow_run_id"]})
    ctx = MonitorContext(task_id, rid, "held_out")
    if int(entry["frozen_delta_version"]) != int(ctx.frozen_version):
        raise RuntimeError(
            f"registered at version {entry['frozen_delta_version']}, "
            f"frozen dataset is at {ctx.frozen_version}"
        )
    with timed_stage("read and prepare the rows"):
        df, start, end = request_frame(
            ctx, cfg, task_id, keys, time_col, start, end, latest_days, row_limit
        )
        evaluator = TabularEvaluator(ctx, cfg)
        evaluator.load(df, None, 1.0)
        frame = evaluator.frames["eval"].reset_index(drop=True)
    _take_notes()
    print(f"OK  {len(frame)} row(s) read")
    with timed_stage("score the rows"):
        raw = bundle.predict(frame)
    if isinstance(raw, dict):
        median = np.asarray(raw[0.5], dtype="float64")
        low = np.asarray(raw[0.1], dtype="float64")
        high = np.asarray(raw[0.9], dtype="float64")
    else:
        median = np.asarray(raw, dtype="float64")
        low = high = np.full(len(median), np.nan)
    row_key_list = row_keys(cfg, frame)
    picked = request_positions(frame, row_key_list, time_col, start, end, row_limit)
    now = now_utc()

    def _num(value):
        return None if not np.isfinite(value) else float(value)

    request_rows = [
        (
            request_id,
            task_id,
            entry["model_name"],
            int(entry["version"]),
            json.dumps(keys, sort_keys=True),
            start.isoformat() if start else "",
            end.isoformat() if end else "",
            row_key_list[i],
            _num(median[i]),
            _num(low[i]),
            _num(high[i]),
            int(entry["frozen_delta_version"]),
            rid,
            now,
        )
        for i in picked
    ]
    print(f"{len(request_rows)} row(s) in the window")
    table = create_model_table(
        FOUNDATION_REQUESTS, FOUNDATION_DDL[FOUNDATION_REQUESTS], ecosystem
    )
    append_model_rows(
        spark.createDataFrame(request_rows, FOUNDATION_DDL[FOUNDATION_REQUESTS]),
        FOUNDATION_REQUESTS,
        ecosystem=ecosystem,
    )
    stored = (
        read_model(FOUNDATION_REQUESTS, ecosystem=ecosystem)
        .filter(F.col("request_id") == request_id)
        .count()
    )
    print(f"OK  request {request_id} written to {table}")
    return {
        "request_id": request_id,
        "task_id": task_id,
        "ecosystem": ecosystem,
        "rows": len(request_rows),
        "stored": int(stored),
    }


# COMMAND ----------

# DBTITLE 1,The default request


def default_task_ids() -> list:
    """The tabular tasks that have a registered selected model, both ecosystems."""
    ids = set()
    for eco in REQUEST_ECOSYSTEMS:
        for entry in operate_entries(eco):
            if EVAL_TASKS.get(entry["task_id"], {}).get("kind") == "tabular":
                ids.add(entry["task_id"])
    return sorted(ids)


# COMMAND ----------

# DBTITLE 1,The default request of one task


def score_default_request(task_id: str, rid: str, request_id: str = "") -> dict:
    """The default request of one task: the newest DEFAULT_WINDOW_DAYS days, all keys,
    at most DEFAULT_ROW_LIMIT rows, REQUEST_LIMIT_SECONDS at most."""
    with candidate_guard(
        f"request {task_id}", REQUEST_LIMIT_SECONDS, REQUEST_STACK_SECONDS
    ):
        return score_request(
            task_id,
            {},
            "",
            "",
            request_id,
            rid,
            latest_days=DEFAULT_WINDOW_DAYS,
            row_limit=DEFAULT_ROW_LIMIT,
        )


# COMMAND ----------

# DBTITLE 1,One default request per tabular task


def score_default_requests(rid: str):
    """One request per tabular task: the newest DEFAULT_WINDOW_DAYS days, all keys,
    at most DEFAULT_ROW_LIMIT rows. A task that fails is listed; the others run."""
    results, failed = [], []
    for task_id in default_task_ids():
        started = _time_request.monotonic()
        try:
            out = score_default_request(task_id, rid)
            results.append(out)
            took = _time_request.monotonic() - started
            print(f"OK   {task_id}: {out['rows']} row(s) in {took:.0f} s")
        except Exception as exc:
            failed.append((task_id, f"{type(exc).__name__}: {str(exc)[:300]}"))
            print(f"FAIL {task_id}: {failed[-1][1]}")
        _gc.collect()
    return results, failed
