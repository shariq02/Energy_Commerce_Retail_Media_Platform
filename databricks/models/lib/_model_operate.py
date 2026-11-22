# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # MODEL OPERATE LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the monitoring rules, the window reads and the scoring of a request
# MAGIC with the registered selected model of a tabular task. Pulled in with
# MAGIC `%run ../lib/_model_operate` after `_model_common`, `_model_evaluate` and
# MAGIC `_model_govern`. A request names a task, keys and a window, never feature
# MAGIC values. Without a task, the default request scores the newest days of every
# MAGIC tabular task.

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
        df = df.filter(F.to_date(F.col(time_col)) >= F.lit(start.isoformat()))
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
        "scoring_requests", MODEL_DDL["scoring_requests"], ecosystem
    )
    append_model_rows(
        spark.createDataFrame(request_rows, MODEL_DDL["scoring_requests"]),
        "scoring_requests",
        ecosystem=ecosystem,
    )
    stored = (
        read_model("scoring_requests", ecosystem=ecosystem)
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


# COMMAND ----------

# DBTITLE 1,Imports
import json as _json_operate
import math as _math_operate

# COMMAND ----------

# DBTITLE 1,Monitoring settings and constants
# (parameter, default value, description)
OPERATE_DEFAULTS = (
    ("psi_warning", "0.10", "population stability index at or above this: warning"),
    ("psi_flag", "0.25", "population stability index at or above this: flag"),
    (
        "null_rate_increase_pp",
        "5.0",
        "rise of the null rate over the reference, in percentage points: flag",
    ),
    ("reference_bins", "10", "bins of a numeric reference profile"),
    ("max_window_rows", "100000", "rows of a window that are profiled and stored"),
    ("max_categories", "20", "levels kept in a categorical reference profile"),
)
OPERATE_PARAMETERS = tuple(p for p, _v, _d in OPERATE_DEFAULTS)
REFERENCE_WINDOW = "train"
MONITOR_WINDOWS = ("validation", "held_out")
# the tables that hold rows of one registered model
RECORD_TABLES = (
    "reference_profile",
    "model_predictions",
    "monitoring_results",
    "monitoring_flags",
    "monitoring_triggers",
)
PREDICTION_SUBJECT = "__prediction__"
NO_SUBJECT = "__none__"
LEVELS = ("ok", "warning", "flag", "info", "not_applicable")
DRIFT_KINDS = ("feature_drift", "null_rate", "prediction_drift", "prediction_null_rate")
CALENDAR_FEATURES = frozenset(
    ("year", "month", "trend_years", "day_of_year", "iso_week")
)
EPSILON = 1e-4

# COMMAND ----------

# DBTITLE 1,The recorded settings


def operate_spec_from_rows(rows) -> dict:
    """The typed monitoring settings; a missing parameter means setup did not run."""
    have = {r["parameter"]: r["parameter_value"] for r in rows if r["parameter"]}
    missing = [p for p in OPERATE_PARAMETERS if p not in have]
    if missing:
        raise RuntimeError(
            f"monitoring settings not recorded: {missing}; run setup/02_model_setup"
        )
    return {
        "psi_warning": float(have["psi_warning"]),
        "psi_flag": float(have["psi_flag"]),
        "null_rate_increase_pp": float(have["null_rate_increase_pp"]),
        "reference_bins": int(have["reference_bins"]),
        "max_window_rows": int(have["max_window_rows"]),
        "max_categories": int(have["max_categories"]),
    }


# COMMAND ----------

# DBTITLE 1,Population stability index and level


def psi(expected, actual) -> float:
    """Population stability index of two share lists over the same bins."""
    e = np.clip(np.asarray(expected, dtype="float64"), EPSILON, None)
    a = np.clip(np.asarray(actual, dtype="float64"), EPSILON, None)
    return float(np.sum((a - e) * np.log(a / e)))


def psi_level(value, spec: dict) -> str:
    if value is None or not _math_operate.isfinite(value):
        return "not_applicable"
    if value >= spec["psi_flag"]:
        return "flag"
    return "warning" if value >= spec["psi_warning"] else "ok"


def null_level(increase_pp, spec: dict) -> str:
    if increase_pp is None or not _math_operate.isfinite(increase_pp):
        return "not_applicable"
    return "flag" if increase_pp >= spec["null_rate_increase_pp"] else "ok"


# COMMAND ----------

# DBTITLE 1,Columns as numbers or levels


def column_kind(series: pd.Series) -> str:
    """numeric (numbers, booleans, dates) or categorical (text levels)."""
    return "categorical" if FeatureEncoder._kind(series) == "cat" else "numeric"


def numeric_array(series: pd.Series) -> np.ndarray:
    """Float values of a numeric, boolean or date column; NaN where missing."""
    if series.dtype == bool:
        return series.to_numpy(dtype="float64")
    if pd.api.types.is_datetime64_any_dtype(series) or (
        series.dtype == object and series.notna().any() and not _is_number(series)
    ):
        t = pd.to_datetime(series, errors="coerce")
        days = t.astype("datetime64[ns]").astype("int64") / 86_400_000_000_000
        return np.where(t.isna(), np.nan, days)
    return pd.to_numeric(series, errors="coerce").to_numpy(dtype="float64")


def _is_number(series: pd.Series) -> bool:
    non_null = series.dropna()
    return bool(pd.to_numeric(non_null, errors="coerce").notna().mean() >= 0.95)


def level_values(series: pd.Series) -> tuple[np.ndarray, int]:
    """(text of the non-null values, number of null values)."""
    null = series.isna().to_numpy()
    text = series.astype(str).to_numpy(dtype=object)
    return text[~null], int(null.sum())


def _numeric_shares(finite: np.ndarray, edges) -> list:
    edges = np.asarray(edges, dtype="float64")
    if not len(finite):
        return [0.0] * (len(edges) + 1)
    idx = np.searchsorted(edges, finite, side="left")
    counts = np.bincount(idx, minlength=len(edges) + 1)
    return (counts / len(finite)).tolist()


def _level_shares(text: np.ndarray, levels) -> list:
    """Share of each level, then the share of every other value."""
    if not len(text):
        return [0.0] * (len(levels) + 1)
    counts = pd.Series(text).value_counts()
    shares = [float(counts.get(lv, 0)) / len(text) for lv in levels]
    return [*shares, max(0.0, 1.0 - sum(shares))]


# COMMAND ----------

# DBTITLE 1,Reference profile of one column


def _profile_row(subject, value_kind, n_rows, null_rate) -> dict:
    return {
        "subject": subject,
        "value_kind": value_kind,
        "n_rows": int(n_rows),
        "null_rate": null_rate,
        "min_value": None,
        "max_value": None,
        "bin_edges": "[]",
        "bin_shares": "[]",
    }


def profile_series(subject: str, series: pd.Series, spec: dict) -> dict:
    """The reference profile of one column or of the predictions."""
    n = len(series)
    if column_kind(series) == "categorical":
        text, nulls = level_values(series)
        row = _profile_row(subject, "categorical", n, nulls / n if n else None)
        top = pd.Series(text).value_counts().head(spec["max_categories"])
        levels = [str(i) for i in top.index]
        row["bin_edges"] = _json_operate.dumps(levels)
        row["bin_shares"] = _json_operate.dumps(_level_shares(text, levels))
        return row
    values = numeric_array(series)
    finite = values[np.isfinite(values)]
    row = _profile_row(subject, "numeric", n, 1.0 - len(finite) / n if n else None)
    if not len(finite):
        return row
    cuts = np.linspace(0.0, 1.0, spec["reference_bins"] + 1)[1:-1]
    edges = np.unique(np.quantile(finite, cuts))
    row["min_value"], row["max_value"] = float(finite.min()), float(finite.max())
    row["bin_edges"] = _json_operate.dumps(edges.tolist())
    row["bin_shares"] = _json_operate.dumps(_numeric_shares(finite, edges))
    return row


def profile_window(features: pd.DataFrame, prediction: pd.Series, spec: dict) -> list:
    """Profile rows of every feature column and of the prediction."""
    rows = [profile_series(c, features[c], spec) for c in features.columns]
    return [*rows, profile_series(PREDICTION_SUBJECT, prediction, spec)]


# COMMAND ----------

# DBTITLE 1,One window compared with the reference


def _result(kind, subject, value, level, ref, n_window, detail, spec=None) -> dict:
    warning = flag = None
    if spec and kind.endswith("drift"):
        warning, flag = spec["psi_warning"], spec["psi_flag"]
    elif spec and kind.endswith("null_rate"):
        flag = spec["null_rate_increase_pp"]
    return {
        "check_kind": kind,
        "subject": subject,
        "value": value,
        "warning_threshold": warning,
        "flag_threshold": flag,
        "level": level,
        "n_reference": int(ref["n_rows"]) if ref else None,
        "n_window": int(n_window),
        "detail": detail,
    }


def compare_series(ref: dict, subject: str, series: pd.Series, spec: dict, prefix=""):
    """Result rows of one column or of the predictions against the reference. The
    kinds are `feature_drift`, `null_rate` and one informational check; the
    prediction kinds carry the prefix `prediction_`."""
    drift = "prediction_drift" if prefix else "feature_drift"
    nulls = f"{prefix}null_rate"
    n = len(series)
    shares = _json_operate.loads(ref["bin_shares"])
    edges = _json_operate.loads(ref["bin_edges"])
    if ref["value_kind"] == "categorical":
        text, null_count = level_values(series)
        actual = _level_shares(text, edges)
        outside = actual[-1] if len(text) else None
        extra = ("unseen_category", outside, "share of values outside the kept levels")
        n_value = len(text)
    else:
        values = numeric_array(series)
        finite = values[np.isfinite(values)]
        null_count = n - len(finite)
        actual = _numeric_shares(finite, edges)
        low, high = ref["min_value"], ref["max_value"]
        outside = (
            float(np.mean((finite < low) | (finite > high)))
            if len(finite) and low is not None
            else None
        )
        extra = ("out_of_range", outside, "share of values outside the reference range")
        n_value = len(finite)
    value = psi(shares, actual) if n_value and any(shares) else None
    ref_null = ref["null_rate"] or 0.0
    rise = (null_count / n - ref_null) * 100.0 if n else None
    kind_extra = f"prediction_{extra[0]}" if prefix else extra[0]
    drift_level = psi_level(value, spec)
    calendar = not prefix and subject in CALENDAR_FEATURES
    if calendar and drift_level in ("warning", "flag"):
        drift_level = "info"
    return [
        _result(
            drift,
            subject,
            value,
            drift_level,
            ref,
            n_value,
            f"{len(shares)} bins",
            spec,
        ),
        _result(
            nulls,
            subject,
            rise,
            null_level(rise, spec),
            ref,
            n,
            f"reference {ref_null:.4f}, window {null_count / n if n else 0:.4f}",
            spec,
        ),
        _result(
            kind_extra,
            subject,
            extra[1],
            "info" if extra[1] is not None else "not_applicable",
            ref,
            n_value,
            extra[2],
        ),
    ]


def compare_window(ref_by_subject: dict, features, prediction, spec: dict) -> list:
    """All result rows of one window; a column without a reference row is not
    applicable, and a model without feature columns says so."""
    rows = []
    for column in features.columns:
        ref = ref_by_subject.get(column)
        if ref is None:
            rows.append(
                _result(
                    "feature_drift",
                    column,
                    None,
                    "not_applicable",
                    None,
                    len(features),
                    "no reference profile for this column",
                )
            )
            continue
        rows += compare_series(ref, column, features[column], spec)
    if not len(features.columns):
        rows.append(
            _result(
                "feature_drift",
                NO_SUBJECT,
                None,
                "not_applicable",
                None,
                len(prediction),
                "this model has no feature columns in its frame",
            )
        )
    pref = ref_by_subject.get(PREDICTION_SUBJECT)
    if pref is not None:
        rows += compare_series(
            pref, PREDICTION_SUBJECT, prediction, spec, "prediction_"
        )
    return rows


# COMMAND ----------

# DBTITLE 1,Performance against the recorded values


def relative_degradation(held_out_value, validation_value, higher: bool):
    """Relative worsening from the validation value to the held-out value; positive
    means worse. None when a value is missing or the validation value is zero."""
    if held_out_value is None or validation_value is None:
        return None
    t, v = float(held_out_value), float(validation_value)
    if not (_math_operate.isfinite(t) and _math_operate.isfinite(v)) or v == 0:
        return None
    return ((v - t) if higher else (t - v)) / abs(v)


def performance_result(approval: dict, limit: float) -> dict:
    """The result row of the degradation check of one registered model. It reads
    the recorded evaluation and approval values only."""
    higher = TASK_BY_ID[approval["task_id"]]["higher_is_better"]
    value = relative_degradation(
        approval["evaluated_value"], approval["validation_value"], higher
    )
    if value is None:
        level = "not_applicable"
    else:
        level = "flag" if value > limit else "ok"
    detail = (
        f"{approval['primary_metric']}: validation {approval['validation_value']}, "
        f"held-out {approval['evaluated_value']}"
    )
    return {
        "check_kind": "performance",
        "subject": approval["primary_metric"],
        "value": value,
        "warning_threshold": None,
        "flag_threshold": limit,
        "level": level,
        "n_reference": None,
        "n_window": 0,
        "detail": detail,
    }


# COMMAND ----------

# DBTITLE 1,Retraining triggers


def rule_detail(approval: dict, rule_id: str):
    """The recorded text of one rule on the approval row, or None."""
    for r in _json_operate.loads(approval.get("rules") or "[]"):
        if r.get("rule") == rule_id:
            return r.get("detail") or None
    return None


def _trigger(kind, source, condition, detail, value=None, threshold=None) -> dict:
    return {
        "trigger_kind": kind,
        "source": source,
        "condition": condition,
        "detail": detail,
        "value": value,
        "threshold": threshold,
    }


def build_triggers(approval, results, current_frozen, limit) -> list:
    """Trigger records of one registered model. A trigger is a record only: it never
    changes a decision, a status or a model."""
    out = []
    perf = [
        r for r in results if r["check_kind"] == "performance" and r["level"] == "flag"
    ]
    if perf:
        condition = rule_detail(approval, "R04") or (
            "validation-to-evaluated degradation above the limit"
        )
        out.append(
            _trigger(
                "performance_degradation",
                "R04",
                condition,
                perf[0]["detail"],
                perf[0]["value"],
                limit,
            )
        )
    drift = [
        r for r in results if r["level"] == "flag" and r["check_kind"] in DRIFT_KINDS
    ]
    if drift:
        worst = max(drift, key=lambda r: (r["value"] is not None, r["value"] or 0.0))
        out.append(
            _trigger(
                "drift_flag",
                "monitoring",
                "a drift or input-quality check reached the flag level",
                (
                    f"{len(drift)} flag(s); worst {worst['check_kind']} "
                    f"{worst['subject']} in {worst['window_id']}"
                ),
                worst["value"],
                worst["flag_threshold"],
            )
        )
    if (
        current_frozen is not None
        and current_frozen != approval["frozen_delta_version"]
    ):
        condition = rule_detail(approval, "R12") or (
            "the registered model was trained on an older frozen dataset version"
        )
        out.append(
            _trigger(
                "new_frozen_dataset_version",
                "dataset_manifest",
                condition,
                (
                    f"registered at version {approval['frozen_delta_version']}, "
                    f"current version {current_frozen}"
                ),
                float(current_frozen),
                float(approval["frozen_delta_version"]),
            )
        )
    return out


# COMMAND ----------

# DBTITLE 1,Table rows


def entry_link(entry: dict) -> dict:
    return {
        "task_id": entry["task_id"],
        "model_name": entry["model_name"],
        "registry_version": int(entry["version"]),
    }


def result_rows(rows, entry: dict, window_id: str, rid: str, now) -> list:
    """Result rows linked to the registry entry, the window and the run."""
    link = entry_link(entry)
    return [
        {**link, "window_id": window_id, **r, "run_id": rid, "measured_at": now}
        for r in rows
    ]


def reference_rows(rows, entry: dict, rid: str, now) -> list:
    """Reference profile rows linked to the registry entry and its frozen version."""
    link = {
        **entry_link(entry),
        "dataset_id": entry["dataset_id"],
        "frozen_delta_version": entry["frozen_delta_version"],
    }
    return [{**link, **r, "run_id": rid, "recorded_at": now} for r in rows]


def trigger_rows(triggers, entry: dict, rid: str, now) -> list:
    link = entry_link(entry)
    return [{**link, **t, "run_id": rid, "raised_at": now} for t in triggers]


def flag_rows(results, rid: str, now) -> list:
    """The warning and flag results as flag rows."""
    return [
        {
            "task_id": r["task_id"],
            "model_name": r["model_name"],
            "registry_version": r["registry_version"],
            "window_id": r["window_id"],
            "check_kind": r["check_kind"],
            "subject": r["subject"],
            "level": r["level"],
            "value": r["value"],
            "threshold": r["flag_threshold"]
            if r["level"] == "flag"
            else r["warning_threshold"],
            "detail": r["detail"],
            "run_id": rid,
            "flagged_at": now,
        }
        for r in results
        if r["level"] in ("warning", "flag")
    ]


def operate_entries(ecosystem: str) -> list:
    """The latest registry row of each selected model that is not retired."""
    latest = latest_versions(registry_rows(ecosystem))
    return [
        r
        for r in latest.values()
        if r["role"] == "selected" and r["lifecycle_status"] != "retired"
    ]


def models_without_entry(stored, entries) -> list:
    """The (task, model) pairs of `stored` that no operate entry covers."""
    wanted = {(e["task_id"], e["model_name"]) for e in entries}
    return sorted(set(stored) - wanted)


def stored_models(ecosystem: str) -> set:
    """The (task, model) pairs that hold a row in any operate record table."""
    found = set()
    for table in RECORD_TABLES:
        rows = (
            read_model(table, ecosystem=ecosystem)
            .select("task_id", "model_name")
            .distinct()
            .collect()
        )
        found |= {(r["task_id"], r["model_name"]) for r in rows}
    return found


def remove_rows_without_entry(ecosystem: str) -> list:
    """Delete the rows of every model that is not a current operate entry (a model
    that the registry retired). Returns the (task, model) pairs removed."""
    entries = operate_entries(ecosystem)
    if not entries:
        raise RuntimeError(
            f"{ecosystem}: no registered selected model, nothing removed"
        )
    stale = models_without_entry(stored_models(ecosystem), entries)
    for task_id, model_name in stale:
        for table in RECORD_TABLES:
            where = ecosystem_predicate(
                ecosystem, f"task_id = '{task_id}' AND model_name = '{model_name}'"
            )
            spark.sql(f"DELETE FROM {model_fqn(table, ecosystem)} WHERE {where}")
        print(
            f"OK  {ecosystem}: rows of {task_id} {model_name} removed (not registered)"
        )
    return stale


def table_tuples(rows, ddl: str) -> list:
    """Tuples in table order from row dicts."""
    cols = [part.split()[0] for part in ddl.split(", ")]
    return [tuple(r.get(c) for c in cols) for r in rows]


def replace_table_rows(rows: list, table: str, ecosystem: str, predicate: str) -> None:
    """Replace the rows of one table that match the predicate."""
    ddl = MODEL_DDL[table]
    replace_model_rows(
        spark.createDataFrame(table_tuples(rows, ddl), ddl),
        table,
        ecosystem=ecosystem,
        predicate=predicate,
    )


def entry_predicate(entry: dict, extra: str = "") -> str:
    return (
        f"task_id = '{entry['task_id']}' AND model_name = '{entry['model_name']}'"
        f"{extra}"
    )


# COMMAND ----------

# DBTITLE 1,Monitoring findings text
MAX_FINDING_ROWS = 500
_OTHER_ECOSYSTEM_OPERATE = OTHER_ECOSYSTEM_SUFFIXES


def _level_counts(results) -> dict:
    out: dict = {}
    for r in results:
        key = (r["task_id"], r["model_name"], r["window_id"])
        out.setdefault(key, dict.fromkeys(LEVELS, 0))[r["level"]] += 1
    return out


def _worst_drift(results) -> dict:
    worst: dict = {}
    for r in results:
        is_drift = r["check_kind"] in ("feature_drift", "prediction_drift")
        if not is_drift or r["value"] is None:
            continue
        key = (r["task_id"], r["model_name"], r["window_id"])
        if key not in worst or r["value"] > worst[key]["value"]:
            worst[key] = r
    return worst


def _reference_table(reference) -> str:
    by_entry: dict = {}
    for r in reference:
        by_entry.setdefault((r["task_id"], r["model_name"]), []).append(r)
    rows = [
        (
            task,
            model,
            max(r["registry_version"] for r in items),
            items[0]["frozen_delta_version"],
            sum(1 for r in items if r["subject"] != PREDICTION_SUBJECT),
            max(r["n_rows"] for r in items),
        )
        for (task, model), items in sorted(by_entry.items())
    ]
    return markdown_table(
        ["task", "model", "version", "frozen version", "columns", "reference rows"],
        rows,
    )


def _level_table(results) -> str:
    worst = _worst_drift(results)
    rows = []
    for (task, model, window), c in sorted(_level_counts(results).items()):
        w = worst.get((task, model, window))
        subject, value = (w["subject"], _number(w["value"])) if w else ("", "")
        rows.append((task, model, window, *[c[lv] for lv in LEVELS], subject, value))
    return markdown_table(
        ["task", "model", "window", *LEVELS, "worst PSI subject", "worst PSI"], rows
    )


def _flag_table(results) -> list:
    flagged = sorted(
        (r for r in results if r["level"] in ("warning", "flag")),
        key=lambda r: (r["level"] != "flag", r["task_id"], r["window_id"]),
    )
    shown = flagged[:MAX_FINDING_ROWS]
    table = markdown_table(
        [
            "task",
            "model",
            "window",
            "check",
            "subject",
            "level",
            "value",
            "threshold",
            "reference rows",
            "window rows",
        ],
        [
            (
                r["task_id"],
                r["model_name"],
                r["window_id"],
                r["check_kind"],
                r["subject"],
                r["level"],
                _number(r["value"]),
                _number(
                    r["flag_threshold"]
                    if r["level"] == "flag"
                    else r["warning_threshold"]
                ),
                r["n_reference"],
                r["n_window"],
            )
            for r in shown
        ],
    )
    return [f"{len(flagged)} row(s); the first {len(shown)} are listed.", "", table]


def render_monitoring_findings(eco, data: dict, stamp: str) -> str:
    """The monitoring record of one ecosystem as markdown. `data` holds the rows of
    the spec, the reference profile, the prediction counts, the results, the
    triggers and the check results."""
    results = data["results"]
    models = {(r["task_id"], r["model_name"]) for r in data["reference"]}
    lines = [
        f"# {eco.upper()} MONITORING FINDINGS",
        "",
        (
            "_Auto-generated by `databricks/models/operate/04_operate_gate`. "
            "A flag is a record only: it never changes an approval or a lifecycle "
            "status. The windows are time-ordered partitions of the frozen dataset "
            "(a replay); no held-out target is scored. "
            f"Generated: {stamp}_"
        ),
        "",
        "## summary",
        "",
        f"- registered models monitored: {len(models)}",
        f"- result rows: {len(results)}",
        *[
            f"- level {lv}: {sum(1 for r in results if r['level'] == lv)}"
            for lv in LEVELS
        ],
        f"- retraining triggers: {len(data['triggers'])}",
        "",
        "## settings",
        "",
        markdown_table(
            ["parameter", "value", "description", "recorded"],
            [
                (
                    r["parameter"],
                    r["parameter_value"],
                    r["description"],
                    r["recorded_at"],
                )
                for r in sorted(data["spec"], key=lambda r: r["parameter"])
            ],
        ),
        "",
        "## reference profiles (train partition at the registered frozen version)",
        "",
        _reference_table(data["reference"]),
        "",
        "## stored predictions",
        "",
        markdown_table(
            ["task", "model", "window", "rows"],
            [
                (c["task_id"], c["model_name"], c["window_id"], c["rows"])
                for c in sorted(
                    data["predictions"],
                    key=lambda c: (c["task_id"], c["model_name"], c["window_id"]),
                )
            ],
        ),
        "",
        "## monitoring by model and window",
        "",
        _level_table(results),
        "",
        "## warnings and flags",
        "",
        *_flag_table(results),
        "",
        "## performance against the recorded values",
        "",
        markdown_table(
            ["task", "model", "metric", "degradation", "limit", "level", "detail"],
            [
                (
                    r["task_id"],
                    r["model_name"],
                    r["subject"],
                    _number(r["value"]),
                    _number(r["flag_threshold"]),
                    r["level"],
                    r["detail"],
                )
                for r in sorted(results, key=lambda r: (r["task_id"], r["window_id"]))
                if r["check_kind"] == "performance"
            ],
        ),
        "",
        "## retraining triggers",
        "",
        markdown_table(
            [
                "task",
                "model",
                "version",
                "trigger",
                "source",
                "value",
                "threshold",
                "detail",
            ],
            [
                (
                    t["task_id"],
                    t["model_name"],
                    t["registry_version"],
                    t["trigger_kind"],
                    t["source"],
                    _number(t["value"]),
                    _number(t["threshold"]),
                    t["detail"],
                )
                for t in sorted(
                    data["triggers"], key=lambda t: (t["task_id"], t["trigger_kind"])
                )
            ],
        ),
        "",
        "## check results",
        "",
        markdown_table(
            ["component", "check", "status", "detail", "recorded"],
            [
                (
                    c["component"],
                    c["metric_name"],
                    c["status"],
                    c["error_detail"],
                    c["recorded_at"],
                )
                for c in sorted(data["checks"], key=lambda c: str(c["recorded_at"]))
                if not c["metric_name"].endswith(_OTHER_ECOSYSTEM_OPERATE[eco])
            ],
        ),
    ]
    return scrub_private("\n".join(lines))


# COMMAND ----------

# DBTITLE 1,Windows and the evaluation context
# kinds whose rows cannot be sampled without breaking the unit that is scored
NO_CAP_KINDS = ("reconstruction",)
SPARK_CAP_FACTOR = 10
# the survival forest builds one array of rows x event times per tree
SURVIVAL_CHUNK_ROWS = 2000


class MonitorContext(EvalContext):
    """One task read for one window."""

    def __init__(self, task_id: str, rid: str, window_id: str):
        super().__init__(task_id, rid, smoke=False)
        self.window_id = window_id
        self.partition = window_partition(window_id)
        self.component = f"models/operate/{task_id}"


def window_partition(window_id: str) -> str:
    """The partition read for a window id."""
    return {
        "train": "train",
        "validation": "validation",
        "held_out": TEST_PARTITION,
    }[window_id]


# COMMAND ----------

# DBTITLE 1,Row sampling


def key_columns(cfg: dict) -> list:
    spec = cfg["spec"]
    if spec.get("key_cols"):
        return list(spec["key_cols"])
    return [spec["session_col"]] if "session_col" in spec else []


def capped_frame(df, key_cols: list, cap: int):
    """The Spark frame, or a stable hash sample of its key columns above the cap."""
    if not key_cols or not set(key_cols) <= set(df.columns):
        return df
    if df.limit(cap + 1).count() <= cap:
        return df
    fraction = cap / df.count()
    bucket = F.pmod(
        F.xxhash64(*[F.col(c).cast("string") for c in key_cols]), F.lit(10000)
    )
    return df.filter(bucket < int(fraction * 10000))


def sample_positions(n: int, cap: int) -> np.ndarray:
    """All positions, or a sorted random sample of `cap` of them."""
    if n <= cap:
        return np.arange(n)
    rng = np.random.default_rng(MODEL_SEED)
    return np.sort(rng.choice(n, size=cap, replace=False))


def row_keys(cfg: dict, frame: pd.DataFrame) -> list:
    """The key columns of each row joined by a bar; the row number if a key column
    is not in the frame."""
    spec = cfg["spec"]
    cols = key_columns(cfg)
    if "step_col" in spec:
        cols = [*cols, spec["step_col"]]
    if not cols or not set(cols) <= set(frame.columns):
        return [str(i) for i in range(len(frame))]
    text = frame[cols[0]].astype(str)
    for c in cols[1:]:
        text = text + "|" + frame[c].astype(str)
    return text.tolist()


# COMMAND ----------

# DBTITLE 1,The monitored columns of each model kind


def monitored_columns(kind: str, ctx, evaluator, frame: pd.DataFrame) -> list:
    """The model input columns present in the window frame. A kind without such
    columns (reconstruction) has none."""
    spec = evaluator.spec
    if kind == "pumped":
        return [c for c in spec["state_cols"] if c in frame.columns]
    if kind == "ranking":
        return [c for c in (spec["src_col"],) if c in frame.columns]
    if kind not in ("tabular", "survival", "anomaly", "action", "multiclass"):
        return []
    ids = [*spec.get("id_features", ()), *spec.get("extra_features", ())]
    try:
        return resolve_features(
            ctx, frame.columns, id_features=ids, drop=spec.get("drop", ())
        )
    except RuntimeError:
        return []


# COMMAND ----------

# DBTITLE 1,One prediction per row


def sequence_top_items(bundle, frame: pd.DataFrame) -> np.ndarray:
    """The item the sequence network ranks first after each event."""
    import torch

    session_col, step_col, item_col, _truth = bundle.cols
    ordered = frame.sort_values([session_col, step_col]).reset_index(drop=True)
    hist = build_histories(ordered, bundle.item_ids, SEQ_LENGTH, session_col, item_col)
    names = {i: item for item, i in bundle.item_ids.items()}
    bundle.net.eval()
    ids = []
    with torch.no_grad():
        for i in range(0, len(hist), SEQ_BATCH):
            batch = torch.as_tensor(hist[i : i + SEQ_BATCH], dtype=torch.long)
            logits = bundle.net(batch)
            logits[:, 0] = -float("inf")
            ids.append(logits.argmax(1).numpy())
    top = np.concatenate(ids) if ids else np.zeros(0, dtype="int64")
    return np.array([str(names.get(int(i), "unknown")) for i in top], dtype=object)


def _reconstruction_predictions(evaluator, bundle):
    """The predicted value of every masked event, from the model inputs only."""
    values, keys = [], []
    for cfg, sset, events in evaluator.sets:
        for j, var in enumerate(cfg["variables"]):
            x, _truth, _sid = evaluator.data[(cfg["name"], j)]
            pred = np.asarray(
                evaluator._predict(bundle, cfg, sset, events, j, x), dtype="float64"
            )
            values.append(pred)
            keys += [f"{cfg['name']}|{var}|{i}" for i in range(len(pred))]
    joined = np.concatenate(values) if values else np.zeros(0)
    return joined, None, keys


def chunked_risk(bundle, frame: pd.DataFrame) -> np.ndarray:
    """Risk of every row, scored SURVIVAL_CHUNK_ROWS rows at a time."""
    parts = [
        np.asarray(
            bundle.risk(frame.iloc[i : i + SURVIVAL_CHUNK_ROWS]), dtype="float64"
        )
        for i in range(0, len(frame), SURVIVAL_CHUNK_ROWS)
    ]
    return np.concatenate(parts) if parts else np.zeros(0)


def predict_window(kind: str, evaluator, bundle):
    """(numeric predictions, text predictions, row keys) of the window frame. One of
    the first two is None. Row keys are None where the frame rows carry them."""
    frame = evaluator.frames["eval"]
    if kind == "reconstruction":
        return _reconstruction_predictions(evaluator, bundle)
    if kind == "multiclass":
        return None, np.asarray(bundle.predict(frame)).astype(str), None
    if kind == "action":
        return None, np.asarray(bundle.action(frame)).astype(str), None
    if kind == "ranking":
        return None, sequence_top_items(bundle, frame), None
    if kind == "tabular":
        if getattr(bundle, "task", None) == "quantile":
            values = bundle.predict(frame)[0.5]
        else:
            values = bundle.predict(frame)
    elif kind == "survival":
        values = chunked_risk(bundle, frame)
    elif kind == "anomaly":
        values = bundle.score(frame)
    elif kind == "pumped":
        values = bundle.action(frame)
    else:
        raise ValueError(f"no monitoring prediction for the kind {kind}")
    return np.asarray(values, dtype="float64"), None, None


# COMMAND ----------

# DBTITLE 1,Read, score and sample one window


def collect_window(entry: dict, window_id: str, bundle, rid: str, spec: dict) -> dict:
    """Features, predictions and row keys of one window, sampled to the row limit."""
    task_id = entry["task_id"]
    ctx = MonitorContext(task_id, rid, window_id)
    if int(entry["frozen_delta_version"]) != int(ctx.frozen_version):
        raise RuntimeError(
            f"{task_id}: registered at version {entry['frozen_delta_version']}, "
            f"frozen dataset is at {ctx.frozen_version}"
        )
    cfg = EVAL_TASKS[task_id]
    kind = cfg["kind"]
    evaluator = globals()[EVALUATOR_NAMES[kind]](ctx, cfg)
    try:
        df = task_frame(ctx, ctx.partition)
        if kind not in NO_CAP_KINDS:
            cap = SPARK_CAP_FACTOR * spec["max_window_rows"]
            df = capped_frame(df, key_columns(cfg), cap)
        evaluator.load(df, None, 1.0)
        values, labels, keys = predict_window(kind, evaluator, bundle)
        frame = evaluator.frames["eval"]
        n_rows = len(values) if values is not None else len(labels)
        pick = sample_positions(n_rows, spec["max_window_rows"])
        if keys is None:
            columns = monitored_columns(kind, ctx, evaluator, frame)
            features = frame[columns].iloc[pick]
            keys = row_keys(cfg, frame.iloc[pick])
        else:
            features = frame[[]].iloc[:0]
            keys = [keys[i] for i in pick]
        return {
            "window_id": window_id,
            "n_rows": int(n_rows),
            "features": features.reset_index(drop=True),
            "values": None if values is None else values[pick],
            "labels": None if labels is None else labels[pick],
            "keys": keys,
        }
    finally:
        evaluator.frames = {"eval": None, "validation": None}
        evaluator.sampler = None
        del evaluator
        _take_notes()
        _gc.collect()


def prediction_series(data: dict) -> pd.Series:
    """The predictions of a window as one column: numbers or text labels."""
    if data["values"] is not None:
        return pd.Series(data["values"], dtype="float64")
    return pd.Series(data["labels"], dtype="object")


# COMMAND ----------

# DBTITLE 1,Table writers


def write_predictions(entry: dict, data: dict, rid: str, now) -> None:
    link = entry_link(entry)
    labels = data["labels"]
    values = data["values"]
    rows = [
        {
            **link,
            "window_id": data["window_id"],
            "row_key": key,
            "prediction": None if values is None else float(values[i]),
            "prediction_label": None if labels is None else str(labels[i]),
            "frozen_delta_version": int(entry["frozen_delta_version"]),
            "run_id": rid,
            "scored_at": now,
        }
        for i, key in enumerate(data["keys"])
    ]
    eco = TASK_BY_ID[entry["task_id"]]["ecosystem"]
    window = f" AND window_id = '{data['window_id']}'"
    replace_table_rows(rows, "model_predictions", eco, entry_predicate(entry, window))


# COMMAND ----------

# DBTITLE 1,Reference profile and monitoring of one registered model


def profile_entry(entry: dict, rid: str, spec: dict) -> dict:
    """Profile the train window of one registered model and store its predictions
    and its reference profile."""
    eco = TASK_BY_ID[entry["task_id"]]["ecosystem"]
    bundle = load_bundle({"mlflow_run_id": entry["mlflow_run_id"]})
    data = collect_window(entry, REFERENCE_WINDOW, bundle, rid, spec)
    now = now_utc()
    profile = profile_window(data["features"], prediction_series(data), spec)
    rows = reference_rows(profile, entry, rid, now)
    replace_table_rows(rows, "reference_profile", eco, entry_predicate(entry))
    write_predictions(entry, data, rid, now)
    return {"columns": len(data["features"].columns), "rows": data["n_rows"]}


def reference_by_subject(ecosystem: str, entry: dict) -> dict:
    """The recorded reference rows of the registered version of one model."""
    rows = (
        read_model("reference_profile", ecosystem=ecosystem)
        .filter(
            (F.col("task_id") == entry["task_id"])
            & (F.col("model_name") == entry["model_name"])
            & (F.col("registry_version") == int(entry["version"]))
        )
        .collect()
    )
    return {r["subject"]: r.asDict() for r in rows}


def monitor_entry(entry: dict, rid: str, spec: dict) -> dict:
    """Score the validation and held-out windows of one registered model, store the
    predictions and compare each window with the recorded reference profile."""
    eco = TASK_BY_ID[entry["task_id"]]["ecosystem"]
    reference = reference_by_subject(eco, entry)
    if not reference:
        raise RuntimeError(f"{entry['task_id']}: no reference profile recorded")
    bundle = load_bundle({"mlflow_run_id": entry["mlflow_run_id"]})
    counts = {}
    for window_id in MONITOR_WINDOWS:
        data = collect_window(entry, window_id, bundle, rid, spec)
        now = now_utc()
        write_predictions(entry, data, rid, now)
        found = compare_window(
            reference, data["features"], prediction_series(data), spec
        )
        results = result_rows(found, entry, window_id, rid, now)
        window = f" AND window_id = '{window_id}' AND check_kind <> 'performance'"
        result_where = entry_predicate(entry, window)
        replace_table_rows(results, "monitoring_results", eco, result_where)
        flags = flag_rows(results, rid, now)
        flag_window = f" AND window_id = '{window_id}' AND check_kind <> 'performance'"
        flag_where = entry_predicate(entry, flag_window)
        replace_table_rows(flags, "monitoring_flags", eco, flag_where)
        counts[window_id] = {
            lv: sum(1 for r in results if r["level"] == lv) for lv in LEVELS
        }
    return counts


# COMMAND ----------

# DBTITLE 1,One ecosystem


def operate_ecosystem(ecosystem: str, rid: str, step: str) -> list:
    """Run `profile_entry` or `monitor_entry` for every registered selected model of
    the ecosystem. A model that fails is listed; the others still run."""
    rows = read_model("monitoring_spec", ecosystem=ecosystem).collect()
    spec = operate_spec_from_rows([r.asDict() for r in rows])
    run = profile_entry if step == "profile" else monitor_entry
    failed = []
    for entry in sorted(operate_entries(ecosystem), key=lambda e: e["task_id"]):
        try:
            out = run(entry, rid, spec)
            print(f"OK   {entry['task_id']} {entry['model_name']}: {out}")
        except Exception as exc:
            failed.append((entry["task_id"], f"{type(exc).__name__}: {str(exc)[:300]}"))
            print(f"FAIL {entry['task_id']} {entry['model_name']}: {failed[-1][1]}")
        _gc.collect()
    return failed
