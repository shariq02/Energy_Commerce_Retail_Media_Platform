# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # OPERATE RULES LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** the monitoring settings, the reference profile, the comparison of a
# MAGIC window with the reference, the performance check, the retraining triggers and
# MAGIC the monitoring findings text. Pulled in with `%run ../lib/_operate_rules` after
# MAGIC `_model_common` and `_registry_rules`. Plain Python; only the registry reader and
# MAGIC the table writer use Spark. Nothing here scores a model. The findings text is
# MAGIC committed, so it passes through `scrub_private`.

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
PREDICTION_SUBJECT = "__prediction__"
NO_SUBJECT = "__none__"
LEVELS = ("ok", "warning", "flag", "info", "not_applicable")
DRIFT_KINDS = ("feature_drift", "null_rate", "prediction_drift", "prediction_null_rate")
EPSILON = 1e-4

# COMMAND ----------

# DBTITLE 1,The recorded settings


def operate_spec_from_rows(rows) -> dict:
    """The typed monitoring settings; a missing parameter means setup did not run."""
    have = {r["parameter"]: r["parameter_value"] for r in rows if r["parameter"]}
    missing = [p for p in OPERATE_PARAMETERS if p not in have]
    if missing:
        raise RuntimeError(
            f"monitoring settings not recorded: {missing}; run operate/00_operate_setup"
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
    return [
        _result(
            drift,
            subject,
            value,
            psi_level(value, spec),
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


def table_tuples(rows, ddl: str) -> list:
    """Tuples in table order from row dicts."""
    cols = [part.split()[0] for part in ddl.split(", ")]
    return [tuple(r.get(c) for c in cols) for r in rows]


def replace_rows(rows: list, table: str, ecosystem: str, predicate: str) -> None:
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
# a check recorded per ecosystem carries the ecosystem after a colon
_OTHER_ECOSYSTEM_OPERATE = {"energy": ":commerce", "commerce": ":energy"}


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
            "_Auto-generated by `databricks/models/operate/gate/03_export_findings`. "
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
