# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # APPROVAL RULES LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** the approval rule table and the rule engine that turns the recorded
# MAGIC evaluation results of a task into a recommendation. Pulled in with
# MAGIC `%run ../lib/_approval_rules` after `_model_common`. Plain Python, no Spark.

# COMMAND ----------

# DBTITLE 1,Imports
import json as _json

# COMMAND ----------

# DBTITLE 1,Decisions and the rule table
APPROVAL_CONTEXT_TABLE = "evaluation_run_context"
DECISIONS = ("approved", "approved_with_conditions", "not_approved", "deferred")
RESTRICTIONS = ("diagnostic_only", "offline_only")
ROLES = ("selected", "runner_up", "baseline_fallback")
APPROVED = ("approved", "approved_with_conditions")
BLOCKING_RULES = ("R03", "R05", "R07")
DECIDER_ENGINE = "rule_engine"
DECIDER_OWNER = "owner"
# (rule id, class, description, parameter, default value)
APPROVAL_RULES = (
    (
        "R01",
        "selection",
        (
            "one approved model per task, chosen by the validation rank; "
            "the baseline stays as fallback"
        ),
        "",
        "",
    ),
    ("R02", "conditional", "skill interval includes zero", "", ""),
    ("R03", "blocking", "skill interval wholly below zero", "", ""),
    (
        "R04",
        "conditional",
        "validation-to-evaluated degradation above the limit",
        "degradation_relative",
        "0.2",
    ),
    (
        "R05",
        "blocking",
        "evidence floor per task and per metric tier",
        "min_rows",
        "30",
    ),
    (
        "R05",
        "blocking",
        "evidence floor per task and per metric tier",
        "min_blocks",
        "5",
    ),
    ("R06", "restriction", "no ground truth: diagnostic use only", "", ""),
    ("R07", "segment", "a segment with negative skill is not approved", "", ""),
    (
        "R08",
        "conditional",
        "80% interval coverage outside the tolerance",
        "coverage_target",
        "0.8",
    ),
    (
        "R08",
        "conditional",
        "80% interval coverage outside the tolerance",
        "coverage_tolerance",
        "0.1",
    ),
    (
        "R09",
        "restriction",
        "offline evidence only (offline reinforcement learning, injected anomalies)",
        "",
        "",
    ),
    ("R10", "disclosure", "held-out partition read more than once", "", ""),
)
APPROVAL_DEFAULTS = {p: v for _r, _c, _d, p, v in APPROVAL_RULES if p}

# COMMAND ----------

# DBTITLE 1,Task-specific settings
DIAGNOSTIC_ONLY_TASKS = ("weak_supervision.labels",)
OFFLINE_ONLY_TASKS = ("honda_anomaly.anomaly",)
OFFLINE_ONLY_PARADIGMS = ("offline_rl",)
# prefix of the recorded metric, and how its skill is read
SEGMENT_RULES = {
    "weather_imputation.reconstruction": ("skill", "skill_mae__weather__"),
    "bias.bias": ("mae_vs_baseline", "mae__forecast_scope__"),
    "honda_forecast.increment": ("mae_vs_baseline", "mae__channel__"),
}
TASK_NOTES = {
    "next_item_ga4.next_item": [
        (
            "The only baseline is item popularity; there is no baseline that uses "
            "the items already seen in the session."
        )
    ],
}
ENERGY_FIGURES = ("episode_net_abs_policy", "episode_net_abs_logged")

# COMMAND ----------

# DBTITLE 1,Approval settings from the recorded table


def spec_from_rows(rows) -> dict:
    """The typed approval settings; a missing parameter means setup did not run."""
    have = {r["parameter"]: r["parameter_value"] for r in rows if r["parameter"]}
    missing = [k for k in APPROVAL_DEFAULTS if k not in have]
    if missing:
        raise RuntimeError(
            f"approval settings not recorded: {missing}; run approve/00_approval_setup"
        )
    return {
        "degradation_relative": float(have["degradation_relative"]),
        "min_rows": int(have["min_rows"]),
        "min_blocks": int(have["min_blocks"]),
        "coverage_target": float(have["coverage_target"]),
        "coverage_tolerance": float(have["coverage_tolerance"]),
    }


# COMMAND ----------

# DBTITLE 1,Recorded metrics of one model


def model_metrics(rows) -> dict:
    """metric -> (held-out value, validation value) for one model's result rows."""
    return {
        r["metric"]: (r["value"], r.get("validation_value"))
        for r in rows
        if r["metric"] and r["value"] is not None
    }


def _val(metrics: dict, name: str):
    pair = metrics.get(name)
    return None if pair is None else pair[0]


def _rule(rule_id, outcome, detail="") -> dict:
    return {"rule": rule_id, "outcome": outcome, "detail": detail}


# COMMAND ----------

# DBTITLE 1,Evidence floor, per task and per metric tier


def evidence_floor(task: dict, rows, metrics: dict, spec: dict) -> list:
    """R05 failures as text: too few rows or blocks, or a tier or class too thin."""
    fails = []
    n = rows[0].get("n_rows") if rows else None
    if n is None or n < spec["min_rows"]:
        fails.append(f"{n} evaluated rows, floor {spec['min_rows']}")
    blocks = _val(metrics, "bootstrap_blocks")
    if blocks is not None and blocks < spec["min_blocks"]:
        fails.append(f"{blocks:.0f} resampling blocks, floor {spec['min_blocks']}")
    for name, (value, _validation) in metrics.items():
        if name.startswith("n_true__") and value < spec["min_rows"]:
            fails.append(f"tier {name[8:]} has {value:.0f} rows")
    rate = _val(metrics, "base_rate")
    if task["task_type"] == "classification" and rate is not None and n:
        positives = rate * n
        if positives < spec["min_rows"]:
            fails.append(f"{positives:.0f} positive rows")
    return fails


# COMMAND ----------

# DBTITLE 1,Segment decisions


def segment_decisions(task_id: str, metrics: dict, base_metrics: dict) -> list:
    """One entry per segment: skill from the recorded values, no interval."""
    rule = SEGMENT_RULES.get(task_id)
    if rule is None:
        return []
    kind, prefix = rule
    out = []
    for name, (value, _validation) in sorted(metrics.items()):
        if not name.startswith(prefix):
            continue
        if kind == "skill":
            skill = value
        else:
            base = _val(base_metrics, name)
            skill = None if not base or base <= 0 else 1.0 - value / base
        if skill is None:
            continue
        negative = skill < 0
        out.append(
            {
                "segment": name[len(prefix) :],
                "skill": float(skill),
                "decision": "not_approved" if negative else "approved_with_conditions",
                "detail": "negative skill" if negative else "no segment interval",
            }
        )
    return out


# COMMAND ----------


# DBTITLE 1,Task conditions
def _survival_conditions(metrics: dict, base_metrics: dict) -> list:
    out = []
    gap = _val(metrics, "calibration_gap_5y")
    base_gap = _val(base_metrics, "calibration_gap_5y")
    if gap is not None and base_gap is not None and gap > base_gap:
        out.append(
            f"5-year calibration gap {gap:.3g} against {base_gap:.3g} for the "
            "baseline; predicted risk is less well calibrated than the baseline"
        )
    out.append(
        "the number of events in the held-out partition was not recorded; the "
        "concordance and Brier scores rest on an unknown number of events"
    )
    out.append(
        "revalidate on a new frozen dataset version, with the held-out event "
        "count recorded, before relying on the risk values"
    )
    return out


TASK_CONDITIONS = {"survival.unit_lifetime": _survival_conditions}


def _positive_segment_condition(segments: list) -> list:
    """Positive skill without a segment interval is not enough for approval."""
    positive = [s for s in segments if s["decision"] != "not_approved"]
    if not positive:
        return []
    shown = ", ".join(f"{s['segment']} ({s['skill']:.3g})" for s in positive)
    return [
        (
            "segments with positive skill but no uncertainty interval, conditional "
            f"until an interval is recorded: {shown}"
        )
    ]


# COMMAND ----------

# DBTITLE 1,One selected model: rules, conditions and decision


def _skill_rules(metrics: dict) -> tuple[list, list]:
    low = _val(metrics, "skill_primary_ci_low")
    high = _val(metrics, "skill_primary_ci_high")
    if low is None or high is None:
        return [_rule("R02", "condition", "skill interval not recorded")], [
            "skill interval not recorded"
        ]
    if high < 0:
        return [_rule("R03", "blocking", f"interval {low:.4g} to {high:.4g}")], []
    if low <= 0:
        text = f"skill interval {low:.4g} to {high:.4g} includes zero"
        return [_rule("R02", "condition", text)], [text]
    return [], []


def _drift_and_coverage(metrics: dict, spec: dict) -> tuple[list, list]:
    rules, conditions = [], []
    change = _val(metrics, "primary_change_relative")
    if change is not None and change > spec["degradation_relative"]:
        text = (
            f"primary metric {change:+.1%} worse than on validation; revalidate on "
            "newer data before relying on it"
        )
        rules.append(_rule("R04", "condition", text))
        conditions.append(text)
    cover = _val(metrics, "interval_coverage_80")
    if cover is not None and (
        abs(cover - spec["coverage_target"]) > spec["coverage_tolerance"]
    ):
        text = (
            f"80% interval covers {cover:.1%}, target "
            f"{spec['coverage_target']:.0%} plus or minus "
            f"{spec['coverage_tolerance']:.0%}"
        )
        rules.append(_rule("R08", "condition", text))
        conditions.append(text)
    return rules, conditions


def _restriction(task: dict, metrics: dict) -> tuple[str | None, list, list]:
    task_id = task["task_id"]
    if task_id in DIAGNOSTIC_ONLY_TASKS:
        text = "no ground truth: diagnostic use only"
        return "diagnostic_only", [_rule("R06", "restriction", text)], [text]
    if task_id in OFFLINE_ONLY_TASKS or task["paradigm"] in OFFLINE_ONLY_PARADIGMS:
        text = "offline evidence only; operational feasibility is not shown"
        energy = [_val(metrics, k) for k in ENERGY_FIGURES]
        conditions = [text]
        if None not in energy:
            conditions.append(
                f"net energy per episode {energy[0]:.4g} for the policy against "
                f"{energy[1]:.4g} in the logged data"
            )
        return "offline_only", [_rule("R09", "restriction", text)], conditions
    return None, [], []


# COMMAND ----------

# DBTITLE 1,Decision of the selected model


def decide_selected(task, rows, base_rows, spec, reads) -> dict:
    """Decision, restriction, rules, conditions, segments and notes of one model."""
    metrics = model_metrics(rows)
    out = {
        "decision": "deferred",
        "restriction": None,
        "rules": [],
        "conditions": [],
        "segments": [],
        "notes": list(TASK_NOTES.get(task["task_id"], [])),
    }
    if reads > 1:
        text = (
            f"the held-out partition was read in at least {reads} evaluation runs; "
            "the exact number of held-out reads is not recoverable; scores unchanged"
        )
        out["rules"].append(_rule("R10", "disclosure", text))
        out["notes"].append(text)
    if not rows or rows[0]["status"] != "ok":
        detail = "not evaluated" if not rows else str(rows[0].get("detail"))
        out["rules"].insert(0, _rule("R05", "blocking", detail))
        return out
    fails = evidence_floor(task, rows, metrics, spec)
    if fails:
        out["rules"].insert(0, _rule("R05", "blocking", "; ".join(fails)))
        return out
    skill_rules, skill_conditions = _skill_rules(metrics)
    out["rules"] += skill_rules
    out["conditions"] += skill_conditions
    if any(r["rule"] == "R03" for r in skill_rules):
        out["decision"] = "not_approved"
        return out
    rules, conditions = _drift_and_coverage(metrics, spec)
    out["rules"] += rules
    out["conditions"] += conditions
    restriction, rules, conditions = _restriction(task, metrics)
    out["restriction"] = restriction
    out["rules"] += rules
    out["conditions"] += conditions
    segments = segment_decisions(
        task["task_id"], metrics, model_metrics(base_rows or [])
    )
    out["segments"] = segments
    excluded = [s["segment"] for s in segments if s["decision"] == "not_approved"]
    if segments and len(excluded) == len(segments):
        out["rules"].append(
            _rule("R07", "blocking", "every segment has negative skill")
        )
        out["decision"] = "not_approved"
        return out
    if excluded:
        text = f"segments not approved: {', '.join(excluded)}"
        out["rules"].append(_rule("R07", "segment", text))
        out["conditions"].append(text)
    out["conditions"] += _positive_segment_condition(segments)
    task_conditions = TASK_CONDITIONS.get(task["task_id"])
    if task_conditions:
        out["conditions"] += task_conditions(metrics, model_metrics(base_rows or []))
    clean = not out["conditions"] and restriction is None
    out["decision"] = "approved" if clean else "approved_with_conditions"
    return out


# COMMAND ----------

# DBTITLE 1,Recommendation for every forwarded model of one task


def _row_for(task, sel, rows, role, decision) -> dict:
    metrics = model_metrics(rows)
    primary = metrics.get(task["primary_metric"], (None, None))
    return {
        "task_id": task["task_id"],
        "dataset_id": task["dataset_id"],
        "model_name": sel["model_name"],
        "role": role,
        "rank": int(sel["rank"]),
        "decision": decision["decision"],
        "restriction": decision["restriction"],
        "rules": decision["rules"],
        "conditions": decision["conditions"],
        "segments": decision["segments"],
        "notes": decision["notes"],
        "primary_metric": task["primary_metric"],
        "validation_value": primary[1],
        "evaluated_value": primary[0],
        "frozen_delta_version": int(sel["frozen_delta_version"]),
        "mlflow_run_id": sel["mlflow_run_id"],
    }


def _baseline_of(task, selection) -> dict:
    """The forwarded baseline: the one named like the registry baseline, else the
    only baseline row (a runner records its baseline under its own name)."""
    bases = [s for s in selection if s["is_baseline"]]
    named = [s for s in bases if s["model_name"] == task["baseline"]]
    if named:
        return named[0]
    if len(bases) == 1:
        return bases[0]
    raise ValueError(
        f"{task['task_id']}: {len(bases)} baseline rows, none named {task['baseline']}"
    )


def recommend_task(task, selection, results, spec, reads) -> list:
    """Rows for the selected candidate, the runner-up and the baseline fallback.
    The selected candidate is the best forwarded candidate by validation rank."""
    by_model: dict = {}
    for r in results:
        by_model.setdefault(r["model_name"], []).append(r)
    cands = sorted(
        (s for s in selection if not s["is_baseline"]), key=lambda s: s["rank"]
    )
    if not cands:
        raise ValueError(f"{task['task_id']}: no forwarded candidate")
    base = _baseline_of(task, selection)
    base_rows = by_model.get(base["model_name"], [])
    out = []
    for i, sel in enumerate(cands):
        rows = by_model.get(sel["model_name"], [])
        if i == 0:
            decision = decide_selected(task, rows, base_rows, spec, reads)
            out.append(_row_for(task, sel, rows, "selected", decision))
            continue
        probe = decide_selected(task, rows, base_rows, spec, reads)
        note = f"not selected: validation rank {sel['rank']}"
        decision = {
            "decision": "not_approved",
            "restriction": None,
            "rules": [_rule("R01", "selection", note), *probe["rules"]],
            "conditions": [],
            "segments": [],
            "notes": [],
        }
        out.append(_row_for(task, sel, rows, "runner_up", decision))
    fallback = {
        "decision": "approved",
        "restriction": None,
        "rules": [_rule("R01", "selection", "baseline fallback")],
        "conditions": [],
        "segments": [],
        "notes": [],
    }
    out.append(_row_for(task, base, base_rows, "baseline_fallback", fallback))
    return out


# COMMAND ----------

# DBTITLE 1,Rows for the decision table


def to_json(value) -> str:
    return _json.dumps(value, default=str, sort_keys=True)


def approval_tuples(rows, rid, now, decided_by=DECIDER_ENGINE) -> list:
    """model_approval tuples in table order from recommendation rows."""
    return [
        (
            r["task_id"],
            r["dataset_id"],
            r["model_name"],
            r["role"],
            r["rank"],
            r["decision"],
            r["restriction"],
            r["decision"],
            r["restriction"],
            False,
            None,
            to_json(r["rules"]),
            to_json(r["conditions"]),
            to_json(r["segments"]),
            to_json(r["notes"]),
            r["primary_metric"],
            r["validation_value"],
            r["evaluated_value"],
            r["frozen_delta_version"],
            r["mlflow_run_id"],
            decided_by,
            now,
            now,
            rid,
        )
        for r in rows
    ]
