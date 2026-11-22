# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # MODEL GOVERNANCE LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the approval rule table and rule engine, the model card and findings
# MAGIC text, the registry rules and the reproducibility check. Pulled in with
# MAGIC `%run ../lib/_model_govern` after `_model_common`.

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
    (
        "R10",
        "disclosure",
        (
            "held-out partition read in more than one evaluation run; the exact read "
            "count is not recoverable and the card states the minimum number of runs"
        ),
        "",
        "",
    ),
    (
        "R11",
        "conditional",
        "segment with positive skill but no uncertainty interval",
        "",
        "",
    ),
    (
        "R12",
        "conditional",
        (
            "task-specific limits: survival calibration gap, unrecorded held-out "
            "event count and revalidation"
        ),
        "",
        "",
    ),
)
APPROVAL_DEFAULTS = {p: v for _r, _c, _d, p, v in APPROVAL_RULES if p}

# COMMAND ----------

# DBTITLE 1,Task-specific settings
DIAGNOSTIC_ONLY_TASKS: tuple = ()
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
            f"approval settings not recorded: {missing}; run setup/02_model_setup"
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
    positive = _positive_segment_condition(segments)
    out["conditions"] += positive
    out["rules"] += [_rule("R11", "condition", text) for text in positive]
    task_conditions = TASK_CONDITIONS.get(task["task_id"])
    limits = (
        task_conditions(metrics, model_metrics(base_rows or []))
        if task_conditions
        else []
    )
    out["conditions"] += limits
    out["rules"] += [_rule("R12", "condition", text) for text in limits]
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
    probes = {
        s["model_name"]: decide_selected(
            task, by_model.get(s["model_name"], []), base_rows, spec, reads
        )
        for s in cands
    }
    out = []
    for i, sel in enumerate(cands):
        rows = by_model.get(sel["model_name"], [])
        if i == 0:
            out.append(_row_for(task, sel, rows, "selected", probes[sel["model_name"]]))
            continue
        probe = probes[sel["model_name"]]
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


# COMMAND ----------

# DBTITLE 1,Imports
import json as _json_text

# COMMAND ----------

# DBTITLE 1,Small helpers


def _loads(text, default):
    try:
        return _json_text.loads(text) if text else default
    except ValueError:
        return default


_OTHER_ECOSYSTEM_SUFFIX = OTHER_ECOSYSTEM_SUFFIXES


def _bullets(items) -> list:
    return [f"- {_cell(i, 400)}" for i in items] or ["- none"]


def _rule_ids(rules) -> str:
    return ", ".join(sorted({r["rule"] for r in rules})) or "none"


# COMMAND ----------

# DBTITLE 1,Approval findings text


def render_approval_findings(eco, rows, spec_rows, checks, stamp) -> str:
    """The decision record of one ecosystem as markdown."""
    lines = [
        f"# {eco.upper()} APPROVAL FINDINGS",
        "",
        (
            "_Auto-generated by `databricks/models/approve/04_approval_gate`. "
            "The decision belongs to the owner; a row decided by the rule engine "
            f"alone is a recommendation. Generated: {stamp}_"
        ),
        "",
        "## rules",
        "",
        markdown_table(
            ["rule", "class", "description", "parameter", "value"],
            [
                (
                    r["rule_id"],
                    r["rule_class"],
                    r["description"],
                    r["parameter"],
                    r["parameter_value"],
                )
                for r in sorted(spec_rows, key=lambda r: (r["rule_id"], r["parameter"]))
            ],
        ),
        "",
    ]
    chosen = [r for r in rows if r["role"] == "selected"]
    counts: dict = {}
    for r in chosen:
        key = (r["decision"], r["restriction"] or "")
        counts[key] = counts.get(key, 0) + 1
    lines += ["## summary", ""]
    lines.append(
        markdown_table(
            ["decision", "restriction", "tasks"],
            [(d, rs, n) for (d, rs), n in sorted(counts.items())],
        )
    )
    lines += ["", "## decisions", ""]
    lines.append(
        markdown_table(
            [
                "task",
                "model",
                "role",
                "decision",
                "restriction",
                "recommended",
                "overridden",
                "decided by",
                "rules",
            ],
            [
                (
                    r["task_id"],
                    r["model_name"],
                    r["role"],
                    r["decision"],
                    r["restriction"],
                    r["recommended_decision"],
                    r["overridden"],
                    r["decided_by"],
                    _rule_ids(_loads(r["rules"], [])),
                )
                for r in sorted(rows, key=lambda r: (r["task_id"], r["rank"]))
            ],
        )
    )
    overridden = [r for r in rows if r["overridden"]]
    lines += ["", "## overrides", ""]
    lines += [
        f"- {r['task_id']} / {r['model_name']}: {r['recommended_decision']} -> "
        f"{r['decision']}: {_cell(r['override_reason'], 300)}"
        for r in overridden
    ] or ["- none"]
    for r in sorted(chosen, key=lambda r: r["task_id"]):
        lines += ["", f"## {r['task_id']}", ""]
        lines.append(
            f"_model `{r['model_name']}`, decision `{r['decision']}`, restriction "
            f"`{r['restriction'] or 'none'}`, frozen version "
            f"{r['frozen_delta_version']}_"
        )
        lines += ["", "### rules", ""]
        lines.append(
            markdown_table(
                ["rule", "outcome", "detail"],
                [
                    (x["rule"], x["outcome"], x["detail"])
                    for x in _loads(r["rules"], [])
                ],
            )
        )
        lines += ["", "### conditions", ""] + _bullets(_loads(r["conditions"], []))
        segments = _loads(r["segments"], [])
        if segments:
            lines += ["", "### segments", ""]
            lines.append(
                markdown_table(
                    ["segment", "skill", "decision", "detail"],
                    [
                        (s["segment"], _number(s["skill"]), s["decision"], s["detail"])
                        for s in segments
                    ],
                )
            )
        notes = _loads(r["notes"], [])
        if notes:
            lines += ["", "### disclosures", ""] + _bullets(notes)
    lines += ["", "## check results", ""]
    lines.append(
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
                for c in sorted(checks, key=lambda c: str(c["recorded_at"]))
                if not c["metric_name"].endswith(_OTHER_ECOSYSTEM_SUFFIX[eco])
            ],
        )
    )
    return scrub_private("\n".join(lines))


# COMMAND ----------

# DBTITLE 1,Card constants
INTERNAL_METRIC_PREFIXES = ("bootstrap_", "pred_", "reproduc")
# libraries a model needs; the run records every installed library
MODEL_LIBRARIES = {
    "logistic": ("sklearn",),
    "gbt_sklearn": ("sklearn",),
    "poisson_gbt_sklearn": ("sklearn",),
    "forecast_residual_sklearn": ("sklearn",),
    "reward_weighted_gbt_sklearn": ("sklearn",),
    "gbt_lightgbm": ("lightgbm",),
    "gbt_cross_variable_lightgbm": ("lightgbm",),
    "quantile_gbt_lightgbm": ("lightgbm",),
    "survival_forest": ("sksurv",),
    "conservative_q": ("torch",),
    "sequence_gru": ("torch",),
    "sequence_transformer": ("torch",),
}
INTENDED_USE = {
    "price_daily.price": "estimates the daily electricity price in EUR per MWh",
    "price_quarter_hour.price": (
        "estimates the quarter-hour electricity price in EUR per MWh"
    ),
    "price_quarter_hour.negative_price": (
        "estimates the probability that a quarter-hour has a negative electricity price"
    ),
    "load.load": "estimates the electricity load in MWh",
    "bias.bias": (
        "estimates the bias of the published generation forecast in MWh, per forecast scope"
    ),
    "zone_generation.generation": ("estimates the generation in MWh of a control zone"),
    "honda_forecast.increment": (
        "estimates the next energy increment of a Honda site channel"
    ),
    "honda_anomaly.anomaly": (
        "scores Honda site energy readings for anomalies; the evidence rests on "
        "injected anomalies"
    ),
    "ccpp.output": (
        "estimates the net electrical output in MW of a combined cycle power plant "
        "from its operating conditions"
    ),
    "capacity_additions.additions": ("estimates the capacity in MW added per period"),
    "redispatch.any_event": (
        "estimates the probability that a redispatch event occurs"
    ),
    "redispatch.event_energy": (
        "estimates the logarithm of the energy of a redispatch event in MWh"
    ),
    "survival.unit_lifetime": (
        "estimates the lifetime in years of a generation unit and its probability "
        "of still being in operation after 1, 3 and 5 years"
    ),
    "weather_imputation.reconstruction": (
        "fills missing weather values, per weather variable"
    ),
    "rl_pumped_storage.policy": (
        "proposes the net pumped-storage action in MWh; evaluated on logged data only"
    ),
    "rl_redispatch.policy": (
        "proposes the direction of a redispatch action; evaluated on logged data only"
    ),
    "session_purchase_ga4.purchase": (
        "estimates the probability that a GA4 session ends in a purchase, from "
        "the first events of the session"
    ),
    "session_purchase_rees46.purchase": (
        "estimates the probability that a REES46 session ends in a purchase, from "
        "the first events of the session"
    ),
    "lapse_ga4.return": "estimates the probability that a GA4 user returns",
    "lapse_rees46.return": "estimates the probability that a REES46 user returns",
    "next_item_ga4.next_item": (
        "ranks products by the probability of being the next product in a GA4 session"
    ),
    "next_item_rees46.next_item": (
        "ranks products by the probability of being the next product in a REES46 session"
    ),
    "rl_session_sequences.policy": (
        "proposes the next event type of a session; evaluated on logged data only"
    ),
    "variant_pathogenicity.pathogenic": (
        "estimates the probability that a variant is pathogenic from its annotation"
    ),
    "variant_high_impact.high_impact": (
        "estimates the probability that a variant has a high predicted impact"
    ),
    "variant_carrier_screening.carrier": (
        "estimates the probability that a variant is a carrier-screening candidate"
    ),
    "variant_actionable_rare.actionable": (
        "estimates the probability that a rare variant is clinically actionable"
    ),
    "structural_variant_disruption.disruption": (
        "estimates the probability that a structural variant disrupts a gene"
    ),
    "gene_pharmacogene.pharmacogene": (
        "estimates the probability that a gene is a pharmacogene"
    ),
    "gene_cancer_role.cancer_role": "predicts the cancer role class of a gene",
    "benchmark_clinvar_vs_benign.label": (
        "separates ClinVar variants from benign variants on a benchmark set"
    ),
    "benchmark_gnomad_balanced.label": (
        "separates pathogenic from population variants on a balanced benchmark set"
    ),
    "benchmark_proteingym_clinical.label": (
        "separates clinically pathogenic from benign missense variants"
    ),
    "benchmark_pathogenic_coding.label": (
        "separates pathogenic from benign coding variants on a benchmark set"
    ),
    "benchmark_maps.label": "scores variants on the MAPS benchmark set",
    "benchmark_causal_eqtl.label": (
        "separates causal from non-causal expression variants on a benchmark set"
    ),
}

# COMMAND ----------

# DBTITLE 1,Facts for a card: data notes and library versions


def _read_note_lines(context_rows) -> list:
    ready = [
        r for r in context_rows or [] if r["status"] == "data_ready" and r["data_notes"]
    ]
    if not ready:
        return ["- not recorded"]
    out = []
    for note in _loads(ready[-1]["data_notes"], []):
        read = note.get("read")
        if read:
            shown = ", ".join(f"{k} {v}" for k, v in read.items())
            out.append(f"- held-out read: {shown}")
        elif "rows" in note:
            out.append(
                f"- frame: {note.get('rows')} rows, columns {note.get('columns')}, "
                f"rows by partition {note.get('rows_by_partition')}"
            )
    return out or ["- not recorded"]


def _library_line(fit_rows, model_name) -> str:
    needed = MODEL_LIBRARIES.get(model_name)
    rows = [r for r in fit_rows or [] if r["library_versions"]]
    if needed is None or not rows:
        return "not recorded"
    if not needed:
        return "none beyond the standard numerical stack"
    last = max(rows, key=lambda r: str(r["recorded_at"]))
    versions = _loads(last["library_versions"], {})
    shown = [
        f"{k} {versions[k]}" for k in needed if versions.get(k, "missing") != "missing"
    ]
    return ", ".join(shown) or "not recorded"


# COMMAND ----------

# DBTITLE 1,Model card text


def _metric_table(selected, baseline, results) -> str:
    def values(model):
        return {
            r["metric"]: (r["value"], r["validation_value"])
            for r in results
            if r["model_name"] == model
            and r["metric"]
            and "__" not in r["metric"]
            and not r["metric"].startswith(INTERNAL_METRIC_PREFIXES)
        }

    mine = values(selected["model_name"])
    base = values(baseline["model_name"])
    rows = [
        (
            name,
            _number(mine[name][0]),
            _number(mine[name][1]),
            _number(base.get(name, (None, None))[0]),
        )
        for name in sorted(mine)
    ]
    heads = [
        "metric",
        f"{selected['model_name']} held-out",
        f"{selected['model_name']} validation",
        f"{baseline['model_name']} held-out",
    ]
    return markdown_table(heads, rows)


def _intended_use_lines(task, selected) -> list:
    text = INTENDED_USE.get(task["task_id"])
    if text is None:
        return [f"- estimates `{task['target']}` on dataset `{task['dataset_id']}`"]
    lines = [
        f"- {text}",
        f"- target `{task['target']}`, dataset `{task['dataset_id']}`",
    ]
    if selected["restriction"]:
        lines.append(
            f"- restricted to {selected['restriction'].replace('_', ' ')} "
            "(see the conditions below)"
        )
    return lines


def render_model_card(task, selected, baseline, facts, spec, stamp) -> str:
    """One card for an approved model, with its baseline fallback. `facts` holds
    results, flags, eval_context, fit_context, params and extra lines."""
    rules = _loads(selected["rules"], [])
    conditions = _loads(selected["conditions"], [])
    segments = _loads(selected["segments"], [])
    notes = _loads(selected["notes"], [])
    limit = spec["degradation_relative"]
    override = f"yes, {selected['override_reason']}" if selected["overridden"] else "no"
    lines = [
        f"# Model card: {task['task_id']}",
        "",
        (
            f"_model `{selected['model_name']}`, dataset `{task['dataset_id']}`, "
            f"frozen version {selected['frozen_delta_version']}, primary metric "
            f"`{task['primary_metric']}` "
            f"({'higher' if task['higher_is_better'] else 'lower'} is better). "
            f"Generated: {stamp}_"
        ),
        "",
        "## Decision",
        "",
        f"- decision: `{selected['decision']}`",
        f"- restriction: `{selected['restriction'] or 'none'}`",
        f"- recommended by the rules: `{selected['recommended_decision']}`",
        f"- overridden by the owner: {override}",
        f"- decided by: {selected['decided_by']}",
        f"- rules applied: {_rule_ids(rules)}",
        "",
        "## Purpose and task",
        "",
        f"- paradigm: {task['paradigm']}; task type: {task['task_type']}",
        f"- target: `{task['target']}`",
        f"- baseline fallback: `{baseline['model_name']}`",
        "",
        "## Intended use",
        "",
        *_intended_use_lines(task, selected),
        "",
        "## Out of scope",
        "",
        "- use on entities, periods or ecosystems outside the frozen dataset",
        "- operational use of a model restricted to offline or diagnostic use",
        "- registration, versioning and serving are separate steps",
        "",
        "## Data",
        "",
        *_read_note_lines(facts.get("eval_context")),
        *facts.get("extra", []),
        "",
        "## Training",
        "",
        (
            f"- model family: {selected['model_name']}; MLflow run "
            f"`{selected['mlflow_run_id']}`"
        ),
        f"- parameters: {_cell(facts.get('params'), 600) or 'not recorded'}",
        f"- library versions: {_library_line(facts.get('fit_context'), selected['model_name'])}",
        "- processor type: not recorded for the training and evaluation runs",
        "",
        "## Metrics",
        "",
        _metric_table(selected, baseline, facts["results"]),
    ]
    if segments:
        lines += ["", "## Segments", ""]
        lines.append(
            markdown_table(
                ["segment", "skill", "decision", "detail"],
                [
                    (s["segment"], _number(s["skill"]), s["decision"], s["detail"])
                    for s in segments
                ],
            )
        )
    flags = facts.get("flags") or []
    lines += ["", "## Flags", ""]
    lines += _bullets(f"{f['flag']}: {f['detail']}" for f in flags)
    lines += ["", "## Conditions and known limits", ""] + _bullets(conditions)
    lines += ["", "## Disclosures", ""] + _bullets(notes)
    lines += [
        "",
        "## Revalidation trigger",
        "",
        (
            "- a new frozen dataset version, or error on newer data more than "
            f"{limit:.0%} above the validation error"
        ),
        "",
        "## Baseline fallback",
        "",
        (
            f"- `{baseline['model_name']}`: validation "
            f"{_number(baseline['validation_value'])}, held-out "
            f"{_number(baseline['evaluated_value'])} on `{task['primary_metric']}`"
        ),
    ]
    return scrub_private("\n".join(lines))


# COMMAND ----------

# DBTITLE 1,Imports
import hashlib as _hashlib_registry
import json as _json_registry
import math as _math_registry

# COMMAND ----------

# DBTITLE 1,Registry constants
LIFECYCLE = ("registered", "deprecated", "retired")
# Recorded for each new version: the code of the training run (a commit), the data it
# read, the model file and the task definition. Versions made before these columns
# existed hold NULL in all of them and are reported as not recorded.
VERSION_FIELDS = (
    "code_version",
    "data_fingerprint",
    "artifact_sha256",
    "task_spec_hash",
)
REQUIRED_VERSION_FIELDS = ("data_fingerprint", "artifact_sha256", "task_spec_hash")
COMMIT_TAGS = ("mlflow.source.git.commit", "mlflow.databricks.gitRepoCommit")
REGISTRY_ROLES = ("selected", "baseline_fallback")
CARDS_SUBDIR = "src/model_cards"
# a change in one of these fields creates a new version
LINK_FIELDS = (
    "decision",
    "restriction",
    "conditions",
    "mlflow_run_id",
    "frozen_delta_version",
    "approval_run_id",
    "card_path",
)
REPRODUCED = ("matched", "not_comparable")
CHECK_PASSED = "passed"
CHECK_FAILED = "failed"
_OTHER_ECOSYSTEM = OTHER_ECOSYSTEM_SUFFIXES

# COMMAND ----------

# DBTITLE 1,Table readers


def registry_rows(ecosystem: str) -> list:
    rows = read_model("model_registry", ecosystem=ecosystem).collect()
    return [r.asDict() for r in rows]


def check_rows(ecosystem: str) -> list:
    rows = read_model("registry_check", ecosystem=ecosystem).collect()
    return [r.asDict() for r in rows]


# COMMAND ----------

# DBTITLE 1,Entries the registry may hold


def card_path(ecosystem: str, task_id: str) -> str:
    return f"{CARDS_SUBDIR}/{ecosystem}/{task_id}.md"


def artifact_uri(run_id):
    return f"runs:/{run_id}/candidate/candidate.pkl" if run_id else None


def _entry_fields(row, fit_versions, versions=None) -> dict:
    tid = row["task_id"]
    recorded = (versions or {}).get((tid, row["model_name"], row["role"]), {})
    return {
        "task_id": tid,
        "dataset_id": row["dataset_id"],
        "model_name": row["model_name"],
        "role": row["role"],
        "decision": row["decision"],
        "restriction": row["restriction"],
        "conditions": row["conditions"],
        "primary_metric": row["primary_metric"],
        "validation_value": row["validation_value"],
        "frozen_delta_version": row["frozen_delta_version"],
        "mlflow_run_id": row["mlflow_run_id"],
        "artifact_uri": artifact_uri(row["mlflow_run_id"]),
        "approval_run_id": row["run_id"],
        "approval_decided_at": row["decided_at"],
        "card_path": card_path(TASK_BY_ID[tid]["ecosystem"], tid),
        "fit_library_versions": fit_versions.get(tid),
        **{field: recorded.get(field) for field in VERSION_FIELDS},
        "replaces": None,
    }


def registry_entries(approvals, fit_versions, versions=None) -> list:
    """The approved selected model of each task and the baseline fallback of that
    task, as registry fields. Nothing else may be registered."""
    out = []
    for sel in approvals:
        if sel["role"] != "selected" or sel["decision"] not in APPROVED:
            continue
        if sel["decided_by"] != DECIDER_OWNER:
            continue
        fallback = [
            a
            for a in approvals
            if a["task_id"] == sel["task_id"] and a["role"] == "baseline_fallback"
        ]
        out += [_entry_fields(r, fit_versions, versions) for r in (sel, *fallback)]
    return out


# COMMAND ----------

# DBTITLE 1,Version planning


def _entry_key(row) -> tuple:
    return (row["task_id"], row["model_name"], row["role"])


def latest_versions(rows) -> dict:
    """The highest version row of each (task, model, role)."""
    best: dict = {}
    for r in rows:
        key = _entry_key(r)
        if key not in best or r["version"] > best[key]["version"]:
            best[key] = r
    return best


def _replaced_entries(latest, wanted) -> list:
    """A retired version of each entry whose (task, role) the approval gives to
    another model. Only the registry roles are replaced."""
    taken = {
        (w["task_id"], w["role"]): w for w in wanted if w["role"] in REGISTRY_ROLES
    }
    out = []
    for key, cur in sorted(latest.items()):
        task_id, model_name, role = key
        new_owner = taken.get((task_id, role))
        if (
            new_owner is None
            or new_owner["model_name"] == model_name
            or cur["lifecycle_status"] == "retired"
        ):
            continue
        out.append(
            {
                **cur,
                "version": cur["version"] + 1,
                "lifecycle_status": "retired",
                "reason": (
                    f"replaced by {new_owner['model_name']} "
                    f"(approval run {new_owner['approval_run_id']})"
                ),
            }
        )
    return out


def _replaced_links(replaced_rows) -> dict:
    """{(task, role): ["model@vN", ...]}: what the retired rows of a plan replace."""
    links: dict = {}
    for row in replaced_rows:
        key = (row["task_id"], row["role"])
        links.setdefault(key, []).append(f"{row['model_name']}@v{row['version'] - 1}")
    return {key: sorted(found) for key, found in links.items()}


def plan_versions(existing, wanted, changes, rid, now) -> list:
    """New registry rows only. A first entry is version 1. A changed link creates the
    next version with the changed fields as reason. A lifecycle change from `changes`
    ({(task, model, role): {"status", "reason"}}) creates the next version too. A
    version row is never edited."""
    latest = latest_versions(existing)
    replaced = _replaced_entries(latest, wanted)
    links = _replaced_links(replaced)
    new = []
    for w in wanted:
        key = _entry_key(w)
        cur = latest.get(key)
        if cur is None:
            new.append(
                {
                    **w,
                    "version": 1,
                    "lifecycle_status": "registered",
                    "reason": w.get("reason") or "first registration",
                    "replaces": ", ".join(links.get((w["task_id"], w["role"]), []))
                    or None,
                }
            )
            continue
        diff = [f for f in LINK_FIELDS if cur[f] != w[f]]
        if diff:
            new.append(
                {
                    **w,
                    "version": cur["version"] + 1,
                    "lifecycle_status": cur["lifecycle_status"],
                    "reason": f"link changed: {', '.join(diff)}",
                    "replaces": cur.get("replaces"),
                }
            )
    new += replaced
    pending = {**latest, **{_entry_key(r): r for r in new}}
    for key, change in sorted(changes.items()):
        cur = pending.get(key)
        if cur is None:
            raise ValueError(f"lifecycle change for an entry not registered: {key}")
        if change["status"] not in LIFECYCLE:
            raise ValueError(f"lifecycle status not valid: {key}, {change['status']}")
        reason = str(change.get("reason") or "").strip()
        if not reason:
            raise ValueError(f"lifecycle change without a reason: {key}")
        if change["status"] == cur["lifecycle_status"]:
            continue
        row = {
            **cur,
            "version": cur["version"] + 1,
            "lifecycle_status": change["status"],
            "reason": reason,
        }
        new.append(row)
        pending[key] = row
    return [{**r, "run_id": rid, "registered_at": now} for r in new]


# COMMAND ----------

# DBTITLE 1,The version fields of a registered model


def task_spec_hash(task_id: str) -> str:
    """SHA-256 of the task definition (dataset, target, kind, baseline, metric)."""
    text = _json_registry.dumps(TASK_BY_ID[task_id], sort_keys=True, default=str)
    return _hashlib_registry.sha256(text.encode("utf-8")).hexdigest()


def data_fingerprint(task_id: str, version: int) -> str:
    """A fingerprint of the frozen dataset at its Delta version: the row count, the
    schema, and the sum of a hash of the partition and the target of every row. It
    reads two columns. A changed or truncated dataset gives another value."""
    task = TASK_BY_ID[task_id]
    full = ml_fqn(f"dataset_{task['dataset_id']}", task["ecosystem"])
    df = spark.sql(f"SELECT * FROM {full} VERSION AS OF {int(version)}")
    columns = [F.col("partition")]
    if task["target"] in df.columns:
        columns.append(F.col(task["target"]))
    hashed = F.pmod(F.xxhash64(*columns), F.lit(2147483647))
    row = df.agg(F.count(F.lit(1)).alias("n"), F.sum(hashed).alias("h")).first()
    schema = _hashlib_registry.sha256(df.schema.json().encode("utf-8")).hexdigest()
    text = f"{row['n']}|{row['h']}|{schema}"
    return _hashlib_registry.sha256(text.encode("utf-8")).hexdigest()[:32]


def artifact_digest(run_id: str) -> str:
    """SHA-256 of the stored model file of a run, read in blocks."""
    import tempfile

    import mlflow

    with tempfile.TemporaryDirectory() as folder:
        path = mlflow.artifacts.download_artifacts(
            artifact_uri=artifact_uri(run_id), dst_path=folder
        )
        digest = _hashlib_registry.sha256()
        with open(path, "rb") as handle:
            while block := handle.read(8 * 1024 * 1024):
                digest.update(block)
        return digest.hexdigest()


def training_commit(run_id: str):
    """The git commit that MLflow recorded for the training run, or None."""
    import mlflow

    tags = mlflow.tracking.MlflowClient().get_run(run_id).data.tags
    return next((tags[t] for t in COMMIT_TAGS if tags.get(t)), None)


def version_fields_of(entry: dict) -> dict:
    """The four version fields of a new entry; each step prints its time."""
    import time as _time_registry

    out = {}
    for name, step in (
        ("task_spec_hash", lambda: task_spec_hash(entry["task_id"])),
        (
            "data_fingerprint",
            lambda: data_fingerprint(entry["task_id"], entry["frozen_delta_version"]),
        ),
        ("artifact_sha256", lambda: artifact_digest(entry["mlflow_run_id"])),
        ("code_version", lambda: training_commit(entry["mlflow_run_id"])),
    ):
        started = _time_registry.monotonic()
        out[name] = step()
        print(
            f"  {entry['task_id']} {entry['model_name']} {name}: "
            f"{_time_registry.monotonic() - started:.1f} s"
        )
    return out


def version_field_problems(rows) -> list:
    """(task, model, role, version, what) for a row that holds some of the required
    version fields but not all of them. A row with none is older than the fields."""
    problems = []
    for r in rows:
        held = [f for f in REQUIRED_VERSION_FIELDS if r.get(f)]
        if held and len(held) != len(REQUIRED_VERSION_FIELDS):
            lacking = [f for f in REQUIRED_VERSION_FIELDS if not r.get(f)]
            problems.append(
                (r["task_id"], r["model_name"], r["role"], r["version"], lacking)
            )
    return problems


def versions_not_recorded(rows) -> int:
    """How many version rows hold none of the required version fields."""
    return sum(1 for r in rows if not any(r.get(f) for f in REQUIRED_VERSION_FIELDS))


def version_field_mismatches(rows) -> list:
    """Recompute the fields of each row that holds them and list the differences.
    The artifact is read again, so only rows with a recorded hash cost time."""
    out = []
    for r in rows:
        if not r.get("data_fingerprint"):
            continue
        if r["task_spec_hash"] != task_spec_hash(r["task_id"]):
            out.append((r["task_id"], r["model_name"], r["version"], "task_spec_hash"))
        if r["data_fingerprint"] != data_fingerprint(
            r["task_id"], r["frozen_delta_version"]
        ):
            out.append(
                (r["task_id"], r["model_name"], r["version"], "data_fingerprint")
            )
        if r["artifact_sha256"] != artifact_digest(r["mlflow_run_id"]):
            out.append((r["task_id"], r["model_name"], r["version"], "artifact_sha256"))
    return out


# COMMAND ----------

# DBTITLE 1,Check of the planned registry


def planned_registry_problems(existing, planned, approvals) -> list:
    """What the registry guards would find after the planned rows are written:
    (task, what, detail) for every approved task without exactly one entry in each
    registry role, every registered entry that differs from its approval row and
    every registered entry of a task that is not approved."""
    latest = latest_versions([*existing, *planned])
    approved = {
        a["task_id"]
        for a in approvals
        if a["role"] == "selected" and a["decision"] in APPROVED
    }
    wanted = {_entry_key(w): w for w in registry_entries(approvals, {})}
    counts: dict = {}
    for r in latest.values():
        if r["lifecycle_status"] != "retired":
            key = (r["task_id"], r["role"])
            counts[key] = counts.get(key, 0) + 1
    out = []
    for task_id in sorted(approved):
        for role in REGISTRY_ROLES:
            n = counts.get((task_id, role), 0)
            if n != 1:
                out.append((task_id, f"{n} entries in role {role}", ""))
    for key, r in sorted(latest.items()):
        if r["lifecycle_status"] != "registered":
            continue
        if r["task_id"] not in approved:
            out.append((r["task_id"], "registered but not approved", r["model_name"]))
            continue
        w = wanted.get(key)
        diff = [f for f in LINK_FIELDS if w is None or r[f] != w[f]]
        if diff:
            out.append((r["task_id"], f"differs from the approval: {diff}", key[1]))
    return out


# COMMAND ----------

# DBTITLE 1,Environment record and reproducibility outcome


def environment_record() -> dict:
    """Processor type, Python version and library versions of this compute."""
    return {
        "processor_type": _platform.machine(),
        "python_version": _platform.python_version(),
        "library_versions": _json_registry.dumps(
            {name: library_version(name) for name in sorted(LIBRARIES)}, sort_keys=True
        ),
    }


def reproduction_outcome(recorded, rescored, tolerance, comparable=True):
    """(rescore status, relative gap) of one validation re-score against the
    recorded validation value."""
    if not comparable:
        return "not_comparable", None
    if recorded is None or not _math_registry.isfinite(recorded):
        return "no_recorded_value", None
    if rescored is None or not _math_registry.isfinite(rescored):
        return "score_failed", None
    gap = abs(rescored - recorded) / max(abs(recorded), 1e-12)
    return ("matched" if gap <= tolerance else "not_matched"), gap


def check_status(reload_status, rescore_status) -> str:
    ok = reload_status == "ok" and rescore_status in REPRODUCED
    return CHECK_PASSED if ok else CHECK_FAILED


def latest_checks(rows) -> dict:
    """The latest check row of each (task, model, role, processor type)."""
    best: dict = {}
    for r in rows:
        key = (*_entry_key(r), r["processor_type"])
        if key not in best or str(r["checked_at"]) > str(best[key]["checked_at"]):
            best[key] = r
    return best


# COMMAND ----------

# DBTITLE 1,Registry findings text


def _count_lines(rows) -> list:
    counts: dict = {}
    for r in rows:
        key = (r["role"], r["lifecycle_status"])
        counts[key] = counts.get(key, 0) + 1
    return [f"- {role}, {state}: {n}" for (role, state), n in sorted(counts.items())]


def _library_table(checks) -> str:
    """One row per library, one column per processor type, from the latest check."""
    by_type: dict = {}
    for r in sorted(checks, key=lambda r: str(r["checked_at"])):
        by_type[r["processor_type"]] = r
    types = sorted(by_type)
    versions = {
        t: _json_registry.loads(by_type[t]["library_versions"] or "{}") for t in types
    }
    names = sorted({n for v in versions.values() for n in v})
    header = ["item", *types]
    rows = [
        ("python", *[by_type[t]["python_version"] for t in types]),
        *[(n, *[versions[t].get(n) or "not installed" for t in types]) for n in names],
    ]
    return markdown_table(header, rows)


def _check_order(c) -> tuple:
    return (c["task_id"], c["role"], c["model_name"], c["processor_type"])


def render_registry_findings(eco, registry, checks, results, stamp) -> str:
    """The registry and the reproducibility record of one ecosystem as markdown."""
    latest = sorted(
        latest_versions(registry).values(),
        key=lambda r: (r["task_id"], r["role"], r["model_name"]),
    )
    current = latest_checks(checks)
    lines = [
        f"# {eco.upper()} REGISTRY FINDINGS",
        "",
        (
            "_Auto-generated by `databricks/models/register/03_registry_gate`. "
            "A registered version is never edited; a change creates a new version. "
            f"Generated: {stamp}_"
        ),
        "",
        "## summary",
        "",
        f"- entries (latest version of each): {len(latest)}",
        f"- version rows in total: {len(registry)}",
        (
            "- version rows without recorded code, data and file versions (made "
            f"before the columns existed): {versions_not_recorded(registry)}"
        ),
        *_count_lines(latest),
        f"- processor types checked: {sorted({c['processor_type'] for c in checks})}",
        "",
        "## registry (latest version of each entry)",
        "",
        markdown_table(
            [
                "task",
                "model",
                "role",
                "version",
                "lifecycle",
                "decision",
                "restriction",
                "frozen version",
                "run id",
                "card",
            ],
            [
                (
                    r["task_id"],
                    r["model_name"],
                    r["role"],
                    r["version"],
                    r["lifecycle_status"],
                    r["decision"],
                    r["restriction"] or "-",
                    r["frozen_delta_version"],
                    r["mlflow_run_id"],
                    r["card_path"],
                )
                for r in latest
            ],
        ),
        "",
        "## version history (a version above 1)",
        "",
        markdown_table(
            ["task", "model", "role", "version", "lifecycle", "reason", "registered"],
            [
                (
                    r["task_id"],
                    r["model_name"],
                    r["role"],
                    r["version"],
                    r["lifecycle_status"],
                    r["reason"],
                    r["registered_at"],
                )
                for r in sorted(registry, key=lambda r: (*_entry_key(r), r["version"]))
                if r["version"] > 1
            ],
        ),
        "",
        "## reproducibility checks (latest of each entry and processor type)",
        "",
        markdown_table(
            [
                "task",
                "model",
                "role",
                "version",
                "processor",
                "reload",
                "re-score",
                "recorded",
                "re-scored",
                "gap",
                "tolerance",
                "status",
                "detail",
            ],
            [
                (
                    c["task_id"],
                    c["model_name"],
                    c["role"],
                    c["registry_version"],
                    c["processor_type"],
                    c["reload_status"],
                    c["rescore_status"],
                    _number(c["recorded_value"]),
                    _number(c["rescored_value"]),
                    _number(c["gap_relative"]),
                    _number(c["tolerance"]),
                    c["status"],
                    c["detail"] or c["reload_detail"],
                )
                for c in sorted(current.values(), key=_check_order)
            ],
        ),
        "",
        "## environment of the checks",
        "",
        _library_table(checks) if checks else "- no check recorded",
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
                for c in sorted(results, key=lambda c: str(c["recorded_at"]))
                if not c["metric_name"].endswith(_OTHER_ECOSYSTEM[eco])
            ],
        ),
    ]
    return scrub_private("\n".join(lines))


# COMMAND ----------

# DBTITLE 1,Tolerance of the re-score


def tolerance_for(model_name: str, thresholds: dict) -> float:
    return thresholds["reproduction_tolerance_relative"]


# COMMAND ----------

# DBTITLE 1,Check context and the validation frame


class ReproContext(EvalContext):
    """One task scored on the validation partition."""

    def __init__(self, task_id: str, rid: str):
        super().__init__(task_id, rid, smoke=False)
        self.partition = "validation"
        self.component = f"models/register/{task_id}"


def validation_frame(ctx: ReproContext) -> DataFrame:
    """The validation rows of the task; only the earlier partitions are kept."""
    frame = task_frame(ctx, ctx.partition)
    return frame.filter(F.col("partition").isin(*ALLOWED_PARTITIONS))


# COMMAND ----------

# DBTITLE 1,One check row


def check_row(entry: dict, env: dict, rid: str, now, **fields) -> dict:
    row = {
        "task_id": entry["task_id"],
        "model_name": entry["model_name"],
        "role": entry["role"],
        "registry_version": int(entry["version"]),
        **env,
        "reload_status": None,
        "reload_detail": None,
        "rescore_status": None,
        "recorded_value": entry["validation_value"],
        "rescored_value": None,
        "gap_relative": None,
        "tolerance": None,
        "status": CHECK_FAILED,
        "detail": None,
        "run_id": rid,
        "checked_at": now,
    }
    return {**row, **fields}


def _fail_text(exc: Exception) -> str:
    return f"{type(exc).__name__}: {str(exc)[:300]}"


# COMMAND ----------

# DBTITLE 1,Check the models of one task


def _score_entry(evaluator, ctx, bundles, entry, make) -> dict:
    """Score one reloaded model on the validation rows and compare."""
    if not evaluator.can_reproduce:
        return make(
            entry,
            reload_status="ok",
            rescore_status="not_comparable",
            status=check_status("ok", "not_comparable"),
            detail="events are drawn again, the validation value cannot be reproduced",
        )
    try:
        sc = evaluator.score(bundles[entry["model_name"]], "validation", bundles)
        value = sc.metrics.get(ctx.primary_metric)
        sc.release()
    except Exception as exc:
        return make(
            entry,
            reload_status="ok",
            rescore_status="score_failed",
            detail=_fail_text(exc),
        )
    tolerance = tolerance_for(entry["model_name"], ctx.thresholds)
    status, gap = reproduction_outcome(entry["validation_value"], value, tolerance)
    return make(
        entry,
        reload_status="ok",
        rescore_status=status,
        rescored_value=_finite(value),
        gap_relative=gap,
        status=check_status("ok", status),
        detail=f"{ctx.primary_metric} on validation",
    )


def reproduce_task(task_id: str, entries: list, rid: str) -> list:
    """One check row per registered entry of the task."""
    env, now = environment_record(), now_utc()
    ctx = ReproContext(task_id, rid)

    def make(entry, **fields):
        limit = tolerance_for(entry["model_name"], ctx.thresholds)
        fields.setdefault("tolerance", limit)
        return check_row(entry, env, rid, now, **fields)

    cfg = EVAL_TASKS[task_id]
    evaluator = globals()[EVALUATOR_NAMES[cfg["kind"]]](ctx, cfg)
    models = forwarded_models(ctx)
    by_name = {m["model_name"]: m for m in models}
    base_name = next(m["model_name"] for m in models if m["rank"] == 0)
    bundles, errors, rows = {}, {}, []
    try:
        if evaluator.can_reproduce:
            frame = validation_frame(ctx)
            evaluator.load(frame, frame, row_fraction_of(models))
            evaluator.recorded = {n: m["validation"] for n, m in by_name.items()}
        wanted = {base_name, *(e["model_name"] for e in entries)}
        if evaluator.needs_all_bundles:
            wanted |= set(by_name)
        for name in sorted(wanted):
            try:
                bundles[name] = load_bundle(by_name[name])
            except Exception as exc:
                errors[name] = _fail_text(exc)
        if evaluator.can_reproduce and base_name in bundles:
            evaluator.attach(bundles, base_name)
        for entry in entries:
            name = entry["model_name"]
            if name in errors:
                rows.append(
                    make(
                        entry,
                        reload_status="failed",
                        reload_detail=errors[name],
                        rescore_status="not_scored",
                        detail="the stored model did not reload",
                    )
                )
            else:
                rows.append(_score_entry(evaluator, ctx, bundles, entry, make))
    finally:
        evaluator.frames = {"eval": None, "validation": None}
        evaluator.sampler = None
        bundles.clear()
        del evaluator
        _take_notes()
        _gc.collect()
    return rows


# COMMAND ----------

# DBTITLE 1,Check every registered entry of one ecosystem


def reproduce_ecosystem(ecosystem: str, rid: str) -> list:
    """Check rows for the latest version of every entry that is not retired. A task
    that fails as a whole gets a failed row for each of its entries."""
    latest = latest_versions(registry_rows(ecosystem))
    by_task: dict = {}
    for entry in latest.values():
        if entry["lifecycle_status"] != "retired":
            by_task.setdefault(entry["task_id"], []).append(entry)
    env, rows = environment_record(), []
    for task in (t for t in TASKS if t["ecosystem"] == ecosystem):
        tid = task["task_id"]
        if tid not in by_task:
            continue
        try:
            got = reproduce_task(tid, by_task[tid], rid)
        except Exception as exc:
            got = [
                check_row(
                    e,
                    env,
                    rid,
                    now_utc(),
                    reload_status="failed",
                    reload_detail=_fail_text(exc),
                    rescore_status="not_scored",
                    detail="the task could not be checked",
                )
                for e in by_task[tid]
            ]
        for r in got:
            mark = "OK  " if r["status"] == CHECK_PASSED else "FAIL"
            print(
                f"{mark} {tid} {r['model_name']} ({r['role']}): reload "
                f"{r['reload_status']}, re-score {r['rescore_status']}, "
                f"gap {r['gap_relative']}"
            )
        rows += got
    return rows


def record_checks(ecosystem: str, rows: list) -> None:
    """Replace the check rows of this processor type; rows of other processor types
    stay."""
    if not rows:
        return
    cols = row_columns("registry_check", ecosystem=ecosystem)
    tuples = [tuple(r[c] for c in cols) for r in rows]
    replace_model_rows(
        spark.createDataFrame(tuples, MODEL_DDL["registry_check"]),
        "registry_check",
        ecosystem=ecosystem,
        predicate=f"processor_type = '{rows[0]['processor_type']}'",
    )
