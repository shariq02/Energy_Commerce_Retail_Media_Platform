# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # APPROVAL GATE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** after the owner's decisions and the model cards, fail the run when a
# MAGIC task has no decision, a decision is not the owner's, an override has no reason, a
# MAGIC blocking rule is passed without an override, an approved model lacks its
# MAGIC evaluation at the frozen version or its stored artifact, an approved task has no
# MAGIC model card, a card holds a private path, an e-mail address or a planning
# MAGIC reference, or a genomics card or findings text holds an identifier. Then write
# MAGIC the decision record of each ecosystem to the repository as markdown.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Model governance library
# MAGIC %run ../lib/_model_govern

# COMMAND ----------

# DBTITLE 1,Imports
import os as _os
import re as _re_card

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/approve/04_approval_gate"
FINDINGS_DIR = _findings_dir()
CARDS_DIR = _os.path.join(
    _os.path.dirname(_os.path.dirname(FINDINGS_DIR)), "model_cards"
)
PRIVATE = _re_card.compile(r"/Users/(?!<user>)|[\w.+-]+@[\w-]+\.[\w.]+")
DOC_REFERENCE = _re_card.compile(r"doc[s]/|ADR-\d|UC-\d|\bPhase \d|\bEntry \d{3}")
# a genomics text states counts, rates and metrics only: no variant, gene or sample id
GENOMICS_IDENTIFIER = _re_card.compile(
    r"\brs\d{3,}\b|\bENS[GTP]\d{6,}|\bchr[0-9XYM]+[:_-]\d+|\b[NX][MPR]_\d{5,}|\bTCGA-"
)
print(f"OK  cards directory: {CARDS_DIR}")

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Read the decisions and the evaluation record
record = {}
for eco in ECOSYSTEMS:
    approvals = [
        r.asDict() for r in read_model("model_approval", ecosystem=eco).collect()
    ]
    evaluated = {
        (r["task_id"], r["model_name"]): r["frozen_delta_version"]
        for r in read_model("evaluation_results", ecosystem=eco)
        .filter(~F.col("smoke") & (F.col("status") == "ok"))
        .select("task_id", "model_name", "frozen_delta_version")
        .distinct()
        .collect()
    }
    spec = (
        read_model("approval_spec", ecosystem=eco).agg(F.min("recorded_at")).first()[0]
    )
    record[eco] = (approvals, evaluated, spec)
    print(f"{eco}: {len(approvals)} decision row(s)")

# COMMAND ----------

# DBTITLE 1,Every task has a selected model and a baseline fallback
_none = []
for t in TASKS:
    rows = record[t["ecosystem"]][0]
    roles = {r["role"] for r in rows if r["task_id"] == t["task_id"]}
    if not {"selected", "baseline_fallback"} <= roles:
        _none.append(t["task_id"])
guard(COMPONENT, rid, "every_task_has_a_decision", _none, "no decision")

# COMMAND ----------

# DBTITLE 1,Decision values are valid and every decision is the owner's
_rows = [r for approvals, _e, _s in record.values() for r in approvals]
guard(
    COMPONENT,
    rid,
    "decision_values_valid",
    [
        (r["task_id"], r["model_name"], r["decision"], r["restriction"])
        for r in _rows
        if r["decision"] not in DECISIONS
        or r["role"] not in ROLES
        or r["restriction"] not in (None, *RESTRICTIONS)
    ],
    "invalid values",
)
guard(
    COMPONENT,
    rid,
    "every_decision_is_the_owners",
    [
        (r["task_id"], r["model_name"])
        for r in _rows
        if r["decided_by"] != DECIDER_OWNER
    ],
    "decided by the rule engine only",
)

# COMMAND ----------

# DBTITLE 1,An override carries a reason and a blocking rule is passed only by one
guard(
    COMPONENT,
    rid,
    "override_has_a_reason",
    [
        (r["task_id"], r["model_name"])
        for r in _rows
        if r["overridden"] and not (r["override_reason"] or "").strip()
    ],
    "override without a reason",
)
_passed = []
for r in _rows:
    rules = _json.loads(r["rules"] or "[]")
    blocked = any(
        x["rule"] in BLOCKING_RULES and x["outcome"] == "blocking" for x in rules
    )
    justified = r["overridden"] and (r["override_reason"] or "").strip()
    approved = r["role"] == "selected" and r["decision"] in APPROVED
    if approved and blocked and not justified:
        _passed.append((r["task_id"], r["model_name"]))
guard(
    COMPONENT, rid, "blocking_rule_passed_only_by_override", _passed, "approved against"
)

# COMMAND ----------

# DBTITLE 1,An approved model has an evaluation at the frozen version and an artifact
_frozen = {t["task_id"]: frozen_version(t["dataset_id"], t["ecosystem"]) for t in TASKS}
_unsupported = []
for approvals, evaluated, _s in record.values():
    for r in approvals:
        if r["role"] != "selected" or r["decision"] not in APPROVED:
            continue
        key = (r["task_id"], r["model_name"])
        if not (evaluated.get(key) == _frozen[r["task_id"]] and r["mlflow_run_id"]):
            _unsupported.append(key)
guard(
    COMPONENT,
    rid,
    "approved_model_evaluated_at_frozen_version_with_artifact",
    _unsupported,
    "approved without evaluation or artifact",
)

# COMMAND ----------

# DBTITLE 1,Rules were recorded before the first recommendation
_late = []
for eco, (approvals, _e, recorded) in record.items():
    first = min((r["recommended_at"] for r in approvals), default=None)
    if recorded is None or (first is not None and recorded > first):
        _late.append(eco)
guard(COMPONENT, rid, "rules_recorded_before_first_recommendation", _late, "late")

# COMMAND ----------

# DBTITLE 1,Read the approved models and the cards on disk
approved = {}
cards = {}
for eco in ECOSYSTEMS:
    approved[eco] = {
        r["task_id"]: r["model_name"]
        for r in record[eco][0]
        if r["role"] == "selected" and r["decision"] in APPROVED
    }
    folder = _os.path.join(CARDS_DIR, eco)
    cards[eco] = {}
    if _os.path.isdir(folder):
        for name in _os.listdir(folder):
            if name.endswith(".md"):
                with open(_os.path.join(folder, name), encoding="utf-8") as fh:
                    cards[eco][name.removesuffix(".md")] = fh.read()
    print(f"{eco}: {len(approved[eco])} approved task(s), {len(cards[eco])} card(s)")

# COMMAND ----------

# DBTITLE 1,Cards: one per approved task, the approved model, no private text
guard(
    COMPONENT,
    rid,
    "every_approved_task_has_a_card",
    [(eco, t) for eco, tasks in approved.items() for t in tasks if t not in cards[eco]]
    + [
        (eco, t)
        for eco, found in cards.items()
        for t in found
        if t not in approved[eco]
    ],
    "missing or not approved",
)
guard(
    COMPONENT,
    rid,
    "card_names_the_approved_model",
    [
        (eco, t)
        for eco, found in cards.items()
        for t, text in found.items()
        if t in approved[eco] and f"model `{approved[eco][t]}`" not in text
    ],
    "card names another model",
)
guard(
    COMPONENT,
    rid,
    "cards_hold_no_private_or_planning_text",
    [
        (eco, t)
        for eco, found in cards.items()
        for t, text in found.items()
        if PRIVATE.search(text) or DOC_REFERENCE.search(text)
    ],
    "cards with private or planning text",
)

# COMMAND ----------

# DBTITLE 1,Genomics texts hold no variant, gene or sample identifier
_texts = {f"card {t}": text for t, text in cards.get("genomics", {}).items()}
for _name in sorted(_os.listdir(FINDINGS_DIR)) if _os.path.isdir(FINDINGS_DIR) else []:
    if _name.startswith("genomics") and _name.endswith(".md"):
        with open(_os.path.join(FINDINGS_DIR, _name), encoding="utf-8") as fh:
            _texts[f"findings {_name}"] = fh.read()
guard(
    COMPONENT,
    rid,
    "genomics_texts_hold_no_identifier",
    [name for name, text in _texts.items() if GENOMICS_IDENTIFIER.search(text)],
    "texts with an identifier",
)

# COMMAND ----------

# DBTITLE 1,Write the approval findings
_os.makedirs(FINDINGS_DIR, exist_ok=True)
checks = [
    r.asDict()
    for r in spark.table(AUDIT_TABLE)
    .filter((F.col("stage") == ML_STAGE) & F.col("component").like("models/approve/%"))
    .select("component", "metric_name", "status", "error_detail", "recorded_at")
    .collect()
]
checks = current_check_rows(checks, notebook_stems(("approve",)))
stamp = now_utc().strftime("%Y-%m-%dT%H:%MZ")
for eco in ECOSYSTEMS:
    rows = [r.asDict() for r in read_model("model_approval", ecosystem=eco).collect()]
    spec = [r.asDict() for r in read_model("approval_spec", ecosystem=eco).collect()]
    text = render_approval_findings(eco, rows, spec, checks, stamp)
    with open(
        _os.path.join(FINDINGS_DIR, f"{eco}_approval.md"), "w", encoding="utf-8"
    ) as fh:
        fh.write(text + "\n")
    print(f"OK  {eco}_approval.md")

# COMMAND ----------

# DBTITLE 1,Merge the findings of the notebooks
merge_notebook_findings()
