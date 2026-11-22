# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # MODEL CARDS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** write one model card per approved task to the repository
# MAGIC (`src/model_cards/`), from the recorded decisions, evaluation results,
# MAGIC flags and run context. The cards are committed, so private paths and e-mail
# MAGIC addresses are removed.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Model governance library
# MAGIC %run ../lib/_model_govern

# COMMAND ----------

# DBTITLE 1,Imports
import os as _os

# COMMAND ----------

# DBTITLE 1,Configuration
# the cards sit next to the findings: src/model_cards beside src/findings
CARDS_DIR = _os.path.join(
    _os.path.dirname(_os.path.dirname(_findings_dir())), "model_cards"
)
print(f"OK  cards directory: {CARDS_DIR}")
RESULT_FIELDS = (
    "task_id",
    "model_name",
    "metric",
    "value",
    "validation_value",
    "n_rows",
)
CONTEXT_FIELDS = (
    "task_id",
    "status",
    "run_id",
    "data_notes",
    "library_versions",
    "recorded_at",
)

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Event counts of the survival dataset, train and validation only


def survival_event_lines(task_id: str) -> list:
    ctx = TaskContext(task_id, rid, record=False)
    counts = (
        read_frozen(ctx, apply_smoke=False)
        .groupBy("partition")
        .agg(
            F.count("*").alias("rows"),
            F.sum(F.col("target_event").cast("int")).alias("events"),
        )
        .collect()
    )
    lines = [
        f"- events in the frozen data, {r['partition']}: {r['events']} of "
        f"{r['rows']} rows"
        for r in sorted(counts, key=lambda r: r["partition"])
    ]
    return [*lines, "- events in the held-out partition: not recorded"]


EXTRA_FACTS = {t["task_id"]: survival_event_lines for t in tasks_of("survival")}

# COMMAND ----------

# DBTITLE 1,Read the decisions and the evidence
evidence = {}
for eco in ECOSYSTEMS:
    spec = spec_from_rows(read_model("approval_spec", ecosystem=eco).collect())
    approvals = [
        r.asDict() for r in read_model("model_approval", ecosystem=eco).collect()
    ]
    results = [
        r.asDict()
        for r in read_model("evaluation_results", ecosystem=eco)
        .filter(~F.col("smoke") & (F.col("status") == "ok"))
        .select(*RESULT_FIELDS)
        .collect()
    ]
    flags = [
        r.asDict() for r in read_model("evaluation_flags", ecosystem=eco).collect()
    ]
    eval_context = [
        r.asDict()
        for r in read_model(APPROVAL_CONTEXT_TABLE, ecosystem=eco)
        .filter(~F.col("smoke"))
        .select(*CONTEXT_FIELDS)
        .collect()
    ]
    fit_context = [
        r.asDict()
        for r in read_model("task_run_context", ecosystem=eco)
        .filter(~F.col("smoke") & (F.col("status") == "finished"))
        .select(*CONTEXT_FIELDS)
        .collect()
    ]
    params = {
        (r["task_id"], r["model_name"]): r["params"]
        for r in read_model("candidate_results", ecosystem=eco)
        .filter(~F.col("smoke") & (F.col("status") == "ok"))
        .select("task_id", "model_name", "params")
        .distinct()
        .collect()
    }
    evidence[eco] = (spec, approvals, results, flags, eval_context, fit_context, params)
    print(f"{eco}: {len(approvals)} decision row(s)")

# COMMAND ----------

# DBTITLE 1,Build the cards of the approved tasks
cards = {}
stamp = now_utc().strftime("%Y-%m-%dT%H:%MZ")
for eco, (
    spec,
    approvals,
    results,
    flags,
    eval_ctx,
    fit_ctx,
    params,
) in evidence.items():
    cards[eco] = {}
    for sel in (a for a in approvals if a["role"] == "selected"):
        if sel["decision"] not in APPROVED or sel["decided_by"] != DECIDER_OWNER:
            continue
        tid = sel["task_id"]
        base = next(
            a
            for a in approvals
            if a["task_id"] == tid and a["role"] == "baseline_fallback"
        )
        facts = {
            "results": [r for r in results if r["task_id"] == tid],
            "flags": [
                f
                for f in flags
                if f["task_id"] == tid and f["model_name"] == sel["model_name"]
            ],
            "eval_context": [c for c in eval_ctx if c["task_id"] == tid],
            "fit_context": [c for c in fit_ctx if c["task_id"] == tid],
            "params": params.get((tid, sel["model_name"])),
            "extra": EXTRA_FACTS[tid](tid) if tid in EXTRA_FACTS else [],
        }
        cards[eco][tid] = render_model_card(
            TASK_BY_ID[tid], sel, base, facts, spec, stamp
        )
print({eco: len(v) for eco, v in cards.items()})

# COMMAND ----------

# DBTITLE 1,Write the cards and remove cards of tasks no longer approved
for eco, items in cards.items():
    folder = _os.path.join(CARDS_DIR, eco)
    _os.makedirs(folder, exist_ok=True)
    for name in sorted(_os.listdir(folder)):
        if name.endswith(".md") and name.removesuffix(".md") not in items:
            _os.remove(_os.path.join(folder, name))
            print(f"removed {eco}/{name}")
    for tid, text in items.items():
        with open(_os.path.join(folder, f"{tid}.md"), "w", encoding="utf-8") as fh:
            fh.write(text + "\n")
        print(f"OK  {eco}/{tid}.md")

# COMMAND ----------

# DBTITLE 1,Write the findings of this notebook
write_model_findings(
    ECOSYSTEMS,
    "approve__03_model_cards",
    "Model Cards",
    [],
    rid,
    notes={
        eco: [f"{len(items)} card(s) written to src/model_cards/{eco}"]
        for eco, items in cards.items()
    },
)
