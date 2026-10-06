# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MODEL CARD GUARDS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** fail the run when an approved task has no model card, a card holds a workspace user folder, an e-mail address or a planning-document reference, or a card names a model other than the approved one.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Approval rules library
# MAGIC %run ../../lib/_approval_rules

# COMMAND ----------

# DBTITLE 1,Imports
import os as _os
import re as _re_card

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/approve/gate/02_approval_cards"
SOURCE = "models"
CARDS_SUBDIR = "src/model_cards"
PRIVATE = _re_card.compile(r"/Users/(?!<user>)|[\w.+-]+@[\w-]+\.[\w.]+")
DOC_REFERENCE = _re_card.compile(r"doc[s]/|ADR-\d|UC-\d|\bPhase \d|\bEntry \d{3}")

# COMMAND ----------

# DBTITLE 1,Repo-root discovery


def _repo_root():
    p = _os.path.abspath(_os.getcwd())
    for _ in range(12):
        if _os.path.isdir(_os.path.join(p, "src", "schemas")) and _os.path.isdir(
            _os.path.join(p, "databricks")
        ):
            return p
        if _os.path.dirname(p) == p:
            break
        p = _os.path.dirname(p)
    try:
        wp = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
    except Exception as exc:
        raise RuntimeError(
            "repo root not found -- run from inside the repo's Databricks Git folder"
        ) from exc
    i = wp.rfind("/databricks/")
    if i > 0:
        for cand in (wp[:i], "/Workspace" + wp[:i]):
            if _os.path.isdir(_os.path.join(cand, "src", "schemas")):
                return cand
    raise RuntimeError(
        "repo root not found -- run from inside the repo's Databricks Git folder"
    )


CARDS_DIR = _os.path.join(_repo_root(), CARDS_SUBDIR)
print(f"OK  cards directory: {CARDS_DIR}")

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Read the approved models and the cards on disk
approved = {}
cards = {}
for eco in ("energy", "commerce"):
    rows = read_model("model_approval", ecosystem=eco).collect()
    approved[eco] = {
        r["task_id"]: r["model_name"]
        for r in rows
        if r["role"] == "selected" and r["decision"] in APPROVED
    }
    folder = _os.path.join(CARDS_DIR, eco)
    cards[eco] = {}
    if _os.path.isdir(folder):
        for name in _os.listdir(folder):
            if name.endswith(".md"):
                path = _os.path.join(folder, name)
                with open(path, encoding="utf-8") as fh:
                    cards[eco][name.removesuffix(".md")] = fh.read()
    print(f"{eco}: {len(approved[eco])} approved task(s), {len(cards[eco])} card(s)")

# COMMAND ----------

# DBTITLE 1,Every approved task has a card and no card is left over
_missing = [
    (eco, t) for eco, tasks in approved.items() for t in tasks if t not in cards[eco]
]
_extra = [
    (eco, t) for eco, found in cards.items() for t in found if t not in approved[eco]
]
check(
    COMPONENT,
    SOURCE,
    "every_approved_task_has_a_card",
    not _missing and not _extra,
    detail=f"missing: {_missing}; not approved: {_extra}",
    metric_value=float(len(_missing) + len(_extra)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,A card names the approved model
_wrong = [
    (eco, t)
    for eco, found in cards.items()
    for t, text in found.items()
    if t in approved[eco] and f"model `{approved[eco][t]}`" not in text
]
check(
    COMPONENT,
    SOURCE,
    "card_names_the_approved_model",
    not _wrong,
    detail=f"card names another model: {_wrong}",
    metric_value=float(len(_wrong)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Cards hold no private path, e-mail address or planning reference
_private = [
    (eco, t)
    for eco, found in cards.items()
    for t, text in found.items()
    if PRIVATE.search(text) or DOC_REFERENCE.search(text)
]
check(
    COMPONENT,
    SOURCE,
    "cards_hold_no_private_or_planning_text",
    not _private,
    detail=f"cards with private or planning text: {_private}",
    metric_value=float(len(_private)),
    rid=rid,
)
