# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # MODEL EVALUATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** score the stored models forwarded by the candidate gate on the
# MAGIC held-out partition, exactly as stored, one cell per paradigm, and score the
# MAGIC check-only datasets (no train rows) with the model of the task that scores them.
# MAGIC A task that fails does not stop the others; the last cell fails the run when
# MAGIC any task failed.

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

# DBTITLE 1,Smoke setting (True = short test on a small sample of every dataset)
SMOKE = False

# COMMAND ----------

# DBTITLE 1,Run identity
COMPONENT = "models/evaluate/01_evaluate"
rid = model_run_id()
failed = []


def evaluate_tasks(tasks):
    """Evaluate each task; a failure is recorded and the next task still runs."""
    for t in tasks:
        try:
            evaluate_by_id(t["task_id"], rid, SMOKE)
        except Exception as exc:
            failed.append((t["task_id"], f"{type(exc).__name__}: {str(exc)[:300]}"))
            print(f"FAIL {t['task_id']}: {failed[-1][1]}")


# COMMAND ----------

# DBTITLE 1,Tabular tasks (regression, classification, multi-class)
evaluate_tasks(tasks_of(("regression", "classification", "multiclass")))

# COMMAND ----------

# DBTITLE 1,Check-only datasets, scored by the model of another task
for _name, _entry in CHECK_ONLY.items():
    try:
        evaluate_check_only(_name, rid, SMOKE)
    except Exception as exc:
        failed.append((_entry["task_id"], f"{type(exc).__name__}: {str(exc)[:300]}"))
        print(f"FAIL {_entry['task_id']}: {failed[-1][1]}")

# COMMAND ----------

# DBTITLE 1,Survival tasks
evaluate_tasks(tasks_of("survival"))

# COMMAND ----------

# DBTITLE 1,Anomaly tasks
evaluate_tasks(tasks_of("anomaly"))

# COMMAND ----------

# DBTITLE 1,Reconstruction tasks
evaluate_tasks(tasks_of("reconstruction"))

# COMMAND ----------

# DBTITLE 1,Offline policy tasks
evaluate_tasks(tasks_of(("pumped_storage", "action_policy", "session_policy")))

# COMMAND ----------

# DBTITLE 1,Ranking tasks
evaluate_tasks(tasks_of("ranking"))

# COMMAND ----------

# DBTITLE 1,Write the findings of this notebook
for eco in ECOSYSTEMS:
    write_model_findings(
        (eco,),
        "evaluate__01_evaluate",
        "Model Evaluation",
        ["evaluation_results", "evaluation_flags", EVAL_CONTEXT_TABLE],
        rid,
        task_ids=[
            *(t["task_id"] for t in TASKS if t["ecosystem"] == eco),
            *(c["task_id"] for c in CHECK_ONLY.values() if c["ecosystem"] == eco),
        ],
        smoke=SMOKE,
    )

# COMMAND ----------

# DBTITLE 1,Every task was evaluated
guard(COMPONENT, rid, "all_tasks_evaluated", failed, "tasks that failed")
