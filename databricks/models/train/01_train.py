# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # MODEL TRAINING
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** baseline and candidates of every task of the task file on the frozen
# MAGIC datasets (train and validation partitions only), one cell per paradigm. A task
# MAGIC that fails does not stop the others; the last cell fails the run when any task
# MAGIC failed. A task that finished earlier with the same frozen data and the same task
# MAGIC file entry is skipped (RETRAIN = True trains every task again). The findings of
# MAGIC each ecosystem are written at the end.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Model fit library
# MAGIC %run ../lib/_model_fit

# COMMAND ----------

# DBTITLE 1,Run settings (SMOKE = short test on a small sample; RETRAIN = ignore earlier runs)
SMOKE = False
# False: a task that finished earlier with the same frozen data and the same task file
# entry is skipped. True: every task trains again.
RETRAIN = False

# COMMAND ----------

# DBTITLE 1,Run identity
COMPONENT = "models/train/01_train"
rid = model_run_id()
failed = []
skipped = []


def run_tasks(tasks):
    """Run each task; a failure is recorded and the next task still runs. A task
    that finished earlier with the same data and settings is skipped."""
    for t in tasks:
        task_id = t["task_id"]
        try:
            if not RETRAIN and train_is_current(task_id, rid, SMOKE):
                skipped.append(task_id)
                print(f"SKIP {task_id}: finished earlier, same data and settings")
                continue
            run_task(task_id, rid, SMOKE)
        except Exception as exc:
            failed.append((task_id, f"{type(exc).__name__}: {str(exc)[:300]}"))
            print(f"FAIL {task_id}: {failed[-1][1]}")


# COMMAND ----------

# DBTITLE 1,Tabular tasks (regression, classification, multi-class)
run_tasks(tasks_of(("regression", "classification", "multiclass")))

# COMMAND ----------

# DBTITLE 1,Survival tasks
run_tasks(tasks_of("survival"))

# COMMAND ----------

# DBTITLE 1,Anomaly tasks
run_tasks(tasks_of("anomaly"))

# COMMAND ----------

# DBTITLE 1,Reconstruction tasks
run_tasks(tasks_of("reconstruction"))

# COMMAND ----------

# DBTITLE 1,Offline policy tasks
run_tasks(tasks_of(("pumped_storage", "action_policy", "session_policy")))

# COMMAND ----------

# DBTITLE 1,Ranking tasks
run_tasks(tasks_of("ranking"))

# COMMAND ----------

# DBTITLE 1,Write the findings of this notebook
for eco in ECOSYSTEMS:
    write_model_findings(
        (eco,),
        "train__01_train",
        "Model Training",
        ["candidate_results", "task_run_context"],
        rid,
        task_ids=[t["task_id"] for t in TASKS if t["ecosystem"] == eco],
        smoke=SMOKE,
        notes=skip_notes(skipped, "finished earlier with the same data and settings"),
    )

# COMMAND ----------

# DBTITLE 1,Every task ran
guard(COMPONENT, rid, "all_tasks_ran", failed, "tasks that failed")
