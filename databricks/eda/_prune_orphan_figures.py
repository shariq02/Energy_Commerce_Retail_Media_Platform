# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Prune orphaned profiling figures
# MAGIC
# MAGIC **Energy Commerce and Retail Media Analytics Platform**
# MAGIC
# MAGIC The EDA notebooks write PNGs into `src/schemas/profiling/figures/` but
# MAGIC never delete old ones. After the figure-consolidation rework, and after
# MAGIC every re-run, some files on disk are no longer referenced by any
# MAGIC `src/schemas/profiling/<source>.md`. Run this once after re-running the
# MAGIC EDA notebooks to delete the unreferenced PNGs.
# MAGIC
# MAGIC Set `DRY_RUN = False` to actually delete.

# COMMAND ----------

import os
import re

DRY_RUN = False  # True

# COMMAND ----------


def _repo_root():
    p = os.path.abspath(os.getcwd())
    for _ in range(12):
        if os.path.isdir(os.path.join(p, "src", "schemas")) and os.path.isdir(
            os.path.join(p, "databricks", "eda")
        ):
            return p
        if os.path.dirname(p) == p:
            break
        p = os.path.dirname(p)
    raise RuntimeError("repo root not found -- run from inside the repo")


PROF_DIR = os.path.join(_repo_root(), "src", "schemas", "profiling")
FIG_DIR = os.path.join(PROF_DIR, "figures")

referenced = set()
for name in os.listdir(PROF_DIR):
    if name.endswith(".md"):
        with open(os.path.join(PROF_DIR, name), encoding="utf-8") as fh:
            text = fh.read()
        referenced.update(re.findall(r"figures/([A-Za-z0-9_.\-]+\.png)", text))

on_disk = {f for f in os.listdir(FIG_DIR) if f.endswith(".png")}
orphans = sorted(on_disk - referenced)
missing = sorted(referenced - on_disk)

print(f"referenced by .md : {len(referenced)}")
print(f"present on disk   : {len(on_disk)}")
print(f"orphaned on disk  : {len(orphans)}")
for f in orphans:
    print(f"  orphan  {f}")
for f in missing:
    print(f"  MISSING (referenced, not on disk)  {f}")

# COMMAND ----------

if DRY_RUN:
    print("DRY_RUN=True -- nothing deleted. Set DRY_RUN=False to prune.")
else:
    for f in orphans:
        os.remove(os.path.join(FIG_DIR, f))
    print(f"deleted {len(orphans)} orphaned figure(s)")
