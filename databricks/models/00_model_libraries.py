# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MODEL LIBRARIES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** install the model libraries once into a Unity Catalog volume folder;
# MAGIC every model notebook adds that folder to its import path through the shared
# MAGIC library. Idempotent; re-running refreshes the packages.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ./_model_common

# COMMAND ----------

# DBTITLE 1,Configuration
import shutil
import subprocess
import sys
import tempfile

PACKAGES = ["lightgbm", "xgboost", "lifelines", "scikit-survival", "torch"]
FOLDER = library_folder()

# COMMAND ----------

# DBTITLE 1,Create the volume
spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{MODEL_SCHEMAS['energy']}.{LIBRARY_VOLUME}"
)
print(f"OK  volume ready: {FOLDER}")

# COMMAND ----------

# DBTITLE 1,Install one package at a time into a local folder, then copy
for _pkg in PACKAGES:
    _tmp = tempfile.mkdtemp()
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "-q", "--target", _tmp, _pkg],
        check=True,
    )
    shutil.copytree(_tmp, FOLDER, dirs_exist_ok=True)
    shutil.rmtree(_tmp)
    print(f"OK  {_pkg} copied to {FOLDER}")

# COMMAND ----------

# DBTITLE 1,Add the folder to the import path
use_model_libraries()
print(FOLDER in sys.path)

# COMMAND ----------

# DBTITLE 1,Which libraries import
for _name in ["lightgbm", "xgboost", "lifelines", "sksurv", "torch"]:
    _ok = library_available(_name)
    print(f"{'OK  ' if _ok else 'WARN'} {_name}: version {library_version(_name)}")
