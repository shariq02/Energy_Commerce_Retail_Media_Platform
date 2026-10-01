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
# MAGIC library. Libraries that already import are skipped; set `refresh` to `true` to
# MAGIC reinstall everything.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ./_model_common

# COMMAND ----------

# DBTITLE 1,Configuration
import importlib.machinery
import os
import shutil
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor

dbutils.widgets.text("refresh", "false")
REFRESH = dbutils.widgets.get("refresh").lower() == "true"
# import probe per package: a half-copied folder passes `import torch`, not `torch.nn`
PACKAGES = {
    "lightgbm": "lightgbm",
    "xgboost": "xgboost",
    "lifelines": "lifelines",
    "scikit-survival": "sksurv.ensemble",
    "torch": "torch.nn",
}
# release that matches the environment's scikit-learn 1.6
PIP_SPECS = {"scikit-survival": "scikit-survival>=0.24,<0.26"}
FOLDER = library_folder()
TORCH_CPU_INDEX = "https://download.pytorch.org/whl/cpu"
INSTALL_TIMEOUT_SECONDS = 600
COPY_THREADS = 32

# COMMAND ----------

# DBTITLE 1,Create the volume
spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{MODEL_SCHEMAS['energy']}.{LIBRARY_VOLUME}"
)
if REFRESH:
    dbutils.fs.rm(FOLDER, True)
os.makedirs(FOLDER, exist_ok=True)
print(f"OK  volume ready: {FOLDER}")

# COMMAND ----------


# DBTITLE 1,Define the import check, run in a fresh interpreter
def imports_from_folder(module: str) -> bool:
    code = f"import sys; sys.path.append({FOLDER!r}); import {module}"
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, check=False
    )
    return result.returncode == 0


# COMMAND ----------

# DBTITLE 1,Which libraries are missing
missing = {
    pkg: mod for pkg, mod in PACKAGES.items() if REFRESH or not imports_from_folder(mod)
}
print(f"already importable: {[p for p in PACKAGES if p not in missing]}")
print(f"to install: {list(missing)}")

# COMMAND ----------

# DBTITLE 1,Stop when nothing is missing
if not missing:
    dbutils.notebook.exit("all model libraries already installed")

# COMMAND ----------

# DBTITLE 1,Define the install helpers
_BASE_PATH = [p for p in sys.path if p != FOLDER]


def in_base_environment(module: str) -> bool:
    return importlib.machinery.PathFinder.find_spec(module, _BASE_PATH) is not None


def pip_install(pkg: str, target: str, index: str | None) -> bool:
    cmd = [sys.executable, "-m", "pip", "install", "-q", "--no-compile"]
    cmd += ["--disable-pip-version-check", "--target", target]
    cmd += ["--index-url", index] if index else []
    cmd.append(pkg)
    try:
        result = subprocess.run(cmd, timeout=INSTALL_TIMEOUT_SECONDS, check=False)
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        return False


def drop_what_the_environment_has(target: str) -> None:
    dropped = set()
    for name in os.listdir(target):
        path = os.path.join(target, name)
        is_cuda = name.startswith(("nvidia", "triton"))
        is_base = os.path.isdir(path) and in_base_environment(name)
        if is_cuda or (is_base and not name.endswith(".dist-info")):
            shutil.rmtree(path, ignore_errors=True)
            dropped.add(name)
    for name in os.listdir(target):
        top = os.path.join(target, name, "top_level.txt")
        if name.endswith(".dist-info") and os.path.isfile(top):
            with open(top, encoding="utf-8") as handle:
                modules = {line.strip() for line in handle if line.strip()}
            if modules and modules <= dropped:
                shutil.rmtree(os.path.join(target, name), ignore_errors=True)


def clear_old_versions(target: str, folder: str) -> None:
    """Remove what an earlier install of the same packages left in the folder."""
    for name in os.listdir(target):
        if name.endswith(".dist-info"):
            stem = name.rsplit("-", 1)[0]
            for old in os.listdir(folder):
                if old.endswith(".dist-info") and old.rsplit("-", 1)[0] == stem:
                    dbutils.fs.rm(os.path.join(folder, old), True)
        elif os.path.exists(os.path.join(folder, name)):
            dbutils.fs.rm(os.path.join(folder, name), True)


def copy_tree(src: str, dst: str) -> int:
    jobs = []
    for root, _, files in os.walk(src):
        out = os.path.join(dst, os.path.relpath(root, src))
        os.makedirs(out, exist_ok=True)
        jobs += [(os.path.join(root, f), os.path.join(out, f)) for f in files]
    with ThreadPoolExecutor(COPY_THREADS) as pool:
        list(pool.map(lambda j: shutil.copyfile(*j), jobs))
    return len(jobs)


# COMMAND ----------

# DBTITLE 1,Install each missing package into a local folder, keep only what is new, copy
for _pkg in missing:
    _tmp = tempfile.mkdtemp()
    _spec = PIP_SPECS.get(_pkg, _pkg)
    _ok = _pkg == "torch" and pip_install(_spec, _tmp, TORCH_CPU_INDEX)
    _ok = _ok or pip_install(_spec, _tmp, None)
    if _ok:
        drop_what_the_environment_has(_tmp)
        clear_old_versions(_tmp, FOLDER)
        print(f"OK  {_pkg}: {copy_tree(_tmp, FOLDER)} file(s) copied")
    else:
        print(f"WARN {_pkg}: install failed or timed out")
    shutil.rmtree(_tmp, ignore_errors=True)

# COMMAND ----------

# DBTITLE 1,Add the folder to the import path
use_model_libraries()
print(FOLDER in sys.path)

# COMMAND ----------

# DBTITLE 1,Which libraries import
for _pkg, _mod in PACKAGES.items():
    _ok = imports_from_folder(_mod)
    print(
        f"{'OK  ' if _ok else 'WARN'} {_pkg}: version {library_version(_mod.split('.')[0])}"
    )
