# Databricks notebook source
# /// script
# [tool.databricks.environment]
# base_environment = "databricks_ai_v5"
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
# MAGIC %run ./lib/_model_common

# COMMAND ----------

# DBTITLE 1,Configuration
import importlib.machinery
import importlib.metadata
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
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
    "chronos-forecasting": "chronos",
    "timesfm": "timesfm",
    "granite-tsfm": "tsfm_public",
    "tabpfn": "tabpfn",
    "tabicl": "tabicl",
    "momentfm": "momentfm",
    "toto-2": "toto2",
}
# packages whose pinned dependencies cannot be built here: installed without them
NO_DEPENDENCIES = {"momentfm"}
# release that matches the environment's scikit-learn 1.6
PIP_SPECS = {
    "scikit-survival": "scikit-survival>=0.24,<0.26",
    "chronos-forecasting": "chronos-forecasting>=2.0,<3",
}
FOLDER = library_folder()
TORCH_CPU_INDEX = "https://download.pytorch.org/whl/cpu"
# a package that pulls its own torch must not overwrite the CPU build
TORCH_GROUP = ("torch", "torchgen", "functorch")
IMPORT_WORKERS = 4
POLL_SECONDS = 1
HEARTBEAT_SECONDS = 30
SLOW_IMPORT_SECONDS = 5
IMPORT_SECONDS = {}
ERROR_TEXT = {}
ERROR_ROWS = 15
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

# DBTITLE 1,Define the timer of a folder scan


def timed(label: str, call) -> None:
    started = time.monotonic()
    count = call()
    print(f"{label}: {count} item(s), {time.monotonic() - started:.1f} s", flush=True)


# COMMAND ----------

# DBTITLE 1,Time the listing of the library folder
timed("list folder", lambda: len(os.listdir(FOLDER)))

# COMMAND ----------

# DBTITLE 1,Time the package metadata scan of the library folder
timed(
    "metadata scan, folder only",
    lambda: sum(1 for _ in importlib.metadata.distributions(path=[FOLDER])),
)

# COMMAND ----------

# DBTITLE 1,Time the package metadata scan of the import path plus the library folder
timed(
    "metadata scan, import path + folder",
    lambda: sum(1 for _ in importlib.metadata.distributions(path=[*sys.path, FOLDER])),
)

# COMMAND ----------

# DBTITLE 1,Define the reader of the import timing lines


def read_importtime(job: dict) -> None:
    for line in job["proc"].stderr:
        fields = line.split("|")
        if not (line.startswith("import time:") and len(fields) == 3):
            job["text"].append(line)
        elif fields[1].strip().isdigit():
            job["last"] = fields[2].strip()
            seconds = int(fields[1]) / 1e6
            if seconds >= SLOW_IMPORT_SECONDS:
                job["slow"].append(f"{job['last']} {seconds:.0f} s")


# COMMAND ----------

# DBTITLE 1,Define the start of one import in a fresh interpreter


def start_import(module: str) -> dict:
    code = f"import sys; sys.path.append({FOLDER!r}); import {module}"
    cmd = [sys.executable, "-X", "importtime", "-c", code]
    proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, text=True)
    now = time.monotonic()
    job = {"module": module, "proc": proc, "last": "none", "slow": [], "text": []}
    job |= {"shown": 0, "started": now, "beat": now}
    job["reader"] = threading.Thread(target=read_importtime, args=(job,), daemon=True)
    job["reader"].start()
    return job


# COMMAND ----------

# DBTITLE 1,Define the progress lines of a running import


def show_progress(job: dict) -> None:
    for item in job["slow"][job["shown"] :]:
        print(f"     slow import {job['module']}: {item}", flush=True)
    job["shown"] = len(job["slow"])
    now = time.monotonic()
    if job["reader"].is_alive() and now - job["beat"] >= HEARTBEAT_SECONDS:
        job["beat"] = now
        waited = now - job["started"]
        print(
            f"     {job['module']}: {waited:.0f} s, last finished {job['last']}",
            flush=True,
        )


# COMMAND ----------

# DBTITLE 1,Define the import check, IMPORT_WORKERS at a time, no time limit


def check_imports(modules: list[str]) -> dict:
    """Return {module: None or the last line of the error}; the main thread prints."""
    waiting = list(modules)
    running = []
    errors = {}
    while waiting or running:
        while waiting and len(running) < IMPORT_WORKERS:
            running.append(start_import(waiting.pop(0)))
        time.sleep(POLL_SECONDS)
        for job in list(running):
            alive = job["reader"].is_alive()
            show_progress(job)
            if alive:
                continue
            job["proc"].wait()
            IMPORT_SECONDS[job["module"]] = time.monotonic() - job["started"]
            lines = "".join(job["text"]).strip().splitlines()
            failed = job["proc"].returncode != 0
            ERROR_TEXT[job["module"]] = "\n".join(lines) if failed else ""
            errors[job["module"]] = (
                (lines[-1] if lines else "no error text") if failed else None
            )
            running.remove(job)
    return errors


# COMMAND ----------

# DBTITLE 1,Define the import check of one module


def import_error(module: str) -> str | None:
    return check_imports([module])[module]


# COMMAND ----------

# DBTITLE 1,Which libraries are missing
missing = {}
errors = {}
results = {} if REFRESH else check_imports(list(PACKAGES.values()))
for _pkg, _mod in PACKAGES.items():
    _error = "refresh" if REFRESH else results[_mod]
    _line = f"MISS {_pkg}: {_error}" if _error else f"OK   {_pkg}"
    print(_line, flush=True)
    for _row in ERROR_TEXT.get(_mod, "").splitlines()[-ERROR_ROWS:]:
        print(f"     | {_row}", flush=True)
    if _error:
        missing[_pkg] = _mod
        errors[_pkg] = _error
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
    cmd += ["--no-deps"] if pkg in NO_DEPENDENCIES else []
    cmd += ["--index-url", index] if index else []
    cmd.append(pkg)
    return subprocess.run(cmd, check=False).returncode == 0


def is_torch_part(name: str) -> bool:
    return name in TORCH_GROUP or (
        name.startswith("torch-") and name.endswith(".dist-info")
    )


def clear_torch_group() -> None:
    """Remove torch and its CUDA leftovers from the volume folder."""
    for name in os.listdir(FOLDER):
        if is_torch_part(name) or name.startswith(("nvidia", "triton")):
            dbutils.fs.rm(os.path.join(FOLDER, name), True)


def needs_install(pkg: str, error: str | None) -> bool:
    """A reinstall fixes a missing module, not an error inside an installed one."""
    if error is None:
        return False
    return pkg == "torch" or error == "refresh" or "No module named" in error


def drop_what_the_environment_has(target: str, keep_torch: bool = False) -> None:
    dropped = set()
    for name in os.listdir(target):
        path = os.path.join(target, name)
        is_cuda = name.startswith(("nvidia", "triton"))
        is_other_torch = is_torch_part(name) and not keep_torch
        is_base = os.path.isdir(path) and in_base_environment(name)
        if is_cuda or is_other_torch or (is_base and not name.endswith(".dist-info")):
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
_torch_fixed = False
for _pkg, _mod in missing.items():
    _error = import_error(_mod) if _torch_fixed and _pkg != "torch" else errors[_pkg]
    if not needs_install(_pkg, _error):
        _why = "imports now" if _error is None else f"reinstall does not fix: {_error}"
        print(f"SKIP {_pkg}: {_why}", flush=True)
        continue
    if _pkg == "torch":
        clear_torch_group()
    _tmp = tempfile.mkdtemp()
    _spec = PIP_SPECS.get(_pkg, _pkg)
    _ok = _pkg == "torch" and pip_install(_spec, _tmp, TORCH_CPU_INDEX)
    _ok = _ok or pip_install(_spec, _tmp, None)
    if _ok:
        drop_what_the_environment_has(_tmp, keep_torch=_pkg == "torch")
        clear_old_versions(_tmp, FOLDER)
        print(f"OK  {_pkg}: {copy_tree(_tmp, FOLDER)} file(s) copied", flush=True)
        _torch_fixed = _torch_fixed or _pkg == "torch"
    else:
        print(f"WARN {_pkg}: install failed", flush=True)
    shutil.rmtree(_tmp, ignore_errors=True)

# COMMAND ----------

# DBTITLE 1,Add the folder to the import path
use_model_libraries()
print(FOLDER in sys.path)

# COMMAND ----------

# DBTITLE 1,Which libraries import
failed = {}
results = check_imports(list(PACKAGES.values()))
for _pkg, _mod in PACKAGES.items():
    _error = results[_mod]
    _status = "WARN" if _error else "OK  "
    _version = library_version(_mod.split(".")[0])
    print(f"{_status} {_pkg}: version {_version}, import {IMPORT_SECONDS[_mod]:.0f} s")
    if _error:
        failed[_pkg] = _error
        for _row in ERROR_TEXT[_mod].splitlines()[-ERROR_ROWS:]:
            print(f"     | {_row}")
print(f"still failing: {list(failed) or 'none'}")
