# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # BRONZE LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the landing Volume, the merge list and the helpers that the Bronze
# MAGIC notebooks share, pulled in with `%run ./_bronze_common`.

# COMMAND ----------

# DBTITLE 1,Imports
import os
import re

import yaml

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
LANDING_SCHEMA = "bronze"
LANDING_VOLUME = "landing"
MERGES_PATH = "src/schemas/mappings/bronze_merges.yml"

# Characters Delta rejects in a column name.
ILLEGAL_COLUMN_CHARS = re.compile(r"[ ,;{}()\[\]\n\t=.]")

# COMMAND ----------

# DBTITLE 1,Landing Volume helpers


def landing_root() -> str:
    return f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{LANDING_VOLUME}"


def list_files(path: str) -> list[str]:
    """Every file under a folder, sub-folders included."""
    found: list[str] = []
    for item in dbutils.fs.ls(path):
        if item.isDir():
            found.extend(list_files(item.path))
        else:
            found.append(item.path)
    return found


def remove_files(paths: list[str]) -> None:
    for path in paths:
        dbutils.fs.rm(path)


def landing_files() -> list[str]:
    """Every file under the landing root, in any folder. A file name that
    appears twice stops the run (the copy would load twice)."""
    paths = list_files(landing_root())
    names: dict[str, list[str]] = {}
    for path in paths:
        names.setdefault(path.rsplit("/", 1)[-1], []).append(path)
    repeated = {name: found for name, found in names.items() if len(found) > 1}
    if repeated:
        raise RuntimeError(f"FAIL  a file name is in several folders: {repeated}")
    return paths


def claim_files(plan: dict[str, dict[str, list[str]]], seen: set[str]) -> None:
    """Fail when one file matches two sources; report how many landing files
    match no dataset of this notebook."""
    owners: dict[str, list[str]] = {}
    for source, datasets in plan.items():
        for path in {p for files in datasets.values() for p in files}:
            owners.setdefault(path, []).append(source)
    shared = {path: sources for path, sources in owners.items() if len(sources) > 1}
    if shared:
        raise RuntimeError(f"FAIL  a file matches several sources: {shared}")
    unclaimed = sorted(path for path in seen if path not in owners)
    if unclaimed:
        print(f"WARN  {len(unclaimed)} landing file(s) this notebook does not load")


def remove_empty_folders(path: str | None = None) -> None:
    """Remove every folder under the landing root that holds no file."""
    is_root = path is None
    path = path or landing_root()
    for item in dbutils.fs.ls(path):
        if item.isDir():
            remove_empty_folders(item.path)
    if not is_root and not dbutils.fs.ls(path):
        dbutils.fs.rm(path, recurse=True)
        print(f"REMOVED    empty folder {path}")


def remove_loaded(paths: list[str]) -> None:
    """Remove the loaded files, then the folders left empty."""
    remove_files(paths)
    remove_empty_folders()


# COMMAND ----------

# DBTITLE 1,Merge list


def repo_root() -> str:
    """The repo root, from the working folder or the notebook path."""
    path = os.path.abspath(os.getcwd())
    for _ in range(12):
        if os.path.isdir(os.path.join(path, "src", "schemas")):
            return path
        if os.path.dirname(path) == path:
            break
        path = os.path.dirname(path)
    notebook = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
    cut = notebook.rfind("/databricks/")
    for candidate in (notebook[:cut], "/Workspace" + notebook[:cut]):
        if cut > 0 and os.path.isdir(os.path.join(candidate, "src", "schemas")):
            return candidate
    raise RuntimeError("repo root not found -- run from the repo's Git folder")


def load_merges() -> list[dict]:
    """The merged Bronze tables of `bronze_merges.yml`."""
    with open(os.path.join(repo_root(), MERGES_PATH), encoding="utf-8") as fh:
        return yaml.safe_load(fh)["merges"]


def load_groups(schema: str) -> dict[str, tuple[str, str | None, str]]:
    """Staged dataset table -> (merged table, discriminator, value) for the
    merges that a loader builds from the landing files."""
    return {
        part: (m["table"], m["discriminator"], value)
        for m in load_merges()
        if m["schema"] == schema and m.get("at_load")
        for value, part in m["parts"].items()
    }


# COMMAND ----------

# DBTITLE 1,Table helpers


def sanitize_columns(df):
    """Replace the characters Delta rejects in a column name; values stay."""
    renames: dict[str, str] = {}
    for name in df.columns:
        clean = re.sub(r"_+", "_", ILLEGAL_COLUMN_CHARS.sub("_", name)).strip("_")
        if clean != name:
            renames[name] = clean
    final = [renames.get(name, name) for name in df.columns]
    if len(set(final)) != len(final):
        raise RuntimeError(f"column sanitization would collide: {final}")
    for old, new in renames.items():
        df = df.withColumnRenamed(old, new)
    return df, renames


def write_bronze(df, table: str, mode: str) -> int:
    """Write a Delta table (`overwrite` or `append`) and return its row count."""
    writer = df.write.format("delta").mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    else:
        writer = writer.option("mergeSchema", "true")  # a newer file may add a column
    writer.saveAsTable(table)
    return spark.table(table).count()


def check_table(table: str, rows: int) -> str | None:
    """Error text when the table has no columns or no rows, else None."""
    if not spark.table(table).columns:
        return "table has an empty schema"
    if not rows:
        return "table has zero rows after load"
    return None


def print_summary(title: str, results: list[dict]) -> None:
    passed = sum(1 for r in results if r["validated"])
    print("=" * 70)
    print(title)
    print("=" * 70)
    for r in results:
        print(
            f"{r['table']:<60} files={r['files']:<4} rows={r['rows']!s:<11} "
            f"{'PASS' if r['validated'] else 'FAIL'}"
        )
        if r["error"]:
            print(f"    {r['error']}")
    print("-" * 70)
    print(f"Tables loaded and validated: {passed}/{len(results)}")
    print("=" * 70)
