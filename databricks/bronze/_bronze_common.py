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


def landing_path(source: str) -> str:
    return f"{landing_root()}/{source}"


def landing_sources() -> list[str]:
    """Names of the source folders that hold files now."""
    return sorted(
        item.name.rstrip("/") for item in dbutils.fs.ls(landing_root()) if item.isDir()
    )


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


def loose_files() -> list[str]:
    """Files put directly in the landing root, outside any source folder."""
    return [item.path for item in dbutils.fs.ls(landing_root()) if not item.isDir()]


def candidate_files(source: str, folders: set[str], loose: list[str]) -> list[str]:
    """The files of the source folder (if it exists) and the loose files."""
    inside = list_files(landing_path(source)) if source in folders else []
    return inside + loose


def claim_files(plan: dict[str, dict[str, list[str]]], seen: set[str]) -> None:
    """Fail when one file matches two sources; report the seen files (in a source
    folder or loose) that match no dataset of this notebook."""
    owners: dict[str, list[str]] = {}
    for source, datasets in plan.items():
        for path in {p for files in datasets.values() for p in files}:
            owners.setdefault(path, []).append(source)
    shared = {path: sources for path, sources in owners.items() if len(sources) > 1}
    if shared:
        raise RuntimeError(f"FAIL  a file matches several sources: {shared}")
    unclaimed = sorted(path for path in seen if path not in owners)
    if unclaimed:
        print(f"WARN  files this notebook does not load: {unclaimed}")


def remove_source(source: str, paths: list[str]) -> None:
    """Remove the loaded files, then the source folder once it holds no file."""
    remove_files(paths)
    if source in landing_sources() and not list_files(landing_path(source)):
        dbutils.fs.rm(landing_path(source), recurse=True)
        print(f"REMOVED    empty folder {source}/")


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
