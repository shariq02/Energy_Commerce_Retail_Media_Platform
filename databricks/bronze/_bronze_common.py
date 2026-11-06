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
# MAGIC **Purpose:** the landing Volume and the helpers that both Bronze loaders share,
# MAGIC pulled in with `%run ./_bronze_common`.

# COMMAND ----------

# DBTITLE 1,Imports
import re

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
LANDING_SCHEMA = "bronze"
LANDING_VOLUME = "landing"

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
