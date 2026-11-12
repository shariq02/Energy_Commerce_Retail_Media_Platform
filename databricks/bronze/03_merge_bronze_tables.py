# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # BRONZE TABLE MERGE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** build each merged Bronze table of `bronze_merges.yml` (not `at_load`)
# MAGIC from its existing part tables, with the discriminator column first, check the
# MAGIC rows and columns of every part, and write the findings. No table is dropped.
# MAGIC Safe to run again: a merged table is rebuilt, an appended part is added once.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ./_bronze_common

# COMMAND ----------

# DBTITLE 1,Imports
import datetime as dt
import os
from functools import reduce

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
RUN_TABLES = []  # empty = every merge of the list that is not built at load
FINDINGS = "src/findings/consolidation_findings/bronze_merge.md"

merges = [m for m in load_merges() if not m.get("at_load")]
if RUN_TABLES:
    merges = [m for m in merges if m["table"] in RUN_TABLES]
if not merges:
    raise RuntimeError(f"FAIL  no merge selected: {RUN_TABLES}")


def fqn(schema: str, table: str) -> str:
    return f"{CATALOG}.{schema}.{table}"


missing = [
    fqn(m["schema"], part)
    for m in merges
    for part in m["parts"].values()
    if not spark.catalog.tableExists(fqn(m["schema"], part))
]
if missing:
    raise RuntimeError(f"FAIL  part tables not found: {missing}")
print(f"Merges selected: {[m['table'] for m in merges]}")

# COMMAND ----------

# DBTITLE 1,Helper -- union the parts of one merge


def merged_frame(merge: dict, values: list[str]):
    column = merge["discriminator"]
    frames = []
    for value in values:
        part = spark.table(fqn(merge["schema"], merge["parts"][value]))
        columns = part.columns
        frames.append(part.withColumn(column, F.lit(value)).select(column, *columns))
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)


# COMMAND ----------

# DBTITLE 1,Build each merged table
build_results: list[dict] = []
for merge in merges:
    target = fqn(merge["schema"], merge["table"])
    column = merge["discriminator"]
    values = list(merge["parts"])
    result = {"merge": merge, "table": target, "error": None, "written": []}
    try:
        if merge.get("append"):
            present = {
                r[0] for r in spark.table(target).select(column).distinct().collect()
            }
            lacking = sorted(set(merge["kept"]) - present)
            if lacking:
                raise RuntimeError(f"{column} lacks {lacking}")
            values = [v for v in values if v not in present]
            if values:
                write_bronze(merged_frame(merge, values), target, "append")
        else:
            write_bronze(merged_frame(merge, values), target, "overwrite")
        result["written"] = values
        print(f"OK    {merge['table']}: written {values or 'nothing, parts present'}")
    except Exception as exc:
        result["error"] = str(exc)
        print(f"FAIL  {merge['table']}: {exc}")
    build_results.append(result)

# COMMAND ----------

# DBTITLE 1,Check rows and columns of every part
check_rows: list[tuple] = []
for result in build_results:
    merge, target = result["merge"], result["table"]
    if result["error"]:
        failure = (merge["table"], "", "", None, None, "FAIL", result["error"])
        check_rows.append(failure)
        continue
    column = merge["discriminator"]
    merged = spark.table(target)
    counts = {r[0]: r[1] for r in merged.groupBy(column).count().collect()}
    for value, part_name in merge["parts"].items():
        part = spark.table(fqn(merge["schema"], part_name))
        part_rows = part.count()
        lost = sorted(set(part.columns) - set(merged.columns))
        problems = []
        if counts.get(value) != part_rows:
            problems.append(f"rows {counts.get(value)} != {part_rows}")
        if lost:
            problems.append(f"columns missing {lost}")
        status = "FAIL" if problems else "PASS"
        row = (merge["table"], value, part_name, part_rows, counts.get(value), status)
        note = "; ".join(problems)
        check_rows.append((*row, note))
        print(f"{status}  {merge['table']}.{value}: {part_rows} rows {note}")

# COMMAND ----------

# DBTITLE 1,Write the findings


def cell(value) -> str:
    if value is None:
        return ""
    return f"{value:,}" if isinstance(value, int) else str(value).replace("|", "/")


header = ["merged table", "value", "part table", "part rows", "merged rows", "check"]
lines = [
    "# Bronze table merge",
    "",
    f"**Run:** {dt.datetime.now(dt.UTC):%Y-%m-%d %H:%M} UTC",
    "",
    "| " + " | ".join([*header, "note"]) + " |",
    "|" + "---|" * (len(header) + 1),
    *["| " + " | ".join(cell(v) for v in row) + " |" for row in check_rows],
    "",
]
path = os.path.join(repo_root(), FINDINGS)
os.makedirs(os.path.dirname(path), exist_ok=True)
with open(path, "w", encoding="utf-8") as fh:
    fh.write("\n".join(lines))
print(f"Findings written: {FINDINGS}")

# COMMAND ----------

# DBTITLE 1,Summary
failed = [row for row in check_rows if row[5] == "FAIL"]
print(f"Parts checked: {len(check_rows)}, failed: {len(failed)}")
if failed:
    raise RuntimeError("FAIL  at least one part did not merge; see the lines above")
