# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SILVER DUPLICATE KEY DIAGNOSIS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** find why a Silver table has duplicate keys. The tables and their
# MAGIC keys come from the `duplicate_keys` WARN lines of the findings file, so no table
# MAGIC name is written here. For each table the notebook counts the duplicate groups
# MAGIC and, per column, the groups in which that column differs.
# MAGIC
# MAGIC **Read-only on the catalog. Counts only, no row value.** The one write is
# MAGIC `logs/diagnostics/silver_<name>_duplicate_keys.md` in the Git folder; `logs/` is
# MAGIC not tracked. "Run All" is safe.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ./_utility_common

# COMMAND ----------

# DBTITLE 1,Imports
import ast
import os
import re
from datetime import UTC, datetime
from functools import reduce

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
FINDINGS_NAME = (
    "genomics"  # src/findings/silver_findings/<name>.md; schemas <name>_silver*
)
OUTPUT_DIR = "logs/diagnostics"
PROVENANCE_COLUMNS = {"_silver_loaded_at", "_silver_run_id"}

REPO_ROOT = find_repo_root()
FINDINGS_FILE = os.path.join(
    REPO_ROOT, "src", "findings", "silver_findings", f"{FINDINGS_NAME}.md"
)
print(f"Findings file: {FINDINGS_FILE}")

# COMMAND ----------

# DBTITLE 1,Read the duplicate key warnings
SECTION = re.compile(r"^## (\w+)\s*$")
WARNING = re.compile(r"^\| duplicate_keys \|.*\| WARN \| WARN \| key_cols=(\[.*\]) \|")

warnings: list[tuple[str, list[str]]] = []
table_name = None
with open(FINDINGS_FILE, encoding="utf-8") as fh:
    for line in fh:
        section = SECTION.match(line)
        if section:
            table_name = section.group(1)
            continue
        match = WARNING.match(line)
        if match and table_name:
            warnings.append((table_name, ast.literal_eval(match.group(1))))
print(f"{len(warnings)} table(s) with a duplicate key warning")
for name, keys in warnings:
    print(f"  {name}: {keys}")

# COMMAND ----------

# DBTITLE 1,Find the schema of each table
rows = spark.sql(
    f"""
    SELECT table_schema, table_name
    FROM {CATALOG}.information_schema.tables
    WHERE table_schema LIKE '{FINDINGS_NAME}_silver%'
    """
).collect()
schemas_of: dict[str, list[str]] = {}
for row in rows:
    schemas_of.setdefault(row["table_name"], []).append(row["table_schema"])
unknown = [name for name, _ in warnings if len(schemas_of.get(name, [])) != 1]
if unknown:
    raise RuntimeError(f"FAIL  no single schema found for table(s): {unknown}")
print("OK  every table has one schema")

# COMMAND ----------

# DBTITLE 1,Helper -- diagnose one table


def diagnose(name: str, keys: list[str]) -> dict:
    df = spark.table(f"{CATALOG}.{schemas_of[name][0]}.{name}")
    groups = df.groupBy(*keys).count().filter(F.col("count") > 1)
    size_rows = groups.groupBy("count").agg(F.count("*").alias("n")).collect()
    sizes = {row["count"]: row["n"] for row in size_rows}

    marks = {k: f"__key_{k}" for k in keys}
    duplicated = groups.select(*[F.col(k).alias(marks[k]) for k in keys])
    on = reduce(
        lambda a, b: a & b, [df[k].eqNullSafe(duplicated[marks[k]]) for k in keys]
    )
    members = df.join(duplicated, on, "left_semi")

    others = [c for c in df.columns if c not in keys and c not in PROVENANCE_COLUMNS]
    per_group = members.groupBy(*keys).agg(
        *[
            F.countDistinct(F.coalesce(F.col(c).cast("string"), F.lit("<null>"))).alias(
                c
            )
            for c in others
        ]
    )
    differs = [F.col(c) > 1 for c in others]
    result = per_group.agg(
        F.count("*").alias("__groups"),
        F.sum(reduce(lambda a, b: a | b, differs).cast("long")).alias("__any"),
        *[
            F.sum(flag.cast("long")).alias(c)
            for c, flag in zip(others, differs, strict=True)
        ],
    ).first()
    return {
        "groups": result["__groups"],
        "identical": result["__groups"] - (result["__any"] or 0),
        "sizes": sizes,
        "columns": {c: result[c] or 0 for c in others},
    }


# COMMAND ----------

# DBTITLE 1,Diagnose every table
results = {}
for name, keys in warnings:
    results[name] = diagnose(name, keys)
    found = results[name]
    print(f"{name}: {found['groups']:,} groups, {found['identical']:,} identical")

# COMMAND ----------

# DBTITLE 1,Write the diagnosis file
generated_at = datetime.now(tz=UTC)
lines = [
    f"# Silver duplicate key diagnosis -- {FINDINGS_NAME}",
    "",
    f"_Generated: {generated_at.strftime('%Y-%m-%dT%H:%MZ')}. Counts only; no row value._",
    "",
    "## Summary",
    "",
    markdown_table(
        [
            "table",
            "key",
            "duplicate groups",
            "groups with identical rows",
            "group sizes",
        ],
        [
            (
                name,
                ", ".join(keys),
                results[name]["groups"],
                results[name]["identical"],
                ", ".join(
                    f"{s}: {n}" for s, n in sorted(results[name]["sizes"].items())
                ),
            )
            for name, keys in warnings
        ],
    ),
    "",
]
for name, _ in warnings:
    found = results[name]
    differing = sorted(
        ((c, n) for c, n in found["columns"].items() if n),
        key=lambda item: -item[1],
    )
    lines += [
        f"## {name}",
        "",
        "Groups in which the column holds more than one value (nulls count as a value):",
        "",
        markdown_table(
            ["column", "groups that differ", "share of groups"],
            [(c, n, f"{n / found['groups']:.1%}") for c, n in differing]
            or [("none: the rows of every group are identical", 0, "0.0%")],
        ),
        "",
    ]

output_dir = os.path.join(REPO_ROOT, OUTPUT_DIR)
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, f"silver_{FINDINGS_NAME}_duplicate_keys.md")
with open(output_path, "w", encoding="utf-8") as fh:
    fh.write("\n".join(lines))
print(f"Written: {output_path}")
