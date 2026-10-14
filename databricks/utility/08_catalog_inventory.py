# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # CATALOG INVENTORY -- schemas, tables, row counts and sizes
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** list every schema and table in the catalog with its row count
# MAGIC and size, and the totals per schema and overall.
# MAGIC
# MAGIC **Read-only on the catalog** (metadata and `COUNT(*)` only). The one write is
# MAGIC `src/inventory/catalog_inventory_<date>.md` in the Git folder. "Run All" is safe.
# MAGIC
# MAGIC Size is `sizeInBytes` from `DESCRIBE DETAIL` (current table version only). A
# MAGIC view or a non-Delta table has a row count and no size. A table that fails is
# MAGIC listed with its error text.

# COMMAND ----------

# DBTITLE 1,Imports
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
MAX_WORKERS = 8
FINDINGS_SUBDIR = "src/inventory"

print(f"Catalog: {CATALOG}")
print(f"Parallel queries: {MAX_WORKERS}")

# COMMAND ----------

# DBTITLE 1,Read schemas
schemas_df = spark.sql(
    f"""
    SELECT schema_name
    FROM {CATALOG}.information_schema.schemata
    WHERE schema_name <> 'information_schema'
    """
)
schema_names = sorted(r["schema_name"] for r in schemas_df.collect())
print(f"Schemas: {len(schema_names)}")

# COMMAND ----------

# DBTITLE 1,Read tables
tables = [
    (r["table_schema"], r["table_name"], r["table_type"])
    for r in spark.sql(
        f"""
        SELECT table_schema, table_name, table_type
        FROM {CATALOG}.information_schema.tables
        WHERE table_schema <> 'information_schema'
        ORDER BY table_schema, table_name
        """
    ).collect()
]
print(f"Tables and views: {len(tables)}")

# COMMAND ----------

# DBTITLE 1,Define profile_table


def profile_table(item):
    schema_name, table_name, table_type = item
    full_name = f"`{CATALOG}`.`{schema_name}`.`{table_name}`"
    row_count = num_files = size_bytes = error = None
    try:
        row_count = spark.table(full_name).count()
    except Exception as exc:
        error = f"count: {str(exc)[:200]}"
    if table_type != "VIEW":
        try:
            detail = spark.sql(f"DESCRIBE DETAIL {full_name}").first()
            num_files = detail["numFiles"]
            size_bytes = detail["sizeInBytes"]
        except Exception as exc:
            error = (error + " | " if error else "") + f"size: {str(exc)[:200]}"
    return (
        schema_name,
        table_name,
        table_type,
        row_count,
        num_files,
        size_bytes,
        error,
    )


# COMMAND ----------

# DBTITLE 1,Profile all tables in parallel
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
    results = list(pool.map(profile_table, tables))
print(f"Profiled: {len(results)}")

# COMMAND ----------

# DBTITLE 1,Build table inventory
table_schema = StructType(
    [
        StructField("schema_name", StringType()),
        StructField("table_name", StringType()),
        StructField("table_type", StringType()),
        StructField("row_count", LongType()),
        StructField("file_count", LongType()),
        StructField("size_bytes", LongType()),
        StructField("error", StringType()),
    ]
)
tables_df = (
    spark.createDataFrame(results, table_schema)
    .withColumn("size_mb", F.round(F.col("size_bytes") / (1024 * 1024), 2))
    .select(
        "schema_name",
        "table_name",
        "table_type",
        "row_count",
        "file_count",
        "size_bytes",
        "size_mb",
        "error",
    )
    .orderBy("schema_name", "table_name")
)

# COMMAND ----------

# DBTITLE 1,Show every table
display(tables_df)

# COMMAND ----------

# DBTITLE 1,Build schema totals
per_schema_df = tables_df.groupBy("schema_name").agg(
    F.count("*").alias("table_count"),
    F.sum("row_count").alias("row_count"),
    F.sum("file_count").alias("file_count"),
    F.sum("size_bytes").alias("size_bytes"),
)
schema_totals_df = (
    schemas_df.join(per_schema_df, on="schema_name", how="left")
    .fillna(0, subset=["table_count", "row_count", "file_count", "size_bytes"])
    .withColumn("size_mb", F.round(F.col("size_bytes") / (1024 * 1024), 2))
    .withColumn("size_gb", F.round(F.col("size_bytes") / (1024**3), 3))
    .orderBy("schema_name")
)

# COMMAND ----------

# DBTITLE 1,Show schema totals
display(schema_totals_df)

# COMMAND ----------

# DBTITLE 1,Show overall totals
display(
    schema_totals_df.agg(
        F.count("*").alias("schema_count"),
        F.sum("table_count").alias("table_count"),
        F.sum("row_count").alias("row_count"),
        F.sum("file_count").alias("file_count"),
        F.sum("size_bytes").alias("size_bytes"),
        F.round(F.sum("size_bytes") / (1024**3), 3).alias("size_gb"),
    )
)

# COMMAND ----------

# DBTITLE 1,Show objects with errors
display(tables_df.filter(F.col("error").isNotNull()))

# COMMAND ----------

# DBTITLE 1,Define find_repo_root


def find_repo_root():
    path = os.path.abspath(os.getcwd())
    for _ in range(12):
        if os.path.isdir(os.path.join(path, "src")) and os.path.isdir(
            os.path.join(path, "databricks")
        ):
            return path
        if os.path.dirname(path) == path:
            break
        path = os.path.dirname(path)
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
    cut = notebook_path.rfind("/databricks/")
    if cut > 0:
        for candidate in (notebook_path[:cut], "/Workspace" + notebook_path[:cut]):
            if os.path.isdir(os.path.join(candidate, "src")):
                return candidate
    raise RuntimeError(
        "repo root not found -- run from inside the repo's Databricks Git folder"
    )


# COMMAND ----------

# DBTITLE 1,Find the findings directory
FINDINGS_DIR = os.path.join(find_repo_root(), FINDINGS_SUBDIR)
os.makedirs(FINDINGS_DIR, exist_ok=True)
print(f"Findings directory: {FINDINGS_DIR}")

# COMMAND ----------

# DBTITLE 1,Define markdown_table


def markdown_table(columns, rows):
    def cell(value):
        if value is None:
            return ""
        if isinstance(value, int):
            return f"{value:,}"
        return str(value).replace("|", "/").replace("\n", " ")

    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    lines += ["| " + " | ".join(cell(v) for v in row) + " |" for row in rows]
    return "\n".join(lines)


# COMMAND ----------

# DBTITLE 1,Collect results for the findings file
generated_at = datetime.now(UTC)
table_rows = [
    tuple(r)
    for r in tables_df.select(
        "schema_name",
        "table_name",
        "table_type",
        "row_count",
        "file_count",
        "size_bytes",
        "size_mb",
        "error",
    ).collect()
]
schema_rows = [
    tuple(r)
    for r in schema_totals_df.select(
        "schema_name",
        "table_count",
        "row_count",
        "file_count",
        "size_bytes",
        "size_mb",
        "size_gb",
    ).collect()
]
overall = schema_totals_df.agg(
    F.count("*").alias("schema_count"),
    F.sum("table_count").alias("table_count"),
    F.sum("row_count").alias("row_count"),
    F.sum("file_count").alias("file_count"),
    F.sum("size_bytes").alias("size_bytes"),
).first()
error_rows = [r for r in table_rows if r[7] is not None]

# COMMAND ----------

# DBTITLE 1,Build findings text
findings_text = "\n".join(
    [
        f"# Catalog inventory: {CATALOG}",
        "",
        f"Generated: {generated_at.strftime('%Y-%m-%dT%H:%M:%SZ')}",
        "",
        (
            "Size is the data files of the current table version (older versions"
            " kept for time travel are not counted). A view has no size."
        ),
        "",
        "## Overall",
        "",
        markdown_table(
            ["schemas", "tables", "rows", "files", "size_bytes", "size_gb"],
            [
                (
                    overall["schema_count"],
                    overall["table_count"],
                    overall["row_count"],
                    overall["file_count"],
                    overall["size_bytes"],
                    round(overall["size_bytes"] / (1024**3), 3),
                )
            ],
        ),
        "",
        "## Per schema",
        "",
        markdown_table(
            ["schema", "tables", "rows", "files", "size_bytes", "size_mb", "size_gb"],
            schema_rows,
        ),
        "",
        "## Per table",
        "",
        markdown_table(
            [
                "schema",
                "table",
                "type",
                "rows",
                "files",
                "size_bytes",
                "size_mb",
                "error",
            ],
            table_rows,
        ),
        "",
        f"## Errors ({len(error_rows)})",
        "",
        markdown_table(
            ["schema", "table", "error"], [(r[0], r[1], r[7]) for r in error_rows]
        )
        if error_rows
        else "None.",
        "",
    ]
)

# COMMAND ----------

# DBTITLE 1,Write findings file
findings_path = os.path.join(
    FINDINGS_DIR, f"catalog_inventory_{generated_at.strftime('%Y%m%d')}.md"
)
with open(findings_path, "w", encoding="utf-8") as fh:
    fh.write(findings_text)
print(f"Written: {findings_path}")
