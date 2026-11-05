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
# MAGIC **Purpose:** list every schema, table and Volume in the catalog with its row
# MAGIC count and size, the totals per schema and overall, the catalogs, the quota
# MAGIC limits with their usage, and a warning when a guard line is reached.
# MAGIC
# MAGIC **Read-only on the catalog** (metadata, `COUNT(*)` and file listings only). The
# MAGIC one write is `src/inventory/catalog_inventory_<date>.md` in the Git folder.
# MAGIC "Run All" is safe.
# MAGIC
# MAGIC Table size is `sizeInBytes` from `DESCRIBE DETAIL` (current table version only).
# MAGIC A view or a non-Delta table has a row count and no size. A Volume size is the
# MAGIC sum of its file sizes; a listing that passes the time limit is marked partial.
# MAGIC A table or Volume that fails is listed with its error text.

# COMMAND ----------

# DBTITLE 1,Imports
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
MAX_WORKERS = 8
FINDINGS_SUBDIR = "src/inventory"
# Limit on the file listing of one Volume; a listing that is cut is marked partial.
VOLUME_TIME_LIMIT_SECONDS = 300
# Guard lines from the quota rules (tables per schema 100, tables per metastore 500,
# Volumes per metastore 50); a warning prints at these counts.
GUARD_TABLES_PER_SCHEMA = 90
GUARD_TABLES_PER_METASTORE = 400
GUARD_VOLUMES_PER_METASTORE = 40

print(f"Catalog: {CATALOG}")
print(f"Parallel queries: {MAX_WORKERS}")
print(f"Volume listing time limit: {VOLUME_TIME_LIMIT_SECONDS} s")

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
    row_count = num_files = size_bytes = last_modified = error = None
    try:
        row_count = spark.table(full_name).count()
    except Exception as exc:
        error = f"count: {str(exc)[:200]}"
    if table_type != "VIEW":
        try:
            detail = spark.sql(f"DESCRIBE DETAIL {full_name}").first()
            num_files = detail["numFiles"]
            size_bytes = detail["sizeInBytes"]
            last_modified = detail["lastModified"]
        except Exception as exc:
            error = (error + " | " if error else "") + f"size: {str(exc)[:200]}"
    return (
        schema_name,
        table_name,
        table_type,
        row_count,
        num_files,
        size_bytes,
        last_modified,
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
        StructField("last_modified", TimestampType()),
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
        "last_modified",
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
    F.sum(F.when(F.col("table_type") == "VIEW", 1).otherwise(0)).alias("view_count"),
    F.sum("row_count").alias("row_count"),
    F.sum("file_count").alias("file_count"),
    F.sum("size_bytes").alias("size_bytes"),
)
schema_totals_df = (
    schemas_df.join(per_schema_df, on="schema_name", how="left")
    .fillna(
        0,
        subset=["table_count", "view_count", "row_count", "file_count", "size_bytes"],
    )
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
        F.sum("view_count").alias("view_count"),
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

# DBTITLE 1,Read catalogs and Volumes
catalog_rows = []
for row in spark.sql("SHOW CATALOGS").collect():
    name = row[0]
    try:
        count = spark.sql(
            f"""
            SELECT COUNT(*) AS n
            FROM `{name}`.information_schema.tables
            WHERE table_schema <> 'information_schema'
            """
        ).first()["n"]
        catalog_rows.append((name, count, None))
    except Exception as exc:
        catalog_rows.append((name, None, str(exc)[:200]))
print(f"Catalogs: {len(catalog_rows)}")

volumes = [
    (r["volume_schema"], r["volume_name"], r["volume_type"])
    for r in spark.sql(
        f"""
        SELECT volume_schema, volume_name, volume_type
        FROM {CATALOG}.information_schema.volumes
        WHERE volume_schema <> 'information_schema'
        ORDER BY volume_schema, volume_name
        """
    ).collect()
]
print(f"Volumes: {len(volumes)}")

# COMMAND ----------

# DBTITLE 1,Measure Volume sizes


def profile_volume(item):
    schema_name, volume_name, volume_type = item
    root = f"/Volumes/{CATALOG}/{schema_name}/{volume_name}"
    deadline = time.monotonic() + VOLUME_TIME_LIMIT_SECONDS
    file_count = size_bytes = 0
    complete, error = True, None
    pending = [root]
    try:
        while pending:
            if time.monotonic() > deadline:
                complete = False
                break
            for entry in dbutils.fs.ls(pending.pop()):
                if entry.isDir():
                    pending.append(entry.path)
                else:
                    file_count += 1
                    size_bytes += entry.size
    except Exception as exc:
        error = str(exc)[:200]
    return (
        schema_name,
        volume_name,
        volume_type,
        file_count,
        size_bytes,
        "yes" if complete and error is None else "partial",
        error,
    )


with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
    volume_results = list(pool.map(profile_volume, volumes))
volume_rows = [(*r[:5], round(r[4] / (1024 * 1024), 2), *r[5:]) for r in volume_results]
display(
    spark.createDataFrame(
        volume_rows,
        "schema_name string, volume_name string, volume_type string, file_count long,"
        " size_bytes long, size_mb double, complete string, error string",
    )
)

# COMMAND ----------

# DBTITLE 1,Read quota limits and usage
quota_rows, quota_error = [], None
try:
    for item in WorkspaceClient().resource_quotas.list_quotas():
        used = item.quota_count or 0
        limit = item.quota_limit or 0
        quota_rows.append(
            (
                item.quota_name,
                item.parent_full_name,
                str(item.parent_securable_type),
                used,
                limit,
                round(100 * used / limit, 1) if limit else None,
            )
        )
    quota_rows.sort(key=lambda r: (r[5] is None, -(r[5] or 0)))
except Exception as exc:
    quota_error = str(exc)[:300]
print(f"Quota rows: {len(quota_rows)}")
if quota_error:
    print(f"Quota read failed: {quota_error}")

# COMMAND ----------

# DBTITLE 1,Check guard lines
warnings = [
    (
        f"schema {r['schema_name']}: {r['table_count']} tables"
        f" (guard {GUARD_TABLES_PER_SCHEMA})"
    )
    for r in schema_totals_df.collect()
    if r["table_count"] >= GUARD_TABLES_PER_SCHEMA
]
catalog_table_total = sum(r[1] or 0 for r in catalog_rows)
if catalog_table_total >= GUARD_TABLES_PER_METASTORE:
    warnings.append(
        f"metastore: {catalog_table_total} tables and views in all catalogs"
        f" (guard {GUARD_TABLES_PER_METASTORE})"
    )
if len(volumes) >= GUARD_VOLUMES_PER_METASTORE:
    warnings.append(
        f"Volumes: {len(volumes)} in the catalog (guard {GUARD_VOLUMES_PER_METASTORE})"
    )
print(
    "\n".join(f"WARNING {w}" for w in warnings)
    if warnings
    else "No guard line reached."
)

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
        "last_modified",
        "error",
    ).collect()
]
schema_rows = [
    tuple(r)
    for r in schema_totals_df.select(
        "schema_name",
        "table_count",
        "view_count",
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
    F.sum("view_count").alias("view_count"),
    F.sum("row_count").alias("row_count"),
    F.sum("file_count").alias("file_count"),
    F.sum("size_bytes").alias("size_bytes"),
).first()
error_rows = [r for r in table_rows if r[8] is not None]
volume_error_rows = [r for r in volume_rows if r[7] is not None]
volume_bytes = sum(r[4] for r in volume_rows)

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
            " `tables` counts tables and views; `views` is the part that are views."
        ),
        "",
        "## Guard lines",
        "",
        "\n".join(f"- WARNING {w}" for w in warnings) if warnings else "None reached.",
        "",
        "## Overall",
        "",
        markdown_table(
            [
                "schemas",
                "tables",
                "views",
                "rows",
                "files",
                "size_bytes",
                "size_gb",
                "volumes",
                "volume_size_gb",
            ],
            [
                (
                    overall["schema_count"],
                    overall["table_count"],
                    overall["view_count"],
                    overall["row_count"],
                    overall["file_count"],
                    overall["size_bytes"],
                    round(overall["size_bytes"] / (1024**3), 3),
                    len(volume_rows),
                    round(volume_bytes / (1024**3), 3),
                )
            ],
        ),
        "",
        "## Catalogs",
        "",
        markdown_table(
            ["catalog", "tables_and_views", "error"],
            catalog_rows,
        ),
        "",
        "## Quota limits and usage",
        "",
        (
            f"Quota read failed: {quota_error}"
            if quota_error
            else markdown_table(
                ["quota", "parent", "parent_type", "used", "limit", "used_percent"],
                quota_rows,
            )
        ),
        "",
        "## Per schema",
        "",
        markdown_table(
            [
                "schema",
                "tables",
                "views",
                "rows",
                "files",
                "size_bytes",
                "size_mb",
                "size_gb",
            ],
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
                "last_modified",
                "error",
            ],
            table_rows,
        ),
        "",
        "## Volumes",
        "",
        markdown_table(
            [
                "schema",
                "volume",
                "type",
                "files",
                "size_bytes",
                "size_mb",
                "complete",
                "error",
            ],
            volume_rows,
        ),
        "",
        f"## Errors ({len(error_rows) + len(volume_error_rows)})",
        "",
        markdown_table(
            ["schema", "object", "error"],
            [(r[0], r[1], r[8]) for r in error_rows]
            + [(r[0], r[1], r[7]) for r in volume_error_rows],
        )
        if error_rows or volume_error_rows
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
