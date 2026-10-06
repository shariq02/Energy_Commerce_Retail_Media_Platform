# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # CATALOG INVENTORY -- schemas, tables, row counts and sizes
# MAGIC
# MAGIC **ECRMAP -- Energy Commerce and Retail Media Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** list every schema in the catalog, every table in each schema,
# MAGIC the row count and the size of each table, the totals per schema and the
# MAGIC overall totals.
# MAGIC
# MAGIC **Read-only.** The notebook only reads metadata (`information_schema`,
# MAGIC `DESCRIBE DETAIL`) and runs `COUNT(*)`. It creates, changes and deletes
# MAGIC nothing. "Run All" is safe.
# MAGIC
# MAGIC **How the numbers are made:**
# MAGIC - Size is `sizeInBytes` from `DESCRIBE DETAIL`: the data files of the
# MAGIC   current table version. Older versions kept for time travel are not counted.
# MAGIC - Row count is `COUNT(*)`. On a Delta table it is answered from file statistics.
# MAGIC - A view has a row count and no size. A table that is not Delta has a row
# MAGIC   count and no size. A table that fails is listed with its error text.

# COMMAND ----------

# DBTITLE 1,Imports
from concurrent.futures import ThreadPoolExecutor

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
MAX_WORKERS = 8

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
