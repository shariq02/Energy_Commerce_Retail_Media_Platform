# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- CDC OPERATIONAL CURRENT STATE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the seven governed CDC current-state Bronze tables
# MAGIC (`bronze.cdc_<entity>_current`) into source-scoped Silver `op_<entity>`
# MAGIC tables -- live rows only (`_deleted = false`), typed to the operational
# MAGIC contract, UUID key as `source_record_id`, `_op` / `_lsn` / `_event_ts`
# MAGIC retained. The stale Retail-Media CDC tables are out of scope.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Imports + config
from pyspark.sql import functions as F

SOURCE = "cdc"
COMPONENT = "silver/energy/operational/01_cdc_operational_current"
RID = run_id()

CONTRACT = load_contract("synthetic_operational")
IN_SCOPE = [
    "tariffs",
    "products",
    "customers",
    "customer_contracts",
    "meters",
    "orders",
    "order_items",
]
PROVENANCE_COLS = ["_op", "_lsn", "_event_ts"]

TABLES = {t["name"]: t for t in CONTRACT["tables"]}

# COMMAND ----------

# DBTITLE 1,One CDC entity -> Silver op_<entity>


def process(entity: str) -> None:
    tdef = TABLES[entity]
    value_cols = [c["name"] for c in tdef["columns"]]
    pk = tdef["grain"]["primary_key"][0]

    df = read_bronze(f"cdc_{entity}_current").filter(F.col("_deleted") == F.lit(False))

    missing = [c for c in value_cols if c not in df.columns]
    if missing:
        raise RuntimeError(
            f"cdc_{entity}_current is missing contract columns: {missing}"
        )

    df = cast_logical(df, tdef["columns"])
    df = (
        df.select(*value_cols, *PROVENANCE_COLS)
        .withColumn("_lsn", F.col("_lsn").cast("long"))
        .withColumn("_event_ts", F.col("_event_ts").cast("timestamp"))
        .withColumn("_srid", F.col(pk).cast("string"))
    )
    df = add_provenance(df, SOURCE, "_srid", RID)
    write_silver(df, f"op_{entity}", source=SOURCE, component=COMPONENT, rid=RID)


for _e in IN_SCOPE:
    process(_e)

# COMMAND ----------

# DBTITLE 1,Summary
audit(
    COMPONENT,
    SOURCE,
    "cdc_tables_written",
    float(len(IN_SCOPE)),
    status="PASS",
    rid=RID,
)
print("=" * 70)
print(f"CDC OPERATIONAL CURRENT -- COMPLETE  (run_id {RID})")
print("=" * 70)
