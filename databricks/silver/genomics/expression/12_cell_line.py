# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- CELL LINE (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** one record per cell line (DepMap id) with its driver variants as an
# MAGIC array.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Genomics library
# MAGIC %run ../../_genomics_common

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "genomics"
COMPONENT = "silver/genomics/expression/12_cell_line"
RID = run_id()
FINDINGS = "genomics"
TABLE = "cell_line"
RULES = load_genomics_rules()

# COMMAND ----------

# DBTITLE 1,Read Bronze -- Tahoe cell lines
DATASET = "tahoe.cell_line_metadata"
cells = read_genomics_bronze("tahoe_cell_line_metadata")


def clean(column):
    return clean_placeholders(column, RULES, f"{DATASET}.{column}")


keyed = cells.filter(clean("Cell_ID_DepMap").isNotNull())

# COMMAND ----------

# DBTITLE 1,Transform -- group the drivers per cell line
driver = F.struct(
    clean("Driver_Gene_Symbol").alias("gene_symbol"),
    clean("Driver_VarZyg").alias("variant_zygosity"),
    clean("Driver_VarType").alias("variant_type"),
    clean("Driver_ProtEffect_or_CdnaEffect").alias("effect"),
    clean("Driver_Mech_InferDM").alias("mechanism"),
    clean("Driver_GeneType_DM").alias("gene_type"),
)
cell_line = (
    keyed.withColumn("driver", F.when(clean("Driver_Gene_Symbol").isNotNull(), driver))
    .groupBy(clean("Cell_ID_DepMap").alias("cell_line_key"))
    .agg(
        F.min(clean("cell_name")).alias("cell_name"),
        F.min(clean("Cell_ID_Cellosaur")).alias("cellosaur_id"),
        F.min(clean("Organ")).alias("organ"),
        F.sort_array(F.collect_set("driver")).alias("drivers"),
    )
    .withColumn("_srid", F.col("cell_line_key"))
)
cell_line = conform_genomics(add_genomics_provenance(cell_line, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- cell_line
write_silver(cell_line, TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- cell_line
written = spark.table(genomics_table(TABLE))
blocks = inspect_table(
    written,
    TABLE,
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["cell_line_key"],
    extra_checks={
        "rows_without_a_depmap_id": cells.count() - keyed.count(),
        "cell_lines_without_drivers": written.filter(F.size("drivers") == 0).count(),
    },
)

# COMMAND ----------

# DBTITLE 1,Export findings -- cell_line
write_silver_findings(FINDINGS, f"{COMPONENT.split('/')[-1]}__{TABLE}", TABLE, blocks)
