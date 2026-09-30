# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- PLANT OPERATING SAMPLE (CCPP)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the UCI Combined Cycle Power Plant rows (Samples `power-plant`)
# MAGIC as one keyless sample table; source citation required (Tufekci; Kaya,
# MAGIC Tufekci and Gurgen).

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Semantic structures library
# MAGIC %run ../../_semantic_common

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "power_plant_ccpp"
COMPONENT = "silver/energy/generation/04_plant_operating_sample_ccpp"
RID = run_id()
FINDINGS = "energy"
TABLE = "plant_operating_sample"
ROOT = "/Volumes/samples/databricks/datasets/power-plant"
KEEP_FILE = "Sheet1.tsv"
FIELDS = {
    "AT": "ambient_temperature_degc",
    "V": "exhaust_vacuum_cm_of_mercury",
    "AP": "ambient_pressure_mbar",
    "RH": "relative_humidity_percent",
    "PE": "net_electrical_output_mw",
}

# COMMAND ----------

# DBTITLE 1,Read Samples -- every TSV copy
raw = (
    spark.read.option("header", True)
    .option("sep", "\t")
    .option("recursiveFileLookup", True)
    .option("pathGlobFilter", "*.tsv")
    .csv(ROOT)
    .select(
        *[F.col(src).cast("double").alias(dst) for src, dst in FIELDS.items()],
        F.col("_metadata.file_name").alias("_file"),
    )
)

# COMMAND ----------

# DBTITLE 1,Check -- the files are copies of one row set (else halt)
# decimal sum: a bigint sum of hashes overflows under ANSI mode.
_row_hash = F.xxhash64(*FIELDS.values()).cast("decimal(38,0)")
file_proof = raw.groupBy("_file").agg(
    F.count("*").alias("rows"), F.sum(_row_hash).alias("content_hash_sum")
)
_proof = file_proof.collect()
if len({(r["rows"], r["content_hash_sum"]) for r in _proof}) != 1:
    raise ValueError(f"CCPP files differ, not copies: {_proof}")

# COMMAND ----------

# DBTITLE 1,Transform -- one copy, repeats kept and numbered
# The source documents 9,568 rows, so exact repeats are kept, not collapsed.
_repeat = Window.partitionBy(*FIELDS.values()).orderBy(*FIELDS.values())
sample = (
    raw.filter(F.col("_file") == KEEP_FILE)
    .withColumn("repeat_index", F.row_number().over(_repeat))
    .withColumn(
        "quality_flags",
        flag_array(
            {
                "exact_repeat": F.col("repeat_index") > 1,
                "relative_humidity_above_100": F.col("relative_humidity_percent") > 100,
            }
        ),
    )
    .withColumn("measurement_basis", F.lit("plant_operating_sample"))
    .withColumn("sample_key", sha_key(*FIELDS.values(), "repeat_index"))
    .withColumn("source_record_id", F.col("sample_key"))
)
sample = add_semantic_provenance(sample, SOURCE, KEEP_FILE, RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- plant_operating_sample
write_semantic(
    conform(sample, PLANT_OPERATING_SAMPLE_COLUMNS),
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- plant_operating_sample
written = spark.table(semantic_table(TABLE))
findings_blocks = inspect_table(
    written,
    TABLE,
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["sample_key"],
    extra_checks={
        "rows_equal_documented_9568": written.count() == 9568,
        "exact_repeats": written.filter(F.col("repeat_index") > 1).count(),
        "relative_humidity_above_100": written.filter(
            F.array_contains("quality_flags", "relative_humidity_above_100")
        ).count(),
    },
)

# COMMAND ----------

# DBTITLE 1,Export findings -- plant_operating_sample
write_silver_findings(
    FINDINGS,
    f"{COMPONENT.split('/')[-1]}__{TABLE}",
    TABLE,
    [
        *findings_blocks,
        ("file copies (rows, content hash)", rows_to_markdown(file_proof)),
    ],
)
