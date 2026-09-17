# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- HONDA_ELECTRICITY_P
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `honda_iot_electricity_p` into source-scoped Silver at its (frequency,
# MAGIC datetime_utc) grain. One of 6 sibling notebooks in this folder, split
# MAGIC from a single `01_honda_energy.py` -- see the folder's other files for
# MAGIC the other 5 channels. The channel columns keep their source names and
# MAGIC their sign -- a negative value is on-site generation / export, not an
# MAGIC error.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Honda energy shared helpers
# MAGIC %run ./_honda_energy_common

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "honda_iot"
COMPONENT = "silver/energy/iot/honda_energy/01_honda_electricity_p"
RID = run_id()

# Owner decision: drop heating_p.CHP_elec, no alias -- electricity_p.CHP is
# canonical. Pre-drop safeguard below re-checks the duplication first.
_DUPLICATE_AGREEMENT_THRESHOLD = 0.99

# Stuck-reading lookback per frequency, scaled to the same ~10-hour window
# profiling measured at 1h (9 steps back = 10 readings = 10h).
STUCK_LOOKBACK_STEPS_BY_FREQ = {"1h": 9, "15min": 39, "1min": 599}

# COMMAND ----------

# DBTITLE 1,Read Bronze -- honda_iot_electricity_p
_bronze = read_bronze("honda_iot_electricity_p")

# COMMAND ----------

# DBTITLE 1,Transform -- honda_electricity_p (cast)
df = _bronze.withColumn("datetime_utc", F.col("datetime_utc").cast("timestamp"))
for _c in df.columns:
    if _c not in ("frequency", "datetime_utc"):
        df = df.withColumn(_c, F.col(_c).cast("double"))

df_value_cols = [c for c in df.columns if c not in ("frequency", "datetime_utc")]

# COMMAND ----------

# DBTITLE 1,Transform -- honda_electricity_p (no CHP_elec safeguard for this table)
_dup_agreement = None

# COMMAND ----------

# DBTITLE 1,Transform -- honda_electricity_p (5-sigma outlier flags)
df = add_5sigma_outlier_flags(df, df_value_cols)

# COMMAND ----------

# DBTITLE 1,Transform -- honda_electricity_p (stuck-reading flags)
_w = Window.partitionBy("frequency").orderBy("datetime_utc")
df = add_stuck_reading_flags(df, df_value_cols, STUCK_LOOKBACK_STEPS_BY_FREQ, _w)

# COMMAND ----------

# DBTITLE 1,Transform -- honda_electricity_p (provenance)
df = df.withColumn(
    "_srid", sha_key(F.lit("honda_iot_electricity_p"), "frequency", "datetime_utc")
)
df = add_provenance(df, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- honda_electricity_p
write_silver(df, "honda_electricity_p", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect honda_electricity_p + export findings
_findings_blocks = inspect_table(
    df,
    "honda_electricity_p",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["frequency", "datetime_utc"],
    extra_checks={"chp_elec_duplicate_agreement": _dup_agreement}
    if _dup_agreement is not None
    else None,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__honda_electricity_p",
    "honda_electricity_p",
    _findings_blocks,
)
