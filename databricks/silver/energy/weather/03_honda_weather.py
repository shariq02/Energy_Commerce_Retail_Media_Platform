# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- HONDA SITE WEATHER
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `honda_iot_weather` into source-scoped Silver at its
# MAGIC (frequency, datetime_utc) grain. Two on-site sensors renamed to their
# MAGIC English business meaning (`air_temperature_2m`, `global_irradiance`), and
# MAGIC a single synthetic `weather_location = 'honda_site'` -- there is one
# MAGIC physical site and no source-provided location field.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Imports + config
from pyspark.sql import functions as F

SOURCE = "honda_iot"
COMPONENT = "silver/energy/weather/03_honda_weather"
RID = run_id()
BT = "honda_iot_weather"
SILVER_TABLE = "honda_weather"

RENAMES = {
    "WeatherStation_Weather_Ta": "air_temperature_2m",
    "WeatherStation_Weather_Igm": "global_irradiance",
}

# Stuck-reading lookback per frequency, scaled to the same ~10-hour window
# profiling measured at 1h (9 steps back = 10 readings = 10h) -- same as
# 01_honda_energy.py.
STUCK_LOOKBACK_STEPS_BY_FREQ = {"1h": 9, "15min": 39, "1min": 599}

# COMMAND ----------

# DBTITLE 1,honda_iot_weather -> Silver
bronze_df = read_bronze(BT)
df = bronze_df
for src, tgt in RENAMES.items():
    df = df.withColumnRenamed(src, tgt)
df = (
    df.withColumn("air_temperature_2m", F.col("air_temperature_2m").cast("double"))
    .withColumn("global_irradiance", F.col("global_irradiance").cast("double"))
    .withColumn("datetime_utc", F.col("datetime_utc").cast("timestamp"))
    .withColumn("weather_location", F.lit("honda_site"))
)

# Stuck-reading flag, frequency-scaled lookback (see
# STUCK_LOOKBACK_STEPS_BY_FREQ) -- same fix as 01_honda_energy.py.
from pyspark.sql.window import Window as _Window

_w = _Window.partitionBy("frequency").orderBy("datetime_utc")
for _c in ("air_temperature_2m", "global_irradiance"):
    _stuck_expr = F.lit(False)
    for _freq, _lag_n in STUCK_LOOKBACK_STEPS_BY_FREQ.items():
        _lag1 = F.lag(F.col(_c), 1).over(_w)
        _lagN = F.lag(F.col(_c), _lag_n).over(_w)
        _cond = (
            (F.col("frequency") == _freq)
            & F.col(_c).isNotNull()
            & (F.col(_c) == _lag1)
            & (F.col(_c) == _lagN)
        )
        _stuck_expr = F.when(_cond, F.lit(True)).otherwise(_stuck_expr)
    df = df.withColumn(f"_{_c}_stuck_reading_flag", _stuck_expr)

df = df.withColumn("_srid", sha_key(F.lit(BT), "frequency", "datetime_utc"))
df = add_provenance(df, SOURCE, "_srid", RID)
write_silver(df, SILVER_TABLE, source=SOURCE, component=COMPONENT, rid=RID)
_findings_blocks = inspect_table(
    df,
    SILVER_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["frequency", "datetime_utc"],
    df_before=bronze_df,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__{SILVER_TABLE}",
    SILVER_TABLE,
    _findings_blocks,
)
