# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- FACT SITE ENERGY (SPLIT, ONE TABLE PER HONDA CHANNEL)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per (`frequency`, `datetime_utc`), per table.
# MAGIC
# MAGIC **Sources:** the six Honda IoT power/energy Silver tables +
# MAGIC `honda_weather` (Silver, energy_silver).
# MAGIC
# MAGIC **Serves use case:** any Energy use case needing Honda site energy/
# MAGIC weather channels. There is one physical site and no source-provided
# MAGIC location dimension to resolve against.
# MAGIC
# MAGIC **Purpose:** promote each Honda Silver table to its own Gold fact table --
# MAGIC kept split, one table per source table, NOT conformed into a single
# MAGIC `fact_site_energy`. Near pass-through -- the 5-sigma/stuck-reading/
# MAGIC monotonicity flags Silver already computed carry through unaltered.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../../_gold_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "honda_iot"
COMPONENT = "gold/energy/iot/01_fact_site_energy"
RID = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver -- honda_electricity_p
_electricity_p_silver = read_silver("honda_electricity_p")

# COMMAND ----------

# DBTITLE 1,Read Silver -- honda_electricity_w
_electricity_w_silver = read_silver("honda_electricity_w")

# COMMAND ----------

# DBTITLE 1,Read Silver -- honda_heating_p
_heating_p_silver = read_silver("honda_heating_p")

# COMMAND ----------

# DBTITLE 1,Read Silver -- honda_heating_w
_heating_w_silver = read_silver("honda_heating_w")

# COMMAND ----------

# DBTITLE 1,Read Silver -- honda_cooling_p
_cooling_p_silver = read_silver("honda_cooling_p")

# COMMAND ----------

# DBTITLE 1,Read Silver -- honda_cooling_w
_cooling_w_silver = read_silver("honda_cooling_w")

# COMMAND ----------

# DBTITLE 1,Read Silver -- honda_weather
_weather_silver = read_silver("honda_weather")

# COMMAND ----------

# DBTITLE 1,Transform -- fact_site_electricity_p (surrogate key + Gold provenance)
electricity_p = _electricity_p_silver.withColumn(
    "electricity_p_key", surrogate_key("frequency", "datetime_utc")
)
electricity_p = add_gold_provenance(electricity_p, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_site_electricity_p
assert_unique_grain(
    electricity_p,
    ["frequency", "datetime_utc"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_site_electricity_p
write_gold(
    electricity_p,
    "fact_site_electricity_p",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect fact_site_electricity_p + export findings
_findings_blocks = inspect_gold_table(
    electricity_p,
    "fact_site_electricity_p",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["frequency", "datetime_utc"],
    df_before=_electricity_p_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__fact_site_electricity_p",
    "fact_site_electricity_p",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_site_electricity_w (surrogate key + Gold provenance)
electricity_w = _electricity_w_silver.withColumn(
    "electricity_w_key", surrogate_key("frequency", "datetime_utc")
)
electricity_w = add_gold_provenance(electricity_w, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_site_electricity_w
assert_unique_grain(
    electricity_w,
    ["frequency", "datetime_utc"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_site_electricity_w
write_gold(
    electricity_w,
    "fact_site_electricity_w",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect fact_site_electricity_w + export findings
_findings_blocks = inspect_gold_table(
    electricity_w,
    "fact_site_electricity_w",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["frequency", "datetime_utc"],
    df_before=_electricity_w_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__fact_site_electricity_w",
    "fact_site_electricity_w",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_site_heating_p (surrogate key + Gold provenance)
heating_p = _heating_p_silver.withColumn(
    "heating_p_key", surrogate_key("frequency", "datetime_utc")
)
heating_p = add_gold_provenance(heating_p, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_site_heating_p
assert_unique_grain(
    heating_p,
    ["frequency", "datetime_utc"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_site_heating_p
write_gold(
    heating_p, "fact_site_heating_p", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect fact_site_heating_p + export findings
_findings_blocks = inspect_gold_table(
    heating_p,
    "fact_site_heating_p",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["frequency", "datetime_utc"],
    df_before=_heating_p_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__fact_site_heating_p",
    "fact_site_heating_p",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_site_heating_w (surrogate key + Gold provenance)
heating_w = _heating_w_silver.withColumn(
    "heating_w_key", surrogate_key("frequency", "datetime_utc")
)
heating_w = add_gold_provenance(heating_w, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_site_heating_w
assert_unique_grain(
    heating_w,
    ["frequency", "datetime_utc"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_site_heating_w
write_gold(
    heating_w, "fact_site_heating_w", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect fact_site_heating_w + export findings
_findings_blocks = inspect_gold_table(
    heating_w,
    "fact_site_heating_w",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["frequency", "datetime_utc"],
    df_before=_heating_w_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__fact_site_heating_w",
    "fact_site_heating_w",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_site_cooling_p (surrogate key + Gold provenance)
cooling_p = _cooling_p_silver.withColumn(
    "cooling_p_key", surrogate_key("frequency", "datetime_utc")
)
cooling_p = add_gold_provenance(cooling_p, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_site_cooling_p
assert_unique_grain(
    cooling_p,
    ["frequency", "datetime_utc"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_site_cooling_p
write_gold(
    cooling_p, "fact_site_cooling_p", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect fact_site_cooling_p + export findings
_findings_blocks = inspect_gold_table(
    cooling_p,
    "fact_site_cooling_p",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["frequency", "datetime_utc"],
    df_before=_cooling_p_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__fact_site_cooling_p",
    "fact_site_cooling_p",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_site_cooling_w (surrogate key + Gold provenance)
cooling_w = _cooling_w_silver.withColumn(
    "cooling_w_key", surrogate_key("frequency", "datetime_utc")
)
cooling_w = add_gold_provenance(cooling_w, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_site_cooling_w
assert_unique_grain(
    cooling_w,
    ["frequency", "datetime_utc"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_site_cooling_w
write_gold(
    cooling_w, "fact_site_cooling_w", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect fact_site_cooling_w + export findings
_findings_blocks = inspect_gold_table(
    cooling_w,
    "fact_site_cooling_w",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["frequency", "datetime_utc"],
    df_before=_cooling_w_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__fact_site_cooling_w",
    "fact_site_cooling_w",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_site_weather (surrogate key + Gold provenance)
weather = _weather_silver.withColumn(
    "weather_key", surrogate_key("frequency", "datetime_utc")
)
weather = add_gold_provenance(weather, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_site_weather
assert_unique_grain(
    weather,
    ["frequency", "datetime_utc"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_site_weather
write_gold(weather, "fact_site_weather", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect fact_site_weather + export findings
_findings_blocks = inspect_gold_table(
    weather,
    "fact_site_weather",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["frequency", "datetime_utc"],
    df_before=_weather_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__fact_site_weather",
    "fact_site_weather",
    _findings_blocks,
)
