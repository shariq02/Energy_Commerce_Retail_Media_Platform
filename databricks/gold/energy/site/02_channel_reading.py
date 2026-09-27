# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- CHANNEL_READING
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_gold.channel_reading` -- `energy_meter_reading`'s
# MAGIC wide cumulative/increment columns and `thermal_energy`'s wide power
# MAGIC columns unpivoted to one row per site x interval x channel x value kind.
# MAGIC Grain: site x interval start x interval x channel x value kind.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../../_gold_common

# COMMAND ----------

# DBTITLE 1,Gold inspection library
# MAGIC %run ../../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "honda_iot"
COMPONENT = "gold/energy/site/channel_reading"
TABLE = "channel_reading"

_CONTEXT = [
    "location_key",
    "source_location_id",
    "market_area_code",
    "observation_timestamp_native",
    "time_basis",
    "utc_offset_hours",
    F.col("observation_timestamp_utc").alias("interval_start_utc"),
    "observation_timestamp_project",
    "local_date",
    "interval_seconds",
    "interval_reference",
    "sign_convention",
    "quality_flags",
    "measurement_basis",
    "source_system",
    "source_dataset",
    "source_record_id",
]
# Energy meter channels: (subsystem, channel, value_kind, source_column)
_METER_CHANNELS = [
    ("electricity", "total", "cumulative", "electricity_total_kwh"),
    ("electricity", "total", "increment", "electricity_total_increment_kwh"),
    (
        "electricity",
        "solar_photovoltaic",
        "cumulative",
        "electricity_solar_photovoltaic_kwh",
    ),
    (
        "electricity",
        "solar_photovoltaic",
        "increment",
        "electricity_solar_photovoltaic_increment_kwh",
    ),
    (
        "electricity",
        "combined_heat_and_power",
        "cumulative",
        "electricity_combined_heat_and_power_kwh",
    ),
    (
        "electricity",
        "combined_heat_and_power",
        "increment",
        "electricity_combined_heat_and_power_increment_kwh",
    ),
    ("heating", "total", "cumulative", "heating_total_kwh"),
    ("heating", "total", "increment", "heating_total_increment_kwh"),
    (
        "heating",
        "combined_heat_and_power_heat",
        "cumulative",
        "heating_combined_heat_and_power_heat_kwh",
    ),
    (
        "heating",
        "combined_heat_and_power_heat",
        "increment",
        "heating_combined_heat_and_power_heat_increment_kwh",
    ),
    (
        "heating",
        "combined_heat_and_power_electricity",
        "cumulative",
        "heating_combined_heat_and_power_electricity_kwh",
    ),
    (
        "heating",
        "combined_heat_and_power_electricity",
        "increment",
        "heating_combined_heat_and_power_electricity_increment_kwh",
    ),
    ("cooling", "total", "cumulative", "cooling_total_kwh"),
    ("cooling", "total", "increment", "cooling_total_increment_kwh"),
    ("cooling", "electricity", "cumulative", "cooling_electricity_kwh"),
    ("cooling", "electricity", "increment", "cooling_electricity_increment_kwh"),
]

# Thermal power channels: (subsystem, channel, value_kind, source_column)
_THERMAL_CHANNELS = [
    ("heating", "total", "power", "heating_total_w"),
    (
        "heating",
        "combined_heat_and_power_heat",
        "power",
        "heating_combined_heat_and_power_heat_w",
    ),
    ("cooling", "total", "power", "cooling_total_w"),
]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver
energy_meter_reading = read_silver("energy_meter_reading")
thermal_energy = read_silver("thermal_energy")

# COMMAND ----------

# DBTITLE 1,Helper -- unpivot one wide table against a channel list


def _unpivot(df, channels, unit_label):
    rows = []
    for subsystem, channel, value_kind, source_col in channels:
        rows.append(
            df.select(
                *_CONTEXT,
                F.lit(subsystem).alias("subsystem"),
                F.lit(channel).alias("channel"),
                F.lit(value_kind).alias("value_kind"),
                F.col(source_col).alias("value"),
                F.lit(unit_label).alias("unit"),
                F.lit(source_col).alias("source_column"),
            ).filter(F.col("value").isNotNull())
        )
    out = rows[0]
    for r in rows[1:]:
        out = out.unionByName(r)
    return out


# COMMAND ----------

# DBTITLE 1,Build channel_reading
channel_reading = _unpivot(energy_meter_reading, _METER_CHANNELS, "kwh").unionByName(
    _unpivot(thermal_energy, _THERMAL_CHANNELS, "w")
)
channel_reading = add_gold_provenance(channel_reading, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
_GRAIN = [
    "location_key",
    "interval_start_utc",
    "interval_seconds",
    "subsystem",
    "channel",
    "value_kind",
]
assert_unique_grain(
    channel_reading, _GRAIN, component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(channel_reading, TABLE, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    channel_reading, TABLE, source=SOURCE, component=COMPONENT, rid=rid, key_cols=_GRAIN
)
write_gold_findings(SOURCE, f"site__{TABLE}", TABLE, _blocks)
