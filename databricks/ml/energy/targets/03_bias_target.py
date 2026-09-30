# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FORECAST BIAS TARGET
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** realised minus day-ahead forecast per market area, local date and scope,
# MAGIC built from the balance and forecast series (not from the unpaired forecast
# MAGIC table). The PV scope uses the derived forecast.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "smard"
COMPONENT = "ml/energy/targets/bias"
TABLE = "target_forecast_bias"
DAY = "local_day_europe_berlin"
ALL_CARRIERS = [
    "onshore_wind",
    "offshore_wind",
    "photovoltaic",
    "biomass",
    "hydro",
    "lignite",
    "hard_coal",
    "natural_gas",
    "nuclear",
    "pumped_storage",
    "other_conventional",
    "other_renewable",
]
# forecast scope -> realised carriers; the last two mappings are assumed.
SCOPE_CARRIERS = {
    "onshore_wind": (["onshore_wind"], "confirmed"),
    "offshore_wind": (["offshore_wind"], "confirmed"),
    "wind_and_photovoltaic": (
        ["onshore_wind", "offshore_wind", "photovoltaic"],
        "confirmed",
    ),
    "photovoltaic": (["photovoltaic"], "derived_forecast"),
    "other": (["biomass", "hydro", "other_conventional", "other_renewable"], "assumed"),
    "total": (ALL_CARRIERS, "assumed"),
}

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Realised daily generation per carrier
realised_by_carrier = (
    read_gold("energy_balance_component", source=SOURCE)
    .filter(
        (F.col("component_kind") == "generation") & (F.col("interval_reference") == DAY)
    )
    .groupBy("market_area_code", "local_date", "carrier_code")
    .agg(F.sum("energy_mwh").alias("mwh"))
)

# COMMAND ----------

# DBTITLE 1,Realised daily generation per scope
_parts = []
for scope, (carriers, status) in SCOPE_CARRIERS.items():
    _parts.append(
        realised_by_carrier.filter(F.col("carrier_code").isin(*carriers))
        .groupBy("market_area_code", "local_date")
        .agg(
            F.sum("mwh").alias("realised_mwh"),
            F.countDistinct("carrier_code").alias("carriers_present"),
        )
        .filter(F.col("carriers_present") == len(carriers))
        .select(
            "market_area_code",
            "local_date",
            F.lit(scope).alias("forecast_scope"),
            "realised_mwh",
            F.lit(status).alias("scope_mapping_status"),
        )
    )
realised = _parts[0]
for p in _parts[1:]:
    realised = realised.unionByName(p)

# COMMAND ----------

# DBTITLE 1,Forecast daily per scope, with the derived PV forecast
forecast = (
    read_gold("generation_forecast", source=SOURCE)
    .filter(F.col("interval_reference") == DAY)
    .select("market_area_code", "local_date", F.explode("components").alias("c"))
    .filter(F.col("c.forecast_scope") != "photovoltaic")
    .groupBy(
        "market_area_code",
        "local_date",
        F.col("c.forecast_scope").alias("forecast_scope"),
    )
    .agg(F.sum("c.energy_mwh").alias("forecast_mwh"))
)
if spark.catalog.tableExists(ml_fqn("derived_pv_forecast", ECO)):
    _pv = (
        read_ml("derived_pv_forecast", ecosystem=ECO)
        .filter(F.col("interval_reference") == DAY)
        .groupBy("market_area_code", "local_date")
        .agg(F.sum("forecast_photovoltaic_mwh").alias("forecast_mwh"))
        .withColumn("forecast_scope", F.lit("photovoltaic"))
        .select("market_area_code", "local_date", "forecast_scope", "forecast_mwh")
    )
    forecast = forecast.unionByName(_pv)
else:
    print("SKIP photovoltaic scope: derived_pv_forecast does not exist yet")

# COMMAND ----------

# DBTITLE 1,Bias per scope and day
candidates = realised.join(
    forecast, ["market_area_code", "local_date", "forecast_scope"], "left"
)
out = (
    candidates.filter(
        F.col("forecast_mwh").isNotNull() & F.col("realised_mwh").isNotNull()
    )
    .withColumn("target_bias_mwh", F.col("realised_mwh") - F.col("forecast_mwh"))
    .withColumn("provenance_tier", F.lit("constructed"))
)
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Record the dropped rows
_dropped = candidates.count() - out.count()
drop_block = ("dropped_rows", markdown_table(["rows_dropped"], [(_dropped,)]))
print(f"rows dropped as invalid: {_dropped}")

# COMMAND ----------

# DBTITLE 1,Grain and forbidden-column gate
_GRAIN = ["market_area_code", "local_date", "forecast_scope"]
assert_unique_grain(out, _GRAIN, component=COMPONENT, source=SOURCE, rid=rid)
assert_no_forbidden_columns(out, component=COMPONENT, source=SOURCE, rid=rid)
check(
    COMPONENT,
    SOURCE,
    "non_empty",
    out.limit(1).count() > 0,
    detail="no rows produced; check the input filters",
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write
write_ml(out, TABLE, ecosystem=ECO, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect and export findings
_blocks = inspect_ml_table(
    read_ml(TABLE, ecosystem=ECO), TABLE, ecosystem=ECO, key_cols=_GRAIN
)
write_ml_findings(ECO, "targets__" + TABLE, TABLE, _blocks)

# COMMAND ----------

# DBTITLE 1,Add the drop count to the findings
write_ml_findings(ECO, "targets__" + TABLE + "__drops", TABLE + " drops", [drop_block])
