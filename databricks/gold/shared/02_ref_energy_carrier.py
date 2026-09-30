# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- REF_ENERGY_CARRIER
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `shared_conformed.ref_energy_carrier` (canonical carrier list)
# MAGIC and `ref_energy_carrier_map` (every source carrier value seen, mapped to a
# MAGIC canonical `carrier_key` with `match_basis`). The map is built by
# MAGIC deterministic normalisation of the real values found in Silver, plus a
# MAGIC small `authored` override table for the cross-vocabulary synonyms already
# MAGIC verifiable from the two static source decode maps in the codebase (SMARD's
# MAGIC `BALANCE` dict, `power_plant_list.yml`'s `energietraeger` map) -- e.g.
# MAGIC SMARD's `photovoltaic` and the plant register's `solar_radiation` are the
# MAGIC same carrier under different tokens. MaStR's `main_fuel`/`biomass_type`
# MAGIC decode through a live katalog table, not a static map, so no synonym is
# MAGIC authored for it here without the owner reviewing its real distinct values.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../_gold_common

# COMMAND ----------

# DBTITLE 1,Gold inspection library
# MAGIC %run ../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = "gold/shared/ref_energy_carrier"
CARRIER_TABLE = "ref_energy_carrier"
MAP_TABLE = "ref_energy_carrier_map"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver
generation_unit = read_silver("generation_unit")
power_plant_register = read_silver("power_plant_register")
power_plant_capacity_plan = read_silver("power_plant_capacity_plan")
electricity_balance = read_silver("electricity_balance")

# COMMAND ----------

# DBTITLE 1,Helper -- deterministic carrier_key normalisation


def _carrier_key(col):
    k = F.lower(F.trim(col))
    k = F.regexp_replace(k, r"[^a-z0-9]+", "_")
    return F.regexp_replace(k, r"(^_+|_+$)", "")


# COMMAND ----------

# DBTITLE 1,Candidate raw values -- one row per (origin_source_system, source_vocabulary, source_value)
# origin_source_system is the real per-row Silver source (mastr/power_plant_list/
# smard/honda_iot) -- kept under its own name because add_gold_provenance below
# overwrites a column literally named source_system with this table's own SOURCE
# constant, which would otherwise erase the very distinction this map needs.
_candidates = (
    generation_unit.select(
        F.col("source_system").alias("origin_source_system"),
        F.lit("generation_unit.unit_type").alias("source_vocabulary"),
        F.col("unit_type").alias("source_value"),
    )
    .unionByName(
        generation_unit.select(
            F.col("source_system").alias("origin_source_system"),
            F.lit("generation_unit.main_fuel").alias("source_vocabulary"),
            F.col("main_fuel").alias("source_value"),
        )
    )
    .unionByName(
        generation_unit.select(
            F.col("source_system").alias("origin_source_system"),
            F.lit("generation_unit.biomass_type").alias("source_vocabulary"),
            F.col("biomass_type").alias("source_value"),
        )
    )
    .unionByName(
        power_plant_register.select(
            F.col("source_system").alias("origin_source_system"),
            F.lit("power_plant_register.energy_carrier").alias("source_vocabulary"),
            F.col("energy_carrier").alias("source_value"),
        )
    )
    .unionByName(
        power_plant_capacity_plan.select(
            F.col("source_system").alias("origin_source_system"),
            F.lit("power_plant_capacity_plan.energy_carrier").alias(
                "source_vocabulary"
            ),
            F.col("energy_carrier").alias("source_value"),
        )
    )
    .unionByName(
        electricity_balance.select(
            F.col("source_system").alias("origin_source_system"),
            F.lit("electricity_balance.components.carrier_code").alias(
                "source_vocabulary"
            ),
            F.explode("components.carrier_code").alias("source_value"),
        )
    )
    .filter(F.col("source_value").isNotNull() & (F.trim(F.col("source_value")) != ""))
    .distinct()
)

# COMMAND ----------

# DBTITLE 1,Authored synonym overrides -- same carrier, different vocabulary token
# Verified against the two static source decode maps already in the codebase
# (SMARD's BALANCE dict in 02_electricity_smard.py; power_plant_list.yml's
# energietraeger map) -- not a guess from source_value text alone.
AUTHORED_SYNONYMS = {
    "hydro": "hydropower",
    "offshore_wind": "wind_offshore",
    "onshore_wind": "wind_onshore",
    "photovoltaic": "solar_radiation",
}

# COMMAND ----------

# DBTITLE 1,Build ref_energy_carrier_map
_synonym_map = F.create_map([F.lit(x) for kv in AUTHORED_SYNONYMS.items() for x in kv])
ref_energy_carrier_map = (
    _candidates.withColumn("_normalised_key", _carrier_key(F.col("source_value")))
    .withColumn(
        "carrier_key",
        F.coalesce(_synonym_map[F.col("_normalised_key")], F.col("_normalised_key")),
    )
    .withColumn(
        "match_basis",
        F.when(
            _synonym_map[F.col("_normalised_key")].isNotNull(), F.lit("authored")
        ).otherwise(F.lit("exact")),
    )
    .drop("_normalised_key")
)
ref_energy_carrier_map = add_gold_provenance(ref_energy_carrier_map, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Build ref_energy_carrier
ref_energy_carrier = (
    ref_energy_carrier_map.select("carrier_key", "source_value")
    .groupBy("carrier_key")
    .agg(F.first("source_value", ignorenulls=True).alias("display_label"))
)
ref_energy_carrier = add_gold_provenance(ref_energy_carrier, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gates
assert_unique_grain(
    ref_energy_carrier, ["carrier_key"], component=COMPONENT, source=SOURCE, rid=rid
)
assert_unique_grain(
    ref_energy_carrier_map,
    ["origin_source_system", "source_vocabulary", "source_value"],
    component=COMPONENT,
    source=SOURCE,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(
    ref_energy_carrier,
    CARRIER_TABLE,
    schema=SHARED_CONFORMED_SCHEMA,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
)
write_gold(
    ref_energy_carrier_map,
    MAP_TABLE,
    schema=SHARED_CONFORMED_SCHEMA,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    ref_energy_carrier,
    CARRIER_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    schema=SHARED_CONFORMED_SCHEMA,
    key_cols=["carrier_key"],
)
_blocks += inspect_gold_table(
    ref_energy_carrier_map,
    MAP_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    schema=SHARED_CONFORMED_SCHEMA,
    key_cols=["origin_source_system", "source_vocabulary", "source_value"],
)
write_gold_findings(
    SOURCE, "shared__ref_energy_carrier", "ref_energy_carrier(_map)", _blocks
)
