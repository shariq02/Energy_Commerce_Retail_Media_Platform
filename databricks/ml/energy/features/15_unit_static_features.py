# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # UNIT STATIC FEATURES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** wind unit registry attributes known at commissioning for the survival
# MAGIC model. No column derived from the decommissioning outcome is included.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "mastr"
COMPONENT = "ml/energy/features/unit_static_features"
TABLE = "features_unit_static"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Wind units with a commissioning date
units = wind_units(read_gold("generation_unit", source=SOURCE)).filter(
    F.col("lifecycle_state").isin(*OPERATING_STATES)
    & F.col("commissioning_date").isNotNull()
)

# COMMAND ----------

# DBTITLE 1,Zone status and filled hub height
_zm = unit_zone_map(
    units,
    read_gold("grid_connection_point", source=SOURCE),
    read_ml("zone_code_map", ecosystem=ECO),
)
_hub = read_ml("features_hub_height_fill", ecosystem=ECO).select(
    "unit_id", "hub_height_m_filled", "hub_height_was_imputed"
)

# COMMAND ----------

# DBTITLE 1,Assemble the static features
out = (
    units.select(
        "unit_id",
        "capacity_net_kw",
        "capacity_gross_kw",
        "rotor_diameter_m",
        "manufacturer",
        "model_designation",
        "latitude",
        "longitude",
        "federal_state",
        "operator_id",
        "is_offshore",
        F.year("commissioning_date").alias("commissioning_year"),
        F.to_date("commissioning_date").alias("commissioning_date"),
    )
    .join(
        _zm.select(
            "unit_id",
            F.col("market_area_code").alias("control_zone_code"),
            "zone_status",
        ),
        "unit_id",
        "left",
    )
    .join(_hub, "unit_id", "left")
)
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Grain and forbidden-column gate
_GRAIN = ["unit_id"]
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
write_ml_findings(ECO, "features__" + TABLE, TABLE, _blocks)
