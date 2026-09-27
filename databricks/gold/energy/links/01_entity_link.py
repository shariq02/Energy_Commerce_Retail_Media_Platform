# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- ENTITY_LINK
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_gold.entity_link` -- Silver's own `register_link`
# MAGIC rows plus every real, non-fuzzy FK already present as a plain attribute
# MAGIC column elsewhere (unit-operator, unit-location, unit-support, unit-
# MAGIC authorisation, plant-unit, connection-point-location/network/operator,
# MAGIC operator-change previous/new operator). `redispatch`'s intervention-to-
# MAGIC plant name match is NOT included here -- Silver stores the matched name
# MAGIC and a confidence, not a resolved key, and re-deriving that match here
# MAGIC would duplicate fuzzy logic rather than carry a real one; add it once a
# MAGIC resolved key exists. `parent_resolved`/`linked_resolved` check against
# MAGIC this notebook's own entity-kind labels ("unit", "market_actor", ...);
# MAGIC `register_link`'s own `parent_type`/`linked_type` values are carried
# MAGIC through as-is and only resolve if they already match that vocabulary --
# MAGIC verify against a real run before trusting a False there. Grain:
# MAGIC relationship type x parent x linked (with an ordinal for any exact
# MAGIC repeats).

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

# DBTITLE 1,Imports
from pyspark.sql import Window

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = "gold/energy/links/entity_link"
TABLE = "entity_link"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver
register_link = read_silver("register_link")
generation_unit = read_silver("generation_unit")
grid_connection_point = read_silver("grid_connection_point")
grid_operator_change_event = read_silver("grid_operator_change_event")
power_plant_register = read_silver("power_plant_register")

# COMMAND ----------

# DBTITLE 1,Existence sets -- for parent_resolved / linked_resolved
_unit_ids = generation_unit.select("unit_id").distinct()
_market_actor_ids = read_silver("market_actor").select("market_actor_id").distinct()
_location_ids = read_silver("grid_location").select("location_id").distinct()
_support_ids = (
    read_silver("support_registration").select("support_registration_id").distinct()
)
_authorisation_ids = (
    read_silver("unit_authorisation").select("authorisation_id").distinct()
)
_connection_point_ids = grid_connection_point.select("connection_point_id").distinct()
_grid_ids = read_silver("grid_network").select("grid_id").distinct()

_all_ids = (
    _unit_ids.withColumnRenamed("unit_id", "_id")
    .withColumn("_kind", F.lit("unit"))
    .unionByName(
        _market_actor_ids.withColumnRenamed("market_actor_id", "_id").withColumn(
            "_kind", F.lit("market_actor")
        )
    )
    .unionByName(
        _location_ids.withColumnRenamed("location_id", "_id").withColumn(
            "_kind", F.lit("location")
        )
    )
    .unionByName(
        _support_ids.withColumnRenamed("support_registration_id", "_id").withColumn(
            "_kind", F.lit("support_registration")
        )
    )
    .unionByName(
        _authorisation_ids.withColumnRenamed("authorisation_id", "_id").withColumn(
            "_kind", F.lit("authorisation")
        )
    )
    .unionByName(
        _connection_point_ids.withColumnRenamed(
            "connection_point_id", "_id"
        ).withColumn("_kind", F.lit("connection_point"))
    )
    .unionByName(
        _grid_ids.withColumnRenamed("grid_id", "_id").withColumn("_kind", F.lit("grid"))
    )
    .unionByName(
        power_plant_register.select("source_record_id")
        .distinct()
        .withColumnRenamed("source_record_id", "_id")
        .withColumn("_kind", F.lit("plant"))
    )
)


# COMMAND ----------

# DBTITLE 1,register_link rows -- carried through
# origin_source_system preserves the real per-row Silver source -- the plant_unit
# links below come from power_plant_register (power_plant_list), not mastr; add_gold_
# provenance overwrites a column literally named source_system with this table's own
# SOURCE constant, which would otherwise mislabel those rows as mastr.
_LINK_COLUMNS = [
    "relationship_type",
    "parent_type",
    "parent_id",
    "linked_type",
    "linked_id",
    "link_basis",
    "match_confidence",
    "source_record_id",
    "source_dataset",
    "origin_source_system",
]
_from_register_link = register_link.select(
    "relationship_type",
    "parent_type",
    "parent_id",
    "linked_type",
    "linked_id",
    F.lit("register_link").alias("link_basis"),
    F.lit(None).cast("string").alias("match_confidence"),
    "source_record_id",
    "source_dataset",
    F.col("source_system").alias("origin_source_system"),
)

# COMMAND ----------

# DBTITLE 1,Helper -- one attribute-FK link


def _attribute_link(
    df, relationship_type, parent_type, parent_id_col, linked_type, linked_id_col
):
    return df.filter(F.col(linked_id_col).isNotNull()).select(
        F.lit(relationship_type).alias("relationship_type"),
        F.lit(parent_type).alias("parent_type"),
        F.col(parent_id_col).alias("parent_id"),
        F.lit(linked_type).alias("linked_type"),
        F.col(linked_id_col).alias("linked_id"),
        F.lit("attribute_fk").alias("link_basis"),
        F.lit(None).cast("string").alias("match_confidence"),
        "source_record_id",
        "source_dataset",
        F.col("source_system").alias("origin_source_system"),
    )


# COMMAND ----------

# DBTITLE 1,Attribute-FK links from generation_unit
_unit_operator = _attribute_link(
    generation_unit, "unit_operator", "unit", "unit_id", "market_actor", "operator_id"
)
_unit_location = _attribute_link(
    generation_unit, "unit_location", "unit", "unit_id", "location", "location_id"
)
_unit_support_eeg = _attribute_link(
    generation_unit,
    "unit_support_registration",
    "unit",
    "unit_id",
    "support_registration",
    "renewable_energy_act_support_id",
)
_unit_support_kwk = _attribute_link(
    generation_unit,
    "unit_support_registration",
    "unit",
    "unit_id",
    "support_registration",
    "combined_heat_and_power_support_id",
)
_unit_authorisation_link = _attribute_link(
    generation_unit,
    "unit_authorisation",
    "unit",
    "unit_id",
    "authorisation",
    "authorisation_id",
)

# COMMAND ----------

# DBTITLE 1,Attribute-FK links from power_plant_register (plant -> unit)
_plant_unit = _attribute_link(
    power_plant_register,
    "plant_unit",
    "plant",
    "source_record_id",
    "unit",
    "mastr_unit_id",
)

# COMMAND ----------

# DBTITLE 1,Attribute-FK links from grid_connection_point
_cp_location = _attribute_link(
    grid_connection_point,
    "connection_point_location",
    "connection_point",
    "connection_point_id",
    "location",
    "location_id",
)
_cp_network = _attribute_link(
    grid_connection_point,
    "connection_point_network",
    "connection_point",
    "connection_point_id",
    "grid",
    "grid_id",
)
_cp_operator = _attribute_link(
    grid_connection_point,
    "connection_point_operator",
    "connection_point",
    "connection_point_id",
    "market_actor",
    "grid_operator_id",
)

# COMMAND ----------

# DBTITLE 1,Attribute-FK links from grid_operator_change_event
_operator_change_previous = _attribute_link(
    grid_operator_change_event,
    "operator_change_previous_operator",
    "unit",
    "unit_id",
    "market_actor",
    "previous_grid_operator_id",
)
_operator_change_new = _attribute_link(
    grid_operator_change_event,
    "operator_change_new_operator",
    "unit",
    "unit_id",
    "market_actor",
    "new_grid_operator_id",
)

# COMMAND ----------

# DBTITLE 1,Union everything
entity_link = (
    _from_register_link.select(*_LINK_COLUMNS)
    .unionByName(_unit_operator)
    .unionByName(_unit_location)
    .unionByName(_unit_support_eeg)
    .unionByName(_unit_support_kwk)
    .unionByName(_unit_authorisation_link)
    .unionByName(_plant_unit)
    .unionByName(_cp_location)
    .unionByName(_cp_network)
    .unionByName(_cp_operator)
    .unionByName(_operator_change_previous)
    .unionByName(_operator_change_new)
)

# COMMAND ----------

# DBTITLE 1,Resolve parent_resolved / linked_resolved
_parent_check = _all_ids.select(
    F.col("_id").alias("parent_id"),
    F.col("_kind").alias("parent_type"),
    F.lit(True).alias("parent_resolved"),
)
_linked_check = _all_ids.select(
    F.col("_id").alias("linked_id"),
    F.col("_kind").alias("linked_type"),
    F.lit(True).alias("linked_resolved"),
)
entity_link = entity_link.join(
    _parent_check, ["parent_id", "parent_type"], "left"
).fillna({"parent_resolved": False})
entity_link = entity_link.join(
    _linked_check, ["linked_id", "linked_type"], "left"
).fillna({"linked_resolved": False})

# COMMAND ----------

# DBTITLE 1,Derive ordinal and build the Gold row
_w = Window.partitionBy("relationship_type", "parent_id", "linked_id").orderBy(
    F.col("source_record_id").asc_nulls_last()
)
entity_link = entity_link.withColumn("link_ordinal", F.row_number().over(_w))
entity_link = add_gold_provenance(entity_link, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
assert_unique_grain(
    entity_link,
    ["relationship_type", "parent_id", "linked_id", "link_ordinal"],
    component=COMPONENT,
    source=SOURCE,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(entity_link, TABLE, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    entity_link,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["relationship_type", "parent_id", "linked_id", "link_ordinal"],
)
write_gold_findings(SOURCE, f"links__{TABLE}", TABLE, _blocks)
