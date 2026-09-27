# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- MARKET_ACTOR + MARKET_ACTOR_ROLE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_gold.market_actor` -- the actor dimension plus
# MAGIC `n_units_operated`. `energy_gold.market_actor_role` is a separate view
# MAGIC over Silver `market_actor_role` -- a market actor legitimately holds more
# MAGIC than one role registration (Silver's own docstring: "separate
# MAGIC identities"), so it is not merged into `market_actor`; the actor <-> role
# MAGIC relationship is already carried by `register_link` (`entity_link`).
# MAGIC Grain: `market_actor` = actor id; `market_actor_role` = role id.

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
SOURCE = "mastr"
COMPONENT = "gold/energy/grid/market_actor"
TABLE = "market_actor"
ROLE_TABLE = "market_actor_role"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,market_actor_role -- view (one row per role registration, not merged into market_actor)
write_gold_view(f"SELECT * FROM {silver_fqn(ROLE_TABLE)}", ROLE_TABLE, source=SOURCE)

# COMMAND ----------

# DBTITLE 1,Read Silver
market_actor = read_silver("market_actor")
generation_unit = read_silver("generation_unit").select("operator_id")

# COMMAND ----------

# DBTITLE 1,Derive n_units_operated
_unit_counts = generation_unit.groupBy(
    F.col("operator_id").alias("market_actor_id")
).agg(F.count(F.lit(1)).alias("n_units_operated"))
market_actor = market_actor.join(_unit_counts, "market_actor_id", "left").fillna(
    {"n_units_operated": 0}
)

# COMMAND ----------

# DBTITLE 1,Build the Gold row
market_actor = market_actor.drop("_silver_loaded_at", "_silver_run_id")
market_actor = add_gold_provenance(market_actor, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
assert_unique_grain(
    market_actor, ["market_actor_id"], component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(market_actor, TABLE, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    market_actor,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["market_actor_id"],
)
_blocks += inspect_gold_table(
    read_gold(ROLE_TABLE, source=SOURCE),
    ROLE_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["market_actor_role_id"],
)
write_gold_findings(SOURCE, f"grid__{TABLE}", f"{TABLE}(_role)", _blocks)
