# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- DIM MARKET ACTOR
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** `dim_market_actor` -- one row per `market_actor_id`.
# MAGIC `bridge_actor_role` -- one row per (`parent_id`, `linked_id`).
# MAGIC
# MAGIC **Sources:** `mastr_marktakteure` (Silver, energy_silver);
# MAGIC `mastr_actor_role_bridge` (Silver, energy_silver_reference).
# MAGIC
# MAGIC **Serves use case:** any Energy use case needing MaStR market-actor
# MAGIC entities and their role assignments.
# MAGIC
# MAGIC **Purpose:** promote `mastr_marktakteure` to `dim_market_actor` --
# MAGIC natural key `MastrNummer` -> `market_actor_id` per `mappings/mastr.yml`'s
# MAGIC own documented canonical target. `mastr_marktakteure_und_rollen` is not
# MAGIC separately promoted -- its role content is exactly what
# MAGIC `mastr_actor_role_bridge` already captures as an additive relationship;
# MAGIC promoting both would duplicate the same fact. Re-key that bridge against
# MAGIC `dim_market_actor`.

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
SOURCE = "mastr"
COMPONENT = "gold/energy/grid/03_dim_market_actor"
RID = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_marktakteure
_marktakteure_silver = read_silver("mastr_marktakteure")

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_actor_role_bridge
_actor_role_bridge_silver = read_silver("mastr_actor_role_bridge")

# COMMAND ----------

# DBTITLE 1,Transform -- dim_market_actor
dim_market_actor = _marktakteure_silver.withColumnRenamed(
    "MastrNummer", "market_actor_id"
)
dim_market_actor = dim_market_actor.withColumn(
    "market_actor_key", surrogate_key("market_actor_id")
)
dim_market_actor = add_gold_provenance(dim_market_actor, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- dim_market_actor, one row per market_actor_id
assert_unique_grain(
    dim_market_actor,
    ["market_actor_id"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- dim_market_actor
write_gold(
    dim_market_actor, "dim_market_actor", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect dim_market_actor + export findings
_findings_blocks = inspect_gold_table(
    dim_market_actor,
    "dim_market_actor",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["market_actor_id"],
    df_before=_marktakteure_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dim_market_actor",
    "dim_market_actor",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Read Gold -- dim_market_actor
_market_actor = read_gold("dim_market_actor", source="mastr")

# COMMAND ----------

# DBTITLE 1,Transform -- bridge_actor_role
bridge_actor_role = resolve_fk(
    _actor_role_bridge_silver,
    _market_actor,
    fact_key_cols=["parent_id"],
    dim_key_cols=["market_actor_id"],
    dim_surrogate_col="market_actor_key",
    output_col="market_actor_key",
)
bridge_actor_role = bridge_actor_role.withColumn(
    "bridge_actor_role_key", surrogate_key("parent_id", "linked_id")
)
bridge_actor_role = add_gold_provenance(bridge_actor_role, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- bridge_actor_role, one row per (parent_id, linked_id)
assert_unique_grain(
    bridge_actor_role,
    ["parent_id", "linked_id"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- bridge_actor_role
write_gold(
    bridge_actor_role, "bridge_actor_role", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect bridge_actor_role + export findings
_findings_blocks = inspect_gold_table(
    bridge_actor_role,
    "bridge_actor_role",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["parent_id", "linked_id"],
    df_before=_actor_role_bridge_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__bridge_actor_role",
    "bridge_actor_role",
    _findings_blocks,
)
