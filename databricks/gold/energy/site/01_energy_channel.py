# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- ENERGY_CHANNEL
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_gold.energy_channel` -- the channel entity (there is
# MAGIC no real device serial), derived from the distinct site x subsystem x
# MAGIC channel combinations `channel_reading` actually carries. **Run this
# MAGIC notebook after `02_channel_reading.py`** -- despite the file numbering,
# MAGIC this dimension is built from that fact's own output, not the reverse.
# MAGIC Grain: site x subsystem x channel.

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
COMPONENT = "gold/energy/site/energy_channel"
TABLE = "energy_channel"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read the already-built channel_reading
channel_reading = read_gold("channel_reading", source=SOURCE)

# COMMAND ----------

# DBTITLE 1,Derive distinct channels per site
energy_channel = channel_reading.select(
    "location_key", "subsystem", "channel"
).distinct()
energy_channel = add_gold_provenance(energy_channel, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
assert_unique_grain(
    energy_channel,
    ["location_key", "subsystem", "channel"],
    component=COMPONENT,
    source=SOURCE,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(energy_channel, TABLE, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    energy_channel,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["location_key", "subsystem", "channel"],
)
write_gold_findings(SOURCE, f"site__{TABLE}", TABLE, _blocks)
