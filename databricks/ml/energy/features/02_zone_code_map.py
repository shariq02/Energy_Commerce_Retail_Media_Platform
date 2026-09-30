# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ZONE CODE MAP
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** view mapping the four control-zone names to the market-table zone codes,
# MAGIC with a preflight that every control zone in the connection-point table is
# MAGIC mapped.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "mastr"
COMPONENT = "ml/energy/features/zone_code_map"
TABLE = "zone_code_map"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Control zone values present in the connection points
_present = [
    r[0]
    for r in read_gold("grid_connection_point", source=SOURCE)
    .select(normalise_zone_key(F.col("control_zone")))
    .distinct()
    .collect()
    if r[0]
]
print("normalised control zone values:", sorted(_present))

# COMMAND ----------

# DBTITLE 1,Create the view
write_ml_view(
    """
SELECT * FROM VALUES
  ('50hertz', 'fifty_hertz', '50Hertz'),
  ('amprion', 'amprion', 'Amprion'),
  ('tennet', 'tennet_de', 'TenneT'),
  ('transnetbw', 'transnetbw', 'TransnetBW')
AS t(control_zone_key, market_area_code, control_zone_name)
""",
    TABLE,
    ecosystem=ECO,
)

# COMMAND ----------

# DBTITLE 1,Preflight -- every control zone value is mapped
_known = {r["control_zone_key"] for r in read_ml(TABLE, ecosystem=ECO).collect()}
_unmapped = sorted(set(_present) - _known)
check(
    COMPONENT,
    SOURCE,
    "control_zones_mapped",
    not _unmapped,
    detail=f"unmapped control zone values: {_unmapped}",
    rid=rid,
)
print(f"OK  {len(_present)} control zone value(s), all mapped")
