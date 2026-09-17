# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- DWD_CITY_BUNDESLAND_XREF
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the curated 28-station city -> (Bundesland name, Bundesland AGS) lookup, no Bronze source. Bundesland level only -- the conformed spine has no polygon geometry. One of 7 sibling notebooks in this folder, split
# MAGIC from a single `01_dwd_reference.py` -- see the folder's other files for
# MAGIC the rest. Runs before the DWD measurement notebooks.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Imports

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "dwd"
COMPONENT = "silver/energy/_reference/dwd_reference/01_dwd_city_bundesland_xref"
RID = run_id()

CITY_BUNDESLAND = {
    "berlin": ("Berlin", "11"),
    "hamburg": ("Hamburg", "02"),
    "munich": ("Bayern", "09"),
    "cologne_bonn": ("Nordrhein-Westfalen", "05"),
    "frankfurt_am_main": ("Hessen", "06"),
    "stuttgart": ("Baden-Wuerttemberg", "08"),
    "essen": ("Nordrhein-Westfalen", "05"),
    "leipzig": ("Sachsen", "14"),
    "dresden": ("Sachsen", "14"),
    "nuremberg": ("Bayern", "09"),
    "hannover": ("Niedersachsen", "03"),
    "bremen": ("Bremen", "04"),
    "potsdam": ("Brandenburg", "12"),
    "magdeburg": ("Sachsen-Anhalt", "15"),
    "erfurt": ("Thueringen", "16"),
    "trier": ("Rheinland-Pfalz", "07"),
    "saarbruecken": ("Saarland", "10"),
    "kiel": ("Schleswig-Holstein", "01"),
    "rostock_warnemuende": ("Mecklenburg-Vorpommern", "13"),
    "norderney": ("Niedersachsen", "03"),
    "sylt": ("Schleswig-Holstein", "01"),
    "garmisch_partenkirchen": ("Bayern", "09"),
    "zugspitze": ("Bayern", "09"),
    "hohenpeissenberg": ("Bayern", "09"),
    "feldberg_schwarzwald": ("Baden-Wuerttemberg", "08"),
    "cottbus": ("Brandenburg", "12"),
    "goerlitz": ("Sachsen", "14"),
    "braunschweig": ("Niedersachsen", "03"),
}
assert len(CITY_BUNDESLAND) == 28, len(CITY_BUNDESLAND)

# COMMAND ----------

# DBTITLE 1,Transform -- dwd_city_bundesland_xref (curated, no Bronze source)
xref = spark.createDataFrame(
    [(c, bl, ags, "bundesland") for c, (bl, ags) in CITY_BUNDESLAND.items()],
    "city string, bundesland_name string, ags_code string, ags_level string",
)

# COMMAND ----------

# DBTITLE 1,Write Silver -- dwd_city_bundesland_xref
write_silver(
    xref, "dwd_city_bundesland_xref", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect dwd_city_bundesland_xref + export findings
_findings_blocks = inspect_table(
    xref,
    "dwd_city_bundesland_xref",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["city"],
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dwd_city_bundesland_xref",
    "dwd_city_bundesland_xref",
    _findings_blocks,
)
