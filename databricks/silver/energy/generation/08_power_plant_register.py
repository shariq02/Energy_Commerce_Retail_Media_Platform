# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- POWER PLANT REGISTER (BNETZA PLANT LIST)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the plant list as a source-specific `power_plant_register`
# MAGIC (linked to `generation_unit` only through the MaStR unit id) and the
# MAGIC carrier x year `power_plant_capacity_plan` report.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Semantic structures library
# MAGIC %run ../../_semantic_common

# COMMAND ----------

# DBTITLE 1,Imports
from functools import reduce
from operator import or_

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "power_plant_list"
COMPONENT = "silver/energy/generation/08_power_plant_register"
RID = run_id()
FINDINGS = "power_plant_list"
PLANT_BT = "power_plant_list"
PLAN_BT = "power_plant_capacity_additions"
MAPPING = load_mapping(SOURCE)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
LABELS = MAPPING["coded_value_labels"]
GERMAN_DECIMALS = [
    "Bruttoleistung_MW",
    "Nettonennleistung_MW",
    "Grenzkraftwerk_Nettonennleistung_MW",
]
YEAR_COLS = ["Jahr_Inbetriebnahme", "Jahr_Stilllegung"]
JA_NEIN = [
    "Waermeauskopplung_KWK",
    "Erneuerbarer_Energietraeger",
    "Bestandteil_Grenzkraftwerk",
]
CODED = {
    "Datensatztyp": ("record_type", LABELS["datensatztyp"]["map"]),
    "Kraftwerksstatus": ("operating_status", LABELS["kraftwerksstatus"]["map"]),
    "Energietraeger": ("energy_carrier", LABELS["energietraeger"]["map"]),
    "Volleinspeisung_Teileinspeisung": ("feed_in_type", LABELS["feed_in_type"]["map"]),
}
PLAN_NUM_COLS = ["2026", "2027", "2028", "2029", "2026_2029_total"]
_FOLDS = (("ä", "ae"), ("ö", "oe"), ("ü", "ue"), ("ß", "ss"))


def _fold(c):
    """Lower-case, trim and transliterate umlauts (Column or str)."""
    if isinstance(c, str):
        c = c.lower().strip()
        for a, b in _FOLDS:
            c = c.replace(a, b)
        return c
    c = F.lower(F.trim(c))
    for a, b in _FOLDS:
        c = F.regexp_replace(c, a, b)
    return c


REAL_CARRIERS = [_fold(k) for k in LABELS["energietraeger"]["map"]] + ["insgesamt"]
KEYS = {
    "power_plant_register": [
        "mastr_unit_id",
        "plant_name",
        "commissioning_year",
        "_source_id_ordinal",
    ],
    "power_plant_capacity_plan": ["capacity_section", "energy_carrier"],
}

# COMMAND ----------

# DBTITLE 1,Read Bronze -- power_plant_list
plant_bronze = read_bronze(PLANT_BT)

# COMMAND ----------

# DBTITLE 1,Read Bronze -- power_plant_capacity_additions
plan_bronze = read_bronze(PLAN_BT)

# COMMAND ----------

# DBTITLE 1,Transform -- power_plant_register (typing + decode)
plants = plant_bronze
for c in GERMAN_DECIMALS:
    plants = plants.withColumn(c, german_decimal(c))
for c in YEAR_COLS:
    plants = plants.withColumn(c, F.col(c).cast("int"))
for c in JA_NEIN:
    plants = plants.withColumn(
        c,
        F.when(F.col(c) == "Ja", F.lit(True))
        .when(F.col(c) == "Nein", F.lit(False))
        .otherwise(F.lit(None).cast("boolean")),
    )
for raw, (pref, en_map) in CODED.items():
    plants = decode_via_labeled_map(plants, raw, pref, en_map).drop(raw)
plants = apply_renames(plants, NAME_MAP)

# COMMAND ----------

# DBTITLE 1,Transform -- flag rows with no capacity value
plants, capacity_q = value_quarantine(
    plants,
    F.col("capacity_net_mw").isNull() & F.col("border_plant_net_capacity_mw").isNull(),
    flag_col="_capacity_all_null",
    rule_id="german_comma_decimal",
    reason="no capacity value parsed on the row",
    field_name="capacity_gross_mw,capacity_net_mw",
    value_col="plant_name",
    sr_id_col="plant_name",
    source_system=SOURCE,
    bronze_table=PLANT_BT,
)

# COMMAND ----------

# DBTITLE 1,Write Quarantine -- power_plant_register rows with no capacity
write_quarantine(capacity_q, RID)

# COMMAND ----------

# DBTITLE 1,Transform -- Bundesland to AGS (Germany rows)
_ags = bundesland_ags("federal_state")
plants = (
    plants.withColumn(
        "official_municipality_key", F.when(F.col("country") == "Deutschland", _ags)
    )
    .withColumn("official_municipality_key_level", F.lit("bundesland"))
    .withColumn("official_municipality_key_method", F.lit("bundesland_code"))
    .withColumn(
        "_bundesland_out_of_set",
        (F.col("country") == "Deutschland")
        & F.col("federal_state").isNotNull()
        & _ags.isNull(),
    )
)

# COMMAND ----------

# DBTITLE 1,Transform -- carrier cross-check against the MaStR fuel katalog
# MaStR's fuel catalog is "Brennstoff"; additive, never replaces the decode.
_fuel = (
    mastr_catalog_ref("Brennstoff")
    .select(F.col("cat_wert").alias("_mastr_carrier_value"))
    .dropDuplicates()
)
plants = (
    plants.join(
        F.broadcast(_fuel),
        F.col("energy_carrier_code") == F.col("_mastr_carrier_value"),
        "left",
    )
    .withColumn(
        "energy_carrier_mastr_matched", F.col("_mastr_carrier_value").isNotNull()
    )
    .drop("_mastr_carrier_value")
)

# COMMAND ----------

# DBTITLE 1,Transform -- power_plant_register (record key + provenance)
_key = F.concat_ws(
    "|",
    F.coalesce(F.col("mastr_unit_id"), F.lit("NA")),
    F.col("plant_name"),
    F.col("commissioning_year").cast("string"),
)
plants = plants.withColumn("_ppl_key", _key)
plants = within_group_ordinal(
    plants,
    ["_ppl_key"],
    [c for c in plants.columns if c not in ("_ppl_key", "_capacity_all_null")],
)
plants = plants.withColumn("_srid", sha_key("_ppl_key", "_source_id_ordinal")).drop(
    "_ppl_key", "_capacity_all_null"
)
plants = add_semantic_provenance(plants, SOURCE, PLANT_BT, RID, "_srid")

# COMMAND ----------

# DBTITLE 1,Transform -- power_plant_capacity_plan (section + total flag; title/footnote rows quarantined)
plan_src = plan_bronze
_has_number = reduce(or_, [F.col(c).isNotNull() for c in PLAN_NUM_COLS])
plan, footnote_q = row_quarantine(
    plan_src,
    ~(_fold(F.col("energietraeger")).isin(REAL_CARRIERS) | _has_number),
    rule_id="capacity_additions_footnote_rows",
    reason="energietraeger is a section title / legal footnote with no numbers",
    field_name="energietraeger",
    value_col="energietraeger",
    sr_id_col="energietraeger",
    source_system=SOURCE,
    bronze_table=PLAN_BT,
)
for c in PLAN_NUM_COLS:
    plan = plan.withColumn(c, F.col(c).cast("double"))
# Section derived from content: the source has no row order.
_fold_col = _fold(F.col("energietraeger"))
_is_carrier = _fold_col.isin(REAL_CARRIERS) & (_fold_col != "insgesamt")
_add_total = plan.filter(_is_carrier).agg(F.sum("2026_2029_total")).first()[0] or 0.0
_section = (
    F.when(_is_carrier, F.lit("additions"))
    .when(
        (_fold_col == "insgesamt")
        & (F.abs(F.col("2026_2029_total") - F.lit(_add_total)) < 0.05),
        F.lit("additions"),
    )
    .otherwise(F.lit("retirements"))
)
plan = (
    plan.withColumn("capacity_section", _section)
    .withColumn("energietraeger", F.trim(F.col("energietraeger")))
    .withColumn("is_total", _fold(F.col("energietraeger")) == "insgesamt")
    .withColumnRenamed("energietraeger", "energy_carrier")
    .withColumnRenamed("2026_2029_total", "retiring_capacity_total_2026_2029_mw")
)
plan = within_group_ordinal(
    plan, ["capacity_section", "energy_carrier"], ["2026", "2027", "2028", "2029"]
)
plan = plan.withColumn(
    "_srid", sha_key("capacity_section", "energy_carrier", "_source_id_ordinal")
)
plan = add_semantic_provenance(plan, SOURCE, PLAN_BT, RID, "_srid")

# COMMAND ----------

# DBTITLE 1,Write Quarantine -- power_plant_capacity_plan footnote rows
write_quarantine(footnote_q, RID)

# COMMAND ----------

# DBTITLE 1,Configuration -- structure -> frame
STRUCTURES = {"power_plant_register": plants, "power_plant_capacity_plan": plan}

# COMMAND ----------

# DBTITLE 1,Write Silver -- power_plant_register and power_plant_capacity_plan
for name, frame in STRUCTURES.items():
    write_semantic(frame, name, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- power_plant_register and power_plant_capacity_plan
_register = spark.table(semantic_table("power_plant_register"))
findings_blocks = {
    name: inspect_table(
        spark.table(semantic_table(name)),
        name,
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=KEYS[name],
    )
    for name in STRUCTURES
}
findings_blocks["power_plant_register"].append(
    (
        "flags",
        dict_to_markdown_row(
            {
                "bundesland_out_of_set": _register.filter(
                    F.col("_bundesland_out_of_set")
                ).count(),
                "carrier_not_in_mastr_katalog": _register.filter(
                    ~F.col("energy_carrier_mastr_matched")
                ).count(),
            }
        ),
    )
)

# COMMAND ----------

# DBTITLE 1,Export findings -- power_plant_register and power_plant_capacity_plan
for name, blocks in findings_blocks.items():
    write_silver_findings(FINDINGS, f"{COMPONENT.split('/')[-1]}__{name}", name, blocks)
