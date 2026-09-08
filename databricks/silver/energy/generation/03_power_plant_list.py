# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- BNETZA POWER PLANT LIST
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `power_plant_list` and `power_plant_capacity_additions`
# MAGIC into source-scoped Silver. German-comma decimals to double, `Jahr_*` to
# MAGIC integer years, coded German columns decoded, Bundesland to AGS. The
# MAGIC capacity-additions table keeps its wide year columns; its ingested
# MAGIC footnote / section-header rows are row-quarantined. The MaStR unit id is
# MAGIC a reconciliation key, never an identity join.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Imports + config
from pyspark.sql import functions as F

SOURCE = "power_plant_list"
COMPONENT = "silver/energy/generation/03_power_plant_list"
RID = run_id()

CONTRACT = load_contract(SOURCE)
MAPPING = load_mapping(SOURCE)
TABLES = contract_tables(CONTRACT)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
LABELS = MAPPING["coded_value_labels"]

PLANT_BT = "power_plant_list"
ADD_BT = "power_plant_capacity_additions"

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
# Datensatztyp / Kraftwerksstatus / Energietraeger / Volleinspeisung_Teileinspeisung
CODED = {
    "Datensatztyp": ("record_type", LABELS["datensatztyp"]["map"]),
    "Kraftwerksstatus": ("operating_status", LABELS["kraftwerksstatus"]["map"]),
    "Energietraeger": ("energy_carrier", LABELS["energietraeger"]["map"]),
    "Volleinspeisung_Teileinspeisung": ("feed_in_type", LABELS["feed_in_type"]["map"]),
}
REAL_CARRIERS = set(LABELS["energietraeger"]["map"]) | {"Insgesamt", "insgesamt"}

BUNDESLAND_NAME_AGS = {
    "Baden-Württemberg": "08",
    "Baden-Wuerttemberg": "08",
    "Bayern": "09",
    "Berlin": "11",
    "Brandenburg": "12",
    "Bremen": "04",
    "Hamburg": "02",
    "Hessen": "06",
    "Mecklenburg-Vorpommern": "13",
    "Niedersachsen": "03",
    "Nordrhein-Westfalen": "05",
    "Rheinland-Pfalz": "07",
    "Saarland": "10",
    "Sachsen": "14",
    "Sachsen-Anhalt": "15",
    "Schleswig-Holstein": "01",
    "Thüringen": "16",
    "Thueringen": "16",
}


def _bundesland_ags(colname: str):
    m = F.create_map([F.lit(x) for kv in BUNDESLAND_NAME_AGS.items() for x in kv])
    return m[F.col(colname)]


# COMMAND ----------

# DBTITLE 1,power_plant_list -> Silver
p = read_bronze(PLANT_BT)
for c in GERMAN_DECIMALS:
    p = p.withColumn(c, german_decimal(c))
for c in YEAR_COLS:
    p = p.withColumn(c, F.col(c).cast("int"))
for c in JA_NEIN:
    p = p.withColumn(
        c,
        F.when(F.col(c) == "Ja", F.lit(True))
        .when(F.col(c) == "Nein", F.lit(False))
        .otherwise(F.lit(None).cast("boolean")),
    )
for raw, (pref, en_map) in CODED.items():
    p = decode_via_labeled_map(p, raw, pref, en_map).drop(raw)

p = apply_renames(p, NAME_MAP)

p, q = value_quarantine(
    p,
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
write_quarantine(q, RID)

p = (
    p.withColumn(
        "ags_code",
        F.when(F.col("country") == "Deutschland", _bundesland_ags("federal_state")),
    )
    .withColumn("ags_level", F.lit("bundesland"))
    .withColumn("ags_method", F.lit("bundesland_code"))
)

p = p.withColumn(
    "_ppl_key",
    F.concat_ws(
        "|",
        F.coalesce(F.col("mastr_unit_id"), F.lit("NA")),
        F.col("plant_name"),
        F.col("commissioning_year").cast("string"),
    ),
)
content = [c for c in p.columns if c not in ("_ppl_key", "_capacity_all_null")]
p = within_group_ordinal(p, ["_ppl_key"], content)
p = p.withColumn("_srid", sha_key("_ppl_key", "_src_id_ord")).drop(
    "_ppl_key", "_capacity_all_null"
)
p = add_provenance(p, SOURCE, "_srid", RID)
write_silver(p, PLANT_BT, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,power_plant_capacity_additions -> Silver (wide; footnote rows quarantined)
a = read_bronze(ADD_BT)
a, q = row_quarantine(
    a,
    ~F.col("energietraeger").isin(list(REAL_CARRIERS)),
    rule_id="capacity_additions_footnote_rows",
    reason="energietraeger is a section header / sub-total / legal footnote, not a carrier",
    field_name="energietraeger",
    value_col="energietraeger",
    sr_id_col="energietraeger",
    source_system=SOURCE,
    bronze_table=ADD_BT,
)
write_quarantine(q, RID)

for c in ("2026", "2027", "2028", "2029", "2026_2029_total"):
    a = a.withColumn(c, german_decimal(c))
a = a.withColumnRenamed("energietraeger", "energy_carrier").withColumnRenamed(
    "2026_2029_total", "retiring_capacity_total_2026_2029_mw"
)
a = within_group_ordinal(a, ["energy_carrier"], ["2026", "2027", "2028", "2029"])
a = a.withColumn("_srid", sha_key("energy_carrier", "_src_id_ord"))
a = add_provenance(a, SOURCE, "_srid", RID)
write_silver(a, ADD_BT, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 70)
print(f"POWER PLANT LIST -- COMPLETE  (run_id {RID})")
print("=" * 70)
