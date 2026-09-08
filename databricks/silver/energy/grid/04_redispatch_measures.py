# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- REDISPATCH MEASURES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `redispatch_measures` into source-scoped Silver at its
# MAGIC (measure start, affected unit, direction) grain. German date + time text
# MAGIC parsed Europe/Berlin to UTC, coded columns decoded, `ANFORDERNDER_UENB`
# MAGIC split to `requesting_tso_list`, the affected-plant name kept as free
# MAGIC text. Byte-identical duplicates collapse; residual same-key groups get a
# MAGIC content-hash ordinal; inverted time windows are recorded to quarantine,
# MAGIC not dropped.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Imports + config
from pyspark.sql import functions as F

SOURCE = "redispatch"
COMPONENT = "silver/energy/grid/04_redispatch_measures"
RID = run_id()
BT = "redispatch_measures"

MAPPING = load_mapping(SOURCE)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
LABELS = MAPPING["coded_value_labels"]

CODED = {
    "GRUND_DER_MASSNAHME": ("measure_reason", LABELS["grund_der_massnahme"]["map"]),
    "RICHTUNG": ("direction", LABELS["richtung"]["map"]),
    "PRIMAERENERGIEART": (
        "affected_unit_primary_energy",
        LABELS["primaerenergieart"]["map"],
    ),
    "ANWEISENDER_UENB": ("instructing_tso", LABELS["instructing_tso"]["map"]),
}
KEY_COLS = [
    "BEGINN_DATUM",
    "BEGINN_UHRZEIT",
    "BETROFFENE_ANLAGE",
    "RICHTUNG",
    "ANWEISENDER_UENB",
]
DE_TS_FORMATS = ("dd.MM.yyyy HH:mm:ss", "dd.MM.yyyy HH:mm", "dd.MM.yyyy H:mm")

# COMMAND ----------

# DBTITLE 1,redispatch_measures -> Silver
df = read_bronze(BT).dropDuplicates()
df = df.withColumn("_srid_base", F.concat_ws("|", *[F.col(c) for c in KEY_COLS]))

for c in ("MITTLERE_LEISTUNG_MW", "MAXIMALE_LEISTUNG_MW", "GESAMTE_ARBEIT_MWH"):
    df = df.withColumn(c, F.col(c).cast("double"))

for raw, (pref, en_map) in CODED.items():
    df = decode_via_labeled_map(df, raw, pref, en_map).drop(raw)

df = apply_renames(df, NAME_MAP)

df = (
    df.withColumn(
        "measure_start_ts",
        parse_ts(
            F.concat_ws(" ", F.col("measure_start_date"), F.col("measure_start_time")),
            DE_TS_FORMATS,
            "Europe/Berlin",
        ),
    )
    .withColumn(
        "measure_end_ts",
        parse_ts(
            F.concat_ws(" ", F.col("measure_end_date"), F.col("measure_end_time")),
            DE_TS_FORMATS,
            "Europe/Berlin",
        ),
    )
    .withColumn(
        "requesting_tso_list",
        F.split(
            F.regexp_replace(F.trim(F.col("requesting_tso")), r"\s*&\s*", "&"), "&"
        ),
    )
)

inv = df.filter(F.col("measure_start_ts") > F.col("measure_end_ts"))
write_quarantine(
    _q_rows(
        inv,
        "inverted_measure_window",
        "measure_start_ts is after measure_end_ts",
        "measure_start_ts,measure_end_ts",
        "measure_start_ts",
        "_srid_base",
        SOURCE,
        BT,
    ),
    RID,
)

content = [c for c in df.columns if c not in ("_srid_base",)]
df = within_group_ordinal(df, ["_srid_base"], content)
df = df.withColumn("_srid", sha_key("_srid_base", "_src_id_ord")).drop("_srid_base")
df = add_provenance(df, SOURCE, "_srid", RID)
write_silver(df, BT, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 70)
print(f"REDISPATCH MEASURES -- COMPLETE  (run_id {RID})")
print("=" * 70)
