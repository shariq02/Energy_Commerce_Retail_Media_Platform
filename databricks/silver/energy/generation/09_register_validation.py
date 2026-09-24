# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- REGISTER VALIDATION (MASTR, PLANT LIST)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** read-only checks across the register structures: identity
# MAGIC uniqueness, disjoint id spaces, link coverage and plant -> unit coverage.

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
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "silver/energy/generation/09_register_validation"
FINDINGS = "mastr"
# structure -> the column register_link ids refer to
ID_COLUMN = {
    "generation_unit": "unit_id",
    "support_registration": "support_registration_id",
    "unit_authorisation": "source_record_id",
    "unit_repowering": "source_record_id",
    "grid_location": "source_record_id",
    "grid_connection_point": "source_record_id",
    "market_actor": "source_record_id",
    "mastr_code_list": "code_id",
}

# COMMAND ----------

# DBTITLE 1,Read Silver -- register structures
T = {name: spark.table(semantic_table(name)) for name in ID_COLUMN}
links = spark.table(semantic_table("register_link"))
plants = spark.table(semantic_table("power_plant_register"))

# COMMAND ----------

# DBTITLE 1,Check -- unit_id unique across unit types
_dup_units = T["generation_unit"].groupBy("unit_id").count().filter("count > 1").count()
report("generation_unit unit_id unique", _dup_units == 0, f"duplicates {_dup_units}")

# COMMAND ----------

# DBTITLE 1,Check -- EEG and KWK registration ids are disjoint
_shared_ids = (
    T["support_registration"]
    .groupBy("support_registration_id")
    .agg(F.countDistinct("support_scheme").alias("schemes"))
    .filter("schemes > 1")
    .count()
)
report("support id spaces disjoint", _shared_ids == 0, f"shared ids {_shared_ids}")

# COMMAND ----------

# DBTITLE 1,Helper -- ids of one structure


def ids_of(name: str):
    return T[name].select(F.col(ID_COLUMN[name]).cast("string").alias("id")).distinct()


# COMMAND ----------

# DBTITLE 1,Check -- link coverage per relationship (parent and linked ids found)
_rows = []
for rel, parent_type, linked_type in (
    links.select("relationship_type", "parent_type", "linked_type").distinct().collect()
):
    _l = links.filter(F.col("relationship_type") == rel)
    _n = _l.count()
    _parent = _l.join(ids_of(parent_type), _l.parent_id == F.col("id"), "left_semi")
    _linked = _l.join(ids_of(linked_type), _l.linked_id == F.col("id"), "left_semi")
    _rows.append((rel, _n, _parent.count() / max(_n, 1), _linked.count() / max(_n, 1)))
keep(
    "register_link: share of parent and linked ids found in their structures",
    spark.createDataFrame(
        _rows,
        (
            "relationship_type string, links long, parent_found double, "
            "linked_found double"
        ),
    ),
)

# COMMAND ----------

# DBTITLE 1,Check -- power_plant_register rows whose MaStR unit id is a generation_unit
_with_id = plants.filter(F.col("mastr_unit_id").isNotNull())
_found = _with_id.join(
    ids_of("generation_unit"), _with_id.mastr_unit_id == F.col("id"), "left_semi"
).count()
_total = _with_id.count()
report(
    "power_plant_register -> generation_unit",
    True,
    f"{_found} of {_total} plant rows with a MaStR unit id match a unit",
    status="INFO",
)

# COMMAND ----------

# DBTITLE 1,Build findings -- check results table
findings_blocks = checks_blocks()

# COMMAND ----------

# DBTITLE 1,Export findings -- register validation
write_silver_findings(
    FINDINGS,
    f"{COMPONENT.split('/')[-1]}__validation",
    "register validation",
    findings_blocks,
)
