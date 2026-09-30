# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # WEAK SUPERVISION LABELS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** labelling functions over the series coverage reconciliation, declared data
# MAGIC gaps and the registry migration era. Records per-function coverage and the
# MAGIC conflict rate between the two series functions.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "mastr"
COMPONENT = "ml/energy/targets/weak_supervision"
TABLE = "target_weak_labels"
MIGRATION_DATE = "2019-06-01"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Read the coverage, gap and unit inputs
coverage = read_shared_conformed("series_coverage")
gaps = read_shared_conformed("data_gap_period")
units = read_gold("generation_unit", source=SOURCE).filter(
    F.col("registration_date").isNotNull()
)

# COMMAND ----------

# DBTITLE 1,Series labels: reconciliation status against declared gaps
_has_gap = gaps.select("place_key", "measure").distinct().withColumn("_g", F.lit(True))
series = coverage.select("place_key", "measure", "reconciliation_status").join(
    _has_gap, ["place_key", "measure"], "left"
)
lf_reconciliation = series.select(
    F.lit("series").alias("entity_type"),
    F.concat_ws("|", "place_key", "measure").alias("entity_id"),
    F.lit("reconciliation_status").alias("lf_name"),
    F.when(F.col("reconciliation_status").isin("matched", "reported_only"), "gap")
    .otherwise("no_gap")
    .alias("lf_label"),
)
lf_declared = series.select(
    F.lit("series").alias("entity_type"),
    F.concat_ws("|", "place_key", "measure").alias("entity_id"),
    F.lit("declared_gap_present").alias("lf_name"),
    F.when(F.col("_g"), "gap").otherwise("no_gap").alias("lf_label"),
)

# COMMAND ----------

# DBTITLE 1,Unit labels: registration era
lf_era = units.select(
    F.lit("unit").alias("entity_type"),
    F.col("unit_id").alias("entity_id"),
    F.lit("registration_era").alias("lf_name"),
    F.when(
        F.col("registration_date") >= F.lit(MIGRATION_DATE).cast("date"),
        "post_migration",
    )
    .otherwise("pre_migration")
    .alias("lf_label"),
)

# COMMAND ----------

# DBTITLE 1,Stack the labels
out = (
    lf_reconciliation.unionByName(lf_declared)
    .unionByName(lf_era)
    .withColumn("provenance_tier", F.lit("weak"))
)
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Coverage and conflict of the labelling functions
_cov = [
    (r["lf_name"], r["n"])
    for r in out.groupBy("lf_name").agg(F.count("*").alias("n")).collect()
]
_wide = (
    out.filter(F.col("entity_type") == "series")
    .groupBy("entity_id")
    .pivot("lf_name", ["reconciliation_status", "declared_gap_present"])
    .agg(F.first("lf_label"))
)
_both = _wide.filter(
    F.col("reconciliation_status").isNotNull()
    & F.col("declared_gap_present").isNotNull()
)
_conflict = _both.filter(
    F.col("reconciliation_status") != F.col("declared_gap_present")
).count()
_n_both = _both.count()
drop_block = (
    "labelling_function_report",
    markdown_table(
        ["lf_name", "labelled_entities"],
        _cov + [("conflict_rate_series_functions", f"{_conflict}/{_n_both}")],
    ),
)
print(_cov, "conflicts", _conflict, "of", _n_both)

# COMMAND ----------

# DBTITLE 1,Grain and forbidden-column gate
_GRAIN = ["entity_type", "entity_id", "lf_name"]
assert_unique_grain(out, _GRAIN, component=COMPONENT, source=SOURCE, rid=rid)
assert_no_forbidden_columns(out, component=COMPONENT, source=SOURCE, rid=rid)
check(
    COMPONENT,
    SOURCE,
    "non_empty",
    out.limit(1).count() > 0,
    detail="no rows produced; check the input filters",
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write
write_ml(out, TABLE, ecosystem=ECO, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect and export findings
_blocks = inspect_ml_table(
    read_ml(TABLE, ecosystem=ECO), TABLE, ecosystem=ECO, key_cols=_GRAIN
)
write_ml_findings(ECO, "targets__" + TABLE, TABLE, _blocks)

# COMMAND ----------

# DBTITLE 1,Add the drop count to the findings
write_ml_findings(ECO, "targets__" + TABLE + "__drops", TABLE + " drops", [drop_block])
