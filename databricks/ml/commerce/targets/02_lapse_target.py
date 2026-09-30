# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # LAPSE TARGET
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** whether a user returns after the cutoff. REES46: October cohort, any
# MAGIC session in the first 30 days of November. GA4: cohorts at two cutoffs, any
# MAGIC later session up to the end of the data (thin, declared).

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "commerce"
SOURCE = "rees46"
COMPONENT = "ml/commerce/targets/lapse"
TABLE = "target_lapse_rees46"
GA4_TABLE = "target_lapse_ga4"
REES46_CUTOFF = REES46_EVALUATION_START
REES46_LABEL_DAYS = 30
GA4_CUTOFFS = [GA4_LAPSE_TRAIN_CUTOFF, GA4_LAPSE_EVALUATION_CUTOFF]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,REES46 cohort and return label
_s = read_gold("rees46_session", source=SOURCE)
_c = F.lit(REES46_CUTOFF).cast("timestamp")
_end = F.date_add(F.lit(REES46_CUTOFF).cast("date"), REES46_LABEL_DAYS).cast(
    "timestamp"
)
candidates = _s.groupBy("user_id").agg(
    F.max((F.col("session_start_utc") < _c).cast("int")).alias("in_cohort"),
    F.max(
        ((F.col("session_start_utc") >= _c) & (F.col("session_start_utc") < _end)).cast(
            "int"
        )
    ).alias("returned"),
)
out = (
    candidates.filter(F.col("in_cohort") == 1)
    .select(
        "user_id",
        F.lit(REES46_CUTOFF).cast("date").alias("cutoff_date"),
        (F.col("returned") == 1).alias("target_returned"),
        F.lit(REES46_LABEL_DAYS).alias("label_window_days"),
    )
    .withColumn("provenance_tier", F.lit("constructed"))
)
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Record the cohort size
drop_block = (
    "cohort",
    markdown_table(
        ["users_seen", "users_in_cohort"], [(candidates.count(), out.count())]
    ),
)

# COMMAND ----------

# DBTITLE 1,Grain and forbidden-column gate
_GRAIN = ["user_id", "cutoff_date"]
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
    read_ml(TABLE, ecosystem=ECO),
    TABLE,
    ecosystem=ECO,
    key_cols=_GRAIN,
    target_col="target_returned",
)
write_ml_findings(ECO, "targets__" + TABLE, TABLE, _blocks)

# COMMAND ----------

# DBTITLE 1,Add the cohort report to the findings
write_ml_findings(
    ECO, "targets__" + TABLE + "__cohort", TABLE + " cohort", [drop_block]
)

# COMMAND ----------

# DBTITLE 1,GA4 cohorts and return labels
_g = read_gold("ga4_session", source="ga4")
_data_end = _g.agg(F.max("session_start_utc")).first()[0]
_parts = []
for _cut in GA4_CUTOFFS:
    _cc = F.lit(_cut).cast("timestamp")
    _parts.append(
        _g.groupBy("user_pseudo_id")
        .agg(
            F.max((F.col("session_start_utc") < _cc).cast("int")).alias("in_cohort"),
            F.max((F.col("session_start_utc") >= _cc).cast("int")).alias("returned"),
        )
        .filter(F.col("in_cohort") == 1)
        .select(
            F.col("user_pseudo_id").alias("user_id"),
            F.lit(_cut).cast("date").alias("cutoff_date"),
            (F.col("returned") == 1).alias("target_returned"),
            F.datediff(F.lit(_data_end).cast("date"), F.lit(_cut).cast("date")).alias(
                "label_window_days"
            ),
        )
    )
out_ga4 = _parts[0]
for _p in _parts[1:]:
    out_ga4 = out_ga4.unionByName(_p)
out_ga4 = out_ga4.withColumn("provenance_tier", F.lit("constructed"))
out_ga4 = add_ml_provenance(out_ga4, GA4_TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate for GA4
assert_unique_grain(
    out_ga4,
    ["user_id", "cutoff_date"],
    component=COMPONENT + "/ga4",
    source="ga4",
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write GA4
write_ml(
    out_ga4,
    GA4_TABLE,
    ecosystem=ECO,
    source="ga4",
    component=COMPONENT + "/ga4",
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Inspect and export GA4 findings
_blocks = inspect_ml_table(
    read_ml(GA4_TABLE, ecosystem=ECO),
    GA4_TABLE,
    ecosystem=ECO,
    key_cols=["user_id", "cutoff_date"],
    target_col="target_returned",
)
write_ml_findings(ECO, "targets__" + GA4_TABLE, GA4_TABLE, _blocks)
