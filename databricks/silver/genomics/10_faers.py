# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- FAERS (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** FAERS case versions and their drugs.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Genomics library
# MAGIC %run ../_genomics_common

# COMMAND ----------

# DBTITLE 1,Entity helpers
# MAGIC %run ../_genomics_entities

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql import functions as F

SOURCE = "genomics"
COMPONENT = "silver/genomics/10_faers"
RID = run_id()
FINDINGS = "genomics"
RULES = load_genomics_rules()

# COMMAND ----------

# DBTITLE 1,Build -- faers_case
# -- Transform -- arrays per case version
DEMO = "faers.demo"

text = text_for(DEMO)

reactions = (
    read_genomics_bronze("faers_reac")
    .groupBy("quarter", "primaryid")
    .agg(
        F.sort_array(
            F.collect_set(
                F.struct(
                    source_text("pt", "faers.reac").alias("reaction"),
                    source_text("drug_rec_act", "faers.reac").alias(
                        "drug_recurrence_action"
                    ),
                )
            )
        ).alias("reactions")
    )
)
outcomes = (
    read_genomics_bronze("faers_outc")
    .groupBy("quarter", "primaryid")
    .agg(
        F.sort_array(F.collect_set(source_text("outc_cod", "faers.outc"))).alias(
            "outcomes"
        )
    )
)
report_sources = (
    read_genomics_bronze("faers_rpsr")
    .groupBy("quarter", "primaryid")
    .agg(
        F.sort_array(F.collect_set(source_text("rpsr_cod", "faers.rpsr"))).alias(
            "report_sources"
        )
    )
)

# -- Transform -- demographics, dates and provenance
demo = read_genomics_bronze("faers_demo")
case = (
    demo.join(reactions, ["quarter", "primaryid"], "left")
    .join(outcomes, ["quarter", "primaryid"], "left")
    .join(report_sources, ["quarter", "primaryid"], "left")
    .select(
        "quarter",
        F.col("primaryid").alias("primary_id"),
        text("caseid").alias("case_id"),
        text("caseversion").alias("case_version"),
        text("i_f_code").alias("initial_or_followup"),
        text("event_dt").alias("event_date_raw"),
        text("mfr_dt").alias("manufacturer_date_raw"),
        text("init_fda_dt").alias("initial_fda_date_raw"),
        text("fda_dt").alias("fda_date_raw"),
        text("rept_dt").alias("report_date_raw"),
        faers_date("event_dt").alias("event_date"),
        faers_date("mfr_dt").alias("manufacturer_date"),
        faers_date("init_fda_dt").alias("initial_fda_date"),
        faers_date("fda_dt").alias("fda_date"),
        faers_date("rept_dt").alias("report_date"),
        text("rept_cod").alias("report_code"),
        text("mfr_num").alias("manufacturer_number"),
        text("mfr_sndr").alias("manufacturer_sender"),
        typed_number("age", "double", RULES, f"{DEMO}.age").alias("age"),
        text("age_cod").alias("age_unit"),
        text("age_grp").alias("age_group"),
        text("sex").alias("sex"),
        text("e_sub").alias("electronic_submission"),
        typed_number("wt", "double", RULES, f"{DEMO}.wt").alias("weight"),
        text("wt_cod").alias("weight_unit"),
        text("occp_cod").alias("occupation_code"),
        text("reporter_country").alias("reporter_country"),
        text("occr_country").alias("occurrence_country"),
        "reactions",
        "outcomes",
        "report_sources",
    )
    .distinct()
    .withColumn("_srid", sha_key("quarter", "primary_id"))
)
case = conform_genomics(add_genomics_provenance(case, "_srid", RID), "faers_case")

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- faers_case
write_silver(case, "faers_case", source=SOURCE, component=COMPONENT, rid=RID)

written = spark.table(genomics_table("faers_case"))
blocks = inspect_table(
    written,
    "faers_case",
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["quarter", "primary_id"],
    extra_checks={
        "quarters": written.select("quarter").distinct().count(),
        "dates_not_parsed": written.filter(
            F.col("event_date_raw").isNotNull() & F.col("event_date").isNull()
        ).count(),
        "case_ids_with_several_versions": written.groupBy("case_id")
        .count()
        .filter(F.col("count") > 1)
        .count(),
        **{
            f"{column}_counts": str(
                {
                    r[column]: r["count"]
                    for r in written.groupBy(column).count().collect()
                }
            )
            for column in written.columns
            if column.endswith("_link_method")
        },
    },
)

write_silver_findings(
    FINDINGS, f"{COMPONENT.split('/')[-1]}__faers_case", "faers_case", blocks
)

# COMMAND ----------

# DBTITLE 1,Build -- faers_case_drug
# -- Read Silver -- drug ids and names
xwalk = read_silver("entity_id_crosswalk")
names = read_silver("entity_name")
drug_map = entity_id_map(xwalk, "drug").unionByName(
    term_map(
        names,
        "drug",
        ("name", "generic_name", "trade_name", "synonym", "brand_mixture"),
        "drug_name",
    )
)

# -- Transform -- indications and therapies per drug sequence
DRUG = "faers.drug"

text = text_for(DRUG)

indications = (
    read_genomics_bronze("faers_indi")
    .groupBy("quarter", "primaryid", F.col("indi_drug_seq").alias("drug_seq"))
    .agg(
        F.sort_array(F.collect_set(source_text("indi_pt", "faers.indi"))).alias(
            "indications"
        )
    )
)
therapies = (
    read_genomics_bronze("faers_ther")
    .groupBy("quarter", "primaryid", F.col("dsg_drug_seq").alias("drug_seq"))
    .agg(
        F.sort_array(
            F.collect_set(
                F.struct(
                    source_text("start_dt", "faers.ther").alias("start_dt"),
                    source_text("end_dt", "faers.ther").alias("end_dt"),
                    source_text("dur", "faers.ther").alias("duration"),
                    source_text("dur_cod", "faers.ther").alias("duration_unit"),
                )
            )
        ).alias("therapies")
    )
)

# -- Transform -- drug records, keys and provenance
drug = (
    read_genomics_bronze("faers_drug")
    .join(indications, ["quarter", "primaryid", "drug_seq"], "left")
    .join(therapies, ["quarter", "primaryid", "drug_seq"], "left")
)
drug = link_entities(
    drug,
    [
        (F.upper(F.trim(text("drugname"))), "drug_name"),
        (F.upper(F.trim(text("prod_ai"))), "drug_name"),
    ],
    drug_map,
    "drug_key",
    "drug_link_method",
)
case_drug = (
    drug.select(
        "quarter",
        F.col("primaryid").alias("primary_id"),
        text("caseid").alias("case_id"),
        F.col("drug_seq").alias("drug_sequence"),
        text("role_cod").alias("role_code"),
        text("drugname").alias("drug_name"),
        text("prod_ai").alias("active_ingredient"),
        text("val_vbm").alias("drug_name_validation_code"),
        text("route").alias("route"),
        text("dose_vbm").alias("dose_verbatim_text"),
        typed_number("cum_dose_chr", "double", RULES, f"{DRUG}.cum_dose_chr").alias(
            "cumulative_dose"
        ),
        text("cum_dose_unit").alias("cumulative_dose_unit"),
        text("dechal").alias("dechallenge"),
        text("rechal").alias("rechallenge"),
        text("lot_num").alias("lot_number"),
        text("exp_dt").alias("expiry_date_raw"),
        text("nda_num").alias("new_drug_application_number"),
        typed_number("dose_amt", "double", RULES, f"{DRUG}.dose_amt").alias(
            "dose_amount"
        ),
        text("dose_unit").alias("dose_unit"),
        text("dose_form").alias("dose_form"),
        text("dose_freq").alias("dose_frequency"),
        "drug_key",
        "drug_link_method",
        "indications",
        "therapies",
    )
    .distinct()
    .withColumn("_srid", sha_key("quarter", "primary_id", "drug_sequence", "drug_name"))
)
case_drug = conform_genomics(
    add_genomics_provenance(case_drug, "_srid", RID), "faers_case_drug"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- faers_case_drug
write_silver(case_drug, "faers_case_drug", source=SOURCE, component=COMPONENT, rid=RID)

written = spark.table(genomics_table("faers_case_drug"))
blocks = inspect_table(
    written,
    "faers_case_drug",
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["quarter", "primary_id", "drug_sequence"],
    extra_checks={
        "drugs_without_a_drug_key": written.filter(F.col("drug_key").isNull()).count(),
        "roles": str(
            {
                r["role_code"]: r["count"]
                for r in written.groupBy("role_code").count().collect()
            }
        ),
        **{
            f"{column}_counts": str(
                {
                    r[column]: r["count"]
                    for r in written.groupBy(column).count().collect()
                }
            )
            for column in written.columns
            if column.endswith("_link_method")
        },
    },
)

write_silver_findings(
    FINDINGS,
    f"{COMPONENT.split('/')[-1]}__faers_case_drug",
    "faers_case_drug",
    blocks,
)
