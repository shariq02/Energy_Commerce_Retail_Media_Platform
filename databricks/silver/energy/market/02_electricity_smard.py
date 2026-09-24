# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- ELECTRICITY (SMARD)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** SMARD series into `electricity_balance`, `electricity_price`
# MAGIC and `electricity_generation_forecast`, one row per (market area, window).

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
SOURCE = "smard"
COMPONENT = "silver/energy/market/02_electricity_smard"
RID = run_id()
BT = "smard_energy_timeseries"
FINDINGS = "energy"
KEY = ["metric", "filter_id", "region", "resolution", "timestamp_utc"]

PRICE_METRIC = "day_ahead_prices"
FORECAST_PREFIX = "forecast_generation_"
DISPUTED_METRIC = "forecast_generation_photovoltaic"
SEMANTIC_ISSUE_REF = "smard.quality_rules.forecast_pv_sign_mirror"

# metric -> (component_kind, carrier_code); values are MWh per window.
BALANCE = {
    **{
        f"generation_{c}": ("generation", c)
        for c in (
            "biomass",
            "hard_coal",
            "hydro",
            "lignite",
            "natural_gas",
            "nuclear",
            "offshore_wind",
            "onshore_wind",
            "other_conventional",
            "other_renewable",
            "photovoltaic",
            "pumped_storage",
        )
    },
    "total_power_consumption": ("consumption", None),
    "pumped_storage_consumption": ("pumped_storage_consumption", "pumped_storage"),
    "residual_load": ("residual_load", None),
}

# COMMAND ----------

# DBTITLE 1,Session time zone
ensure_utc_session()

# COMMAND ----------

# DBTITLE 1,Read Bronze -- smard_energy_timeseries
bronze_df = read_bronze(BT)

# COMMAND ----------

# DBTITLE 1,Dedupe -- collapse identical rows, quarantine same-key value conflicts
_kept, _q = resolve_conflicts(bronze_df, KEY, ["value"], bronze_table=BT)
write_quarantine(_q.withColumn("source_system", F.lit(SOURCE)), RID)
RECONCILIATION = reconciliation_stats(bronze_df, _kept, _q)

# COMMAND ----------

# DBTITLE 1,Transform -- typed long rows with series id and 5-sigma flag
# 5-sigma per series: day and quarter-hour differ ~96x in scale.
_series = F.concat_ws("|", "metric", "filter_id", "region", "resolution")
long_df = (
    _kept.withColumn("value", F.col("value").cast("double"))
    .filter(F.col("value").isNotNull())
    .withColumn("source_series", _series)
)
_stats = long_df.groupBy("source_series").agg(
    F.mean("value").alias("_mean"), F.stddev("value").alias("_sd")
)
_outlier = F.abs(F.col("value") - F.col("_mean")) > 5 * F.col("_sd")
long_df = (
    long_df.join(F.broadcast(_stats), "source_series")
    .withColumn("_outlier", F.coalesce(_outlier, F.lit(False)))
    .drop("_mean", "_sd")
)
# One "<metric>:5sigma" entry per flagged series; collect_list drops NULLs.
_FLAG = F.when(F.col("_outlier"), F.concat(F.col("metric"), F.lit(":5sigma")))

# COMMAND ----------

# DBTITLE 1,Helper -- place/window scaffold for one SMARD row


def _scaffold(df, family: str):
    """region/resolution/timestamp_utc -> place and window columns.
    A day window is the Europe/Berlin local day (23/24/25 h)."""
    utc = F.col("timestamp_utc").cast("timestamp")
    is_day = F.col("resolution") == "day"
    project = F.from_utc_timestamp(utc, PROJECT_TZ)
    next_day_utc = F.to_utc_timestamp(
        F.date_add(F.to_date(project), 1).cast("timestamp"), PROJECT_TZ
    )
    return (
        df.withColumn("source_location_id", F.col("region"))
        .withColumn("market_area_code", lit_map(MARKET_AREA_CODES)[F.col("region")])
        .withColumn("observation_ts_native", F.col("timestamp_utc"))
        .withColumn("time_basis", F.lit("utc"))
        .withColumn("observation_ts_utc", utc)
        .withColumn("observation_ts_project", project)
        .withColumn("local_date", F.to_date(project))
        .withColumn(
            "interval_seconds",
            F.when(is_day, next_day_utc.cast("long") - utc.cast("long"))
            .otherwise(900)
            .cast("int"),
        )
        .withColumn(
            "interval_reference",
            F.when(is_day, F.lit("local_day_europe_berlin")).otherwise(F.lit("clock")),
        )
        .withColumn("measurement_basis", F.lit("market_statistic"))
        .withColumn("sign_convention", F.lit("native_as_published"))
        .withColumn(
            "source_record_id",
            sha_key(F.lit(family), "region", "resolution", "timestamp_utc"),
        )
        .withColumn("observation_key", F.col("source_record_id"))
    )


# COMMAND ----------

# DBTITLE 1,Transform -- electricity_balance rows
_kind = lit_map({m: k for m, (k, _) in BALANCE.items()})
_carrier = lit_map({m: c for m, (_, c) in BALANCE.items() if c})
balance = (
    long_df.filter(F.col("metric").isin(list(BALANCE)))
    .groupBy("region", "resolution", "timestamp_utc")
    .agg(
        F.collect_list(
            F.struct(
                _kind[F.col("metric")].alias("component_kind"),
                _carrier[F.col("metric")].alias("carrier_code"),
                F.col("metric").alias("native_label"),
                F.col("value").alias("energy_mwh"),
                F.lit(None).cast("double").alias("power_w"),
                F.col("source_series").alias("source_series"),
            )
        ).alias("components"),
        F.collect_list(_FLAG).alias("quality_flags"),
    )
)
balance = add_semantic_provenance(
    _scaffold(balance, "electricity_balance"), SOURCE, BT, RID
)

# COMMAND ----------

# DBTITLE 1,Transform -- electricity_price rows
price = long_df.filter(F.col("metric") == PRICE_METRIC).select(
    "region",
    "resolution",
    "timestamp_utc",
    F.col("value").alias("price_eur_per_mwh"),
    F.when(F.col("_outlier"), F.array(F.lit(f"{PRICE_METRIC}:5sigma")))
    .otherwise(F.array().cast("array<string>"))
    .alias("quality_flags"),
)
price = add_semantic_provenance(_scaffold(price, "electricity_price"), SOURCE, BT, RID)

# COMMAND ----------

# DBTITLE 1,Transform -- electricity_generation_forecast rows (disputed PV as sourced)
_disputed = F.col("metric") == DISPUTED_METRIC
_scope = F.regexp_replace("metric", f"^{FORECAST_PREFIX}", "")
_status = F.when(_disputed, F.lit("disputed")).otherwise(F.lit("confirmed"))
forecast = (
    long_df.filter(F.col("metric").startswith(FORECAST_PREFIX))
    .groupBy("region", "resolution", "timestamp_utc")
    .agg(
        F.collect_list(
            F.struct(
                _scope.alias("forecast_scope"),
                F.col("metric").alias("native_label"),
                F.col("value").alias("energy_mwh"),
                _status.alias("semantic_status"),
                F.when(_disputed, F.lit(SEMANTIC_ISSUE_REF)).alias(
                    "semantic_issue_ref"
                ),
                F.col("source_series").alias("source_series"),
            )
        ).alias("components"),
        F.collect_list(_FLAG).alias("quality_flags"),
    )
)
forecast = add_semantic_provenance(
    _scaffold(forecast, "electricity_generation_forecast"), SOURCE, BT, RID
)

# COMMAND ----------

# DBTITLE 1,Configuration -- family -> frame
SMARD_FAMILIES = {
    "electricity_balance": balance,
    "electricity_price": price,
    "electricity_generation_forecast": forecast,
}

# COMMAND ----------

# DBTITLE 1,Write Silver -- family structures (smard)
for fam, fdf in SMARD_FAMILIES.items():
    write_semantic(
        conform(fdf, ENERGY_FAMILY_COLUMNS[fam]),
        fam,
        source=SOURCE,
        component=COMPONENT,
        rid=RID,
        replace_where=f"source_system = '{SOURCE}'",
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- each family structure (smard)
findings_blocks = {}
for fam in SMARD_FAMILIES:
    written = spark.table(semantic_table(fam)).filter(F.col("source_system") == SOURCE)
    findings_blocks[fam] = inspect_table(
        written,
        fam,
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=ENERGY_GRAIN,
        extra_checks=structure_extra_checks(written),
    )

# COMMAND ----------

# DBTITLE 1,Export findings -- each family structure (smard)
for fam, blocks in findings_blocks.items():
    write_silver_findings(
        FINDINGS, f"{COMPONENT.split('/')[-1]}__{fam}", f"{fam} -- smard", blocks
    )

# COMMAND ----------

# DBTITLE 1,Export findings -- dedup/conflict reconciliation proof
write_silver_findings(
    FINDINGS,
    f"{COMPONENT.split('/')[-1]}__reconciliation",
    f"dedup/conflict reconciliation -- {BT}",
    [
        (
            "Bronze -> exact duplicates collapsed -> conflicts quarantined -> kept",
            dict_to_markdown_row(RECONCILIATION),
        )
    ],
)
