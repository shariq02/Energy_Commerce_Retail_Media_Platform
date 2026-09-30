# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- GRID INTERVENTION EVENT (REDISPATCH)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** one row per redispatch measure with start/end instants,
# MAGIC decoded labels next to the native text, market-area codes for the TSOs
# MAGIC and an exact normalised match of the affected asset to the plant register.

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
SOURCE = "redispatch"
COMPONENT = "silver/energy/grid/05_grid_intervention_redispatch"
RID = run_id()
BT = "redispatch_measures"
FINDINGS = "energy"
TABLE = "grid_intervention_event"
DE_TS_FORMATS = ("dd.MM.yyyy HH:mm:ss", "dd.MM.yyyy HH:mm", "dd.MM.yyyy H:mm")
LABELS = load_mapping(SOURCE)["coded_value_labels"]
NUMERIC = {
    "MITTLERE_LEISTUNG_MW": "mean_power_mw",
    "MAXIMALE_LEISTUNG_MW": "max_power_mw",
    "GESAMTE_ARBEIT_MWH": "energy_mwh",
}
# Set to "utc" or "europe_berlin_wall_clock" to overrule the cross-tab below.
TIME_BASIS_OVERRIDE = None

# COMMAND ----------

# DBTITLE 1,Session time zone
ensure_utc_session()

# COMMAND ----------

# DBTITLE 1,Read Bronze -- redispatch_measures
bronze_df = read_bronze(BT)

# COMMAND ----------

# DBTITLE 1,Dedupe -- collapse identical rows (no natural key)
_kept, _q = resolve_conflicts(
    bronze_df, bronze_df.columns, bronze_df.columns, bronze_table=BT
)
write_quarantine(_q.withColumn("source_system", F.lit(SOURCE)), RID)
RECONCILIATION = reconciliation_stats(bronze_df, _kept, _q)

# COMMAND ----------

# DBTITLE 1,Check -- time basis: are the 22:00/23:00 start spikes Berlin midnight in UTC?
# If the stamps are UTC, the 22:00 spike falls in summer time (CEST) and the
# 23:00 spike in winter time (CET); as Berlin wall clock both hours share the
# same CEST share. Rule: UTC when CEST share(22) - CEST share(23) >= 0.3.
_start_naive = parse_ts(
    F.concat_ws(" ", "BEGINN_DATUM", "BEGINN_UHRZEIT"), DE_TS_FORMATS, "UTC"
)
_offset_s = F.from_utc_timestamp(_start_naive, PROJECT_TZ).cast(
    "long"
) - _start_naive.cast("long")
tz_xtab = (
    _kept.select(
        F.hour(_start_naive).alias("start_hour"),
        (_offset_s == 7200).cast("int").alias("cest"),
    )
    .groupBy("start_hour")
    .agg(F.count("*").alias("rows"), F.avg("cest").alias("cest_share"))
    .orderBy("start_hour")
)
_share = {r["start_hour"]: r["cest_share"] for r in tz_xtab.collect()}
_zones = [r[0] for r in _kept.select("ZEITZONE_VON").distinct().collect()]
TIME_BASIS = TIME_BASIS_OVERRIDE or (
    "utc"
    if _share.get(22, 0) - _share.get(23, 0) >= 0.3
    else "europe_berlin_wall_clock"
)
print(f"ZEITZONE_VON={_zones}  CEST share 22h/23h={_share.get(22)}/{_share.get(23)}")
print(f"time basis -> {TIME_BASIS}")

# COMMAND ----------

# DBTITLE 1,Helper -- native date + clock time to a UTC instant


def _instant(date_col: str, time_col: str):
    naive = parse_ts(F.concat_ws(" ", date_col, time_col), DE_TS_FORMATS, "UTC")
    return naive if TIME_BASIS == "utc" else F.to_utc_timestamp(naive, PROJECT_TZ)


# COMMAND ----------

# DBTITLE 1,Transform -- one event row per measure
_market = lit_map(MARKET_AREA_CODES)
_requesting = F.split(
    F.regexp_replace(F.trim("ANFORDERNDER_UENB"), r"\s*&\s*", "&"), "&"
)
events = _kept
for raw, name in NUMERIC.items():
    events = events.withColumn(
        name, F.regexp_replace(F.trim(raw), ",", ".").cast("double")
    )
events = (
    events.withColumn(
        "event_start_native", F.concat_ws(" ", "BEGINN_DATUM", "BEGINN_UHRZEIT")
    )
    .withColumn("event_end_native", F.concat_ws(" ", "ENDE_DATUM", "ENDE_UHRZEIT"))
    .withColumn("time_basis", F.lit(TIME_BASIS))
    .withColumn("event_start_utc", _instant("BEGINN_DATUM", "BEGINN_UHRZEIT"))
    .withColumn("event_end_utc", _instant("ENDE_DATUM", "ENDE_UHRZEIT"))
    .withColumn(
        "event_start_project", F.from_utc_timestamp("event_start_utc", PROJECT_TZ)
    )
    .withColumn("event_end_project", F.from_utc_timestamp("event_end_utc", PROJECT_TZ))
    .withColumn(
        "duration_hours",
        (F.col("event_end_utc").cast("long") - F.col("event_start_utc").cast("long"))
        / 3600.0,
    )
    .withColumn("reason_native", F.col("GRUND_DER_MASSNAHME"))
    .withColumn(
        "reason", lit_map(LABELS["grund_der_massnahme"]["map"])[F.col("reason_native")]
    )
    .withColumn("direction_native", F.col("RICHTUNG"))
    .withColumn(
        "direction", lit_map(LABELS["richtung"]["map"])[F.col("direction_native")]
    )
    .withColumn(
        "instructing_transmission_system_operator_native", F.col("ANWEISENDER_UENB")
    )
    .withColumn("instructing_market_area_code", _market[F.trim("ANWEISENDER_UENB")])
    .withColumn(
        "requesting_transmission_system_operator_native", F.col("ANFORDERNDER_UENB")
    )
    .withColumn(
        "requesting_market_area_codes",
        F.filter(
            F.transform(_requesting, lambda x: _market[x]), lambda x: x.isNotNull()
        ),
    )
    .withColumn("affected_asset_text", F.trim("BETROFFENE_ANLAGE"))
    .withColumn("primary_energy_type_native", F.col("PRIMAERENERGIEART"))
    .withColumn(
        "primary_energy_type",
        lit_map(LABELS["primaerenergieart"]["map"])[F.col("PRIMAERENERGIEART")],
    )
    .withColumn(
        "measurement_basis",
        F.lit("transmission_system_operator_reported_measure_pre_2021"),
    )
    .withColumn("event_key", sha_key(*bronze_df.columns))
    .withColumn("source_record_id", F.col("event_key"))
)

# COMMAND ----------

# DBTITLE 1,Helper -- normalised name for the exact-match pass


def _normalise(col: str):
    return F.upper(F.trim(F.regexp_replace(col, r"\s+", " ")))


# COMMAND ----------

# DBTITLE 1,Transform -- affected unit: exact normalised name match to the plant register
# First pass only; its match rate decides whether a fuzzy pass is warranted.
_plants = (
    spark.table(semantic_table("power_plant_register"))
    .select("plant_name")
    .dropna()
    .withColumn("_norm_plant", _normalise("plant_name"))
    .groupBy("_norm_plant")
    .agg(F.min("plant_name").alias("plant_name"))
)
events = (
    events.withColumn("_norm_affected", _normalise("affected_asset_text"))
    .join(F.broadcast(_plants), F.col("_norm_affected") == F.col("_norm_plant"), "left")
    .withColumn("affected_unit_match_name", F.col("plant_name"))
    .withColumn(
        "affected_unit_match_confidence",
        F.when(F.col("plant_name").isNotNull(), F.lit("exact_normalised")).otherwise(
            F.lit("unmatched")
        ),
    )
    .drop("_norm_affected", "_norm_plant", "plant_name")
)

# COMMAND ----------

# DBTITLE 1,Transform -- quality flags
_residual = F.abs(
    F.col("energy_mwh") - F.col("mean_power_mw") * F.col("duration_hours")
) / F.col("energy_mwh")
events = events.withColumn(
    "quality_flags",
    flag_array(
        {
            "negative_duration": F.col("duration_hours") < 0,
            "mean_power_above_max": F.col("mean_power_mw") > F.col("max_power_mw"),
            "energy_vs_power_x_duration_gt_5pct": (F.col("duration_hours") > 0)
            & (F.col("energy_mwh") > 0)
            & (_residual > 0.05),
        }
    ),
)
events = add_semantic_provenance(events, SOURCE, BT, RID)

# COMMAND ----------

# DBTITLE 1,Write Quarantine -- inverted time windows (row kept, flagged)
write_quarantine(
    _q_rows(
        events.filter(F.col("duration_hours") < 0),
        "inverted_measure_window",
        "event_start_utc is after event_end_utc",
        "event_start_utc,event_end_utc",
        "event_start_native",
        "source_record_id",
        SOURCE,
        BT,
    ),
    RID,
)

# COMMAND ----------

# DBTITLE 1,Write Silver -- grid_intervention_event
write_semantic(
    conform(events, GRID_INTERVENTION_EVENT_COLUMNS),
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    replace_where=f"source_system = '{SOURCE}'",
)

# COMMAND ----------

# DBTITLE 1,Inspect -- grid_intervention_event
written = spark.table(semantic_table(TABLE)).filter(F.col("source_system") == SOURCE)
findings_blocks = inspect_table(
    written,
    TABLE,
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["event_key"],
    df_before=bronze_df,
    extra_checks={
        "time_basis": TIME_BASIS,
        "zeitzone_von_values": _zones,
        "flag_counts": {
            r["f"]: r["count"]
            for r in written.select(F.explode("quality_flags").alias("f"))
            .groupBy("f")
            .count()
            .collect()
        },
        "undecoded_rows": written.filter(
            (F.col("reason_native").isNotNull() & F.col("reason").isNull())
            | (F.col("direction_native").isNotNull() & F.col("direction").isNull())
            | (
                F.col("instructing_transmission_system_operator_native").isNotNull()
                & F.col("instructing_market_area_code").isNull()
            )
        ).count(),
        "affected_unit_match_rate": written.filter(
            F.col("affected_unit_match_confidence") == "exact_normalised"
        ).count()
        / max(written.count(), 1),
    },
)

# COMMAND ----------

# DBTITLE 1,Export findings -- grid_intervention_event
write_silver_findings(
    FINDINGS,
    f"{COMPONENT.split('/')[-1]}__{TABLE}",
    TABLE,
    [
        *findings_blocks,
        ("start hour by CEST share (time-basis evidence)", rows_to_markdown(tz_xtab)),
        (
            "Bronze -> exact duplicates collapsed -> conflicts quarantined -> kept",
            dict_to_markdown_row(RECONCILIATION),
        ),
    ],
)
