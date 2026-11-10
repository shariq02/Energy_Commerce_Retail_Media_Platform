# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SILVER -- PLANTS, SAMPLES AND INTERVENTIONS (ENERGY)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the power plant list (register and capacity plan), the combined cycle
# MAGIC plant operating sample, the grid intervention events of the transmission system
# MAGIC operators (redispatch) and the device telemetry snapshot.

# COMMAND ----------

# DBTITLE 1,Semantic structures library
# MAGIC %run ../_semantic_common

# COMMAND ----------

# DBTITLE 1,Imports
from functools import reduce
from operator import or_

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "silver/energy/05_plants_samples_interventions"
FINDINGS = "energy"
RID = run_id()
NO_TEXT = F.lit(None).cast("string")

# Power plant list
PLANT_SOURCE = "power_plant_list"
PLANT_BT = "power_plant_list"
PLAN_BT = "power_plant_capacity_additions"
PLANT_MAPPING = load_mapping(PLANT_SOURCE)
PLANT_NAME_MAP = flatten_business_names(PLANT_MAPPING, PLANT_SOURCE)
LABELS = PLANT_MAPPING["coded_value_labels"]
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

# Combined cycle plant sample
CCPP_SOURCE = "power_plant_ccpp"
CCPP_ROOT = "/Volumes/samples/databricks/datasets/power-plant"
KEEP_FILE = "Sheet1.tsv"
CCPP_FIELDS = {
    "AT": "ambient_temperature_degc",
    "V": "exhaust_vacuum_cm_of_mercury",
    "AP": "ambient_pressure_mbar",
    "RH": "relative_humidity_percent",
    "PE": "net_electrical_output_mw",
}

# Redispatch measures
GRID_SOURCE = "redispatch"
GRID_BT = "redispatch_measures"
DE_TS_FORMATS = ("dd.MM.yyyy HH:mm:ss", "dd.MM.yyyy HH:mm", "dd.MM.yyyy H:mm")
GRID_LABELS = load_mapping(GRID_SOURCE)["coded_value_labels"]
NUMERIC = {
    "MITTLERE_LEISTUNG_MW": "mean_power_mw",
    "MAXIMALE_LEISTUNG_MW": "max_power_mw",
    "GESAMTE_ARBEIT_MWH": "energy_mwh",
}
# Set to "utc" or "europe_berlin_wall_clock" to overrule the cross-tab below.
TIME_BASIS_OVERRIDE = None

# Device telemetry
IOT_SOURCE = "iot"
IOT_ROOT = "/Volumes/samples/databricks/datasets/iot"

# COMMAND ----------

# DBTITLE 1,Session time zone
ensure_utc_session()

# COMMAND ----------

# DBTITLE 1,Transform -- power_plant_register (typing, decode, Bundesland key, carrier check)
plant_bronze = read_bronze(PLANT_BT)
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
plants = apply_renames(plants, PLANT_NAME_MAP)

# Rows with no capacity value are recorded in the quarantine, not dropped.
plants, capacity_q = value_quarantine(
    plants,
    F.col("capacity_net_mw").isNull() & F.col("border_plant_net_capacity_mw").isNull(),
    flag_col="_capacity_all_null",
    rule_id="german_comma_decimal",
    reason="no capacity value parsed on the row",
    field_name="capacity_gross_mw,capacity_net_mw",
    value_col="plant_name",
    sr_id_col="plant_name",
    source_system=PLANT_SOURCE,
    bronze_table=PLANT_BT,
)
write_quarantine(capacity_q, RID)

# Bundesland to AGS (Germany rows).
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

# Carrier cross-check against the MaStR fuel catalog (additive, the decode stays).
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

# Record key and provenance (rows that share a key are numbered, never dropped).
plants = plants.withColumn(
    "_ppl_key",
    F.concat_ws(
        "|",
        F.coalesce(F.col("mastr_unit_id"), F.lit("NA")),
        F.col("plant_name"),
        F.col("commissioning_year").cast("string"),
    ),
)
plants = within_group_ordinal(
    plants,
    ["_ppl_key"],
    [c for c in plants.columns if c not in ("_ppl_key", "_capacity_all_null")],
)
plants = plants.withColumn("_srid", sha_key("_ppl_key", "_source_id_ordinal")).drop(
    "_ppl_key", "_capacity_all_null"
)
plants = add_semantic_provenance(plants, PLANT_SOURCE, PLANT_BT, RID, "_srid")

# COMMAND ----------

# DBTITLE 1,Transform -- power_plant_capacity_plan (section and total flag; title rows quarantined)
plan_bronze = read_bronze(PLAN_BT)
_has_number = reduce(or_, [F.col(c).isNotNull() for c in PLAN_NUM_COLS])
plan, footnote_q = row_quarantine(
    plan_bronze,
    ~(_fold(F.col("energietraeger")).isin(REAL_CARRIERS) | _has_number),
    rule_id="capacity_additions_footnote_rows",
    reason="energietraeger is a section title / legal footnote with no numbers",
    field_name="energietraeger",
    value_col="energietraeger",
    sr_id_col="energietraeger",
    source_system=PLANT_SOURCE,
    bronze_table=PLAN_BT,
)
write_quarantine(footnote_q, RID)
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
plan = add_semantic_provenance(plan, PLANT_SOURCE, PLAN_BT, RID, "_srid")

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- power_plant_register and power_plant_capacity_plan
power_plant_register = write_inspect_export(
    plants,
    "power_plant_register",
    source=PLANT_SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=[
        "mastr_unit_id",
        "plant_name",
        "commissioning_year",
        "_source_id_ordinal",
    ],
    writer=write_semantic,
    extra_checks={
        "bundesland_out_of_set": lambda t: t.filter(
            F.col("_bundesland_out_of_set")
        ).count(),
        "carrier_not_in_mastr_katalog": lambda t: t.filter(
            ~F.col("energy_carrier_mastr_matched")
        ).count(),
    },
)
write_inspect_export(
    plan,
    "power_plant_capacity_plan",
    source=PLANT_SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["capacity_section", "energy_carrier"],
    writer=write_semantic,
)

# COMMAND ----------

# DBTITLE 1,Read Samples -- every combined cycle plant TSV copy, and the copy check
ccpp_raw = (
    spark.read.option("header", True)
    .option("sep", "\t")
    .option("recursiveFileLookup", True)
    .option("pathGlobFilter", "*.tsv")
    .csv(CCPP_ROOT)
    .select(
        *[F.col(src).cast("double").alias(dst) for src, dst in CCPP_FIELDS.items()],
        F.col("_metadata.file_name").alias("_file"),
    )
)
# The files must be copies of one row set (else halt). Decimal sum: a bigint sum
# of hashes overflows under ANSI mode.
_row_hash = F.xxhash64(*CCPP_FIELDS.values()).cast("decimal(38,0)")
file_proof = ccpp_raw.groupBy("_file").agg(
    F.count("*").alias("rows"), F.sum(_row_hash).alias("content_hash_sum")
)
_proof = file_proof.collect()
if len({(r["rows"], r["content_hash_sum"]) for r in _proof}) != 1:
    raise ValueError(f"CCPP files differ, not copies: {_proof}")

# COMMAND ----------

# DBTITLE 1,Transform -- plant_operating_sample (one copy, repeats kept and numbered)
# The source documents 9,568 rows, so exact repeats are kept, not collapsed.
_repeat = Window.partitionBy(*CCPP_FIELDS.values()).orderBy(*CCPP_FIELDS.values())
sample = (
    ccpp_raw.filter(F.col("_file") == KEEP_FILE)
    .withColumn("repeat_index", F.row_number().over(_repeat))
    .withColumn("exact_repeat", F.col("repeat_index") > 1)
    .withColumn("relative_humidity_above_100", F.col("relative_humidity_percent") > 100)
    .withColumn("measurement_basis", F.lit("plant_operating_sample"))
    .withColumn("sample_key", sha_key(*CCPP_FIELDS.values(), "repeat_index"))
    .withColumn("source_record_id", F.col("sample_key"))
)
sample = conform(
    add_semantic_provenance(sample, CCPP_SOURCE, KEEP_FILE, RID),
    PLANT_OPERATING_SAMPLE_COLUMNS,
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- plant_operating_sample
write_inspect_export(
    sample,
    "plant_operating_sample",
    source=CCPP_SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["sample_key"],
    writer=write_semantic,
    extra_checks={
        "rows_equal_documented_9568": lambda t: t.count() == 9568,
        "exact_repeats": lambda t: t.filter(F.col("repeat_index") > 1).count(),
        "relative_humidity_above_100": lambda t: t.filter(
            F.col("relative_humidity_above_100")
        ).count(),
    },
    extra_blocks=[("file copies (rows, content hash)", rows_to_markdown(file_proof))],
)

# COMMAND ----------

# DBTITLE 1,Read Bronze -- redispatch measures, deduplicated, and the time-basis check
grid_bronze = read_bronze(GRID_BT)
_kept, _q = resolve_conflicts(
    grid_bronze, grid_bronze.columns, grid_bronze.columns, bronze_table=GRID_BT
)
write_quarantine(_q.withColumn("source_system", F.lit(GRID_SOURCE)), RID)
grid_reconciliation = reconciliation_stats(grid_bronze, _kept, _q)

# Are the 22:00/23:00 start spikes Berlin midnight in UTC? If the stamps are UTC,
# the 22:00 spike falls in summer time (CEST) and the 23:00 spike in winter time
# (CET). Rule: UTC when CEST share(22) - CEST share(23) >= 0.3.
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

# DBTITLE 1,Transform -- grid_intervention_event (one row per measure)


def _instant(date_col, time_col):
    naive = parse_ts(F.concat_ws(" ", date_col, time_col), DE_TS_FORMATS, "UTC")
    return naive if TIME_BASIS == "utc" else F.to_utc_timestamp(naive, PROJECT_TZ)


def _normalise(col):
    return F.upper(F.trim(F.regexp_replace(col, r"\s+", " ")))


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
        "reason",
        lit_map(GRID_LABELS["grund_der_massnahme"]["map"])[F.col("reason_native")],
    )
    .withColumn("direction_native", F.col("RICHTUNG"))
    .withColumn(
        "direction",
        lit_map(GRID_LABELS["richtung"]["map"])[F.col("direction_native")],
    )
    .withColumn(
        "instructing_transmission_system_operator_native", F.col("ANWEISENDER_UENB")
    )
    .withColumn("instructing_market_area_code", _market[F.trim("ANWEISENDER_UENB")])
    .withColumn(
        "requesting_transmission_system_operator_native", F.col("ANFORDERNDER_UENB")
    )
    .withColumn("affected_asset_text", F.trim("BETROFFENE_ANLAGE"))
    .withColumn("primary_energy_type_native", F.col("PRIMAERENERGIEART"))
    .withColumn(
        "primary_energy_type",
        lit_map(GRID_LABELS["primaerenergieart"]["map"])[F.col("PRIMAERENERGIEART")],
    )
    .withColumn(
        "measurement_basis",
        F.lit("transmission_system_operator_reported_measure_pre_2021"),
    )
    .withColumn("event_key", sha_key(*grid_bronze.columns))
    .withColumn("source_record_id", F.col("event_key"))
)
# One boolean column per market area that requested the measure (the requesting
# operator text can name several areas).
_requested = F.filter(
    F.transform(_requesting, lambda x: _market[x]), lambda x: x.isNotNull()
)
for area in MARKET_AREA_VALUES:
    events = events.withColumn(
        f"requested_by_{area}", F.array_contains(_requested, area)
    )

# Affected unit: exact normalised name match to the plant register (first pass
# only; its match rate decides whether a fuzzy pass is warranted).
_plants = (
    power_plant_register.select("plant_name")
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

# Quality flags.
_residual = F.abs(
    F.col("energy_mwh") - F.col("mean_power_mw") * F.col("duration_hours")
) / F.col("energy_mwh")
events = (
    events.withColumn("negative_duration", F.col("duration_hours") < 0)
    .withColumn(
        "mean_power_above_maximum_power", F.col("mean_power_mw") > F.col("max_power_mw")
    )
    .withColumn(
        "energy_differs_from_power_times_duration",
        (F.col("duration_hours") > 0) & (F.col("energy_mwh") > 0) & (_residual > 0.05),
    )
)
events = add_semantic_provenance(events, GRID_SOURCE, GRID_BT, RID)

# Inverted time windows: the row stays, the quarantine records it.
write_quarantine(
    _q_rows(
        events.filter(F.col("duration_hours") < 0),
        "inverted_measure_window",
        "event_start_utc is after event_end_utc",
        "event_start_utc,event_end_utc",
        "event_start_native",
        "source_record_id",
        GRID_SOURCE,
        GRID_BT,
    ),
    RID,
)
events = conform(events, GRID_INTERVENTION_EVENT_COLUMNS)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- grid_intervention_event
write_inspect_export(
    events,
    "grid_intervention_event",
    source=GRID_SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["event_key"],
    df_before=grid_bronze,
    writer=write_semantic,
    extra_checks={
        "time_basis": TIME_BASIS,
        "zeitzone_von_values": _zones,
        "negative_duration_events": lambda t: t.filter(
            F.col("negative_duration")
        ).count(),
        "undecoded_rows": lambda t: t.filter(
            (F.col("reason_native").isNotNull() & F.col("reason").isNull())
            | (F.col("direction_native").isNotNull() & F.col("direction").isNull())
            | (
                F.col("instructing_transmission_system_operator_native").isNotNull()
                & F.col("instructing_market_area_code").isNull()
            )
        ).count(),
        "affected_unit_match_rate": lambda t: (
            t.filter(
                F.col("affected_unit_match_confidence") == "exact_normalised"
            ).count()
            / max(t.count(), 1)
        ),
    },
    extra_blocks=[
        ("start hour by CEST share (time-basis evidence)", rows_to_markdown(tz_xtab)),
        (
            "Bronze -> exact duplicates collapsed -> conflicts quarantined -> kept",
            dict_to_markdown_row(grid_reconciliation),
        ),
    ],
)

# COMMAND ----------

# DBTITLE 1,Read Samples -- iot devices, deduplicated
iot_raw = (
    spark.read.option("recursiveFileLookup", True)
    .option("pathGlobFilter", "*.json")
    .json(IOT_ROOT)
)
_content = [c for c in iot_raw.columns if c != "device_id"]
_kept_iot, _q_iot = resolve_conflicts(
    iot_raw, ["device_id"], _content, bronze_table="iot_devices"
)
write_quarantine(_q_iot.withColumn("source_system", F.lit(IOT_SOURCE)), RID)
iot_reconciliation = reconciliation_stats(iot_raw, _kept_iot, _q_iot)

# COMMAND ----------

# DBTITLE 1,Transform -- device_telemetry_snapshot (one row per device, epoch milliseconds)
devices = (
    _kept_iot.withColumn("source_device_id", F.col("device_id").cast("string"))
    .withColumn("ip_address", F.col("ip"))
    .withColumn("latitude", F.col("latitude").cast("double"))
    .withColumn("longitude", F.col("longitude").cast("double"))
    .withColumn("country_code", F.col("cca2"))
    .withColumn("country_code_alpha3", F.col("cca3"))
    .withColumn("country_name", F.col("cn"))
    .withColumn("observation_timestamp_native", F.col("timestamp").cast("string"))
    .withColumn(
        "observation_timestamp_utc",
        (F.col("timestamp").cast("double") / 1000).cast("timestamp"),
    )
    .withColumn("temperature_degc", F.col("temp").cast("double"))
    .withColumn("humidity_percent", F.col("humidity").cast("double"))
    .withColumn("co2_level", F.col("c02_level").cast("double"))
    .withColumn("battery_level", F.col("battery_level").cast("double"))
    .withColumn("lcd_label", F.col("lcd"))
    .withColumn("data_origin", F.lit("synthetic_suspected"))
    .withColumn("measurement_basis", F.lit("device_snapshot"))
    .withColumn("device_key", sha_key(F.lit(IOT_SOURCE), "device_id"))
    .withColumn("source_record_id", F.col("device_key"))
)
devices = conform(
    add_semantic_provenance(devices, IOT_SOURCE, "iot_devices", RID),
    DEVICE_TELEMETRY_SNAPSHOT_COLUMNS,
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- device_telemetry_snapshot
write_inspect_export(
    devices,
    "device_telemetry_snapshot",
    source=IOT_SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["device_key"],
    df_before=iot_raw,
    writer=write_semantic,
    extra_blocks=[
        (
            "Bronze -> exact duplicates collapsed -> conflicts quarantined -> kept",
            dict_to_markdown_row(iot_reconciliation),
        )
    ],
)
