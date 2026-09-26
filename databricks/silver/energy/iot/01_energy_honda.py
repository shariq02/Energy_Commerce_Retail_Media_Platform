# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- ENERGY (HONDA SITE)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Honda P channels into `electricity_balance` and
# MAGIC `thermal_energy`, W registers into `energy_meter_reading`; native sign,
# MAGIC one row per (site, instant, frequency).

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

# DBTITLE 1,Weather specifications (Honda interval seconds)
# MAGIC %run ../weather/_weather_specs

# COMMAND ----------

# DBTITLE 1,Honda flag helpers
# MAGIC %run ./honda_energy/_honda_energy_common

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "honda_iot"
COMPONENT = "silver/energy/iot/01_energy_honda"
RID = run_id()
FINDINGS = "energy"
SITE_ID = "honda_site"
KEY = ["frequency", "datetime_utc"]
CHANNELS = {
    "electricity": ["total", "PV", "CHP"],
    "heating": ["total", "CHP_heat", "CHP_elec"],
    "cooling": ["total", "cool_elec"],
}
STUCK_LOOKBACK_STEPS_BY_FREQ = {"1h": 9, "15min": 39, "1min": 599}
DUPLICATE_AGREEMENT_THRESHOLD = 0.99
_W = Window.partitionBy("frequency").orderBy(F.col("datetime_utc").cast("timestamp"))

# P channel -> (component_kind, carrier_code) in electricity_balance.
BALANCE = {
    "electricity__total": ("net_total", None),
    "electricity__PV": ("generation", "photovoltaic"),
    "electricity__CHP": ("generation", "combined_heat_and_power"),
    "cooling__cool_elec": ("subsystem_consumption", None),
}
THERMAL = {
    "heating_total_w": "heating__total",
    "heating_combined_heat_and_power_heat_w": "heating__CHP_heat",
    "cooling_total_w": "cooling__total",
}

# COMMAND ----------

# DBTITLE 1,Session time zone
ensure_utc_session()

# COMMAND ----------

# DBTITLE 1,Read Bronze -- honda_iot_{electricity,heating,cooling}
bronze = {t: read_bronze(f"honda_iot_{t}") for t in CHANNELS}

# COMMAND ----------

# DBTITLE 1,Dedupe -- collapse identical rows, quarantine same-key value conflicts
kept, RECONCILIATION = {}, {}
for t, cols in CHANNELS.items():
    bt = f"honda_iot_{t}"
    kept[t], _q = resolve_conflicts(
        bronze[t], ["measurement_type", *KEY], cols, bronze_table=bt
    )
    write_quarantine(_q.withColumn("source_system", F.lit(SOURCE)), RID)
    RECONCILIATION[bt] = reconciliation_stats(bronze[t], kept[t], _q)

# COMMAND ----------

# DBTITLE 1,Helper -- one measurement type across the three tables, prefixed


def _wide(measurement_type: str):
    """(frequency, datetime_utc) rows with `<table>__<channel>` doubles."""
    joined = None
    for t, cols in CHANNELS.items():
        part = (
            kept[t]
            .filter(F.col("measurement_type") == measurement_type)
            .select(*KEY, *[F.col(c).cast("double").alias(f"{t}__{c}") for c in cols])
        )
        joined = part if joined is None else joined.join(part, KEY, "full_outer")
    return joined


# COMMAND ----------

# DBTITLE 1,Transform -- P and W wide frames
p_wide = _wide("p")
w_wide = _wide("w")
W_COLS = [c for c in w_wide.columns if c not in KEY]

# COMMAND ----------

# DBTITLE 1,Check -- heating P CHP_elec still mirrors electricity P CHP (else halt)
_pair = p_wide.filter(
    F.col("electricity__CHP").isNotNull() & F.col("heating__CHP_elec").isNotNull()
)
_agreement = _pair.filter(
    F.col("electricity__CHP") == F.col("heating__CHP_elec")
).count() / max(_pair.count(), 1)
print(f"CHP_elec mirror agreement: {_agreement:.6f}")
if _agreement < DUPLICATE_AGREEMENT_THRESHOLD:
    raise ValueError("heating P CHP_elec no longer mirrors electricity P CHP")

# COMMAND ----------

# DBTITLE 1,Helper -- place/window scaffold, flags to quality_flags


def _scaffold(df, family: str, dataset: str, cols: list, extra_flags=None):
    """Site place/window columns plus quality_flags from the 5-sigma and
    stuck-reading helpers (and any `extra_flags`)."""
    df = add_5sigma_outlier_flags(df, cols)
    df = add_stuck_reading_flags(df, cols, STUCK_LOOKBACK_STEPS_BY_FREQ, _W)
    flags = {
        **{f"{c}:5sigma": F.col(f"_{c}_5sigma_outlier") for c in cols},
        **{f"{c}:stuck": F.col(f"_{c}_stuck_reading_flag") for c in cols},
        **(extra_flags or {}),
    }
    df = df.withColumn("quality_flags", flag_array(flags))
    df = df.drop(*[c for c in df.columns if c.startswith("_")])
    df = (
        df.withColumn("source_location_id", F.lit(SITE_ID))
        .withColumn("location_key", location_key(SOURCE, "source_location_id"))
        .withColumn("observation_timestamp_native", F.col("datetime_utc"))
        .withColumn("time_basis", F.lit("utc"))
        .withColumn(
            "observation_timestamp_utc", F.col("datetime_utc").cast("timestamp")
        )
        .withColumn(
            "interval_seconds",
            lit_map(HONDA_INTERVAL_SECONDS)[F.col("frequency")].cast("int"),
        )
        .withColumn("interval_reference", F.lit("clock"))
        .withColumn("measurement_basis", F.lit("site_meter"))
        .withColumn("sign_convention", F.lit("native_generation_negative"))
        .withColumn("source_record_id", sha_key(F.lit(family), *KEY))
        .withColumn("observation_key", F.col("source_record_id"))
    )
    return add_semantic_provenance(add_project_time(df), SOURCE, dataset, RID)


# COMMAND ----------

# DBTITLE 1,Transform -- electricity_balance rows (P; heating CHP_elec mirror left out)
_components = F.filter(
    F.array(
        *[
            F.when(
                F.col(c).isNotNull(),
                F.struct(
                    F.lit(kind).alias("component_kind"),
                    F.lit(carrier).cast("string").alias("carrier_code"),
                    F.lit(c.replace("__", ".")).alias("native_label"),
                    F.lit(None).cast("double").alias("energy_mwh"),
                    F.col(c).alias("power_w"),
                    F.lit(f"honda_iot_{c.split('__')[0]}.p").alias("source_series"),
                ),
            )
            for c, (kind, carrier) in BALANCE.items()
        ]
    ),
    lambda x: x.isNotNull(),
)
balance = _scaffold(
    p_wide.select(*KEY, *BALANCE).withColumn("components", _components),
    "electricity_balance",
    "honda_iot_p",
    list(BALANCE),
).filter(F.size("components") > 0)

# COMMAND ----------

# DBTITLE 1,Transform -- thermal_energy rows (P)
thermal = p_wide.select(*KEY, *[F.col(src).alias(dst) for dst, src in THERMAL.items()])
thermal = _scaffold(
    any_present(thermal, list(THERMAL)), "thermal_energy", "honda_iot_p", list(THERMAL)
)

# COMMAND ----------

# DBTITLE 1,Transform -- energy_meter_reading rows (W registers + derived increment)
# Registers are monotone in magnitude (generation counts down); a violation
# is a drop in |reading|. Increment = next reading minus this one.
meter = w_wide
_ts = F.col("datetime_utc").cast("timestamp")
_next_gap_s = F.unix_timestamp(F.lead(_ts).over(_W)) - F.unix_timestamp(_ts)
_interval_s = lit_map(HONDA_INTERVAL_SECONDS)[F.col("frequency")]
for ch, (t, native) in HONDA_METER_CHANNELS.items():
    reading = F.col(f"{t}__{native}")
    meter = (
        meter.withColumn(f"{ch}_kwh", reading)
        .withColumn(f"{ch}_increment_kwh", F.lead(reading).over(_W) - reading)
        .withColumn(
            f"_{ch}_increment_gap",
            F.coalesce(
                F.col(f"{ch}_increment_kwh").isNotNull() & (_next_gap_s > _interval_s),
                F.lit(False),
            ),
        )
    )
_meter_cols = [f"{ch}_kwh" for ch in HONDA_METER_CHANNELS]
_monotone = {
    f"{c}:magnitude_decrease": F.coalesce(
        F.abs(F.col(c)) < F.abs(F.lag(F.col(c)).over(_W)), F.lit(False)
    )
    for c in _meter_cols
}
_monotone.update(
    {
        f"{ch}_increment_kwh:spans_gap": F.col(f"_{ch}_increment_gap")
        for ch in HONDA_METER_CHANNELS
    }
)
meter = _scaffold(
    any_present(meter.drop(*W_COLS), _meter_cols),
    "energy_meter_reading",
    "honda_iot_w",
    _meter_cols,
    _monotone,
).withColumn(
    "increment_derivation_rule",
    F.lit("next reading minus this reading, same frequency"),
)

# COMMAND ----------

# DBTITLE 1,Configuration -- family -> frame
HONDA_FAMILIES = {
    "electricity_balance": balance,
    "thermal_energy": thermal,
    "energy_meter_reading": meter,
}

# COMMAND ----------

# DBTITLE 1,Write Silver -- family structures (honda)
for fam, fdf in HONDA_FAMILIES.items():
    write_semantic(
        conform(fdf, ENERGY_FAMILY_COLUMNS[fam]),
        fam,
        source=SOURCE,
        component=COMPONENT,
        rid=RID,
        replace_where=f"source_system = '{SOURCE}'",
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- each family structure (honda)
findings_blocks = {}
for fam in HONDA_FAMILIES:
    written = spark.table(semantic_table(fam)).filter(F.col("source_system") == SOURCE)
    findings_blocks[fam] = inspect_table(
        written,
        fam,
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=ENERGY_GRAIN,
        extra_checks={
            **structure_extra_checks(written),
            "chp_elec_mirror_agreement": _agreement,
        },
    )

# COMMAND ----------

# DBTITLE 1,Export findings -- each family structure (honda)
for fam, blocks in findings_blocks.items():
    write_silver_findings(
        FINDINGS, f"{COMPONENT.split('/')[-1]}__{fam}", f"{fam} -- honda", blocks
    )

# COMMAND ----------

# DBTITLE 1,Export findings -- dedup/conflict reconciliation proof, per table
for bt, stats in RECONCILIATION.items():
    write_silver_findings(
        FINDINGS,
        f"{COMPONENT.split('/')[-1]}__{bt}__reconciliation",
        f"dedup/conflict reconciliation -- {bt}",
        [
            (
                "Bronze -> exact duplicates collapsed -> conflicts quarantined -> kept",
                dict_to_markdown_row(stats),
            )
        ],
    )
