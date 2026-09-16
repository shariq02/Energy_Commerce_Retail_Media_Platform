# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- HONDA ENERGY (ELECTRICITY / HEATING / COOLING)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the six Honda IoT power (`_p`) and energy (`_w`) Bronze
# MAGIC tables into source-scoped Silver at their (frequency, datetime_utc) grain.
# MAGIC The channel columns keep their source names and their sign -- a negative
# MAGIC value is on-site generation / export, not an error.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Imports + config
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE = "honda_iot"
COMPONENT = "silver/energy/iot/01_honda_energy"
RID = run_id()

# Bronze table -> Silver table (the `_iot` infix is dropped).
TABLE_MAP = {
    "honda_iot_electricity_p": "honda_electricity_p",
    "honda_iot_electricity_w": "honda_electricity_w",
    "honda_iot_heating_p": "honda_heating_p",
    "honda_iot_heating_w": "honda_heating_w",
    "honda_iot_cooling_p": "honda_cooling_p",
    "honda_iot_cooling_w": "honda_cooling_w",
}

# Owner decision: drop heating_p.CHP_elec, no alias -- electricity_p.CHP is
# canonical. Pre-drop safeguard below re-checks the duplication first.
_DUPLICATE_AGREEMENT_THRESHOLD = 0.99

# Stuck-reading lookback per frequency, scaled to the same ~10-hour window
# profiling measured at 1h (9 steps back = 10 readings = 10h).
STUCK_LOOKBACK_STEPS_BY_FREQ = {"1h": 9, "15min": 39, "1min": 599}

# COMMAND ----------

# DBTITLE 1,One Honda energy table -> Silver


def process(bt: str, silver_table: str) -> None:
    df = read_bronze(bt).withColumn(
        "datetime_utc", F.col("datetime_utc").cast("timestamp")
    )
    for c in df.columns:
        if c not in ("frequency", "datetime_utc"):
            df = df.withColumn(c, F.col(c).cast("double"))

    value_cols = [c for c in df.columns if c not in ("frequency", "datetime_utc")]

    # Pre-drop duplication safeguard (heating_p only). electricity_p is
    # processed first (TABLE_MAP order), so its Silver output already exists
    # by the time heating_p runs.
    _dup_agreement = None
    if bt == "honda_iot_heating_p" and "CHP_elec" in df.columns:
        try:
            _elec = read_silver("honda_electricity_p").select(
                "frequency", "datetime_utc", F.col("CHP").alias("_elec_chp")
            )
            _joined = df.join(_elec, ["frequency", "datetime_utc"], "inner")
            _total = _joined.count()
            _agree = _joined.filter(F.col("CHP_elec") == F.col("_elec_chp")).count()
            _dup_agreement = (_agree / _total) if _total else None
        except Exception as exc:
            print(f"SKIP pre-drop safeguard (electricity_p not yet written?): {exc}")
        if (
            _dup_agreement is not None
            and _dup_agreement >= _DUPLICATE_AGREEMENT_THRESHOLD
        ):
            df = df.drop("CHP_elec")
            value_cols = [c for c in value_cols if c != "CHP_elec"]
            print(
                f"dropped heating_p.CHP_elec -- agreement with "
                f"electricity_p.CHP = {_dup_agreement:.4f} (>= "
                f"{_DUPLICATE_AGREEMENT_THRESHOLD})"
            )
        elif _dup_agreement is not None:
            print(
                f"REOPENED: heating_p.CHP_elec vs electricity_p.CHP agreement = "
                f"{_dup_agreement:.4f} (< {_DUPLICATE_AGREEMENT_THRESHOLD}) -- "
                "keeping the column, not dropping. Investigate before this "
                "decision is reapplied."
            )

    # 5-sigma outlier flag per value column, computed from that column's own
    # mean/sd (per-source profiling data-quality section).
    if value_cols:
        stat_exprs = []
        for c in value_cols:
            stat_exprs += [F.mean(c).alias(f"{c}__mean"), F.stddev(c).alias(f"{c}__sd")]
        _stats = df.agg(*stat_exprs).first()
        for c in value_cols:
            mean_c, sd_c = _stats[f"{c}__mean"], _stats[f"{c}__sd"]
            df = df.withColumn(
                f"_{c}_5sigma_outlier",
                F.when(
                    F.lit(sd_c).isNotNull() & (F.lit(sd_c) > 0) & F.col(c).isNotNull(),
                    F.abs(F.col(c) - F.lit(mean_c)) > (F.lit(5.0) * F.lit(sd_c)),
                ).otherwise(F.lit(False)),
            )

    # Stuck-reading flag: flat for a ~10-hour window, matching
    # profiling's "value == value 1 and 9 steps back" definition AT 1h
    # (9 steps back = 10 readings = 10h). A fixed 9-step lookback applied
    # uniformly across frequencies previously inflated the 1min/15min result
    # (10 minutes/150 minutes flat is a much weaker signal than 10 hours) --
    # the lookback now scales per frequency to the same ~10h window.
    w = Window.partitionBy("frequency").orderBy("datetime_utc")
    for c in value_cols:
        stuck_expr = F.lit(False)
        for freq, lag_n in STUCK_LOOKBACK_STEPS_BY_FREQ.items():
            lag1 = F.lag(F.col(c), 1).over(w)
            lagN = F.lag(F.col(c), lag_n).over(w)
            cond = (
                (F.col("frequency") == freq)
                & F.col(c).isNotNull()
                & (F.col(c) == lag1)
                & (F.col(c) == lagN)
            )
            stuck_expr = F.when(cond, F.lit(True)).otherwise(stuck_expr)
        df = df.withColumn(f"_{c}_stuck_reading_flag", stuck_expr)

    # Meter monotonicity-violation flag, _w (cumulative-meter) tables
    # only (honda_iot.md S01 Temporal Consistency). Row-level: value decreased
    # vs the immediately preceding reading in the same frequency partition.
    if silver_table.endswith("_w"):
        for c in value_cols:
            prev = F.lag(F.col(c), 1).over(w)
            df = df.withColumn(
                f"_{c}_meter_monotonicity_violated",
                F.col(c).isNotNull() & prev.isNotNull() & (F.col(c) < prev),
            )

    df = df.withColumn("_srid", sha_key(F.lit(bt), "frequency", "datetime_utc"))
    df = add_provenance(df, SOURCE, "_srid", RID)
    write_silver(df, silver_table, source=SOURCE, component=COMPONENT, rid=RID)
    _findings_blocks = inspect_table(
        df,
        silver_table,
        source=SOURCE,
        component=COMPONENT,
        rid=RID,
        key_cols=["frequency", "datetime_utc"],
        extra_checks={"chp_elec_duplicate_agreement": _dup_agreement}
        if _dup_agreement is not None
        else None,
    )
    write_silver_findings(
        SOURCE,
        f"{COMPONENT.split('/')[-1]}__{silver_table}",
        silver_table,
        _findings_blocks,
    )


for _bt, _st in TABLE_MAP.items():
    process(_bt, _st)
