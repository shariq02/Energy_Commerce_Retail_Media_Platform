# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # CALENDAR AND HOLIDAY FEATURES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** market area by local date: weekday, month, rule-derived national holiday,
# MAGIC bridge day and local day length.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "smard"
COMPONENT = "ml/energy/features/calendar_holiday"
TABLE = "features_calendar_market_area"
DATE_FROM = "2013-01-01"
DATE_TO = "2027-12-31"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Session time zone
set_utc_session()

# COMMAND ----------

# DBTITLE 1,Date spine
dates = spark.range(1).select(
    F.explode(
        F.sequence(F.lit(DATE_FROM).cast("date"), F.lit(DATE_TO).cast("date"))
    ).alias("local_date")
)

# COMMAND ----------

# DBTITLE 1,Market areas and their holiday country
areas = spark.createDataFrame(
    list(MARKET_AREA_COUNTRY.items()), "market_area_code string, holiday_country string"
)

# COMMAND ----------

# DBTITLE 1,Rule-based holiday dates
_rows = [
    (country, d)
    for country in HOLIDAY_RULES
    for year in range(int(DATE_FROM[:4]), int(DATE_TO[:4]) + 1)
    for d in holiday_dates(country, year)
]
holidays = spark.createDataFrame(_rows, "holiday_country string, holiday_date date")

# COMMAND ----------

# DBTITLE 1,Build the calendar features
base = dates.crossJoin(areas).join(
    holidays.withColumnRenamed("holiday_date", "local_date").withColumn(
        "is_holiday", F.lit(True)
    ),
    ["holiday_country", "local_date"],
    "left",
)
base = base.withColumn("is_holiday", F.coalesce(F.col("is_holiday"), F.lit(False)))
w = Window.partitionBy("market_area_code").orderBy("local_date")
out = (
    base.withColumn("day_of_week", F.dayofweek("local_date"))
    .withColumn("is_weekend", F.col("day_of_week").isin(1, 7))
    .withColumn("month", F.month("local_date"))
    .withColumn("day_of_year", F.dayofyear("local_date"))
    .withColumn("iso_week", F.weekofyear("local_date"))
    .withColumn("year", F.year("local_date"))
    .withColumn("holiday_next_day", F.lead("is_holiday", 1).over(w))
    .withColumn("holiday_previous_day", F.lag("is_holiday", 1).over(w))
    .withColumn(
        "is_bridge_day",
        (
            (F.col("day_of_week") == 2)
            & F.coalesce(F.col("holiday_next_day"), F.lit(False))
        )
        | (
            (F.col("day_of_week") == 6)
            & F.coalesce(F.col("holiday_previous_day"), F.lit(False))
        ),
    )
    .withColumn("local_day_hours", local_day_hours("local_date"))
    .withColumn("as_of_ts", local_day_start_utc("local_date"))
    .drop("holiday_next_day", "holiday_previous_day")
)
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Grain and forbidden-column gate
_GRAIN = ["market_area_code", "local_date"]
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
write_ml_findings(ECO, "features__" + TABLE, TABLE, _blocks)
