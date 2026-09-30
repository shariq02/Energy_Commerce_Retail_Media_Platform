# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # HUB HEIGHT FILL
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** fill missing wind hub heights by the method that scores best when known
# MAGIC heights are masked (group median, nearest neighbours, regression), fitted
# MAGIC on units commissioned in the training calendar.

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
COMPONENT = "ml/energy/features/hub_height_fill"
TABLE = "features_hub_height_fill"
TRAIN_END = SPLIT_CALENDARS["energy_daily"]["train"][1]
FEATURES = [
    "capacity_net_kw",
    "rotor_diameter_m",
    "commissioning_year",
    "is_offshore_int",
    "manufacturer_code",
]
GROUP = ["is_offshore_int", "commissioning_year"]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Wind units to a small pandas frame
_units = wind_units(read_gold("generation_unit", source=SOURCE)).filter(
    F.col("lifecycle_state").isin(*OPERATING_STATES)
    & F.col("commissioning_date").isNotNull()
)
pdf = _units.select(
    "unit_id",
    "hub_height_m",
    "capacity_net_kw",
    "rotor_diameter_m",
    "manufacturer",
    F.year("commissioning_date").alias("commissioning_year"),
    F.to_date("commissioning_date").alias("commissioning_day"),
    F.col("is_offshore").cast("int").alias("is_offshore_int"),
).toPandas()
assert len(pdf) < 200_000, "hub height fill is for the small wind unit table only"
pdf["manufacturer_code"] = pdf["manufacturer"].astype("category").cat.codes

# COMMAND ----------

# DBTITLE 1,Split into the fit population and the units to fill
import pandas as pd

_cut = pd.Timestamp(TRAIN_END).date()
fit_known = pdf[
    pdf["hub_height_m"].notna() & (pdf["commissioning_day"] <= _cut)
].reset_index(drop=True)
to_fill = pdf[pdf["hub_height_m"].isna()].reset_index(drop=True)
print(f"fit population {len(fit_known)} known units; {len(to_fill)} units to fill")

# COMMAND ----------

# DBTITLE 1,Mask-and-score the three methods
scores = static_impute_scores(fit_known, "hub_height_m", FEATURES, GROUP)
best_method = min(scores, key=scores.get)
print(scores, "->", best_method)

# COMMAND ----------

# DBTITLE 1,Fill the missing hub heights with the best method
import numpy as np

filled = pdf.copy()
if len(to_fill):
    pred = static_impute_predict(
        fit_known, to_fill, "hub_height_m", FEATURES, best_method, GROUP
    )
    filled.loc[filled["hub_height_m"].isna(), "hub_height_m"] = np.asarray(pred, float)
filled["hub_height_was_imputed"] = pdf["hub_height_m"].isna()
filled["hub_height_method"] = np.where(
    filled["hub_height_was_imputed"], best_method, "observed"
)
out = spark.createDataFrame(
    filled[["unit_id", "hub_height_m", "hub_height_was_imputed", "hub_height_method"]]
    .rename(columns={"hub_height_m": "hub_height_m_filled"})
    .astype({"unit_id": str})
)
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Record the fitted imputer
record_imputer(
    "zone_weather",
    ECO,
    fold_id=-1,
    rows=[
        (
            "hub_height_m",
            best_method,
            {
                "scores": scores,
                "features": FEATURES,
                "group": GROUP,
                "fit_end": TRAIN_END,
            },
        )
    ],
    fit_rows=len(fit_known),
)

# COMMAND ----------

# DBTITLE 1,Grain and forbidden-column gate
_GRAIN = ["unit_id"]
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
