# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # CREATE ENERGY + WEATHER DEEPENING BRONZE UPLOAD-UNIT VOLUMES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Create the Bronze upload-unit Volumes needed for this
# MAGIC acquisition wave. DWD and SMARD's prior Volumes were dropped by their
# MAGIC Bronze load notebooks after the first-wave load passed, so those are
# MAGIC re-created here alongside the three net-new sources (MaStR,
# MAGIC Kraftwerksliste, Redispatch).
# MAGIC
# MAGIC Structure only, no Bronze table. Safe to run repeatedly -- every
# MAGIC statement uses `IF NOT EXISTS`, never drops a Volume.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

CATALOG = "energy_commerce_retail_media"
VOLUME_SCHEMA = "bronze"

# MaStR splits analytical (entity/relationship tables) from reference
# (small code-list tables); DWD/SMARD are re-created, not new.
VOLUME_UNITS = [
    "dwd_analytical",
    "dwd_metadata",
    "smard_analytical",
    "mastr_analytical",
    "mastr_reference",
    "power_plant_list_analytical",
    "redispatch_analytical",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Volumes

# COMMAND ----------

# DBTITLE 1,Create Bronze upload-unit volumes
for volume in VOLUME_UNITS:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{VOLUME_SCHEMA}.{volume}")
    print(f"OK  volume ready: /Volumes/{CATALOG}/{VOLUME_SCHEMA}/{volume}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verify Volumes

# COMMAND ----------

# DBTITLE 1,Verify volumes present
found_volumes = {
    row.volume_name
    for row in spark.sql(f"SHOW VOLUMES IN {CATALOG}.{VOLUME_SCHEMA}").collect()
}
missing_volumes = [v for v in VOLUME_UNITS if v not in found_volumes]

print(f"Volumes found in {CATALOG}.{VOLUME_SCHEMA}: {sorted(found_volumes)}")

if missing_volumes:
    raise RuntimeError(f"FAIL  missing volumes after creation: {missing_volumes}")
print(f"OK  all {len(VOLUME_UNITS)} required volumes present")

# COMMAND ----------

# DBTITLE 1,Verify volume accessibility
inaccessible_volumes = []
for volume in VOLUME_UNITS:
    volume_path = f"/Volumes/{CATALOG}/{VOLUME_SCHEMA}/{volume}"
    try:
        dbutils.fs.ls(volume_path)
        print(f"OK  accessible: {volume_path}")
    except Exception as exc:
        inaccessible_volumes.append((volume_path, str(exc)))

if inaccessible_volumes:
    detail = "; ".join(f"{path} -> {err}" for path, err in inaccessible_volumes)
    raise RuntimeError(f"FAIL  inaccessible volumes: {detail}")
print(f"OK  all {len(VOLUME_UNITS)} volumes accessible")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Final Summary

# COMMAND ----------

# DBTITLE 1,Final PASS/FAIL summary
print("=" * 70)
print("ENERGY + WEATHER DEEPENING VOLUME SETUP SUMMARY")
print("=" * 70)
print(f"Catalog         : {CATALOG}")
print(f"Volumes created : {len(VOLUME_UNITS)}  (under {CATALOG}.{VOLUME_SCHEMA})")
for volume in VOLUME_UNITS:
    print(f"                   /Volumes/{CATALOG}/{VOLUME_SCHEMA}/{volume}")
print("-" * 70)
print("RESULT: PASS -- Volumes created and verified.")
print("=" * 70)
