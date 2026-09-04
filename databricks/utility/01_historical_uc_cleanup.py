# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # HISTORICAL UNITY CATALOG CLEANUP -- iPinYou / Criteo Attribution / KDD Cup 2012
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** inspect, then optionally delete, the Unity Catalog Bronze objects
# MAGIC belonging to the three historical, out-of-target-scope sources -- iPinYou,
# MAGIC Criteo Attribution, and KDD Cup 2012 Track 2. The
# MAGIC repo-side code/config/contract cleanup for these sources is already done
# MAGIC (moved to `archive/historical/`); this notebook is the corresponding
# MAGIC Databricks-side cleanup, run manually and deliberately.
# MAGIC
# MAGIC **DO NOT "Run All".** Read every inspection cell's output first. Every
# MAGIC deletion cell is isolated, individually labelled, and safe to skip. Nothing
# MAGIC in this notebook runs automatically or on a schedule.
# MAGIC
# MAGIC **Explicitly out of scope for this notebook:** Search Visibility, and every
# MAGIC other current-scope source/table. If an inspection cell below ever shows a
# MAGIC Search Visibility object matching a historical-source filter, stop and do
# MAGIC not delete it -- report it instead.
# MAGIC
# MAGIC **Known historical Bronze objects (from the repo's own storage/deletion
# MAGIC records), to be confirmed live by the inspection cells below, not
# MAGIC assumed:**
# MAGIC - `bronze.kddcup2012_click_prediction`
# MAGIC - `bronze.criteo_attribution_events`
# MAGIC - `bronze.ipinyou_training`
# MAGIC - `bronze.ipinyou_leaderboard`
# MAGIC - `bronze.ipinyou_reference`
# MAGIC
# MAGIC No CDC, Silver, Gold, or streaming objects exist for these three sources --
# MAGIC they were batch-only, Bronze-stage sources with no further build.

# COMMAND ----------

# MAGIC %md
# MAGIC ## SECTION 1 -- CONFIGURATION (read-only)
# MAGIC
# MAGIC No object is touched by this section. Adjust `CATALOG` here if your
# MAGIC workspace uses a different catalog name than the repo default.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"

# Source-prefix filters used throughout this notebook's discovery cells.
HISTORICAL_SOURCE_PREFIXES = ["ipinyou_", "criteo_attribution_", "kddcup2012_"]

# The specific tables this notebook was written against, per the repo's own
# storage/deletion records. The inspection cells below confirm (or
# contradict) this list against the live catalog -- they do not assume it is
# correct.
EXPECTED_HISTORICAL_TABLES = [
    "kddcup2012_click_prediction",
    "criteo_attribution_events",
    "ipinyou_training",
    "ipinyou_leaderboard",
    "ipinyou_reference",
]

# Explicitly excluded from every filter/deletion in this notebook -- current
# scope, must never be matched by the "ipinyou_/criteo_attribution_/kddcup2012_"
# prefixes above, listed here only as a documented negative check.
CURRENT_SCOPE_PREFIXES_NOT_TOUCHED = ["search_visibility_"]

print(f"Catalog:  {CATALOG}")
print(f"Schema:   {BRONZE_SCHEMA}")
print(f"Historical prefixes: {HISTORICAL_SOURCE_PREFIXES}")
print(
    f"Expected historical tables ({len(EXPECTED_HISTORICAL_TABLES)}): {EXPECTED_HISTORICAL_TABLES}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SECTION 2 -- READ-ONLY INSPECTION
# MAGIC
# MAGIC Every cell in this section is a `SHOW` / `DESCRIBE` / `SELECT COUNT` /
# MAGIC `SELECT ... LIMIT` read. None of them modifies anything. Run all of them
# MAGIC and review the printed output before going anywhere near Section 3.

# COMMAND ----------

# DBTITLE 1,2.1 -- Catalog exists and is reachable
display(spark.sql(f"SHOW CATALOGS LIKE '{CATALOG}'"))

# COMMAND ----------

# DBTITLE 1,2.2 -- Schemas in the target catalog
display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

# DBTITLE 1,2.3 -- Every table currently in the bronze schema (full inventory, not filtered)
all_bronze_tables_df = spark.sql(f"SHOW TABLES IN {CATALOG}.{BRONZE_SCHEMA}")
display(all_bronze_tables_df)
all_bronze_table_names = sorted(r["tableName"] for r in all_bronze_tables_df.collect())
print(f"Total tables in {CATALOG}.{BRONZE_SCHEMA}: {len(all_bronze_table_names)}")

# COMMAND ----------

# DBTITLE 1,2.4 -- Filter to historical-source tables only (ipinyou_ / criteo_attribution_ / kddcup2012_)
historical_table_names = sorted(
    t
    for t in all_bronze_table_names
    if any(t.startswith(p) for p in HISTORICAL_SOURCE_PREFIXES)
)
print(f"Historical-source tables found live ({len(historical_table_names)}):")
for t in historical_table_names:
    print(f"  {t}")

missing_vs_expected = sorted(
    set(EXPECTED_HISTORICAL_TABLES) - set(historical_table_names)
)
extra_vs_expected = sorted(
    set(historical_table_names) - set(EXPECTED_HISTORICAL_TABLES)
)
if missing_vs_expected:
    print(
        f"\nEXPECTED BUT NOT FOUND LIVE (already gone, or naming differs): {missing_vs_expected}"
    )
if extra_vs_expected:
    print(
        f"\nFOUND LIVE BUT NOT IN THE EXPECTED LIST -- investigate before deleting: {extra_vs_expected}"
    )
if not missing_vs_expected and not extra_vs_expected:
    print("\nLive catalog matches the expected historical-table list exactly.")

# COMMAND ----------

# DBTITLE 1,2.5 -- Explicit negative check: confirm no current-scope (Search Visibility) table is caught by these filters
current_scope_false_positives = sorted(
    t
    for t in historical_table_names
    if any(t.startswith(p) for p in CURRENT_SCOPE_PREFIXES_NOT_TOUCHED)
)
assert not current_scope_false_positives, (
    f"STOP: a current-scope table matched a historical filter: {current_scope_false_positives}"
)
print(
    "OK -- no current-scope (Search Visibility) table matched the historical filters."
)

# COMMAND ----------

# DBTITLE 1,2.6 -- Per-table detail: DESCRIBE DETAIL (location, format, size, last modified)
for t in historical_table_names:
    print("=" * 78)
    print(t)
    print("=" * 78)
    display(spark.sql(f"DESCRIBE DETAIL {CATALOG}.{BRONZE_SCHEMA}.{t}"))

# COMMAND ----------

# DBTITLE 1,2.7 -- Per-table row counts
row_counts = {}
for t in historical_table_names:
    n = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{t}").count()
    row_counts[t] = n
    print(f"  {t:<32} {n:>12,} rows")
print(
    f"\nTotal rows across {len(row_counts)} historical tables: {sum(row_counts.values()):,}"
)

# COMMAND ----------

# DBTITLE 1,2.8 -- Per-table schema (DESCRIBE TABLE)
for t in historical_table_names:
    print("=" * 78)
    print(t)
    print("=" * 78)
    display(spark.sql(f"DESCRIBE TABLE {CATALOG}.{BRONZE_SCHEMA}.{t}"))

# COMMAND ----------

# DBTITLE 1,2.9 -- Per-table sample rows (5 rows each, sanity check before deletion)
for t in historical_table_names:
    print("=" * 78)
    print(t)
    print("=" * 78)
    display(spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{t}").limit(5))

# COMMAND ----------

# DBTITLE 1,2.10 -- Any surviving Unity Catalog Volumes for these sources (should already be dropped)
# Each Bronze load notebook drops its source Volume(s) on a full successful
# pass -- so this cell is expected to show nothing. It exists to confirm
# that, not to assume it.
try:
    all_volumes_df = spark.sql(f"SHOW VOLUMES IN {CATALOG}.{BRONZE_SCHEMA}")
    display(all_volumes_df)
    all_volume_names = sorted(r["volume_name"] for r in all_volumes_df.collect())
    surviving_historical_volumes = sorted(
        v
        for v in all_volume_names
        if any(v.startswith(p) for p in HISTORICAL_SOURCE_PREFIXES)
    )
    print(f"Surviving historical-source volumes: {surviving_historical_volumes}")
    if not surviving_historical_volumes:
        print(
            "None found -- consistent with the Bronze notebooks' drop-on-full-pass contract."
        )
except Exception as exc:
    print(f"Could not list volumes (non-fatal, inspection only): {exc}")

# COMMAND ----------

# DBTITLE 1,2.11 -- Consolidated pre-deletion summary
print("=" * 78)
print("PRE-DELETION SUMMARY")
print("=" * 78)
print(f"Catalog.Schema:              {CATALOG}.{BRONZE_SCHEMA}")
print(f"Historical tables found:     {len(historical_table_names)}")
for t in historical_table_names:
    print(f"  - {t:<32} {row_counts.get(t, 'unknown'):>12} rows")
print(
    f"Mismatch vs. expected list:  missing={missing_vs_expected}  extra={extra_vs_expected}"
)
print(f"Current-scope false positives (must be empty): {current_scope_false_positives}")
print("=" * 78)
print("Review the output above in full before running any cell in Section 3.")
print("Each deletion cell below is independent -- run only the ones you confirm.")
print("=" * 78)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SECTION 3 -- DELETION (isolated, one object per cell)
# MAGIC
# MAGIC **STOP.** Do not run this section unless Section 2's output above genuinely
# MAGIC confirms these are the historical, out-of-scope objects you intend to
# MAGIC remove. Each cell drops exactly one table and is independent of every
# MAGIC other cell -- running one does not require or trigger any other. Every
# MAGIC cell is `DROP TABLE IF EXISTS`, so re-running a cell (or running it after
# MAGIC another cell already removed the object) is a safe no-op, not an error.
# MAGIC
# MAGIC These five tables are historical Bronze build record only -- they are not
# MAGIC read by any current-scope pipeline, use case, or Silver/Gold notebook (none exist
# MAGIC for these sources). Deleting them does not affect Search Visibility, SMARD,
# MAGIC DWD, Honda IoT, REES46, or the operational/CDC tables.

# COMMAND ----------

# DBTITLE 1,3.1 -- DELETE ONLY: bronze.kddcup2012_click_prediction (KDD Cup 2012 Track 2)
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{BRONZE_SCHEMA}.kddcup2012_click_prediction")
print(f"Dropped (if it existed): {CATALOG}.{BRONZE_SCHEMA}.kddcup2012_click_prediction")

# COMMAND ----------

# DBTITLE 1,3.2 -- DELETE ONLY: bronze.criteo_attribution_events (Criteo Attribution)
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{BRONZE_SCHEMA}.criteo_attribution_events")
print(f"Dropped (if it existed): {CATALOG}.{BRONZE_SCHEMA}.criteo_attribution_events")

# COMMAND ----------

# DBTITLE 1,3.3 -- DELETE ONLY: bronze.ipinyou_training (iPinYou)
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{BRONZE_SCHEMA}.ipinyou_training")
print(f"Dropped (if it existed): {CATALOG}.{BRONZE_SCHEMA}.ipinyou_training")

# COMMAND ----------

# DBTITLE 1,3.4 -- DELETE ONLY: bronze.ipinyou_leaderboard (iPinYou)
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{BRONZE_SCHEMA}.ipinyou_leaderboard")
print(f"Dropped (if it existed): {CATALOG}.{BRONZE_SCHEMA}.ipinyou_leaderboard")

# COMMAND ----------

# DBTITLE 1,3.5 -- DELETE ONLY: bronze.ipinyou_reference (iPinYou)
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{BRONZE_SCHEMA}.ipinyou_reference")
print(f"Dropped (if it existed): {CATALOG}.{BRONZE_SCHEMA}.ipinyou_reference")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.x -- Optional: any surviving historical Volume found in cell 2.10
# MAGIC
# MAGIC Cell 2.10 is expected to find nothing (Volumes are dropped automatically by
# MAGIC the Bronze load notebooks on a successful full pass). If it *did* find a
# MAGIC surviving volume, add one isolated `DROP VOLUME IF EXISTS
# MAGIC {CATALOG}.{BRONZE_SCHEMA}.<volume_name>` cell per volume here, naming the
# MAGIC exact volume from that cell's output. None is pre-written below because no
# MAGIC volume name can be confirmed without running Section 2 first -- do not
# MAGIC invent one.

# COMMAND ----------

# MAGIC %md
# MAGIC ## SECTION 4 -- POST-DELETION VERIFICATION (read-only)
# MAGIC
# MAGIC Run this section after Section 3 to confirm exactly what was removed and
# MAGIC that nothing else was affected.

# COMMAND ----------

# DBTITLE 1,4.1 -- Re-list bronze schema tables and diff against the pre-deletion snapshot
post_bronze_tables_df = spark.sql(f"SHOW TABLES IN {CATALOG}.{BRONZE_SCHEMA}")
display(post_bronze_tables_df)
post_bronze_table_names = sorted(
    r["tableName"] for r in post_bronze_tables_df.collect()
)

removed = sorted(set(all_bronze_table_names) - set(post_bronze_table_names))
unexpectedly_removed = sorted(set(removed) - set(EXPECTED_HISTORICAL_TABLES))
still_present_historical = sorted(
    t
    for t in post_bronze_table_names
    if any(t.startswith(p) for p in HISTORICAL_SOURCE_PREFIXES)
)

print(f"Tables removed this session: {removed}")
if unexpectedly_removed:
    print(
        f"WARNING -- removed table(s) not in the expected historical list: {unexpectedly_removed}"
    )
else:
    print("OK -- every removed table was on the expected historical list.")
print(f"Historical-source tables still present: {still_present_historical}")

# COMMAND ----------

# DBTITLE 1,4.2 -- Confirm every current-scope table is still present and untouched
# Spot-checks a handful of tables that must never be affected by this notebook.
CURRENT_SCOPE_SPOT_CHECK = [
    "dwd_air_temperature",
    "smard_energy_timeseries",
    "honda_iot_electricity_p",
    "rees46_events",
    "search_visibility_events",
    "search_visibility_repository",
]
missing_current_scope = sorted(
    t for t in CURRENT_SCOPE_SPOT_CHECK if t not in post_bronze_table_names
)
assert not missing_current_scope, (
    f"STOP: a current-scope table is missing after this notebook ran: {missing_current_scope}"
)
print("OK -- all spot-checked current-scope tables are still present.")
for t in CURRENT_SCOPE_SPOT_CHECK:
    present = t in post_bronze_table_names
    print(f"  {t:<32} {'present' if present else 'MISSING'}")

# COMMAND ----------

# DBTITLE 1,4.3 -- Final summary
print("=" * 78)
print("POST-DELETION SUMMARY")
print("=" * 78)
print(
    f"Tables in {CATALOG}.{BRONZE_SCHEMA} before this session: {len(all_bronze_table_names)}"
)
print(
    f"Tables in {CATALOG}.{BRONZE_SCHEMA} now:                 {len(post_bronze_table_names)}"
)
print(f"Removed this session:                                    {removed}")
print(
    f"Historical-source tables remaining live:                 {still_present_historical}"
)
print("=" * 78)

# COMMAND ----------

from pyspark.sql import functions as F

catalog = "energy_commerce_retail_media"
schema = "bronze"

tables = [
    r.tableName for r in spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
]

results = []

for table in tables:
    detail = spark.sql(f"DESCRIBE DETAIL {catalog}.{schema}.{table}").first()
    results.append(
        (
            table,
            detail["numFiles"],
            detail["sizeInBytes"],
            detail["sizeInBytes"] / (1024 * 1024),
        )
    )

df = spark.createDataFrame(
    results, ["table_name", "num_files", "size_bytes", "size_mb"]
)

display(df.orderBy(F.desc("size_mb")))

display(
    df.agg(
        F.count("*").alias("table_count"),
        F.sum("size_bytes").alias("total_size_bytes"),
        F.round(F.sum("size_mb"), 2).alias("total_size_mb"),
    )
)
