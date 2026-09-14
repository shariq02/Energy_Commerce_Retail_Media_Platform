# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # CDC UNITY CATALOG CLEANUP -- Operational / Retail-Media CDC Bronze
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** inspect, then optionally delete, the Unity Catalog Bronze
# MAGIC objects produced by the retired CDC pipeline -- every `bronze.cdc_*_current`
# MAGIC / `bronze.cdc_*_history` table and the `cdc_operational_landing` Volume. The
# MAGIC repo-side code/config cleanup (generators, CDC ingestion, Kafka/Debezium,
# MAGIC streaming notebooks, operational DDL) is already done; this notebook is the
# MAGIC corresponding Databricks-side cleanup, run manually and deliberately.
# MAGIC
# MAGIC **DO NOT "Run All".** Read every inspection cell's output first. Every
# MAGIC deletion cell is isolated, individually labelled, and safe to skip. Nothing
# MAGIC in this notebook runs automatically or on a schedule.
# MAGIC
# MAGIC **Explicitly out of scope for this notebook:** every current-scope source
# MAGIC table (DWD, SMARD, MaStR, power plant list, redispatch, Honda IoT, REES46,
# MAGIC Search Visibility). None of those tables carries a `cdc_` prefix, but the
# MAGIC inspection cells below confirm that rather than assume it.
# MAGIC
# MAGIC **Candidate CDC entities (from the pipeline's own table-naming
# MAGIC convention), to be confirmed live by the inspection cells below, not
# MAGIC assumed:** 7 operational -- `tariffs`, `products`, `customers`,
# MAGIC `customer_contracts`, `meters`, `orders`, `order_items` -- plus 3 retail-media
# MAGIC entities the pipeline also captured earlier in its history -- `advertisers`,
# MAGIC `campaigns`, `campaign_budgets`. Each entity has up to two tables
# MAGIC (`cdc_<entity>_current`, `cdc_<entity>_history`), so up to 20 tables total.
# MAGIC Whether all, some, or none of these are still live is exactly what Section 2
# MAGIC determines -- the pipeline may already have been narrowed to the 7
# MAGIC operational entities only.
# MAGIC
# MAGIC No Silver, Gold, or other downstream object was ever built on these tables.

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
CDC_VOLUME = "cdc_operational_landing"

# Table-name prefix used throughout this notebook's discovery cells -- no
# current-scope table uses this prefix.
CDC_TABLE_PREFIX = "cdc_"

CANDIDATE_ENTITIES = [
    "tariffs",
    "products",
    "customers",
    "customer_contracts",
    "meters",
    "orders",
    "order_items",
    "advertisers",
    "campaigns",
    "campaign_budgets",
]
CANDIDATE_SUFFIXES = ["current", "history"]
CANDIDATE_TABLES = [
    f"cdc_{entity}_{suffix}"
    for entity in CANDIDATE_ENTITIES
    for suffix in CANDIDATE_SUFFIXES
]

print(f"Catalog:  {CATALOG}")
print(f"Schema:   {BRONZE_SCHEMA}")
print(f"Volume:   {CDC_VOLUME}")
print(f"Candidate entities ({len(CANDIDATE_ENTITIES)}): {CANDIDATE_ENTITIES}")
print(f"Candidate tables ({len(CANDIDATE_TABLES)}): {CANDIDATE_TABLES}")

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

# DBTITLE 1,2.2 -- Every table currently in the bronze schema (full inventory, not filtered)
all_bronze_tables_df = spark.sql(f"SHOW TABLES IN {CATALOG}.{BRONZE_SCHEMA}")
display(all_bronze_tables_df)
all_bronze_table_names = sorted(r["tableName"] for r in all_bronze_tables_df.collect())
print(f"Total tables in {CATALOG}.{BRONZE_SCHEMA}: {len(all_bronze_table_names)}")

# COMMAND ----------

# DBTITLE 1,2.3 -- Filter to CDC tables only (cdc_ prefix), no assumption about which entities
cdc_table_names = sorted(
    t for t in all_bronze_table_names if t.startswith(CDC_TABLE_PREFIX)
)
print(f"CDC-prefixed tables found live ({len(cdc_table_names)}):")
for t in cdc_table_names:
    print(f"  {t}")

missing_vs_candidates = sorted(set(CANDIDATE_TABLES) - set(cdc_table_names))
extra_vs_candidates = sorted(set(cdc_table_names) - set(CANDIDATE_TABLES))
if missing_vs_candidates:
    print(
        f"\nCANDIDATE BUT NOT FOUND LIVE (already gone, or never built): {missing_vs_candidates}"
    )
if extra_vs_candidates:
    print(
        f"\nFOUND LIVE BUT NOT A CANDIDATE -- investigate before deleting: {extra_vs_candidates}"
    )
if not missing_vs_candidates and not extra_vs_candidates:
    print("\nLive catalog matches the candidate CDC-table list exactly.")

# COMMAND ----------

# DBTITLE 1,2.4 -- Explicit negative check: confirm no current-scope table carries the cdc_ prefix
CURRENT_SCOPE_SPOT_CHECK = [
    "dwd_air_temperature",
    "smard_energy_timeseries",
    "mastr_marktakteure",
    "power_plant_list",
    "redispatch_measures",
    "honda_iot_electricity_p",
    "rees46_events",
    "search_visibility_events",
    "search_visibility_repository",
]
current_scope_false_positives = sorted(
    t for t in CURRENT_SCOPE_SPOT_CHECK if t.startswith(CDC_TABLE_PREFIX)
)
assert not current_scope_false_positives, (
    f"STOP: a current-scope table name matched the CDC filter: {current_scope_false_positives}"
)
print("OK -- no current-scope table name matches the cdc_ prefix (by construction).")

# COMMAND ----------

# DBTITLE 1,2.5 -- Per-table detail: DESCRIBE DETAIL (location, format, size, last modified)
for t in cdc_table_names:
    print("=" * 78)
    print(t)
    print("=" * 78)
    display(spark.sql(f"DESCRIBE DETAIL {CATALOG}.{BRONZE_SCHEMA}.{t}"))

# COMMAND ----------

# DBTITLE 1,2.6 -- Per-table row counts
row_counts = {}
for t in cdc_table_names:
    n = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{t}").count()
    row_counts[t] = n
    print(f"  {t:<32} {n:>12,} rows")
print(f"\nTotal rows across {len(row_counts)} CDC tables: {sum(row_counts.values()):,}")

# COMMAND ----------

# DBTITLE 1,2.7 -- Per-table sample rows (5 rows each, sanity check before deletion)
for t in cdc_table_names:
    print("=" * 78)
    print(t)
    print("=" * 78)
    display(spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{t}").limit(5))

# COMMAND ----------

# DBTITLE 1,2.8 -- Every cdc_-prefixed Volume, not just the known cdc_operational_landing
# Discovered by prefix, the same way the tables above are -- so a second or
# renamed CDC volume is caught here too, not just the one name this notebook
# happens to know about.
cdc_volume_names: list[str] = []
try:
    all_volumes_df = spark.sql(f"SHOW VOLUMES IN {CATALOG}.{BRONZE_SCHEMA}")
    display(all_volumes_df)
    all_volume_names = sorted(r["volume_name"] for r in all_volumes_df.collect())
    cdc_volume_names = sorted(
        v for v in all_volume_names if v.startswith(CDC_TABLE_PREFIX)
    )
    print(
        f"CDC-prefixed volumes found live ({len(cdc_volume_names)}): {cdc_volume_names}"
    )
    if CDC_VOLUME not in cdc_volume_names:
        print(
            f"Note: the known '{CDC_VOLUME}' is not among them (already gone, or never built)."
        )
    extra_cdc_volumes = sorted(set(cdc_volume_names) - {CDC_VOLUME})
    if extra_cdc_volumes:
        print(
            f"ADDITIONAL cdc_-prefixed volume(s) beyond the known one: {extra_cdc_volumes}"
        )
    for v in cdc_volume_names:
        display(dbutils.fs.ls(f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{v}"))
except Exception as exc:
    print(f"Could not list volumes (non-fatal, inspection only): {exc}")

# COMMAND ----------

# DBTITLE 1,2.9 -- Consolidated pre-deletion summary
print("=" * 78)
print("PRE-DELETION SUMMARY")
print("=" * 78)
print(f"Catalog.Schema:              {CATALOG}.{BRONZE_SCHEMA}")
print(f"CDC tables found:            {len(cdc_table_names)}")
for t in cdc_table_names:
    print(f"  - {t:<32} {row_counts.get(t, 'unknown'):>12} rows")
print(
    f"Mismatch vs. candidates:     missing={missing_vs_candidates}  extra={extra_vs_candidates}"
)
print(f"Current-scope false positives (must be empty): {current_scope_false_positives}")
print("=" * 78)
print("Review the output above in full before running any cell in Section 3.")
print("Each deletion cell below is independent -- run only the ones you confirm.")
print("=" * 78)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SECTION 3 -- DELETION (isolated, one entity per cell)
# MAGIC
# MAGIC **STOP.** Do not run this section unless Section 2's output above genuinely
# MAGIC confirms these are CDC-pipeline objects you intend to remove. Each cell
# MAGIC drops both tables (`_current`, `_history`) for exactly one entity and is
# MAGIC independent of every other cell -- running one does not require or trigger
# MAGIC any other. Every statement is `DROP TABLE IF EXISTS`, so re-running a cell
# MAGIC (or running it after another cell already removed the object) is a safe
# MAGIC no-op, not an error.
# MAGIC
# MAGIC These tables are retired-pipeline Bronze build record only -- they are not
# MAGIC read by any current-scope pipeline, use case, or Silver/Gold notebook.
# MAGIC Deleting them does not affect DWD, SMARD, MaStR, power plant list,
# MAGIC redispatch, Honda IoT, REES46, or Search Visibility.

# COMMAND ----------


def drop_entity(entity: str) -> None:
    for suffix in CANDIDATE_SUFFIXES:
        full = f"{CATALOG}.{BRONZE_SCHEMA}.cdc_{entity}_{suffix}"
        spark.sql(f"DROP TABLE IF EXISTS {full}")
        print(f"Dropped (if it existed): {full}")


# COMMAND ----------

# DBTITLE 1,3.1 -- DELETE ONLY: cdc_tariffs_current / cdc_tariffs_history
drop_entity("tariffs")

# COMMAND ----------

# DBTITLE 1,3.2 -- DELETE ONLY: cdc_products_current / cdc_products_history
drop_entity("products")

# COMMAND ----------

# DBTITLE 1,3.3 -- DELETE ONLY: cdc_customers_current / cdc_customers_history
drop_entity("customers")

# COMMAND ----------

# DBTITLE 1,3.4 -- DELETE ONLY: cdc_customer_contracts_current / cdc_customer_contracts_history
drop_entity("customer_contracts")

# COMMAND ----------

# DBTITLE 1,3.5 -- DELETE ONLY: cdc_meters_current / cdc_meters_history
drop_entity("meters")

# COMMAND ----------

# DBTITLE 1,3.6 -- DELETE ONLY: cdc_orders_current / cdc_orders_history
drop_entity("orders")

# COMMAND ----------

# DBTITLE 1,3.7 -- DELETE ONLY: cdc_order_items_current / cdc_order_items_history
drop_entity("order_items")

# COMMAND ----------

# DBTITLE 1,3.8 -- DELETE ONLY: cdc_advertisers_current / cdc_advertisers_history (retail-media)
drop_entity("advertisers")

# COMMAND ----------

# DBTITLE 1,3.9 -- DELETE ONLY: cdc_campaigns_current / cdc_campaigns_history (retail-media)
drop_entity("campaigns")

# COMMAND ----------

# DBTITLE 1,3.10 -- DELETE ONLY: cdc_campaign_budgets_current / cdc_campaign_budgets_history (retail-media)
drop_entity("campaign_budgets")

# COMMAND ----------

# DBTITLE 1,3.11 -- DELETE: every cdc_-prefixed Volume found live in cell 2.8
# Run only after every table cell above (or the ones that applied) has run --
# this removes the landing storage those tables were built from. Drops every
# volume cell 2.8 found (the known cdc_operational_landing plus any other
# cdc_-prefixed volume), not just the one name this notebook was written
# against -- consistent with the prefix-based table deletion above.
for _v in cdc_volume_names:
    spark.sql(f"DROP VOLUME IF EXISTS {CATALOG}.{BRONZE_SCHEMA}.{_v}")
    print(f"Dropped (if it existed): {CATALOG}.{BRONZE_SCHEMA}.{_v}")
if not cdc_volume_names:
    print("No cdc_-prefixed volume was found live in cell 2.8 -- nothing to drop.")

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
unexpectedly_removed = sorted(set(removed) - set(CANDIDATE_TABLES))
still_present_cdc = sorted(
    t for t in post_bronze_table_names if t.startswith(CDC_TABLE_PREFIX)
)

print(f"Tables removed this session: {removed}")
if unexpectedly_removed:
    print(
        f"WARNING -- removed table(s) not in the candidate CDC list: {unexpectedly_removed}"
    )
else:
    print("OK -- every removed table was on the candidate CDC list.")
print(f"CDC-prefixed tables still present: {still_present_cdc}")

# COMMAND ----------

# DBTITLE 1,4.2 -- Confirm every current-scope table is still present and untouched
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

# DBTITLE 1,4.3 -- Confirm no cdc_-prefixed Volume remains
post_volumes_df = spark.sql(f"SHOW VOLUMES IN {CATALOG}.{BRONZE_SCHEMA}")
post_volume_names = {r["volume_name"] for r in post_volumes_df.collect()}
still_present_cdc_volumes = sorted(
    v for v in post_volume_names if v.startswith(CDC_TABLE_PREFIX)
)
print(f"cdc_-prefixed volumes remaining after cleanup: {still_present_cdc_volumes}")

# COMMAND ----------

# DBTITLE 1,4.4 -- Final summary
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
print(f"CDC-prefixed tables remaining live:                      {still_present_cdc}")
print("=" * 78)
print(
    "NEXT STEP (not part of this notebook): re-run "
    "databricks/schema_registry/01_snapshot_bronze_schema.py to produce "
    "bronze_schema_v002.md."
)
print("=" * 78)