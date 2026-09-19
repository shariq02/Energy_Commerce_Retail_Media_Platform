# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # TABLE CONSOLIDATION VERIFICATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** read-only checks on the live catalog for each table
# MAGIC consolidation candidate -- grain, keys, schema, timestamps, identifiers,
# MAGIC sparsity, provenance, Silver/Gold duplication. Each check prints its own
# MAGIC result; the last cell prints a summary and a per-group verdict.
# MAGIC
# MAGIC Run all cells in order. Nothing is written.

# COMMAND ----------

# DBTITLE 1,Imports
from functools import reduce

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Config -- schemas
CAT = "energy_commerce_retail_media"
BRONZE = f"{CAT}.bronze"
SILVER = f"{CAT}.energy_silver"
SILVER_REF = f"{CAT}.energy_silver_reference"
GOLD = f"{CAT}.energy_gold"
C_SILVER = f"{CAT}.commerce_silver"
C_SILVER_REF = f"{CAT}.commerce_silver_reference"
C_GOLD = f"{CAT}.commerce_gold"
QUALITY = f"{CAT}.quality"

# Row-heavy pass-through pairs (~110M rows) are skipped unless True.
RUN_HEAVY = False

VERDICT_ORDER = ["REJECTS", "ERROR", "REVIEW", "SUPPORTS"]

# COMMAND ----------

# DBTITLE 1,Config -- table lists
DWD_HOURLY = [
    "air_temperature",
    "cloudiness",
    "moisture",
    "precipitation",
    "pressure",
    "sun",
    "wind",
    "dew_point",
    "soil_temperature",
    "visibility",
    "cloud_type",
    "wind_synop",
    "extreme_wind",
    "weather_phenomena",
]
DWD_KEY_COLUMNS = {
    "STATIONS_ID",
    "city",
    "observation_ts",
    "ags_code",
    "ags_level",
    "ags_method",
    "eor",
    "_had_key_conflict",
    "qn_level_code",
    "qn_level_label_de",
    "qn_level",
}
HONDA_SILVER = [
    "honda_electricity_p",
    "honda_electricity_w",
    "honda_heating_p",
    "honda_heating_w",
    "honda_cooling_p",
    "honda_cooling_w",
    "honda_weather",
]
HONDA_NON_VALUE = {
    "frequency",
    "datetime_utc",
    "source_record_id",
    "source_system",
    "ecosystem",
}
UNIT_TYPES = [
    "wind",
    "biomasse",
    "wasser",
    "verbrennung",
    "kernkraft",
    "geothermie_gsgk",
]
EEG_TYPES = ["wind", "biomasse", "wasser", "geothermie_gsgk"]
BRIDGES = {
    "eeg_support_unit": "mastr_eeg_support_unit_bridge",
    "kwk_support_unit": "mastr_kwk_support_unit_bridge",
    "authorisation_unit": "mastr_authorisation_unit_bridge",
    "repowering_eeg": "mastr_repowering_eeg_bridge",
    "location_unit": "mastr_location_unit_bridge",
    "location_connection": "mastr_location_connection_bridge",
    "actor_role": "mastr_actor_role_bridge",
}
SMALL_LISTS = ["einheitentypen", "lokationstypen", "marktfunktionen", "marktrollen"]
CATALOGS = [("katalogkategorien", "Name"), ("katalogwerte", "Wert")] + [
    (name, "Wert") for name in SMALL_LISTS
]
PROVENANCE = {
    "source_record_id",
    "source_system",
    "ecosystem",
    "_silver_loaded_at",
    "_silver_run_id",
}

# COMMAND ----------

# DBTITLE 1,Config -- provenance groups and pass-through pairs
# (schema, tables, typed): typed groups must be unique across their tables.
PROVENANCE_GROUPS = {
    "dwd_hourly": (SILVER, [f"dwd_{p}" for p in DWD_HOURLY], False),
    "honda": (SILVER, HONDA_SILVER, False),
    "bridges": (SILVER, list(BRIDGES.values()), True),
    "catalogs": (SILVER_REF, [f"mastr_{n}" for n, _ in CATALOGS], True),
}

# (silver schema, silver table, gold schema, gold table)
PASS_THROUGH_PAIRS = [
    (SILVER, "honda_electricity_p", GOLD, "fact_site_electricity_p"),
    (SILVER, "honda_weather", GOLD, "fact_site_weather"),
    (SILVER, "smard_energy_timeseries", GOLD, "fact_market_timeseries"),
    (SILVER, "power_plant_capacity_additions", GOLD, "fact_capacity_addition"),
    (SILVER, "mastr_repowering_eeg_bridge", GOLD, "bridge_repowering_eeg"),
    (SILVER, "mastr_lokationen", GOLD, "dim_location"),
    (SILVER, "mastr_netze", GOLD, "dim_network"),
    (SILVER, "mastr_netzanschlusspunkte", GOLD, "dim_grid_connection_point"),
    (SILVER_REF, "mastr_katalogkategorien", GOLD, "dim_mastr_katalog_kategorie"),
    (SILVER_REF, "mastr_einheitentypen", GOLD, "dim_einheitentyp"),
    (SILVER_REF, "mastr_lokationstypen", GOLD, "dim_lokationstyp"),
    (SILVER_REF, "dwd_parameter_catalog", GOLD, "dim_dwd_parameter_catalog"),
    (SILVER_REF, "honda_channel_catalog", GOLD, "dim_device"),
    (C_SILVER_REF, "search_visibility_repository", C_GOLD, "dim_repository"),
]
HEAVY_PAIRS = [
    (C_SILVER, "ga4_events", C_GOLD, "fact_web_event"),
    (C_SILVER, "rees46_events", C_GOLD, "fact_customer_activity_event"),
]

# COMMAND ----------

# DBTITLE 1,Results store
RESULTS = []

# COMMAND ----------

# DBTITLE 1,Def -- print_rows


def print_rows(header, rows):
    table = [[str(h) for h in header]] + [[str(v) for v in r] for r in rows]
    widths = [max(len(r[i]) for r in table) for i in range(len(header))]
    for r in table:
        print("  " + " | ".join(v.ljust(w) for v, w in zip(r, widths)))


# COMMAND ----------

# DBTITLE 1,Def -- tbl


def tbl(schema, name):
    return spark.table(f"{schema}.{name}")


# COMMAND ----------

# DBTITLE 1,Def -- union_all


def union_all(dfs, allow_missing=False):
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=allow_missing), dfs)


# COMMAND ----------

# DBTITLE 1,Def -- dupes


def dupes(df, keys):
    return df.groupBy(*keys).count().filter("count > 1").count()


# COMMAND ----------

# DBTITLE 1,Def -- worst_verdict


def worst_verdict(verdicts):
    return min(verdicts, key=VERDICT_ORDER.index)


# COMMAND ----------

# DBTITLE 1,Def -- record


def record(check_id, groups, question, verdict, metrics):
    RESULTS.append(
        {
            "id": check_id,
            "groups": groups,
            "question": question,
            "verdict": verdict,
            "metrics": metrics,
        }
    )
    print(f"[{verdict}] {check_id} {question}")
    print(f"    {metrics}")


# COMMAND ----------

# DBTITLE 1,Def -- run_check


def run_check(check_id, groups, question, fn):
    print(f"\n=== {check_id} {question} ===")
    try:
        verdict, metrics = fn()
    except Exception as exc:
        verdict, metrics = "ERROR", f"{type(exc).__name__}: {str(exc)[:240]}"
    record(check_id, groups, question, verdict, metrics)


# COMMAND ----------

# MAGIC %md
# MAGIC ## V01 -- DWD hourly: (station, hour) key overlap

# COMMAND ----------

# DBTITLE 1,Def -- check_dwd_overlap


def check_dwd_overlap():
    keys = union_all(
        [
            tbl(SILVER, f"dwd_{p}")
            .select("STATIONS_ID", "observation_ts")
            .withColumn("p", F.lit(p))
            for p in DWD_HOURLY
        ]
    )
    per_key = keys.groupBy("STATIONS_ID", "observation_ts").agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("p").alias("n_products"),
    )
    hist = per_key.groupBy("n_rows", "n_products").count().collect()
    union_keys = sum(r["count"] for r in hist)
    dup_keys = sum(r["count"] for r in hist if r["n_rows"] > r["n_products"])
    avg_products = sum(r["n_products"] * r["count"] for r in hist) / union_keys
    by_n = {}
    for r in hist:
        by_n[r["n_products"]] = by_n.get(r["n_products"], 0) + r["count"]
    print("keys by number of products present:")
    print_rows(["n_products", "keys"], sorted(by_n.items()))
    per_product = keys.groupBy("p").count().orderBy("count").collect()
    print("rows per product:")
    print_rows(
        ["product", "rows", "share_of_union_keys"],
        [(r["p"], r["count"], f"{r['count'] / union_keys:.1%}") for r in per_product],
    )
    if dup_keys > 0:
        verdict = "REJECTS"
    elif avg_products >= 8:
        verdict = "SUPPORTS"
    else:
        verdict = "REVIEW"
    return verdict, (
        f"union_keys={union_keys:,}, dup_keys={dup_keys}, "
        f"avg_products_per_key={avg_products:.2f}"
    )


# COMMAND ----------

# DBTITLE 1,Run V01
run_check(
    "V01",
    ["dwd_hourly_gold", "dwd_hourly_silver"],
    "DWD hourly key overlap and duplicates",
    check_dwd_overlap,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V02 -- DWD hourly: column-name and type collisions

# COMMAND ----------

# DBTITLE 1,Def -- check_dwd_columns


def check_dwd_columns():
    seen = {}
    for p in DWD_HOURLY:
        for f in tbl(SILVER, f"dwd_{p}").schema.fields:
            if f.name not in PROVENANCE:
                seen.setdefault(f.name, {})[p] = f.dataType.simpleString()
    shared = {c: v for c, v in seen.items() if len(v) > 1}
    conflicts = {
        c: sorted(set(v.values()))
        for c, v in shared.items()
        if len(set(v.values())) > 1
    }
    needs_prefix = sorted(c for c in shared if c not in DWD_KEY_COLUMNS)
    print("shared non-key columns (need a product prefix):")
    print_rows(
        ["column", "n_products", "types"],
        [(c, len(shared[c]), sorted(set(shared[c].values()))) for c in needs_prefix],
    )
    print("type conflicts:", conflicts or "none")
    verdict = "REJECTS" if conflicts else "SUPPORTS"
    return verdict, (
        f"shared_columns={len(shared)}, need_prefix={len(needs_prefix)}, "
        f"type_conflicts={len(conflicts)}"
    )


# COMMAND ----------

# DBTITLE 1,Run V02
run_check(
    "V02",
    ["dwd_hourly_gold", "dwd_hourly_silver"],
    "DWD hourly column and type collisions",
    check_dwd_columns,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V03 -- DWD: timestamp grid, range, stations (solar included)

# COMMAND ----------

# DBTITLE 1,Def -- check_dwd_timestamps


def check_dwd_timestamps():
    rows = []
    for p in DWD_HOURLY + ["solar"]:
        r = (
            tbl(SILVER, f"dwd_{p}")
            .agg(
                F.min("observation_ts").alias("min_ts"),
                F.max("observation_ts").alias("max_ts"),
                F.count("*").alias("n"),
                F.countDistinct("STATIONS_ID").alias("stations"),
                F.avg(
                    (
                        (F.minute("observation_ts") == 0)
                        & (F.second("observation_ts") == 0)
                    ).cast("int")
                ).alias("on_hour"),
            )
            .first()
        )
        rows.append(
            (p, r["min_ts"], r["max_ts"], r["n"], r["stations"], r["on_hour"] or 0.0)
        )
    print_rows(
        ["product", "min_ts", "max_ts", "rows", "stations", "share_on_the_hour"],
        [(*r[:5], round(r[5], 4)) for r in rows],
    )
    off_grid = [r[0] for r in rows if r[0] != "solar" and r[5] < 1.0]
    solar_on_hour = next(r[5] for r in rows if r[0] == "solar")
    if off_grid:
        verdict = "REJECTS"
    elif solar_on_hour < 1.0:
        verdict = "SUPPORTS"
    else:
        verdict = "REVIEW"
    return verdict, (
        f"hourly_off_grid={off_grid or 'none'}, solar_share_on_the_hour={solar_on_hour:.4f}"
    )


# COMMAND ----------

# DBTITLE 1,Run V03
run_check(
    "V03",
    ["dwd_hourly_gold", "dwd_hourly_silver"],
    "DWD timestamp grid and solar separation",
    check_dwd_timestamps,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V04 -- Honda: key coverage across the seven Silver tables

# COMMAND ----------

# DBTITLE 1,Def -- check_honda_keys


def check_honda_keys():
    keys = union_all(
        [
            tbl(SILVER, t).select("frequency", "datetime_utc").withColumn("t", F.lit(t))
            for t in HONDA_SILVER
        ]
    )
    per_key = keys.groupBy("frequency", "datetime_utc").agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("t").alias("n_tables"),
    )
    hist = per_key.groupBy("frequency", "n_tables", "n_rows").count().collect()
    total = sum(r["count"] for r in hist)
    full = sum(r["count"] for r in hist if r["n_tables"] == len(HONDA_SILVER))
    dup_keys = sum(r["count"] for r in hist if r["n_rows"] > r["n_tables"])
    by = {}
    for r in hist:
        k = (r["frequency"], r["n_tables"])
        by[k] = by.get(k, 0) + r["count"]
    print("keys by frequency and number of tables present:")
    print_rows(
        ["frequency", "n_tables", "keys"], sorted((*k, v) for k, v in by.items())
    )
    share = full / total
    if dup_keys > 0 or share < 0.99:
        verdict = "REJECTS"
    elif share >= 0.999:
        verdict = "SUPPORTS"
    else:
        verdict = "REVIEW"
    return verdict, (
        f"union_keys={total:,}, in_all_{len(HONDA_SILVER)}_tables={share:.4%}, "
        f"dup_keys={dup_keys}"
    )


# COMMAND ----------

# DBTITLE 1,Run V04
run_check(
    "V04",
    ["honda_silver", "honda_gold"],
    "Honda key coverage across tables",
    check_honda_keys,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V05 -- Honda: channel catalog vs Silver value columns

# COMMAND ----------

# DBTITLE 1,Def -- check_honda_catalog


def check_honda_catalog():
    rows = (
        tbl(SILVER_REF, "honda_channel_catalog")
        .select("subsystem", "measurement_type", "channel_code")
        .collect()
    )
    expected = {
        (
            f"honda_{r['subsystem']}_{'p' if r['measurement_type'] == 'power' else 'w'}",
            r["channel_code"],
        )
        for r in rows
    }
    actual = set()
    for sub in ("electricity", "heating", "cooling"):
        for suffix in ("p", "w"):
            name = f"honda_{sub}_{suffix}"
            for c in tbl(SILVER, name).columns:
                if c not in HONDA_NON_VALUE and not c.startswith("_"):
                    actual.add((name, c))
    only_silver = sorted(actual - expected)
    only_catalog = sorted(expected - actual)
    print("in Silver, not in catalog:", only_silver or "none")
    print("in catalog, not in Silver:", only_catalog or "none")
    verdict = "SUPPORTS" if not only_silver and not only_catalog else "REJECTS"
    return verdict, (
        f"catalog_channels={len(expected)}, silver_value_columns={len(actual)}, "
        f"only_silver={len(only_silver)}, only_catalog={len(only_catalog)}"
    )


# COMMAND ----------

# DBTITLE 1,Run V05
run_check(
    "V05",
    ["honda_silver", "honda_gold"],
    "Honda channel catalog matches Silver columns",
    check_honda_catalog,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V06 -- MaStR units: column population per technology

# COMMAND ----------

# DBTITLE 1,Def -- check_unit_sparsity


def check_unit_sparsity():
    tables = {t: tbl(SILVER, f"mastr_einheiten_{t}") for t in UNIT_TYPES}
    union = union_all(
        [df.withColumn("unit_type", F.lit(t)) for t, df in tables.items()],
        allow_missing=True,
    )
    cols = [c for c in union.columns if c != "unit_type"]
    common = set.intersection(*[set(df.columns) for df in tables.values()])
    rows = (
        union.groupBy("unit_type")
        .agg(
            F.count("*").alias("__rows"),
            *[F.count(F.col(f"`{c}`")).alias(c) for c in cols],
        )
        .collect()
    )
    stats = []
    common_gaps = 0
    for r in rows:
        n = r["__rows"] or 1
        pop = {c: (r[c] or 0) / n for c in cols}
        common_ok = sum(pop[c] > 0 for c in common)
        common_gaps += len(common) - common_ok
        stats.append(
            (
                r["unit_type"],
                r["__rows"],
                sum(v > 0 for v in pop.values()),
                sum(v > 0.5 for v in pop.values()),
                f"{common_ok}/{len(common)}",
            )
        )
    print_rows(
        ["unit_type", "rows", "cols_any", "cols_over_50pct", "common_populated"],
        sorted(stats),
    )
    verdict = "SUPPORTS" if common_gaps == 0 else "REVIEW"
    return verdict, (
        f"union_columns={len(cols)}, common_columns={len(common)}, "
        f"common_unpopulated_type_pairs={common_gaps}"
    )


# COMMAND ----------

# DBTITLE 1,Run V06
run_check(
    "V06",
    ["mastr_units"],
    "MaStR unit superset sparsity",
    check_unit_sparsity,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V07 -- MaStR units: unit_id collisions across technologies

# COMMAND ----------

# DBTITLE 1,Def -- check_unit_ids


def check_unit_ids():
    ids = union_all(
        [
            tbl(SILVER, f"mastr_einheiten_{t}")
            .select("unit_id")
            .withColumn("t", F.lit(t))
            for t in UNIT_TYPES
        ]
    )
    colliding = ids.groupBy("unit_id").count().filter("count > 1").count()
    prefixes = (
        ids.groupBy(F.substring("unit_id", 1, 3).alias("prefix"), "t")
        .count()
        .orderBy("prefix", "t")
        .collect()
    )
    print("unit_id prefix per technology:")
    print_rows(
        ["prefix", "technology", "rows"],
        [(r["prefix"], r["t"], r["count"]) for r in prefixes],
    )
    verdict = "SUPPORTS" if colliding == 0 else "REJECTS"
    return verdict, f"colliding_unit_ids={colliding}"


# COMMAND ----------

# DBTITLE 1,Run V07
run_check(
    "V07",
    ["mastr_units"],
    "MaStR unit_id uniqueness across technologies",
    check_unit_ids,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V08 -- MaStR EEG / KWK: identifier safety

# COMMAND ----------

# DBTITLE 1,Def -- check_eeg_kwk


def check_eeg_kwk():
    eeg = union_all(
        [
            tbl(SILVER, f"mastr_anlagen_eeg_{t}").select(
                F.col("eeg_support_id").alias("id")
            )
            for t in EEG_TYPES
        ]
    )
    kwk = tbl(SILVER, "mastr_anlagen_kwk").select(F.col("kwk_support_id").alias("id"))
    eeg_dupes = eeg.groupBy("id").count().filter("count > 1").count()
    overlap = eeg.select("id").intersect(kwk).count()
    eeg_prefix = eeg.groupBy(F.substring("id", 1, 3).alias("p")).count().collect()
    kwk_prefix = kwk.groupBy(F.substring("id", 1, 3).alias("p")).count().collect()
    print("EEG id prefixes:", {r["p"]: r["count"] for r in eeg_prefix})
    print("KWK id prefixes:", {r["p"]: r["count"] for r in kwk_prefix})
    if eeg_dupes > 0:
        verdict = "REJECTS"
    elif overlap > 0:
        verdict = "REVIEW"
    else:
        verdict = "SUPPORTS"
    return verdict, f"eeg_duplicated_ids={eeg_dupes}, eeg_kwk_id_overlap={overlap}"


# COMMAND ----------

# DBTITLE 1,Run V08
run_check(
    "V08",
    ["mastr_support"],
    "EEG and KWK identifier safety",
    check_eeg_kwk,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V09 -- MaStR bridges: typed-key collision safety

# COMMAND ----------

# DBTITLE 1,Def -- check_bridges


def check_bridges():
    pairs = union_all(
        [
            tbl(SILVER, t)
            .select("parent_id", "linked_id", "source_record_id")
            .withColumn("rel", F.lit(rel))
            for rel, t in BRIDGES.items()
        ]
    )
    shared_pairs = (
        pairs.groupBy("parent_id", "linked_id")
        .agg(F.countDistinct("rel").alias("n_rel"))
        .filter("n_rel > 1")
        .count()
    )
    shared_ids = (
        pairs.groupBy("source_record_id")
        .agg(F.countDistinct("rel").alias("n"))
        .filter("n > 1")
        .count()
    )
    per_rel = (
        pairs.groupBy("rel")
        .agg(
            F.count("*").alias("pairs"),
            F.countDistinct("parent_id").alias("parents"),
            F.countDistinct("linked_id").alias("linked"),
        )
        .orderBy("rel")
        .collect()
    )
    print_rows(
        ["relationship", "pairs", "parents", "linked"],
        [(r["rel"], r["pairs"], r["parents"], r["linked"]) for r in per_rel],
    )
    prefixes = (
        pairs.groupBy(
            "rel",
            F.substring("parent_id", 1, 3).alias("parent_prefix"),
            F.substring("linked_id", 1, 3).alias("linked_prefix"),
        )
        .count()
        .orderBy("rel")
        .collect()
    )
    print("id prefixes per relationship:")
    print_rows(
        ["relationship", "parent_prefix", "linked_prefix", "pairs"],
        [
            (r["rel"], r["parent_prefix"], r["linked_prefix"], r["count"])
            for r in prefixes
        ],
    )
    verdict = "SUPPORTS" if shared_pairs == 0 and shared_ids == 0 else "REVIEW"
    return verdict, (
        f"pairs_under_multiple_relationships={shared_pairs}, "
        f"source_record_id_shared_across_relationships={shared_ids}"
    )


# COMMAND ----------

# DBTITLE 1,Run V09
run_check(
    "V09",
    ["bridges_silver", "bridges_gold"],
    "MaStR bridge pair and record-id collisions",
    check_bridges,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V10 -- MaStR code lists: Id collisions and overlap

# COMMAND ----------

# DBTITLE 1,Def -- catalog_rows


def catalog_rows(name, value_col):
    return tbl(SILVER_REF, f"mastr_{name}").select(
        F.col("Id").cast("string").alias("Id"),
        F.col(value_col).cast("string").alias("Wert"),
        F.lit(name).alias("kind"),
    )


# COMMAND ----------

# DBTITLE 1,Def -- check_code_lists


def check_code_lists():
    everything = union_all([catalog_rows(n, v) for n, v in CATALOGS])
    per_id = everything.groupBy("Id").agg(F.countDistinct("kind").alias("n_kinds"))
    reused = per_id.filter("n_kinds > 1").count()
    total_ids = per_id.count()
    values = catalog_rows("katalogwerte", "Wert").select("Id", "Wert")
    rows = []
    for name in SMALL_LISTS:
        small = catalog_rows(name, "Wert").select("Id", "Wert")
        rows.append((name, small.count(), small.join(values, ["Id", "Wert"]).count()))
    print("small lists already present in katalogwerte (Id, Wert):")
    print_rows(["list", "rows", "also_in_katalogwerte"], rows)
    contained = [r[0] for r in rows if r[1] > 0 and r[1] == r[2]]
    verdict = "REVIEW" if contained else "SUPPORTS"
    return verdict, (
        f"ids_reused_across_catalogs={reused} of {total_ids}, "
        f"lists_fully_inside_katalogwerte={contained or 'none'}"
    )


# COMMAND ----------

# DBTITLE 1,Run V10
run_check(
    "V10",
    ["bronze_mastr_lookups", "codes_silver", "codes_gold"],
    "MaStR code-list Id collisions and containment",
    check_code_lists,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V11 -- MaStR change events: identifier and time safety

# COMMAND ----------

# DBTITLE 1,Def -- check_change_events


def check_change_events():
    unit = tbl(SILVER, "mastr_unit_deletion_events").select(
        F.col("unit_id").alias("entity_id"),
        F.expr("try_cast(last_updated_at AS timestamp)").alias("ts"),
        F.lit("unit_deletion").alias("kind"),
    )
    actor = tbl(SILVER, "mastr_actor_deletion_events").select(
        F.col("market_actor_id").alias("entity_id"),
        F.expr("try_cast(last_updated_at AS timestamp)").alias("ts"),
        F.lit("actor_deletion").alias("kind"),
    )
    change = tbl(SILVER, "mastr_grid_operator_change_events").select(
        F.col("unit_id").alias("entity_id"),
        F.expr("try_cast(grid_operator_change_effective_date AS timestamp)").alias(
            "ts"
        ),
        F.lit("grid_operator_change").alias("kind"),
    )
    events = union_all([unit, actor, change])
    rows = (
        events.groupBy("kind", F.substring("entity_id", 1, 3).alias("prefix"))
        .agg(
            F.count("*").alias("rows"),
            F.min("ts").alias("min_ts"),
            F.max("ts").alias("max_ts"),
        )
        .orderBy("kind", "prefix")
        .collect()
    )
    print_rows(
        ["kind", "id_prefix", "rows", "min_ts", "max_ts"],
        [(r["kind"], r["prefix"], r["rows"], r["min_ts"], r["max_ts"]) for r in rows],
    )
    overlap = unit.select("entity_id").intersect(actor.select("entity_id")).count()
    types = {
        "unit_deletion.last_updated_at": dict(
            tbl(SILVER, "mastr_unit_deletion_events").dtypes
        ).get("last_updated_at"),
        "grid_change.effective_date": dict(
            tbl(SILVER, "mastr_grid_operator_change_events").dtypes
        ).get("grid_operator_change_effective_date"),
    }
    print("time column types:", types)
    verdict = "SUPPORTS" if overlap == 0 else "REJECTS"
    return verdict, f"unit_actor_id_overlap={overlap}"


# COMMAND ----------

# DBTITLE 1,Run V11
run_check(
    "V11",
    ["events_silver", "events_gold"],
    "MaStR change-event id namespaces and time types",
    check_change_events,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V12 -- Commerce: identity overlap between GA4 and REES46

# COMMAND ----------

# DBTITLE 1,Def -- check_commerce_identity


def check_commerce_identity():
    ga_sessions = tbl(C_GOLD, "fact_customer_session_ga4")
    re_sessions = tbl(C_GOLD, "fact_customer_session_rees46")
    user_overlap = (
        ga_sessions.select(F.col("user_pseudo_id").cast("string").alias("v"))
        .distinct()
        .intersect(
            re_sessions.select(F.col("user_id").cast("string").alias("v")).distinct()
        )
        .count()
    )
    session_overlap = (
        ga_sessions.select(F.col("session_id").cast("string").alias("v"))
        .distinct()
        .intersect(
            re_sessions.select(
                F.col("user_session").cast("string").alias("v")
            ).distinct()
        )
        .count()
    )
    ga_products = tbl(C_GOLD, "dim_product_ga4").select(
        F.col("item_id").cast("string").alias("v")
    )
    re_products = tbl(C_GOLD, "dim_product_rees46").select(
        F.col("product_id").cast("string").alias("v")
    )
    product_overlap = ga_products.intersect(re_products).count()
    ga_names = tbl(C_SILVER, "ga4_events").groupBy("event_name").count()
    re_types = tbl(C_SILVER, "rees46_events").groupBy("event_type").count()
    print("GA4 event_name (top 12):")
    print_rows(
        ["event_name", "rows"],
        [
            (r["event_name"], r["count"])
            for r in ga_names.orderBy(F.desc("count")).limit(12).collect()
        ],
    )
    print("REES46 event_type:")
    print_rows(
        ["event_type", "rows"],
        [(r["event_type"], r["count"]) for r in re_types.collect()],
    )
    clean = user_overlap == 0 and session_overlap == 0 and product_overlap == 0
    verdict = "SUPPORTS" if clean else "REVIEW"
    return verdict, (
        f"user_id_overlap={user_overlap}, session_id_overlap={session_overlap}, "
        f"product_id_overlap={product_overlap}"
    )


# COMMAND ----------

# DBTITLE 1,Run V12
run_check(
    "V12",
    ["commerce_silver", "commerce_gold"],
    "Commerce GA4 and REES46 identity overlap",
    check_commerce_identity,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V13 -- DWD reference: key uniqueness and window alignment

# COMMAND ----------

# DBTITLE 1,Def -- check_dwd_reference


def check_dwd_reference():
    geo = tbl(SILVER_REF, "dwd_station_geography")
    names = tbl(SILVER_REF, "dwd_station_name_history")
    device = tbl(SILVER_REF, "dwd_device_instrument")
    unit = tbl(SILVER_REF, "dwd_parameter_unit")
    gaps = tbl(SILVER_REF, "dwd_missing_value_periods")
    dup = {
        "geography": dupes(geo, ["station_id", "valid_from"]),
        "name_history": dupes(names, ["station_id", "valid_from"]),
        "device_instrument": dupes(
            device, ["station_id", "parameter_category", "valid_from"]
        ),
        "parameter_unit": dupes(
            unit, ["station_id", "parameter_source_code", "valid_from"]
        ),
        "missing_value_periods": dupes(
            gaps, ["station_id", "parameter_source_code", "gap_start_ts"]
        ),
    }
    print("duplicate keys per table:", dup)
    geo_keys = geo.select("station_id", "valid_from")
    name_keys = names.select("station_id", "valid_from")
    only_geo = geo_keys.exceptAll(name_keys).count()
    only_name = name_keys.exceptAll(geo_keys).count()
    print(f"intervals only in geography: {only_geo}, only in name_history: {only_name}")
    types = {
        "valid_from": dict(geo.dtypes).get("valid_from"),
        "gap_start_ts": dict(gaps.dtypes).get("gap_start_ts"),
    }
    print("period column types:", types)
    verdict = "SUPPORTS" if sum(dup.values()) == 0 else "REJECTS"
    return verdict, (
        f"duplicate_keys={sum(dup.values())}, geography_only={only_geo}, "
        f"name_history_only={only_name}, period_types={types}"
    )


# COMMAND ----------

# DBTITLE 1,Run V13
run_check(
    "V13",
    ["dwd_ref_silver", "dwd_ref_gold"],
    "DWD reference key uniqueness and window alignment",
    check_dwd_reference,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V14 -- Bronze: identical-schema groups

# COMMAND ----------

# DBTITLE 1,Def -- check_bronze_schemas


def check_bronze_schemas():
    pair_equal = {}
    for c in ("electricity", "heating", "cooling"):
        p = tbl(BRONZE, f"honda_iot_{c}_p").schema
        w = tbl(BRONZE, f"honda_iot_{c}_w").schema
        pair_equal[c] = p == w
    lookups = {tbl(BRONZE, f"mastr_{n}").schema.simpleString() for n in SMALL_LISTS}
    print("honda p/w schema equal:", pair_equal)
    print("distinct schemas among the four MaStR lookups:", len(lookups))
    ok = all(pair_equal.values()) and len(lookups) == 1
    return ("SUPPORTS" if ok else "REJECTS"), (
        f"honda_pairs_equal={sum(pair_equal.values())}/3, lookup_distinct_schemas={len(lookups)}"
    )


# COMMAND ----------

# DBTITLE 1,Run V14
run_check(
    "V14",
    ["bronze_honda_pairs", "bronze_mastr_lookups"],
    "Bronze identical-schema groups",
    check_bronze_schemas,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V15 -- source_record_id uniqueness (within and across tables)

# COMMAND ----------

# DBTITLE 1,Def -- check_provenance


def check_provenance():
    verdicts = []
    parts = []
    for label, (schema, names, typed) in PROVENANCE_GROUPS.items():
        ids = union_all(
            [
                tbl(schema, n).select("source_record_id").withColumn("t", F.lit(n))
                for n in names
            ]
        )
        within = (
            ids.groupBy("t", "source_record_id").count().filter("count > 1").count()
        )
        across = 0
        if typed:
            across = (
                ids.groupBy("source_record_id")
                .agg(F.countDistinct("t").alias("n"))
                .filter("n > 1")
                .count()
            )
        print(
            f"{label}: duplicates_within_table={within}, shared_across_tables={across}"
        )
        if within > 0:
            verdicts.append("REJECTS")
        elif across > 0:
            verdicts.append("REVIEW")
        else:
            verdicts.append("SUPPORTS")
        parts.append(f"{label}(within={within}, across={across})")
    return worst_verdict(verdicts), ", ".join(parts)


# COMMAND ----------

# DBTITLE 1,Run V15
run_check(
    "V15",
    [
        "dwd_hourly_gold",
        "dwd_hourly_silver",
        "honda_silver",
        "bridges_silver",
        "codes_silver",
    ],
    "source_record_id uniqueness",
    check_provenance,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V16 -- Silver to Gold: pass-through content comparison

# COMMAND ----------

# DBTITLE 1,Def -- check_pass_through


def check_pass_through(pair):
    s_schema, s_name, g_schema, g_name = pair
    silver = tbl(s_schema, s_name)
    gold = tbl(g_schema, g_name)
    shared = [c for c in silver.columns if c in gold.columns]
    cols = [F.col(f"`{c}`") for c in shared]
    hash_sum = F.sum(F.xxhash64(*cols).cast("decimal(38,0)")).alias("h")
    s_stats = silver.agg(F.count("*").alias("n"), hash_sum).first()
    g_stats = gold.agg(F.count("*").alias("n"), hash_sum).first()
    added = [c for c in gold.columns if c not in silver.columns]
    dropped = [c for c in silver.columns if c not in gold.columns]
    extra = [c for c in added if not (c.endswith("_key") or c.startswith("_gold_"))]
    same = s_stats["n"] == g_stats["n"] and s_stats["h"] == g_stats["h"]
    print(
        f"{g_name}: rows {s_stats['n']:,} vs {g_stats['n']:,}, shared_hash_equal={same}"
    )
    print(f"    added={added}, dropped={dropped}")
    if not same:
        verdict = "REJECTS"
    elif extra:
        verdict = "REVIEW"
    else:
        verdict = "SUPPORTS"
    return verdict, (
        f"rows={s_stats['n']:,}/{g_stats['n']:,}, hash_equal={same}, "
        f"business_columns_added={extra or 'none'}"
    )


# COMMAND ----------

# DBTITLE 1,Run V16 -- pass-through pairs
for _pair in PASS_THROUGH_PAIRS + (HEAVY_PAIRS if RUN_HEAVY else []):
    run_check(
        f"V16:{_pair[3]}",
        ["gold_duplication"],
        f"{_pair[3]} vs {_pair[1]}",
        lambda pair=_pair: check_pass_through(pair),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## V17 -- Silver to Gold: PIT enrichment can be a view

# COMMAND ----------

# DBTITLE 1,Def -- check_pit_enrichment


def check_pit_enrichment():
    silver = tbl(SILVER, "dwd_air_temperature")
    gold = tbl(GOLD, "fact_weather_air_temperature")
    s_rows = silver.count()
    g_rows = gold.count()
    stats = gold.agg(
        F.avg(F.col("weather_station_key").isNull().cast("int")).alias("null_share"),
        F.countDistinct("weather_station_key").alias("station_keys"),
    ).first()
    dup_keys = dupes(gold, ["STATIONS_ID", "observation_ts"])
    added = [c for c in gold.columns if c not in silver.columns]
    print(f"silver_rows={s_rows:,}, gold_rows={g_rows:,}, added_columns={added}")
    null_share = stats["null_share"] or 0.0
    ok = s_rows == g_rows and dup_keys == 0 and null_share <= 0.01
    return ("SUPPORTS" if ok else "REVIEW"), (
        f"rows_equal={s_rows == g_rows}, dup_keys={dup_keys}, "
        f"null_station_key_share={null_share:.4%}, distinct_station_keys={stats['station_keys']}"
    )


# COMMAND ----------

# DBTITLE 1,Run V17
run_check(
    "V17",
    ["gold_duplication", "dwd_hourly_gold"],
    "PIT-enriched weather fact adds only keys",
    check_pit_enrichment,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V18 -- Coordinate conflict folds into dim_location

# COMMAND ----------

# DBTITLE 1,Def -- check_coord_conflict


def check_coord_conflict():
    conflict = tbl(GOLD, "fact_location_coordinate_conflict")
    location = tbl(GOLD, "dim_location")
    c_rows = conflict.count()
    c_ids = conflict.select("location_id").distinct().count()
    l_rows = location.count()
    l_ids = location.select("location_id").distinct().count()
    orphans = (
        conflict.select("location_id")
        .distinct()
        .join(location.select("location_id").distinct(), "location_id", "left_anti")
        .count()
    )
    flags = conflict.groupBy("_coordinate_conflict").count().collect()
    print(
        "conflict flag counts:", {r["_coordinate_conflict"]: r["count"] for r in flags}
    )
    ok = c_rows == c_ids and l_rows == l_ids and orphans == 0
    return ("SUPPORTS" if ok else "REJECTS"), (
        f"conflict_rows={c_rows:,} (distinct {c_ids:,}), dim_location_rows={l_rows:,} "
        f"(distinct {l_ids:,}), conflict_ids_not_in_dim_location={orphans}"
    )


# COMMAND ----------

# DBTITLE 1,Run V18
run_check(
    "V18",
    ["coord_conflict"],
    "Coordinate-conflict grain matches dim_location",
    check_coord_conflict,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V19 -- Quality: watermarks vs audit log overlap

# COMMAND ----------

# DBTITLE 1,Def -- check_quality_overlap


def check_quality_overlap():
    watermarks = tbl(QUALITY, "pipeline_watermarks").select(
        "run_id", "component", "rows_written"
    )
    audit = (
        tbl(QUALITY, "quality_audit_log")
        .filter("metric_name = 'rows_written'")
        .select("run_id", "component", F.col("metric_value").alias("audit_rows"))
    )
    stats = (
        watermarks.join(audit, ["run_id", "component"], "full")
        .agg(
            F.count("*").alias("pairs"),
            F.sum(F.col("rows_written").isNull().cast("int")).alias("audit_only"),
            F.sum(F.col("audit_rows").isNull().cast("int")).alias("watermark_only"),
            F.sum((F.col("rows_written") != F.col("audit_rows")).cast("int")).alias(
                "mismatch"
            ),
        )
        .first()
    )
    print(
        f"pairs={stats['pairs']:,}, audit_only={stats['audit_only']}, "
        f"watermark_only={stats['watermark_only']}, value_mismatch={stats['mismatch']}"
    )
    ok = (stats["watermark_only"] or 0) == 0 and (stats["mismatch"] or 0) == 0
    return ("SUPPORTS" if ok else "REVIEW"), (
        f"pairs={stats['pairs']:,}, watermark_only={stats['watermark_only']}, "
        f"value_mismatch={stats['mismatch']}"
    )


# COMMAND ----------

# DBTITLE 1,Run V19
run_check(
    "V19",
    ["quality_log"],
    "Watermarks duplicate the audit-log rows_written metric",
    check_quality_overlap,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# DBTITLE 1,Def -- print_summary


def print_summary():
    print("\n" + "=" * 100)
    print("SUMMARY")
    print("=" * 100)
    for r in RESULTS:
        print(f"[{r['verdict']:<8}] {r['id']:<40} {r['question']}")
        print(f"             {r['metrics'][:220]}")
    counts = {}
    for r in RESULTS:
        counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1
    print("\nverdict counts:", {v: counts.get(v, 0) for v in VERDICT_ORDER})
    by_group = {}
    for r in RESULTS:
        for g in r["groups"]:
            by_group.setdefault(g, []).append(r["verdict"])
    print("\nper-group verdict (worst check wins):")
    print_rows(
        ["group", "verdict", "checks"],
        [(g, worst_verdict(v), len(v)) for g, v in sorted(by_group.items())],
    )
    print("\nSUPPORTS = evidence favours consolidation; REVIEW = needs a design")
    print("decision (e.g. discriminator in key); REJECTS = evidence against;")
    print("ERROR = check failed to run (see message above).")


# COMMAND ----------

# DBTITLE 1,Run summary
print_summary()
