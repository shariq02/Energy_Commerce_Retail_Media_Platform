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
# MAGIC sparsity, provenance, Silver/Gold duplication. Output goes to
# MAGIC `src/schemas/consolidation_findings/table_verification.md` (summary and
# MAGIC per-group verdicts first, then one section per check).
# MAGIC
# MAGIC Run all cells in order. Tables are only read; the findings file is the
# MAGIC only write.

# COMMAND ----------

# DBTITLE 1,Imports
import os
from functools import reduce

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Config

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

FINDINGS_DIR = ("src", "schemas", "consolidation_findings")
FINDINGS_FILE = "table_verification.md"

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

# (schema, tables, typed, roll-up groups): typed groups must be unique across tables.
PROVENANCE_GROUPS = {
    "dwd_hourly": (
        SILVER,
        [f"dwd_{p}" for p in DWD_HOURLY],
        False,
        ["dwd_hourly_gold", "dwd_hourly_silver"],
    ),
    "honda": (SILVER, HONDA_SILVER, False, ["honda_silver", "honda_gold"]),
    "bridges": (
        SILVER,
        list(BRIDGES.values()),
        True,
        ["bridges_silver", "bridges_gold"],
    ),
    "catalogs": (
        SILVER_REF,
        [f"mastr_{n}" for n, _ in CATALOGS],
        True,
        ["codes_silver", "codes_gold"],
    ),
}

# (silver schema, silver table, gold schema, gold table)
PASS_THROUGH_PAIRS = [
    (SILVER, "honda_electricity_w", GOLD, "fact_site_electricity_w"),
    (SILVER, "honda_heating_p", GOLD, "fact_site_heating_p"),
    (SILVER, "honda_heating_w", GOLD, "fact_site_heating_w"),
    (SILVER, "honda_cooling_p", GOLD, "fact_site_cooling_p"),
    (SILVER, "honda_cooling_w", GOLD, "fact_site_cooling_w"),
    (SILVER_REF, "mastr_marktfunktionen", GOLD, "dim_marktfunktion"),
    (SILVER_REF, "mastr_marktrollen", GOLD, "dim_marktrolle"),
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
]
HEAVY_PAIRS = [
    (C_SILVER, "ga4_events", C_GOLD, "fact_web_event"),
    (C_SILVER, "rees46_events", C_GOLD, "fact_customer_activity_event"),
]

RESULTS = []
CURRENT = []

# COMMAND ----------

# DBTITLE 1,Helpers


def tbl(schema, name):
    return spark.table(f"{schema}.{name}")


def union_all(dfs, allow_missing=False):
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=allow_missing), dfs)


def dupes(df, keys):
    return df.groupBy(*keys).count().filter("count > 1").count()


def worst_verdict(verdicts):
    return min(verdicts, key=VERDICT_ORDER.index)


def cell(value):
    return str(value).replace("|", "\\|").replace("\n", " ")


def md_table(header, rows):
    lines = ["| " + " | ".join(cell(h) for h in header) + " |"]
    lines.append("|" + "---|" * len(header))
    lines += ["| " + " | ".join(cell(v) for v in r) + " |" for r in rows]
    return lines


def note(*parts):
    CURRENT.append("- " + " ".join(str(x) for x in parts))


def table(header, rows):
    CURRENT.extend(["", *md_table(header, rows), ""])


def record(check_id, groups, question, verdict, metrics, detail):
    RESULTS.append(
        {
            "id": check_id,
            "groups": groups,
            "question": question,
            "verdict": verdict,
            "metrics": metrics,
            "detail": detail,
        }
    )


def run_check(check_id, groups, question, fn):
    CURRENT.clear()
    try:
        verdict, metrics = fn()
    except Exception as exc:
        verdict, metrics = "ERROR", f"{type(exc).__name__}: {str(exc)[:240]}"
    record(check_id, groups, question, verdict, metrics, list(CURRENT))


def repo_root():
    p = os.path.abspath(os.getcwd())
    for _ in range(12):
        if os.path.isdir(os.path.join(p, "src", "schemas")) and os.path.isdir(
            os.path.join(p, "databricks")
        ):
            return p
        if os.path.dirname(p) == p:
            break
        p = os.path.dirname(p)
    try:
        nb = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
    except Exception as exc:
        raise RuntimeError(
            "repo root not found -- run from inside the repo's Databricks Git folder"
        ) from exc
    i = nb.rfind("/databricks/")
    if i > 0:
        for cand in (nb[:i], "/Workspace" + nb[:i]):
            if os.path.isdir(os.path.join(cand, "src", "schemas")):
                return cand
    raise RuntimeError(
        "repo root not found -- run from inside the repo's Databricks Git folder"
    )


# COMMAND ----------

# DBTITLE 1,V01 -- DWD hourly: (station, hour) key overlap


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
    hist = (
        per_key.withColumn(
            "decade", (F.floor(F.year("observation_ts") / 10) * 10).cast("int")
        )
        .groupBy("n_rows", "n_products", "decade")
        .count()
        .collect()
    )
    union_keys = sum(r["count"] for r in hist)
    dup_keys = sum(r["count"] for r in hist if r["n_rows"] > r["n_products"])
    avg_products = sum(r["n_products"] * r["count"] for r in hist) / union_keys
    by_n, by_decade = {}, {}
    for r in hist:
        by_n[r["n_products"]] = by_n.get(r["n_products"], 0) + r["count"]
        d = by_decade.setdefault(r["decade"], [0, 0, 0])
        d[0] += r["count"]
        d[1] += r["n_products"] * r["count"]
        d[2] += r["count"] if r["n_products"] == len(DWD_HOURLY) else 0
    note("keys by number of products present:")
    table(["n_products", "keys"], sorted(by_n.items()))
    note("density by decade:")
    table(
        ["decade", "keys", "avg_products_per_key", "share_with_all_products"],
        [
            (k, v[0], f"{v[1] / v[0]:.2f}", f"{v[2] / v[0]:.1%}")
            for k, v in sorted(by_decade.items())
        ],
    )
    per_product = keys.groupBy("p").count().orderBy("count").collect()
    note("rows per product:")
    table(
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


run_check(
    "V01",
    ["dwd_hourly_gold", "dwd_hourly_silver"],
    "DWD hourly key overlap and duplicates",
    check_dwd_overlap,
)

# COMMAND ----------

# DBTITLE 1,V02 -- DWD hourly: column-name and type collisions


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
    note("shared non-key columns (need a product prefix):")
    table(
        ["column", "n_products", "types"],
        [(c, len(shared[c]), sorted(set(shared[c].values()))) for c in needs_prefix],
    )
    note("type conflicts:", conflicts or "none")
    verdict = "REJECTS" if conflicts else "SUPPORTS"
    return verdict, (
        f"shared_columns={len(shared)}, need_prefix={len(needs_prefix)}, "
        f"type_conflicts={len(conflicts)}"
    )


run_check(
    "V02",
    ["dwd_hourly_gold", "dwd_hourly_silver"],
    "DWD hourly column and type collisions",
    check_dwd_columns,
)

# COMMAND ----------

# DBTITLE 1,V03 -- DWD: timestamp grid, range, stations (solar included)


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
    table(
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


run_check(
    "V03",
    ["dwd_hourly_gold", "dwd_hourly_silver"],
    "DWD timestamp grid and solar separation",
    check_dwd_timestamps,
)

# COMMAND ----------

# DBTITLE 1,V04 -- Honda: key coverage across the seven Silver tables


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
        F.min("t").alias("first_t"),
    )
    hist = (
        per_key.withColumn("only_in", F.when(F.col("n_tables") == 1, F.col("first_t")))
        .groupBy("frequency", "n_tables", "n_rows", "only_in")
        .count()
        .collect()
    )
    total = sum(r["count"] for r in hist)
    full = sum(r["count"] for r in hist if r["n_tables"] == len(HONDA_SILVER))
    dup_keys = sum(r["count"] for r in hist if r["n_rows"] > r["n_tables"])
    by, single = {}, {}
    for r in hist:
        k = (r["frequency"], r["n_tables"])
        by[k] = by.get(k, 0) + r["count"]
        if r["n_tables"] == 1:
            k1 = (r["frequency"], r["only_in"])
            single[k1] = single.get(k1, 0) + r["count"]
    note("keys by frequency and number of tables present:")
    table(["frequency", "n_tables", "keys"], sorted((*k, v) for k, v in by.items()))
    note("keys present in exactly one table:")
    table(
        ["frequency", "only_in_table", "keys"],
        sorted((*k, v) for k, v in single.items()),
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


run_check(
    "V04",
    ["honda_silver", "honda_gold"],
    "Honda key coverage across tables",
    check_honda_keys,
)

# COMMAND ----------

# DBTITLE 1,V05 -- Honda: channel catalog vs Silver value columns


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
    note("in Silver, not in catalog:", only_silver or "none")
    note("in catalog, not in Silver:", only_catalog or "none")
    verdict = "SUPPORTS" if not only_silver and not only_catalog else "REJECTS"
    return verdict, (
        f"catalog_channels={len(expected)}, silver_value_columns={len(actual)}, "
        f"only_silver={only_silver or 'none'}, only_catalog={only_catalog or 'none'}"
    )


run_check(
    "V05",
    ["honda_silver", "honda_gold"],
    "Honda channel catalog matches Silver columns",
    check_honda_catalog,
)

# COMMAND ----------

# DBTITLE 1,V06 -- MaStR units: column population per technology


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
    stats, empty_common = [], {}
    for r in rows:
        n = r["__rows"] or 1
        pop = {c: (r[c] or 0) / n for c in cols}
        empty = sorted(c for c in common if pop[c] == 0)
        if empty:
            empty_common[r["unit_type"]] = empty
        stats.append(
            (
                r["unit_type"],
                r["__rows"],
                sum(v > 0 for v in pop.values()),
                sum(v > 0.5 for v in pop.values()),
                f"{len(common) - len(empty)}/{len(common)}",
            )
        )
    table(
        ["unit_type", "rows", "cols_any", "cols_over_50pct", "common_populated"],
        sorted(stats),
    )
    note("common columns entirely empty for a technology:", empty_common or "none")
    min_share = min(s[2] for s in stats) / len(cols)
    verdict = "SUPPORTS" if min_share >= 0.3 else "REVIEW"
    return verdict, (
        f"union_columns={len(cols)}, common_columns={len(common)}, "
        f"min_populated_column_share={min_share:.0%}, "
        f"technologies_with_empty_common_columns={len(empty_common)}"
    )


run_check(
    "V06",
    ["mastr_units"],
    "MaStR unit superset sparsity",
    check_unit_sparsity,
)

# COMMAND ----------

# DBTITLE 1,V07 -- MaStR units: unit_id collisions across technologies


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
    note("unit_id prefix per technology:")
    table(
        ["prefix", "technology", "rows"],
        [(r["prefix"], r["t"], r["count"]) for r in prefixes],
    )
    verdict = "SUPPORTS" if colliding == 0 else "REJECTS"
    return verdict, f"colliding_unit_ids={colliding}"


run_check(
    "V07",
    ["mastr_units"],
    "MaStR unit_id uniqueness across technologies",
    check_unit_ids,
)

# COMMAND ----------

# DBTITLE 1,V08 -- MaStR EEG / KWK: identifier safety


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
    note("EEG id prefixes:", {r["p"]: r["count"] for r in eeg_prefix})
    note("KWK id prefixes:", {r["p"]: r["count"] for r in kwk_prefix})
    if eeg_dupes > 0:
        verdict = "REJECTS"
    elif overlap > 0:
        verdict = "REVIEW"
    else:
        verdict = "SUPPORTS"
    return verdict, f"eeg_duplicated_ids={eeg_dupes}, eeg_kwk_id_overlap={overlap}"


run_check(
    "V08",
    ["mastr_support"],
    "EEG and KWK identifier safety",
    check_eeg_kwk,
)

# COMMAND ----------

# DBTITLE 1,V09 -- MaStR bridges: typed-key collision safety


def id_shape(col):
    return (
        F.when(F.col(col).rlike("^[0-9]+$"), F.lit("<numeric>"))
        .when(F.col(col).rlike("^[A-Z]{3}[0-9]"), F.substring(col, 1, 3))
        .otherwise(F.lit("<text>"))
    )


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
    table(
        ["relationship", "pairs", "parents", "linked"],
        [(r["rel"], r["pairs"], r["parents"], r["linked"]) for r in per_rel],
    )
    shapes = (
        pairs.groupBy(
            "rel",
            id_shape("parent_id").alias("parent_shape"),
            id_shape("linked_id").alias("linked_shape"),
        )
        .count()
        .orderBy("rel", F.desc("count"))
        .collect()
    )
    note("id shape per relationship (MaStR number prefix, <numeric> or <text>):")
    table(
        ["relationship", "parent_shape", "linked_shape", "pairs"],
        [(r["rel"], r["parent_shape"], r["linked_shape"], r["count"]) for r in shapes],
    )
    verdict = "SUPPORTS" if shared_pairs == 0 and shared_ids == 0 else "REVIEW"
    return verdict, (
        f"pairs_under_multiple_relationships={shared_pairs}, "
        f"source_record_id_shared_across_relationships={shared_ids}"
    )


run_check(
    "V09",
    ["bridges_silver", "bridges_gold"],
    "MaStR bridge pair and record-id collisions",
    check_bridges,
)

# COMMAND ----------

# DBTITLE 1,V10 -- MaStR code lists: Id collisions and overlap


def catalog_rows(name, value_col):
    return tbl(SILVER_REF, f"mastr_{name}").select(
        F.col("Id").cast("string").alias("Id"),
        F.col(value_col).cast("string").alias("Wert"),
        F.lit(name).alias("kind"),
    )


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
    note("small lists already present in katalogwerte (Id, Wert):")
    table(["list", "rows", "also_in_katalogwerte"], rows)
    contained = [r[0] for r in rows if r[1] > 0 and r[1] == r[2]]
    verdict = "REVIEW" if contained else "SUPPORTS"
    return verdict, (
        f"ids_reused_across_catalogs={reused} of {total_ids}, "
        f"lists_fully_inside_katalogwerte={contained or 'none'}"
    )


run_check(
    "V10",
    ["bronze_mastr_lookups", "codes_silver", "codes_gold"],
    "MaStR code-list Id collisions and containment",
    check_code_lists,
)

# COMMAND ----------

# DBTITLE 1,V11 -- MaStR change events: identifier and time safety


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
            F.avg(F.col("ts").isNull().cast("int")).alias("null_ts"),
            F.min("ts").alias("min_ts"),
            F.max("ts").alias("max_ts"),
        )
        .orderBy("kind", "prefix")
        .collect()
    )
    table(
        ["kind", "id_prefix", "rows", "null_ts_share", "min_ts", "max_ts"],
        [
            (
                r["kind"],
                r["prefix"],
                r["rows"],
                f"{r['null_ts']:.2%}",
                r["min_ts"],
                r["max_ts"],
            )
            for r in rows
        ],
    )
    totals, nulls = {}, {}
    for r in rows:
        totals[r["kind"]] = totals.get(r["kind"], 0) + r["rows"]
        nulls[r["kind"]] = nulls.get(r["kind"], 0) + r["null_ts"] * r["rows"]
    null_share = {k: nulls[k] / totals[k] for k in totals}
    overlap = unit.select("entity_id").intersect(actor.select("entity_id")).count()
    note(
        "time column types:",
        {
            "unit_deletion.last_updated_at": dict(
                tbl(SILVER, "mastr_unit_deletion_events").dtypes
            ).get("last_updated_at"),
            "grid_change.effective_date": dict(
                tbl(SILVER, "mastr_grid_operator_change_events").dtypes
            ).get("grid_operator_change_effective_date"),
        },
    )
    if overlap > 0:
        verdict = "REJECTS"
    elif any(v > 0.9 for v in null_share.values()):
        verdict = "REVIEW"
    else:
        verdict = "SUPPORTS"
    return verdict, (
        f"unit_actor_id_overlap={overlap}, "
        f"null_timestamp_share={ {k: f'{v:.2%}' for k, v in null_share.items()} }"
    )


run_check(
    "V11",
    ["events_silver", "events_gold"],
    "MaStR change-event id namespaces and time types",
    check_change_events,
)

# COMMAND ----------

# DBTITLE 1,V12 -- Commerce: identity overlap between GA4 and REES46


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
    ga_events = tbl(C_SILVER, "ga4_events")
    re_events = tbl(C_SILVER, "rees46_events")
    ga_rows, re_rows = ga_events.count(), re_events.count()
    note(f"event rows: GA4={ga_rows:,}, REES46={re_rows:,}")
    ga_names = ga_events.groupBy("event_name").count()
    re_types = re_events.groupBy("event_type").count()
    note("GA4 event_name (top 12):")
    table(
        ["event_name", "rows"],
        [
            (r["event_name"], r["count"])
            for r in ga_names.orderBy(F.desc("count")).limit(12).collect()
        ],
    )
    note("REES46 event_type:")
    table(
        ["event_type", "rows"],
        [(r["event_type"], r["count"]) for r in re_types.collect()],
    )
    clean = user_overlap == 0 and session_overlap == 0 and product_overlap == 0
    verdict = "SUPPORTS" if clean else "REVIEW"
    return verdict, (
        f"user_id_overlap={user_overlap}, session_id_overlap={session_overlap}, "
        f"product_id_overlap={product_overlap}, "
        f"ga4_events={ga_rows:,}, rees46_events={re_rows:,}"
    )


run_check(
    "V12",
    ["commerce_silver", "commerce_gold"],
    "Commerce GA4 and REES46 identity overlap",
    check_commerce_identity,
)

# COMMAND ----------

# DBTITLE 1,V13 -- DWD reference: key uniqueness and window alignment


def check_dwd_reference():
    tables = {
        "geography": (
            tbl(SILVER_REF, "dwd_station_geography"),
            ["station_id", "valid_from"],
        ),
        "name_history": (
            tbl(SILVER_REF, "dwd_station_name_history"),
            ["station_id", "valid_from"],
        ),
        "device_instrument": (
            tbl(SILVER_REF, "dwd_device_instrument"),
            ["station_id", "parameter_category", "valid_from"],
        ),
        "parameter_unit": (
            tbl(SILVER_REF, "dwd_parameter_unit"),
            ["station_id", "parameter_source_code", "valid_from"],
        ),
        "missing_value_periods": (
            tbl(SILVER_REF, "dwd_missing_value_periods"),
            ["station_id", "parameter_source_code", "gap_start_ts"],
        ),
    }
    natural = {n: dupes(df, keys) for n, (df, keys) in tables.items()}
    ordinal = {n: dupes(df, [*keys, "_src_id_ord"]) for n, (df, keys) in tables.items()}
    note("duplicate keys on the natural key:", natural)
    note("duplicate keys including _src_id_ord:", ordinal)
    geo_keys = tables["geography"][0].select("station_id", "valid_from")
    name_keys = tables["name_history"][0].select("station_id", "valid_from")
    only_geo = geo_keys.exceptAll(name_keys).count()
    only_name = name_keys.exceptAll(geo_keys).count()
    note(f"intervals only in geography: {only_geo}, only in name_history: {only_name}")
    types = {
        "valid_from": dict(tables["geography"][0].dtypes).get("valid_from"),
        "gap_start_ts": dict(tables["missing_value_periods"][0].dtypes).get(
            "gap_start_ts"
        ),
    }
    note("period column types:", types)
    if sum(ordinal.values()) > 0:
        verdict = "REJECTS"
    elif sum(natural.values()) > 0:
        verdict = "REVIEW"
    else:
        verdict = "SUPPORTS"
    return verdict, (
        f"natural_key_dups={natural}, dups_with_src_id_ord={sum(ordinal.values())}, "
        f"geography_only={only_geo}, name_history_only={only_name}, period_types={types}"
    )


run_check(
    "V13",
    ["dwd_ref_silver", "dwd_ref_gold"],
    "DWD reference key uniqueness and window alignment",
    check_dwd_reference,
)

# COMMAND ----------

# DBTITLE 1,V14 -- Bronze: identical-schema groups


def check_bronze_schemas():
    pair_equal = {}
    for c in ("electricity", "heating", "cooling"):
        p = tbl(BRONZE, f"honda_iot_{c}_p").schema
        w = tbl(BRONZE, f"honda_iot_{c}_w").schema
        pair_equal[c] = p == w
    lookups = {tbl(BRONZE, f"mastr_{n}").schema.simpleString() for n in SMALL_LISTS}
    note("honda p/w schema equal:", pair_equal)
    note("distinct schemas among the four MaStR lookups:", len(lookups))
    ok = all(pair_equal.values()) and len(lookups) == 1
    return ("SUPPORTS" if ok else "REJECTS"), (
        f"honda_pairs_equal={sum(pair_equal.values())}/3, lookup_distinct_schemas={len(lookups)}"
    )


run_check(
    "V14",
    ["bronze_honda_pairs", "bronze_mastr_lookups"],
    "Bronze identical-schema groups",
    check_bronze_schemas,
)

# COMMAND ----------

# DBTITLE 1,V15 -- source_record_id uniqueness (within and across tables)


def check_provenance(schema, names, typed):
    ids = union_all(
        [
            tbl(schema, n).select("source_record_id").withColumn("t", F.lit(n))
            for n in names
        ]
    )
    within = ids.groupBy("t", "source_record_id").count().filter("count > 1").count()
    across = 0
    if typed:
        across = (
            ids.groupBy("source_record_id")
            .agg(F.countDistinct("t").alias("n"))
            .filter("n > 1")
            .count()
        )
    if within > 0:
        verdict = "REJECTS"
    elif across > 0:
        verdict = "REVIEW"
    else:
        verdict = "SUPPORTS"
    return verdict, f"duplicates_within_table={within}, shared_across_tables={across}"


for _label, (_schema, _names, _typed, _groups) in PROVENANCE_GROUPS.items():
    run_check(
        f"V15:{_label}",
        _groups,
        f"source_record_id uniqueness ({_label})",
        lambda s=_schema, n=_names, t=_typed: check_provenance(s, n, t),
    )

# COMMAND ----------

# DBTITLE 1,V16 -- Silver to Gold: pass-through content comparison


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
    renamed = bool(extra) and len(extra) == len(dropped)
    same = s_stats["n"] == g_stats["n"] and s_stats["h"] == g_stats["h"]
    note(
        f"{g_name}: rows {s_stats['n']:,} vs {g_stats['n']:,}, shared_hash_equal={same}"
    )
    note(f"added={added}, dropped={dropped}")
    if not same:
        verdict = "REJECTS"
    elif extra and not renamed:
        verdict = "REVIEW"
    else:
        verdict = "SUPPORTS"
    return verdict, (
        f"rows={s_stats['n']:,}/{g_stats['n']:,}, hash_equal={same}, "
        f"business_columns_added={extra or 'none'}, renamed_only={renamed}"
    )


for _pair in PASS_THROUGH_PAIRS + (HEAVY_PAIRS if RUN_HEAVY else []):
    run_check(
        f"V16:{_pair[3]}",
        ["gold_duplication"],
        f"{_pair[3]} vs {_pair[1]}",
        lambda pair=_pair: check_pass_through(pair),
    )

# COMMAND ----------

# DBTITLE 1,V17 -- Silver to Gold: PIT enrichment can be a view


def check_pit_enrichment():
    allowed = {"valid_from", "valid_to", "weather_station_key"}
    rows, bad = [], []
    for p in [*DWD_HOURLY, "solar"]:
        silver = tbl(SILVER, f"dwd_{p}")
        gold = tbl(GOLD, f"fact_weather_{p}")
        s_rows = silver.count()
        g = gold.agg(
            F.count("*").alias("n"),
            F.avg(F.col("weather_station_key").isNull().cast("int")).alias("nulls"),
            F.countDistinct("weather_station_key").alias("keys"),
        ).first()
        added = [c for c in gold.columns if c not in silver.columns]
        extra = [
            c
            for c in added
            if c not in allowed
            and not c.endswith("_key")
            and not c.startswith("_gold_")
        ]
        null_share = g["nulls"] or 0.0
        if s_rows != g["n"] or null_share > 0.01 or extra:
            bad.append(p)
        rows.append(
            (p, s_rows, g["n"], f"{null_share:.4%}", g["keys"], extra or "none")
        )
    table(
        [
            "product",
            "silver_rows",
            "gold_rows",
            "null_station_key",
            "station_keys",
            "extra_columns",
        ],
        rows,
    )
    dup_keys = dupes(
        tbl(GOLD, "fact_weather_air_temperature"), ["STATIONS_ID", "observation_ts"]
    )
    note("duplicate (station, ts) in fact_weather_air_temperature:", dup_keys)
    verdict = "SUPPORTS" if not bad and dup_keys == 0 else "REVIEW"
    return (
        verdict,
        f"products_failing={bad or 'none'}, dup_keys_air_temperature={dup_keys}",
    )


run_check(
    "V17",
    ["gold_duplication", "dwd_hourly_gold"],
    "PIT-enriched weather facts add only keys",
    check_pit_enrichment,
)

# COMMAND ----------

# DBTITLE 1,V18 -- Coordinate conflict folds into dim_location


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
    flagged = sum(r["count"] for r in flags if r["_coordinate_conflict"])
    without_row = 1 - c_rows / l_rows
    note(
        "conflict flag counts:", {r["_coordinate_conflict"]: r["count"] for r in flags}
    )
    note(f"dim_location rows without a conflict row: {without_row:.2%}")
    ok = c_rows == c_ids and l_rows == l_ids and orphans == 0
    return ("SUPPORTS" if ok else "REJECTS"), (
        f"conflict_rows={c_rows:,} (distinct {c_ids:,}), flagged={flagged:,}, "
        f"dim_location_rows={l_rows:,} (distinct {l_ids:,}), "
        f"dim_location_without_conflict_row={without_row:.2%}, "
        f"conflict_ids_not_in_dim_location={orphans}"
    )


run_check(
    "V18",
    ["coord_conflict"],
    "Coordinate-conflict grain matches dim_location",
    check_coord_conflict,
)

# COMMAND ----------

# DBTITLE 1,V19 -- Quality: watermarks vs audit log overlap


def check_quality_overlap():
    watermarks = (
        tbl(QUALITY, "pipeline_watermarks")
        .groupBy("run_id", "component")
        .agg(F.count("*").alias("w_rows"), F.sum("rows_written").alias("w_sum"))
    )
    audit = (
        tbl(QUALITY, "quality_audit_log")
        .filter("metric_name = 'rows_written'")
        .groupBy("run_id", "component")
        .agg(F.count("*").alias("a_rows"), F.sum("metric_value").alias("a_sum"))
    )
    s = (
        watermarks.join(audit, ["run_id", "component"], "full")
        .agg(
            F.count("*").alias("pairs"),
            F.sum(F.col("w_rows").isNull().cast("int")).alias("audit_only"),
            F.sum(F.col("a_rows").isNull().cast("int")).alias("watermark_only"),
            F.sum(((F.col("w_rows") > 1) | (F.col("a_rows") > 1)).cast("int")).alias(
                "multi_row"
            ),
            F.sum((F.col("w_sum") != F.col("a_sum")).cast("int")).alias("sum_mismatch"),
            F.sum(
                (
                    (F.col("w_rows") == 1)
                    & (F.col("a_rows") == 1)
                    & (F.col("w_sum") != F.col("a_sum"))
                ).cast("int")
            ).alias("single_row_mismatch"),
        )
        .first()
    )
    note(
        f"pairs={s['pairs']:,}, audit_only={s['audit_only']}, "
        f"watermark_only={s['watermark_only']}, multi_row_pairs={s['multi_row']}, "
        f"sum_mismatch={s['sum_mismatch']}, single_row_mismatch={s['single_row_mismatch']}"
    )
    ok = (s["watermark_only"] or 0) == 0 and (s["sum_mismatch"] or 0) == 0
    return ("SUPPORTS" if ok else "REVIEW"), (
        f"pairs={s['pairs']:,}, watermark_only={s['watermark_only']}, "
        f"sum_mismatch={s['sum_mismatch']}, single_row_mismatch={s['single_row_mismatch']}, "
        f"multi_row_pairs={s['multi_row']}"
    )


run_check(
    "V19",
    ["quality_log"],
    "Watermarks duplicate the audit-log rows_written metric",
    check_quality_overlap,
)

# COMMAND ----------

# DBTITLE 1,V20 -- Gold bridges: foreign-key resolution rates


def check_bridge_fk():
    specs = [
        ("bridge_eeg_support_unit", ["generation_unit_key"]),
        ("bridge_kwk_support_unit", ["generation_unit_key"]),
        ("bridge_authorisation_unit", ["generation_unit_key"]),
        ("bridge_location_unit", ["location_key", "generation_unit_key"]),
        ("bridge_location_connection", ["location_key", "grid_connection_point_key"]),
        ("bridge_actor_role", ["market_actor_key"]),
    ]
    rows, weak = [], []
    for name, keys in specs:
        agg = (
            tbl(GOLD, name)
            .agg(
                F.count("*").alias("n"),
                *[F.avg(F.col(k).isNull().cast("int")).alias(k) for k in keys],
            )
            .first()
        )
        for k in keys:
            share = agg[k] or 0.0
            rows.append((name, k, agg["n"], f"{share:.2%}"))
            if share > 0.05:
                weak.append(f"{name}.{k}={share:.1%}")
    table(["bridge", "key", "rows", "null_share"], rows)
    verdict = "SUPPORTS" if not weak else "REVIEW"
    return verdict, f"keys_over_5pct_null={weak or 'none'}"


run_check(
    "V20",
    ["bridges_gold"],
    "Gold bridge foreign-key resolution rates",
    check_bridge_fk,
)

# COMMAND ----------

# DBTITLE 1,V21 -- DWD hourly: shared station columns agree across products


def check_dwd_shared_columns():
    combos = union_all(
        [
            tbl(SILVER, f"dwd_{p}")
            .select("STATIONS_ID", "city", "ags_code", "ags_level", "ags_method")
            .distinct()
            for p in DWD_HOURLY
        ]
    ).distinct()
    per_station = combos.groupBy("STATIONS_ID").agg(F.count("*").alias("n_combos"))
    total = per_station.count()
    conflicts = per_station.filter("n_combos > 1")
    n_conflicts = conflicts.count()
    table(
        ["station", "distinct_combos"],
        [(r["STATIONS_ID"], r["n_combos"]) for r in conflicts.limit(20).collect()],
    )
    verdict = "SUPPORTS" if n_conflicts == 0 else "REVIEW"
    return (
        verdict,
        f"stations={total}, stations_with_conflicting_city_or_ags={n_conflicts}",
    )


run_check(
    "V21",
    ["dwd_hourly_gold", "dwd_hourly_silver"],
    "DWD shared station columns agree across products",
    check_dwd_shared_columns,
)

# COMMAND ----------

# DBTITLE 1,V22 -- DWD hourly: value agreement of overlapping variables


def check_overlap_values():
    pairs = [
        ("air_temperature_2m", "air_temperature", "moisture"),
        ("air_temperature_2m", "air_temperature", "dew_point"),
        ("total_cloud_cover", "cloudiness", "cloud_type"),
        ("pressure_station_level", "pressure", "moisture"),
    ]
    rows, differ = [], []
    for col, a, b in pairs:
        left = tbl(SILVER, f"dwd_{a}").select(
            "STATIONS_ID", "observation_ts", F.col(col).alias("va")
        )
        right = tbl(SILVER, f"dwd_{b}").select(
            "STATIONS_ID", "observation_ts", F.col(col).alias("vb")
        )
        r = (
            left.join(right, ["STATIONS_ID", "observation_ts"])
            .filter(F.col("va").isNotNull() & F.col("vb").isNotNull())
            .agg(
                F.count("*").alias("n"),
                F.avg((F.col("va") == F.col("vb")).cast("int")).alias("equal"),
                F.avg(F.abs(F.col("va") - F.col("vb"))).alias("mean_abs_diff"),
            )
            .first()
        )
        equal = r["equal"] or 0.0
        rows.append(
            (col, a, b, r["n"], f"{equal:.2%}", round(r["mean_abs_diff"] or 0.0, 4))
        )
        if equal < 0.99:
            differ.append(f"{col}:{a}/{b}")
    table(
        ["variable", "product_a", "product_b", "joined_rows", "equal", "mean_abs_diff"],
        rows,
    )
    verdict = "SUPPORTS" if not differ else "REVIEW"
    return verdict, f"pairs_below_99pct_equal={differ or 'none'}"


run_check(
    "V22",
    ["dwd_hourly_gold", "dwd_hourly_silver"],
    "DWD overlapping variables: value agreement across products",
    check_overlap_values,
)

# COMMAND ----------

# DBTITLE 1,Summary


def build_findings():
    counts = {v: 0 for v in VERDICT_ORDER}
    by_group = {}
    for r in RESULTS:
        counts[r["verdict"]] += 1
        for g in r["groups"]:
            by_group.setdefault(g, []).append(r["verdict"])
    lines = [
        "# TABLE CONSOLIDATION VERIFICATION FINDINGS",
        "",
        (
            "_Auto-generated by `databricks/utility/03_consolidation_verification.py`. "
            "Re-running replaces this file._"
        ),
        "",
        "## Summary",
        "",
        *md_table(
            ["check", "verdict", "question", "metrics"],
            [(r["id"], r["verdict"], r["question"], r["metrics"]) for r in RESULTS],
        ),
        "",
        "**Verdict counts:** " + ", ".join(f"{v}={n}" for v, n in counts.items()),
        "",
        "### Per-group verdict (worst check wins)",
        "",
        *md_table(
            ["group", "verdict", "checks"],
            [(g, worst_verdict(v), len(v)) for g, v in sorted(by_group.items())],
        ),
        "",
        (
            "SUPPORTS = evidence favours consolidation; REVIEW = needs a design "
            "decision (e.g. a discriminator in the key); REJECTS = evidence against; "
            "ERROR = the check failed to run."
        ),
    ]
    for r in RESULTS:
        lines += [
            "",
            f"## {r['id']} -- {r['question']}",
            "",
            f"- verdict: {r['verdict']}",
            f"- groups: {', '.join(r['groups'])}",
            f"- metrics: {r['metrics']}",
            *r["detail"],
        ]
    return "\n".join(lines) + "\n", counts


def write_findings(text):
    d = os.path.join(repo_root(), *FINDINGS_DIR)
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, FINDINGS_FILE)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(text)
    os.replace(tmp, path)
    return path


_text, _counts = build_findings()
print(f"findings export -> {write_findings(_text)}  ({len(RESULTS)} checks, {_counts})")
