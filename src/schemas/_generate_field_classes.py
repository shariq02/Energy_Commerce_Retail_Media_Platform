"""Generate the central energy_silver field-class registry seed.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: September 2026

Purpose: one authoritative place that enumerates every column every Silver
notebook emits, with its class -- source_provided / derived / synthetic --
and its target_schema (energy_silver / energy_silver_reference /
commerce_silver / commerce_silver_reference), resolved from
source_ecosystem_map.yml plus the fixed reference-table set. The Silver
notebooks validate against this; they never author it.

Reads: src/schemas/contracts/*.yml + src/schemas/mappings/*.yml +
src/schemas/reference/source_ecosystem_map.yml.

`build_rows()` + `assert_registry_complete()` are imported directly by
`databricks/silver/00_silver_setup.py`, which computes and loads
`quality.field_class_registry` from them at Silver-setup time -- no committed
CSV is required for normal execution. `render()`/`main()` below still write
src/schemas/field_classes/energy_silver_field_classes.csv as an optional,
human-readable artifact for local/CI diffing, kept in sync by the tests.

Usage:
    python3 src/schemas/_generate_field_classes.py
    python3 src/schemas/_generate_field_classes.py --check   # fail if stale
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
from pathlib import Path

try:  # imported as src.schemas._generate_field_classes (e.g. 00_silver_setup.py)
    from . import _silver_notebook_scan
except ImportError:  # run as a standalone script -- its own dir is on sys.path[0]
    import _silver_notebook_scan
import yaml

ROOT = Path(__file__).resolve().parents[2]
CONTRACTS = ROOT / "src" / "schemas" / "contracts"
MAPPINGS = ROOT / "src" / "schemas" / "mappings"
OUT = ROOT / "src" / "schemas" / "field_classes" / "energy_silver_field_classes.csv"
SILVER_ROOT = ROOT / "databricks" / "silver"

# Governance / provenance columns present on every primary Silver table.
GOVERNANCE = {
    "source_system": ("derived", "constant per source", "silver governance"),
    "source_record_id": (
        "derived",
        "natural key or deterministic composite",
        "silver governance",
    ),
    "ecosystem": (
        "derived",
        "source_ecosystem_map.yml at the Silver boundary",
        "provenance standard",
    ),
    "_silver_loaded_at": (
        "derived",
        "current_timestamp() at write",
        "silver governance",
    ),
    "_silver_run_id": ("derived", "the Silver run id", "silver governance"),
}
CONFLICT_COLS = {
    "_had_key_conflict": (
        "derived",
        "a resolved same-key conflict existed",
        "silver conflict rule",
    ),
    "_src_id_disambiguated": (
        "derived",
        "the content-hash ordinal disambiguated the composite key",
        "silver source_record_id rule",
    ),
    "_src_id_ord": (
        "derived",
        "1-based content-hash ordinal within a composite-key group",
        "silver source_record_id rule",
    ),
}
GEO_COLS = {
    "ags_code": (
        "derived",
        "curated / prefix attribution to the Bundesland AGS",
        "geography attribution, Bundesland level",
    ),
    "ags_level": (
        "derived",
        "geography level achieved (bundesland)",
        "geography attribution limit",
    ),
    "ags_method": (
        "derived",
        "attribution method (city_lookup / ags_prefix / bundesland_code)",
        "geography attribution",
    ),
}


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _contract_tables(contract: dict) -> dict:
    if "tables" in contract:
        return {t["name"]: t for t in contract["tables"] if "columns" in t}
    if "columns" in contract:
        name = contract.get("bronze_table", contract["source"]).split(".")[-1]
        return {name: {"name": name, "columns": contract["columns"]}}
    return {}


def _flatten_business_names(mapping: dict, source: str) -> dict[str, str]:
    """Bronze column -> English business name, per the mapping's own structure."""
    bn = mapping.get("business_names", {}) or {}
    out: dict[str, str] = {}
    if source == "dwd":
        for code, spec in (bn.get("parameters") or {}).items():
            out[code] = spec["business_name"] if isinstance(spec, dict) else spec
        out.update(bn.get("metadata_fields") or {})
    elif source == "smard":
        out.update(bn.get("columns") or {})
    elif source == "mastr":
        out.update(bn.get("identifiers") or {})
        out.update(bn.get("common_fields") or {})
    else:  # power_plant_list, redispatch
        out.update(bn.get("fields") or {})
    return out


def _coded_columns(mapping: dict, source: str) -> list[str]:
    """Bronze columns that get the <x>_code / <x>_label_de / <x> decode triple."""
    if source == "mastr":
        return list(
            (mapping.get("coded_value_labels", {}) or {}).get(
                "named_category_bindings", {}
            )
        )
    labels = mapping.get("coded_value_labels", {}) or {}
    scopes = []
    for spec in labels.values():
        if isinstance(spec, dict) and "scope" in spec:
            col = spec["scope"].split(" -- ")[0].split(".")[-1].strip()
            scopes.append(col)
    return scopes


# Per-source Silver-table topology: which Bronze table(s) map to which Silver
# table(s), plus additive bridges. The Silver notebooks follow this naming.
TOPOLOGY: dict[str, dict] = {
    "dwd": {
        # measurement tables keep their name; metadata tables keep their name;
        # + a derived dwd_parameter_catalog + dwd_city_bundesland_xref
        "extra_tables": {
            "dwd_parameter_catalog": {
                "parameter_source_code": (
                    "source_provided",
                    "dwd_parameter_unit.Parameter",
                    "dwd contract",
                ),
                "parameter_business_name": (
                    "derived",
                    "business-name mapping (parameters)",
                    "business-name mapping",
                ),
                "parameter_unit": (
                    "derived",
                    "dwd_parameter_unit.Einheit",
                    "unit registry",
                ),
                "parameter_description_de": (
                    "source_provided",
                    "dwd_parameter_unit.Parameterbeschreibung",
                    "dwd contract",
                ),
                "catalog_vintage": (
                    "derived",
                    "pinned DWD archive vintage",
                    "decode-authority vintage",
                ),
            },
            "dwd_city_bundesland_xref": {
                "city": (
                    "source_provided",
                    "DWD station folder name",
                    "dwd contract station_set",
                ),
                "bundesland_name": (
                    "derived",
                    "curated 28-station city -> Bundesland",
                    "geography attribution",
                ),
                "ags_code": (
                    "derived",
                    "curated 28-station city -> Bundesland AGS",
                    "geography attribution",
                ),
                "ags_level": ("derived", "'bundesland'", "geography attribution limit"),
            },
            "dwd_missingness_reconciliation": {
                "station_id": (
                    "derived",
                    "STATIONS_ID from each dwd_hourly measurement table",
                    "cross-table missingness check",
                ),
                "parameter_source_code": (
                    "derived",
                    "exploded from each measurement table's own value columns",
                    "cross-table missingness check",
                ),
                "_missingness_reconciliation_status": (
                    "derived",
                    "'matched' / 'observed_only' / 'reported_only' from a full outer join",
                    "cross-table missingness check",
                ),
            },
        },
        "geo_tables": {
            "dwd_air_temperature",
            "dwd_cloudiness",
            "dwd_moisture",
            "dwd_precipitation",
            "dwd_pressure",
            "dwd_sun",
            "dwd_wind",
            "dwd_dew_point",
            "dwd_visibility",
            "dwd_cloud_type",
            "dwd_wind_synop",
            "dwd_extreme_wind",
            "dwd_weather_phenomena",
            "dwd_soil_temperature",
            "dwd_solar",
            "dwd_station_geography",
        },
        "ts_rename": {"MESS_DATUM": "observation_ts"},
        "qn_prefix": "qn_level",
    },
    "smard": {
        "extra_tables": {},
        "geo_tables": set(),
        "value_flags": ["metric_semantic_status"],
    },
    "mastr": {
        "extra_tables": {
            "mastr_location_coordinate_conflict": {
                "location_id": (
                    "derived",
                    "gathered from the 6 mastr_einheiten_* Silver tables",
                    "cross-table coordinate-agreement check",
                ),
                "distinct_coords": (
                    "derived",
                    "countDistinct(lat, lon) rounded to 2dp across linked units",
                    "cross-table coordinate-agreement check",
                ),
                "_coordinate_conflict": (
                    "derived",
                    "distinct_coords > 1",
                    "cross-table coordinate-agreement check",
                ),
            },
        },
        "geo_tables": {
            "mastr_einheiten_wind",
            "mastr_einheiten_biomasse",
            "mastr_einheiten_wasser",
            "mastr_einheiten_verbrennung",
            "mastr_einheiten_kernkraft",
            "mastr_einheiten_geothermie_gsgk",
        },
        "bridges": {
            "mastr_eeg_support_unit_bridge",
            "mastr_kwk_support_unit_bridge",
            "mastr_authorisation_unit_bridge",
            "mastr_repowering_eeg_bridge",
            "mastr_actor_role_bridge",
            "mastr_location_unit_bridge",
            "mastr_location_connection_bridge",
        },
        "renamed_tables": {
            "mastr_geloeschte_deaktivierte_einheiten": "mastr_unit_deletion_events",
            "mastr_geloeschte_deaktivierte_marktakteure": "mastr_actor_deletion_events",
            "mastr_einheiten_aenderung_netzbetreiberzuordnungen": "mastr_grid_operator_change_events",
        },
    },
    "power_plant_list": {
        "extra_tables": {},
        "geo_tables": {"power_plant_list"},
        "value_flags_by_table": {"power_plant_capacity_additions": []},
    },
    "redispatch": {
        "extra_tables": {},
        "geo_tables": set(),
        "ts_pairs": {
            "measure_start_ts": ["BEGINN_DATUM", "BEGINN_UHRZEIT"],
            "measure_end_ts": ["ENDE_DATUM", "ENDE_UHRZEIT"],
        },
    },
}

# Silver topology for the foundation sources -- Honda IoT, REES46 and search
# visibility -- whose Silver shape is enumerated here rather than derived from
# an energy / weather contract.
FOUNDATION_SOURCES = {
    "honda_iot": {
        "silver": {
            "honda_electricity_p": ["frequency", "datetime_utc", "total", "PV", "CHP"],
            "honda_electricity_w": ["frequency", "datetime_utc", "total", "PV", "CHP"],
            "honda_heating_p": [
                "frequency",
                "datetime_utc",
                "total",
                "CHP_heat",
                "CHP_elec",
            ],
            "honda_heating_w": [
                "frequency",
                "datetime_utc",
                "total",
                "CHP_heat",
                "CHP_elec",
            ],
            "honda_cooling_p": ["frequency", "datetime_utc", "total", "cool_elec"],
            "honda_cooling_w": ["frequency", "datetime_utc", "total", "cool_elec"],
            "honda_weather": [
                "frequency",
                "datetime_utc",
                "air_temperature_2m",
                "global_irradiance",
                "weather_location",
            ],
            "honda_channel_catalog": [
                "site_name",
                "subsystem",
                "measurement_type",
                "channel_code",
                "description",
            ],
        },
        "synthetic": {
            "honda_weather": {
                "weather_location": (
                    "synthetic",
                    "constant 'honda_site' -- single fixed site",
                    "synthetic field",
                )
            },
            "honda_channel_catalog": {
                c: (
                    "synthetic",
                    "curated -- Honda ships no device master file",
                    "01_honda_channel_catalog.py",
                )
                for c in (
                    "site_name",
                    "subsystem",
                    "measurement_type",
                    "channel_code",
                    "description",
                )
            },
        },
    },
    "rees46": {
        "silver": {
            "rees46_events": [
                "event_time",
                "event_type",
                "product_id",
                "category_id",
                "category_code",
                "brand",
                "price",
                "user_id",
                "user_session",
                "currency_unknown",
            ],
        },
        "deferred": {
            "rees46_user_country_synthetic": {
                "country_synthetic": (
                    "synthetic",
                    "DEFERRED -- no defensible generation rule; not built in the silver layer",
                    "localisation design",
                )
            }
        },
    },
    "ga4": {
        "silver": {
            # The 8 top-level Bronze columns -- event_params/ecommerce/items
            # stay nested (Delta STRUCT/ARRAY); the registry classifies the
            # top-level column only, matching what df.columns returns for a
            # nested field, not each inner field individually.
            "ga4_events": [
                "event_date",
                "event_timestamp",
                "event_name",
                "user_pseudo_id",
                "geo_country",
                "event_params",
                "ecommerce",
                "items",
            ],
            # ga4_items: ga4_events.items exploded to item grain -- item_id is
            # overloaded (campaign id on promotion events, product id
            # otherwise), only separable once exploded (03_ga4_items.py).
            "ga4_items": [
                "event_date",
                "event_timestamp",
                "user_pseudo_id",
                "event_name",
                "item_ordinal",
                "item_id",
                "item_name",
                "item_category",
                "price",
                "quantity",
                "item_revenue",
                "item_context",
            ],
            # ga4_transactions: ga4_events.ecommerce filtered + flattened to
            # transaction-bearing event grain (04_ga4_transactions.py).
            "ga4_transactions": [
                "event_date",
                "event_timestamp",
                "user_pseudo_id",
                "event_name",
                "transaction_id",
                "purchase_revenue",
                "unique_items",
                "total_item_quantity",
            ],
        },
        "synthetic": {
            "ga4_items": {
                "item_ordinal": (
                    "derived",
                    "posexplode_outer position within the items array",
                    "explode ordinal",
                ),
                "item_context": (
                    "derived",
                    "'promotion' for view_promotion/select_promotion events, else 'product'",
                    "ga4 contract / business rule",
                ),
            }
        },
    },
}


# Reference-layer tables -- additive/decode/catalog tables, never the primary
# source-grain table. Every other table is primary. Matches exactly what
# databricks/silver/{energy,commerce}/_reference/*.py write (verified against
# those notebooks' write_silver() calls, not re-derived from a naming rule).
REFERENCE_TABLES = {
    "dwd_city_bundesland_xref",
    "dwd_station_geography",
    "dwd_station_name_history",
    "dwd_device_instrument",
    "dwd_parameter_unit",
    "dwd_parameter_catalog",
    "dwd_missing_value_periods",
    "mastr_katalogkategorien",
    "mastr_katalogwerte",
    "mastr_einheitentypen",
    "mastr_lokationstypen",
    "mastr_marktfunktionen",
    "mastr_marktrollen",
    "honda_channel_catalog",
}

# Per-table column-rename override for a bespoke rename that diverges from
# the source's generic business-name mapping. dwd_missing_value_periods
# keeps Von_Datum/Bis_Datum as gap_start_ts/gap_end_ts (its own notebook's
# explicit .withColumnRenamed), not the generic valid_from/valid_to every
# other DWD metadata table uses -- verified against that notebook, not
# guessed.
TABLE_COLUMN_RENAME_OVERRIDES = {
    "dwd_missing_value_periods": {
        "Von_Datum": "gap_start_ts",
        "Bis_Datum": "gap_end_ts",
    },
}

# flag_col columns value_quarantine() adds on top of the source's own
# contract columns -- verified against every value_quarantine() call in
# databricks/silver/, not derivable from the contract itself.
VALUE_QUARANTINE_FLAGS = {
    "dwd_station_geography": [
        (
            "_coord_outside_de_bbox",
            "station coordinate outside the Germany bounding box",
        ),
    ],
    "power_plant_list": [
        (
            "_capacity_all_null",
            "no capacity value parsed on the row",
        ),
    ],
}

# table_name prefix -> the contract's source_system (the source_ecosystem_map.yml
# key), longest/most-specific prefix first so "power_plant_" is checked before
# any shorter prefix could apply.
_PREFIX_TO_SOURCE_SYSTEM = (
    ("power_plant_", "power_plant_list"),
    ("redispatch_", "redispatch"),
    ("mastr_", "mastr"),
    ("smard_", "smard"),
    ("honda_", "honda_iot"),
    ("rees46_", "rees46"),
    ("dwd_", "dwd"),
    ("ga4_", "ga4"),
)


def _ecosystem_by_source_system() -> dict[str, str]:
    doc = _load(ROOT / "src" / "schemas" / "reference" / "source_ecosystem_map.yml")
    return {m["source_system"]: m["ecosystem"] for m in doc["mappings"]}


def _source_system_for_table(table_name: str) -> str:
    for prefix, source_system in _PREFIX_TO_SOURCE_SYSTEM:
        if table_name.startswith(prefix):
            return source_system
    message = f"no known source prefix for table {table_name!r}"
    raise ValueError(message)


def target_schema_for(table_name: str, ecosystem_map: dict[str, str]) -> str:
    source_system = _source_system_for_table(table_name)
    ecosystem = ecosystem_map.get(source_system, "energy")
    base = f"{ecosystem}_silver"
    return f"{base}_reference" if table_name in REFERENCE_TABLES else base


def build_rows() -> list[dict]:
    rows: list[dict] = []

    def add(table: str, col: str, cls: str, rule: str, ref: str) -> None:
        rows.append(
            {
                "table_name": table,
                "column_name": col,
                "field_class": cls,
                "derivation_rule": rule,
                "source_reference": ref,
            }
        )

    def add_governance(
        table: str, *, conflict: bool = False, disambig: bool = False
    ) -> None:
        for c, (cls, rule, ref) in GOVERNANCE.items():
            add(table, c, cls, rule, ref)
        if conflict:
            add(table, "_had_key_conflict", *CONFLICT_COLS["_had_key_conflict"])
        if disambig:
            for c in ("_src_id_disambiguated", "_src_id_ord"):
                add(table, c, *CONFLICT_COLS[c])

    # --- energy & weather wave, from contracts + mappings ---
    for source in ("dwd", "smard", "mastr", "power_plant_list", "redispatch"):
        contract = _load(CONTRACTS / f"{source}.yml")
        mapping = _load(MAPPINGS / f"{source}.yml")
        topo = TOPOLOGY[source]
        bn = _flatten_business_names(mapping, source)
        coded = set(_coded_columns(mapping, source))
        tables = _contract_tables(contract)

        for bt, tdef in tables.items():
            st = topo.get("renamed_tables", {}).get(bt, bt) if source == "mastr" else bt
            for col in tdef["columns"]:
                name = col["name"]
                out_name = bn.get(name, name)
                # DWD MESS_DATUM -> observation_ts
                out_name = topo.get("ts_rename", {}).get(name, out_name)
                # per-table override (e.g. dwd_missing_value_periods)
                out_name = TABLE_COLUMN_RENAME_OVERRIDES.get(st, {}).get(name, out_name)
                add(
                    st,
                    out_name,
                    "source_provided",
                    "typed cast + English business rename"
                    if out_name != name
                    else "typed cast",
                    f"{source} contract / business-name mapping",
                )
                # decode triple for coded columns
                if source == "mastr":
                    match_coded = name in coded
                else:
                    match_coded = name in coded or out_name in coded
                if match_coded:
                    pref = bn.get(name, name)
                    add(
                        st,
                        f"{pref}_code",
                        "derived",
                        "source code, verbatim",
                        "coded-value decode triple",
                    )
                    add(
                        st,
                        f"{pref}_label_de",
                        "derived",
                        "German label from the decode reference",
                        "coded-value decode triple",
                    )
                    # the English label reuses the business-name column (out_name)
                # DWD QN column -> qn_level triple
                if source == "dwd" and name.startswith("QN_"):
                    for suf in ("qn_level_code", "qn_level_label_de", "qn_level"):
                        add(
                            st,
                            suf,
                            "derived",
                            "DWD quality-level decode",
                            "DWD quality-level decode",
                        )
            # governance
            has_conflict = (
                source == "dwd"
                and st.startswith("dwd_")
                and "observation" in " ".join(c["name"] for c in tdef["columns"])
                or (
                    source == "dwd"
                    and any(c["name"] == "MESS_DATUM" for c in tdef["columns"])
                )
            )
            disambig = (
                source in ("power_plant_list", "redispatch")
                or (
                    source == "mastr"
                    and st
                    in (
                        "mastr_grid_operator_change_events",
                        "mastr_bilanzierungsgebiete",
                    )
                )
                or st
                in (
                    "dwd_missing_value_periods",
                    "dwd_station_geography",
                    "dwd_station_name_history",
                    "dwd_device_instrument",
                    "dwd_parameter_unit",
                )
            )
            add_governance(st, conflict=has_conflict, disambig=disambig)
            # value_quarantine flag columns -- the notebook's own flag_col=
            # argument, not derivable from the contract; verified against every
            # value_quarantine() call in databricks/silver/, not guessed.
            for flag_col, reason in VALUE_QUARANTINE_FLAGS.get(st, []):
                add(
                    st,
                    flag_col,
                    "derived",
                    reason,
                    "value_quarantine flag column",
                )
            # geo
            if bt in topo.get("geo_tables", set()):
                for c, (cls, rule, ref) in GEO_COLS.items():
                    add(st, c, cls, rule, ref)
            # DWD observation_ts derived timestamp + solar WOZ
            if source == "dwd" and any(
                c["name"] == "MESS_DATUM" for c in tdef["columns"]
            ):
                add(
                    st,
                    "observation_ts",
                    "derived",
                    "MESS_DATUM parsed (UTC)",
                    "UTC conversion",
                )
            if bt == "dwd_solar":
                add(
                    st,
                    "observation_woz",
                    "derived",
                    "MESS_DATUM_WOZ kept as true local solar time",
                    "local solar time",
                )

        # smard derived
        if source == "smard":
            for c, rule in (
                ("market_zone", "region -> dim_market zone vocabulary"),
                ("unit", "per metric x resolution, unit registry"),
                ("metric_business_name", "business-name mapping (metrics)"),
                (
                    "metric_semantic_status",
                    "'disputed' for the PV-forecast mirror, else 'confirmed'",
                ),
                (
                    "semantic_issue_ref",
                    "link to the contract quality rule for a disputed metric",
                ),
                ("observation_ts", "timestamp_utc verified Europe/Berlin -> UTC"),
            ):
                add(
                    "smard_energy_timeseries",
                    c,
                    "derived",
                    rule,
                    "smard contract / localisation",
                )
        # redispatch derived timestamps + tso list
        if source == "redispatch":
            for c, rule in (
                (
                    "measure_start_ts",
                    "BEGINN_DATUM + BEGINN_UHRZEIT, Europe/Berlin -> UTC",
                ),
                ("measure_end_ts", "ENDE_DATUM + ENDE_UHRZEIT, Europe/Berlin -> UTC"),
                ("requesting_tso_list", "ANFORDERNDER_UENB split on '&'"),
            ):
                add(
                    "redispatch_measures",
                    c,
                    "derived",
                    rule,
                    "redispatch contract / localisation",
                )
        # mastr bridges
        if source == "mastr":
            for br in topo["bridges"]:
                add(
                    br,
                    "parent_id",
                    "source_provided",
                    "the owning record's MaStR id",
                    "mastr contract",
                )
                add(
                    br,
                    "linked_id",
                    "source_provided",
                    "one exploded id from the delimited link array",
                    "mastr contract",
                )
                add_governance(br)

        # extra derived tables
        for tname, cols in topo.get("extra_tables", {}).items():
            for c, (cls, rule, ref) in cols.items():
                add(tname, c, cls, rule, ref)

    # --- foundation sources, from the enumerated topology ---
    for src, spec in FOUNDATION_SOURCES.items():
        for st, cols in spec.get("silver", {}).items():
            for c in cols:
                cls = "source_provided"
                rule = "typed cast (+ rename where localised)"
                syn = spec.get("synthetic", {}).get(st, {})
                if c in syn:
                    cls, rule, ref = syn[c]
                    add(st, c, cls, rule, ref)
                    continue
                add(st, c, cls, rule, f"{src} contract")
            has_conflict = st in ("rees46_events", "ga4_events")
            add_governance(st, conflict=has_conflict)
        for st, cols in spec.get("deferred", {}).items():
            for c, (cls, rule, ref) in cols.items():
                add(st, c, cls, rule, ref)
        # honda weather geo (single fixed site -- no ags, weather_location is synthetic)
        if src == "rees46":
            add(
                "rees46_events",
                "currency_unknown",
                "derived",
                "price currency undocumented -- relative measures only",
                "rees46 contract / localisation",
            )

    # de-dup (a column can be added twice by overlapping rules)
    seen: set[tuple[str, str]] = set()
    uniq: list[dict] = []
    for r in rows:
        key = (r["table_name"], r["column_name"])
        if key in seen:
            continue
        seen.add(key)
        uniq.append(r)

    ecosystem_map = _ecosystem_by_source_system()
    for r in uniq:
        r["target_schema"] = target_schema_for(r["table_name"], ecosystem_map)

    return sorted(uniq, key=lambda r: (r["table_name"], r["column_name"]))


def render(rows: list[dict]) -> str:
    buf = io.StringIO()
    w = csv.DictWriter(
        buf,
        fieldnames=[
            "table_name",
            "column_name",
            "field_class",
            "derivation_rule",
            "source_reference",
            "target_schema",
        ],
        lineterminator="\n",
    )
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue()


def assert_registry_complete(rows: list[dict]) -> None:
    """Hard-fail if a table some Silver notebook actually writes has no row
    here -- this is the generator's own knowledge falling behind real code,
    distinct from `--check`'s concern (the committed seed falling behind the
    generator). Never silently emit an incomplete registry, and never guess a
    classification for an undeclared table -- raises so both the CLI and a
    library caller (e.g. `00_silver_setup.py`) get a catchable, clear error
    naming exactly which table(s) and notebook(s) are missing."""
    produced = {r["table_name"] for r in rows}
    missing = {
        nb: gap
        for nb, targets in _silver_notebook_scan.tables_by_notebook(SILVER_ROOT).items()
        if (gap := targets - produced)
    }
    if missing:
        message = (
            f"field-class registry has no entry for tables real Silver notebooks "
            f"write: {missing} -- add each to TOPOLOGY / FOUNDATION_SOURCES in "
            "_generate_field_classes.py; this is never auto-guessed"
        )
        raise RuntimeError(message)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="exit non-zero if the seed is stale"
    )
    args = parser.parse_args()

    rows = build_rows()
    try:
        assert_registry_complete(rows)
    except RuntimeError as exc:
        print(exc, file=sys.stderr)
        return 1
    text = render(rows)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    if args.check:
        current = OUT.read_text(encoding="utf-8") if OUT.exists() else ""
        if current != text:
            print(
                f"stale: {OUT} -- run `python src/schemas/_generate_field_classes.py`",
                file=sys.stderr,
            )
            return 1
        print(f"OK  {OUT} is up to date")
        return 0
    OUT.write_text(text, encoding="utf-8")
    print(f"wrote {OUT}  ({text.count(chr(10)) - 1} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
