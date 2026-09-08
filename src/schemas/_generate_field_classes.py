"""Generate the central energy_silver field-class registry seed.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: September 2026

Purpose: one authoritative place that enumerates every column every Silver
notebook emits, with its class -- source_provided / derived / synthetic. The
Silver notebooks validate against this; they never author it.

Reads: src/schemas/contracts/*.yml + src/schemas/mappings/*.yml.
Writes: src/schemas/field_classes/energy_silver_field_classes.csv
(loaded into energy_silver.field_class_registry by databricks/silver/00_silver_setup.py).

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

import yaml

ROOT = Path(__file__).resolve().parents[2]
CONTRACTS = ROOT / "src" / "schemas" / "contracts"
MAPPINGS = ROOT / "src" / "schemas" / "mappings"
OUT = ROOT / "src" / "schemas" / "field_classes" / "energy_silver_field_classes.csv"

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
        "extra_tables": {},
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

# Silver topology for the foundation sources -- Honda IoT, REES46, search
# visibility and the CDC operational tables -- whose Silver shape is enumerated
# here rather than derived from an energy / weather contract.
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
        },
        "synthetic": {
            "honda_weather": {
                "weather_location": (
                    "synthetic",
                    "constant 'honda_site' -- single fixed site",
                    "synthetic field",
                )
            }
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
    "search_visibility": {
        "silver": {
            "search_visibility_events": [
                "citableContent",
                "click_through",
                "clicks",
                "country",
                "period",
                "device",
                "impressions",
                "index",
                "position",
                "url",
                "repository_id",
            ],
            "search_visibility_repository": [
                "repository_id",
                "country",
                "ir_platform",
                "name",
            ],
        },
    },
    "cdc": {
        "silver": {
            "op_tariffs": [
                "tariff_id",
                "tariff_code",
                "name",
                "energy_type",
                "unit_price_eur_per_kwh",
                "standing_charge_eur_per_month",
                "contract_term_months",
                "active",
                "valid_from",
                "valid_to",
                "created_at",
                "updated_at",
                "_op",
                "_lsn",
                "_event_ts",
            ],
            "op_products": [
                "product_id",
                "sku",
                "name",
                "category",
                "unit_price_eur",
                "active",
                "created_at",
                "updated_at",
                "_op",
                "_lsn",
                "_event_ts",
            ],
            "op_customers": [
                "customer_id",
                "customer_number",
                "first_name",
                "last_name",
                "email",
                "phone",
                "street",
                "house_number",
                "postal_code",
                "city",
                "country_code",
                "date_of_birth",
                "signed_up_at",
                "status",
                "created_at",
                "updated_at",
                "_op",
                "_lsn",
                "_event_ts",
            ],
            "op_customer_contracts": [
                "contract_id",
                "contract_number",
                "customer_id",
                "tariff_id",
                "start_date",
                "end_date",
                "status",
                "billing_day",
                "created_at",
                "updated_at",
                "_op",
                "_lsn",
                "_event_ts",
            ],
            "op_meters": [
                "meter_id",
                "meter_serial",
                "contract_id",
                "meter_type",
                "melo_id",
                "installed_on",
                "removed_on",
                "status",
                "created_at",
                "updated_at",
                "_op",
                "_lsn",
                "_event_ts",
            ],
            "op_orders": [
                "order_id",
                "order_number",
                "customer_id",
                "order_status",
                "ordered_at",
                "currency",
                "items_subtotal_eur",
                "shipping_fee_eur",
                "total_eur",
                "created_at",
                "updated_at",
                "_op",
                "_lsn",
                "_event_ts",
            ],
            "op_order_items": [
                "order_item_id",
                "order_id",
                "product_id",
                "quantity",
                "unit_price_eur",
                "line_total_eur",
                "created_at",
                "_op",
                "_lsn",
                "_event_ts",
            ],
        },
    },
}


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
            disambig = source in ("power_plant_list", "redispatch") or (
                source == "mastr" and st == "mastr_grid_operator_change_events"
            )
            add_governance(st, conflict=has_conflict, disambig=disambig)
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
            has_conflict = st in ("rees46_events", "search_visibility_events")
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
        ],
        lineterminator="\n",
    )
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="exit non-zero if the seed is stale"
    )
    args = parser.parse_args()

    text = render(build_rows())
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
