# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SILVER -- MASTR REGISTERS (ENERGY)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the German market master data register (MaStR): its code lists (the
# MAGIC `reference_code_list` rows of MaStR), the generation units of eleven types (six
# MAGIC technologies, solar, storage and gas) in one table, the storage plants, the
# MAGIC support registrations, authorisations, repowering records, market
# MAGIC actors and roles, the grid locations, connection points, networks and balancing
# MAGIC areas, the register change events (deletions and grid-operator changes in one
# MAGIC table) and the typed links between the register entities. The counts, the
# MAGIC carrier key, the lifecycle state and the link flags are computed here.

# COMMAND ----------

# DBTITLE 1,Semantic structures library
# MAGIC %run ../_semantic_common

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = "silver/energy/04_mastr_registers"
FINDINGS = "energy"
RID = run_id()
MAPPING = load_mapping(SOURCE)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)
# Constant by construction in each single-technology table.
STRUCTURAL_CONSTANT_COLS = ["Energietraeger", "Land"]
# unit type -> Bronze table (the type of a first-pass table is its suffix)
UNIT_TABLES = {
    **{
        t: f"mastr_einheiten_{t}"
        for t in (
            "wind",
            "biomasse",
            "wasser",
            "verbrennung",
            "kernkraft",
            "geothermie_gsgk",
        )
    },
    "solar": "mastr_einheiten_solar",
    "electricity_storage": "mastr_einheiten_stromspeicher",
    "gas_producer": "mastr_einheiten_gaserzeuger",
    "gas_storage": "mastr_einheiten_gasspeicher",
    "gas_consumer": "mastr_einheiten_gasverbraucher",
}
# The gas units are no electricity carrier: no carrier key.
NO_CARRIER_TYPES = ("gas_producer", "gas_storage", "gas_consumer")
STORAGE_PLANT_TABLES = ("mastr_anlagen_stromspeicher", "mastr_anlagen_gasspeicher")
# Bronze table -> (support_scheme, unit_type, registration id column)
SUPPORT = {
    "mastr_anlagen_eeg_wind": ("renewable_energy_act", "wind", "EegMaStRNummer"),
    "mastr_anlagen_eeg_biomasse": (
        "renewable_energy_act",
        "biomasse",
        "EegMaStRNummer",
    ),
    "mastr_anlagen_eeg_wasser": ("renewable_energy_act", "wasser", "EegMaStRNummer"),
    "mastr_anlagen_eeg_solar": ("renewable_energy_act", "solar", "EegMaStRNummer"),
    "mastr_anlagen_eeg_speicher": (
        "renewable_energy_act",
        "electricity_storage",
        "EegMaStRNummer",
    ),
    "mastr_anlagen_eeg_geothermie_gsgk": (
        "renewable_energy_act",
        "geothermie_gsgk",
        "EegMaStRNummer",
    ),
    "mastr_anlagen_kwk": ("combined_heat_and_power", None, "KwkMastrNummer"),
}
# structure -> (Bronze table, id column), each keyed by its own MaStR id.
SINGLE = {
    "unit_authorisation": ("mastr_einheiten_genehmigung", "GenMastrNummer"),
    "unit_repowering": ("mastr_ertuechtigungen", "Id"),
    "market_actor": ("mastr_marktakteure", "MastrNummer"),
    "market_actor_role": ("mastr_marktakteure_und_rollen", "MastrNummer"),
    "grid_connection_point": (
        "mastr_netzanschlusspunkte",
        "NetzanschlusspunktMastrNummer",
    ),
    "grid_network": ("mastr_netze", "MastrNummer"),
}
LOCATION_BT = "mastr_lokationen"
BALANCING_BT = "mastr_bilanzierungsgebiete"
DELETIONS = {
    "unit_deletion": ("mastr_geloeschte_deaktivierte_einheiten", "unit_id"),
    "actor_deletion": (
        "mastr_geloeschte_deaktivierte_marktakteure",
        "market_actor_id",
    ),
}
CHANGE_BT = "mastr_einheiten_aenderung_netzbetreiberzuordnungen"
CHANGE_KEY = ["unit_id", "grid_operator_change_effective_date"]
LINKED_UNITS = "VerknuepfteEinheitenMaStRNummern"
AUTHORISATION_BT = "mastr_einheiten_genehmigung"
REPOWERING_BT = "mastr_ertuechtigungen"

# COMMAND ----------

# DBTITLE 1,Transform -- reference_code_list (MaStR code lists in one long table)
codes = (
    read_bronze("mastr_katalogkategorien")
    .select(
        F.lit("katalogkategorien").alias("catalog_kind"),
        strip_float_suffix("Id").alias("code_id"),
        F.lit(None).cast("string").alias("parent_id"),
        F.col("Name").alias("label"),
        F.lit("mastr_katalogkategorien").alias("source_dataset"),
    )
    .unionByName(
        read_bronze("mastr_katalogwerte").select(
            F.lit("katalogwerte").alias("catalog_kind"),
            strip_float_suffix("Id").alias("code_id"),
            strip_float_suffix("KatalogKategorieId").alias("parent_id"),
            F.col("Wert").alias("label"),
            F.lit("mastr_katalogwerte").alias("source_dataset"),
        )
    )
    .unionByName(
        read_bronze("mastr_code_lookup").select(
            "catalog_kind",
            strip_float_suffix("Id").alias("code_id"),
            F.lit(None).cast("string").alias("parent_id"),
            F.col("Wert").alias("label"),
            F.lit("mastr_code_lookup").alias("source_dataset"),
        )
    )
    .dropDuplicates()
    .withColumn("_srid", sha_key("catalog_kind", "code_id"))
)
codes = add_semantic_provenance(codes, SOURCE, None, RID, "_srid")

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- reference_code_list (MaStR rows)
write_inspect_export(
    conform(codes, REFERENCE_CODE_LIST_COLUMNS),
    "reference_code_list",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["catalog_kind", "code_id"],
    writer=write_semantic,
    replace_where="source_system = 'mastr'",
    extra_checks=structure_extra_checks,
    section="reference_code_list -- mastr",
)

# COMMAND ----------

# DBTITLE 1,Transform -- generation_unit (eleven unit types, one table with unit_type)
units = None
for unit_type, bt in UNIT_TABLES.items():
    df = _drop_structural_constants(
        mastr_standardise(read_bronze(bt), NAME_MAP, CODED, table=bt)
    )
    quarantine_out_of_bbox(df, bt)
    df = (
        attach_ags_prefix(df, "source_municipality_key")
        .withColumn("unit_type", F.lit(unit_type))
        .withColumn("source_dataset", F.lit(bt))
    )
    units = df if units is None else units.unionByName(df, allowMissingColumns=True)

# Counts of the unit's support ids and of the authorisation and repowering records
# that name it; the carrier key prefers the main fuel, then the biomass type, then
# the sea or land of a wind unit, then the unit type; the lifecycle state comes from
# the unit's own dates.
authorisations = (
    link_pairs(read_bronze(AUTHORISATION_BT), "GenMastrNummer", LINKED_UNITS)
    .groupBy(F.col("linked_id").alias("unit_id"))
    .agg(F.countDistinct("parent_id").alias("authorisation_count"))
)
repowerings = (
    link_pairs(read_bronze(REPOWERING_BT), "Id", "EegMastrNummer")
    .groupBy(F.col("linked_id").alias("renewable_energy_act_support_id"))
    .agg(F.countDistinct("parent_id").alias("repowering_count"))
)
units = (
    units.join(authorisations, "unit_id", "left")
    .join(repowerings, "renewable_energy_act_support_id", "left")
    .fillna(0, ["authorisation_count", "repowering_count"])
    .withColumn(
        "support_registration_count",
        F.col("renewable_energy_act_support_id").isNotNull().cast("int")
        + F.col("combined_heat_and_power_support_id").isNotNull().cast("int"),
    )
    .withColumn(
        "carrier_key",
        F.coalesce(
            energy_carrier_key(F.col("main_fuel")),
            energy_carrier_key(F.col("biomass_type")),
            energy_carrier_key(F.col("wind_onshore_or_offshore")),
            F.when(
                ~F.col("unit_type").isin(*NO_CARRIER_TYPES),
                energy_carrier_key(F.col("unit_type")),
            ),
        ),
    )
    .withColumn(
        "lifecycle_state",
        F.when(F.col("final_decommissioning_date").isNotNull(), "decommissioned")
        .when(
            F.col("provisional_shutdown_start_date").isNotNull(),
            "provisionally_shutdown",
        )
        .when(F.col("commissioning_date").isNotNull(), "operating")
        .when(F.col("planned_commissioning_date").isNotNull(), "planned")
        .otherwise("unknown"),
    )
)
units = units.withColumn("_srid", sha_key("unit_type", "unit_id"))
units = add_semantic_provenance(units, SOURCE, None, RID, "_srid")

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- generation_unit
generation_unit = write_inspect_export(
    units,
    "generation_unit",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["unit_type", "unit_id"],
    writer=write_semantic,
    extra_checks=lambda t: {
        "rows_by_unit_type": {
            r["unit_type"]: r["count"] for r in t.groupBy("unit_type").count().collect()
        },
        "rows_by_lifecycle_state": {
            r["lifecycle_state"]: r["count"]
            for r in t.groupBy("lifecycle_state").count().collect()
        },
        "units_without_a_carrier_key": t.filter(F.col("carrier_key").isNull()).count(),
    },
)

# COMMAND ----------

# DBTITLE 1,Transform -- support_registration (EEG and KWK in one table)
registrations = None
for bt, (scheme, unit_type, id_col) in SUPPORT.items():
    df = mastr_standardise(read_bronze(bt), NAME_MAP, CODED, table=bt)
    df = (
        df.withColumn(
            "support_registration_id",
            F.col(NAME_MAP.get(id_col, id_col)).cast("string"),
        )
        .withColumn("support_scheme", F.lit(scheme))
        .withColumn("unit_type", F.lit(unit_type).cast("string"))
        .withColumn("source_dataset", F.lit(bt))
    )
    registrations = (
        df
        if registrations is None
        else registrations.unionByName(df, allowMissingColumns=True)
    )
registrations = registrations.withColumn(
    "_srid", sha_key("support_scheme", "support_registration_id")
)
registrations = add_semantic_provenance(registrations, SOURCE, None, RID, "_srid")

# COMMAND ----------

# DBTITLE 1,Transform -- authorisations, repowering, market actors, connection points, networks
bronze_single = {name: read_bronze(bt) for name, (bt, _) in SINGLE.items()}
STRUCTURES = {"support_registration": registrations}
for name, (bt, id_col) in SINGLE.items():
    STRUCTURES[name] = mastr_structure(
        bronze_single[name], bt, id_col, name_map=NAME_MAP, coded_map=CODED, rid=RID
    )
# Electricity and gas storage plants in one table, each keyed by its MaStR number.
STRUCTURES["storage_plant"] = None
for bt in STORAGE_PLANT_TABLES:
    plants = mastr_structure(
        read_bronze(bt), bt, "MaStRNummer", name_map=NAME_MAP, coded_map=CODED, rid=RID
    )
    STRUCTURES["storage_plant"] = (
        plants
        if STRUCTURES["storage_plant"] is None
        else STRUCTURES["storage_plant"].unionByName(plants, allowMissingColumns=True)
    )

# Units an actor operates and its market roles; connection points and locations of
# a network.
operated = generation_unit.groupBy(F.col("operator_id").alias("market_actor_id")).agg(
    F.countDistinct("unit_id").alias("operated_unit_count")
)
roles_per_actor = (
    bronze_single["market_actor_role"]
    .groupBy(F.col("MarktakteurMastrNummer").cast("string").alias("market_actor_id"))
    .agg(F.countDistinct("Marktrolle").alias("market_role_count"))
)
STRUCTURES["market_actor"] = (
    STRUCTURES["market_actor"]
    .join(operated, "market_actor_id", "left")
    .join(roles_per_actor, "market_actor_id", "left")
    .fillna(0, ["operated_unit_count", "market_role_count"])
)
per_network = (
    STRUCTURES["grid_connection_point"]
    .groupBy("grid_id")
    .agg(
        F.countDistinct("connection_point_id").alias("connection_point_count"),
        F.countDistinct("location_id").alias("location_count"),
    )
)
STRUCTURES["grid_network"] = (
    STRUCTURES["grid_network"]
    .join(per_network, "grid_id", "left")
    .fillna(0, ["connection_point_count", "location_count"])
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- support, authorisation, repowering, actors, topology parts
for name, frame in STRUCTURES.items():
    write_inspect_export(
        frame,
        name,
        source=SOURCE,
        component=COMPONENT,
        rid=RID,
        findings=FINDINGS,
        key_cols=["source_record_id"],
        df_before=bronze_single.get(name),
        writer=write_semantic,
    )

# COMMAND ----------

# DBTITLE 1,Transform -- grid_location with its coordinate-conflict columns
locations = mastr_structure(
    read_bronze(LOCATION_BT),
    LOCATION_BT,
    "MastrNummer",
    name_map=NAME_MAP,
    coded_map=CODED,
    rid=RID,
)
# Units of one location that disagree on the coordinate at two decimals.
unit_coords = generation_unit.filter(
    F.col("location_id").isNotNull()
    & F.col("latitude").isNotNull()
    & F.col("longitude").isNotNull()
).select(
    "location_id",
    F.round("latitude", 2).alias("lat2dp"),
    F.round("longitude", 2).alias("lon2dp"),
)
conflict = (
    unit_coords.dropDuplicates()
    .groupBy("location_id")
    .agg(F.countDistinct("lat2dp", "lon2dp").alias("distinct_coordinate_count"))
    .withColumn("coordinate_conflict", F.col("distinct_coordinate_count") > 1)
)


def id_list_count(column):
    """Distinct ids in a delimited id list; 0 when the list is empty."""
    ids = F.array_distinct(F.split(F.trim(F.col(column)), r"[,;\s]+"))
    return F.when(F.col(column).isNull(), F.lit(0)).otherwise(
        F.size(F.filter(ids, lambda x: x != ""))
    )


grid_location = (
    locations.join(conflict, "location_id", "left")
    .withColumn("unit_count", id_list_count("linked_unit_ids"))
    .withColumn("connection_point_count", id_list_count("connection_point_ids"))
)

# COMMAND ----------

# DBTITLE 1,Transform -- balancing_area (Id not unique: ordinal, never a drop)
_names = {**NAME_MAP, **MASTR_KEY_NAMES[BALANCING_BT]}
_id = _names["Id"]
areas = mastr_standardise(
    read_bronze(BALANCING_BT).dropDuplicates(), _names, CODED, table=BALANCING_BT
)
areas = within_group_ordinal(
    areas,
    [_id],
    [
        "area_energy_identification_code",
        "control_zone",
        "balancing_area_connection_point",
    ],
)
per_area = (
    STRUCTURES["grid_connection_point"]
    .groupBy(F.col("balancing_area_connection_point_id").alias(_id))
    .agg(F.countDistinct("connection_point_id").alias("connection_point_count"))
)
areas = (
    areas.join(per_area, _id, "left")
    .fillna(0, ["connection_point_count"])
    .withColumn("_srid", sha_key(_id, "_source_id_ordinal"))
)
balancing_area = add_semantic_provenance(areas, SOURCE, BALANCING_BT, RID, "_srid")

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- grid_location and balancing_area
write_inspect_export(
    grid_location,
    "grid_location",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    writer=write_semantic,
    extra_checks=lambda t: {
        "locations_with_a_coordinate_conflict": t.filter(
            F.col("coordinate_conflict")
        ).count()
    },
)
write_inspect_export(
    balancing_area,
    "balancing_area",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    writer=write_semantic,
)

# COMMAND ----------

# DBTITLE 1,Transform -- register_change_event (deletions and grid-operator changes)
events = []
for kind, (bt, id_col) in DELETIONS.items():
    df = mastr_standardise(read_bronze(bt), NAME_MAP, CODED, table=bt)
    df = df.withColumn("_srid", sha_key(F.lit(kind), F.col(id_col).cast("string")))
    events.append(
        add_semantic_provenance(df, SOURCE, bt, RID, "_srid").withColumn(
            "event_kind", F.lit(kind)
        )
    )

commissioning = generation_unit.filter(F.col("commissioning_date").isNotNull()).select(
    "unit_id", "commissioning_date"
)
changes = mastr_standardise(read_bronze(CHANGE_BT), NAME_MAP, CODED, table=CHANGE_BT)
changes = within_group_ordinal(
    changes, CHANGE_KEY, [c for c in changes.columns if c not in CHANGE_KEY]
)
_registered = F.col("grid_operator_change_registered_date")
_effective = F.col("grid_operator_change_effective_date")
changes = (
    changes.join(F.broadcast(commissioning), "unit_id", "left")
    .withColumn(
        "_date_order_violation_registered_before_effective", _registered < _effective
    )
    .withColumn(
        "_date_order_violation_commissioning_after_change",
        F.col("commissioning_date") > _effective,
    )
    .drop("commissioning_date")
    .withColumn(
        "_srid",
        sha_key(F.lit("grid_operator_change"), *CHANGE_KEY, "_source_id_ordinal"),
    )
)
events.append(
    add_semantic_provenance(changes, SOURCE, CHANGE_BT, RID, "_srid").withColumn(
        "event_kind", F.lit("grid_operator_change")
    )
)
register_change_event = events[0]
for part in events[1:]:
    register_change_event = register_change_event.unionByName(
        part, allowMissingColumns=True
    )

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- register_change_event
write_inspect_export(
    register_change_event,
    "register_change_event",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    writer=write_semantic,
    extra_checks=structure_extra_checks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- register_link (every MaStR link as a typed pair)
_EEG = [
    "mastr_anlagen_eeg_wind",
    "mastr_anlagen_eeg_biomasse",
    "mastr_anlagen_eeg_wasser",
    "mastr_anlagen_eeg_geothermie_gsgk",
    "mastr_anlagen_eeg_solar",
    "mastr_anlagen_eeg_speicher",
]
# relationship_type -> (parent_type, linked_type, Bronze tables, parent col, link col)
LINKS = {
    "renewable_energy_act_support_unit": (
        "support_registration",
        "generation_unit",
        _EEG,
        "EegMaStRNummer",
        LINKED_UNITS,
    ),
    "combined_heat_and_power_support_unit": (
        "support_registration",
        "generation_unit",
        ["mastr_anlagen_kwk"],
        "KwkMastrNummer",
        LINKED_UNITS,
    ),
    "authorisation_unit": (
        "unit_authorisation",
        "generation_unit",
        [AUTHORISATION_BT],
        "GenMastrNummer",
        LINKED_UNITS,
    ),
    "repowering_renewable_energy_act": (
        "unit_repowering",
        "support_registration",
        [REPOWERING_BT],
        "Id",
        "EegMastrNummer",
    ),
    "storage_plant_unit": (
        "storage_plant",
        "generation_unit",
        list(STORAGE_PLANT_TABLES),
        "MaStRNummer",
        LINKED_UNITS,
    ),
    "location_unit": (
        "grid_location",
        "generation_unit",
        [LOCATION_BT],
        "MastrNummer",
        LINKED_UNITS,
    ),
    "location_connection": (
        "grid_location",
        "grid_connection_point",
        [LOCATION_BT],
        "MastrNummer",
        "NetzanschlusspunkteMaStRNummern",
    ),
}
link_bronze = {t: read_bronze(t) for rel in LINKS.values() for t in rel[2]}
links = None
for rel, (parent_type, linked_type, tables, parent_col, link_col) in LINKS.items():
    for t in tables:
        part = (
            link_pairs(link_bronze[t], parent_col, link_col)
            .withColumn("relationship_type", F.lit(rel))
            .withColumn("parent_type", F.lit(parent_type))
            .withColumn("linked_type", F.lit(linked_type))
            .withColumn("source_dataset", F.lit(t))
        )
        links = part if links is None else links.unionByName(part)

# An actor -> market role link: the role label is resolved to its code id.
roles = codes.filter(F.col("catalog_kind") == "marktrollen").select(
    F.trim("label").alias("_role_label"), F.col("code_id").alias("_role_id")
)
actor_roles = (
    bronze_single["market_actor_role"]
    .select(
        F.col("MarktakteurMastrNummer").cast("string").alias("parent_id"),
        F.trim(F.col("Marktrolle").cast("string")).alias("_label"),
    )
    .dropna()
    .dropDuplicates()
    .join(F.broadcast(roles), F.col("_label") == F.col("_role_label"), "left")
    .withColumn("linked_id", F.coalesce(F.col("_role_id"), F.col("_label")))
    .drop("_label", "_role_label", "_role_id")
    .withColumn("relationship_type", F.lit("actor_role"))
    .withColumn("parent_type", F.lit("market_actor"))
    .withColumn("linked_type", F.lit("reference_code_list"))
    .withColumn("source_dataset", F.lit("mastr_marktakteure_und_rollen"))
)
_link_key = ["relationship_type", "parent_id", "linked_id"]
# Whether the parent and the linked id exist in their own structure.
link_ids = register_link_ids(
    {
        "generation_unit": generation_unit,
        "grid_location": grid_location,
        "reference_code_list": codes,
        **{
            name: STRUCTURES[name]
            for name in (
                "support_registration",
                "unit_authorisation",
                "unit_repowering",
                "grid_connection_point",
                "market_actor",
                "storage_plant",
            )
        },
    }
)
links = (
    links.unionByName(actor_roles)
    .dropDuplicates(_link_key)
    .join(
        link_ids.select(
            F.col("entity_type").alias("parent_type"),
            F.col("id").alias("parent_id"),
            F.lit(True).alias("parent_resolved"),
        ),
        ["parent_type", "parent_id"],
        "left",
    )
    .join(
        link_ids.select(
            F.col("entity_type").alias("linked_type"),
            F.col("id").alias("linked_id"),
            F.lit(True).alias("linked_resolved"),
        ),
        ["linked_type", "linked_id"],
        "left",
    )
    .fillna(False, ["parent_resolved", "linked_resolved"])
    .withColumn("_srid", sha_key(*_link_key))
)
links = conform(
    add_semantic_provenance(links, SOURCE, None, RID, "_srid"), REGISTER_LINK_COLUMNS
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- register_link
write_inspect_export(
    links,
    "register_link",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=_link_key,
    writer=write_semantic,
    extra_checks=lambda t: {
        "rows_by_relationship_type": {
            r["relationship_type"]: r["count"]
            for r in t.groupBy("relationship_type").count().collect()
        },
        "links_with_an_unresolved_id": t.filter(
            ~F.col("parent_resolved") | ~F.col("linked_resolved")
        ).count(),
    },
)

# COMMAND ----------

# DBTITLE 1,Check and export -- rejected values of the lenient date parser
export_rejected_values(FINDINGS, COMPONENT)
