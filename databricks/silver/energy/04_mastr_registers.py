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
# MAGIC `reference_code_list` rows of MaStR), the generation units of six technologies in
# MAGIC one table, the support registrations, authorisations, repowering records, market
# MAGIC actors and roles, the grid locations, connection points, networks and balancing
# MAGIC areas, the register change events (deletions and grid-operator changes in one
# MAGIC table) and the typed links between the register entities.

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
UNIT_TYPES = (
    "wind",
    "biomasse",
    "wasser",
    "verbrennung",
    "kernkraft",
    "geothermie_gsgk",
)
# Bronze table -> (support_scheme, unit_type, registration id column)
SUPPORT = {
    "mastr_anlagen_eeg_wind": ("renewable_energy_act", "wind", "EegMaStRNummer"),
    "mastr_anlagen_eeg_biomasse": (
        "renewable_energy_act",
        "biomasse",
        "EegMaStRNummer",
    ),
    "mastr_anlagen_eeg_wasser": ("renewable_energy_act", "wasser", "EegMaStRNummer"),
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

# DBTITLE 1,Transform -- generation_unit (six technologies, one table with unit_type)
units = None
for unit_type in UNIT_TYPES:
    bt = f"mastr_einheiten_{unit_type}"
    df = _drop_structural_constants(mastr_standardise(read_bronze(bt), NAME_MAP, CODED))
    quarantine_out_of_bbox(df, bt)
    df = (
        attach_ags_prefix(df, "source_municipality_key")
        .withColumn("unit_type", F.lit(unit_type))
        .withColumn("source_dataset", F.lit(bt))
    )
    units = df if units is None else units.unionByName(df, allowMissingColumns=True)
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
    key_cols=["unit_id"],
    writer=write_semantic,
    extra_checks=lambda t: {
        "rows_by_unit_type": {
            r["unit_type"]: r["count"] for r in t.groupBy("unit_type").count().collect()
        }
    },
)

# COMMAND ----------

# DBTITLE 1,Transform -- support_registration (EEG and KWK in one table)
registrations = None
for bt, (scheme, unit_type, id_col) in SUPPORT.items():
    df = mastr_standardise(read_bronze(bt), NAME_MAP, CODED)
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
grid_location = locations.join(conflict, "location_id", "left")

# COMMAND ----------

# DBTITLE 1,Transform -- balancing_area (Id not unique: ordinal, never a drop)
_names = {**NAME_MAP, **MASTR_KEY_NAMES[BALANCING_BT]}
_id = _names["Id"]
areas = mastr_standardise(read_bronze(BALANCING_BT).dropDuplicates(), _names, CODED)
areas = within_group_ordinal(
    areas,
    [_id],
    [
        "area_energy_identification_code",
        "control_zone",
        "balancing_area_connection_point",
    ],
)
areas = areas.withColumn("_srid", sha_key(_id, "_source_id_ordinal"))
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
    df = mastr_standardise(read_bronze(bt), NAME_MAP, CODED)
    df = df.withColumn("_srid", sha_key(F.lit(kind), F.col(id_col).cast("string")))
    events.append(
        add_semantic_provenance(df, SOURCE, bt, RID, "_srid").withColumn(
            "event_kind", F.lit(kind)
        )
    )

commissioning = generation_unit.filter(F.col("commissioning_date").isNotNull()).select(
    "unit_id", "commissioning_date"
)
changes = mastr_standardise(read_bronze(CHANGE_BT), NAME_MAP, CODED)
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
]
_UNITS = "VerknuepfteEinheitenMaStRNummern"
# relationship_type -> (parent_type, linked_type, Bronze tables, parent col, link col)
LINKS = {
    "renewable_energy_act_support_unit": (
        "support_registration",
        "generation_unit",
        _EEG,
        "EegMaStRNummer",
        _UNITS,
    ),
    "combined_heat_and_power_support_unit": (
        "support_registration",
        "generation_unit",
        ["mastr_anlagen_kwk"],
        "KwkMastrNummer",
        _UNITS,
    ),
    "authorisation_unit": (
        "unit_authorisation",
        "generation_unit",
        ["mastr_einheiten_genehmigung"],
        "GenMastrNummer",
        _UNITS,
    ),
    "repowering_renewable_energy_act": (
        "unit_repowering",
        "support_registration",
        ["mastr_ertuechtigungen"],
        "Id",
        "EegMastrNummer",
    ),
    "location_unit": (
        "grid_location",
        "generation_unit",
        [LOCATION_BT],
        "MastrNummer",
        _UNITS,
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
links = (
    links.unionByName(actor_roles)
    .dropDuplicates(_link_key)
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
        }
    },
)
