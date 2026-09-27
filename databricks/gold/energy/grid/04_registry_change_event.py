# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- REGISTRY_CHANGE_EVENT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_gold.registry_change_event` -- one log combining
# MAGIC `unit_deletion_event`, `actor_deletion_event` and
# MAGIC `grid_operator_change_event`, with `event_type`/`subject_type` and every
# MAGIC type-specific column kept (NULL where irrelevant). Grain: subject x event
# MAGIC type x effective date x ordinal.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../../_gold_common

# COMMAND ----------

# DBTITLE 1,Gold inspection library
# MAGIC %run ../../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import Window

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = "gold/energy/grid/registry_change_event"
TABLE = "registry_change_event"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver
unit_deletion_event = read_silver("unit_deletion_event")
actor_deletion_event = read_silver("actor_deletion_event")
grid_operator_change_event = read_silver("grid_operator_change_event")

# COMMAND ----------

# DBTITLE 1,Common column shape -- NULL where a source type has no such field
_COLUMN_TYPES = [
    ("subject_type", "string"),
    ("subject_id", "string"),
    ("event_type", "string"),
    ("effective_date", "timestamp"),
    ("status_code", "string"),
    ("status_label_de", "string"),
    ("status", "string"),
    ("change_type", "string"),
    ("previous_grid_operator_id", "string"),
    ("new_grid_operator_id", "string"),
    ("location_id", "string"),
    ("connection_point_id", "string"),
    ("registered_date", "timestamp"),
    ("date_order_violation_registered_before_effective", "boolean"),
    ("date_order_violation_commissioning_after_change", "boolean"),
    ("unmatched_code_columns", "string"),
    ("source_record_id", "string"),
    ("source_dataset", "string"),
    ("source_system", "string"),
]


def _conform(df, mapping):
    return df.select(
        *[
            (F.col(mapping[c]) if c in mapping else F.lit(None)).cast(t).alias(c)
            for c, t in _COLUMN_TYPES
        ]
    )


# COMMAND ----------

# DBTITLE 1,Build the unit-deletion rows
_unit = _conform(
    unit_deletion_event.withColumn("_subject_type", F.lit("unit")).withColumn(
        "_event_type", F.lit("unit_deletion")
    ),
    {
        "subject_type": "_subject_type",
        "subject_id": "unit_id",
        "event_type": "_event_type",
        "effective_date": "last_updated_at",
        "status_code": "operating_status_code",
        "status_label_de": "operating_status_label_de",
        "status": "operating_status",
        "unmatched_code_columns": "_unmatched_code_columns",
        "source_record_id": "source_record_id",
        "source_dataset": "source_dataset",
        "source_system": "source_system",
    },
)

# COMMAND ----------

# DBTITLE 1,Build the actor-deletion rows
_actor = _conform(
    actor_deletion_event.withColumn("_subject_type", F.lit("actor")).withColumn(
        "_event_type", F.lit("actor_deletion")
    ),
    {
        "subject_type": "_subject_type",
        "subject_id": "market_actor_id",
        "event_type": "_event_type",
        "effective_date": "last_updated_at",
        "status_code": "actor_status_code",
        "status_label_de": "actor_status_label_de",
        "status": "actor_status",
        "unmatched_code_columns": "_unmatched_code_columns",
        "source_record_id": "source_record_id",
        "source_dataset": "source_dataset",
        "source_system": "source_system",
    },
)

# COMMAND ----------

# DBTITLE 1,Build the grid-operator-change rows
_operator_change = _conform(
    grid_operator_change_event.withColumn("_subject_type", F.lit("unit")).withColumn(
        "_event_type", F.lit("grid_operator_change")
    ),
    {
        "subject_type": "_subject_type",
        "subject_id": "unit_id",
        "event_type": "_event_type",
        "effective_date": "grid_operator_change_effective_date",
        "change_type": "change_type",
        "previous_grid_operator_id": "previous_grid_operator_id",
        "new_grid_operator_id": "new_grid_operator_id",
        "location_id": "location_id",
        "connection_point_id": "connection_point_id",
        "registered_date": "grid_operator_change_registered_date",
        "date_order_violation_registered_before_effective": (
            "_date_order_violation_registered_before_effective"
        ),
        "date_order_violation_commissioning_after_change": (
            "_date_order_violation_commissioning_after_change"
        ),
        "unmatched_code_columns": "_unmatched_code_columns",
        "source_record_id": "source_record_id",
        "source_dataset": "source_dataset",
        "source_system": "source_system",
    },
)

# COMMAND ----------

# DBTITLE 1,Union and derive the ordinal
registry_change_event = _unit.unionByName(_actor).unionByName(_operator_change)
_w = Window.partitionBy("subject_type", "subject_id", "event_type").orderBy(
    F.col("effective_date").asc_nulls_last()
)
registry_change_event = registry_change_event.withColumn(
    "event_ordinal", F.row_number().over(_w)
)
registry_change_event = add_gold_provenance(registry_change_event, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
assert_unique_grain(
    registry_change_event,
    ["subject_type", "subject_id", "event_type", "effective_date", "event_ordinal"],
    component=COMPONENT,
    source=SOURCE,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(registry_change_event, TABLE, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    registry_change_event,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=[
        "subject_type",
        "subject_id",
        "event_type",
        "effective_date",
        "event_ordinal",
    ],
)
write_gold_findings(SOURCE, f"grid__{TABLE}", TABLE, _blocks)
