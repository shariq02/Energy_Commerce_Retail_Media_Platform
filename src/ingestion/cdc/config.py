"""CDC pipeline -- shared configuration.

Energy Commerce and Retail Media Analytics Platform
Author: Sharique Mohammad
Date: August 2026

Purpose: the fixed names and paths the CDC pieces share -- the 7 operational
tables, their Debezium topic names, the local landing/state locations, and the
broker / schema-registry / Databricks endpoints (from the environment).
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parents[3]

# The 7 physical operational tables, parents first. Every table is captured as
# an independent stream; order_items is not folded into orders.
TABLES: tuple[str, ...] = (
    "tariffs",
    "products",
    "customers",
    "customer_contracts",
    "meters",
    "orders",
    "order_items",
)

# Primary key column per table -- the stable row identity for dedup and merge.
PRIMARY_KEY: dict[str, str] = {
    "tariffs": "tariff_id",
    "products": "product_id",
    "customers": "customer_id",
    "customer_contracts": "contract_id",
    "meters": "meter_id",
    "orders": "order_id",
    "order_items": "order_item_id",
}

SOURCE_SCHEMA = "operational"

# Debezium topic naming: "<prefix>.<schema>.<table>", one topic per table.
TOPIC_PREFIX = os.getenv("DEBEZIUM_TOPIC_PREFIX", "ecrmap")
PUBLICATION_NAME = os.getenv("DEBEZIUM_PUBLICATION_NAME", "ecrmap_cdc_pub")
SLOT_NAME = os.getenv("DEBEZIUM_SLOT_NAME", "ecrmap_cdc_slot")
CONNECTOR_NAME = os.getenv("DEBEZIUM_CONNECTOR_NAME", "ecrmap-postgres-connector")
SNAPSHOT_MODE = os.getenv("DEBEZIUM_SNAPSHOT_MODE", "initial")


def topic_for(table: str) -> str:
    """Full Debezium topic name for an operational table."""
    return f"{TOPIC_PREFIX}.{SOURCE_SCHEMA}.{table}"


def all_topics() -> list[str]:
    return [topic_for(t) for t in TABLES]


def qualified_table(table: str) -> str:
    return f"{SOURCE_SCHEMA}.{table}"


# --- infrastructure endpoints ------------------------------------------------

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
SCHEMA_COMPATIBILITY = os.getenv("SCHEMA_REGISTRY_COMPATIBILITY", "BACKWARD")
CONSUMER_GROUP = os.getenv("CDC_CONSUMER_GROUP", "ecrmap-cdc-consumer")
CONNECT_REST_URL = os.getenv("KAFKA_CONNECT_REST_URL", "http://localhost:8083")

# Source PostgreSQL -- the consumer reads the cluster identity once at startup to
# detect a rebuilt/restored source lifetime (see source_identity.py).
POSTGRES = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "ecrmap"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
}

# --- local landing / state -------------------------------------------------

CDC_DATA_DIR = REPO_ROOT / "data" / "cdc"
LANDING_DIR = CDC_DATA_DIR / "landing"
STATE_DIR = CDC_DATA_DIR / "state"
SNAPSHOT_DIR = CDC_DATA_DIR / "snapshots"
STATE_DB = STATE_DIR / "consumer_state.db"

# Kafka Connect standalone offset file (outside data/, kept with the config).
CONNECT_DIR = REPO_ROOT / "cdc" / "connect"
CONNECT_OFFSETS = CONNECT_DIR / "offsets" / "connect.offsets"


def landing_dir(table: str) -> Path:
    return LANDING_DIR / table


def ensure_dirs() -> None:
    for path in (LANDING_DIR, STATE_DIR, SNAPSHOT_DIR, CONNECT_OFFSETS.parent):
        path.mkdir(parents=True, exist_ok=True)
    for table in TABLES:
        landing_dir(table).mkdir(parents=True, exist_ok=True)


# --- Databricks Volume target (upload leg) --------------------------------

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN_DBT") or os.getenv("DATABRICKS_TOKEN")
DATABRICKS_CATALOG = os.getenv("DATABRICKS_CATALOG", "energy_commerce_retail_media")
CDC_VOLUME = "cdc_operational_landing"


def volume_root() -> str:
    return f"/Volumes/{DATABRICKS_CATALOG}/bronze/{CDC_VOLUME}"
