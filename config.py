# Configuration
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: August 2026
#
# Purpose: centralize all configuration settings.
"""
Configuration settings for ECRMAP (Ecosystem-Centric Real-World
Multi-Domain Analytics Platform).
ALL SENSITIVE DATA IN .env FILE - NEVER COMMIT .env TO GITHUB

This file manages:
- PostgreSQL serving-database connection settings
- GCP / BigQuery acquisition configuration
- Databricks configuration
- FastAPI configuration
- Layer-specific settings (Bronze, Silver, Gold)
- AI / RAG configuration
- Logging configuration

Usage:
    from config import get_database_url, GCP_CONFIG
"""

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables first
load_dotenv()

# ====================================================================
# PROJECT ROOT
# ====================================================================

PROJECT_ROOT = Path(__file__).parent
PROJECT_NAME = os.getenv("PROJECT_NAME", "ecrmap")
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

# ====================================================================
# DIRECTORY STRUCTURE
# ====================================================================

# Source directories
SRC_DIR = PROJECT_ROOT / "src"
SCHEMAS_DIR = SRC_DIR / "schemas"
QUALITY_DIR = SRC_DIR / "quality"

# Application directories
FASTAPI_DIR = PROJECT_ROOT / "fastapi"
DATABRICKS_DIR = PROJECT_ROOT / "databricks"
TERRAFORM_DIR = PROJECT_ROOT / "terraform"
GRAFANA_DIR = PROJECT_ROOT / "grafana"
AI_DIR = PROJECT_ROOT / "ai"
POSTGRES_DIR = PROJECT_ROOT / "postgres"

# Data directories (gitignored)
DATA_DIR = PROJECT_ROOT / "data"
DATA_RAW_DIR = DATA_DIR / "raw"
DATA_SAMPLES_DIR = DATA_DIR / "samples"
DATA_STAGING_DIR = DATA_DIR / "staging"
DATA_PROCESSED_DIR = DATA_DIR / "processed"

# Test and scripts directories
TESTS_DIR = PROJECT_ROOT / "tests"
TESTS_UNIT_DIR = TESTS_DIR / "unit"
TESTS_INTEGRATION_DIR = TESTS_DIR / "integration"
TESTS_DATA_QUALITY_DIR = TESTS_DIR / "data_quality"
TESTS_AI_DIR = TESTS_DIR / "ai"
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
SCRIPTS_DOWNLOAD_DIR = SCRIPTS_DIR / "download"
SCRIPTS_UTILITIES_DIR = SCRIPTS_DIR / "utilities"

# Docs directory (local only - gitignored, not committed)
DOCS_DIR = PROJECT_ROOT / "docs"

# Logs directory
LOGS_DIR = PROJECT_ROOT / "logs"
LOGS_INGESTION_DIR = LOGS_DIR / "ingestion"
LOGS_DATABRICKS_DIR = LOGS_DIR / "databricks"
LOGS_API_DIR = LOGS_DIR / "api"
LOGS_AI_DIR = LOGS_DIR / "ai"

# ChromaDB vector store path
CHROMA_DB_PATH = Path(os.getenv("CHROMA_DB_PATH", "./ai/vector_store/data"))

# Auto-create runtime directories on import
for directory in [
    DATA_RAW_DIR,
    DATA_SAMPLES_DIR,
    DATA_STAGING_DIR,
    DATA_PROCESSED_DIR,
    TESTS_UNIT_DIR,
    TESTS_INTEGRATION_DIR,
    TESTS_DATA_QUALITY_DIR,
    TESTS_AI_DIR,
    SCRIPTS_DOWNLOAD_DIR,
    SCRIPTS_UTILITIES_DIR,
    LOGS_INGESTION_DIR,
    LOGS_DATABRICKS_DIR,
    LOGS_API_DIR,
    LOGS_AI_DIR,
    CHROMA_DB_PATH,
]:
    directory.mkdir(parents=True, exist_ok=True)

# ====================================================================
# POSTGRESQL SERVING DATABASE
# Read-only BI serving store, loaded from Databricks via a local export.
# Distinct from anything operational -- it holds only published serving
# datasets. Not yet provisioned; see postgres/serving/00_bootstrap_serving.sql.
# ====================================================================

DATABASE_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "ecrmap_serving"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}


def get_database_url() -> str:
    """Return SQLAlchemy-compatible PostgreSQL serving-database URL."""
    cfg = DATABASE_CONFIG
    return (
        f"postgresql://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    )


# ====================================================================
# GCP CONFIGURATION
# ====================================================================

GCP_CONFIG = {
    "application_credentials": os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
    "project_id": os.getenv("GCP_PROJECT_ID"),
    "region": os.getenv("GCP_REGION", "europe-west3"),
    "billing_account_id": os.getenv("GCP_BILLING_ACCOUNT_ID"),
}

# BigQuery -- Google-source acquisition interface only. One dataset receives
# query output for Google-native sources (GA4 first); the export is pulled
# down locally and loaded into Databricks Bronze.
BIGQUERY_CONFIG = {
    "project_id": os.getenv("BIGQUERY_PROJECT_ID"),
    "datasets": {
        "acquisition": os.getenv("BIGQUERY_DATASET_ACQUISITION", "ecrmap_acquisition"),
    },
}

# ====================================================================
# DATABRICKS CONFIGURATION
# ====================================================================

DATABRICKS_CONFIG = {
    "host": os.getenv("DATABRICKS_HOST"),
    "token": os.getenv("DATABRICKS_TOKEN"),
    "catalog": os.getenv("DATABRICKS_CATALOG", "energy_commerce_retail_media"),
    # Unity Catalog schemas. Silver is split per the 100-table-per-schema
    # quota; domain separation stays at folder level. Commerce schemas are
    # added when the Commerce wave reaches Silver.
    "schemas": {
        "bronze": "bronze",
        "energy_silver": "energy_silver",
        "energy_silver_reference": "energy_silver_reference",
        "energy_gold": "energy_gold",
        "shared_conformed": "shared_conformed",
        "quality": "quality",
        "eda": "eda",
    },
}

# ====================================================================
# FASTAPI CONFIGURATION
# Scoped narrowly to the AI agent's service layer
# ====================================================================

FASTAPI_CONFIG = {
    "host": os.getenv("FASTAPI_HOST", "0.0.0.0"),
    "port": int(os.getenv("FASTAPI_PORT", "8000")),
    "reload": os.getenv("FASTAPI_RELOAD", "true").lower() == "true",
    "title": "ECRMAP Intelligence Agent API",
    "version": "0.1.0",
}

# ====================================================================
# AI / LLM / RAG CONFIGURATION
# ====================================================================

AI_CONFIG = {
    "llm_api_key": os.getenv("LLM_API_KEY"),
    "llm_model": os.getenv("LLM_MODEL"),
    "chroma_db_path": str(CHROMA_DB_PATH),
}

# Controlled tool-calling functions the LLM may use. The LLM never
# receives unrestricted database access.
AI_TOOLS = [
    "get_metric",
    "compare_periods",
    "analyse_market_conditions",
    "analyse_weather_conditions",
    "analyse_customer_demand",
]

# ====================================================================
# BRONZE LAYER CONFIGURATION
# Preserves what arrived, unmodified, source-attributed
# ====================================================================

BRONZE_CONFIG = {
    "compression": os.getenv("BRONZE_PARQUET_COMPRESSION", "snappy"),
    "partition_keys": [
        "source_system",
        "event_type",
        "year",
        "month",
        "day",
        "hour",
    ],
}

# ====================================================================
# SILVER LAYER CONFIGURATION
# Cleaned, validated, normalized per source's data contract
# ====================================================================

SILVER_CONFIG = {
    "completeness_threshold": float(os.getenv("SILVER_COMPLETENESS_THRESHOLD", "0.95")),
    "uniqueness_threshold": float(os.getenv("SILVER_UNIQUENESS_THRESHOLD", "1.0")),
    "batch_size": int(os.getenv("SILVER_BATCH_SIZE", "10000")),
}

# ====================================================================
# GOLD LAYER CONFIGURATION
# Integrated across domains, canonically mapped, includes Germany
# localisation mapping
# ====================================================================

GOLD_CONFIG = {
    "snapshot_interval_hours": int(os.getenv("GOLD_SNAPSHOT_INTERVAL_HOURS", "1")),
    "retention_days": int(os.getenv("GOLD_RETENTION_DAYS", "90")),
    "aggregation_batch_size": int(os.getenv("GOLD_AGGREGATION_BATCH_SIZE", "50000")),
    # Every canonical entity carries source_system + source_record_id +
    # canonical_id
    "required_provenance_fields": ["source_system", "source_record_id", "canonical_id"],
    "scd2_tables": ["dim_customer"],
    "scd1_tables": ["dim_product"],
    "static_tables": [
        "dim_date",
        "dim_time",
        "dim_geography",
        "dim_market",
        "dim_weather_location",
    ],
}

# ====================================================================
# ANOMALY DETECTION CONFIGURATION
# ML signal plus GenAI explanation
# ====================================================================

ANOMALY_CONFIG = {
    "demand_spike_std_multiplier": 2.0,
    "price_volatility_threshold_pct": 20.0,
    "consumption_anomaly_threshold_pct": 15.0,
}

# ====================================================================
# DOMAINS CONFIGURATION
# ====================================================================

DOMAINS = {
    "energy_market": {
        "name": "Energy Market",
        "source": "SMARD",
    },
    "weather": {
        "name": "Weather",
        "source": "DWD",
    },
    "iot_consumption": {
        "name": "IoT / Consumption",
        "source": "Honda Research Institute Europe Smart Building Dataset",
    },
    "commerce": {
        "name": "Commerce",
        "source": "GA4 (primary), REES46, Search Visibility",
        "is_german_source": False,
    },
}

# ====================================================================
# LOGGING CONFIGURATION
# ====================================================================

LOGGING_CONFIG = {
    "level": os.getenv("LOG_LEVEL", "INFO"),
    "format": "%(asctime)s [%(levelname)8s] %(name)s - %(message)s",
    "date_format": "%Y-%m-%d %H:%M:%S",
    "app_log": str(LOGS_DIR / "app.log"),
    "error_log": str(LOGS_DIR / "error.log"),
    "ingestion_log": str(LOGS_INGESTION_DIR / "ingestion.log"),
    "databricks_log": str(LOGS_DATABRICKS_DIR / "databricks.log"),
    "api_log": str(LOGS_API_DIR / "api.log"),
    "ai_log": str(LOGS_AI_DIR / "ai.log"),
}


def get_logger(name: str) -> logging.Logger:
    """
    Get a configured logger for the given module name.
    Routes logs to appropriate layer-specific log files.

    Usage:
        from config import get_logger
        logger = get_logger(__name__)
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    level = getattr(logging, LOGGING_CONFIG["level"].upper(), logging.INFO)
    fmt = logging.Formatter(
        LOGGING_CONFIG["format"], datefmt=LOGGING_CONFIG["date_format"]
    )

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(fmt)

    # Master app log handler
    app_handler = RotatingFileHandler(
        LOGGING_CONFIG["app_log"],
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    app_handler.setLevel(level)
    app_handler.setFormatter(fmt)

    # Error log handler
    error_handler = logging.FileHandler(LOGGING_CONFIG["error_log"], encoding="utf-8")
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(fmt)

    # Layer-specific log routing
    layer_log_file = None

    if "ingestion" in name:
        layer_log_file = LOGGING_CONFIG["ingestion_log"]
    elif "databricks" in name:
        layer_log_file = LOGGING_CONFIG["databricks_log"]
    elif "api" in name or "fastapi" in name or "router" in name:
        layer_log_file = LOGGING_CONFIG["api_log"]
    elif "ai" in name or "agent" in name:
        layer_log_file = LOGGING_CONFIG["ai_log"]

    if layer_log_file:
        layer_handler = logging.FileHandler(layer_log_file, encoding="utf-8")
        layer_handler.setLevel(level)
        layer_handler.setFormatter(fmt)
        logger.addHandler(layer_handler)

    logger.setLevel(level)
    logger.addHandler(ch)
    logger.addHandler(app_handler)
    logger.addHandler(error_handler)
    logger.propagate = False

    return logger


# ====================================================================
# VALIDATION
# ====================================================================


def validate_config() -> bool:
    """Validate critical configuration settings."""
    errors = []

    if not GCP_CONFIG["application_credentials"]:
        errors.append("GOOGLE_APPLICATION_CREDENTIALS not set in .env")

    if not GCP_CONFIG["project_id"]:
        errors.append("GCP_PROJECT_ID not set in .env")

    if not DATABRICKS_CONFIG["token"]:
        errors.append("DATABRICKS_TOKEN not set in .env")

    if errors:
        print("Configuration errors:")
        for error in errors:
            print(f"  - {error}")
        return False

    return True


# ====================================================================
# MAIN - RUN DIRECTLY TO VERIFY CONFIG
# ====================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("ENERGY COMMERCE & RETAIL MEDIA ANALYTICS PLATFORM - CONFIGURATION")
    print("=" * 70)
    print(f"Project root:        {PROJECT_ROOT}")
    print(f"Environment:         {ENVIRONMENT}")
    print(f"Serving database:    {DATABASE_CONFIG['database']}")
    print(f"Serving DB host:     {DATABASE_CONFIG['host']}")
    print(f"GCP project:         {GCP_CONFIG['project_id']}")
    print(f"GCP region:          {GCP_CONFIG['region']}")
    print(f"BigQuery project:    {BIGQUERY_CONFIG['project_id']}")
    print(f"BQ acquisition ds:   {BIGQUERY_CONFIG['datasets']['acquisition']}")
    print(f"Databricks host:     {DATABRICKS_CONFIG['host']}")
    print(f"Data directory:      {DATA_DIR}")
    print(f"Logs directory:      {LOGS_DIR}")

    print("\n" + "=" * 70)
    print("CONFIGURATION VALIDATION")
    print("=" * 70)

    if validate_config():
        print("Status: PASSED")
    else:
        print("Status: FAILED - Fix errors above")
