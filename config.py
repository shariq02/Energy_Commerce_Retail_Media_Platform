# ====================================================================
# Configuration Settings for Energy Commerce & Retail Media Analytics
# Platform
# Author: Sharique Mohammad
# Date: August 2026
# ====================================================================
# FILE: config.py (Project Root)
# Purpose: Centralize all configuration settings
# ====================================================================
"""
Configuration settings for the Energy Commerce & Retail Media Analytics
Platform.
ALL SENSITIVE DATA IN .env FILE - NEVER COMMIT .env TO GITHUB

This file manages:
- PostgreSQL connection settings
- Redpanda/Kafka connection settings
- GCP / GCS / BigQuery configuration
- CDC / Debezium configuration
- Databricks configuration
- Dagster configuration
- dbt configuration
- FastAPI configuration
- Data generator configuration
- Layer-specific settings (Landing, Standardized, Canonical/Curated)
- AI / RAG configuration
- Logging configuration

Usage:
    from config import get_database_url, KAFKA_CONFIG, GCP_CONFIG
"""

import os
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables first
load_dotenv()

# ====================================================================
# PROJECT ROOT
# ====================================================================

PROJECT_ROOT = Path(__file__).parent
PROJECT_NAME = os.getenv('PROJECT_NAME', 'energy-commerce-retail-media-platform')
ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')

# ====================================================================
# DIRECTORY STRUCTURE
# ====================================================================

# Source directories
SRC_DIR = PROJECT_ROOT / "src"
INGESTION_DIR = SRC_DIR / "ingestion"
GENERATORS_DIR = SRC_DIR / "generators"
SCHEMAS_DIR = SRC_DIR / "schemas"
QUALITY_DIR = SRC_DIR / "quality"

# Application directories
FASTAPI_DIR = PROJECT_ROOT / "fastapi"
DBT_DIR = PROJECT_ROOT / "dbt"
DATABRICKS_DIR = PROJECT_ROOT / "databricks"
DAGSTER_DIR = PROJECT_ROOT / "dagster"
TERRAFORM_DIR = PROJECT_ROOT / "terraform"
GRAFANA_DIR = PROJECT_ROOT / "grafana"
AI_DIR = PROJECT_ROOT / "ai"
POSTGRES_DIR = PROJECT_ROOT / "postgres"
REDPANDA_DIR = PROJECT_ROOT / "redpanda"
CDC_DIR = PROJECT_ROOT / "cdc"

# SQL directories
SQL_DIR = PROJECT_ROOT / "sql"
SQL_DDL_DIR = SQL_DIR / "ddl"
SQL_ANALYTICAL_DIR = SQL_DIR / "analytical"
SQL_VALIDATION_DIR = SQL_DIR / "validation"

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
TESTS_PIPELINES_DIR = TESTS_DIR / "pipelines"
TESTS_AI_DIR = TESTS_DIR / "ai"
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
SCRIPTS_SETUP_DIR = SCRIPTS_DIR / "setup"
SCRIPTS_DOWNLOAD_DIR = SCRIPTS_DIR / "download"
SCRIPTS_UTILITIES_DIR = SCRIPTS_DIR / "utilities"

# Docs directory (local only - gitignored, not committed)
DOCS_DIR = PROJECT_ROOT / "docs"

# Logs directory
LOGS_DIR = PROJECT_ROOT / "logs"
LOGS_INGESTION_DIR = LOGS_DIR / "ingestion"
LOGS_DATABRICKS_DIR = LOGS_DIR / "databricks"
LOGS_DBT_DIR = LOGS_DIR / "dbt"
LOGS_DAGSTER_DIR = LOGS_DIR / "dagster"
LOGS_API_DIR = LOGS_DIR / "api"
LOGS_AI_DIR = LOGS_DIR / "ai"

# ChromaDB vector store path
CHROMA_DB_PATH = Path(os.getenv('CHROMA_DB_PATH', './ai/vector_store/data'))

# Auto-create runtime directories on import
for directory in [
    DATA_RAW_DIR,
    DATA_SAMPLES_DIR,
    DATA_STAGING_DIR,
    DATA_PROCESSED_DIR,
    TESTS_UNIT_DIR,
    TESTS_INTEGRATION_DIR,
    TESTS_DATA_QUALITY_DIR,
    TESTS_PIPELINES_DIR,
    TESTS_AI_DIR,
    SCRIPTS_SETUP_DIR,
    SCRIPTS_DOWNLOAD_DIR,
    SCRIPTS_UTILITIES_DIR,
    LOGS_INGESTION_DIR,
    LOGS_DATABRICKS_DIR,
    LOGS_DBT_DIR,
    LOGS_DAGSTER_DIR,
    LOGS_API_DIR,
    LOGS_AI_DIR,
    CHROMA_DB_PATH,
]:
    directory.mkdir(parents=True, exist_ok=True)

# ====================================================================
# POSTGRESQL DATABASE (operational system)
# ====================================================================

DATABASE_CONFIG = {
    'host': os.getenv('POSTGRES_HOST'),
    'port': int(os.getenv('POSTGRES_PORT')),
    'database': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
}


def get_database_url() -> str:
    """Return SQLAlchemy-compatible PostgreSQL connection URL."""
    cfg = DATABASE_CONFIG
    return (
        f"postgresql://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    )


# ====================================================================
# REDPANDA / KAFKA CONFIGURATION
# ====================================================================

KAFKA_CONFIG = {
    'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'client_id': os.getenv('KAFKA_CLIENT_ID', 'ecrmap-producer'),
    'compression_type': os.getenv('KAFKA_COMPRESSION_TYPE', 'gzip'),
    'acks': os.getenv('KAFKA_ACKS', 'all'),
    'retries': int(os.getenv('KAFKA_RETRIES', 3)),
    'batch_size': int(os.getenv('KAFKA_BATCH_SIZE', 16384)),
    'linger_ms': int(os.getenv('KAFKA_LINGER_MS', 10)),
}

# Topics carry replayed historical data (IoT, REES46, iPinYou) plus CDC
# change events - see PIPELINE_DESIGN Section 2-3
KAFKA_TOPICS = {
    'market': os.getenv('TOPIC_MARKET', 'market.events'),
    'weather': os.getenv('TOPIC_WEATHER', 'weather.events'),
    'iot': os.getenv('TOPIC_IOT', 'iot.consumption'),
    'commerce': os.getenv('TOPIC_COMMERCE', 'commerce.events'),
    'retail_media': os.getenv('TOPIC_RETAIL_MEDIA', 'retailmedia.events'),
    'cdc_operational': os.getenv('TOPIC_CDC_OPERATIONAL', 'cdc.operational'),
}

KAFKA_CONSUMER_GROUPS = {
    'databricks_streaming': 'databricks-streaming-group',
    'cdc_consumer': 'cdc-consumer-group',
}

# ====================================================================
# GCP CONFIGURATION
# ====================================================================

GCP_CONFIG = {
    'application_credentials': os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
    'project_id': os.getenv('GCP_PROJECT_ID'),
    'region': os.getenv('GCP_REGION', 'europe-west3'),
    'billing_account_id': os.getenv('GCP_BILLING_ACCOUNT_ID'),
}

# Google Cloud Storage
GCS_CONFIG = {
    'raw_bucket': os.getenv('GCS_RAW_BUCKET', 'ecrmap-dev-raw'),
    'staged_bucket': os.getenv('GCS_STAGED_BUCKET', 'ecrmap-dev-staged'),
}

# BigQuery
# See docs/TECHNOLOGY_BASELINE for the Sandbox vs bounded-cost operating
# mode decision (ADR-011 consequences) - CDC upserts and streaming loads
# require a billing-enabled configuration, not the free Sandbox.
BIGQUERY_CONFIG = {
    'project_id': os.getenv('BIGQUERY_PROJECT_ID'),
    'datasets': {
        'raw': os.getenv('BIGQUERY_DATASET_RAW', 'ecrmap_raw'),
        'staging': os.getenv('BIGQUERY_DATASET_STAGING', 'ecrmap_staging'),
        'core': os.getenv('BIGQUERY_DATASET_CORE', 'ecrmap_core'),
        'marts': os.getenv('BIGQUERY_DATASET_MARTS', 'ecrmap_marts'),
        'semantic': os.getenv('BIGQUERY_DATASET_SEMANTIC', 'ecrmap_semantic'),
    },
}

# ====================================================================
# CDC / DEBEZIUM CONFIGURATION
# ====================================================================

DEBEZIUM_CONFIG = {
    'connector_name': os.getenv('DEBEZIUM_CONNECTOR_NAME', 'ecrmap-postgres-connector'),
    'slot_name': os.getenv('DEBEZIUM_SLOT_NAME', 'ecrmap_slot'),
    'topic_prefix': os.getenv('DEBEZIUM_TOPIC_PREFIX', 'cdc'),
    # Entities captured from the operational PostgreSQL system -
    # DATA_SOURCES Section 9
    'tables': [
        'customers',
        'customer_contracts',
        'tariffs',
        'meters',
        'products',
        'orders',
        'advertisers',
        'campaigns',
        'campaign_budgets',
    ],
}

# ====================================================================
# DATABRICKS CONFIGURATION
# ====================================================================

DATABRICKS_CONFIG = {
    'host': os.getenv('DATABRICKS_HOST'),
    'token': os.getenv('DATABRICKS_TOKEN'),
    'token_dbt': os.getenv('DATABRICKS_TOKEN_DBT'),
    'catalog': 'ecrmap',
    # Layer names per ADR-011 - Landing / Standardized / Canonical,
    # not Bronze / Silver / Gold
    'schemas': {
        'eda': 'eda',
        'landing': 'landing',
        'standardized': 'standardized',
        'canonical': 'canonical',
        'quality': 'quality',
    },
}

# ====================================================================
# DAGSTER CONFIGURATION
# ====================================================================

DAGSTER_CONFIG = {
    'home': os.getenv('DAGSTER_HOME'),
    'schedules': {
        'batch_ingestion': '0 1 * * *',
        'standardized_transform': '0 2 * * *',
        'canonical_build': '0 3 * * *',
        'dbt_run': '0 4 * * *',
        'quality_report': '0 5 * * *',
    },
}

# ====================================================================
# DBT CONFIGURATION
# ====================================================================

DBT_CONFIG = {
    'profiles_dir': os.getenv('DBT_PROFILES_DIR'),
    'project_dir': str(DBT_DIR),
    # dbt targets BigQuery (dbt-bigquery adapter), not Databricks -
    # see ADR-011: BigQuery is the analytical platform, dbt owns the
    # analytical modelling on top of it
    'target': ENVIRONMENT,
}

# ====================================================================
# FASTAPI CONFIGURATION
# Scoped narrowly to the AI agent's service layer - see ADR-007
# ====================================================================

FASTAPI_CONFIG = {
    'host': os.getenv('FASTAPI_HOST', '0.0.0.0'),
    'port': int(os.getenv('FASTAPI_PORT', 8000)),
    'reload': os.getenv('FASTAPI_RELOAD', 'true').lower() == 'true',
    'title': 'Energy Commerce Intelligence Agent API',
    'version': '0.1.0',
}

# ====================================================================
# AI / LLM / RAG CONFIGURATION
# ====================================================================

AI_CONFIG = {
    'llm_api_key': os.getenv('LLM_API_KEY'),
    'llm_model': os.getenv('LLM_MODEL'),
    'chroma_db_path': str(CHROMA_DB_PATH),
}

# Controlled tool-calling functions the LLM may use - TECHNOLOGY_BASELINE
# Section 20, AI_DESIGN Section 3. The LLM never receives unrestricted
# database access.
AI_TOOLS = [
    'get_metric',
    'compare_periods',
    'analyse_market_conditions',
    'analyse_weather_conditions',
    'analyse_customer_demand',
    'analyse_campaign_performance',
]

# ====================================================================
# DATA GENERATOR CONFIGURATION
# Replay of historical public data (IoT, REES46, iPinYou) as simulated
# real-time events - PIPELINE_DESIGN Section 2
# ====================================================================

GENERATOR_CONFIG = {
    'events_per_second': int(os.getenv('GENERATOR_EVENTS_PER_SECOND', 10)),
    'replay_speed': int(os.getenv('GENERATOR_REPLAY_SPEED', 60)),
    'anomaly_mode': os.getenv('GENERATOR_ANOMALY_MODE', 'false').lower() == 'true',
    'anomaly_probability': float(os.getenv('GENERATOR_ANOMALY_PROBABILITY', 0.05)),
    'sources': ['iot', 'commerce', 'retail_media'],
}

# ====================================================================
# LANDING LAYER CONFIGURATION
# Preserves what arrived, unmodified, source-attributed - ARCHITECTURE
# Section 7, ADR-011
# ====================================================================

LANDING_CONFIG = {
    'poll_timeout': int(os.getenv('LANDING_POLL_TIMEOUT', 1000)),
    'max_poll_records': int(os.getenv('LANDING_MAX_POLL_RECORDS', 500)),
    'auto_offset_reset': os.getenv('LANDING_AUTO_OFFSET_RESET', 'earliest'),
    'write_interval': int(os.getenv('LANDING_WRITE_INTERVAL', 60)),
    'compression': os.getenv('LANDING_PARQUET_COMPRESSION', 'snappy'),
    'checkpoint_interval': int(os.getenv('LANDING_CHECKPOINT_INTERVAL', 300)),
    'gcs_partition_keys': ['source_system', 'event_type', 'year', 'month', 'day', 'hour'],
}

# ====================================================================
# STANDARDIZED LAYER CONFIGURATION
# Cleaned, validated, normalized per source's data contract - ADR-011
# ====================================================================

STANDARDIZED_CONFIG = {
    'completeness_threshold': float(os.getenv('STANDARDIZED_COMPLETENESS_THRESHOLD', 0.95)),
    'uniqueness_threshold': float(os.getenv('STANDARDIZED_UNIQUENESS_THRESHOLD', 1.0)),
    'batch_size': int(os.getenv('STANDARDIZED_BATCH_SIZE', 10000)),
}

# ====================================================================
# CANONICAL / CURATED LAYER CONFIGURATION
# Integrated across domains, canonically mapped, includes Germany
# localisation mapping (UC-10) - ADR-011
# ====================================================================

CANONICAL_CONFIG = {
    'snapshot_interval_hours': int(os.getenv('CANONICAL_SNAPSHOT_INTERVAL_HOURS', 1)),
    'retention_days': int(os.getenv('CANONICAL_RETENTION_DAYS', 90)),
    'aggregation_batch_size': int(os.getenv('CANONICAL_AGGREGATION_BATCH_SIZE', 50000)),
    # Every canonical entity carries source_system + source_record_id +
    # canonical_id - DATA_MODEL Section 8
    'required_provenance_fields': ['source_system', 'source_record_id', 'canonical_id'],
    'scd2_tables': ['dim_customer', 'dim_advertiser'],
    'scd1_tables': ['dim_product', 'dim_campaign'],
    'static_tables': ['dim_date', 'dim_time', 'dim_geography', 'dim_market', 'dim_weather_location'],
}

# ====================================================================
# ANOMALY DETECTION CONFIGURATION
# UC-04 - ML signal built Phase 14, GenAI explanation added Phase 16
# ====================================================================

ANOMALY_CONFIG = {
    'demand_spike_std_multiplier': 2.0,
    'price_volatility_threshold_pct': 20.0,
    'consumption_anomaly_threshold_pct': 15.0,
}

# ====================================================================
# DOMAINS CONFIGURATION
# ====================================================================

DOMAINS = {
    'energy_market': {
        'name': 'Energy Market',
        'source': 'SMARD',
    },
    'weather': {
        'name': 'Weather',
        'source': 'DWD',
    },
    'iot_consumption': {
        'name': 'IoT / Consumption',
        'topic': KAFKA_TOPICS['iot'],
        'source': 'Honda Research Institute Europe Smart Building Dataset',
    },
    'commerce': {
        'name': 'Commerce',
        'topic': KAFKA_TOPICS['commerce'],
        'source': 'REES46',
        'is_german_source': False,
    },
    'retail_media': {
        'name': 'Retail Media',
        'topic': KAFKA_TOPICS['retail_media'],
        'source': 'iPinYou',
        'is_german_source': False,
    },
    'energy_retail': {
        'name': 'Energy Retail',
        'source': 'synthetic operational data (CDC)',
    },
}

# ====================================================================
# LOGGING CONFIGURATION
# ====================================================================

LOGGING_CONFIG = {
    'level': os.getenv('LOG_LEVEL', 'INFO'),
    'format': '%(asctime)s [%(levelname)8s] %(name)s - %(message)s',
    'date_format': '%Y-%m-%d %H:%M:%S',
    'app_log': str(LOGS_DIR / 'app.log'),
    'error_log': str(LOGS_DIR / 'error.log'),
    'ingestion_log': str(LOGS_INGESTION_DIR / 'ingestion.log'),
    'databricks_log': str(LOGS_DATABRICKS_DIR / 'databricks.log'),
    'dbt_log': str(LOGS_DBT_DIR / 'dbt.log'),
    'dagster_log': str(LOGS_DAGSTER_DIR / 'dagster.log'),
    'api_log': str(LOGS_API_DIR / 'api.log'),
    'ai_log': str(LOGS_AI_DIR / 'ai.log'),
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

    level = getattr(logging, LOGGING_CONFIG['level'].upper(), logging.INFO)
    fmt = logging.Formatter(
        LOGGING_CONFIG['format'],
        datefmt=LOGGING_CONFIG['date_format']
    )

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(fmt)

    # Master app log handler
    app_handler = RotatingFileHandler(
        LOGGING_CONFIG['app_log'],
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding='utf-8'
    )
    app_handler.setLevel(level)
    app_handler.setFormatter(fmt)

    # Error log handler
    error_handler = logging.FileHandler(
        LOGGING_CONFIG['error_log'],
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(fmt)

    # Layer-specific log routing
    layer_log_file = None

    if 'ingestion' in name or 'generator' in name:
        layer_log_file = LOGGING_CONFIG['ingestion_log']
    elif 'databricks' in name:
        layer_log_file = LOGGING_CONFIG['databricks_log']
    elif 'dbt' in name:
        layer_log_file = LOGGING_CONFIG['dbt_log']
    elif 'dagster' in name:
        layer_log_file = LOGGING_CONFIG['dagster_log']
    elif 'api' in name or 'fastapi' in name or 'router' in name:
        layer_log_file = LOGGING_CONFIG['api_log']
    elif 'ai' in name or 'agent' in name:
        layer_log_file = LOGGING_CONFIG['ai_log']

    if layer_log_file:
        layer_handler = logging.FileHandler(layer_log_file, encoding='utf-8')
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

    if not DATABASE_CONFIG['password']:
        errors.append("POSTGRES_PASSWORD not set in .env")

    if not KAFKA_CONFIG['bootstrap_servers']:
        errors.append("KAFKA_BOOTSTRAP_SERVERS not set in .env")

    if not GCP_CONFIG['application_credentials']:
        errors.append("GOOGLE_APPLICATION_CREDENTIALS not set in .env")

    if not GCP_CONFIG['project_id']:
        errors.append("GCP_PROJECT_ID not set in .env")

    if not DATABRICKS_CONFIG['token']:
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

if __name__ == '__main__':
    print("=" * 70)
    print("ENERGY COMMERCE & RETAIL MEDIA ANALYTICS PLATFORM - CONFIGURATION")
    print("=" * 70)
    print(f"Project root:        {PROJECT_ROOT}")
    print(f"Environment:         {ENVIRONMENT}")
    print(f"Database:            {DATABASE_CONFIG['database']}")
    print(f"Database host:       {DATABASE_CONFIG['host']}")
    print(f"Kafka bootstrap:     {KAFKA_CONFIG['bootstrap_servers']}")
    print(f"GCP project:         {GCP_CONFIG['project_id']}")
    print(f"GCP region:          {GCP_CONFIG['region']}")
    print(f"GCS raw bucket:      {GCS_CONFIG['raw_bucket']}")
    print(f"BigQuery project:    {BIGQUERY_CONFIG['project_id']}")
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
