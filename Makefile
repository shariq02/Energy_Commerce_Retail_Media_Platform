# Energy Commerce & Retail Media Analytics Platform - Makefile
# Common commands for development workflow

# Keep __pycache__ out of the tree for every recipe below.
export PYTHONDONTWRITEBYTECODE := 1

.PHONY: help setup setup-dev test test-unit test-integration test-regression test-performance lint lint-fix terraform-init terraform-plan terraform-apply terraform-destroy redpanda-start redpanda-stop cdc-install cdc-topics cdc-connect-start cdc-connect-stop cdc-schemas cdc-driver cdc-consume cdc-sync cdc-cycle dagster-start dbt-run dbt-test grafana-start generator-start api-start gcp-auth clean

help:
	@echo "Energy Commerce & Retail Media Analytics Platform - Available Commands"
	@echo "========================================================================"
	@echo ""
	@echo "SETUP"
	@echo "  setup              - Install Python dependencies"
	@echo "  setup-dev          - Install with development dependencies"
	@echo ""
	@echo "TESTING"
	@echo "  test               - Run all tests"
	@echo "  test-unit          - Run unit tests only"
	@echo "  test-integration   - Run integration tests only"
	@echo "  test-regression    - Run regression tests only"
	@echo "  test-performance   - Run performance tests only"
	@echo ""
	@echo "CODE QUALITY"
	@echo "  lint               - Run ruff linter"
	@echo "  lint-fix           - Auto-fix ruff errors"
	@echo ""
	@echo "GCP / TERRAFORM"
	@echo "  gcp-auth           - Authenticate gcloud application-default credentials"
	@echo "  terraform-init     - Initialise Terraform"
	@echo "  terraform-plan     - Show Terraform execution plan"
	@echo "  terraform-apply    - Apply Terraform changes to GCP"
	@echo "  terraform-destroy  - Destroy all Terraform-managed GCP resources"
	@echo ""
	@echo "SERVICES"
	@echo "  redpanda-start     - Start Redpanda broker"
	@echo "  redpanda-stop      - Stop Redpanda broker"
	@echo ""
	@echo "CDC"
	@echo "  cdc-install        - Download Kafka Connect + Debezium PostgreSQL plugin"
	@echo "  cdc-topics         - Create the 10 per-table CDC topics on Redpanda"
	@echo "  cdc-connect-start  - Start the standalone Debezium connector"
	@echo "  cdc-connect-stop   - Stop the standalone Debezium connector"
	@echo "  cdc-schemas        - Register the CDC event schemas in Schema Registry"
	@echo "  cdc-driver         - Apply operational changes (CYCLES=N)"
	@echo "  cdc-consume        - Run the local consumer, land events to data/cdc/"
	@echo "  cdc-sync           - Upload landed files to the Databricks Volume"
	@echo "  cdc-cycle          - Topics -> schemas -> baseline -> changes -> consume -> sync"
	@echo "  dagster-start      - Start Dagster dev UI and orchestration"
	@echo "  dbt-run            - Run all dbt models"
	@echo "  dbt-test           - Run all dbt tests"
	@echo "  grafana-start      - Start Grafana"
	@echo "  generator-start    - Build the operational seed dataset (CSV)"
	@echo "  api-start          - Start FastAPI server (AI agent service layer)"
	@echo ""
	@echo "UTILITIES"
	@echo "  clean              - Remove cache and artifacts"

# ====================================================================
# SETUP
# ====================================================================

setup:
	python3 -m pip install --break-system-packages -r requirements.txt

setup-dev:
	python3 -m pip install --break-system-packages -e .[dev]

# ====================================================================
# TESTING
# ====================================================================

test:
	PYTHONPATH=$(PWD) pytest tests/ -v

test-unit:
	PYTHONPATH=$(PWD) pytest tests/unit/ tests/pipelines/ tests/ai/ -v \
		--cov=src --cov=fastapi --cov=ai \
		--cov-report=term-missing \
		--cov-report=html

test-integration:
	PYTHONPATH=$(PWD) pytest tests/integration/ -v

test-regression:
	PYTHONPATH=$(PWD) pytest tests/ -m regression -v

test-performance:
	PYTHONPATH=$(PWD) pytest tests/ -m performance -v

# ====================================================================
# CODE QUALITY
# ====================================================================

lint:
	ruff check . --exclude data/ --exclude docs/ --exclude .terraform/

lint-fix:
	ruff check --fix .
	ruff check --fix --unsafe-fixes .

# ====================================================================
# GCP / TERRAFORM
# ====================================================================

gcp-auth:
	gcloud auth login
	gcloud auth application-default login

terraform-init:
	cd terraform && terraform init

terraform-plan:
	cd terraform && terraform plan

terraform-apply:
	cd terraform && terraform apply

terraform-destroy:
	cd terraform && terraform destroy

# ====================================================================
# SERVICES
# ====================================================================

redpanda-start:
	rpk redpanda start --overprovisioned --smp 1 --memory 200M --reserve-memory 0M &

redpanda-stop:
	rpk redpanda stop

# ====================================================================
# CDC (operational PostgreSQL -> Debezium -> Redpanda -> local consumer)
# ====================================================================

cdc-install:
	bash scripts/setup/install_kafka_connect.sh

cdc-topics:
	PYTHONPATH=$(PWD) python3 -m src.ingestion.cdc.topics create

cdc-connect-start:
	PYTHONPATH=$(PWD) python3 -m src.ingestion.cdc.connect start

cdc-connect-stop:
	PYTHONPATH=$(PWD) python3 -m src.ingestion.cdc.connect stop

cdc-schemas:
	PYTHONPATH=$(PWD) python3 -m src.ingestion.cdc.register_schemas

cdc-driver:
	PYTHONPATH=$(PWD) python3 -m src.ingestion.cdc.change_driver --cycles $(or $(CYCLES),40)

cdc-consume:
	PYTHONPATH=$(PWD) python3 -m src.ingestion.cdc.consumer

cdc-sync:
	PYTHONPATH=$(PWD) python3 -m src.ingestion.cdc.sync_to_databricks

cdc-cycle:
	PYTHONPATH=$(PWD) python3 -m src.ingestion.cdc.run_cdc_cycle --cycles $(or $(CYCLES),40)

dagster-start:
	PYTHONPATH=$(PWD) dagster dev -m dagster

dbt-run:
	cd dbt && dbt run

dbt-test:
	cd dbt && dbt test

grafana-start:
	grafana-server --homepath /usr/share/grafana &

generator-start:
	PYTHONPATH=$(PWD) python3 src/generators/main.py

api-start:
	PYTHONPATH=$(PWD) uvicorn fastapi.main:app --host 0.0.0.0 --port 8000 --reload

# ====================================================================
# UTILITIES
# ====================================================================

clean:
	rm -rf .coverage coverage.xml htmlcov/
	rm -rf .pytest_cache
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
