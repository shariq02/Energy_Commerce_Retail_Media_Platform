# ECRMAP - Ecosystem-Centric Real-World Multi-Domain Analytics Platform - Makefile
# Common commands for development workflow

# Keep __pycache__ out of the tree for every recipe below.
export PYTHONDONTWRITEBYTECODE := 1

.PHONY: help setup setup-dev test test-unit test-integration test-regression test-performance lint lint-fix terraform-init terraform-plan terraform-apply terraform-destroy grafana-start api-start gcp-auth clean

help:
	@echo "ECRMAP - Ecosystem-Centric Real-World Multi-Domain Analytics Platform - Available Commands"
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
	@echo "  grafana-start      - Start Grafana"
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
	PYTHONPATH=$(PWD) pytest tests/unit/ tests/ai/ -v \
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

grafana-start:
	grafana-server --homepath /usr/share/grafana &

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
