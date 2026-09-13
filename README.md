# ECRMAP — Ecosystem-Centric Real-World Multi-Domain Analytics Platform

A multi-ecosystem data and analytics platform built on GCP, Databricks, and BigQuery. ECRMAP acquires heterogeneous real-world data across independent ecosystems, preserves source meaning and provenance at every stage, and builds governed analytical data products that support BI, ML, and AI/GenAI as downstream consumers.

## Ecosystems

Energy, Commerce / Digital Behaviour, Mobility, Healthcare, and Agriculture — parallel peers, not a fixed or ranked list. Each ecosystem defines its own sources, domains, models, and use cases; none is "future" relative to another. Energy and Commerce / Digital Behaviour currently have acquired, profiled data; Mobility, Healthcare, and Agriculture are validated future directions.

## Tech Stack

Databricks | PySpark | Delta Lake / Unity Catalog | BigQuery (Google-source acquisition only) | GCP | Terraform | PostgreSQL (BI serving only) | FastAPI | Grafana | Power BI | GitHub Actions

Databricks owns Bronze → Silver → Gold → Analytical Processing and the semantic layer. Execution is a direct notebook/script sequence — no orchestration platform. Governance is cross-cutting, not a separate layer.

## Status

Environment, infrastructure, first-wave source acquisition/profiling/contracts, the multi-ecosystem platform architecture, and the German energy & weather deepening wave (through Bronze/profiling/contracts/localisation) are complete. The September 2026 architecture redesign and its repository cleanup are done. The current build step is the Commerce acquisition wave (GA4 + REES46 + Search Visibility), then Commerce Bronze/profiling/contracts, then Databricks Silver. Live build status and the full plan are tracked in the design documentation.

## Documentation

Full design documentation lives in the `docs/` tree (gitignored). Start with its top-level index.

---

*ECRMAP*
