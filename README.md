# ECRMAP — Ecosystem-Centric Real-World Multi-Domain Analytics Platform

A multi-ecosystem data and analytics platform built on GCP, Databricks, and BigQuery. ECRMAP acquires heterogeneous real-world data across independent ecosystems, preserves source meaning and provenance at every stage, and builds governed analytical data products that support BI, ML, and AI/GenAI as downstream consumers.

## Ecosystems

Energy, Commerce / Digital Behaviour, Mobility, Healthcare, and Agriculture — parallel peers, not a fixed or ranked list. Each ecosystem defines its own sources, domains, models, and use cases; none is "future" relative to another. Energy and Commerce / Digital Behaviour currently have acquired, profiled data; Mobility, Healthcare, and Agriculture are validated future directions.

## Tech Stack

GCP | BigQuery | Databricks | PySpark | Redpanda | PostgreSQL | Debezium | Dagster | Terraform | FastAPI | Grafana | Power BI | GitHub Actions

The analytical-modelling layer above BigQuery (staging → intermediate → marts → semantic) is a future detailed-design decision — no specific tool is assumed.

## Status

Environment, infrastructure, first-wave source acquisition/profiling/contracts, the operational PostgreSQL database, CDC/streaming, and the multi-ecosystem platform architecture are complete. Live build status and the full plan are tracked in the design documentation.

## Documentation

Full design documentation lives in the `docs/` tree (gitignored). Start with its top-level index.

---

*ECRMAP*
