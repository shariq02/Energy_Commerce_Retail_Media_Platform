# ECRMAP — Ecosystem-Centric Real-World Multi-Domain Analytics Platform

A multi-ecosystem data and analytics platform built on GCP, Databricks, and BigQuery. ECRMAP acquires heterogeneous real-world data across independent ecosystems, preserves source meaning and provenance at every stage, and builds governed analytical data products that support BI, ML, and AI/GenAI as downstream consumers.

## Ecosystems

Energy, Commerce / Digital Behaviour, Mobility, Healthcare, and Agriculture — parallel peers, not a fixed or ranked list. Each ecosystem defines its own sources, domains, models, and use cases; none is "future" relative to another. Energy and Commerce / Digital Behaviour currently have acquired, profiled data; Mobility, Healthcare, and Agriculture are validated future directions (`docs/ECOSYSTEM_EXPANSION_SCOPE_20260903_v1.md`).

## Tech Stack

GCP | BigQuery | Databricks | PySpark | Redpanda | PostgreSQL | Debezium | Dagster | Terraform | FastAPI | Grafana | Power BI | GitHub Actions

The analytical-modelling layer above BigQuery (staging → intermediate → marts → semantic) is a future detailed-design decision — no specific tool is assumed.

## Status

Phases 0–6 complete (environment, infrastructure, first-wave source acquisition/profiling/contracts, operational PostgreSQL, CDC/streaming). Phase 7a (multi-ecosystem platform architecture) complete. See `docs/MASTER_BUILD_FLOW_20260903_v2.md` for live build status and `docs/PROJECT_PLAN_20260903_v4.md` for the full phase plan.

## Documentation

Full design documentation lives in `docs/` (gitignored). Start with `docs/README.md`.

---

*ECRMAP*
