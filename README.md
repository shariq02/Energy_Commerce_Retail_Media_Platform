# ECRMAP — Ecosystem-Centric Real-World Multi-Domain Analytics Platform

A multi-ecosystem data and analytics platform built on GCP and Databricks (BigQuery is used only as the acquisition interface for Google datasets). ECRMAP acquires heterogeneous real-world data across independent ecosystems, preserves source meaning and provenance at every stage, and builds governed analytical data products that support BI, ML, and the platform's own GenAI capabilities as downstream consumers.

**GenAI is a first-class part of ECRMAP from the beginning.** It applies across the whole project — development, engineering, analysis, documentation, research, reasoning and other project work — from the first phase (GenAI-assisted engineering). The later AI phases are where ECRMAP builds its own GenAI capabilities into the platform: RAG, agents, MCP tooling, evaluation and related capabilities. The later phases are not where GenAI starts.

## Ecosystems

Energy, Commerce, Mobility, Healthcare, and Agriculture — five committed peer ecosystems, not a fixed or ranked list. Each ecosystem defines its own sources, domains, models, and use cases. The **current build is Energy + Commerce** (Commerce is retail-only: GA4 and REES46); Mobility, Healthcare, and Agriculture are committed ecosystems whose source build-out is a later cycle.

## Tech Stack

Databricks | PySpark | Delta Lake / Unity Catalog | BigQuery (Google-source acquisition only) | GCP | Terraform | PostgreSQL (BI serving only) | FastAPI (API / Application Serving Platform) | Grafana | Power BI | GitHub Actions

Databricks owns Bronze → Silver → Gold → Analytical Processing and the semantic layer. Execution is a direct notebook/script sequence — no orchestration platform. Governance is cross-cutting, not a separate layer.

## Status

Phases 0–9 are complete for the current-scope sources: environment, infrastructure, acquisition, Bronze, profiling, contracts, localisation, Silver and Gold. Phases 10 onward (analytical processing, ML, BI serving, the AI product capability, API serving, closure) are not started. EDA/profiling is done for four Databricks Samples datasets (`power-plant`, `iot`, `weather`, `accuweather`) whose Silver has not started. The plan has 22 phases (0–21). Live build status and the full plan are tracked in the design documentation.

The repository and Unity Catalog names still contain "Retail Media" for historical reasons; Retail Media / advertising is not a target domain.

## Documentation

Full design documentation lives in the `docs/` tree (gitignored). Start with its top-level index (`docs/README.md`, "Current Project State").

---

*ECRMAP*
