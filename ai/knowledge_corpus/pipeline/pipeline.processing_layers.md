---
unit_id: pipeline.processing_layers
kind: pipeline
ecosystem: shared
source:
- name: pipeline_design
  sha256: af21799da1175fa1cf39358407c5d1068c455632d84ef5c7a61826c76b202fd1
version: 1
last_updated: '2026-10-07'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-07'
  approved_version: 1
  approved_content_hash: 9212d4811dd976bb2de56ef393c543a23a01aba3f255f08532f07a217f46e9c0
---

# Pipeline: processing layers in Databricks

One chain runs in Databricks: Bronze, Silver, Gold, Analytical Processing. There is no second input path.

- `bronze/`: one notebook per source, flat table names.
- `silver/`: source-scoped tables, cleaned, typed and localised. There is one primary Silver table per Bronze source table. Folders are grouped by ecosystem and domain.
- `gold/`: the governed domain model, in `{ecosystem}_gold` and `shared_conformed`.
- `analytics/`: derived facts, domain marts, metric definitions and cross-ecosystem contextual analytics.
- `serving/`: local export of selected marts for the PostgreSQL load.
- `quality/`: layer-transition gates and the shared `quality` schema objects.
- `setup/`: creation of schemas, the catalog and Volumes.
