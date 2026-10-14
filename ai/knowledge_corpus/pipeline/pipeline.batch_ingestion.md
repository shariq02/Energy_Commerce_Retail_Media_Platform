---
unit_id: pipeline.batch_ingestion
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
  approved_content_hash: 915e6243d8347f6daad854989e5c59c0679cf39d37e8ed3c9d9274f521ff8f95
---

# Pipeline: batch ingestion

Acquisition by source:
- SMARD, DWD (28 curated stations), MaStR, power plant list and redispatch: scripted download of public data.
- Honda IoT: manual download of `reduced_data.zip` from Dryad.
- REES46 (commerce): manual download from Kaggle, which needs an account and token.
- GA4 (commerce): BigQuery query and export, not a plain file download.

Each file source follows one path: download or BigQuery export, raw landing in `data/raw/{source}/`, validation against the data contract, extraction or chunking into `data/staging/{source}/`, upload to Unity Catalog, then the Bronze, Silver, Gold and Analytical Processing notebooks.

Raw data is never uploaded directly. Only the staging output reaches Unity Catalog. Each source has its own data contract.
