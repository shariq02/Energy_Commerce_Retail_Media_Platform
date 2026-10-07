---
unit_id: contract.ga4
kind: contract
ecosystem: commerce
source:
- path: src/schemas/contracts/ga4.yml
  sha256: 8e7024cc801555559612b0de2bc4be42651989e308db441875d3411ac0ed9c31
version: 1
last_updated: '2026-10-06'
approval:
  status: approved
  approver: Sharique
  approved_at: '2026-10-06'
  approved_version: 1
---

# Contract: ga4

Google Analytics 4 event-level export for the Google Merchandise Store, Google's own public GA4-BigQuery-export-shaped sample dataset (`bigquery-public-data.ga4_obfuscated_sample_ecommerce`). Real GA4 export schema and real (obfuscated) event data, but not a live production GA4 property this project owns or controls. Static historical snapshot: 92 daily tables, 2020-11-01 through 2021-01-31 (Google's sample grows over time; re-check table count before assuming this range is still current). Not a German source -- GA4's own `geo` fields carry whatever countries the sample traffic came from, observed or not.
