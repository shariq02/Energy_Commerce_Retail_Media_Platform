---
unit_id: governance.data_contracts
kind: governance
ecosystem: shared
source:
- name: governance_design
  sha256: cee19de81195e0394240d9a6ae112d5f780056ade6e78ec055d2d548728a6f7e
version: 1
last_updated: '2026-10-07'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-07'
  approved_version: 1
  approved_content_hash: 1e1c5b1e89d485fbcd63ba05b8ddab8b2ca9f1734f05a9b229a51c2c7a7c4db9
---

# Governance: data contracts and schema evolution

Every ingested source has an explicit data contract. A contract defines the expected schema and types, the required and optional fields, the null and range constraints, and the expected update frequency.

Every contract records the derived field `ecosystem`. The Silver layer assigns it from a governed map of source system to ecosystem. The allowed values are `energy` and `commerce`. A change to the map is a recorded decision.

A contract violation blocks the matching Silver build.

Most sources are static, one-time acquisitions. SMARD and DWD are scripted and can be re-run. GA4 can be queried again through BigQuery. If a source changes shape, profiling detects it and the contract version is raised. Each Silver, Gold and Analytics table has one contract-defined shape. A change is a new contract version and a regenerated table. It is never a parallel `_v2` table.
