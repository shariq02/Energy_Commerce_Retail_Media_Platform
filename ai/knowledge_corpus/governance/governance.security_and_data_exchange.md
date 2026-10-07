---
unit_id: governance.security_and_data_exchange
kind: governance
ecosystem: shared
source:
- name: governance_design
  sha256: eff32be5623c6cd21edad505ff965a7f5d24cc1568bbb88f55d0023d31fbb9c1
version: 1
last_updated: '2026-10-07'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-07'
  approved_version: 1
  approved_content_hash: 25bd3ec8eebf7949f3325c7a74c43a9ab79117ef13fc57849b6c28ec9e170fc5
---

# Governance: security and secure data exchange

Access control uses GCP IAM, row-level security and column-level security on the Databricks and serving layers.

The acquired third-party datasets contain no personal data that belongs to this project. No synthetic operational customer database exists.

Sensitive data is never exposed through the AI agent's tool layer. The agent reads the governed semantic layer only.
