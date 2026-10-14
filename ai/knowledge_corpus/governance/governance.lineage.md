---
unit_id: governance.lineage
kind: governance
ecosystem: shared
source:
- name: governance_design
  sha256: e5a42951df9371a423dc0115f1145b6823e3265cdd0a58a4c6988c355537e39f
version: 1
last_updated: '2026-10-07'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-07'
  approved_version: 1
  approved_content_hash: c0bf72848546d84cd5174c6c0b0283bef7dbd7ea554d76829a1ab156443bd993
---

# Governance: lineage

Data flows from Source to Bronze, Silver, Gold and Analytical Processing, then to a metric or mart. A local export loads PostgreSQL serving, which feeds Power BI and Grafana. The semantic layer and the governed model outputs feed the AI capability.

The `source_system` value traces every downstream row, serving row, mart and metric back to its origin. From Silver onward every lineage node also carries the `ecosystem` attribute. Bronze nodes carry no `ecosystem`. Lineage, ownership, cost and access reporting can be sliced per ecosystem.

GenAI-assisted engineering is not a data stage, so it is not part of the lineage.
