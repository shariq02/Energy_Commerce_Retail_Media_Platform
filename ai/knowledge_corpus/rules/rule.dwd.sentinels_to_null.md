---
unit_id: rule.dwd.sentinels_to_null
kind: rule
ecosystem: energy
source:
- path: src/schemas/contracts/dwd.yml
  sha256: d5c2f3fd5ab18edbc7086ec69acdece5ec039d8325c6645cec90304c632fd91c
version: 1
last_updated: '2026-10-06'
approval:
  status: approved
  approver: mr.nobody
  approved_at: '2026-10-07'
  approved_version: 1
  approved_content_hash: 46e36a41a04e950a3def25b56b98bc3eeea1d89fc800ed5708bb7f4dfecbe5d8
---

# Rule: sentinels_to_null (dwd)

**Expression:** -999 / -999.0 / -99.9 / '' in value columns are missing markers, never real values -- convert to NULL before any statistic

**Severity:** block
