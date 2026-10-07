---
unit_id: rule.dwd.cloud_type_qn_defect
kind: rule
ecosystem: energy
source:
- path: src/schemas/contracts/dwd.yml
  sha256: d5c2f3fd5ab18edbc7086ec69acdece5ec039d8325c6645cec90304c632fd91c
version: 1
last_updated: '2026-10-06'
approval:
  status: approved
  approver: Sharique
  approved_at: '2026-10-06'
  approved_version: 1
---

# Rule: cloud_type_qn_defect (dwd)

**Expression:** dwd_cloud_type.QN_8 carries the literal '-999' as the flag value in ~128k rows -- not a valid quality level; quarantine those rows

**Severity:** block
