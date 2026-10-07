---
unit_id: rule.dwd.qn_regime_dependent
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

# Rule: qn_regime_dependent (dwd)

**Expression:** QN vocabulary changed across decades -- decode QN against the scheme valid for the record's era, never use QN as a physical feature

**Severity:** warn
