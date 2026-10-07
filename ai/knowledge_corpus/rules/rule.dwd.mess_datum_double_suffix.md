---
unit_id: rule.dwd.mess_datum_double_suffix
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

# Rule: mess_datum_double_suffix (dwd)

**Expression:** MESS_DATUM may carry a trailing '.0' from a double-inferred column -- strip before parsing yyyyMMddHH

**Severity:** block
