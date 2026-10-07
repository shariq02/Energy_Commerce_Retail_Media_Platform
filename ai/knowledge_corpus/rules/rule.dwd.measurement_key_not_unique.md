---
unit_id: rule.dwd.measurement_key_not_unique
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

# Rule: measurement_key_not_unique (dwd)

**Expression:** (STATIONS_ID, MESS_DATUM) is NOT unique in any measurement table except dwd_solar; identical duplicate rows may be collapsed

**Severity:** warn
