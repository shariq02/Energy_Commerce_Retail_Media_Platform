---
unit_id: rule.dwd.solar_is_ten_minute
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

# Rule: solar_is_ten_minute (dwd)

**Expression:** dwd_solar is a 10-minute series with a unique key and its own MESS_DATUM format (yyyyMMddHH:mm) plus MESS_DATUM_WOZ true-local-time -- do not resample to hourly implicitly

**Severity:** warn
