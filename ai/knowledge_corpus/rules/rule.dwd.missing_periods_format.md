---
unit_id: rule.dwd.missing_periods_format
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

# Rule: missing_periods_format (dwd)

**Expression:** dwd_missing_value_periods.Von_Datum / Bis_Datum use 'dd.mm.yyyy-HH:MM', a different format from the measurement MESS_DATUM -- parse explicitly before any temporal comparison

**Severity:** block
