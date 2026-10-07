---
unit_id: rule.dwd.conflicting_dup_keys
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

# Rule: conflicting_dup_keys (dwd)

**Expression:** conflicting (STATIONS_ID, MESS_DATUM) duplicate groups exist in dwd_cloudiness (6), dwd_cloud_type (6), dwd_soil_temperature (7284) and dwd_wind (20783) -- a deterministic conflict-resolution rule is required before counting or splitting

**Severity:** block
