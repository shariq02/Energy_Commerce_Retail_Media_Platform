---
unit_id: rule.honda_iot.key_unique
kind: rule
ecosystem: energy
source:
- path: src/schemas/contracts/honda_iot.yml
  sha256: ecce2378681f571721ff13c94ef4488889694aecc7508f455237a1737e27b568
version: 1
last_updated: '2026-10-06'
approval:
  status: approved
  approver: Sharique
  approved_at: '2026-10-06'
  approved_version: 1
---

# Rule: key_unique (honda_iot)

**Expression:** (frequency, datetime_utc) unique in every table

**Severity:** block
