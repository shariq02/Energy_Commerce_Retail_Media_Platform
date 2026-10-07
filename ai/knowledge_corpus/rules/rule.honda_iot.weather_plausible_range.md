---
unit_id: rule.honda_iot.weather_plausible_range
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

# Rule: weather_plausible_range (honda_iot)

**Expression:** Ta in [-40, 50] degC; Igm in [0, 1500] W/m2

**Severity:** warn
