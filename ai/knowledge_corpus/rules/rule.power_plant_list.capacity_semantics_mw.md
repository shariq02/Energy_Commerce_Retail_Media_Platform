---
unit_id: rule.power_plant_list.capacity_semantics_mw
kind: rule
ecosystem: energy
source:
- path: src/schemas/contracts/power_plant_list.yml
  sha256: 7e01952a23d6a17bf8522674ec2d155cb0037d724cd3a4392d9fc4a4614c6a50
version: 1
last_updated: '2026-10-06'
approval:
  status: approved
  approver: mr.nobody
  approved_at: '2026-10-07'
  approved_version: 1
  approved_content_hash: 734461a5cdd3e071c593dd421b6d82edfa0982607807fce1844414da3c917825
---

# Rule: capacity_semantics_mw (power_plant_list)

**Expression:** Bruttoleistung_MW / Nettonennleistung_MW / Grenzkraftwerk_Nettonennleistung_MW are interpreted as MEGAWATTS

**Severity:** info
