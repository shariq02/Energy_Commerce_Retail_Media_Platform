---
unit_id: rule.power_plant_list.year_not_date
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
  approved_content_hash: 7d9b82d7b6b66905f2db070403a1cdc2da9c86811f9cbf1fdfc20b94de0dcdd2
---

# Rule: year_not_date (power_plant_list)

**Expression:** Jahr_Inbetriebnahme / Jahr_Stilllegung are integer years -- cast to int, never to a timestamp; within-year ordering is not available

**Severity:** block
