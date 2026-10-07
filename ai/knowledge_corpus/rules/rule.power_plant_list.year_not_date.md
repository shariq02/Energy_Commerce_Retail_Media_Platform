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
  approver: Sharique
  approved_at: '2026-10-06'
  approved_version: 1
---

# Rule: year_not_date (power_plant_list)

**Expression:** Jahr_Inbetriebnahme / Jahr_Stilllegung are integer years -- cast to int, never to a timestamp; within-year ordering is not available

**Severity:** block
