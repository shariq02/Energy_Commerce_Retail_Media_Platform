---
unit_id: rule.power_plant_list.commissioning_before_decommissioning
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

# Rule: commissioning_before_decommissioning (power_plant_list)

**Expression:** Jahr_Inbetriebnahme <= Jahr_Stilllegung where both present

**Severity:** warn
