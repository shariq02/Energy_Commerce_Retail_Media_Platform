---
unit_id: rule.power_plant_list.mastr_link_is_subset
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
  approved_content_hash: 5e33bf52af09e4926175b6dadf8c29bbc86f532aa13a8f8e1f0503b8150670d7
---

# Rule: mastr_link_is_subset (power_plant_list)

**Expression:** EinheitMastrNummer links to mastr_einheiten_* for large plants only (~83% of rows carry an id, not unique) -- this is a reference / reconciliation relationship, NOT an identity join; aggregated small-plant rows have no id

**Severity:** block
