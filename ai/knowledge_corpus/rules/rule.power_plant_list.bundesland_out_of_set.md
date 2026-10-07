---
unit_id: rule.power_plant_list.bundesland_out_of_set
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
  approved_content_hash: 402d35ceaf16b03e7c7962da0a1fec9e3bba186b49b98150b1885f079a3405d6
---

# Rule: bundesland_out_of_set (power_plant_list)

**Expression:** Bundesland should be one of the 16 German states; 'Nordsee' (offshore) and 'None' also occur

**Severity:** warn
