---
unit_id: rule.power_plant_list.capacity_additions_is_projection
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
  approved_content_hash: 30405fc43856b0f2108215fa0610f9a3e5c1aaee23b7a5066304be30b9af8100
---

# Rule: capacity_additions_is_projection (power_plant_list)

**Expression:** power_plant_capacity_additions rows are revised projections -- model as a Silver planning/forecast fact, never merged into current-state plant attributes

**Severity:** block
