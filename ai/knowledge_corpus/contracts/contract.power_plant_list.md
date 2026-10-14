---
unit_id: contract.power_plant_list
kind: contract
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
  approved_content_hash: 95ebd4bb0d857807a460bb96ca1d48c1b8bb648adc2281de4591c1e914936999
---

# Contract: power_plant_list

Bundesnetzagentur Kraftwerksliste -- the register of German power plants above the ~10 MW net reporting threshold (plus aggregated small-plant rows and decommissioned plants), one quarterly snapshot. Includes a separate summary of expected conventional-capacity retirements 2026-2029 driven by the coal-exit law (KVBG) and grid-relevance provisions. Static one-time acquisition, public download, no auth.
