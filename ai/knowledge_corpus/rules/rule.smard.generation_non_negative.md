---
unit_id: rule.smard.generation_non_negative
kind: rule
ecosystem: energy
source:
- path: src/schemas/contracts/smard.yml
  sha256: 3a017f241e155f2beda0fdc12e732adf9711b612c68bebb88e2fda19030e4b70
version: 1
last_updated: '2026-10-06'
approval:
  status: approved
  approver: mr.nobody
  approved_at: '2026-10-07'
  approved_version: 1
  approved_content_hash: c99c9478c9bfe16a7a87a4074a9bc5eb0911e424f26cf580c8f10b7f7c40f8da
---

# Rule: generation_non_negative (smard)

**Expression:** value >= 0 where metric like 'generation_%' or metric = 'total_power_consumption'

**Severity:** block
