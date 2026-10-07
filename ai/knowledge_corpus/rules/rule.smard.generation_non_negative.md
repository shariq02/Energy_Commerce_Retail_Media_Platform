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
  approver: Sharique
  approved_at: '2026-10-06'
  approved_version: 1
---

# Rule: generation_non_negative (smard)

**Expression:** value >= 0 where metric like 'generation_%' or metric = 'total_power_consumption'

**Severity:** block
