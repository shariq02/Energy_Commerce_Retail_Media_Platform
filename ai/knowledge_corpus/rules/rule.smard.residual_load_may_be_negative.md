---
unit_id: rule.smard.residual_load_may_be_negative
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

# Rule: residual_load_may_be_negative (smard)

**Expression:** value may be < 0 where metric = 'residual_load' (load minus renewables can be negative)

**Severity:** info
