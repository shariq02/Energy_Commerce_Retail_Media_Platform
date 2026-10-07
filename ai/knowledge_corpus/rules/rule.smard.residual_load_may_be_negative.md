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
  approver: mr.nobody
  approved_at: '2026-10-07'
  approved_version: 1
  approved_content_hash: aa3a641de37cf7c23cd2660ce049b1950d9f13da8f1b8c97e35c4a70a3399773
---

# Rule: residual_load_may_be_negative (smard)

**Expression:** value may be < 0 where metric = 'residual_load' (load minus renewables can be negative)

**Severity:** info
