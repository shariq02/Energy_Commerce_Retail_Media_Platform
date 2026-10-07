---
unit_id: rule.smard.no_regional_total_pooling
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

# Rule: no_regional_total_pooling (smard)

**Expression:** a metric's regional (control-area) rows must not be pooled with its DE-LU total, nor its day resolution with its quarterhour resolution -- same quantity, different aggregation

**Severity:** block
