---
unit_id: rule.smard.additive_identity_not_clean
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
  approved_content_hash: b3d2b05be01735dea4c7465228b437caa94211a4f496e9bd85e6e18018f89d7b
---

# Rule: additive_identity_not_clean (smard)

**Expression:** the published generation / load series are NOT a clean additive set -- residual_load cannot be reconstructed from the other series without an extra term / vintage reconciliation

**Severity:** warn
