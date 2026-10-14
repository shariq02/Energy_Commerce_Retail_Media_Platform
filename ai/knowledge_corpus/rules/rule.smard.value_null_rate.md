---
unit_id: rule.smard.value_null_rate
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
  approved_content_hash: f456b79d5e71a4f9f6509099464cddf0dd503e05325f71eacdb6b5a5c8fb0926
---

# Rule: value_null_rate (smard)

**Expression:** null(value) / count(*) <= 0.03

**Severity:** warn
