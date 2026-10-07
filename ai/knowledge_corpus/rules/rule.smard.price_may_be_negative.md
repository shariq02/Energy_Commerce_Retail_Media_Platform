---
unit_id: rule.smard.price_may_be_negative
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
  approved_content_hash: c44dbaa58ab69236967494eaa51aae22357b2ff034de3bb8138bd13771682e79
---

# Rule: price_may_be_negative (smard)

**Expression:** value may be < 0 where metric = 'day_ahead_prices' (negative prices are a real market outcome)

**Severity:** info
