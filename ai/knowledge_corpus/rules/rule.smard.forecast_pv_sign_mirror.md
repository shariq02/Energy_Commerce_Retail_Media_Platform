---
unit_id: rule.smard.forecast_pv_sign_mirror
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

# Rule: forecast_pv_sign_mirror (smard)

**Expression:** metric='forecast_generation_photovoltaic' is an EXACT -1x mirror of metric='forecast_generation_wind_and_photovoltaic' (observed range -1,271,058 .. -66,771; every other forecast metric is positive)

**Severity:** block
