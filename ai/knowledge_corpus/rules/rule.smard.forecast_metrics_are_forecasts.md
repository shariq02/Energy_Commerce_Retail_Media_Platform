---
unit_id: rule.smard.forecast_metrics_are_forecasts
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
  approved_content_hash: e5974023bb3e4235ca465c892376ece290d1d34e00dffef2ff2bbd0d3685f2e2
---

# Rule: forecast_metrics_are_forecasts (smard)

**Expression:** the 6 forecast_generation_* metrics are published BEFORE their target time -- pairing a forecast with its realised counterpart must respect each series' own availability time, and a later-vintage forecast for the same target timestamp is leakage

**Severity:** warn
