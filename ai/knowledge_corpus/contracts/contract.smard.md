---
unit_id: contract.smard
kind: contract
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

# Contract: smard

Bundesnetzagentur SMARD open electricity-market data -- day-ahead prices, realised generation by fuel, load / consumption, residual load, and transmission-system-operator forecasts, in long format (one metric per row). Covers the DE-LU bidding zone plus the four control areas (50Hertz, Amprion, TenneT, TransnetBW) for the metrics SMARD breaks down regionally, at daily and quarter-hourly resolution. Static one-time acquisition, public API, no auth.
