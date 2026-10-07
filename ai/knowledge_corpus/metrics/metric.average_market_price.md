---
unit_id: metric.average_market_price
kind: metric
ecosystem: energy
source:
- path: databricks/analytics/energy/semantic/01_metric_definitions.py
  sha256: 09880d5b47524c7a8be5d3d18a54d97be03294f5695328d82d5f7ba57836df49
version: 1
last_updated: '2026-10-06'
approval:
  status: approved
  approver: Sharique
  approved_at: '2026-10-06'
  approved_version: 1
---

# Metric: average_market_price

**Definition:** Mean day-ahead electricity price per market area and day.

**Grain:** market_area_code x local_date
