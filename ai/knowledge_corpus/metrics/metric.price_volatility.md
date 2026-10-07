---
unit_id: metric.price_volatility
kind: metric
ecosystem: energy
source:
- path: databricks/analytics/energy/semantic/01_metric_definitions.py
  sha256: 09880d5b47524c7a8be5d3d18a54d97be03294f5695328d82d5f7ba57836df49
version: 1
last_updated: '2026-10-06'
approval:
  status: approved
  approver: mr.nobody
  approved_at: '2026-10-07'
  approved_version: 1
  approved_content_hash: 0466f32f6cdfa2a6eae582359796bd15ca2966104d58593861c2ec953a15cd2f
---

# Metric: price_volatility

**Definition:** Within-day dispersion of the day-ahead price.

**Grain:** market_area_code x local_date
