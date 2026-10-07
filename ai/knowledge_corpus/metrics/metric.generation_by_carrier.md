---
unit_id: metric.generation_by_carrier
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
  approved_content_hash: adae0835cd6726c485cdd2515ab9361b5e29abcd6139bc1b4080cde25cdfdc40
---

# Metric: generation_by_carrier

**Definition:** Electricity generation, by resolved energy carrier, per market area and day.

**Grain:** market_area_code x local_date x carrier_key
