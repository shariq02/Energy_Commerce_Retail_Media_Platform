---
unit_id: metric.site_energy_consumption
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
  approved_content_hash: 7c87b5120b6e78a148cb4378636c251897e970d9c6c336257926f6f82bb1975e
---

# Metric: site_energy_consumption

**Definition:** Daily site energy flow per subsystem/channel, additive readings only.

**Grain:** location_key x local_date x subsystem x channel
