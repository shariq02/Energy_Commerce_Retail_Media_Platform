---
unit_id: metric.site_energy_weather_context
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
  approved_content_hash: 4d486302486ed03d4b30987329878e5d7fffb640977cef1c584f039570c14704
---

# Metric: site_energy_weather_context

**Definition:** Site daily energy paired with that site's own mean air temperature.

**Grain:** location_key x local_date x subsystem x channel
