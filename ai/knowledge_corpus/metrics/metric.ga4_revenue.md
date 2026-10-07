---
unit_id: metric.ga4_revenue
kind: metric
ecosystem: commerce
source:
- path: databricks/analytics/commerce/semantic/01_metric_definitions.py
  sha256: 9c06a9603f64e132ac09691a5d4cba829b9f97f3dfbb8c86c47649ce8637d7d8
version: 1
last_updated: '2026-10-06'
approval:
  status: approved
  approver: Sharique
  approved_at: '2026-10-06'
  approved_version: 1
---

# Metric: ga4_revenue

**Definition:** Daily GA4 purchase revenue, by country.

**Grain:** geo_country x local_date
