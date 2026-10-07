---
unit_id: metric.ga4_sessions
kind: metric
ecosystem: commerce
source:
- path: databricks/analytics/commerce/semantic/01_metric_definitions.py
  sha256: 9c06a9603f64e132ac09691a5d4cba829b9f97f3dfbb8c86c47649ce8637d7d8
version: 1
last_updated: '2026-10-06'
approval:
  status: approved
  approver: mr.nobody
  approved_at: '2026-10-07'
  approved_version: 1
  approved_content_hash: dcc668d359ed44507f3fc6a4671ab86647ee3157a4bca5a89e7a5ecdfd1d5aaa
---

# Metric: ga4_sessions

**Definition:** Daily GA4 session count, by country.

**Grain:** geo_country x local_date
