---
unit_id: metric.rees46_event_demand
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
  approved_content_hash: d34e4a21a699c9bad42aa2188b03acd25e093adc1b24cfe6aff10210b17f57d4
---

# Metric: rees46_event_demand

**Definition:** Daily REES46 event volume, by top-level category and event type. No currency -- relative measure only.

**Grain:** local_date x category_l1 x event_type
