---
unit_id: rule.power_plant_list.capacity_additions_footnote_rows
kind: rule
ecosystem: energy
source:
- path: src/schemas/contracts/power_plant_list.yml
  sha256: 7e01952a23d6a17bf8522674ec2d155cb0037d724cd3a4392d9fc4a4614c6a50
version: 1
last_updated: '2026-10-06'
approval:
  status: approved
  approver: mr.nobody
  approved_at: '2026-10-07'
  approved_version: 1
  approved_content_hash: 683b7d0cb81640995ef9d0621f9786f5314d758f1e7d5b6f425712d448925f9c
---

# Rule: capacity_additions_footnote_rows (power_plant_list)

**Expression:** power_plant_capacity_additions.energietraeger contains section headers, sub-total labels ('Insgesamt', 'davon aus 1. Ausschreibungsrunde') and multi-sentence legal footnote text ('(1) Es ist zu beachten, dass ...', '[2] Der zustaendige Uebertragungsnetzbetreiber ...') ingested as data rows, mixed with the ~5 real carrier values (Erdgas, Braunkohle, Batteriespeicher, Pumpspeicher, Abfall, ...)

**Severity:** block
