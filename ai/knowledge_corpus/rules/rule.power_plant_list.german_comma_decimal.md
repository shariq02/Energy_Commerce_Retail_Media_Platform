---
unit_id: rule.power_plant_list.german_comma_decimal
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
  approved_content_hash: 508adc79f04cb5c9de6870cc34698e602582116618ecdb0c4d1a82e706eb3590
---

# Rule: german_comma_decimal (power_plant_list)

**Expression:** every numeric column uses ',' as the decimal separator -- cast explicitly, quarantine parse failures

**Severity:** block
