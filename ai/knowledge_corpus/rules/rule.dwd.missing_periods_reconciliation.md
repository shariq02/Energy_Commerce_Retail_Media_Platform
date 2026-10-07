---
unit_id: rule.dwd.missing_periods_reconciliation
kind: rule
ecosystem: energy
source:
- path: src/schemas/contracts/dwd.yml
  sha256: d5c2f3fd5ab18edbc7086ec69acdece5ec039d8325c6645cec90304c632fd91c
version: 1
last_updated: '2026-10-06'
approval:
  status: approved
  approver: Sharique
  approved_at: '2026-10-06'
  approved_version: 1
---

# Rule: missing_periods_reconciliation (dwd)

**Expression:** DWD-reported missing periods and observed -999/blank do not fully line up (6 station x parameter observed-without-report, 42 reported-without-observed) -- flag with a data-quality column, do NOT reconcile by deleting rows

**Severity:** warn
