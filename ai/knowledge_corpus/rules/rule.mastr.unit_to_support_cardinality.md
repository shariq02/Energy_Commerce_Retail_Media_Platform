---
unit_id: rule.mastr.unit_to_support_cardinality
kind: rule
ecosystem: energy
source:
- path: src/schemas/contracts/mastr.yml
  sha256: 60dff1d9abb3b0ae79ad2e2a324f473be5f04f9e659133fcf28dcadccdfc55fc
version: 1
last_updated: '2026-10-06'
approval:
  status: approved
  approver: Sharique
  approved_at: '2026-10-06'
  approved_version: 1
---

# Rule: unit_to_support_cardinality (mastr)

**Expression:** EEG (anlagen_eeg_*) and KWK (anlagen_kwk) are 1:1 with a unit; einheiten_genehmigung is ~1:1 (5% orphan); ertuechtigungen is 1:N. Use the confirmed cardinality (profiling section 06) with a LEFT join and an unmatched flag -- never an assumed 1:1 that multiplies unit attributes.

**Severity:** block
