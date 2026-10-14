---
unit_id: rule.mastr.lokationen_link_arrays
kind: rule
ecosystem: energy
source:
- path: src/schemas/contracts/mastr.yml
  sha256: 60dff1d9abb3b0ae79ad2e2a324f473be5f04f9e659133fcf28dcadccdfc55fc
version: 1
last_updated: '2026-10-06'
approval:
  status: approved
  approver: mr.nobody
  approved_at: '2026-10-07'
  approved_version: 1
  approved_content_hash: 07b4d0e73fd66b8a3eb2cf991b3a763a005ca7aa8e5ceb542ad5f081ddca10ab
---

# Rule: lokationen_link_arrays (mastr)

**Expression:** mastr_lokationen.VerknuepfteEinheitenMaStRNummern (up to 1806 entries) and NetzanschlusspunkteMaStRNummern must be split/exploded into proper M:N bridge tables at Silver before any join; also anlagen_eeg_*.VerknuepfteEinheitenMaStRNummern, einheiten_verbrennung.MastrNummernKombibetrieb, anlagen_eeg_wasser.ErtuechtigungIds, netze.Bilanzierungsgebiete

**Severity:** block
