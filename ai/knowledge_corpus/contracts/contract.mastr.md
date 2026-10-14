---
unit_id: contract.mastr
kind: contract
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
  approved_content_hash: e6328ae2a4ed3873f731ed63d3ddc1e48592a40fa56cb836974bb6ac4d35d44b
---

# Contract: mastr

The Bundesnetzagentur Marktstammdatenregister -- the complete legal register of the German electricity (and gas) market: every generation unit, its EEG / KWK support and its authorisation, the market actors, the grid connection topology, and MaStR's own change history and code catalogs. A single Gesamtdatenexport snapshot. Static one-time acquisition, public download, no auth. Solar and pure-storage object types are deferred from staging in this snapshot (they explain several 'partial-scope' orphan rates below).
