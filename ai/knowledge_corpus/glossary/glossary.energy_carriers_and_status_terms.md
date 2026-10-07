---
unit_id: glossary.energy_carriers_and_status_terms
kind: glossary
ecosystem: energy
source:
- name: localisation_record
  sha256: 2f0d4f9a5a5abb5cafce8f44b7780ff525e3bc33125b76c3d801c71cbd85510b
version: 1
last_updated: '2026-10-07'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-07'
  approved_version: 1
  approved_content_hash: 5a34928cd6104d7c0c07436ffa0c64d278e4626214cf8d344fdb3cd574ac24e1
---

# Glossary: energy carriers and operating status

Every source that carries a concept spells it the same way, so the Gold layer needs no second normalisation.

Energy carrier names: `wind_onshore`, `wind_offshore`, `solar_photovoltaic`, `hydropower`, `pumped_storage`, `biomass`, `natural_gas`, `hard_coal`, `lignite`, `nuclear`, `geothermal`, `battery_storage`, `waste`, `mine_gas`, `other_renewable`, `other_conventional`, `heat`.

The redispatch primary energy type has only three coarse values (conventional, renewable, other). It maps to the coarse buckets (`other_conventional`, `other_renewable`), not to single carriers.

Operating-status buckets for MaStR and the power plant list: `planned`, `in_operation`, `temporarily_shut_down`, `permanently_shut_down`, `reserve`. The legal basis that BNetzA reports is kept in a separate `status_legal_basis` attribute and is not folded into the bucket.
