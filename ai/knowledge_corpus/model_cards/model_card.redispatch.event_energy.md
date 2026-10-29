---
unit_id: model_card.redispatch.event_energy
kind: model_card
ecosystem: energy
source:
- path: src/model_cards/energy/redispatch.event_energy.md
  sha256: 72a95b493c38a795c5a8bc3672e19002128d3d0cc52d17adf1128dee1a81fce6
version: 1
last_updated: '2026-10-27'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-27'
  approved_version: 1
  approved_content_hash: cb60cd636a874ad530b77a23d31ab99d5c7c4f6126296a9f3acb7cf62ed0e200
---

# Model card: redispatch.event_energy

_model `gbt_sklearn`, primary metric `mae` (lower is better)_

## Decision
- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: R10

## Purpose and task
- paradigm: supervised; task type: regression
- target: `target_log_event_energy_mwh`
- baseline fallback: `zone_mean`

## Intended use
- estimates the logarithm of the energy of a redispatch event in MWh
- target `target_log_event_energy_mwh`, dataset `redispatch`

## Out of scope
- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Primary metric
- mae: gbt_sklearn held-out 1.077; gbt_sklearn validation 1.033; zone_mean held-out 1.334
- skill_primary: gbt_sklearn held-out 0.1926
- skill_primary_ci_high: gbt_sklearn held-out 0.2388
- skill_primary_ci_low: gbt_sklearn held-out 0.1544

## Flags
- none

## Conditions and known limits
- none

## Disclosures
- the held-out partition was read in at least 5 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger
- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback
- `zone_mean`: validation 1.182, held-out 1.334 on `mae`
