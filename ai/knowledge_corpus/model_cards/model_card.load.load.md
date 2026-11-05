---
unit_id: model_card.load.load
kind: model_card
ecosystem: energy
source:
- path: src/model_cards/energy/load.load.md
  sha256: e5d4a4ad35f3cbb834181a82b09e81fc09ebb27b634e9f897188ce6fbea75067
version: 1
last_updated: '2026-10-27'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-27'
  approved_version: 1
  approved_content_hash: dfd596e52055db22657b42b63eef869cb8ba5336c2be79152ab50e070c0ab237
---

# Model card: load.load

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
- target: `target_value_mwh`
- baseline fallback: `seasonal_mean`

## Intended use
- estimates the electricity load in MWh
- target `target_value_mwh`, dataset `load`

## Out of scope
- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Primary metric
- mae: gbt_sklearn held-out 2.684e+04; gbt_sklearn validation 2.486e+04; seasonal_mean held-out 6.419e+04
- skill_primary: gbt_sklearn held-out 0.5819
- skill_primary_ci_high: gbt_sklearn held-out 0.6357
- skill_primary_ci_low: gbt_sklearn held-out 0.5374

## Flags
- none

## Conditions and known limits
- none

## Disclosures
- the held-out partition was read in at least 4 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger
- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback
- `seasonal_mean`: validation 5.7e+04, held-out 6.419e+04 on `mae`
