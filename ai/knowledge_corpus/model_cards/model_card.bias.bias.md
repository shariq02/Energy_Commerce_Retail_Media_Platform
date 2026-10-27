---
unit_id: model_card.bias.bias
kind: model_card
ecosystem: energy
source:
- path: src/model_cards/energy/bias.bias.md
  sha256: c35983bcc47ff8464dbca051435eaa541a668bae3a0faa22d4f9a39d95782228
version: 1
last_updated: '2026-10-27'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-27'
  approved_version: 1
  approved_content_hash: b5f1810d1b7ce8bd8b3a51cd38bf5f794209e5a3d2e97d24f1d79456e5a67568
---

# Model card: bias.bias

_model `gbt_lightgbm`, primary metric `mae` (lower is better)_

## Decision
- decision: `approved_with_conditions`
- restriction: `none`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R07, R10, R11

## Purpose and task
- paradigm: supervised; task type: regression
- target: `target_bias_mwh`
- baseline fallback: `zero_correction`

## Intended use
- estimates the bias of the published generation forecast in MWh, per forecast scope
- target `target_bias_mwh`, dataset `bias`

## Out of scope
- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Primary metric
- mae: gbt_lightgbm held-out 1.625e+04; gbt_lightgbm validation 1.533e+04; zero_correction held-out 1.025e+05
- skill_primary: gbt_lightgbm held-out 0.8415
- skill_primary_ci_high: gbt_lightgbm held-out 0.8523
- skill_primary_ci_low: gbt_lightgbm held-out 0.8292

## Flags
- none

## Conditions and known limits
- segments not approved: offshore_wind
- segments with positive skill but no uncertainty interval, conditional until an interval is recorded: onshore_wind (0.0313), other (0.972), photovoltaic (0.0295), wind_and_photovoltaic (0.0334)

## Disclosures
- the held-out partition was read in at least 4 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger
- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback
- `zero_correction`: validation 9.365e+04, held-out 1.025e+05 on `mae`
