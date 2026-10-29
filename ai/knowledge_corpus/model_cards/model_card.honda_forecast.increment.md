---
unit_id: model_card.honda_forecast.increment
kind: model_card
ecosystem: energy
source:
- path: src/model_cards/energy/honda_forecast.increment.md
  sha256: 842b3a9a4f3a243695d858b0cefae4d47784ebec01eb55476009877028676804
version: 1
last_updated: '2026-10-27'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-27'
  approved_version: 1
  approved_content_hash: 3f3c5ac7139f9c91bb6c9e99bd2b50790d99261e013dffd7c98484ae853f0bd5
---

# Model card: honda_forecast.increment

_model `chronos2_lora`, primary metric `mae` (lower is better)_

## Decision
- decision: `approved_with_conditions`
- restriction: `none`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R10, R11, R13

## Purpose and task
- paradigm: supervised; task type: regression
- target: `target_increment`
- baseline fallback: `persistence`

## Intended use
- estimates the next energy increment of a Honda site channel
- target `target_increment`, dataset `honda_forecast`

## Out of scope
- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Primary metric
- mae: chronos2_lora held-out 18.13; chronos2_lora validation 16.15; persistence held-out 23.28
- skill_primary: chronos2_lora held-out 0.2214
- skill_primary_ci_high: chronos2_lora held-out 0.2784
- skill_primary_ci_low: chronos2_lora held-out 0.1768

## Flags
- none

## Conditions and known limits
- contamination flag unresolved: the pretraining data of the model was not shown to be free of the evaluated data; revision 29ec3766d36d; pretraining data per register: named on the model card; read before the contamination check; task data names found: none
- segments with positive skill but no uncertainty interval, conditional until an interval is recorded: combined_heat_and_power (0.186), solar_photovoltaic (0.116), total (0.271)

## Disclosures
- the held-out partition was read in at least 4 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged
- foundation model amazon/chronos-2 at revision 29ec3766d36d, licence apache-2.0, ran lora

## Revalidation trigger
- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback
- `persistence`: validation 20.3, held-out 23.28 on `mae`
