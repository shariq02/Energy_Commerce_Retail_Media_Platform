---
unit_id: model_card.survival.unit_lifetime
kind: model_card
ecosystem: energy
source:
- path: src/model_cards/energy/survival.unit_lifetime.md
  sha256: 2a5e6426128be8b0817a9ce7fa2e632ad7b6efed44046bc65c2c92eac367da0f
version: 1
last_updated: '2026-10-27'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-27'
  approved_version: 1
  approved_content_hash: 5a720b4e58a2c9b9a15da8839ef9f75259868f078e6f7d532ddbb9fdb04d3c10
---

# Model card: survival.unit_lifetime

_model `survival_forest`, primary metric `concordance` (higher is better)_

## Decision
- decision: `approved_with_conditions`
- restriction: `none`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R10, R12

## Purpose and task
- paradigm: survival; task type: survival
- target: `target_duration_years`
- baseline fallback: `kaplan_meier`

## Intended use
- estimates the lifetime in years of a generation unit and its probability of still being in operation after 1, 3 and 5 years
- target `target_duration_years`, dataset `survival`

## Out of scope
- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Primary metric
- concordance: survival_forest held-out 0.9626; survival_forest validation 0.9641; kaplan_meier held-out 0.5031
- skill_primary: survival_forest held-out 0.4594
- skill_primary_ci_high: survival_forest held-out 0.4672
- skill_primary_ci_low: survival_forest held-out 0.4516

## Flags
- none

## Conditions and known limits
- 5-year calibration gap 0.00354 against 4.05e-05 for the baseline; predicted risk is less well calibrated than the baseline
- the number of events in the held-out partition was not recorded; the concordance and Brier scores rest on an unknown number of events
- revalidate on a new frozen dataset version, with the held-out event count recorded, before relying on the risk values

## Disclosures
- the held-out partition was read in at least 10 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger
- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback
- `kaplan_meier`: validation 0.504, held-out 0.5031 on `concordance`
