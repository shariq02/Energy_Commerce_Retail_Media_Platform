---
unit_id: model_card.honda_anomaly.anomaly
kind: model_card
ecosystem: energy
source:
- path: src/model_cards/energy/honda_anomaly.anomaly.md
  sha256: e5bf3b389e0c137a4731744b73b20aa2bcbc1e32e81415e9eff7d9d94734c7b8
version: 1
last_updated: '2026-10-27'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-27'
  approved_version: 1
  approved_content_hash: 08898d3acbf90a526b5bebef3e68291ac069d4b137dcadbbdd89248df03b4d25
---

# Model card: honda_anomaly.anomaly

_model `forecast_residual_sklearn`, primary metric `pr_auc` (higher is better)_

## Decision
- decision: `approved_with_conditions`
- restriction: `offline_only`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R09, R10

## Purpose and task
- paradigm: unsupervised; task type: anomaly
- target: `injected_anomaly`
- baseline fallback: `seasonal_zscore`

## Intended use
- scores Honda site energy readings for anomalies; the evidence rests on injected anomalies
- target `injected_anomaly`, dataset `honda_anomaly`
- restricted to offline only (see the conditions below)

## Out of scope
- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Primary metric
- pr_auc: forecast_residual_sklearn held-out 0.5587; forecast_residual_sklearn validation 0.6517; seasonal_zscore held-out 0.357
- skill_primary: forecast_residual_sklearn held-out 0.2016
- skill_primary_ci_high: forecast_residual_sklearn held-out 0.3074
- skill_primary_ci_low: forecast_residual_sklearn held-out 0.08569

## Flags
- none

## Conditions and known limits
- offline evidence only; operational feasibility is not shown

## Disclosures
- the held-out partition was read in at least 3 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger
- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback
- `seasonal_zscore`: validation 0.442, held-out 0.357 on `pr_auc`
