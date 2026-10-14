# Model card: session_purchase_rees46.purchase

_model `gbt_lightgbm`, dataset `session_purchase_rees46`, frozen version 1, primary metric `pr_auc` (higher is better). Generated: 2026-10-04T18:03Z_

## Decision

- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: none

## Purpose and task

- paradigm: supervised; task type: classification
- target: `target_purchase_after_prefix`
- baseline fallback: `base_rate`

## Intended use

- estimates the probability that a REES46 session ends in a purchase, from the first events of the session
- target `target_purchase_after_prefix`, dataset `session_purchase_rees46`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 1313711 rows, columns 30, rows by partition {'test': 1313711}
- held-out read: partition test, frozen_delta_version 1, rows 1313711, groups 406466
- frame: 916277 rows, columns 30, rows by partition {'validation': 916277}

## Training

- model family: gbt_lightgbm; MLflow run `a7df49464788464e802fed4d197a5b41`
- parameters: {'row_fraction': 0.7, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: lightgbm 4.7.0
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_lightgbm held-out | gbt_lightgbm validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.08171 | 0.08129 | 0.08171 |
| calibration_error | 0.0144 | 0.01401 | 0.001492 |
| log_loss | 0.2126 | 0.2119 | 0.2829 |
| pr_auc | 0.4401 | 0.4395 | 0.08171 |
| pr_auc_lift | 5.386 | 5.407 | 1 |
| primary_change_relative | -0.001352 |  | -0.005129 |
| skill_primary | 0.3584 |  |  |
| skill_primary_ci_high | 0.3786 |  |  |
| skill_primary_ci_low | 0.3443 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- none

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `base_rate`: validation 0.08129, held-out 0.08171 on `pr_auc`
