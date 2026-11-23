# Model card: session_purchase_rees46.purchase

_model `gbt_lightgbm`, dataset `session_purchase_rees46`, frozen version 15, primary metric `pr_auc` (higher is better). Generated: 2026-11-23T13:07Z_

## Decision

- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: R10

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
- held-out read: partition test, frozen_delta_version 15, rows 1313711, groups 406466
- frame: 1309405 rows, columns 30, rows by partition {'validation': 1309405}

## Training

- model family: gbt_lightgbm; MLflow run `3342180df4ac47e1b0fd0c8adf21a866`
- parameters: {'row_fraction': 1.0, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: lightgbm 4.7.0
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_lightgbm held-out | gbt_lightgbm validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.08171 | 0.08128 | 0.08171 |
| calibration_error | 0.01379 | 0.01334 | 0.001506 |
| log_loss | 0.2125 | 0.2117 | 0.2829 |
| pr_auc | 0.4403 | 0.4399 | 0.08171 |
| pr_auc_lift | 5.389 | 5.412 | 1 |
| primary_change_relative | -0.0009798 |  | -0.005282 |
| skill_primary | 0.3586 |  |  |
| skill_primary_ci_high | 0.3765 |  |  |
| skill_primary_ci_low | 0.3432 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `base_rate`: validation 0.08128, held-out 0.08171 on `pr_auc`
