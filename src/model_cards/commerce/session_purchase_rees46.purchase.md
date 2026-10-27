# Model card: session_purchase_rees46.purchase

_model `gbt_sklearn`, dataset `session_purchase_rees46`, frozen version 1, primary metric `pr_auc` (higher is better). Generated: 2026-10-26T17:52Z_

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
- held-out read: partition test, frozen_delta_version 1, rows 1313711, groups 406466
- frame: 785035 rows, columns 30, rows by partition {'validation': 785035}

## Training

- model family: gbt_sklearn; MLflow run `08d766159e7147f9aa2be4018d79d835`
- parameters: {'row_fraction': 0.6, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: sklearn 1.6.1
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_sklearn held-out | gbt_sklearn validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.08171 | 0.08131 | 0.08171 |
| calibration_error | 0.01476 | 0.01437 | 0.001452 |
| log_loss | 0.2129 | 0.2125 | 0.2829 |
| pr_auc | 0.4398 | 0.4378 | 0.08171 |
| pr_auc_lift | 5.382 | 5.384 | 1 |
| primary_change_relative | -0.004488 |  | -0.004879 |
| skill_primary | 0.358 |  |  |
| skill_primary_ci_high | 0.3776 |  |  |
| skill_primary_ci_low | 0.3446 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- the held-out partition was read in at least 3 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `base_rate`: validation 0.08131, held-out 0.08171 on `pr_auc`
