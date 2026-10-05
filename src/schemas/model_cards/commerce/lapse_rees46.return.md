# Model card: lapse_rees46.return

_model `gbt_lightgbm`, dataset `lapse_rees46`, frozen version 0, primary metric `pr_auc` (higher is better). Generated: 2026-10-04T18:03Z_

## Decision

- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: none

## Purpose and task

- paradigm: supervised; task type: classification
- target: `target_returned`
- baseline fallback: `base_rate`

## Intended use

- estimates the probability that a REES46 user returns
- target `target_returned`, dataset `lapse_rees46`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 453593 rows, columns 12, rows by partition {'test': 453593}
- held-out read: partition test, frozen_delta_version 0, rows 453593, groups 453593
- frame: 453829 rows, columns 12, rows by partition {'validation': 453829}

## Training

- model family: gbt_lightgbm; MLflow run `8bd902503c664e05a8252492f9cbec52`
- parameters: {'row_fraction': 1.0, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: lightgbm 4.7.0
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_lightgbm held-out | gbt_lightgbm validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.463 | 0.4634 | 0.463 |
| calibration_error | 0.002463 | 0.00216 | 0.0001588 |
| log_loss | 0.59 | 0.5906 | 0.6904 |
| pr_auc | 0.7321 | 0.7318 | 0.463 |
| pr_auc_lift | 1.581 | 1.579 | 1 |
| primary_change_relative | -0.0004111 |  | 0.0008643 |
| skill_primary | 0.2692 |  |  |
| skill_primary_ci_high | 0.2717 |  |  |
| skill_primary_ci_low | 0.2642 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- none

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `base_rate`: validation 0.4634, held-out 0.463 on `pr_auc`
