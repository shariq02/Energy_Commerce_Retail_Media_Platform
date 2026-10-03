# Model card: lapse_rees46.return

_model `gbt_lightgbm`, dataset `lapse_rees46`, frozen version 0, primary metric `pr_auc` (higher is better). Generated: 2026-10-03T23:11Z_

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

- estimates `target_returned` for the entities and period of dataset `lapse_rees46`
- comparison and analysis with the stated conditions below

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
- library versions: cloudpickle 3.0.0, lifelines 0.30.3, lightgbm 4.7.0, psutil 5.9.0, sklearn 1.6.1, sksurv 0.25.0, torch 2.14.1+cpu, xgboost 3.4.1
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_lightgbm held-out | gbt_lightgbm validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.463 | 0.4634 | 0.463 |
| bootstrap_blocks | 1.002e+05 |  |  |
| bootstrap_resamples_valid | 200 |  |  |
| bootstrap_rows_used | 1.002e+05 |  |  |
| calibration_error | 0.002463 | 0.00216 | 0.0001588 |
| log_loss | 0.59 | 0.5906 | 0.6904 |
| pr_auc | 0.7321 | 0.7318 | 0.463 |
| pr_auc_lift | 1.581 | 1.579 | 1 |
| pred_finite_share | 1 |  | 1 |
| pred_mean | 0.4631 | 0.4629 | 0.4631 |
| pred_positive_share | 0.3659 | 0.3653 | 0 |
| pred_std | 0.2132 |  | 2.776e-16 |
| primary_change_relative | -0.0004111 |  | 0.0008643 |
| reproduced_validation_value | 0.7318 |  | 0.4634 |
| reproduction_gap_relative | 0 |  | 0 |
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
