# Model card: session_purchase_ga4.purchase

_model `gbt_sklearn`, dataset `session_purchase_ga4`, frozen version 0, primary metric `pr_auc` (higher is better). Generated: 2026-10-03T23:11Z_

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

- estimates `target_purchase_after_prefix` for the entities and period of dataset `session_purchase_ga4`
- comparison and analysis with the stated conditions below

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 2724 rows, columns 21, rows by partition {'test': 2724}
- held-out read: partition test, frozen_delta_version 0, rows 2724, groups 2180
- frame: 2609 rows, columns 21, rows by partition {'validation': 2609}

## Training

- model family: gbt_sklearn; MLflow run `ca4d9bf7143b4fe78c0f684e7dcc2f15`
- parameters: {'row_fraction': 1.0, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 15, 'min_child_samples': 50}
- library versions: cloudpickle 3.0.0, lifelines 0.30.3, lightgbm 4.7.0, psutil 5.9.0, sklearn 1.6.1, sksurv 0.25.0, torch 2.14.1+cpu, xgboost 3.4.1
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_sklearn held-out | gbt_sklearn validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.2551 | 0.2376 | 0.2551 |
| bootstrap_blocks | 2180 |  |  |
| bootstrap_resamples_valid | 200 |  |  |
| bootstrap_rows_used | 2724 |  |  |
| calibration_error | 0.01892 | 0.01851 | 0.01122 |
| log_loss | 0.5054 | 0.5002 | 0.5682 |
| pr_auc | 0.4584 | 0.3957 | 0.2551 |
| pr_auc_lift | 1.797 | 1.665 | 1 |
| pred_finite_share | 1 |  | 1 |
| pred_mean | 0.2474 | 0.2517 | 0.2439 |
| pred_positive_share | 0.0569 | 0.05788 | 0 |
| pred_std | 0.1363 |  | 5.551e-17 |
| primary_change_relative | -0.1585 |  | -0.07364 |
| reproduced_validation_value | 0.3957 |  | 0.2376 |
| reproduction_gap_relative | 0 |  | 0 |
| skill_primary | 0.2033 |  |  |
| skill_primary_ci_high | 0.2341 |  |  |
| skill_primary_ci_low | 0.1731 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- none

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `base_rate`: validation 0.2376, held-out 0.2551 on `pr_auc`
