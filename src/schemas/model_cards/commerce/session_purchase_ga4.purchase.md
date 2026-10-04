# Model card: session_purchase_ga4.purchase

_model `gbt_sklearn`, dataset `session_purchase_ga4`, frozen version 0, primary metric `pr_auc` (higher is better). Generated: 2026-10-04T17:19Z_

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

- estimates the probability that a GA4 session ends in a purchase, from the first events of the session
- target `target_purchase_after_prefix`, dataset `session_purchase_ga4`

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
- library versions: sklearn 1.6.1
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_sklearn held-out | gbt_sklearn validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.2551 | 0.2376 | 0.2551 |
| calibration_error | 0.01892 | 0.01851 | 0.01122 |
| log_loss | 0.5054 | 0.5002 | 0.5682 |
| pr_auc | 0.4584 | 0.3957 | 0.2551 |
| pr_auc_lift | 1.797 | 1.665 | 1 |
| primary_change_relative | -0.1585 |  | -0.07364 |
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
